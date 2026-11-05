"""
position_manager.py — 持仓退出与结算逻辑

职责拆分：
  - PnL 计算（纯函数）
  - CLOB 卖出（单轮）
  - 链上赎回兜底
  - 平仓循环编排（多轮残差卖出）
  - DB 记录
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import ROUND_DOWN, Decimal
from typing import Optional

from loguru import logger
from sqlalchemy import update

from app.models import Side
from app.pm_client import PolymarketClient
from app.runtime_config import get_config
from app.strategy.models import OpenPosition
from app.strategy.position_state import ExitReason


# ── 结果类型 ─────────────────────────────────────────────────────────────────

class CLOBExitError(Exception):
    """
    Raised by _close_loop when CLOB sell fails for a non-recoverable reason
    (e.g. persistent balance error, min_exit_price exceeded).
    The exit_controller catches this and retries up to sell_max_retry_count times.
    """
    def __init__(self, reason: str):
        super().__init__(f"sell failed: {reason}")
        self.reason = reason


class AlreadyClosedError(Exception):
    """
    Raised by _close_with_override when all tokens are already sold (live_balance=0).
    This replaces the previous RuntimeError(string) pattern which required string matching in callers.
    Carries the already-computed PnL so the caller doesn't need to re-compute it.
    """
    def __init__(self, asset: str, pnl_pct: float, pnl_usd: float):
        super().__init__(f"all tokens already sold for {asset}")
        self.pnl_pct = pnl_pct
        self.pnl_usd = pnl_usd


class LiveTraderUnavailableError(Exception):
    """
    Raised when a position entered in live mode must be closed, but no LiveTrader
    is currently available.  Callers must keep the position open and retry later —
    silently recording it as a dry close would leave real tokens unsold.
    """
    def __init__(self, asset: str):
        super().__init__(f"live trader unavailable while closing {asset}")
        self.asset = asset


@dataclass
class SellOutcome:
    """单次 sell_token 或 redeem 的结果"""
    success: bool
    tokens: float = 0.0
    usdc: float = 0.0
    fill_price: float = 0.0
    error: str = ""
    is_redeem: bool = False
    fee_usdc: float = 0.0


# ── PositionManager ────────────────────────────────────────────────────────────

class PositionManager:
    """
    封装持仓退出与结算逻辑：

      PnL 计算（纯函数）：
        unrealized_pnl_pct()
        closed_pnl()

      平仓入口：
        close_position()          — 对外主入口，协调所有子步骤

      内部子步骤：
        _fetch_sell_price()      — 从 market 或 API 拉取当前卖价
        _clob_exit()             — 单轮 CLOB sell，返回 SellOutcome
        _redeem_exit()           — 链上赎回兜底，返回 SellOutcome
        _close_loop()            — 多轮残差卖出循环
        _compute_pnl()           — 纯函数：计算 PnL
        _record_trade()           — DB + JSONL 写入
    """

    def __init__(self, pm: PolymarketClient):
        self._pm = pm

    # ── PnL 计算（纯函数）───────────────────────────────────────────────────

    def unrealized_pnl_pct(
        self, up_mid: float, down_mid: float, pos: OpenPosition
    ) -> float | None:
        """
        Returns the unrealized PnL percentage, or None if it cannot be computed.
        Returns None when:
          - current price or effective_entry_price is invalid (zero/negative)
          - effective_entry_price is near zero (would produce extreme/meaningless values)
        Callers must handle None and skip the PnL check in that case.
        """
        current = up_mid if pos.side == Side.UP else down_mid
        if current <= 0:
            return None
        if pos.effective_entry_price <= 0:
            return None
        # Guard: if anchor is near-zero, a tiny price movement could produce
        # extreme PnL values (e.g., anchor=1e-15, mid=0.45 → pnl = +45e12%).
        # Treat near-zero anchor as uncomputable to prevent spurious TP/SL triggers.
        if pos.effective_entry_price < 1e-9:
            return None
        return (current - pos.effective_entry_price) / pos.effective_entry_price * 100

    def _compute_pnl(
        self,
        pos: OpenPosition,
        exit_price: float,
        filled_tokens: float,
        exit_fee_usdc: float = 0.0,
    ) -> tuple[float, float]:
        """
        纯函数：根据 exit_price 和实际成交 token 数计算 PnL。
        不读写任何状态，不访问 DB。

        返回的是**扣除入场和出场手续费后的净 PnL**：
          gross_usd = filled_tokens × (exit_price - effective_entry_price)
          net_usd   = gross_usd - entry_fee_share - exit_fee_usdc
          pnl_pct   = net_usd / (cost_share + entry_fee_share) × 100

        Returns: (net_pnl_pct, net_pnl_usd)
        """
        if pos.effective_entry_price <= 0:
            return -100.0, 0.0

        # CRITICAL FIX: prevent NaN/inf when filled_tokens <= 0.
        if filled_tokens <= 0:
            return 0.0, 0.0

        ratio = filled_tokens / pos.tokens_bought if pos.tokens_bought > 0 else 1.0
        cost_usdc = pos.actual_cost_usdc * ratio
        entry_fee = float(getattr(pos, "entry_fee_usdc", 0.0) or 0.0) * ratio

        gross_pnl_usd = filled_tokens * (exit_price - pos.effective_entry_price)
        total_fee = entry_fee + float(exit_fee_usdc or 0.0)
        net_pnl_usd = gross_pnl_usd - total_fee

        invested = cost_usdc + entry_fee
        net_pnl_pct = (net_pnl_usd / invested * 100.0) if invested > 1e-12 else 0.0
        return net_pnl_pct, net_pnl_usd

    @staticmethod
    def _fee_bps_for(pos: OpenPosition) -> int:
        bps = int(getattr(pos, "taker_fee_bps", 0) or 0)
        if bps <= 0:
            bps = int(float(get_config().get("default_fee_bps", 30)))
        return max(0, bps)

    def _estimate_exit_fee(self, pos: OpenPosition, exit_price: float, tokens: float) -> float:
        """Fee estimate for dry-run / missing CLOB fee data."""
        if tokens <= 0 or exit_price <= 0:
            return 0.0
        return max(0.0, exit_price * tokens * self._fee_bps_for(pos) / 10_000.0)

    def closed_pnl(
        self, pos: OpenPosition, exit_price: float
    ) -> tuple[float, float]:
        return self._compute_pnl(pos, exit_price, pos.tokens_bought)

    # ── 价格获取 ────────────────────────────────────────────────────────────

    async def _fetch_sell_price(
        self,
        pos: OpenPosition,
        resolved_market=None,
        up_mid_cached: Optional[float] = None,
        down_mid_cached: Optional[float] = None,
    ) -> float:
        """
        获取当前卖价。
        优先使用 tick() 时已缓存的 _up_mid / _down_mid（节省 ~200-500ms HTTP 开销），
        无缓存时从 API 拉取。
        """
        mkt = resolved_market if resolved_market is not None else pos.market
        if mkt is not None:
            # 优先用 tick() 时已缓存的实时价格。
            # NOTE: _up_mid/_down_mid 的默认值是 0.0（不是 None），所以必须
            # 显式检查 > 0，否则会在价格尚未刷新时直接返回 0.0。
            cached_up = getattr(mkt, "_up_mid", 0.0) or 0.0
            cached_down = getattr(mkt, "_down_mid", 0.0) or 0.0
            if cached_up > 0 and cached_down > 0:
                return cached_up if pos.side == Side.UP else cached_down
            # 否则从 API 拉取
            up, down = await self._fetch_prices(mkt)
            return up if pos.side == Side.UP else down
        return 0.5  # 无 market 信息时的保守值

    async def _fetch_prices(self, market) -> tuple[float, float]:
        up_mid = down_mid = 0.5
        try:
            up_mid = await self._pm.get_midpoint(market.token_up_id)
        except Exception:
            pass
        try:
            down_mid = await self._pm.get_midpoint(market.token_down_id)
        except Exception:
            pass
        return up_mid, down_mid

    # ── CLOB 卖出（单轮）───────────────────────────────────────────────────

    async def _clob_exit(
        self,
        pos: OpenPosition,
        tokens_to_sell: float,
        sell_price: float,
        neg_risk: bool,
        live_trader,
    ) -> SellOutcome:
        """
        执行一轮 CLOB sell。
        始终返回 SellOutcome：成功时 success=True，失败时 success=False 并附带 error。
        caller 根据 outcome.success 判断是否走 redeem fallback。
        """
        result = await live_trader.sell_token(
            pos.token_id_bought,
            tokens_to_sell,
            sell_price,
            self._fee_bps_for(pos),
            neg_risk=neg_risk,
            # user_usdc_balance is intentionally omitted for SELL orders.
            # The CLOB validates SELL against token balance (not pUSD), so passing a
            # human-unit pUSD value would be incorrectly interpreted as micro-units by
            # the SDK and cause "not enough balance" errors.
        )
        if result and result.success:
            # Invalidate token balance cache so the next get_token_balance() always
            # hits live CLOB.  Without this, the 1.5s TTL cache can return a stale
            # balance after a partial fill, causing the next sell round to attempt to
            # sell more tokens than are available and trigger a 400 "not enough balance"
            # error on CLOB's internal matched-orders accounting.
            live_trader.invalidate_token_balance_cache(pos.token_id_bought)
            return SellOutcome(
                success=True,
                tokens=result.tokens if result.tokens else tokens_to_sell,
                usdc=result.usdc,
                fill_price=result.fill_price,
                fee_usdc=float(result.fee_usdc or 0.0),
            )
        error = result.error if result else "unknown"
        return SellOutcome(success=False, error=error)

    # ── 链上赎回兜底 ────────────────────────────────────────────────────────

    async def _redeem_exit(
        self,
        pos: OpenPosition,
        tokens_to_redeem: float,
        live_trader,
    ) -> SellOutcome:
        """
        执行链上 redeemPositions 赎回。
        返回 SellOutcome(is_redeem=True)。
        redeem 成功时 tokens=全部代为赎回的 tokens，usdc=实际回收的 USDC。
        """
        try:
            redeem_result = await live_trader.redeem_open_positions_async(
                size_threshold=0.01, condition_id=pos.condition_id,
            )
            redeem_usdc = float(redeem_result.get("total_usdc", 0.0))
            logger.info(
                "PositionManager redeem: {} tokens, recovered {:.4f} USDC",
                tokens_to_redeem, redeem_usdc,
            )
            return SellOutcome(
                success=True,
                tokens=tokens_to_redeem,
                usdc=redeem_usdc,
                fill_price=(
                    redeem_usdc / tokens_to_redeem if tokens_to_redeem > 1e-9
                    else 0.0
                ),
                is_redeem=True,
            )
        except Exception as exc:
            logger.error("PositionManager redeem failed: {}", exc)
            return SellOutcome(success=False, error=str(exc))

    # ── 多轮残差卖出循环 ────────────────────────────────────────────────────

    async def _close_loop(
        self,
        pos: OpenPosition,
        sell_price: float,
        neg_risk: bool,
        live_trader,
        already_recorded: bool,
        tokens_at_entry: float,
        close_reason: str = ExitReason.TAKE_PROFIT,
        closed_at_override: Optional[datetime] = None,
    ) -> tuple[float, float, float, str, float, bool]:
        """
        循环执行 CLOB sell，直到余额清零或超过最大轮数。
        Returns: (total_sold_tokens, total_sold_usdc, total_fee_usdc,
                  final_reason, avg_exit_price, already_recorded)

        Partial fills are handled internally: when CLOB only fills part of the order the
        loop continues immediately rather than raising.  This guarantees that all tokens
        are sold before the function returns, preventing addon logic from re-entering
        while a partial fill is still being worked off.

        CRITICAL: pos.tokens_bought and pos.actual_cost_usdc are NEVER mutated inside
        this function.  All trade recording uses tokens_at_entry (the original position
        size at entry).  The caller is responsible for updating pos state after this
        function returns.
        """
        # tokens_at_entry: the authoritative position size for PnL / trade recording.
        # Never use remaining_tokens for _record_trade — that's for balance accounting only.
        remaining_tokens = float(pos.tokens_bought)
        total_sold_tokens = 0.0
        total_sold_usdc = 0.0
        total_fee_usdc = 0.0
        # Track actual average fill price across all CLOB fills (excl. dust/force-sell
        # cleanup rounds which pollute totals and must not distort avg_exit).
        _actual_fill_total_usdc = 0.0
        _actual_fill_total_tokens = 0.0

        def _avg_fill() -> float:
            if _actual_fill_total_tokens > 1e-9:
                return _actual_fill_total_usdc / _actual_fill_total_tokens
            return sell_price

        residual_rounds = 0
        max_rounds = 3
        final_reason = ""

        while True:
            # 每轮重新查询 live balance，避免缓存延迟
            # Invalidate cache FIRST so we always read fresh CLOB data.
            # Also invalidate before the sell call — sell_token has its own internal
            # balance queries; invalidating before sell ensures those queries see fresh data
            # when a prior matched sell order is being settled by CLOB.
            live_trader.invalidate_token_balance_cache(pos.token_id_bought)
            current_live = await live_trader.get_token_balance(pos.token_id_bought)
            live_balance = current_live if current_live is not None else remaining_tokens

            tokens_to_sell = min(max(remaining_tokens, 0.0), max(live_balance, 0.0))

            if tokens_to_sell <= 1e-9:
                # 余额已清 — 理论上不应该走到这里（close_position 已检查 live_balance>0），
                # 但以防万一（并发平仓等），补写 Trade 记录以保证完整性
                final_reason = ExitReason.ALREADY_CLOSED
                # 此分支 tokens_to_sell==0，无实际卖出，不做除法，直接用市场价
                avg_exit = sell_price
                if not already_recorded and total_sold_tokens <= 1e-9:
                    await self._record_trade(
                        pos, avg_exit, close_reason, closed_at_override,
                        total_sold_tokens=tokens_at_entry,
                        exit_fee_usdc=total_fee_usdc,
                    )
                    pos._trade_recorded = True
                    already_recorded = True
                return total_sold_tokens, total_sold_usdc, total_fee_usdc, final_reason, avg_exit, already_recorded

            outcome = await self._clob_exit(
                pos, tokens_to_sell, sell_price, neg_risk, live_trader,
            )

            # ── dust_skipped：余额低于 tick_size ──────────────────────────────
            if not outcome.success and outcome.error == "dust_skipped":
                logger.info(
                    "close_loop {} dust skipped (tracked={:.6f}, live={:.6f})",
                    pos.asset, tokens_to_sell, live_balance,
                )
                # Record the trade using the ORIGINAL position size (tokens_at_entry),
                # not the residual amount.  Dust cleanup is balance-only and MUST NOT
                # alter the PnL record.
                if not already_recorded:
                    await self._record_trade(
                        pos, sell_price, close_reason, closed_at_override,
                        total_sold_tokens=tokens_at_entry,
                        exit_fee_usdc=total_fee_usdc,
                    )
                    # CRITICAL: also set pos._trade_recorded so close_position's
                    # own _record_trade call at the end does not create a duplicate.
                    pos._trade_recorded = True
                    already_recorded = True
                    pnl_pct, pnl_usd = self._compute_pnl(
                        pos, sell_price, tokens_at_entry, total_fee_usdc,
                    )
                    logger.info(
                        "close_loop {} trade recorded: {} tokens @ {:.4f}, pnl={:.2f}%",
                        pos.asset, tokens_at_entry, sell_price, pnl_pct,
                    )
                final_reason = ExitReason.DUST_CLEARED
                # 尝试最后清仓一次（用当前实际余额），但不算入 PnL 统计
                final_bal = await live_trader.get_token_balance(pos.token_id_bought)
                if final_bal is not None and final_bal > 1e-9:
                    final_outcome = await self._clob_exit(
                        pos, final_bal, sell_price, neg_risk, live_trader,
                    )
                    if final_outcome.success:
                        logger.info(
                            "close_loop {} dust final sell: {:.6f} tokens @ {:.4f}",
                            pos.asset, final_outcome.tokens, final_outcome.fill_price,
                        )
                # avg_exit uses sell_price — the dust sell is not part of the trade PnL
                avg_exit = sell_price
                return total_sold_tokens, total_sold_usdc, total_fee_usdc, final_reason, avg_exit, already_recorded

            if not outcome.success:
                error = outcome.error
                is_price_too_low = any(
                    kw in error.lower()
                    for kw in ("out of safe range", "min_exit_price", "dust_skipped")
                )
                if is_price_too_low:
                    redeem_outcome = await self._redeem_exit(pos, tokens_to_sell, live_trader)
                    if redeem_outcome.success and redeem_outcome.usdc > 0:
                        avg_exit = (
                            redeem_outcome.usdc / tokens_to_sell
                            if tokens_to_sell > 1e-9
                            else sell_price
                        )
                        return tokens_to_sell, redeem_outcome.usdc, total_fee_usdc, "redeem_fallback", avg_exit, already_recorded
                    elif sell_price > 1e-9:
                        return tokens_to_sell, tokens_to_sell * sell_price, total_fee_usdc, ExitReason.CLOB_REDEEM, sell_price, already_recorded
                    else:
                        return tokens_to_sell, 0.0, total_fee_usdc, ExitReason.REDEEM_ZERO, 0.0, already_recorded
                raise CLOBExitError(error)

            # ── CLOB 卖出成功 ────────────────────────────────────────────────
            actual_tokens = outcome.tokens
            actual_usdc = outcome.usdc

            # ── 部分成交检测 ────────────────────────────────────────────────
            # sell_token caps by live balance, but CLOB balance cache may lag.
            # Use pre/post balance diff to determine the authoritative filled amount.
            if actual_tokens and abs(actual_tokens - tokens_to_sell) > 1e-9:
                post_sell_live = None
                for attempt in range(3):
                    live_trader.invalidate_token_balance_cache(pos.token_id_bought)
                    post_sell_live = await live_trader.get_token_balance(pos.token_id_bought)
                    if post_sell_live is None:
                        await asyncio.sleep(0.3 * (attempt + 1))
                        continue
                    break

                # CRITICAL: NEVER mutate pos state inside this function.
                # Use local remaining_tokens only.  The caller updates pos after return.
                if post_sell_live is not None:
                    remaining_tokens = max(0.0, remaining_tokens - actual_tokens)
                    logger.debug(
                        "close_loop {} partial fill: sold={:.4f}, remaining={:.4f} — continuing loop",
                        pos.asset, actual_tokens, remaining_tokens,
                    )
                else:
                    logger.warning(
                        "close_loop {} post-sell balance unavailable after 3 attempts — "
                        "using API result (remaining={:.4f})",
                        pos.asset, remaining_tokens,
                    )
                    remaining_tokens = max(0.0, remaining_tokens - actual_tokens)

                # Add partial fill to totals and continue immediately.
                total_sold_tokens += actual_tokens
                total_sold_usdc += actual_usdc
                total_fee_usdc += float(outcome.fee_usdc or 0.0)
                # Track actual fill average separately from dust/force-sell cleanup proceeds.
                _actual_fill_total_tokens += actual_tokens
                _actual_fill_total_usdc += actual_usdc

                # Invalidate token cache BEFORE querying live balance so that
                # subsequent iterations always read the true CLOB balance, not a
                # stale cached value.  Without this, the TTL cache (1.5 s) can
                # return a stale inflated balance after a partial fill, causing
                # the next sell round to attempt to sell more tokens than are
                # actually available, triggering a 400 "not enough balance" error.
                live_trader.invalidate_token_balance_cache(pos.token_id_bought)

                post_live = await live_trader.get_token_balance(pos.token_id_bought)
                if post_live is None or post_live <= 1e-9:
                    avg_exit = _avg_fill()
                    return total_sold_tokens, total_sold_usdc, total_fee_usdc, "", avg_exit, already_recorded
                continue

            total_sold_tokens += actual_tokens
            total_sold_usdc += actual_usdc
            total_fee_usdc += float(outcome.fee_usdc or 0.0)
            remaining_tokens = max(0.0, remaining_tokens - actual_tokens)
            final_reason = close_reason
            # Track actual fill average separately from dust/force-sell cleanup proceeds.
            _actual_fill_total_tokens += actual_tokens
            _actual_fill_total_usdc += actual_usdc

            # Invalidate token balance cache so the next iteration always reads fresh data.
            live_trader.invalidate_token_balance_cache(pos.token_id_bought)

            # 检查是否还有残差
            post_live = await live_trader.get_token_balance(pos.token_id_bought)
            if post_live is None or post_live <= 1e-9:
                avg_exit = _avg_fill()
                return total_sold_tokens, total_sold_usdc, total_fee_usdc, final_reason, avg_exit, already_recorded

            residual_rounds += 1
            if residual_rounds > max_rounds:
                logger.error(
                    "close_loop {} exhausted {} rounds — force-sell remaining {:.4f}",
                    pos.asset, max_rounds, post_live,
                )
                # Record trade using tokens_at_entry (original position size) BEFORE
                # the force-sell pollutes the balance.  The PnL is based on what was
                # entered, not what we failed to sell.
                # NOTE: initialise unconditionally — already_recorded may be True,
                # in which case avg_first_exit is still needed by the return below.
                avg_first_exit = _avg_fill()
                if not already_recorded:
                    await self._record_trade(
                        pos, avg_first_exit, close_reason, closed_at_override,
                        total_sold_tokens=tokens_at_entry,
                        exit_fee_usdc=total_fee_usdc,
                    )
                    # CRITICAL: also set pos._trade_recorded so close_position's
                    # own _record_trade call at the end does not create a duplicate.
                    pos._trade_recorded = True
                    already_recorded = True
                    pnl_pct, _ = self._compute_pnl(
                        pos, avg_first_exit, tokens_at_entry, total_fee_usdc,
                    )
                    logger.info(
                        "close_loop {} trade recorded (残差轮次耗尽): "
                        "{} tokens @ {:.4f}, pnl={:.2f}%",
                        pos.asset, tokens_at_entry, avg_first_exit, pnl_pct,
                    )
                final_reason = ExitReason.RESIDUAL_EXHAUSTED
                force = await self._clob_exit(pos, post_live, sell_price, neg_risk, live_trader)
                if force.success:
                    # Force-sell proceeds are for balance cleanup only — do NOT add to
                    # total_sold_tokens/total_sold_usdc for PnL purposes (trade already recorded).
                    # Return the actual fill average from real CLOB fills, NOT the cleanup price.
                    remaining_tokens = 0.0
                    return total_sold_tokens, total_sold_usdc, total_fee_usdc, final_reason, avg_first_exit, already_recorded
                # CLOB force-sell 失败 → 链上 redeem 兜底，防止 token 永久锁在 CLOB
                logger.warning(
                    "close_loop {} CLOB force-sell failed — attempting on-chain redeem for {:.4f} tokens",
                    pos.asset, post_live,
                )
                redeem_outcome = await self._redeem_exit(pos, post_live, live_trader)
                if redeem_outcome.success and redeem_outcome.usdc > 0:
                    redeem_avg = redeem_outcome.usdc / post_live if post_live > 1e-9 else 0.0
                    logger.info(
                        "close_loop {} redeem fallback recovered {:.4f} USDC from {:.4f} tokens (avg={:.4f})",
                        pos.asset, redeem_outcome.usdc, post_live, redeem_avg,
                    )
                    return total_sold_tokens, total_sold_usdc, total_fee_usdc, ExitReason.REDEEM_SUCCESS, redeem_avg, already_recorded
                # 所有兜底均失败 → operator 告警（CRITICAL），抛出专用异常让调用方不要再重试
                logger.error(
                    "CRITICAL: close_loop {} EXHAUSTED ALL RECOVERY OPTIONS — "
                    "{} tokens ({:.4f} USDC at avg_exit {:.4f}) may be permanently locked in CLOB. "
                    "Manual intervention required. "
                    "condition_id={} asset={} pos_tokens={:.4f}",
                    pos.asset,
                    post_live,
                    post_live * sell_price,
                    sell_price,
                    pos.condition_id[:16] + "..." if pos.condition_id else "N/A",
                    pos.asset,
                    pos.tokens_bought,
                )
                raise CLOBExitError(
                    f"force-sell failed; redeem also failed for {post_live} tokens"
                )

            logger.info(
                "close_loop {} round {}/{}: {:.4f} tokens remain — retrying",
                pos.asset, residual_rounds, max_rounds, post_live,
            )
            remaining_tokens = max(0.0, post_live)

    # ── 平仓入口 ────────────────────────────────────────────────────────────

    async def close_position(
        self,
        pos: OpenPosition,
        close_reason: str,
        override_exit_price: Optional[float] = None,
        closed_at_override: Optional[datetime] = None,
        live_trader=None,
        skip_record: bool = False,
        resolved_market=None,
        settle_up_wins: Optional[bool] = None,
        partial_close_pct: Optional[float] = None,
    ) -> tuple[float, float]:
        """
        平仓主入口。
        settle_up_wins: 从 settle_if_due 获取的结算胜负，用于 Gamma 超时时的兜底 effective price。
        partial_close_pct: 部分平仓比例（0.0~1.0），目前固定为 None（全额平仓）。
        Returns: (pnl_pct, pnl_usd)
        """
        mkt = resolved_market if resolved_market is not None else pos.market
        already_recorded = pos._trade_recorded
        # Always reset to 1.0 (full close) on new close call; override only for partial.
        pos._pending_partial_pct = partial_close_pct if partial_close_pct is not None else 1.0

        # ── Execution-mode guard ──────────────────────────────────────────
        # The position's own entry mode is authoritative.  A live position must
        # never be recorded as a dry close (which would strand real tokens in the
        # wallet), and a dry position must never send a real sell order even if a
        # caller passes a LiveTrader reference.
        pos_mode = getattr(pos, "execution_mode", "dry")
        if pos_mode == "live":
            if live_trader is None:
                raise LiveTraderUnavailableError(pos.asset)
        else:
            live_trader = None

        # ── skip_record 路径：仅清理链上余额 ────────────────────────────────
        if skip_record:
            if live_trader is not None:
                try:
                    await live_trader.redeem_open_positions_async(
                        size_threshold=0.01, condition_id=pos.condition_id,
                    )
                except Exception as exc:
                    logger.debug("skip_record redeem non-fatal: {}", exc)
            return 0.0, 0.0

        # settle 路径（override_exit_price=None，但 settle_up_wins 有值）：
        # UP 胜出：UP 方向 exit=1.0，DOWN 方向 exit=0.0
        # DOWN 胜出：UP 方向 exit=0.0，DOWN 方向 exit=1.0
        if override_exit_price is None and settle_up_wins is not None:
            if pos.side == Side.UP:
                override_exit_price = 1.0 if settle_up_wins else 0.0
            else:
                override_exit_price = 0.0 if settle_up_wins else 1.0

        if override_exit_price is not None:
            pnl_pct, pnl_usd = await self._close_with_override(
                pos, close_reason, override_exit_price,
                closed_at_override, live_trader, mkt, already_recorded,
            )
            pos._last_pnl_pct = pnl_pct
            pos._last_pnl_usd = pnl_usd
            return pnl_pct, pnl_usd

        # ── 普通路径：先用 cached price，无则拉取 ────────────────────────────
        sell_price = await self._fetch_sell_price(pos, mkt)

        # Dry-run 成本模型：按资产滑点扣减卖出价，使模拟 PnL 更接近实盘
        if live_trader is None and sell_price > 0:
            cfg = get_config()
            slip_key = "btc_slippage_pct" if pos.asset == "BTC" else "eth_slippage_pct"
            slip_pct = max(0.0, float(cfg.get(slip_key, 1.0)))
            sell_price = max(0.0001, sell_price * (1.0 - slip_pct / 100.0))

        dry_exit_fee = self._estimate_exit_fee(pos, sell_price, pos.tokens_bought)
        pnl_pct_preview, _ = self._compute_pnl(
            pos, sell_price, pos.tokens_bought, dry_exit_fee,
        )

        if live_trader is None:
            if not already_recorded:
                pos._trade_recorded = True
                pos._trade_recorded_reason = close_reason
                await self._record_trade(
                    pos, sell_price, close_reason, closed_at_override,
                    total_sold_tokens=pos.tokens_bought,
                    exit_fee_usdc=dry_exit_fee,
                )
            pnl_pct, pnl_usd = self._compute_pnl(
                pos, sell_price, pos.tokens_bought, dry_exit_fee,
            )
            pos._last_pnl_pct = pnl_pct
            pos._last_pnl_usd = pnl_usd
            return pnl_pct, pnl_usd

        # ── 同步 live balance（带重试，最多 3 次）────────────────────────────────
        neg_risk = mkt.neg_risk if mkt else False
        # Trigger balance cache refresh (has side-effect of resetting _balance_cache_stale flag)
        await live_trader.get_balance_and_allowance("COLLATERAL")

        live_balance = None
        for attempt in range(3):
            live_balance = await live_trader.get_token_balance(pos.token_id_bought)
            if live_balance is not None:
                break
            logger.warning(
                "close_position {} token balance query attempt {}/3 failed — invalidating cache",
                pos.asset, attempt + 1,
            )
            live_trader.invalidate_token_balance_cache(pos.token_id_bought)
            if attempt < 2:
                await asyncio.sleep(1.0)
        if live_balance is None:
            logger.warning(
                "close_position {} token balance unavailable after 3 attempts — falling back to in-memory tokens_bought",
                pos.asset,
            )
            live_balance = pos.tokens_bought

        if live_balance <= 1e-9:
            logger.info(
                "close_position {} all tokens already sold (live=0)",
                pos.asset,
            )
            # Capture tokens_at_entry before using it (same value as the non-branch path below).
            tokens_at_entry = pos.tokens_bought
            if not already_recorded:
                pos._trade_recorded = True
                pos._trade_recorded_reason = ExitReason.ALREADY_CLOSED
                await self._record_trade(
                    pos, sell_price, ExitReason.ALREADY_CLOSED, closed_at_override,
                    total_sold_tokens=tokens_at_entry,
                    exit_fee_usdc=0.0,
                )
            pnl_pct, pnl_usd = self._compute_pnl(pos, sell_price, tokens_at_entry, 0.0)
            pos._last_pnl_pct = pnl_pct
            pos._last_pnl_usd = pnl_usd
            return pnl_pct, pnl_usd

        # tokens_at_entry: the authoritative position size at the time close_position was called.
        # This is the only correct value for trade recording and PnL computation.
        # _close_loop may update pos.tokens_bought (residual after partial fills) for its
        # own balance accounting, but trade records always use tokens_at_entry.
        tokens_at_entry = pos.tokens_bought

        # ── 部分平仓：仅卖出 50% ──────────────────────────────────────────────
        if partial_close_pct and partial_close_pct > 0 and live_trader is not None:
            tokens_to_sell = tokens_at_entry * partial_close_pct
            live_trader.invalidate_token_balance_cache(pos.token_id_bought)
            current_live = await live_trader.get_token_balance(pos.token_id_bought)
            if current_live is not None:
                tokens_to_sell = min(tokens_to_sell, max(current_live, 0.0))
            if tokens_to_sell > 1e-9:
                outcome = await self._clob_exit(
                    pos, tokens_to_sell, sell_price, neg_risk, live_trader,
                )
                if outcome.success:
                    # 更新持仓：减少 token 数量和成本
                    sold_ratio = tokens_to_sell / tokens_at_entry
                    pos.tokens_bought = float(pos.tokens_bought) - tokens_to_sell
                    pos.actual_cost_usdc = float(pos.actual_cost_usdc) * (1.0 - sold_ratio)
                    if pos.tokens_bought > 1e-12:
                        pos.effective_entry_price = float(
                            Decimal(str(pos.actual_cost_usdc)) / Decimal(str(pos.tokens_bought))
                        )
                    pos.anchor_price = pos.effective_entry_price
                    logger.info(
                        "PARTIAL EXIT {} sold {:.2f}% ({} tokens) @ {:.4f}, remaining={:.4f}",
                        pos.asset, partial_close_pct * 100, tokens_to_sell,
                        outcome.fill_price, pos.tokens_bought,
                    )
                    # 记录这笔交易
                    exit_fee = float(outcome.fee_usdc or 0.0)
                    if not already_recorded:
                        pos._trade_recorded = True
                        pos._trade_recorded_reason = close_reason
                        await self._record_trade(
                            pos, outcome.fill_price, close_reason, closed_at_override,
                            total_sold_tokens=tokens_to_sell,
                            exit_fee_usdc=exit_fee,
                        )
                    # Bug-4 fix: PnL 分母统一用 tokens_at_entry（入场总量），
                    # 与全额平仓路径（_close_loop）保持一致。
                    pnl_pct, pnl_usd = self._compute_pnl(
                        pos, outcome.fill_price, tokens_at_entry, exit_fee,
                    )
                    pos._last_pnl_pct = pnl_pct
                    pos._last_pnl_usd = pnl_usd
                    return pnl_pct, pnl_usd

        # ── 执行多轮卖出循环 ─────────────────────────────────────────────────
        (
            total_sold_tokens,
            total_sold_usdc,
            total_fee_usdc,
            final_reason,
            avg_exit,
            recorded_by_loop,
        ) = await self._close_loop(
            pos, sell_price, neg_risk, live_trader,
            already_recorded, tokens_at_entry,
            close_reason, closed_at_override,
        )

        # ── 记录平仓 ────────────────────────────────────────────────────────
        # PnL is always computed from the FULL position (all fills) at avg_exit.
        if not recorded_by_loop:
            pos._trade_recorded = True
            pos._trade_recorded_reason = final_reason or close_reason
            await self._record_trade(
                pos, avg_exit, pos._trade_recorded_reason, closed_at_override,
                total_sold_tokens=tokens_at_entry,
                exit_fee_usdc=total_fee_usdc,
            )
        pnl_pct, pnl_usd = self._compute_pnl(
            pos, avg_exit, tokens_at_entry, total_fee_usdc,
        )
        pos._last_pnl_pct = pnl_pct
        pos._last_pnl_usd = pnl_usd
        return pnl_pct, pnl_usd

    async def _close_with_override(
        self,
        pos: OpenPosition,
        close_reason: str,
        override_exit_price: float,
        closed_at_override: Optional[datetime],
        live_trader,
        mkt,
        already_recorded: bool,
    ) -> tuple[float, float]:
        """override_exit_price 专用路径：结算/强制平仓"""
        avg_exit = override_exit_price
        final_reason: str = close_reason  # 兜底初始化，所有分支必须显式覆盖
        pnl_pct = 0.0
        pnl_usd = 0.0
        exit_fee = 0.0

        # CRITICAL: capture tokens_at_entry BEFORE any mutation.  This is the
        # authoritative position size for trade recording and PnL computation.
        # _clob_exit does not mutate pos.state, but the caller passes this same
        # pos to _record_trade, so the snapshot must be taken here.
        tokens_at_entry = pos.tokens_bought

        if live_trader is not None:
            live_trader.invalidate_token_balance_cache(pos.token_id_bought)
            neg_risk = mkt.neg_risk if mkt else False

            # 同步 live balance（带重试，最多 3 次）
            live_balance = None
            for attempt in range(3):
                live_balance = await live_trader.get_token_balance(pos.token_id_bought)
                if live_balance is not None:
                    break
                logger.warning(
                    "close_with_override {} token balance attempt {}/3 failed",
                    pos.asset, attempt + 1,
                )
                live_trader.invalidate_token_balance_cache(pos.token_id_bought)
                if attempt < 2:
                    await asyncio.sleep(1.0)
            if live_balance is None:
                logger.warning(
                    "close_with_override {} token balance unavailable — falling back to in-memory tokens_bought",
                    pos.asset,
                )
                live_balance = pos.tokens_bought
            tokens_to_sell = max(live_balance, 0.0)

            if tokens_to_sell > 1e-9:
                outcome = await self._clob_exit(
                    pos, tokens_to_sell, override_exit_price, neg_risk, live_trader,
                )
                if outcome.success:
                    avg_exit = outcome.fill_price
                    final_reason = close_reason
                    exit_fee = float(outcome.fee_usdc or 0.0)
                    # Invalidate pUSD collateral cache so next entry sees fresh balance
                    live_trader.invalidate_balance_cache()
                else:
                    # CLOB 拒绝 → redeem 兜底
                    redeem_outcome = await self._redeem_exit(
                        pos, tokens_to_sell, live_trader,
                    )
                    if redeem_outcome.success and redeem_outcome.usdc > 0:
                        avg_exit = redeem_outcome.fill_price
                        final_reason = ExitReason.REDEEM_SUCCESS
                    elif override_exit_price > 1e-9:
                        avg_exit = override_exit_price
                        final_reason = ExitReason.REDEEM_FAIL
                    else:
                        avg_exit = 0.0
                        final_reason = ExitReason.REDEEM_ZERO

                if not already_recorded:
                    pos._trade_recorded = True
                    pos._trade_recorded_reason = final_reason
                    await self._record_trade(
                        pos, avg_exit, final_reason, closed_at_override,
                        total_sold_tokens=tokens_at_entry,
                        exit_fee_usdc=exit_fee,
                    )
                pnl_pct, pnl_usd = self._compute_pnl(
                    pos, avg_exit, tokens_at_entry, exit_fee,
                )
            else:
                # All tokens already sold — raise so caller treats this as a clean
                # close and clears ctx.position. Previously this silently returned,
                # leaving ctx.position in place and blocking hot-restart entries.
                # Compute PnL from the authoritative tokens_at_entry (snapshot taken at
                # the top of this method, before any state was mutated).
                if not already_recorded:
                    pos._trade_recorded = True
                    pos._trade_recorded_reason = close_reason
                    await self._record_trade(
                        pos, override_exit_price, close_reason,
                        closed_at_override,
                        total_sold_tokens=tokens_at_entry,
                        exit_fee_usdc=0.0,
                    )
                pnl_pct, pnl_usd = self._compute_pnl(
                    pos, override_exit_price, tokens_at_entry, 0.0,
                )
                avg_exit = override_exit_price
                raise AlreadyClosedError(pos.asset, pnl_pct, pnl_usd)
        else:
            if not already_recorded:
                pos._trade_recorded = True
                pos._trade_recorded_reason = close_reason
                await self._record_trade(
                    pos, override_exit_price, close_reason, closed_at_override,
                    total_sold_tokens=tokens_at_entry,
                    exit_fee_usdc=0.0,
                )
            pnl_pct, pnl_usd = self._compute_pnl(
                pos, override_exit_price, tokens_at_entry, 0.0,
            )
            avg_exit = override_exit_price

        pos._last_pnl_pct = pnl_pct
        pos._last_pnl_usd = pnl_usd
        return pnl_pct, pnl_usd

    # ── 结算（窗口到期）──────────────────────────────────────────────────────

    async def settle_if_due(
        self,
        pos: OpenPosition,
        resolved_market=None,
    ) -> tuple[bool, Optional[datetime], Optional[float], Optional[dict], Optional[bool]]:
        """
        检查窗口是否已到期且结算结果可用。

        Returns: (settled, settle_at, effective_price, backfill_data, up_wins)
        up_wins is None when settled=False; otherwise True/False indicating which side won.

        结算流程（live / dry 一致）：
          1. 窗口结束后先等待 settle_grace_sec
          2. 向 Gamma 查询结算结果；拿到结果即返回
          3. 超过 grace + max_wait 仍拿不到 → 回退到 CLOB 中间价
          4. 其余情况返回 settled=False，调用方必须继续持仓等待，
             不能按中间价卖出（到期后 CLOB 通常已停止撮合）

        每次调用只查询一次 Gamma，不在这里 sleep/重试；主循环会在后续
        tick 继续调度，避免阻塞策略循环。
        """
        cfg = get_config()
        grace_sec = cfg.get("settle_grace_sec", 90)
        max_wait_sec = cfg.get("max_settle_wait_sec", 600)
        now = datetime.now(timezone.utc)
        # past_window_sec: seconds elapsed since window closed.  Positive after window_end,
        # negative before.  Used to check grace period and compute settle timeout.
        past_window_sec = (now - pos.window_end).total_seconds()

        if past_window_sec < grace_sec:
            return False, None, None, None, None

        # 结算必须用建仓窗口的价格，不允许跨窗口判断胜负
        settle_mkt = pos.market or resolved_market

        res = await self._resolve(pos.condition_id)
        if res and res.get("resolved"):
            up_wins = res.get("up_wins")
            if up_wins is not None:
                # effective 直接从结算结果得出（胜负 = 1.0/0.0），避免用已过期 CLOB 盘口
                effective = 1.0 if (up_wins and pos.side == Side.UP) or (not up_wins and pos.side == Side.DOWN) else 0.0
                up_final = 1.0 if up_wins else 0.0
                down_final = 0.0 if up_wins else 1.0
                backfill = {
                    "up_wins": up_wins,
                    "up_price_final": up_final,
                    "down_price_final": down_final,
                }
                return True, now, effective, backfill, up_wins

        if past_window_sec >= grace_sec + max_wait_sec:
            # 结算兜底条件：
            #   past_window_sec = elapsed since window_end
            #   grace_sec       = wait for Gamma to become available (default 90s after window_end)
            #   max_wait_sec    = additional timeout before giving up on Gamma (default 600s)
            #   Total max wait  = grace_sec + max_wait_sec = 90 + 600 = 690s after window close
            # If Gamma still hasn't resolved within this window, fall back to CLOB mid-price.
            if settle_mkt:
                up_mid, down_mid = await self._fetch_prices(settle_mkt)
            else:
                up_mid, down_mid = 0.5, 0.5
            effective = up_mid if pos.side == Side.UP else down_mid
            up_wins = up_mid >= 0.5
            backfill = {
                "up_wins": up_wins,
                "up_price_final": up_mid,
                "down_price_final": down_mid,
            }
            logger.warning(
                "settle_if_due {}: Gamma resolution unavailable after {:.0f}s past window_end — "
                "falling back to CLOB mid (up={:.4f})",
                pos.condition_id[:12], past_window_sec, up_mid,
            )
            return True, now, effective, backfill, up_wins

        logger.debug(
            "settle_if_due {}: awaiting Gamma resolution ({:.0f}s past window_end)",
            pos.condition_id[:12], past_window_sec,
        )
        return False, None, None, None, None

    async def _resolve(self, condition_id: str) -> Optional[dict]:
        try:
            return await self._pm.get_market_resolution(condition_id)
        except Exception:
            return None

    # ── DB + JSONL 记录 ─────────────────────────────────────────────────────

    async def _record_trade(
        self,
        pos: OpenPosition,
        exit_price: float,
        close_reason: str,
        closed_at_override: Optional[datetime] = None,
        total_sold_tokens: Optional[float] = None,
        exit_fee_usdc: float = 0.0,
    ):
        """
        将平仓记录写入 DB (Trade 表) 和 JSONL 备份。
        同时把之前暂存的 Fill 记录的 trade_id 更新为正式 ID。

        pnl_usd / pnl_pct 为扣除双边手续费后的净值；gross_pnl_usd 保留
        未扣手续费的盈亏，fee_usdc 记录本笔合计手续费。
        """
        filled_tokens = total_sold_tokens if total_sold_tokens is not None else pos.tokens_bought
        ratio = filled_tokens / pos.tokens_bought if pos.tokens_bought > 0 else 1.0
        entry_fee = float(getattr(pos, "entry_fee_usdc", 0.0) or 0.0) * ratio
        gross_pnl_usd = filled_tokens * (exit_price - pos.effective_entry_price)
        total_fee = entry_fee + float(exit_fee_usdc or 0.0)
        pnl_pct, pnl_usd = self._compute_pnl(
            pos, exit_price, filled_tokens, exit_fee_usdc,
        )

        from app.database import async_session
        from app.models import EntryRecord, ExitRecord, Fill, Trade

        mode = getattr(pos, "execution_mode", "") or (
            "live" if get_config().get("live_mode", False) else "dry"
        )
        now = closed_at_override if closed_at_override is not None else datetime.now(timezone.utc)
        partial_pct = pos._pending_partial_pct

        async with async_session() as db:
            # ── Count actual Fill records for this position ─────────────────────
            from sqlalchemy import func, select
            fill_count_row = await db.execute(
                select(func.count(Fill.id)).where(
                    Fill.condition_id == pos.condition_id,
                    Fill.asset == pos.asset,
                )
            )
            actual_fills = fill_count_row.scalar() or 1

            # ── 写入 ExitRecord（卖出记录）──────────────────────────────────
            exit_record = ExitRecord(
                asset=pos.asset,
                condition_id=pos.condition_id,
                side=pos.side,
                exit_price=exit_price,
                tokens_exited=filled_tokens,
                partial_pct=partial_pct,
                pnl_pct=pnl_pct,
                pnl_usd=pnl_usd,
                gross_pnl_usd=gross_pnl_usd,
                fee_usdc=exit_fee_usdc,
                entry_record_id=pos.entry_record_id or None,
                close_reason=close_reason,
                mode=mode,
                closed_at=now,
            )
            db.add(exit_record)
            await db.flush()

            # ── 写入/更新 Trade（全生命周期记录，保持兼容）──────────────
            trade = Trade(
                asset=pos.asset,
                condition_id=pos.condition_id,
                side=pos.side,
                entry_price=pos.entry_price,
                effective_entry_price=pos.effective_entry_price,
                tokens_bought=filled_tokens,
                exit_price=exit_price,
                pnl_pct=pnl_pct,
                pnl_usd=pnl_usd,
                gross_pnl_usd=gross_pnl_usd,
                fee_usdc=total_fee,
                close_reason=close_reason,
                bet_usdc=pos.actual_cost_usdc,
                mode=mode,
                fills=actual_fills,
                strategy="main",
                entry_core_agree=int(getattr(pos, "entry_core_agree", 0) or 0),
                opened_at=pos.opened_at,
                closed_at=now,
            )
            db.add(trade)
            await db.flush()

            # Link any orphan Fill records for this position (trade_id=NULL).
            # Filter by both asset AND condition_id to avoid linking fills from different windows.
            await db.execute(
                update(Fill)
                .where(
                    Fill.asset == pos.asset,
                    Fill.condition_id == pos.condition_id,
                    Fill.trade_id.is_(None),
                )
                .values(trade_id=trade.id)
            )

            await db.commit()

        pos._trade_recorded = True
        pos._trade_recorded_reason = close_reason
