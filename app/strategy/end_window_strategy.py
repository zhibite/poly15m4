"""
end_window_strategy.py — 末端顺势策略编排层

与 SinglePositionStrategy 并行运行，在 T=12min~T=14.5min 窗口末端独立决策。
完全自包含：持有自己的持仓状态，不污染 ctx.position，通过 pos_mgr 直接执行平仓。

执行顺序（每个 tick）：
  1. 退出检查（优先级最高：E1 > E2 > E3 > E4）
  2. 入场检查（无持仓时：H1 > H2 > H3 > 置信度）
  3. 窗口切换时重置状态
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from loguru import logger

from app.models import Side
from app.strategy.end_window_signal import EndWindowConfidence, compute_confidence, determine_direction
from app.strategy.end_window_controller import EndWindowController, EndWindowExitSignal
from app.strategy.window_context import WindowContext
from app.pm_client import PolymarketClient


@dataclass
class EndPosition:
    """
    末端策略持仓结构。

    字段完全兼容 OpenPosition 的关键子集，可直接传入 PositionManager.close_position()。
    不需要 ctx.position 的所有字段（如 tp_hit、_trade_recorded_reason 等持仓管理字段），
    只需要 close_position() 真正需要的字段。
    """
    asset: str
    direction: str                    # "UP" / "DOWN"
    token_id: str                    # 买入的 token_id
    tokens_bought: float
    entry_price: float               # 成交价
    effective_entry_price: float     # 含手续费的加权均价
    cost_usdc: float                # 实际花费的 USDC
    stake_usdc: float               # 原始投入
    condition_id: str = ""
    opened_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    window_start: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    window_end: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    side: Side = Side.UP
    token_id_bought: str = ""       # 同 token_id（兼容性）
    cost_basis: float = 0.0
    actual_cost_usdc: float = 0.0
    taker_fee_bps: int = 30
    min_exit_price: float = 0.01
    sell_retry_count: int = 0
    _pending_close: bool = False
    _exit_retry_done: bool = False
    _trade_recorded: bool = False
    _trade_recorded_reason: str = ""
    _last_pnl_pct: float = 0.0
    _last_pnl_usd: float = 0.0
    _is_orphan_position: bool = False
    entry_record_id: int | None = None
    _pending_partial_pct: float = 1.0
    tp_pct: float = 0.0
    sl_pct: float = 0.0
    entry_core_agree: int = 0
    bet_usdc: float = 0.0
    anchor_price: float = 0.0
    tp_hit: Optional[dict] = None
    entry_anchor_deviation_pct: float = 0.0
    market = None  # MarketInfo reference


class EndWindowStrategy:
    """
    末端顺势策略：与主策略并行，独立持仓，互不叠加。

    完全自包含设计：
    - 持有自己的 EndPosition，不碰 ctx.position
    - 持有自己的 PositionManager，通过 close_position() 直接执行平仓
    - 不污染主策略的任何状态

    对外 API：
      tick()       — 每个 tick 调用一次（由 SinglePositionStrategy.run_forever 编排）
      sync_params() — 同步运行时配置
      reset()       — 窗口切换时调用
    """

    def __init__(self, pos_mgr=None, pm=None):
        self.signal = EndWindowController()
        self._pos_mgr = pos_mgr                # PositionManager instance
        self._pm = pm                          # PolymarketClient (for orderbook)
        self._positions: dict[str, EndPosition] = {}
        self._entry_confidences: dict[str, EndWindowConfidence] = {}
        self._live_trader = None             # 每次 tick 由 run_forever 传入

    # ── 参数同步 ─────────────────────────────────────────────────────────────

    def sync_params(self, cfg: dict) -> None:
        self.signal.sync_params(cfg)

    # ── 主 tick ─────────────────────────────────────────────────────────────

    async def tick(
        self,
        asset: str,
        ctx: WindowContext,
        indicator_signal_result,       # dict from get_indicators()
        main_has_position: bool,
        main_direction: str,
        live_trader=None,
        *,
        market_duration_sec: float = 900.0,
    ) -> tuple[bool, str]:
        """
        返回 (是否有新操作, 操作描述)

        编排顺序：
          1. 退出检查（已持仓）
          2. 入场检查（无持仓）
        """
        mkt = ctx.market
        if not mkt:
            return False, ""

        self._live_trader = live_trader
        elapsed = ctx.elapsed()
        up_mid = mkt._up_mid
        down_mid = mkt._down_mid

        # 拉取盘口数据（异步，非阻塞）
        # direction 未确定时先拉 UP 的盘口，direction 确定后再拉对应盘口
        direction_raw = determine_direction(up_mid)
        if direction_raw != "NONE":
            token_id = mkt.token_up_id if direction_raw == "UP" else mkt.token_down_id
            book = await self._pm.get_orderbook(token_id) if self._pm else None
        else:
            book = await self._pm.get_orderbook(mkt.token_up_id) if self._pm else None

        # ── 1. 退出检查 ────────────────────────────────────────────────
        if asset in self._positions:
            pos = self._positions[asset]
            exit_sig = self.signal.check_exit(
                elapsed=elapsed,
                window_duration=market_duration_sec,
                direction=pos.direction,
                entry_price=pos.entry_price,
                current_price=up_mid if pos.direction == "UP" else down_mid,
                pnl_pct=self._calc_pnl_pct(pos, up_mid, down_mid),
            )
            if exit_sig.should_exit:
                return self._handle_exit(asset, ctx, exit_sig)
            return False, ""

        # ── 2. 入场检查 ────────────────────────────────────────────────
        return await self._check_entry(
            asset, ctx, indicator_signal_result,
            elapsed, up_mid, down_mid,
            main_has_position, main_direction,
            market_duration_sec=market_duration_sec,
            orderbook=book,
        )

    # ── 入场 ───────────────────────────────────────────────────────────────

    async def _check_entry(
        self,
        asset: str,
        ctx: WindowContext,
        indicator_signal_result,
        elapsed: float,
        up_mid: float,
        down_mid: float,
        main_has_position: bool,
        main_direction: str,
        *,
        market_duration_sec: float = 900.0,
        orderbook: dict | None = None,
    ) -> tuple[bool, str]:
        mkt = ctx.market
        direction = determine_direction(up_mid)
        if direction == "NONE":
            return False, "no_direction"

        # H1: 时机窗口
        ok, reason = self.signal.check_hard_timing(elapsed)
        if not ok:
            return False, f"hard_fail:{reason}"

        # H2: 最低入场价格
        ok, reason = self.signal.check_min_entry_price(up_mid, direction)
        if not ok:
            return False, f"price_reject:{reason}"

        # H3: 主策略互斥
        ok, reason = self.signal.check_main_strategy_conflict(
            main_has_position, main_direction, direction,
        )
        if not ok:
            return False, f"conflict:{reason}"

        # 计算五维置信度（含 D5 流动性）
        conf = compute_confidence(
            indicator_signal_result=indicator_signal_result,
            window_open_up=mkt.window_open_up,
            current_up=up_mid,
            up_samples=getattr(ctx, "up_samples", []),
            direction=direction,
            orderbook=orderbook,
        )

        # 置信度阈值 + 仓位决策
        ok, reason, size = self.signal.check_entry(
            elapsed=elapsed,
            current_mid=up_mid,
            direction=direction,
            confidence_total=conf.total,
            main_has_position=main_has_position,
            main_direction=main_direction,
            asset=asset,
        )
        if not ok:
            return False, reason

        # 执行入场
        entry_price = up_mid if direction == "UP" else down_mid
        ep = await self._do_entry(asset, ctx, direction, entry_price, size, orderbook=orderbook)
        if ep is None:
            return False, "entry_failed"

        self._positions[asset] = ep
        self._entry_confidences[asset] = conf
        self.signal.set_position(asset, True)

        mode = "LIVE" if self._live_trader else "DRY"
        logger.info(
            "END_ENTER [{}][{}] {} @ {:.4f} size={:.2f}U conf={:.1f} {}",
            mode, asset, direction, entry_price, size, conf.total,
            conf.log_summary(),
        )
        return True, f"entered:{direction}"

    async def _do_entry(
        self,
        asset: str,
        ctx: WindowContext,
        direction: str,
        price: float,
        stake: float,
        *,
        orderbook: dict | None = None,
    ) -> Optional[EndPosition]:
        """执行入场（live 或 dry）。返回 EndPosition，失败返回 None。"""
        mkt = ctx.market
        side = Side.UP if direction == "UP" else Side.DOWN
        token_id = mkt.token_up_id if side == Side.UP else mkt.token_down_id

        # Dry-run 路径
        if self._live_trader is None:
            tokens = stake / price if price > 1e-9 else 0.0
            return EndPosition(
                asset=asset, direction=direction,
                token_id=token_id, token_id_bought=token_id,
                tokens_bought=tokens, entry_price=price,
                effective_entry_price=price, cost_usdc=stake,
                stake_usdc=stake, cost_basis=stake,
                actual_cost_usdc=stake, side=side,
                bet_usdc=stake, anchor_price=price,
                condition_id=mkt.condition_id,
                opened_at=datetime.now(timezone.utc),
                window_start=mkt.window_start,
                window_end=mkt.window_end,
                market=mkt,
            )

        # Live 路径：先用盘口估算滑点，超限则跳过
        from app.pm_client import estimate_buy_slippage
        slip = 2.0  # 默认 2%
        if orderbook and self._pm:
            slip_est = await estimate_buy_slippage(token_id, stake, pm_client=self._pm)
            if slip_est.error:
                logger.debug("END_ENTRY slippage est error: {}", slip_est.error)
            else:
                slip = max(slip_est.slippage_pct, 0.5)  # 至少 0.5%
                if not slip_est.sufficient:
                    logger.warning(
                        "END_ENTRY [{}] {} shallow depth: tokens={:.1f} stake={:.1f}U — proceed with slip={:.1f}%",
                        asset, direction, slip_est.depth_at_fill, stake, slip,
                    )
                else:
                    logger.debug(
                        "END_ENTRY [{}] {} slip_est={:.1f}% depth={:.1f}x",
                        asset, direction, slip, slip_est.pct_of_depth,
                    )

        try:
            result = await self._live_trader.buy_token(
                token_id=token_id,
                stake=stake,
                price=price,
                neg_risk=mkt.neg_risk,
                slippage_pct=slip,
            )
            if result and getattr(result, "success", False):
                tokens = float(getattr(result, "tokens", stake / price))
                cost = float(getattr(result, "usdc", stake))
                fill_price = float(getattr(result, "fill_price", price))
                ep = EndPosition(
                    asset=asset, direction=direction,
                    token_id=token_id, token_id_bought=token_id,
                    tokens_bought=tokens, entry_price=fill_price,
                    effective_entry_price=fill_price, cost_usdc=cost,
                    stake_usdc=stake, cost_basis=cost,
                    actual_cost_usdc=cost, side=side,
                    bet_usdc=stake, anchor_price=fill_price,
                    condition_id=mkt.condition_id,
                    opened_at=datetime.now(timezone.utc),
                    window_start=mkt.window_start,
                    window_end=mkt.window_end,
                    market=mkt,
                )
                return ep
        except Exception as exc:
            logger.warning("END_ENTRY {} {} failed: {}", asset, direction, exc)
        return None

    # ── 退出 ───────────────────────────────────────────────────────────────

    async def _handle_exit(
        self,
        asset: str,
        ctx: WindowContext,
        exit_sig: EndWindowExitSignal,
    ) -> tuple[bool, str]:
        """
        处理退出：直接从 _positions 中弹出持仓，通过 pos_mgr 执行平仓。

        不操作 ctx.position，主策略的状态完全不受影响。
        """
        pos = self._positions.pop(asset, None)
        self._entry_confidences.pop(asset, None)
        self.signal.set_position(asset, False)

        if pos is None:
            return True, f"exited:{exit_sig.reason}"

        pnl_pct = exit_sig.pnl_pct

        # 记录每日盈亏（所有退出类型都记录）
        self.signal.record_pnl(asset, pnl_pct)

        mode = "LIVE" if self._live_trader else "DRY"
        reason_tag = f"END_{exit_sig.reason}"

        # Dry-run：只记录日志
        if self._live_trader is None or self._pos_mgr is None:
            logger.info(
                "END_EXIT [{}][{}] {} pnl%={:+.1f}% reason={}",
                mode, asset, pos.direction, pnl_pct, exit_sig.reason,
            )
            return True, f"exited:{exit_sig.reason}"

        # Live 路径：通过 pos_mgr 执行实际平仓
        pos._pending_close = True
        asyncio.create_task(
            self._do_close(asset, pos, reason_tag, ctx),
            name=f"end-close-{asset}",
        )
        logger.info(
            "END_EXIT [{}][{}] {} pnl%={:+.1f}% reason={}",
            mode, asset, pos.direction, pnl_pct, exit_sig.reason,
        )
        return True, f"exited:{exit_sig.reason}"

    async def _do_close(
        self,
        asset: str,
        pos: EndPosition,
        reason: str,
        ctx: WindowContext,
    ) -> None:
        """
        异步执行末端策略持仓的平仓。

        使用 pos_mgr.close_position() 执行，与主策略持仓平仓共用同一套机制
        （CLOB 卖出 → 链上 redeem 兜底 → DB 记录）。
        """
        try:
            if self._pos_mgr is not None:
                await self._pos_mgr.close_position(
                    pos=pos,
                    close_reason=reason,
                    live_trader=self._live_trader,
                    resolved_market=pos.market,
                )
        except Exception as exc:
            logger.warning("END_CLOSE [{}] failed: {}", asset, exc)
        finally:
            pos._pending_close = False
            pos.sell_retry_count = 0

    # ── PnL 计算 ─────────────────────────────────────────────────────────

    def _calc_pnl_pct(self, pos: EndPosition, up_mid: float, down_mid: float) -> float:
        """计算浮盈百分比（相对于 entry_price）"""
        current_price = up_mid if pos.direction == "UP" else down_mid
        if pos.effective_entry_price < 1e-9:
            return 0.0
        return (current_price - pos.effective_entry_price) / pos.effective_entry_price * 100.0

    # ── 窗口切换时重置 ───────────────────────────────────────────────────

    def reset(self, asset: str) -> None:
        """窗口切换时调用：清空该资产的末端持仓和置信度"""
        popped = self._positions.pop(asset, None)
        self._entry_confidences.pop(asset, None)
        self.signal.reset(asset)
        if popped:
            logger.info("END_RESET [{}] — cleared position (direction={})", asset, popped.direction)

    # ── 查询 ───────────────────────────────────────────────────────────────

    def has_position(self, asset: str) -> bool:
        return asset in self._positions

    def get_position(self, asset: str) -> Optional[EndPosition]:
        return self._positions.get(asset)

    def get_confidence(self, asset: str) -> Optional[EndWindowConfidence]:
        return self._entry_confidences.get(asset)
