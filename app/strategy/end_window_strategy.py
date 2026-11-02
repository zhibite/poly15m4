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
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from loguru import logger

from app.models import Side
from app.strategy.end_window_signal import EndWindowConfidence, compute_confidence, determine_direction
from app.strategy.end_window_controller import EndWindowController, EndWindowExitSignal
from app.strategy.models import MarketInfo
from app.strategy.position_manager import AlreadyClosedError, LiveTraderUnavailableError
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
    # 入场时的执行模式（live / dry）与平仓重试状态
    execution_mode: str = "dry"
    _close_attempts: int = 0
    _next_close_retry_at: float = 0.0
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
    # 注意：必须有类型注解，否则 dataclass 不会把它当作字段，
    # 构造时传入 market=... 会抛 TypeError。
    market: Optional[MarketInfo] = None


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
        # 同一窗口只允许入场一次（平仓后也不再入场），值为 condition_id
        self._traded_conditions: dict[str, str] = {}
        # 实际 LiveTrader 引用（由 SinglePositionStrategy.set_live_trader 同步）。
        # tick() 的 live_trader 参数只表示“本次是否允许实盘入场”，不要覆盖它。
        self._live_trader = None
        # 跟踪异步平仓任务，shutdown 时先等待其完成再关闭客户端。
        self._close_tasks: set[asyncio.Task] = set()

    def _track_close_task(self, task: asyncio.Task) -> None:
        self._close_tasks.add(task)
        task.add_done_callback(self._close_tasks.discard)

    async def close(self, timeout: float = 10.0) -> None:
        """Wait for in-flight end-window close tasks before shutdown."""
        tasks = [t for t in self._close_tasks if not t.done()]
        if not tasks:
            return
        logger.info("Waiting for {} in-flight end-window close task(s)...", len(tasks))
        _done, pending = await asyncio.wait(tasks, timeout=timeout)
        if pending:
            logger.warning(
                "{} end-window close task(s) did not finish in {:.0f}s — cancelling",
                len(pending), timeout,
            )
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

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
        allow_entry: bool = True,
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

        # NOTE: self._live_trader is the *actual* trader reference (kept in sync by
        # SinglePositionStrategy.set_live_trader).  The `live_trader` parameter is
        # the entry permission (None when live_mode=False), so it must not
        # overwrite the exit-side reference.
        elapsed = ctx.elapsed()
        up_mid = mkt._up_mid
        down_mid = mkt._down_mid

        # 盘口数据只在真正需要时拉取（见 _check_entry 的硬条件检查之后），
        # 避免整个窗口期间每个 tick 都发起一次 CLOB /book 请求。

        # ── 1. 退出检查 ────────────────────────────────────────────────
        if asset in self._positions:
            pos = self._positions[asset]
            if getattr(pos, "_pending_close", False):
                return False, ""
            if time.time() < getattr(pos, "_next_close_retry_at", 0.0):
                return False, ""
            exit_sig = self.signal.check_exit(
                elapsed=elapsed,
                window_duration=market_duration_sec,
                direction=pos.direction,
                entry_price=pos.entry_price,
                current_price=up_mid if pos.direction == "UP" else down_mid,
                pnl_pct=self._calc_pnl_pct(pos, up_mid, down_mid),
            )
            if exit_sig.should_exit:
                return await self._handle_exit(asset, ctx, exit_sig)
            return False, ""

        if not allow_entry:
            return False, "entry_not_allowed"

        # ── 2. 入场检查 ────────────────────────────────────────────────
        return await self._check_entry(
            asset, ctx, indicator_signal_result,
            elapsed, up_mid, down_mid,
            main_has_position, main_direction,
            market_duration_sec=market_duration_sec,
            entry_live_trader=live_trader,
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
        entry_live_trader=None,
    ) -> tuple[bool, str]:
        mkt = ctx.market
        if mkt is None:
            return False, "no_market"
        direction = determine_direction(up_mid)
        if direction == "NONE":
            return False, "no_direction"

        # 同一窗口只允许一次建仓（即使已经止盈/止损离场）
        if self._traded_conditions.get(asset) == mkt.condition_id:
            return False, "already_traded_window"

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

        # 通过硬条件后再拉盘口（D5 流动性 + 滑点估算），减少无谓请求
        orderbook: dict | None = None
        if self._pm is not None:
            token_id = mkt.token_up_id if direction == "UP" else mkt.token_down_id
            orderbook = await self._pm.get_orderbook(token_id)

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
        if isinstance(indicator_signal_result, dict):
            core_agree = int(indicator_signal_result.get("core_agree", 0))
        else:
            core_agree = int(getattr(indicator_signal_result, "core_agree", 0))
        ep = await self._do_entry(
            asset, ctx, direction, entry_price, size,
            orderbook=orderbook, core_agree=core_agree,
            live_trader=entry_live_trader,
        )
        if ep is None:
            return False, "entry_failed"

        self._positions[asset] = ep
        self._entry_confidences[asset] = conf
        self._traded_conditions[asset] = mkt.condition_id
        self.signal.set_position(asset, True)

        mode = "LIVE" if entry_live_trader else "DRY"
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
        core_agree: int = 0,
        live_trader=None,
    ) -> Optional[EndPosition]:
        """执行入场（live 或 dry），并写入 EntryRecord / Fill。"""
        mkt = ctx.market
        if mkt is None:
            logger.warning("END_ENTRY [{}] skipped: no market info", asset)
            return None
        side = Side.UP if direction == "UP" else Side.DOWN
        token_id = mkt.token_up_id if side == Side.UP else mkt.token_down_id

        # 实盘模式下 LiveTrader 不可用时禁止静默降级为 dry 模拟
        from app.runtime_config import get_config
        if get_config().get("live_mode", False) and live_trader is None:
            logger.error(
                "END_ENTRY [{}] live_mode=True but LiveTrader is unavailable — skipping entry",
                asset,
            )
            return None

        fee_rate_bps = 0
        if self._pm is not None:
            try:
                fee_rate_bps = await self._pm.get_fee_rate_bps(token_id)
            except Exception:
                logger.debug("END_ENTRY fee_rate_bps unavailable for {} — using 0 bps", asset)

        # Dry-run 路径
        if live_trader is None:
            tokens = stake / price if price > 1e-9 else 0.0
            ep = EndPosition(
                asset=asset, direction=direction,
                token_id=token_id, token_id_bought=token_id,
                tokens_bought=tokens, entry_price=price,
                effective_entry_price=price, cost_usdc=stake,
                stake_usdc=stake, cost_basis=stake,
                actual_cost_usdc=stake, side=side,
                bet_usdc=stake, anchor_price=price,
                execution_mode="dry",
                entry_core_agree=core_agree,
                condition_id=mkt.condition_id,
                opened_at=datetime.now(timezone.utc),
                window_start=mkt.window_start,
                window_end=mkt.window_end,
                market=mkt,
            )
            await self._save_entry_record(ep, mkt, fee_rate_bps, "dry", mid_price=price)
            return ep

        # Live 路径：先用盘口估算滑点
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
                        "END_ENTRY [{}] {} shallow depth: available={:.1f} tokens stake={:.1f}U coverage={:.1f}x — proceed with slip={:.1f}%",
                        asset, direction, slip_est.depth_at_fill, stake,
                        slip_est.pct_of_depth, slip,
                    )
                else:
                    logger.debug(
                        "END_ENTRY [{}] {} slip_est={:.1f}% depth={:.1f}x",
                        asset, direction, slip, slip_est.pct_of_depth,
                    )

        try:
            result = await live_trader.buy_token(
                token_id=token_id,
                size_usdc=stake,
                midpoint=price,
                fee_rate_bps=fee_rate_bps,
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
                    execution_mode="live",
                    entry_core_agree=core_agree,
                    taker_fee_bps=fee_rate_bps,
                    condition_id=mkt.condition_id,
                    opened_at=datetime.now(timezone.utc),
                    window_start=mkt.window_start,
                    window_end=mkt.window_end,
                    market=mkt,
                )
                await self._save_entry_record(ep, mkt, fee_rate_bps, "live", mid_price=price)
                return ep
        except Exception as exc:
            logger.warning("END_ENTRY {} {} failed: {}", asset, direction, exc)
        return None

    async def _save_entry_record(
        self,
        pos: EndPosition,
        mkt: MarketInfo,
        fee_rate_bps: int,
        mode: str,
        *,
        mid_price: float,
    ) -> bool:
        """写入末端策略的 EntryRecord + Fill（与主策略数据口径一致）。"""
        from app.database import async_session
        from app.models import EntryRecord, Fill

        try:
            async with async_session() as db:
                fee_usdc = float(pos.cost_usdc) * fee_rate_bps / 10_000
                entry_record = EntryRecord(
                    asset=pos.asset,
                    condition_id=mkt.condition_id,
                    side=pos.side,
                    entry_price=float(pos.entry_price),
                    effective_entry_price=float(pos.effective_entry_price),
                    tokens_bought=float(pos.tokens_bought),
                    cost_usdc=float(pos.cost_usdc),
                    fee_usdc=fee_usdc,
                    mode=mode,
                    entry_core_agree=int(pos.entry_core_agree),
                )
                db.add(entry_record)
                await db.flush()

                fill = Fill(
                    trade_id=None,
                    asset=pos.asset,
                    condition_id=mkt.condition_id,
                    side=pos.side,
                    fill_price=float(pos.entry_price),
                    tokens=float(pos.tokens_bought),
                    cost_usdc=float(pos.cost_usdc),
                    fee_usdc=fee_usdc,
                    fill_index=0,
                    vwap_after=float(pos.effective_entry_price),
                    mid_price_at_fill=float(mid_price),
                    mode=mode,
                )
                db.add(fill)
                await db.flush()
                await db.commit()

                pos.entry_record_id = entry_record.id
                logger.info(
                    "END_SAVED entry_record id={} fill id={} for {} ({})",
                    entry_record.id, fill.id, pos.asset, mode,
                )
                return True
        except Exception as exc:
            pos._is_orphan_position = True
            logger.error(
                "END_ENTRY failed to save entry/fill record for {}: {}",
                pos.asset, exc,
            )
            return False

    # ── 退出 ───────────────────────────────────────────────────────────────

    async def _handle_exit(
        self,
        asset: str,
        ctx: WindowContext,
        exit_sig: EndWindowExitSignal,
    ) -> tuple[bool, str]:
        """
        处理退出：保留持仓在 _positions 中直到平仓成功，避免平仓失败后
        持仓从内存消失（真实 token 仍留在钱包里）。

        不操作 ctx.position，主策略的状态完全不受影响。
        """
        pos = self._positions.get(asset)
        if pos is None:
            return False, ""
        if getattr(pos, "_pending_close", False):
            return False, ""

        pnl_pct = exit_sig.pnl_pct
        # 每日亏损控制以 USDC 计，而不是百分比
        pnl_usd = float(pos.actual_cost_usdc) * pnl_pct / 100.0
        pos._last_pnl_pct = pnl_pct
        pos._last_pnl_usd = pnl_usd

        # live 持仓必须使用真实 LiveTrader；不可用时保留持仓并稍后重试。
        close_trader = None
        if getattr(pos, "execution_mode", "dry") == "live":
            close_trader = self._live_trader
            if close_trader is None:
                logger.error(
                    "END_EXIT [{}] live position but LiveTrader unavailable — "
                    "keeping position and retrying (condition={})",
                    asset, pos.condition_id[:12],
                )
                pos._next_close_retry_at = time.time() + 30.0
                return False, "live_trader_unavailable"

        if self._pos_mgr is None:
            logger.error("END_EXIT [{}] pos_mgr unavailable — keeping position", asset)
            return False, "pos_mgr_unavailable"

        reason_tag = f"END_{exit_sig.reason}"
        pos._pending_close = True
        try:
            task = asyncio.create_task(
                self._do_close(
                    asset, pos, reason_tag, pnl_usd, close_trader,
                    reinsert_on_failure=True,
                ),
                name=f"end-close-{asset}",
            )
            self._track_close_task(task)
        except RuntimeError as exc:
            pos._pending_close = False
            logger.error("END_EXIT [{}] cannot schedule close: {}", asset, exc)
            return False, "cannot_schedule_close"
        mode = "LIVE" if close_trader else "DRY"
        logger.info(
            "END_EXIT [{}][{}] {} pnl%={:+.1f}% pnl_usd={:+.2f} reason={}",
            mode, asset, pos.direction, pnl_pct, pnl_usd, exit_sig.reason,
        )
        return True, f"exited:{exit_sig.reason}"

    async def _do_close(
        self,
        asset: str,
        pos: EndPosition,
        reason: str,
        pnl_usd: float = 0.0,
        live_trader=None,
        *,
        reinsert_on_failure: bool = True,
    ) -> bool:
        """
        异步执行末端策略持仓的平仓。

        使用 pos_mgr.close_position() 执行，与主策略持仓平仓共用同一套机制
        （CLOB 卖出 → 链上 redeem 兜底 → DB 记录）。

        只有成功（或链上已无余额）才移除持仓并记录每日盈亏；失败时保留
        持仓并设置冷却时间，由后续 tick 再次尝试退出。
        """
        success = False
        try:
            if self._pos_mgr is None:
                raise RuntimeError("pos_mgr unavailable")
            await self._pos_mgr.close_position(
                pos=pos,
                close_reason=reason,
                live_trader=live_trader,
                resolved_market=pos.market,
            )
            success = True
        except AlreadyClosedError as exc:
            # 链上已无余额，视为平仓完成。
            success = True
            if exc.pnl_usd is not None:
                pnl_usd = float(exc.pnl_usd)
        except LiveTraderUnavailableError as exc:
            logger.error(
                "END_CLOSE [{}] {} — keeping position for retry", asset, exc,
            )
        except Exception as exc:
            logger.warning(
                "END_CLOSE [{}] failed: {} — keeping position for retry", asset, exc,
            )
        finally:
            pos._pending_close = False

        if success:
            if self._positions.get(asset) is pos:
                self._positions.pop(asset, None)
            self._entry_confidences.pop(asset, None)
            self.signal.set_position(asset, False)
            # Prefer the actual realized PnL written by PositionManager; the
            # exit-signal estimate is only a fallback (e.g. already-closed).
            actual_pnl = float(getattr(pos, "_last_pnl_usd", pnl_usd) or pnl_usd)
            self.signal.record_pnl(asset, actual_pnl)
            pos.sell_retry_count = 0
            pos._close_attempts = 0
            pos._next_close_retry_at = 0.0
        else:
            pos._close_attempts = getattr(pos, "_close_attempts", 0) + 1
            pos._next_close_retry_at = time.time() + min(
                60.0, 5.0 * pos._close_attempts,
            )
            if reinsert_on_failure and asset not in self._positions:
                self._positions[asset] = pos
                self.signal.set_position(asset, True)
            elif not reinsert_on_failure:
                logger.error(
                    "END_RESET [{}] close failed after window switch — "
                    "manual redeem may be required (condition_id={}, tokens={:.4f})",
                    asset, pos.condition_id[:12], pos.tokens_bought,
                )
        return success

    # ── PnL 计算 ─────────────────────────────────────────────────────────

    def _calc_pnl_pct(self, pos: EndPosition, up_mid: float, down_mid: float) -> float:
        """计算浮盈百分比（相对于 entry_price）"""
        current_price = up_mid if pos.direction == "UP" else down_mid
        if pos.effective_entry_price < 1e-9:
            return 0.0
        return (current_price - pos.effective_entry_price) / pos.effective_entry_price * 100.0

    # ── 窗口切换时重置 ───────────────────────────────────────────────────

    def reset(self, asset: str) -> None:
        """
        窗口切换时调用。

        若有未平仓的末端持仓，不能直接丢弃：窗口切换后 token 仍在钱包里，
        必须异步执行平仓/赎回。这里通过 pos_mgr 调度一次 best-effort 平仓。
        """
        popped = self._positions.pop(asset, None)
        self._entry_confidences.pop(asset, None)
        self._traded_conditions.pop(asset, None)
        self.signal.reset(asset)
        if not popped:
            return

        logger.warning(
            "END_RESET [{}] — window switched with open position (direction={}, tokens={:.4f}); closing it",
            asset, popped.direction, popped.tokens_bought,
        )
        if self._pos_mgr is None:
            logger.error(
                "END_RESET [{}] — pos_mgr unavailable, position left open "
                "(condition_id={})", asset, popped.condition_id[:12],
            )
            return

        # live 持仓必须使用真实 LiveTrader；不可用时只能告警（token 仍在钱包）。
        close_trader = None
        if getattr(popped, "execution_mode", "dry") == "live":
            close_trader = self._live_trader
            if close_trader is None:
                logger.error(
                    "END_RESET [{}] live position but LiveTrader unavailable — "
                    "manual redeem may be required (condition_id={}, tokens={:.4f})",
                    asset, popped.condition_id[:12], popped.tokens_bought,
                )
                return

        up_mid = popped.market._up_mid if popped.market else 0.5
        down_mid = popped.market._down_mid if popped.market else 0.5
        pnl_pct = self._calc_pnl_pct(popped, up_mid, down_mid)
        pnl_usd = float(popped.actual_cost_usdc) * pnl_pct / 100.0

        popped._pending_close = True
        try:
            task = asyncio.create_task(
                self._do_close(
                    asset, popped, "END_WINDOW_SWITCH", pnl_usd, close_trader,
                    reinsert_on_failure=False,
                ),
                name=f"end-close-{asset}-reset",
            )
            self._track_close_task(task)
        except RuntimeError as exc:
            popped._pending_close = False
            logger.error("END_RESET [{}] — cannot schedule close (no running loop): {}", asset, exc)

    # ── 查询 ───────────────────────────────────────────────────────────────

    def has_position(self, asset: str) -> bool:
        return asset in self._positions

    def get_position(self, asset: str) -> Optional[EndPosition]:
        return self._positions.get(asset)

    def get_confidence(self, asset: str) -> Optional[EndWindowConfidence]:
        return self._entry_confidences.get(asset)
