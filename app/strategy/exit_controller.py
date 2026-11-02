"""
exit_controller.py — 退出控制器（简版：TP + SL 直接触发，无三重门）

止盈：PnL >= tp_pct（基于入场价的固定 %）
止损：PnL <= sl_pct（固定 -10%）
窗口到期：持有到结算
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from loguru import logger

from app.models import Side
from app.strategy.models import OpenPosition
from app.strategy.position_manager import AlreadyClosedError, CLOBExitError, PositionManager
from app.strategy.position_state import ExitReason
from app.strategy.window_context import WindowContext


@dataclass
class ExitSignal:
    """退出信号"""

    should_exit: bool = False
    reason: str = ""
    tp_hit_data: Optional[dict] = None
    exit_price_override: Optional[float] = None
    closed_at_override: Optional[datetime] = None
    partial_close_pct: float = 0.0


class ExitController:
    """简版退出控制器。"""

    def __init__(
        self,
        pos_mgr: PositionManager,
        wm=None,
        decision_logger=None,
        risk_manager=None,
    ):
        self._pos_mgr = pos_mgr
        self._decision_logger = decision_logger
        self._risk_manager = risk_manager
        self._exit_retry_locks: dict[str, asyncio.Lock] = {}

        # 出场策略参数（可通过 /api/config 做 A/B）
        self._hold_to_settlement = False
        self._tp_enabled = True
        self._sl_enabled = True
        self._tp_sl_disable_after_elapsed_sec = 0.0
        self._cost_buffer_pct = 0.0
        self._trailing_stop_pct = 0.0
        self._trailing_activation_pct = 20.0

    def sync_params(self, cfg: dict) -> None:
        self._hold_to_settlement = bool(cfg.get("exit_hold_to_settlement", False))
        self._tp_enabled = bool(cfg.get("exit_tp_enabled", True))
        self._sl_enabled = bool(cfg.get("exit_sl_enabled", True))
        self._tp_sl_disable_after_elapsed_sec = float(
            cfg.get("exit_tp_sl_disable_after_elapsed_sec", 0.0)
        )
        self._cost_buffer_pct = float(cfg.get("exit_cost_buffer_pct", 0.0))
        self._trailing_stop_pct = float(cfg.get("exit_trailing_stop_pct", 0.0))
        self._trailing_activation_pct = float(
            cfg.get("exit_trailing_activation_pct", 20.0)
        )

    def check_exit(self, ctx: WindowContext) -> ExitSignal:
        """
        决策顺序：
        1. 无效持仓 → 退出
        2. PnL >= tp_pct → 止盈退出
        3. PnL <= sl_pct → 止损退出

        窗口到期结算不在这里处理：SinglePositionStrategy 会在 tick 开头
        把到期持仓移入 pending settlement 队列，等待 Gamma 结算结果后
        再通过 PositionManager 结算，避免在已停止撮合的市场里挂卖单。
        """
        pos = ctx.position
        if not pos:
            return ExitSignal()

        now = datetime.now(timezone.utc)
        mkt = ctx.market

        if getattr(pos, "_pending_close", False):
            return ExitSignal()

        # 无效持仓
        if pos.tokens_bought <= 1e-9:
            return ExitSignal(
                should_exit=True,
                reason=ExitReason.REASON_INVALID,
            )

        up_mid = mkt._up_mid if mkt else 0.5
        down_mid = mkt._down_mid if mkt else 0.5
        current_price = up_mid if pos.side == Side.UP else down_mid
        pnl_pct = self._pos_mgr.unrealized_pnl_pct(up_mid, down_mid, pos)
        elapsed_s = round((now - pos.window_start).total_seconds(), 1)

        logger.debug(
            "EXIT [{}] entry={:.4f} cur={:.4f} pnl={:+.1f}% | tp={:.1f}% sl={:.1f}%",
            pos.asset,
            pos.entry_price, current_price,
            pnl_pct if pnl_pct else 0.0,
            pos.tp_pct,
            pos.sl_pct,
        )

        if pnl_pct is None:
            return ExitSignal()

        # 更新峰值（移动止损使用）
        if pos.peak_price <= 0 or current_price > pos.peak_price:
            pos.peak_price = current_price
        if pnl_pct > pos.peak_pnl_pct:
            pos.peak_pnl_pct = pnl_pct

        # TP/SL 是否生效：hold_to_settlement 或超过指定 elapsed 后交给结算
        tp_sl_active = not self._hold_to_settlement
        if (
            self._tp_sl_disable_after_elapsed_sec > 0
            and elapsed_s >= self._tp_sl_disable_after_elapsed_sec
        ):
            tp_sl_active = False

        # 移动止损（优先级高于固定止盈止损）
        if (
            tp_sl_active
            and self._trailing_stop_pct > 0
            and pos.peak_pnl_pct >= self._trailing_activation_pct
            and pnl_pct <= pos.peak_pnl_pct - self._trailing_stop_pct
        ):
            pos.tp_hit = {
                "kind": "移动止损",
                "elapsed_sec": elapsed_s,
                "exit_price": current_price,
                "pnl_pct": round(pnl_pct, 2),
                "peak_pnl_pct": round(pos.peak_pnl_pct, 2),
            }
            logger.info(
                "EXIT [{}] TRAILING_STOP: pnl={:+.1f}% <= peak={:+.1f}% - {:.1f}%",
                pos.asset, pnl_pct, pos.peak_pnl_pct, self._trailing_stop_pct,
            )
            return ExitSignal(
                should_exit=True,
                reason=ExitReason.REASON_TRAILING,
                tp_hit_data=pos.tp_hit,
            )

        # 止盈（成本缓冲：TP 阈值上移，避免把手续费/滑点算成利润）
        effective_tp = pos.tp_pct + self._cost_buffer_pct
        if tp_sl_active and self._tp_enabled and pnl_pct >= effective_tp:
            pos.tp_hit = {
                "kind": ExitReason.TAKE_PROFIT,
                "elapsed_sec": elapsed_s,
                "exit_price": current_price,
                "pnl_pct": round(pnl_pct, 2),
                "effective_tp_pct": round(effective_tp, 2),
            }
            logger.info(
                "EXIT [{}] TAKE_PROFIT: pnl={:+.1f}% >= tp={:.1f}% (base={:.1f}% + cost={:.1f}%)",
                pos.asset, pnl_pct, effective_tp, pos.tp_pct, self._cost_buffer_pct,
            )
            return ExitSignal(
                should_exit=True,
                reason=ExitReason.REASON_TP,
                tp_hit_data=pos.tp_hit,
            )

        # 止损（成本缓冲：SL 阈值下移）
        effective_sl = pos.sl_pct - self._cost_buffer_pct
        if tp_sl_active and self._sl_enabled and pnl_pct <= effective_sl:
            pos.tp_hit = {
                "kind": ExitReason.STOP_LOSS,
                "elapsed_sec": elapsed_s,
                "exit_price": current_price,
                "pnl_pct": round(pnl_pct, 2),
                "effective_sl_pct": round(effective_sl, 2),
            }
            logger.info(
                "EXIT [{}] STOP_LOSS: pnl={:+.1f}% <= sl={:.1f}% (base={:.1f}% - cost={:.1f}%)",
                pos.asset, pnl_pct, effective_sl, pos.sl_pct, self._cost_buffer_pct,
            )
            return ExitSignal(
                should_exit=True,
                reason=ExitReason.REASON_SL,
                tp_hit_data=pos.tp_hit,
            )

        return ExitSignal()

    def get_unrealized_pnl_pct(self, ctx: WindowContext) -> float | None:
        pos = ctx.position
        if not pos:
            return 0.0
        mkt = ctx.market
        up_mid = mkt._up_mid if mkt else 0.5
        down_mid = mkt._down_mid if mkt else 0.5
        return self._pos_mgr.unrealized_pnl_pct(up_mid, down_mid, pos)

    async def handle_exit(
        self,
        asset: str,
        ctx: WindowContext,
        exit_signal: ExitSignal,
        live_trader,
    ) -> None:
        pos = ctx.position
        if not pos or getattr(pos, "_pending_close", False):
            return

        reason = exit_signal.reason

        if reason == ExitReason.REASON_TP:
            await self.spawn_close(
                asset, pos, ExitReason.TAKE_PROFIT,
                live_trader=live_trader,
                ctx=ctx,
                partial_close_pct=exit_signal.partial_close_pct,
            )

        elif reason == ExitReason.REASON_SL:
            await self.spawn_close(
                asset, pos, ExitReason.STOP_LOSS,
                live_trader=live_trader,
                ctx=ctx,
            )

        elif reason == ExitReason.REASON_TRAILING:
            await self.spawn_close(
                asset, pos, ExitReason.TRAILING_STOP,
                live_trader=live_trader,
                ctx=ctx,
            )

        elif reason == ExitReason.REASON_INVALID:
            await self.spawn_close(
                asset, pos, ExitReason.INVALID_POSITION,
                live_trader=live_trader,
                ctx=ctx,
            )

    async def spawn_close(
        self,
        asset: str,
        pos: OpenPosition,
        reason: str,
        live_trader,
        ctx: WindowContext,
        settle_up_wins: Optional[bool] = None,
        partial_close_pct: Optional[float] = None,
    ) -> None:
        pos._pending_close = True
        ctx._pending_close = True
        asyncio.create_task(
            self._do_close(
                asset, pos, reason, live_trader, ctx,
                settle_up_wins=settle_up_wins,
                partial_close_pct=partial_close_pct,
            ),
            name=f"close-{asset}",
        )

    def _record_risk(self, pos: OpenPosition) -> None:
        """把已实现盈亏上报给主策略风控（每个持仓只上报一次）。"""
        if self._risk_manager is None or getattr(pos, "_risk_recorded", False):
            return
        pos._risk_recorded = True
        try:
            self._risk_manager.record_exit(
                float(getattr(pos, "_last_pnl_usd", 0.0) or 0.0),
                float(getattr(pos, "_last_pnl_pct", 0.0) or 0.0),
            )
        except Exception as exc:
            # 风控统计失败不能影响已经完成的平仓流程
            logger.warning("RISK record_exit failed for {}: {}", pos.asset, exc)

    async def _do_close(
        self,
        asset: str,
        pos: OpenPosition,
        reason: str,
        live_trader,
        ctx: WindowContext,
        settle_up_wins: Optional[bool] = None,
        partial_close_pct: Optional[float] = None,
    ) -> None:

        def _cleanup():
            # 只有当 ctx 仍指向本持仓时才清理 ctx 状态：
            # 平仓任务可能跨越窗口切换，此时的 ctx 已属于新窗口，
            # 不能误清掉新窗口的持仓 / pending 标记。
            if ctx.position is pos:
                ctx.on_exit_complete()
            if ctx.condition_id == pos.condition_id:
                ctx._pending_close = False
            pos._pending_close = False
            pos.sell_retry_count = 0

        if ctx.condition_id != pos.condition_id:
            logger.warning(
                "EXIT {} aborted: window switched (pos={} ctx={})",
                asset, pos.condition_id[:12], ctx.condition_id[:12],
            )
            _cleanup()
            return

        resolved_mkt = ctx.market
        override_exit = None

        if reason == ExitReason.REASON_INVALID:
            override_exit = 0.0
        elif reason == ExitReason.REASON_SETTLE and settle_up_wins is not None:
            override_exit = 1.0 if settle_up_wins else 0.0

        tp_hit = getattr(pos, "tp_hit", None)
        try:
            await self._pos_mgr.close_position(
                pos,
                reason,
                override_exit_price=override_exit,
                live_trader=live_trader,
                resolved_market=resolved_mkt,
                settle_up_wins=settle_up_wins,
                partial_close_pct=partial_close_pct,
            )
            self._record_risk(pos)
            _cleanup()

            if tp_hit:
                pnl_recorded = getattr(pos, "_last_pnl_pct", None)
                if pnl_recorded is not None and pnl_recorded != 0.0:
                    logger.info(
                        "EXIT RESULT [{}] {} pnl={:+.2f}% exit={} trigger={:.2f}% elapsed={:.0f}s",
                        asset,
                        tp_hit.get("kind", reason),
                        pnl_recorded,
                        tp_hit.get("exit_price", 0),
                        tp_hit.get("pnl_pct", 0),
                        tp_hit.get("elapsed_sec", 0),
                    )
            return

        except AlreadyClosedError as exc:
            logger.info("EXIT [{}] already sold, pnl={:.2f}u", asset, exc.pnl_usd or 0)
            pos._last_pnl_pct = exc.pnl_pct
            pos._last_pnl_usd = exc.pnl_usd
            self._record_risk(pos)
            _cleanup()
            return

        except CLOBExitError as exc:
            current_retries = getattr(pos, "sell_retry_count", 0) + 1
            pos.sell_retry_count = current_retries
            if current_retries >= 3:
                logger.error("EXIT [{}] sell exhausted — force settle", asset)
                try:
                    await self._pos_mgr.close_position(
                        pos, ExitReason.SETTLE_EXHAUSTED,
                        override_exit_price=override_exit,
                        live_trader=live_trader,
                        resolved_market=resolved_mkt,
                    )
                    self._record_risk(pos)
                    _cleanup()
                    return
                except AlreadyClosedError:
                    self._record_risk(pos)
                    _cleanup()
                    return
                except Exception as inner_exc:
                    logger.error("EXIT [{}] settle_exhausted failed: {}", asset, inner_exc)
                    _cleanup()
                    return
            logger.warning("EXIT [{}] sell failed ({}/3): {}", asset, current_retries, exc.reason)
            _cleanup()
            return

        except asyncio.CancelledError:
            logger.warning("EXIT [{}] cancelled", asset)
            _cleanup()
            raise

        except Exception as exc:
            logger.exception("EXIT [{}] _do_close error: {}", asset, exc)
            lock = self._exit_retry_locks.setdefault(asset, asyncio.Lock())
            async with lock:
                if not getattr(pos, "_exit_retry_done", False):
                    logger.info("EXIT [{}] retrying once in 2 seconds", asset)
                    pos._exit_retry_done = True
                    await asyncio.sleep(2.0)
                    try:
                        await self._pos_mgr.close_position(
                            pos, reason,
                            override_exit_price=override_exit,
                            live_trader=live_trader,
                            resolved_market=resolved_mkt,
                        )
                        self._record_risk(pos)
                        _cleanup()
                        return
                    except Exception as retry_exc:
                        logger.exception("EXIT [{}] retry also failed: {}", asset, retry_exc)
            _cleanup()
