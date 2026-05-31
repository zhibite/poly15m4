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
    ):
        self._pos_mgr = pos_mgr
        self._decision_logger = decision_logger
        self._exit_retry_locks: dict[str, asyncio.Lock] = {}

    def sync_params(self, cfg: dict) -> None:
        pass

    def check_exit(self, ctx: WindowContext) -> ExitSignal:
        """
        决策顺序：
        1. 窗口到期 → 结算
        2. 无效持仓 → 退出
        3. PnL >= tp_pct → 止盈退出
        4. PnL <= sl_pct → 止损退出
        """
        pos = ctx.position
        if not pos:
            return ExitSignal()

        now = datetime.now(timezone.utc)
        mkt = ctx.market

        if getattr(pos, "_pending_close", False):
            return ExitSignal()

        # 窗口到期 → 结算
        if now >= pos.window_end:
            exit_px = mkt._up_mid if (mkt and pos.side == Side.UP) else (mkt._down_mid if mkt else 0.5)
            return ExitSignal(
                should_exit=True,
                reason=ExitReason.REASON_SETTLE,
                exit_price_override=exit_px,
                closed_at_override=now,
            )

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

        # 止盈
        if pnl_pct >= pos.tp_pct:
            pos.tp_hit = {
                "kind": ExitReason.TAKE_PROFIT,
                "elapsed_sec": elapsed_s,
                "exit_price": current_price,
                "pnl_pct": round(pnl_pct, 2),
            }
            logger.info(
                "EXIT [{}] TAKE_PROFIT: pnl={:+.1f}% >= tp={:.1f}%",
                pos.asset, pnl_pct, pos.tp_pct,
            )
            return ExitSignal(
                should_exit=True,
                reason=ExitReason.REASON_TP,
                tp_hit_data=pos.tp_hit,
            )

        # 止损
        if pnl_pct <= pos.sl_pct:
            pos.tp_hit = {
                "kind": ExitReason.STOP_LOSS,
                "elapsed_sec": elapsed_s,
                "exit_price": current_price,
                "pnl_pct": round(pnl_pct, 2),
            }
            logger.info(
                "EXIT [{}] STOP_LOSS: pnl={:+.1f}% <= sl={:.1f}%",
                pos.asset, pnl_pct, pos.sl_pct,
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

        elif reason == ExitReason.REASON_SETTLE:
            _, _, _, _, up_wins = await self._pos_mgr.settle_if_due(
                pos, resolved_market=ctx.market,
            )
            await self.spawn_close(
                asset, pos, ExitReason.REASON_SETTLE,
                live_trader=live_trader,
                ctx=ctx,
                settle_up_wins=up_wins,
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
            ctx.on_exit_complete()
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
                    _cleanup()
                    return
                except AlreadyClosedError:
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
                        _cleanup()
                        return
                    except Exception as retry_exc:
                        logger.exception("EXIT [{}] retry also failed: {}", asset, retry_exc)
            _cleanup()
