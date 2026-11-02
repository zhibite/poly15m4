"""
risk_manager.py — 主策略账户级风控

职责：
  - 每日已实现亏损上限（USDC）
  - 连续亏损次数上限 + 冷却期
  - 启动时从 DB 恢复当日状态，避免重启后重置风控

只统计主策略（close_reason 不以 END_ 开头）的 ExitRecord。
末端策略有自己的每日亏损控制。
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from loguru import logger


class RiskManager:
    def __init__(self):
        self._day: str = ""
        self._realized_pnl: float = 0.0
        self._consecutive_losses: int = 0
        self._cooldown_until: float = 0.0

        self._max_daily_loss_usdc: float = -20.0
        self._max_consecutive_losses: int = 3
        self._loss_cooldown_sec: int = 900

    # ── 参数 ──────────────────────────────────────────────────────────────

    def sync_params(self, cfg: dict) -> None:
        self._max_daily_loss_usdc = float(
            cfg.get("main_max_daily_loss_usdc", -20.0)
        )
        self._max_consecutive_losses = int(
            float(cfg.get("main_max_consecutive_losses", 3))
        )
        self._loss_cooldown_sec = int(
            float(cfg.get("main_loss_cooldown_sec", 900))
        )
        self._rollover()

    def _rollover(self) -> None:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self._day == today:
            return
        self._day = today
        self._realized_pnl = 0.0
        self._consecutive_losses = 0
        self._cooldown_until = 0.0

    # ── 状态更新 ──────────────────────────────────────────────────────────

    def record_exit(self, pnl_usd: float, pnl_pct: float = 0.0) -> None:
        """记录一笔主策略已实现盈亏（每笔持仓只调用一次）。"""
        self._rollover()
        self._realized_pnl += float(pnl_usd)

        if pnl_usd < 0:
            self._consecutive_losses += 1
            if (
                self._max_consecutive_losses > 0
                and self._consecutive_losses >= self._max_consecutive_losses
            ):
                self._cooldown_until = time.time() + self._loss_cooldown_sec
                logger.warning(
                    "RISK: {} consecutive losses (last pnl={:+.2f}u) — "
                    "cooldown {:.0f}s",
                    self._consecutive_losses, pnl_usd, self._loss_cooldown_sec,
                )
        else:
            self._consecutive_losses = 0

        logger.info(
            "RISK: recorded pnl={:+.2f}u ({:+.2f}%) | day_pnl={:+.2f}u | "
            "consecutive_losses={}",
            pnl_usd, pnl_pct, self._realized_pnl, self._consecutive_losses,
        )
        if (
            self._max_daily_loss_usdc < 0
            and self._realized_pnl <= self._max_daily_loss_usdc
        ):
            logger.warning(
                "RISK: daily loss limit reached ({:+.2f}u <= {:+.2f}u) — "
                "no new entries today",
                self._realized_pnl, self._max_daily_loss_usdc,
            )

    def can_enter(self) -> tuple[bool, str]:
        """返回 (是否允许开新仓, 原因)。"""
        self._rollover()
        now = time.time()
        if self._cooldown_until and now >= self._cooldown_until:
            # 冷却结束，重新计数，避免一次冷却后立刻因历史连亏再次触发
            self._cooldown_until = 0.0
            self._consecutive_losses = 0
            logger.info("RISK: consecutive-loss cooldown expired — trading resumed")

        if (
            self._max_daily_loss_usdc < 0
            and self._realized_pnl <= self._max_daily_loss_usdc
        ):
            return False, "daily_loss_limit"
        if now < self._cooldown_until:
            return False, "loss_cooldown"
        return True, "ok"

    def snapshot(self) -> dict:
        """供 WebSocket / dashboard 展示的只读状态。"""
        self._rollover()
        allowed, reason = self.can_enter()
        return {
            "day": self._day,
            "realized_pnl_usd": round(self._realized_pnl, 4),
            "consecutive_losses": self._consecutive_losses,
            "cooldown_until": self._cooldown_until,
            "max_daily_loss_usdc": self._max_daily_loss_usdc,
            "max_consecutive_losses": self._max_consecutive_losses,
            "can_enter": allowed,
            "pause_reason": reason,
        }

    # ── 重启恢复 ──────────────────────────────────────────────────────────

    async def load_from_db(self) -> None:
        """从当日主策略 ExitRecord 恢复已实现盈亏和连续亏损状态。"""
        try:
            from sqlalchemy import select

            from app.database import async_session
            from app.models import ExitRecord

            today_start = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0,
            )
            self._rollover()

            async with async_session() as db:
                result = await db.execute(
                    select(ExitRecord.pnl_usd)
                    .where(
                        ExitRecord.closed_at >= today_start,
                        ExitRecord.close_reason.not_like("END_%"),
                    )
                    .order_by(ExitRecord.closed_at.desc())
                )
                pnls = [float(v or 0.0) for v in result.scalars().all()]

                # 最近 N 笔（按时间倒序）的前导亏损数
                consecutive = 0
                for pnl in pnls:
                    if pnl < 0:
                        consecutive += 1
                    else:
                        break

            self._realized_pnl = sum(pnls)
            self._consecutive_losses = consecutive
            logger.info(
                "RISK restored: day_pnl={:+.2f}u, consecutive_losses={}, samples={}",
                self._realized_pnl, self._consecutive_losses, len(pnls),
            )
        except Exception as exc:
            logger.warning("RISK load_from_db failed (starting flat): {}", exc)
