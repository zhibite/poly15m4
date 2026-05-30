"""
decision_logger.py — 市场结算追踪（已移除 JSONL，改为 DB 记录）

保留功能：
- 背景轮询市场结算结果（_track_resolution）
- 按 condition_id 取消追踪（cancel_tracking）
- 优雅关闭（close）
"""

import asyncio
from datetime import datetime, timezone

from loguru import logger

from app.config import settings


class DecisionLogger:
    """
    管理每个 condition_id 的结算轮询任务。
    不再写入 JSONL 文件，交易记录通过 EntryRecord / ExitRecord 保存到 DB。
    """

    def __init__(self, pm_client, resolution_poll_sec: int = 20, max_poll_minutes: int = 10):
        self._pm = pm_client
        self._pending_ids: set[str] = set()
        self._lock = asyncio.Lock()
        self._poll_sec = resolution_poll_sec
        self._max_polls = max_poll_minutes * 60 // resolution_poll_sec
        self._bg_tasks: set = set()
        self._track_tasks: dict[str, asyncio.Task] = {}

    async def close(self) -> None:
        """Cancel all pending resolution-tracking tasks on shutdown."""
        if not self._bg_tasks:
            return
        logger.info("DecisionLogger shutting down — cancelling {} background tasks...", len(self._bg_tasks))
        for t in list(self._bg_tasks):
            try:
                t.cancel()
            except Exception:
                pass
        try:
            await asyncio.wait_for(
                asyncio.gather(
                    *[t for t in self._bg_tasks if t is not None],
                    return_exceptions=True,
                ),
                timeout=30.0,
            )
        except asyncio.TimeoutError:
            stuck = [f"<Task {id(t)}>" for t in self._bg_tasks if not t.done()]
            logger.warning("DecisionLogger shutdown timed out — {} task(s) did not finish", len(stuck))
        self._bg_tasks.clear()
        self._track_tasks.clear()
        logger.info("DecisionLogger shutdown complete.")

    def cancel_tracking(self, condition_id: str) -> bool:
        """Cancel the resolution-tracking task for a specific condition_id."""
        task = self._track_tasks.pop(condition_id, None)
        if task is not None:
            task.cancel()
            logger.debug("cancel_tracking {} — task cancelled", condition_id[:12])
            return True
        return False

    # ── 公开方法（JSONL 相关已移除，DB 层处理记录）───────────────────────────

    async def log_entry_decision(self, **kwargs):
        """已废弃：入场记录通过 EntryRecord 写入 DB，此方法保留为空操作。"""
        pass

    async def log_exit_decision(self, **kwargs):
        """已废弃：出场记录通过 ExitRecord 写入 DB，此方法保留为空操作。"""
        pass

    def backfill_settlement(self, **kwargs):
        """已废弃：结算回填由 DB 层处理，此方法保留为空操作。"""
        pass

    # ── 内部追踪 ─────────────────────────────────────────────────────────

    async def _track_resolution(self, condition_id: str, window_end_iso: str):
        """后台任务：窗口结束后轮询 Gamma 结算结果（不写文件）。"""
        try:
            window_end = datetime.fromisoformat(window_end_iso.replace("Z", "+00:00"))
        except Exception:
            return

        async with self._lock:
            if condition_id in self._pending_ids:
                return
            self._pending_ids.add(condition_id)

        # 等待窗口到期
        while True:
            now = datetime.now(timezone.utc)
            remaining = (window_end - now).total_seconds()
            if remaining <= 0:
                break
            await asyncio.sleep(min(5.0, remaining))

        # 等待 grace period 后轮询结算
        await asyncio.sleep(60)
        try:
            resolved = await self._poll_until_resolved(condition_id)
        except asyncio.CancelledError:
            async with self._lock:
                self._pending_ids.discard(condition_id)
                self._track_tasks.pop(condition_id, None)
            return

        async with self._lock:
            self._pending_ids.discard(condition_id)
            self._track_tasks.pop(condition_id, None)

        if not resolved:
            logger.warning("resolution timed out for {}", condition_id[:12])

    async def _poll_until_resolved(self, condition_id: str) -> bool:
        """最多轮询 self._max_polls 次。"""
        for _ in range(self._max_polls):
            await asyncio.sleep(self._poll_sec)
            res = await self._pm.get_market_resolution(condition_id)
            if res and res.get("resolved"):
                up_wins = res.get("up_wins")
                outcome = res.get("outcome", "")
                up_final = down_final = None
                try:
                    mkt_info = await self._pm.get_clob_market_info(condition_id)
                    if mkt_info:
                        prices = mkt_info.get("outcomePrices", "")
                        if prices:
                            parts = prices.split(" ")
                            if len(parts) >= 2:
                                up_final = float(parts[0].replace(",", ""))
                                down_final = float(parts[1].replace(",", ""))
                except Exception:
                    pass
                logger.info("resolution: {} -> up_wins={} outcome={}", condition_id[:12], up_wins, outcome)
                return True
        return False
