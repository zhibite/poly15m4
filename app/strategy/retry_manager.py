"""
retry_manager.py — 共享 FOK 重试调度器

entry_controller 和 ExitController 各维护一套结构几乎相同的重试状态。
抽取为共享模块，消除重复代码。

Entry 模式（max_retries=3, backoff_sec=2*attempt）：
  key → {stake, direction, price, retry_count, next_fire_at}

Add 模式（有 max_retries=5，backoff_sec=5s，超时 60s 自动清除）：
  key → {addons_done, stake, trigger_px, retry_count, next_fire_at, first_fire_at}

所有 public 方法均为 async，内部通过 asyncio.Lock 保护 _state 字典，
避免在 poll_interval=1s 高频场景下，fire_entry_retries 和 _do_entry
并发访问同一 asset 的重试队列时出现 RuntimeError: dictionary changed size during iteration。

使用方式：
  rm = RetryManager(mode="entry")
  if await rm.schedule(asset, stake=1.0, direction="UP", price=0.3):
      logger.info("retry scheduled")
  for item in await rm.pop_ready(asset, now_ts):
      await fire_item(item)
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal

from loguru import logger


@dataclass
class RetryItem:
    """一次待重试的操作"""
    key: str
    stake: float
    direction: str
    price: float
    retry_count: int
    next_fire_at: float
    addons_done: int = 0
    trigger_px: float = 0.0
    entry_kind: str = ""
    is_balance_retry: bool = False


@dataclass
class RetryManager:
    """
    统一的 FOK 失败重试调度器。

    支持两种模式：
      "entry" — 有最大重试次数（3次），带 backoff 延迟
      "add"   — 有最大重试次数（5次），backoff 延迟 5s，超时 60s 自动清除

    所有 public 方法均为 async，内部通过 asyncio.Lock 保护 _state 字典。
    每个实例拥有独立的状态字典（通过 field(default_factory=dict) 实现），
    支持多交易对并行运行而不互相干扰。
    """

    mode: Literal["entry", "add"] = "entry"
    # 实例级状态字典（而非类级别），每个 RetryManager 实例独立运行，互不干扰
    _state: dict[str, dict] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    # ── 调度 ─────────────────────────────────────────────────────────────

    async def schedule(self, key: str, **kwargs) -> bool:
        """
        注册/更新一次重试。

        entry 模式参数：stake, direction, price
        add 模式参数：stake, trigger_px, addons_done

        Returns:
          False = 达到最大重试次数，不再调度
        """
        async with self._lock:
            if self.mode == "entry":
                return self._schedule_entry(key, **kwargs)
            else:
                return self._schedule_add(key, **kwargs)

    def _schedule_entry(
        self,
        key: str,
        stake: float,
        direction: str,
        price: float,
    ) -> bool:
        # Bug-10 fix: reduce backoff from 10s to 5s multiplier.
        # With multiplier=5: retries at 5/10/15s → total wait 30s (was 60s).
        # This prevents exhausting retries mid-window during slow FOK fills.
        MAX_RETRIES = 3
        MAX_BACKOFF_SEC = 5.0   # cap at 5s → max total wait = 5+10+15 = 30s (was 10+20+30 = 60s)
        BACKOFF_MULTIPLIER = 5.0
        state = self._state.setdefault(key, {
            "stake": stake,
            "direction": direction,
            "price": price,
            "retry_count": 0,
            "next_fire_at": 0.0,
        })
        state["stake"] = stake
        state["direction"] = direction
        state["price"] = price

        if state["retry_count"] >= MAX_RETRIES:
            logger.error("{} retry max (3) exceeded for {} — abandoning", self.mode, key)
            self._state.pop(key, None)
            return False

        state["retry_count"] += 1
        backoff_sec = min(MAX_BACKOFF_SEC, BACKOFF_MULTIPLIER * state["retry_count"])
        state["next_fire_at"] = datetime.now(timezone.utc).timestamp() + backoff_sec
        logger.info(
            "{} retry {}/3 scheduled for {} (backoff={}s)",
            self.mode, state["retry_count"], key, backoff_sec,
        )
        return True

    def _schedule_add(
        self,
        key: str,
        stake: float,
        trigger_px: float,
        addons_done: int,
    ) -> bool:
        MAX_RETRIES = 5
        TIMEOUT_SEC = 60.0
        now_ts = datetime.now(timezone.utc).timestamp()

        state = self._state.setdefault(key, {
            "stake": stake,
            "trigger_px": trigger_px,
            "addons_done": addons_done,
            "retry_count": 0,
            "next_fire_at": now_ts,
            "first_fire_at": now_ts,
        })

        state["stake"] = stake
        state["trigger_px"] = trigger_px
        state["addons_done"] = addons_done

        if now_ts - state["first_fire_at"] > TIMEOUT_SEC:
            logger.warning("ADD retry timeout ({}s) for {} — abandoning", TIMEOUT_SEC, key)
            self._state.pop(key, None)
            return False

        if state["retry_count"] >= MAX_RETRIES:
            logger.error("ADD retry max ({}): {} — abandoning", MAX_RETRIES, key)
            self._state.pop(key, None)
            return False

        state["retry_count"] += 1
        state["next_fire_at"] = now_ts + 5.0
        logger.info(
            "ADD retry {}/{} scheduled for {} (backoff=5s, elapsed={:.0f}s)",
            state["retry_count"], MAX_RETRIES, key, now_ts - state["first_fire_at"],
        )
        return True

    # ── 查询 ─────────────────────────────────────────────────────────────

    async def pop_ready(self, key: str, now_ts: float | None = None) -> list[RetryItem]:
        """返回 key 下所有已到期的重试项。调用后不自动清除。"""
        if now_ts is None:
            now_ts = datetime.now(timezone.utc).timestamp()
        async with self._lock:
            if key not in self._state:
                return []

            state = self._state[key]
            next_fire = state.get("next_fire_at", 0.0)
            if next_fire > 0 and now_ts < next_fire:
                return []

            if self.mode == "entry":
                return [RetryItem(
                    key=key,
                    stake=state["stake"],
                    direction=state["direction"],
                    price=state["price"],
                    retry_count=state["retry_count"],
                    next_fire_at=next_fire,
                )]
            else:
                return [RetryItem(
                    key=key,
                    stake=state["stake"],
                    direction="",
                    price=0.0,
                    retry_count=state["retry_count"],
                    next_fire_at=next_fire,
                    addons_done=state["addons_done"],
                    trigger_px=state["trigger_px"],
                )]

    async def is_empty(self, key: str) -> bool:
        async with self._lock:
            return key not in self._state

    async def get(self, key: str) -> RetryItem | None:
        async with self._lock:
            state = self._state.get(key)
            if state is None:
                return None
            if self.mode == "entry":
                return RetryItem(
                    key=key,
                    stake=state["stake"],
                    direction=state["direction"],
                    price=state["price"],
                    retry_count=state["retry_count"],
                    next_fire_at=state["next_fire_at"],
                )
            return RetryItem(
                key=key,
                stake=state["stake"],
                direction="",
                price=0.0,
                retry_count=state.get("retry_count", 0),
                next_fire_at=state.get("next_fire_at", 0.0),
                addons_done=state["addons_done"],
                trigger_px=state["trigger_px"],
                is_balance_retry=state.get("is_balance_retry", False),
            )

    async def clear(self, key: str) -> None:
        """清除指定 key 的重试状态"""
        async with self._lock:
            self._state.pop(key, None)

    async def clear_if_fill_count_changed(
        self, key: str, current_fill_count: int,
    ) -> None:
        """Add 模式：若 fill_count 已增长，清除旧重试记录"""
        async with self._lock:
            state = self._state.get(key)
            if state is None:
                return
            prev_addons = state.get("addons_done", -1)
            if current_fill_count - 1 > prev_addons:
                logger.debug(
                    "{} retry {} cleared: fill_count grew ({} → {})",
                    self.mode, key, prev_addons, current_fill_count - 1,
                )
                self._state.pop(key, None)

    # ── 余额不足重试 ───────────────────────────────────────────────

    async def schedule_balance_retry(
        self, key: str, stake: float, trigger_px: float,
    ) -> bool:
        """注册余额不足导致的下次补仓重试。"""
        BALANCE_BACKOFF_SEC = 10.0
        BALANCE_MAX_RETRIES = 10
        BALANCE_TIMEOUT_SEC = 120.0
        balance_key = f"{key}:bal"

        async with self._lock:
            state = self._state.setdefault(balance_key, {
                "stake": stake,
                "trigger_px": trigger_px,
                "retry_count": 0,
                "next_fire_at": 0.0,
                "first_fire_at": datetime.now(timezone.utc).timestamp(),
                "is_balance_retry": True,
            })
            state["stake"] = stake
            state["trigger_px"] = trigger_px

            now_ts = datetime.now(timezone.utc).timestamp()
            if now_ts - state["first_fire_at"] > BALANCE_TIMEOUT_SEC:
                logger.warning(
                    "balance retry timeout ({}s) for {} — abandoning",
                    BALANCE_TIMEOUT_SEC, balance_key,
                )
                self._state.pop(balance_key, None)
                return False

            if state["retry_count"] >= BALANCE_MAX_RETRIES:
                logger.error(
                    "balance retry max ({}): {} — abandoning",
                    BALANCE_MAX_RETRIES, balance_key,
                )
                self._state.pop(balance_key, None)
                return False

            state["retry_count"] += 1
            state["next_fire_at"] = now_ts + BALANCE_BACKOFF_SEC
            logger.info(
                "balance retry {}/{} scheduled for {} (backoff={}s, elapsed={:.0f}s)",
                state["retry_count"], BALANCE_MAX_RETRIES, balance_key,
                BALANCE_BACKOFF_SEC, now_ts - state["first_fire_at"],
            )
            return True

    async def pop_ready_balance(
        self, key: str, now_ts: float | None = None,
    ) -> list[RetryItem]:
        """返回余额不足重试队列中所有已到期的项。调用后不自动清除。"""
        if now_ts is None:
            now_ts = datetime.now(timezone.utc).timestamp()
        balance_key = f"{key}:bal"
        async with self._lock:
            state = self._state.get(balance_key)
            if state is None or not state.get("is_balance_retry", False):
                return []
            next_fire = state.get("next_fire_at", 0.0)
            if next_fire > 0 and now_ts < next_fire:
                return []
            return [RetryItem(
                key=balance_key,
                stake=state["stake"],
                direction="",
                price=0.0,
                retry_count=state["retry_count"],
                next_fire_at=next_fire,
                addons_done=0,
                trigger_px=state["trigger_px"],
                is_balance_retry=True,
            )]

    async def clear_balance_retry(self, key: str) -> None:
        """清除余额不足重试状态"""
        async with self._lock:
            self._state.pop(f"{key}:bal", None)

    # ── 兼容层：同步 key 列表快照 ────────────────────────────────────────
    # fire_entry_retries 中使用 list(_state.keys()) 遍历，
    # 在持锁的情况下获取快照，避免迭代过程中字典被修改。

    async def snapshot_keys(self) -> list[str]:
        """返回当前所有 key 的快照列表（线程安全）。"""
        async with self._lock:
            return list(self._state.keys())
