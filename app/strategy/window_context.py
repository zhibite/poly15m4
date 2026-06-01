"""
window_context.py — 窗口生命周期状态（单持仓版）

每个品种维护一个 WindowContext 实例，集中管理该品种当前窗口的所有状态。
当窗口切换（condition_id 变化）时，由 WindowManager 负责创建新实例、丢弃旧实例。

简化说明（2026-05-30）：
- 无补仓相关字段
- 单单持仓：只允许一次入场，入场后只做止盈止损/窗口结算
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

from loguru import logger

from app.models import Side
from app.strategy.position_state import PositionState

if TYPE_CHECKING:
    from app.strategy.models import MarketInfo, OpenPosition


@dataclass(frozen=True)
class PriceSnapshot:
    """不可变价格快照"""
    up_mid: float = 0.5
    down_mid: float = 0.5

    def current_price(self, side: "Side") -> float:
        return self.up_mid if side == Side.UP else self.down_mid


@dataclass
class WindowContext:
    """
    一个 Polymarket 窗口的生命周期状态容器。
    """

    asset: str
    condition_id: str
    window_start: datetime
    window_end: datetime

    # 在本窗口内已开仓过的方向集合
    traded_sides: set[Side] = field(default_factory=set)

    # 状态机
    state: PositionState = PositionState.NO_POSITION

    # 当前窗口对应的 MarketInfo
    market: Optional["MarketInfo"] = field(default=None)

    # 实时价格快照
    price_snapshot: PriceSnapshot = field(default_factory=PriceSnapshot)

    # 当前窗口内的持仓
    position: Optional["OpenPosition"] = field(default=None)

    # 平仓占位符
    _pending_close: bool = field(default=False)

    # 当前入场决策的指标信号（用于日志记录）
    # 入场时的指标信号元数据（供决策日志使用）
    entry_signal_meta: Optional[dict] = field(default=None)

    # 标记本 tick 内窗口是否刚切换（新窗口的第一 tick 跳过 elapsed cutoff 检查）
    is_fresh: bool = field(default=False)

    # ── 末端策略：UP 价格采样序列（每分钟追加一条，用于 D3 和 D4）──────────
    # 保留最近 16 条（T=15min = 15 条 + 1 条缓冲）
    up_samples: list[float] = field(default_factory=list)
    # 上次追加时的 UTC 分钟数（避免每 tick 重复追加同一条价格）
    _last_append_minute: int = field(default_factory=-1, repr=False)

    # ── 便捷属性 ──────────────────────────────────────────────────────────

    @property
    def has_position(self) -> bool:
        return self.position is not None

    @property
    def is_traded(self) -> bool:
        return len(self.traded_sides) > 0

    # ── 交易记录 ──────────────────────────────────────────────────────────

    def mark_traded(self, side: Side) -> None:
        self.traded_sides.add(side)

    # ── 状态转换 ──────────────────────────────────────────────────────────

    def on_exit_complete(self) -> None:
        """
        平仓完成后调用：
        1. 重置孤儿标记（H4）
        2. 清除持仓引用
        3. 清除 ctx 级别的平仓占位符
        4. 状态机切换回 NO_POSITION
        """
        # 重置孤儿标记（H4）：平仓完成意味着数据链路已完整，孤儿状态已由 R11 解除
        if self.position is not None:
            self.position._is_orphan_position = False
        self.position = None
        self._pending_close = False
        self.state = PositionState.NO_POSITION

    # ── 重置 ──────────────────────────────────────────────────────────────

    def reset_for_new_window(self) -> None:
        """
        新窗口开始时重置所有窗口级状态。
        简化版：直接重置，不保留任何历史。
        """
        self.traded_sides.clear()
        self.state = PositionState.NO_POSITION
        # H4 fix: clear orphan marker from previous window's position.
        # If a fill-save failed in the previous window, the orphan flag is no longer
        # relevant after a new window begins.
        if self.position is not None:
            self.position._is_orphan_position = False
        self.position = None
        self._pending_close = False
        self.is_fresh = True
        self.up_samples.clear()
        self._last_append_minute = -1

    def elapsed(self, now: Optional[datetime] = None) -> float:
        if now is None:
            now = datetime.now(timezone.utc)
        elapsed = (now - self.window_start).total_seconds()
        # C4 fix: guard against negative elapsed (datetime.min start before first discovery)
        # and against extremely large values that would immediately block entry.
        if elapsed < 0:
            return 0.0
        # P3-8 fix: if elapsed is over 1 day, the window_start is almost certainly a
        # default sentinel value (datetime.min) that was never properly set by discover_market.
        # Clamp to 0 to avoid immediately blocking entry, and log a warning so operators
        # can detect misconfiguration or discover_market failures.
        if elapsed > 86400:
            logger.warning(
                "elapsed [{}] clamped: window_start={} elapsed={:.0f}s (>86400) — "
                "discover_market may have failed or condition_id is invalid ({})",
                self.asset,
                self.window_start.isoformat() if self.window_start else "None",
                elapsed,
                self.condition_id[:12] + "..." if self.condition_id else "(empty)",
            )
            return 0.0
        return elapsed

    def is_late(self, cutoff_sec: float) -> bool:
        return self.elapsed() >= cutoff_sec - 1e-9

    # ── 末端策略：UP 价格采样 ────────────────────────────────────────────

    def append_up_sample(self, up_mid: float) -> None:
        """每分钟追加一条 UP 中价采样（去重：同分钟内不重复追加）"""
        current_minute = datetime.now(timezone.utc).minute
        if current_minute == self._last_append_minute:
            return
        self._last_append_minute = current_minute
        self.up_samples.append(up_mid)
        if len(self.up_samples) > 16:
            self.up_samples = self.up_samples[-16:]
