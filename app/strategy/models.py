"""
models.py — 数据模型
仅包含数据结构，不含业务逻辑。
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from app.models import Side


@dataclass
class MarketInfo:
    """Polymarket 固定周期窗口市场信息（例如 5m / 15m）。"""
    asset: str
    condition_id: str
    token_up_id: str
    token_down_id: str
    window_start: datetime
    window_end: datetime
    neg_risk: bool = False

    # 动态注入的实时中间价（由策略主循环写入，避免重复查询）
    _up_mid: float = 0.0
    _down_mid: float = 0.0

    # 窗口开启时刻的 Binary Option 挂单价（discover_market 时记录）
    window_open_up: float = 0.0
    window_open_down: float = 0.0

    # 窗口开启时刻的 BTC/ETH 美元现货价（用于第3次补仓的现货价差过滤）
    window_spot_price: float = 0.0
    # 当前 tick 的 BTC/ETH 美元现货价（实时更新）
    current_spot_price: float = 0.0

    # ── ATR 计算用：最近 N 个 tick 的价格快照 ──────────────────────────
    # 由 exit_controller._record_price_tick 写入，每次 tick 更新
    _price_history: list = field(default_factory=list)


@dataclass
class OpenPosition:
    """当前持仓信息."""
    asset: str
    condition_id: str
    side: Side
    token_id_bought: str
    entry_price: float
    effective_entry_price: float
    tokens_bought: float
    bet_usdc: float
    actual_cost_usdc: float
    taker_fee_bps: int
    min_exit_price: float
    opened_at: datetime
    window_start: datetime
    window_end: datetime
    sell_retry_count: int = 0
    # Prevent _check_exit from re-triggering close while a close is already in progress
    _pending_close: bool = False
    # 平仓重试保护：exit_controller._do_close 检测到平仓失败后设 True，
    # 防止同一次退出事件无限重试。成功或窗口切换后重置。
    _exit_retry_done: bool = False
    # 平仓已触发标记（窗口过期/强制平仓时置 True，平仓成功后重置）
    _close_triggered: bool = False
    market: MarketInfo | None = None

    # ── 单单持仓止盈止损阈值（由 entry_controller 入场时设置）─────────────
    # 止盈阈值（概率%，如 65.0 表示 65%）
    tp_pct: float = 0.0
    # 止损阈值（负值，如 -10.0 表示 -10%）
    sl_pct: float = 0.0

    # ── 入场信息 ────────────────────────────────────────────────────
    entry_core_agree: int = 0
    # 入场时的锚点偏离百分比
    entry_anchor_deviation_pct: float = 0.0
    # 部分止盈已触发（防止同一仓位多次触发部分止盈）
    _partial_exit_triggered: bool = False
    # 孤儿持仓标记：入场成功但 DB 记录失败时设为 True，下次 startup 的 R11 孤儿检测会处理
    _is_orphan_position: bool = False
    # 关联的 EntryRecord ID（入场时写入，便于出场时关联）；None 表示未关联
    entry_record_id: int | None = None
    # 当前待平仓比例（0.0~1.0），由 close_position 写入，_record_trade 读取
    _pending_partial_pct: float = 1.0
    # ── 平仓记录保护（防止重复记录）────────────────────────────────
    _trade_recorded: bool = False
    _trade_recorded_reason: str = ""
    _last_pnl_pct: float = 0.0
    _last_pnl_usd: float = 0.0
    # 平仓重试标志：True 表示已尝试过一次 retry，防止无限重试
    _exit_retry_done: bool = False
    # 锚点价（用于部分止盈时的参考价）
    anchor_price: float = 0.0
