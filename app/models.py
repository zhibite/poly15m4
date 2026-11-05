"""
models.py — SQLAlchemy models: Trade + User.
"""

import enum
from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, Enum as SAEnum, Float, Integer, String, Text
from app.database import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Side(str, enum.Enum):
    UP = "UP"
    DOWN = "DOWN"


class Fill(Base):
    """记录每笔交易的每次成交（含首单和补仓）。"""
    __tablename__ = "fills"

    id = Column(Integer, primary_key=True, autoincrement=True)
    # nullable=True: Fill records are created before a Trade exists (trade_id set to NULL).
    # _record_trade links them by updating trade_id to the real Trade.id.
    trade_id = Column(Integer, index=True, nullable=True)
    asset = Column(String, index=True, nullable=False)
    condition_id = Column(String, index=True, nullable=False)
    side = Column(SAEnum(Side), nullable=False)

    # 成交信息
    fill_price = Column(Float, nullable=False)
    tokens = Column(Float, nullable=False)
    cost_usdc = Column(Float, nullable=False)
    fee_usdc = Column(Float, default=0.0)

    # 补仓序号（0=首单，1=第一次补仓，以此类推）
    fill_index = Column(Integer, default=0, nullable=False)
    # 本次成交后的累计均价
    vwap_after = Column(Float, nullable=False)
    # 本次成交时的市场中间价（用于判断触发条件）
    mid_price_at_fill = Column(Float, nullable=True)

    mode = Column(String(8), default="dry", nullable=False)
    created_at = Column(DateTime, default=_utcnow)


class EntryRecord(Base):
    """
    入场记录（买入）。每次开仓生成一条，与 ExitRecord 一一对应（通过 condition_id 关联）。
    同一 condition_id 只会有一条 EntryRecord。
    """
    __tablename__ = "entry_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    asset = Column(String, index=True, nullable=False)
    condition_id = Column(String, index=True, nullable=False)
    side = Column(SAEnum(Side), nullable=False)

    # 标的、方向、入场价格
    entry_price = Column(Float, nullable=False)          # 原始成交价
    effective_entry_price = Column(Float, nullable=False) # 实际成本均价
    tokens_bought = Column(Float, nullable=False)         # 买入 token 数量
    cost_usdc = Column(Float, nullable=False)             # 实际花费 USDC（含手续费）
    fee_usdc = Column(Float, default=0.0)                 # 手续费

    # 入场元信息
    mode = Column(String(8), default="dry", nullable=False)  # dry / live
    opened_at = Column(DateTime, default=_utcnow)

    # 入场信号强度（用于事后分析）
    entry_core_agree = Column(Integer, default=0)


class ExitRecord(Base):
    """
    出场记录（卖出）。每次平仓（含部分平仓）生成一条。
    完整持仓平仓：pnl_pct 基于 (exit_price - effective_entry_price) / effective_entry_price
    部分平仓：pnl_pct 基于本次卖出部分的 (exit_price - effective_entry_price)
    """
    __tablename__ = "exit_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    asset = Column(String, index=True, nullable=False)
    condition_id = Column(String, index=True, nullable=False)
    side = Column(SAEnum(Side), nullable=False)

    # 出场价格
    exit_price = Column(Float, nullable=False)

    # 平仓数量（本笔卖出数量；全额平仓时 = 全部持仓）
    tokens_exited = Column(Float, nullable=False)

    # 部分平仓比例（0.0 ~ 1.0；全额平仓为 1.0）
    partial_pct = Column(Float, default=1.0)

    # 本次 PnL（pnl_usd / pnl_pct 为扣除双边手续费后的净值）
    pnl_pct = Column(Float, nullable=True)   # 净收益率（基于总投入 + 入场手续费）
    pnl_usd = Column(Float, nullable=True)   # 净盈亏
    gross_pnl_usd = Column(Float, nullable=True, default=0.0)  # 未扣手续费盈亏
    fee_usdc = Column(Float, default=0.0)    # 本笔卖出的手续费

    # 关联的入场记录
    entry_record_id = Column(Integer, index=True, nullable=True)

    # 出场原因
    close_reason = Column(Text, default="")

    mode = Column(String(8), default="dry", nullable=False)
    closed_at = Column(DateTime, default=_utcnow)


class Trade(Base):
    """
    完整交易记录（入场+出场全生命周期）。保留以兼容历史数据和 API 查询。
    """
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, autoincrement=True)
    asset = Column(String, index=True, nullable=False)
    condition_id = Column(String, index=True, nullable=False)
    side = Column(SAEnum(Side), nullable=False)

    entry_price = Column(Float, nullable=False)
    effective_entry_price = Column(Float, nullable=False)
    tokens_bought = Column(Float, nullable=False)
    exit_price = Column(Float, nullable=True)

    pnl_pct = Column(Float, nullable=True)   # 净收益率
    pnl_usd = Column(Float, nullable=True)   # 净盈亏
    gross_pnl_usd = Column(Float, nullable=True, default=0.0)  # 未扣手续费盈亏
    fee_usdc = Column(Float, default=0.0)    # 双边手续费合计
    close_reason = Column(Text, default="")

    bet_usdc = Column(Float, nullable=False)
    mode = Column(String(8), default="dry", nullable=False)
    fills = Column(Integer, default=1, nullable=False)
    # main — 历史数据中可能仍有 end（末端策略已移除）
    strategy = Column(String(8), default="main", nullable=False, index=True)
    entry_core_agree = Column(Integer, default=0, nullable=False)

    opened_at = Column(DateTime, default=_utcnow)
    closed_at = Column(DateTime, default=_utcnow)


class DecisionSnapshot(Base):
    """
    每次「新 bar 信号计算」保存一份完整决策快照，用于事后的信号有效性分析。

    只保存主策略 EntryController 的决策。历史数据中可能仍有 strategy='end'
    （末端策略已移除）。
    """
    __tablename__ = "decision_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=_utcnow, index=True)
    asset = Column(String, index=True, nullable=False)
    condition_id = Column(String, index=True, nullable=False)
    bar_ts = Column(Integer, default=0, nullable=False, index=True)
    window_start = Column(DateTime, nullable=True)
    elapsed_sec = Column(Float, default=0.0)
    mode = Column(String(8), default="dry", nullable=False)

    up_mid = Column(Float, default=0.5)
    down_mid = Column(Float, default=0.5)
    decision_price = Column(Float, default=0.0)
    direction = Column(String(8), default="NONE")
    core_agree = Column(Integer, default=0)
    core_contradict = Column(Integer, default=0)
    strength = Column(Integer, default=0)
    advanced_filter = Column(String(16), default="neutral")
    suggested_action = Column(String(16), default="SKIP")
    skip_reason = Column(Text, default="")

    # 完整的 Core 6 / Advanced 5 详情（JSON 文本）
    core_json = Column(Text, default="")
    advanced_json = Column(Text, default="")

    # 因子模型结果
    factor_mode = Column(Integer, default=0)
    factor_net = Column(Float, default=0.0)
    factor_agreement = Column(Float, default=0.0)
    factor_conflict = Column(Float, default=0.0)
    factor_direction = Column(String(8), default="NONE")
    factors_json = Column(Text, default="")

    entered = Column(Integer, default=0, nullable=False)
    entry_record_id = Column(Integer, nullable=True)


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(64), unique=True, index=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    is_admin = Column(Integer, default=0)
    created_at = Column(DateTime, default=_utcnow)
