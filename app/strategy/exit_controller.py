"""
exit_controller.py — 退出控制器（单单持仓版，三重门信号辅助退出）

三重门逻辑：
  Gate 1（信号反向早平）：持仓方向与实时 Core 6 共识反向 ≥ 4/6 → 立即离场
  Gate 2（信号续持）：    TP 阈值触发但实时信号同向 ≥ 4/6 → 跳过止盈继续持仓
  Gate 3（移动止损）：    概率触发或信号反转时激活，跟随市场移动

日志策略：
  - INFO：三重门任一命中时（续持 / 早平 / 移动止损激活）
  - INFO：每次 check_exit 调用时（记录完整决策上下文，便于回测分析）
  - DEBUG：阈值计算过程（仅需要深入调试时启用）
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from loguru import logger

from app.models import Side
from app.strategy.models import OpenPosition
from app.strategy.position_manager import AlreadyClosedError, CLOBExitError, PositionManager
from app.strategy.position_state import ExitReason
from app.strategy.window_context import WindowContext


# ─────────────────────────────────────────────────────────────────────────────
# ATR 工具
# ─────────────────────────────────────────────────────────────────────────────

def _compute_atr_probability(mkt, lookback: int = 6) -> float:
    """
    计算概率 ATR（Average True Range in probability space）。

    Polymarket 合约的价格本质是概率值 [0, 1]。
    ATR_probability 衡量的是：最近 N 个 tick 内，概率的真实波动幅度均值。

    计算方法（与标准 ATR 相同）：
      TR = max(
          |当前UP - 上一UP|,
          |当前UP - 上一DOWN|,
          |上一UP - 上一DOWN|   ← 跨合约窗口的真实波动
      )
    取最近 lookback 个 TR 的指数移动平均（alpha = 2/(lookback+1)）。

    返回值单位：概率百分比（如 0.05 表示 5% 的概率波动）。
    """
    if mkt is None:
        return 0.05  # 无数据时保守默认值

    history = getattr(mkt, "_price_history", [])
    if len(history) < 2:
        return 0.05  # 历史数据不足时用默认值

    window = history[-lookback:]
    true_ranges: list[float] = []

    for i in range(1, len(window)):
        prev = window[i - 1]
        curr = window[i]
        prev_up = prev.get("up_mid", 0.5)
        prev_down = prev.get("down_mid", 0.5)
        curr_up = curr.get("up_mid", 0.5)
        curr_down = curr.get("down_mid", 0.5)

        tr = max(
            abs(curr_up - prev_up),
            abs(curr_up - prev_down),
            abs(prev_up - prev_down),
        )
        true_ranges.append(tr)

    if not true_ranges:
        return 0.05

    alpha = 2.0 / (lookback + 1.0)
    atr = true_ranges[0]
    for tr in true_ranges[1:]:
        atr = alpha * tr + (1.0 - alpha) * atr
    return max(atr, 0.01)


def _record_price_tick(mkt, up_mid: float, down_mid: float) -> None:
    """将当前 tick 的价格快照写入 history，供 ATR 计算使用。"""
    if mkt is None:
        return
    if not hasattr(mkt, "_price_history"):
        mkt._price_history = []
    mkt._price_history.append({
        "up_mid": up_mid,
        "down_mid": down_mid,
    })
    if len(mkt._price_history) > 20:
        mkt._price_history = mkt._price_history[-20:]


# ─────────────────────────────────────────────────────────────────────────────
# 退出信号
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ExitSignal:
    """退出信号"""

    should_exit: bool = False
    reason: str = ""
    reason_detail: str = ""   # 三重门命中详情
    tp_hit_data: Optional[dict] = None
    exit_price_override: Optional[float] = None
    closed_at_override: Optional[datetime] = None
    stop_loss_pct: float = 0.0
    take_profit_pct: float = 0.0
    partial_close_pct: float = 0.0


# ─────────────────────────────────────────────────────────────────────────────
# 实时信号（持仓期 Core 6 共识）
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class RealtimeSignal:
    """
    持仓期的实时信号状态。
    由 check_exit 调用时根据传入的 indicators dict 计算。
    """
    agree_count: int = 0          # Core 6 同向数量（0~6）
    agree_dir: str = ""           # "UP" / "DOWN" / "MIXED"
    reverse_count: int = 0        # Core 6 反向数量（0~6）
    # ATR
    atr_value: float = 0.0        # 概率 ATR（0~1）
    atr_mult: float = 1.0         # 波动倍数（1.0~3.0）
    trend_strength: float = 0.0    # 趋势强度（0~100）
    trend_mult: float = 1.0        # 趋势乘数（极强>70→1.2，弱<30→0.8）
    # CVD 反向（P0-3: "最强单信号"，持仓期若 CVD 反向 Core 方向须立即离场）
    cvd_reverse: bool = False     # CVD Z>1σ 且方向与持仓相反
    cvd_z: float = 0.0            # CVD Z-score
    # 动态阈值
    dynamic_sl: float = 0.0       # 动态止损（负值）
    dynamic_tp1: float = 0.0      # 动态 TP1
    dynamic_tp2: float = 0.0      # 动态 TP2
    trailing_gap: float = 0.0     # 移动止损间距
    # 三重门命中结果
    gate1_triggered: bool = False
    gate1_action: str = ""
    gate1_reason: str = ""
    gate2_triggered: bool = False
    gate2_action: str = ""
    gate2_reason: str = ""
    gate3_triggered: bool = False
    gate3_action: str = ""
    gate3_reason: str = ""
    # 本次决策
    decision: str = "HOLD"  # HOLD / EXIT_NOW / TP1 / TP2 / TRAILING


def _compute_trend_strength(indicators: dict | None, pos_side: Side) -> tuple[float, float]:
    """
    计算趋势强度（0~100）和趋势乘数。

    趋势强度基于：
      - RSI 偏离中性程度（50）
      - MACD 柱状图动能
      - Core 6 共识数量（realtime_agree）
      - 趋势持续性（RSI 历史趋势）

    趋势乘数（trend_mult）：
      - 极强（>70）：TP 目标 +20%，移动止损扩大 1.5×
      - 强（50~70）：标准行为（×1.0）
      - 中（30~50）：TP 目标 -10%，移动止损收紧 0.85×
      - 弱（<30）：TP 目标 -20%，移动止损收紧 0.75×

    Returns: (trend_strength, trend_mult)
    """
    if indicators is None:
        return 50.0, 1.0

    # 1. RSI 偏离（权重 25%，偏离越大强度越高）
    ind_rsi = indicators.get("rsi", {})
    rsi_val = float(ind_rsi.get("value", 50.0))
    # 偏离程度：0（=50）→ 50（=0 or 100）
    rsi_dev = abs(rsi_val - 50.0) * 2.0  # 0~100

    # 2. MACD 动能（权重 20%，histogram 与价格方向一致则强）
    ind_macd = indicators.get("macd", {})
    macd_hist_list = ind_macd.get("histogram", [])
    macd_momentum = 0.0
    if macd_hist_list:
        recent = macd_hist_list[-3:]  # 最近3个histogram
        if pos_side == Side.UP:
            macd_momentum = sum(min(h, 0.01) for h in recent) / 3 * 100  # 正值表示 UP 动能
        else:
            macd_momentum = sum(max(h, -0.01) for h in recent) / 3 * 100 * -1
    macd_score = max(0.0, min(macd_momentum, 100.0))

    # 3. 布林带位置（权重 15%，触及边缘则强）
    ind_bb = indicators.get("bbands", {})
    bb_sig = ind_bb.get("signal", "within")
    bb_score = 80.0 if bb_sig in ("upper_touch", "lower_touch") else 40.0

    # 4. VWAP 偏离（权重 15%，价格显著偏离 VWAP 表示趋势强）
    ind_vwap = indicators.get("vwap", {})
    vwap_sig = ind_vwap.get("signal", "at")
    vwap_score = 80.0 if vwap_sig in ("above", "below") else 30.0

    # 5. Core 6 共识数量（权重 25%，已在 _compute_realtime_signal 中计算）
    # 通过 RSI/macd/vwap 等指标间接反映，此处取 RSI 偏离和 MACD 动能的组合
    core_score = (rsi_dev * 0.5 + macd_score * 0.5)  # 已反映在 rsi_dev 和 macd_score 中

    # 综合趋势强度（加权平均）
    trend_strength = (
        rsi_dev * 0.25 +
        macd_score * 0.20 +
        bb_score * 0.15 +
        vwap_score * 0.15 +
        core_score * 0.25
    )
    trend_strength = max(0.0, min(trend_strength, 100.0))

    # 趋势乘数
    if trend_strength > 70:
        trend_mult = 1.2
    elif trend_strength >= 50:
        trend_mult = 1.0
    elif trend_strength >= 30:
        trend_mult = 0.85
    else:
        trend_mult = 0.75

    return trend_strength, trend_mult


def _compute_realtime_signal(
    indicators: dict | None,
    pos_side: Side,
    mkt=None,
    *,
    base_sl: float = -10.0,
    base_tp1: float = 15.0,
    base_tp2: float = 30.0,
    base_trailing_gap: float = 0.20,
) -> RealtimeSignal:
    """
    根据传入的实时指标 dict，计算 Core 6 共识数量和方向。
    同时使用 Polymarket 概率 ATR（基于 _price_history）计算动态阈值，
    实现三重门 + 标准止盈止损的统一阈值体系。

    mkt: MarketInfo 实例（含 _price_history），用于计算概率 ATR。
         若为 None 则降级使用 Binance 美元 ATR（向后兼容）。
    base_sl / base_tp1 / base_tp2 / base_trailing_gap:
         运行时可调参数，由 sync_params 注入（P3-10）。
    """
    # ── P0 Fix: 优先使用 Polymarket 概率 ATR，fallback 到 Binance 美元 ATR ──
    # 文档第五章明确要求概率 ATR：
    #   atr_probability = EMA(TR_1...TR_N)，TR = max(|UP_t-UP_{t-1}|, |UP_t-DOWN_{t-1}|, ...)
    # 若 mkt 不可用则降级使用 indicators["atr"]（Binance 美元 ATR）
    if mkt is not None:
        atr_val = _compute_atr_probability(mkt, lookback=6)
    else:
        atr_val = 0.02
        if indicators and "atr" in indicators:
            atr_val = float(indicators["atr"])

    atr_mult = min(1.0 + atr_val / 0.02, 3.0)

    # 趋势强度 + 乘数
    trend_strength, trend_mult = _compute_trend_strength(indicators, pos_side)

    # 基础阈值（由 sync_params 注入，支持运行时热更新）
    dyn_sl = max(base_sl * atr_mult, -30.0)
    dyn_sl = min(dyn_sl, -5.0)
    dyn_tp1 = max(base_tp1 * atr_mult * trend_mult, 8.0)
    dyn_tp2 = max(base_tp2 * atr_mult * trend_mult, dyn_tp1 + 5.0)
    trailing_gap = 0.20 / trend_mult

    # Core 6 共识计算（从 indicators dict 提取）
    agree_count = 0
    reverse_count = 0
    agree_dir = "MIXED"

    if indicators:
        # 从 indicators 还原每个 Core 指标的 agree 状态
        ind_rsi = indicators.get("rsi", {})
        ind_macd = indicators.get("macd", {})
        ind_vwap = indicators.get("vwap", {})
        ind_ema = indicators.get("ema", {})
        ind_pivot = indicators.get("pivot", {})
        ind_bbands = indicators.get("bbands", {})

        def _is_agree(ind: dict, pos_side: Side) -> bool | None:
            sig = ind.get("signal", "")
            if pos_side == Side.UP:
                if sig in ("oversold", "golden", "above", "upper_touch", "below"):
                    return True
                if sig in ("overbought", "death", "lower_touch"):
                    return False
            else:  # Side.DOWN
                if sig in ("overbought", "death", "lower_touch"):
                    return True
                if sig in ("oversold", "golden", "above", "upper_touch"):
                    return False
            return None

        indicators_list = [
            ("RSI", ind_rsi),
            ("MACD", ind_macd),
            ("VWAP", ind_vwap),
            ("EMA", ind_ema),
            ("PIVOT", ind_pivot),
            ("BBANDS", ind_bbands),
        ]

        for _, ind in indicators_list:
            result = _is_agree(ind, pos_side)
            if result is True:
                agree_count += 1
            elif result is False:
                reverse_count += 1

        # 确定 agree_dir
        if agree_count > reverse_count:
            agree_dir = "UP" if pos_side == Side.UP else "DOWN"
        elif reverse_count > agree_count:
            agree_dir = "DOWN" if pos_side == Side.UP else "UP"
        else:
            agree_dir = "MIXED"

        # ── CVD 反向检测（P0-3: 最强单信号）──────────────────────────
        # CVD Z>1σ 且方向与持仓方向相反 → 立即离场
        cvd_reverse = False
        cvd_z = 0.0
        ind_cvd = indicators.get("cvd", {}) if indicators else {}
        cvd_z = float(ind_cvd.get("zscore", 0.0))
        if cvd_z > 1.0:
            cvd_val = float(ind_cvd.get("value", 0.0))
            cvd_dir = "UP" if cvd_val > 0 else "DOWN"
            # CVD 方向与持仓方向相反
            if cvd_dir != pos_side.value:
                cvd_reverse = True

    return RealtimeSignal(
        agree_count=agree_count,
        agree_dir=agree_dir,
        reverse_count=reverse_count,
        atr_value=atr_val,
        atr_mult=atr_mult,
        trend_strength=trend_strength,
        trend_mult=trend_mult,
        dynamic_sl=dyn_sl,
        dynamic_tp1=dyn_tp1,
        dynamic_tp2=dyn_tp2,
        trailing_gap=trailing_gap,
        cvd_reverse=cvd_reverse,
        cvd_z=cvd_z,
    )


# ─────────────────────────────────────────────────────────────────────────────
# 退出控制器
# ─────────────────────────────────────────────────────────────────────────────

class ExitController:
    """
    单单持仓退出控制器（三重门信号辅助版）。

    对外 API：
      check_exit()   — 检查退出条件（主循环每 tick 调用一次）
      sync_params()  — 从 runtime_config 同步参数
      handle_exit()  — 执行退出
    """

    def __init__(
        self,
        pos_mgr: PositionManager,
        wm=None,
        decision_logger=None,
    ):
        self._pos_mgr = pos_mgr
        self._wm = wm
        self._decision_logger = decision_logger
        self._exit_retry_locks: dict[str, asyncio.Lock] = {}
        # 每轮 check_exit 的信号快照（用于日志）
        self._last_signal: dict[str, RealtimeSignal] = {}
        # ── P3-10 Fix: 支持运行时调参 ────────────────────────────────
        self._base_sl: float = -10.0
        self._base_tp1: float = 15.0
        self._base_tp2: float = 30.0
        self._base_trailing_gap: float = 0.20

    def sync_params(self, cfg: dict) -> None:
        """从 runtime_config 同步退出阈值参数，支持运行时热更新"""
        self._base_sl = float(cfg.get("sl_base_pct", -10.0))
        self._base_tp1 = float(cfg.get("tp1_base_pct", 15.0))
        self._base_tp2 = float(cfg.get("tp2_base_pct", 30.0))
        self._base_trailing_gap = float(cfg.get("trailing_gap_base", 0.20))

    # ── P3-12 Fix: 废弃 _get_dynamic_thresholds，统一由 _compute_realtime_signal 计算 ─
    # 此函数已被 _compute_realtime_signal 取代（内含概率 ATR 计算），
    # 保留仅用于向后兼容，不推荐新代码调用。
    def _get_dynamic_thresholds(
        self, mkt, entry_price: float, pos: "OpenPosition",
    ) -> tuple[float, float, float]:
        """
        [DEPRECATED] 基于概率 ATR 动态计算止盈止损阈值。
        请直接调用 _compute_realtime_signal(indicators, pos.side, mkt)。
        Returns: (dynamic_sl, dynamic_tp1, dynamic_tp2)
        """
        atr = _compute_atr_probability(mkt, lookback=6)
        mult = min(1.0 + atr / 0.02, 3.0)
        dyn_sl  = max(min(self._base_sl * mult, -5.0), -30.0)
        dyn_tp1 = max(self._base_tp1 * mult, 8.0)
        dyn_tp2 = max(self._base_tp2 * mult, dyn_tp1 + 5.0)
        return dyn_sl, dyn_tp1, dyn_tp2

    # ── 三重门决策 ─────────────────────────────────────────────────────────

    def _apply_three_gates(
        self,
        pos: "OpenPosition",
        sig: RealtimeSignal,
        current_price: float,
        delta_pct: float,
        now: datetime,
        mkt=None,
    ) -> tuple[ExitSignal, RealtimeSignal]:
        """
        执行三重门决策，返回 (ExitSignal, 更新后的 RealtimeSignal)。
        只在三重门命中时打印 INFO 日志。

        mkt: MarketInfo 实例（含 _price_history），用于重新计算实时信号（含 CVD 反向检测）。
        """
        pos_side = pos.side
        tp_stage = getattr(pos, "_tp_stage", 0)

        # ── Gate 1a：Core 6 信号反向早平 ───────────────────────────────
        # ── Gate 1b：CVD 反向立即平仓（最强单信号，优先级高于 Core 反向）──
        cvd_reverse = sig.cvd_reverse
        cvd_z = sig.cvd_z

        if cvd_reverse:
            sig.gate1_triggered = True
            sig.gate1_action = "EXIT_NOW"
            sig.gate1_reason = (
                f"Gate1: CVD 反向最强信号 Z={cvd_z:.2f}，立即离场"
                f" pnl={delta_pct:+.1f}%"
            )
            pos.tp_hit = {
                "kind": ExitReason.STOP_LOSS_EARLY,
                "gate": "gate1_cvd_reverse",
                "elapsed_sec": round((now - pos.window_start).total_seconds(), 1),
                "exit_price": current_price,
                "pnl_pct": round(delta_pct, 2),
                "realtime_agree": sig.agree_count,
                "cvd_z": cvd_z,
                "entry_core_agree": pos.entry_core_agree,
            }
            sig.decision = "EXIT_NOW"
            return ExitSignal(
                should_exit=True,
                reason="GATE1_EARLY_EXIT",
                reason_detail=sig.gate1_reason,
                tp_hit_data=pos.tp_hit,
                stop_loss_pct=sig.dynamic_sl,
            ), sig

        if sig.reverse_count >= 4:
            sig.gate1_triggered = True
            sig.gate1_action = "EXIT_NOW"
            sig.gate1_reason = (
                f"Gate1: Core反转 {sig.reverse_count}/6 "
                f"opposite_to_pos={pos_side.value} pnl={delta_pct:+.1f}%"
            )
            pos.tp_hit = {
                "kind": ExitReason.STOP_LOSS_EARLY,
                "gate": "gate1_signal_reverse",
                "elapsed_sec": round((now - pos.window_start).total_seconds(), 1),
                "exit_price": current_price,
                "pnl_pct": round(delta_pct, 2),
                "realtime_agree": sig.agree_count,
                "realtime_reverse": sig.reverse_count,
                "entry_core_agree": pos.entry_core_agree,
            }
            sig.decision = "EXIT_NOW"
            return ExitSignal(
                should_exit=True,
                reason="GATE1_EARLY_EXIT",
                reason_detail=sig.gate1_reason,
                tp_hit_data=pos.tp_hit,
                stop_loss_pct=sig.dynamic_sl,
            ), sig

        # ── Gate 2：TP 信号续持 ──────────────────────────────────────────
        if tp_stage == 0 and delta_pct >= sig.dynamic_tp1:
            if sig.agree_count >= 4:
                sig.gate2_triggered = True
                sig.gate2_action = "CONTINUE"
                sig.gate2_reason = (
                    f"Gate2: TP1命中(+{delta_pct:.1f}%)但信号同向{sig.agree_count}/6，"
                    f"跳过止盈继续持仓"
                )
                sig.decision = "HOLD"
                return ExitSignal(
                    should_exit=False,
                    reason="GATE2_SKIP_TP",
                    reason_detail=sig.gate2_reason,
                    stop_loss_pct=sig.dynamic_sl,
                    take_profit_pct=pos.tp_pct,
                ), sig

        if tp_stage == 1 and delta_pct >= sig.dynamic_tp2:
            if sig.agree_count >= 3:
                sig.gate2_triggered = True
                sig.gate2_action = "CONTINUE"
                sig.gate2_reason = (
                    f"Gate2: TP2命中(+{delta_pct:.1f}%)但信号同向{sig.agree_count}/6，"
                    f"跳过止盈继续持仓"
                )
                sig.decision = "HOLD"
                return ExitSignal(
                    should_exit=False,
                    reason="GATE2_SKIP_TP2",
                    reason_detail=sig.gate2_reason,
                    stop_loss_pct=sig.dynamic_sl,
                    take_profit_pct=pos.tp_pct,
                ), sig

        # ── Gate 3：移动止损信号增强 ─────────────────────────────────────
        trailing_activate = getattr(pos, "trailing_stop_activate_pct", 70.0) / 100.0
        if not getattr(pos, "trailing_stop_activated", False):
            # 提前激活条件：信号转弱（< 3/6 同向）OR 概率已达激活线
            if sig.agree_count < 3 or current_price >= trailing_activate:
                sig.gate3_triggered = True
                sig.gate3_action = "ACTIVATE_TRAILING"
                sig.gate3_reason = (
                    f"Gate3: 激活移动止损 "
                    f"(prob={current_price:.4f}>={trailing_activate:.4f} "
                    f"OR agree={sig.agree_count}/6<3)"
                )
                pos.trailing_stop_activated = True
                pos.trailing_stop_pct = max(0.0, current_price - sig.trailing_gap)
                logger.info(
                    "GATE3 ACTIVATE [{}] {} prob={:.4f} trailing_line={:.4f} "
                    "agree={}/6 atr_mult={:.2f} trend={:.0f} trend_mult={:.2f}",
                    pos.asset, pos_side.value, current_price,
                    pos.trailing_stop_pct, sig.agree_count, sig.atr_mult,
                    sig.trend_strength, sig.trend_mult,
                )

        # 移动止损跟踪
        if getattr(pos, "trailing_stop_activated", False):
            trail_line = getattr(pos, "trailing_stop_pct", 0.0)
            if trail_line > 0 and current_price <= trail_line:
                sig.gate3_triggered = True
                sig.gate3_action = "TRAILING_EXIT"
                sig.gate3_reason = (
                    f"Gate3: 跟踪线被触及 {trail_line:.4f}<=当前{current_price:.4f}"
                )
                pos.tp_hit = {
                    "kind": "移动止盈（跟踪线）",
                    "gate": "gate3_trailing",
                    "elapsed_sec": round((now - pos.window_start).total_seconds(), 1),
                    "exit_price": current_price,
                    "pnl_pct": round(delta_pct, 2),
                    "trailing_stop_pct": round(trail_line * 100, 2),
                    "realtime_agree": sig.agree_count,
                    "entry_core_agree": pos.entry_core_agree,
                }
                sig.decision = "TRAILING"
                return ExitSignal(
                    should_exit=True,
                    reason="GATE3_TRAILING_EXIT",
                    reason_detail=sig.gate3_reason,
                    tp_hit_data=pos.tp_hit,
                    stop_loss_pct=sig.dynamic_sl,
                    take_profit_pct=pos.tp_pct,
                ), sig
            # 跟随市场上移跟踪线（只上移，不下移）
            if current_price > trail_line + 0.005:
                pos.trailing_stop_pct = max(trail_line, current_price - sig.trailing_gap)

        return ExitSignal(
            should_exit=False,
            stop_loss_pct=sig.dynamic_sl,
            take_profit_pct=pos.tp_pct,
        ), sig

    # ── 标准止盈止损（三重门未命中时执行）──────────────────────────────────

    def _apply_standard_exit(
        self,
        pos: "OpenPosition",
        sig: RealtimeSignal,
        current_price: float,
        pnl_pct: float,
        now: datetime,
    ) -> tuple[ExitSignal, RealtimeSignal]:
        """三重门未触发时，执行标准的止盈止损检查。"""
        tp_stage = getattr(pos, "_tp_stage", 0)
        delta_pct = pnl_pct

        # 标准止损
        if pnl_pct <= sig.dynamic_sl:
            pos.tp_hit = {
                "kind": ExitReason.STOP_LOSS,
                "gate": "standard_sl",
                "elapsed_sec": round((now - pos.window_start).total_seconds(), 1),
                "exit_price": current_price,
                "pnl_pct": round(pnl_pct, 2),
                "atr_multiplier": sig.atr_mult,
                "entry_core_agree": pos.entry_core_agree,
            }
            return ExitSignal(
                should_exit=True,
                reason="SL_FULL",
                reason_detail=f"标准止损 pnl={pnl_pct:.2f}% <= SL={sig.dynamic_sl:.1f}%",
                tp_hit_data=pos.tp_hit,
                stop_loss_pct=sig.dynamic_sl,
                take_profit_pct=pos.tp_pct,
            ), sig

        # 标准 TP1（Gate2 未跳过时）
        if tp_stage == 0 and delta_pct >= sig.dynamic_tp1:
            pos._tp_stage = 1
            pos._partial_exit_triggered = True
            pos.tp_hit = {
                "kind": ExitReason.TAKE_PROFIT,
                "gate": "standard_tp1",
                "elapsed_sec": round((now - pos.window_start).total_seconds(), 1),
                "exit_price": current_price,
                "pnl_pct": round(pnl_pct, 2),
                "dynamic_tp1": sig.dynamic_tp1,
                "atr_mult": sig.atr_mult,
                "entry_core_agree": pos.entry_core_agree,
                "realtime_agree": sig.agree_count,
            }
            logger.info(
                "EXIT CHECK [{}] {} TP1 hit: pnl={:.1f}% delta={:.1f}% "
                "tp1={:.1f}% atr_mult={:.2f} trend={:.0f} trend_mult={:.2f} "
                "entry_agree={}/6 realtime_agree={}/6",
                pos.asset, pos.side.value, pnl_pct, delta_pct, sig.dynamic_tp1,
                sig.atr_mult, sig.trend_strength, sig.trend_mult,
                pos.entry_core_agree, sig.agree_count,
            )
            return ExitSignal(
                should_exit=True,
                reason="TP",
                reason_detail=f"标准TP1 pnl={pnl_pct:.1f}%>={sig.dynamic_tp1:.1f}%",
                tp_hit_data=pos.tp_hit,
                stop_loss_pct=sig.dynamic_sl,
                take_profit_pct=pos.tp_pct,
                partial_close_pct=0.5,
            ), sig

        # 标准 TP2
        if tp_stage == 1 and delta_pct >= sig.dynamic_tp2:
            pos._tp_stage = 2
            pos.tp_hit = {
                "kind": ExitReason.TAKE_PROFIT_2,
                "gate": "standard_tp2",
                "elapsed_sec": round((now - pos.window_start).total_seconds(), 1),
                "exit_price": current_price,
                "pnl_pct": round(pnl_pct, 2),
                "dynamic_tp2": sig.dynamic_tp2,
                "atr_mult": sig.atr_mult,
                "entry_core_agree": pos.entry_core_agree,
                "realtime_agree": sig.agree_count,
            }
            logger.info(
                "EXIT CHECK [{}] {} TP2 hit: pnl={:.1f}% delta={:.1f}% "
                "tp2={:.1f}% atr_mult={:.2f} trend={:.0f} trend_mult={:.2f} "
                "entry_agree={}/6 realtime_agree={}/6",
                pos.asset, pos.side.value, pnl_pct, delta_pct, sig.dynamic_tp2,
                sig.atr_mult, sig.trend_strength, sig.trend_mult,
                pos.entry_core_agree, sig.agree_count,
            )
            return ExitSignal(
                should_exit=True,
                reason="TP2",
                reason_detail=f"标准TP2 pnl={pnl_pct:.1f}%>={sig.dynamic_tp2:.1f}%",
                tp_hit_data=pos.tp_hit,
                stop_loss_pct=sig.dynamic_sl,
                take_profit_pct=pos.tp_pct,
                partial_close_pct=0.6,
            ), sig

        return ExitSignal(
            should_exit=False,
            stop_loss_pct=sig.dynamic_sl,
            take_profit_pct=pos.tp_pct,
        ), sig

    # ── 主检查入口 ─────────────────────────────────────────────────────────

    def check_exit(
        self,
        ctx: WindowContext,
        indicators: dict | None = None,
    ) -> ExitSignal:
        """
        检查退出条件。

        决策顺序：
        1. 窗口到期 / 无效持仓 → 直接退出
        2. Gate 1（信号反向早平）→ 立即退出
        3. Gate 2（信号续持）    → 跳过止盈，继续持仓
        4. Gate 3（移动止损）    → 激活跟踪 / 跟踪线触及则退出
        5. 标准止盈止损（以上均未触发时执行）

        indicators: 实时指标 dict（从 strategy 层传入，用于计算 realtime Core 6 共识）
        """
        pos = ctx.position
        if not pos:
            return ExitSignal()

        now = datetime.now(timezone.utc)
        mkt = ctx.market

        if getattr(pos, "_pending_close", False):
            return ExitSignal()

        # 窗口已到期
        if now >= pos.window_end:
            exit_px = 0.5
            if mkt:
                exit_px = mkt._up_mid if pos.side == Side.UP else mkt._down_mid
            return ExitSignal(
                should_exit=True,
                reason="settle",
                reason_detail="窗口到期结算",
                exit_price_override=exit_px,
                closed_at_override=now,
            )

        # 无效持仓
        if pos.tokens_bought <= 1e-9:
            return ExitSignal(
                should_exit=True,
                reason="invalid_tokens",
                reason_detail="tokens_bought <= 0",
            )

        up_mid = mkt._up_mid if mkt else 0.5
        down_mid = mkt._down_mid if mkt else 0.5

        # 记录价格快照（供 ATR 计算）
        _record_price_tick(mkt, up_mid, down_mid)

        current_price = up_mid if pos.side == Side.UP else down_mid
        pnl_pct = self._pos_mgr.unrealized_pnl_pct(up_mid, down_mid, pos)

        # 实时信号（概率 ATR 阈值 + Core 6 共识）
        sig = _compute_realtime_signal(
            indicators, pos.side, mkt,
            base_sl=self._base_sl,
            base_tp1=self._base_tp1,
            base_tp2=self._base_tp2,
            base_trailing_gap=self._base_trailing_gap,
        )

        # ── P3-14 Fix: 每 tick 日志降为 DEBUG，避免日志淹没 ─────────────────
        # 记录到实例变量，供 strategy 层或 WebSocket 调用
        self._last_signal[pos.asset] = sig

        elapsed_s = round((now - pos.window_start).total_seconds(), 1)
        window_rem = round(max(0.0, (pos.window_end - now).total_seconds()), 1)

        logger.debug(
            "EXIT CHECK [{}] elapsed={:.0f}s rem={:.0f}s | "
            "entry={:.4f} cur={:.4f} pnl={:+.1f}% | "
            "sl={:.1f}% tp1={:.1f}% tp2={:.1f}% trailing_gap={:.3f} | "
            "atr={:.4f} atr_mult={:.2f} | "
            "trend={:.0f} trend_mult={:.2f} | "
            "agree={}/6({}) rev={}/6 cvd_rev={} | "
            "tp_stage={} trailing_act={}",
            pos.asset,
            elapsed_s, window_rem,
            pos.entry_price, current_price,
            pnl_pct if pnl_pct else 0.0,
            sig.dynamic_sl, sig.dynamic_tp1, sig.dynamic_tp2, sig.trailing_gap,
            sig.atr_value, sig.atr_mult,
            sig.trend_strength, sig.trend_mult,
            sig.agree_count, sig.agree_dir,
            sig.reverse_count, sig.cvd_reverse,
            getattr(pos, "_tp_stage", 0),
            getattr(pos, "trailing_stop_activated", False),
        )

        if pnl_pct is None:
            return ExitSignal(
                stop_loss_pct=sig.dynamic_sl,
                take_profit_pct=pos.tp_pct,
            )

        # ── P1-6 Fix: 窗口末期优先持有 ─────────────────────────────────
        # 文档第十条：窗口剩余 < 60s 时，无论信号如何，优先持有到期结算
        # 避免窗口末期因信号反转或 TP 触发而产生不必要的摩擦成本（手续费+滑点）
        if window_rem < 60.0:
            logger.debug(
                "EXIT CHECK [{}] HOLD (window_end_rem={:.0f}s < 60s) — 信号={}/6 "
                "rev={}/6 cvd_rev={} pnl={:+.1f}%",
                pos.asset, window_rem,
                sig.agree_count, sig.reverse_count, sig.cvd_reverse, pnl_pct,
            )
            return ExitSignal(
                should_exit=False,
                stop_loss_pct=sig.dynamic_sl,
                take_profit_pct=pos.tp_pct,
            )

        # ── 三重门决策 ────────────────────────────────────────────────────
        signal_exit, sig = self._apply_three_gates(
            pos, sig, current_price, pnl_pct, now, mkt,
        )
        if signal_exit.should_exit:
            self._log_gate_exit(pos.asset, pos.side.value, signal_exit, sig, pnl_pct, pos)
            return signal_exit

        # Gate 命中但选择持仓（HOLD）
        if signal_exit.reason.startswith("GATE2"):
            logger.info(
                "GATE2 CONTINUE [{}] {} pnl={:+.1f}% "
                "agree={}/6 entry_agree={}/6 | {}",
                pos.asset, pos.side.value, pnl_pct,
                sig.agree_count, pos.entry_core_agree or 0,
                signal_exit.reason_detail,
            )

        # ── 标准止盈止损 ──────────────────────────────────────────────────
        standard_exit, sig = self._apply_standard_exit(
            pos, sig, current_price, pnl_pct, now,
        )
        if standard_exit.should_exit:
            return standard_exit

        return ExitSignal(
            should_exit=False,
            stop_loss_pct=sig.dynamic_sl,
            take_profit_pct=pos.tp_pct,
        )

    def _log_gate_exit(
        self,
        asset: str,
        pos_side: str,
        signal_exit: ExitSignal,
        sig: RealtimeSignal,
        pnl_pct: float,
        pos: "OpenPosition",
    ) -> None:
        """打印三重门退出时的详细信息。"""
        gate = signal_exit.reason.split("_")[0]  # GATE1 / GATE2 / GATE3
        kind = signal_exit.tp_hit_data.get("kind", signal_exit.reason) if signal_exit.tp_hit_data else signal_exit.reason
        detail = signal_exit.reason_detail or ""
        logger.info(
            "GATE EXIT [{}] {} gate={} pnl={:+.1f}% "
            "entry_agree={}/6 realtime_agree={}/6 "
            "atr_mult={:.2f} | {}",
            asset, pos_side, gate, pnl_pct,
            sig.agree_count,
            pos.entry_core_agree,
            sig.atr_mult,
            detail,
        )

    def get_unrealized_pnl_pct(self, ctx: WindowContext) -> float | None:
        pos = ctx.position
        if not pos:
            return 0.0
        mkt = ctx.market
        up_mid = mkt._up_mid if mkt else 0.5
        down_mid = mkt._down_mid if mkt else 0.5
        return self._pos_mgr.unrealized_pnl_pct(up_mid, down_mid, pos)

    # ── Exit 处理入口 ───────────────────────────────────────────────────

    async def handle_exit(
        self,
        asset: str,
        ctx: "WindowContext",
        exit_signal: ExitSignal,
        live_trader,
    ) -> None:
        pos = ctx.position
        if not pos:
            return

        if getattr(pos, "_pending_close", False):
            return

        reason = exit_signal.reason

        if reason == "TP":
            await self.spawn_close(
                asset, pos, ExitReason.TAKE_PROFIT,
                live_trader=live_trader,
                ctx=ctx,
                partial_close_pct=exit_signal.partial_close_pct,
            )

        elif reason == "TP2":
            await self.spawn_close(
                asset, pos, ExitReason.TAKE_PROFIT_2,
                live_trader=live_trader,
                ctx=ctx,
                partial_close_pct=exit_signal.partial_close_pct,
            )

        elif reason == "SL_FULL":
            await self.spawn_close(
                asset, pos, ExitReason.STOP_LOSS,
                live_trader=live_trader,
                ctx=ctx,
            )

        elif reason == "GATE1_EARLY_EXIT":
            await self.spawn_close(
                asset, pos, ExitReason.STOP_LOSS_EARLY,
                live_trader=live_trader,
                ctx=ctx,
            )

        elif reason == "GATE3_TRAILING_EXIT":
            await self.spawn_close(
                asset, pos, ExitReason.TRAILING_EXIT,
                live_trader=live_trader,
                ctx=ctx,
            )

        elif reason == "settle":
            settled, closed_at, effective, backfill, up_wins = await self._pos_mgr.settle_if_due(
                pos, resolved_market=ctx.market,
            )
            await self.spawn_close(
                asset, pos, ExitReason.SETTLE,
                live_trader=live_trader,
                ctx=ctx,
                settle_up_wins=up_wins,
            )

        elif reason == "invalid_tokens":
            await self.spawn_close(
                asset, pos, ExitReason.INVALID_POSITION,
                live_trader=live_trader,
                ctx=ctx,
            )

    # ── 平仓触发 ─────────────────────────────────────────────────────────

    async def spawn_close(
        self,
        asset: str,
        pos: OpenPosition,
        reason: str,
        live_trader,
        ctx: WindowContext,
        tp_trigger_exit_price: Optional[float] = None,
        settle_up_wins: Optional[bool] = None,
        partial_close_pct: Optional[float] = None,
    ) -> None:
        pos._pending_close = True
        ctx._pending_close = True
        asyncio.create_task(
            self._do_close(
                asset, pos, reason, live_trader, ctx,
                tp_trigger_exit_price=tp_trigger_exit_price,
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
        tp_trigger_exit_price: Optional[float],
        settle_up_wins: Optional[bool] = None,
        partial_close_pct: Optional[float] = None,
    ) -> None:

        def _cleanup():
            ctx.on_exit_complete()
            ctx._pending_close = False
            pos._pending_close = False
            pos._exit_retry_done = False
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

        # Bug-1 fix: settle 路径使用 settle_up_wins 计算 exit price。
        # UP 胜出 → UP token exit=1.0，DOWN token exit=0.0
        # DOWN 胜出 → UP token exit=0.0，DOWN token exit=1.0
        if reason == "settle" and settle_up_wins is not None:
            override_exit = 1.0 if settle_up_wins else 0.0
            logger.info(
                "EXIT settle override [{}]: up_wins={}, override_exit={}",
                asset, settle_up_wins, override_exit,
            )
        elif reason == "settle" and tp_trigger_exit_price is not None:
            # 兜底：若 settle_up_wins 不可用，用传入的 tp_trigger_exit_price
            override_exit = tp_trigger_exit_price
        elif reason == "invalid_tokens":
            override_exit = tp_trigger_exit_price

        tp_hit = getattr(pos, "tp_hit", None)
        try:
            await self._pos_mgr.close_position(
                pos,
                reason,
                override_exit_price=override_exit,
                live_trader=live_trader,
                resolved_market=resolved_mkt,
                tp_trigger_exit_price=tp_trigger_exit_price,
                settle_up_wins=settle_up_wins,
                partial_close_pct=partial_close_pct,
            )
            _cleanup()

            if tp_hit and reason in (
                ExitReason.TAKE_PROFIT,
                ExitReason.TAKE_PROFIT_2,
                ExitReason.STOP_LOSS,
                ExitReason.STOP_LOSS_EARLY,
                ExitReason.TRAILING_EXIT,
                ExitReason.SETTLE,
                ExitReason.INVALID_POSITION,
            ):
                pnl_recorded = getattr(pos, "_last_pnl_pct", None)
                gate = tp_hit.get("gate", "")
                entry_info = f"[entry_agree={pos.entry_core_agree} realtime_agree={tp_hit.get('realtime_agree','?')}]"
                if pnl_recorded is None:
                    pass
                elif pnl_recorded == 0.0:
                    logger.info(
                        "EXIT RESULT [{}] {} {} gate={} pnl=0.00% (保本) {}",
                        asset, tp_hit.get("kind", reason), entry_info, gate,
                        tp_hit.get("gate", ""),
                    )
                else:
                    slip = pnl_recorded - tp_hit.get("pnl_pct", 0)
                    logger.info(
                        "EXIT RESULT [{}] {} {} gate={} pnl={:+.2f}% "
                        "exit_price={} trigger_pnl={:.2f}% slip={:+.2f}% "
                        "atr_mult={:.2f} elapsed={:.0f}s",
                        asset,
                        tp_hit.get("kind", reason),
                        entry_info,
                        gate,
                        pnl_recorded,
                        tp_hit.get("exit_price", 0),
                        tp_hit.get("pnl_pct", 0),
                        slip,
                        tp_hit.get("atr_mult", tp_hit.get("atr_multiplier", 0)),
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
            max_retry = 3
            if current_retries >= max_retry:
                logger.error(
                    "EXIT [{}] sell exhausted ({} attempts) — force settle",
                    asset, current_retries,
                )
                try:
                    await self._pos_mgr.close_position(
                        pos, "settle_exhausted",
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
                    logger.error(
                        "EXIT [{}] settle_exhausted failed ({}): {}",
                        asset, type(inner_exc).__name__, inner_exc,
                    )
                    _cleanup()
                    return
            logger.warning(
                "EXIT [{}] sell failed ({}/{}): {}",
                asset, current_retries, max_retry, exc.reason,
            )
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
