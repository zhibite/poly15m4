"""
exit_controller.py — 退出控制器（单单持仓版，三重门信号辅助退出）

三重门逻辑：
  Gate 1（信号反向早平）：持仓方向与实时 Core 6 共识反向 ≥ 4/6 → 立即离场
  Gate 2（信号续持）：    TP 阈值触发但实时信号同向 ≥ 3/6 → 跳过止盈继续持仓
  Gate 3（信号减弱止盈）：盈利状态下信号减弱（< 3/6）→ 止盈退出

日志策略：
  - INFO：三重门任一命中时（续持 / 早平 / 信号减弱退出）
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
    atr_mult: float = 1.0         # 波动倍数（1.0~3.0），仅用于 SL 自适应
    # CVD 反向（P0-3: "最强单信号"，持仓期若 CVD 反向 Core 方向须立即离场）
    # CVD 反向（P0-3: "最强单信号"，持仓期若 CVD 反向 Core 方向须立即离场）
    cvd_reverse: bool = False     # CVD Z>1σ 且方向与持仓相反
    cvd_z: float = 0.0            # CVD Z-score
    # 动态阈值
    dynamic_tp: float = 0.0       # 止盈阈值（基于入场价的固定 %）
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


def _compute_realtime_signal(
    indicators: dict | None,
    pos_side: Side,
    mkt=None,
    *,
    entry_price: float = 0.5,        # 入场价（用于止盈阈值计算，必须稳定不变）
    tp_high_min: float = 30.0,       # 入场价 0.2 时 TP 上限（%）
    tp_high_max: float = 10.0,      # 入场价 0.6 时 TP 下限（%）
    tp_entry_min: float = 0.2,       # 映射区间下限
    tp_entry_max: float = 0.6,       # 映射区间上限
) -> RealtimeSignal:
    """
    根据传入的实时指标 dict，计算 Core 6 共识数量和方向。

    mkt: MarketInfo 实例（含 _price_history），用于计算概率 ATR。
         若为 None 则降级使用 Binance 美元 ATR（向后兼容）。
    entry_price: 入场时的 UP/DOWN 价格（用于止盈阈值计算，必须稳定不变）。
         与当前市场价无关，这样持仓期间 TP 目标不会随价格变化而漂移。
    tp_high_min / tp_high_max / tp_entry_min / tp_entry_max:
         入场价 → 止盈百分比的线性映射参数，由 sync_params 注入。
         入场价越低，止盈目标越高（如 0.2 → +30%，0.6 → +10%）。
    """
    # ── P0 Fix: 优先使用 Polymarket 概率 ATR，fallback 到 Binance 美元 ATR ──
    if mkt is not None:
        atr_val = _compute_atr_probability(mkt, lookback=6)
    else:
        atr_val = 0.02
        if indicators and "atr" in indicators:
            atr_val = float(indicators["atr"])

    atr_mult = min(1.0 + atr_val / 0.02, 2.0)

    # 动态止盈：基于入场价的线性插值
    # 入场价越低 → 止盈目标越高
    # 例：tp_entry_min=0.2 → tp_high_min=30%  |  tp_entry_max=0.6 → tp_high_max=10%
    # 固定使用 entry_price（入场价），避免持仓期间 TP 目标随当前价漂移
    entry_clamped = max(tp_entry_min, min(tp_entry_max, entry_price))
    t = (entry_clamped - tp_entry_min) / (tp_entry_max - tp_entry_min)  # 0→低价入, 1→高价入
    dyn_tp = tp_high_min + (tp_high_max - tp_high_min) * t             # 低价入→高TP, 高价入→低TP

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
            """
            Return True if the indicator signal aligns with the position direction,
            False if it opposes it, None if it is neutral.

            BBands signals in probability space:
              - upper_touch: upward momentum (price hitting/reaching upper band)
              - lower_touch: downward momentum (price hitting/falling through lower band)
            """
            sig = ind.get("signal", "")
            if pos_side == Side.UP:
                # UP-aligned: oversold bounce, golden cross, price above, upper touch.
                # UP-opposing: overbought, death cross, lower touch.
                if sig in ("oversold", "golden", "above", "upper_touch"):
                    return True
                if sig in ("overbought", "death", "lower_touch"):
                    return False
            else:  # Side.DOWN
                # DOWN-aligned: overbought drop, death cross, lower touch.
                # DOWN-opposing: oversold, golden cross, upper touch.
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
        dynamic_tp=dyn_tp,
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
        # TP 阈值参数：入场价 → 止盈映射（见 runtime_config TP_HIGH_MIN/MAX）
        self._tp_high_min: float = 30.0    # 入场价 0.2 时的 TP 上限
        self._tp_high_max: float = 10.0    # 入场价 0.6 时的 TP 下限
        self._tp_entry_min: float = 0.2    # 映射区间下限
        self._tp_entry_max: float = 0.6    # 映射区间上限

    def sync_params(self, cfg: dict) -> None:
        """从 runtime_config 同步退出阈值参数，支持运行时热更新"""
        self._tp_high_min = float(cfg.get("tp_high_min", 30.0))
        self._tp_high_max = float(cfg.get("tp_high_max", 10.0))
        self._tp_entry_min = float(cfg.get("tp_entry_min", 0.2))
        self._tp_entry_max = float(cfg.get("tp_entry_max", 0.6))

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

        # ── Gate 1a：Core 6 信号反向早平 ───────────────────────────────
        # ── Gate 1b：CVD 反向立即平仓（最强单信号，优先级高于 Core 反向）──
        cvd_reverse = sig.cvd_reverse
        cvd_z = sig.cvd_z

        if cvd_reverse:
            sig.gate1_triggered = True
            sig.gate1_action = "EXIT_NOW"
            sig.gate1_reason = (
                f"Gate1: CVD reverse strongest signal Z={cvd_z:.2f}, exit immediately"
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
                stop_loss_pct=-10.0,
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
                stop_loss_pct=-10.0,
            ), sig

        # ── Gate 2：TP 阈值到达时信号续持检查 ──────────────────────────────
        if delta_pct >= pos.tp_pct:
            if sig.agree_count >= 3:
                sig.gate2_triggered = True
                sig.gate2_action = "CONTINUE"
                sig.gate2_reason = (
                    f"Gate2: TP hit(+{delta_pct:.1f}%) but signal aligned {sig.agree_count}/6, "
                    f"skip TP (tp_pct={pos.tp_pct:.1f}%), continue holding"
                )
                return ExitSignal(
                    should_exit=False,
                    reason="GATE2_SKIP_TP",
                    reason_detail=sig.gate2_reason,
                    stop_loss_pct=-10.0,
                    take_profit_pct=pos.tp_pct,
                ), sig

        # ── Gate 3：信号减弱且已实现一定利润 → 退出 ─────────────────────────
        # 仅在盈利状态下触发，避免被动止损
        if sig.agree_count < 3 and delta_pct > 0:
            sig.gate3_triggered = True
            sig.gate3_action = "SIGNAL_WEAKEN_EXIT"
            sig.gate3_reason = (
                f"Gate3: signal weakened agree={sig.agree_count}/6<3, "
                f"pnl={delta_pct:.1f}% > 0"
            )
            sig.decision = "GATE3_EXIT"
            logger.info(
                "GATE3 EXIT [{}] {} agree={}/6 pnl={:.1f}% entry={:.4f} cur={:.4f}",
                pos.asset, pos_side.value, sig.agree_count, delta_pct,
                pos.entry_price, current_price,
            )
            return ExitSignal(
                should_exit=True,
                reason="GATE3_SIGNAL_WEAKEN",
                reason_detail=sig.gate3_reason,
                stop_loss_pct=-10.0,
                take_profit_pct=pos.tp_pct,
            ), sig

        return ExitSignal(
            should_exit=False,
            stop_loss_pct=-10.0,
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
        """
        三重门未触发时，执行标准止盈止损检查。

        止损逻辑（固定阈值 + 信号确认）：
          亏损 <= -20%   → 无条件立即止损
          亏损 > -20% 但 <= -10%：
            信号同向 >= 3 → 继续持有，等待反转盈利
            信号不足 < 3  → 全部止损退出

        止盈逻辑（单一阈值）：
          PnL >= pos.tp_pct → 检查信号
            信号同向 >= 3 → 跳过，继续持有
            信号不足     → 触发全部止盈
        """
        # ── 止损：-20% 无条件止损 ─────────────────────────────────────────────
        if pnl_pct <= -20.0:
            pos.tp_hit = {
                "kind": ExitReason.STOP_LOSS,
                "gate": "standard_sl",
                "elapsed_sec": round((now - pos.window_start).total_seconds(), 1),
                "exit_price": current_price,
                "pnl_pct": round(pnl_pct, 2),
                "realtime_agree": sig.agree_count,
                "entry_core_agree": pos.entry_core_agree,
            }
            logger.info(
                "EXIT CHECK [{}] {} STOP_LOSS: pnl={:.1f}% <= -20% — 无条件止损",
                pos.asset, pos.side.value, pnl_pct,
            )
            return ExitSignal(
                should_exit=True,
                reason="SL_FULL",
                reason_detail=f"亏损 {pnl_pct:.1f}% <= -20%，无条件止损",
                tp_hit_data=pos.tp_hit,
                stop_loss_pct=-20.0,
                take_profit_pct=pos.tp_pct,
            ), sig

        # ── 止损：-10% 触发，信号确认后可继续持有 ──────────────────────────────
        if pnl_pct <= -10.0:
            if sig.agree_count >= 3:
                logger.info(
                    "EXIT CHECK [{}] {} SL TRIGGER but HOLD (信号续持): "
                    "pnl={:.1f}% agree={}/6 — 等待反转盈利",
                    pos.asset, pos.side.value, pnl_pct, sig.agree_count,
                )
                return ExitSignal(
                    should_exit=False,
                    reason="SL_SKIP",
                    reason_detail=(
                        f"亏损 {pnl_pct:.1f}% > -20% 且信号同向 agree={sig.agree_count}/6>=3，"
                        f"继续持有等待反转"
                    ),
                    stop_loss_pct=-10.0,
                    take_profit_pct=pos.tp_pct,
                ), sig
            else:
                pos.tp_hit = {
                    "kind": ExitReason.STOP_LOSS,
                    "gate": "standard_sl",
                    "elapsed_sec": round((now - pos.window_start).total_seconds(), 1),
                    "exit_price": current_price,
                    "pnl_pct": round(pnl_pct, 2),
                    "realtime_agree": sig.agree_count,
                    "entry_core_agree": pos.entry_core_agree,
                }
                logger.info(
                    "EXIT CHECK [{}] {} STOP_LOSS: pnl={:.1f}% <= -10% "
                    "agree={}/6 < 3 — 止损退出",
                    pos.asset, pos.side.value, pnl_pct, sig.agree_count,
                )
                return ExitSignal(
                    should_exit=True,
                    reason="SL_FULL",
                    reason_detail=f"亏损 {pnl_pct:.1f}% > -20% 且信号不足 agree={sig.agree_count}/6<3，止损",
                    tp_hit_data=pos.tp_hit,
                    stop_loss_pct=-10.0,
                    take_profit_pct=pos.tp_pct,
                ), sig

        # ── 单一止盈阈值：达到 → 信号确认 → 触发或跳过 ──────────────────────
        if pnl_pct >= pos.tp_pct:
            if sig.agree_count >= 3:
                # 信号仍同向 → 跳过止盈，继续持有
                logger.info(
                    "EXIT CHECK [{}] {} TP hit but SKIP (信号续持): "
                    "pnl={:.1f}% tp={:.1f}% agree={}/6 — HOLD",
                    pos.asset, pos.side.value, pnl_pct, pos.tp_pct, sig.agree_count,
                )
                return ExitSignal(
                    should_exit=False,
                    reason="TP_SKIP",
                    reason_detail=(
                        f"止盈阈值 pnl={pnl_pct:.1f}%>={pos.tp_pct:.1f}% 但信号同向 "
                        f"agree={sig.agree_count}/6>=3，继续持有"
                    ),
                    stop_loss_pct=-10.0,
                    take_profit_pct=pos.tp_pct,
                ), sig

            # 信号不足 → 触发全部止盈 100%
            pos.tp_hit = {
                "kind": ExitReason.TAKE_PROFIT,
                "gate": "standard_tp",
                "elapsed_sec": round((now - pos.window_start).total_seconds(), 1),
                "exit_price": current_price,
                "pnl_pct": round(pnl_pct, 2),
                "tp_pct": pos.tp_pct,
                "atr_mult": sig.atr_mult,
                "entry_core_agree": pos.entry_core_agree,
                "realtime_agree": sig.agree_count,
            }
            logger.info(
                "EXIT CHECK [{}] {} TP hit: pnl={:.1f}% tp={:.1f}% "
                "atr_mult={:.2f} entry_agree={}/6 realtime_agree={}/6",
                pos.asset, pos.side.value, pnl_pct, pos.tp_pct,
                sig.atr_mult, pos.entry_core_agree, sig.agree_count,
            )
            return ExitSignal(
                should_exit=True,
                reason="TP",
                reason_detail=f"止盈触发 pnl={pnl_pct:.1f}%>={pos.tp_pct:.1f}%",
                tp_hit_data=pos.tp_hit,
                stop_loss_pct=-10.0,
                take_profit_pct=pos.tp_pct,
                partial_close_pct=1.0,
            ), sig

        return ExitSignal(
            should_exit=False,
            stop_loss_pct=-10.0,
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

        # 实时信号（Core 6 共识 + 止盈阈值）
        # 注意：dyn_tp 必须用入场价计算，不能用当前价（否则价格上涨后 TP 目标反而降低）
        sig = _compute_realtime_signal(
            indicators, pos.side, mkt,
            entry_price=pos.entry_price,
            tp_high_min=self._tp_high_min,
            tp_high_max=self._tp_high_max,
            tp_entry_min=self._tp_entry_min,
            tp_entry_max=self._tp_entry_max,
        )

        # ── P3-14 Fix: 每 tick 日志降为 DEBUG，避免日志淹没 ─────────────────
        # 记录到实例变量，供 strategy 层或 WebSocket 调用
        self._last_signal[pos.asset] = sig

        elapsed_s = round((now - pos.window_start).total_seconds(), 1)
        window_rem = round(max(0.0, (pos.window_end - now).total_seconds()), 1)

        logger.debug(
            "EXIT CHECK [{}] elapsed={:.0f}s rem={:.0f}s | "
            "entry={:.4f} cur={:.4f} pnl={:+.1f}% | "
            "sl=-10% tp={:.1f}% | "
            "atr={:.4f} atr_mult={:.2f} | "
            "agree={}/6({}) rev={}/6 cvd_rev={}",
            pos.asset,
            elapsed_s, window_rem,
            pos.entry_price, current_price,
            pnl_pct if pnl_pct else 0.0,
            pos.tp_pct,
            sig.atr_value, sig.atr_mult,
            sig.agree_count, sig.agree_dir,
            sig.reverse_count, sig.cvd_reverse,
        )

        if pnl_pct is None:
            return ExitSignal(
                stop_loss_pct=-10.0,
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
                stop_loss_pct=-10.0,
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
            stop_loss_pct=-10.0,
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

        elif reason == "GATE3_SIGNAL_WEAKEN":
            await self.spawn_close(
                asset, pos, ExitReason.GATE3_EXIT,
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
            pos.sell_retry_count = 0
            # NOTE: Do NOT reset _exit_retry_done here — it must stay True once set
            # so the retry branch can distinguish "first failure" from "already retried".

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
                ExitReason.GATE3_EXIT,
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
