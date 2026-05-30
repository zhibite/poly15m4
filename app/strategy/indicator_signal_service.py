"""
indicator_signal_service.py — 指标信号聚合服务

整合10个技术指标的信号输出，为入场决策提供统一的信号强度和过滤建议。
由 EntryController 和监控面板调用。

信号强度等级：
  0  — 无信号，不入场
  3  — 试探信号（3/6 Core），轻仓
  4  — 标准信号（4/6 Core），全仓
  5  — 极强信号（5/6 Core），1.5×仓
  6  — 完美信号（6/6 Core），2×仓

Advanced 过滤结果：
  - skip       ：CVD 背离或其他强反向信号，必须跳过
  - reduce     ：Funding 极端反向 / OBI 反向，建议减仓
  - confirm    ：Advanced 信号与 Core 同向，建议加仓
  - neutral    ：无特殊信号，正常执行
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Optional
import asyncio

from loguru import logger

# 指标计算函数（从 indicators.py 导入）
from app.api.indicators import (
    fetch_binance_klines,
    fetch_binance_funding,
    fetch_binance_oi,
    fetch_binance_obi,
    fetch_binance_cvd,
    _build_liquidation,
    _calc_rsi,
    _calc_macd_series,
    _calc_vwap,
    _calc_pivot,
    _calc_atr,
    _ema,
)


# ═══════════════════════════════════════════════════════════════════════════════
# 信号枚举
# ═══════════════════════════════════════════════════════════════════════════════

class SignalDir(Enum):
    UP   = "UP"
    DOWN = "DOWN"
    SKIP = "SKIP"    # 无方向信号（中性区 / 横盘）
    NONE = "NONE"    # 数据不足，无法判断


class FilterResult(Enum):
    CONFIRM = "confirm"   # Advanced 信号与 Core 同向，建议加仓
    NEUTRAL = "neutral"   # 无特殊信号，正常执行
    REDUCE  = "reduce"    # 建议减仓（Funding 极端反向 / OBI 反向）
    SKIP    = "skip"      # 必须跳过（CVD 背离或其他强反向信号）


# ═══════════════════════════════════════════════════════════════════════════════
# 信号结果
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class IndicatorSignal:
    """单个指标的信号"""
    name: str
    direction: SignalDir
    agree: bool          # 是否与 Core 趋势一致（用于统计共识）
    reason: str          # 信号原因说明
    raw_value: str       # 原始数值描述


@dataclass
class CoreSignals:
    """Core 指标信号汇总（现为 6 个）"""
    rsi:    IndicatorSignal
    macd:   IndicatorSignal
    vwap:   IndicatorSignal
    ema:    IndicatorSignal
    pivot:  IndicatorSignal
    bbands: IndicatorSignal

    @property
    def total_agree(self) -> int:
        return sum(1 for s in [self.rsi, self.macd, self.vwap, self.ema, self.pivot, self.bbands] if s.agree)

    @property
    def primary_direction(self) -> SignalDir:
        """多数方向（排除 SKIP）"""
        dirs = [s.direction for s in [self.rsi, self.macd, self.vwap, self.ema, self.pivot, self.bbands] if s.direction != SignalDir.SKIP]
        if not dirs:
            return SignalDir.SKIP
        up   = dirs.count(SignalDir.UP)
        down = dirs.count(SignalDir.DOWN)
        return SignalDir.UP if up >= down else SignalDir.DOWN


@dataclass
class AdvancedFilters:
    """Advanced 5 过滤结果"""
    funding:       IndicatorSignal
    cvd:           IndicatorSignal
    liquidation:   IndicatorSignal
    obi:           IndicatorSignal
    oi:            IndicatorSignal

    filter_result: FilterResult = FilterResult.NEUTRAL
    reduce_reason: str = ""           # 如果 REDUCE，原因是什么
    skip_reason:   str = ""           # 如果 SKIP，原因是什么
    confirm_note:  str = ""           # 如果 CONFIRM，原因是


@dataclass
class SignalResult:
    """
    完整的信号聚合结果。
    由 EntryController 和监控面板使用。
    """
    asset: str
    timestamp: datetime
    elapsed_sec: float           # 窗口已过去的时间（秒）
    window_duration_sec: int     # 窗口总时长（秒）
    window_end_sec: float         # 窗口剩余时间（秒）

    # 信号强度 0-6（对应 Core 6 同向信号数量）
    strength: int
    strength_label: str

    # 方向
    direction: SignalDir
    direction_label: str         # 中文描述

    # Core 6 详情
    core: CoreSignals

    # Advanced 5 过滤详情
    advanced: AdvancedFilters

    # 综合共识
    core_agree: int             # Core 指标中同意主方向的数量
    advanced_confirm: bool       # Advanced 5 是否确认主方向

    # 最终决策（由 EntryController 解读，service 只返回原始数据）
    suggested_position_pct: float = 1.0   # 旧字段，保留兼容性
    suggested_action: str = "ENTER"        # ENTER 或 SKIP，由 entry_controller 决定仓位
    skip_reason: str = ""                  # 跳过原因（Core 不够 / CVD 反向 / 等）

    # 当前市场数据
    up_mid:   float = 0.5
    down_mid: float = 0.5
    spot_price: float = 0.0
    atr: float = 0.0
    atr_dollar: float = 0.0        # ATR 的美元价值

    # 价格区间（锚点偏离）
    anchor_deviation_pct: float = 0.0   # 当前价偏离锚点的百分比

    # 原始指标数据
    raw_rsi: float = 50.0
    raw_funding: float = 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# 全局常量
# ═══════════════════════════════════════════════════════════════════════════════

# ATR 阈值（低于此值视为横盘，MACD 信号不可靠）
# 比例 = 0.04% of current price → BTC≈$30 ETH≈$0.8（文档针对 BTC≈$30）
ATR_DOLLAR_FLAT_RATIO: float = 0.0004

# Funding 阈值（Binance 永续）
FUNDING_BEARISH_THRESHOLD:  float = 0.0005   # > +0.05% → 做 DOWN
FUNDING_BULLISH_THRESHOLD:  float = -0.0005  # < -0.05% → 做 UP
# Funding 极端 REDUCE 阈值（覆盖 0.08%~0.1% 区间）
FUNDING_EXTREME_REDUCE_THRESHOLD: float = 0.0008   # > +0.08%

# OI 变化阈值
OI_CHANGE_SIGNIFICANT: float = 0.05   # 5% 变化视为显著

# 布林带参数
BBANDS_PERIOD: int = 20        # 中轨周期（SMA）
BBANDS_STD:    float = 2.0    # 标准差倍数
BBANDS_TOUCH_PCT: float = 0.0005  # 触及阈值（0.05%，上下轨附近即触发，阈值对称）


def _sec_remaining(sec: float) -> str:
    return f"{sec/60:.1f}min" if sec >= 60 else f"{sec:.0f}s"


# ═══════════════════════════════════════════════════════════════════════════════
# 全局滚动历史（模块级别，跨实例共享，支持多币种）
# ═══════════════════════════════════════════════════════════════════════════════

_RSI_HISTORY: dict[str, list[float]] = {}
_MAX_RSI_HISTORY: int = 30

_OI_HISTORY: dict[str, list[float]] = {}
_MAX_OI_HISTORY: int = 30

_CVD_BASELINE: dict[str, list[float]] = {}
_MAX_CVD_BASELINE: int = 10
_CVD_BASELINE_LOCK = asyncio.Lock()


# ═══════════════════════════════════════════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════════════════════════════════════════

def _calc_bbands(closes: list[float], period: int = BBANDS_PERIOD, std_mult: float = BBANDS_STD) -> tuple[float, float, float]:
    """计算布林带：返回 (中轨, 上轨, 下轨)"""
    if len(closes) < period:
        return 0.0, 0.0, 0.0
    recent = closes[-period:]
    mid = sum(recent) / period
    variance = sum((x - mid) ** 2 for x in recent) / period
    std = variance ** 0.5
    return mid, mid + std_mult * std, mid - std_mult * std


# ═══════════════════════════════════════════════════════════════════════════════
# 主服务类
# ═══════════════════════════════════════════════════════════════════════════════

class IndicatorSignalService:
    """
    指标信号聚合服务。

    使用方式：
        svc = IndicatorSignalService(asset="BTC")
        result = await svc.get_signal(
            up_mid=0.45,
            down_mid=0.55,
            window_open_price=105000.0,
            window_start=datetime.now(timezone.utc),
            window_duration_sec=900,
        )
    """

    def __init__(self, asset: str = "BTC"):
        self.asset = asset.upper()
        self.symbol_spot = f"{self.asset}USDT"

    def _rsi_hist(self) -> list[float]:
        return _RSI_HISTORY.setdefault(self.asset, [])

    def _oi_hist(self) -> list[float]:
        return _OI_HISTORY.setdefault(self.asset, [])

    def _cvd_baseline(self) -> list[float]:
        return _CVD_BASELINE.setdefault(self.asset, [])

    # ── RSI 计算（带极端反转检测）─────────────────────────────────────────────
    def _calc_rsi_with_extremes(
        self,
        closes: list[float],
        prev_rsi: float = 50.0,
    ) -> tuple[float, bool, bool]:
        if not closes:
            return 50.0, False, False
        rsi_val = _calc_rsi(closes)
        crossed_down = (prev_rsi >= 70.0) and (rsi_val < 70.0)
        crossed_up = (prev_rsi <= 30.0) and (rsi_val > 30.0)
        return rsi_val, crossed_down, crossed_up

    # ── RSI 背离检测 ────────────────────────────────────────────────────────
    # 价格创新高但 RSI 高点下降 → 做 DOWN
    # 价格创新低但 RSI 低点上升 → 做 UP
    def _check_rsi_divergence(
        self,
        closes: list[float],
        highs: list[float],
        lows: list[float],
    ) -> tuple[bool, SignalDir, str]:
        if len(closes) < 20 or not highs or not lows:
            return False, SignalDir.SKIP, ""
        lookback = min(20, len(closes))
        # All three slices must cover the same lookback window — use the shortest series
        # to ensure closes[i], highs[i], lows[i] all belong to the same bar.
        n = min(lookback, len(closes), len(highs), len(lows))
        price_slice = closes[-n:]
        high_slice = highs[-n:]
        low_slice = lows[-n:]
        if n < 20:
            return False, SignalDir.SKIP, ""

        # Compute RSI series for the lookback window
        rsi_vals = [_calc_rsi(price_slice[:i+1]) for i in range(len(price_slice))]
        if not rsi_vals:
            return False, SignalDir.SKIP, ""

        # Find LAST occurrence so we correctly detect if the last bar is the new extreme
        rsi_lookback = rsi_vals[:n]
        rsi_last_max_pos = len(rsi_lookback) - 1 - rsi_lookback[::-1].index(max(rsi_lookback))
        rsi_new_high = (rsi_last_max_pos == n - 1)
        rsi_last_min_pos = len(rsi_lookback) - 1 - rsi_lookback[::-1].index(min(rsi_lookback))
        rsi_new_low = (rsi_last_min_pos == n - 1)

        price_last_max_pos = len(price_slice) - 1 - price_slice[::-1].index(max(price_slice))
        price_new_high = (price_last_max_pos == n - 1)
        price_last_min_pos = len(price_slice) - 1 - price_slice[::-1].index(min(price_slice))
        price_new_low = (price_last_min_pos == n - 1)

        if price_new_high and rsi_new_high and len(rsi_vals) >= 2:
            prev_rsi_idx = max(0, n - 2)
            if rsi_vals[prev_rsi_idx] > rsi_vals[-1]:
                return True, SignalDir.DOWN, "RSI 看跌背离（价格新高，RSI 高点下降），做 DOWN"
        if price_new_low and rsi_new_low and len(rsi_vals) >= 2:
            prev_rsi_idx = max(0, n - 2)
            if rsi_vals[prev_rsi_idx] < rsi_vals[-1]:
                return True, SignalDir.UP, "RSI 看涨背离（价格新低，RSI 低点上升），做 UP"
        return False, SignalDir.SKIP, ""

    # ── Core 6 指标信号计算 ───────────────────────────────────────────────
    def _calc_core_signals(
        self,
        closes: list[float],
        highs: list[float],
        lows: list[float],
        volumes: list[float],
        up_mid: float,
        down_mid: float,
        window_open_price: float,
        elapsed_sec: float,
        window_duration_sec: int,
    ) -> CoreSignals:
        current_price = closes[-1] if closes else window_open_price
        atr_val = _calc_atr(highs, lows, closes) if highs else 0.0
        atr_dollar = atr_val  # BTC 以美元计价

        # ── RSI（带极端反转 + 背离检测）─────────────────────────────
        prev_rsi = self._rsi_hist()[-1] if self._rsi_hist() else 50.0
        rsi_val, crossed_down, crossed_up = self._calc_rsi_with_extremes(closes, prev_rsi)
        self._rsi_hist().append(rsi_val)
        if len(self._rsi_hist()) > _MAX_RSI_HISTORY:
            self._rsi_hist().pop(0)

        rsi_dir = SignalDir.SKIP
        rsi_agree = False
        rsi_reason = ""

        if crossed_down:
            rsi_dir = SignalDir.DOWN
            rsi_agree = True
            rsi_reason = f"RSI 从 {prev_rsi:.1f} 回落跌破 70，做 DOWN"
        elif crossed_up:
            rsi_dir = SignalDir.UP
            rsi_agree = True
            rsi_reason = f"RSI 从 {prev_rsi:.1f} 反弹突破 30，做 UP"
        else:
            # 背离检测：优先级最高（指南标注为"最强信号"）
            has_div, div_dir, div_reason = self._check_rsi_divergence(closes, highs, lows)
            if has_div:
                rsi_dir = div_dir
                rsi_agree = True
                rsi_reason = div_reason
            elif rsi_val >= 70.0:
                rsi_dir = SignalDir.SKIP
                rsi_reason = f"RSI={rsi_val:.1f} 处于超买区，等回落"
            elif rsi_val <= 30.0:
                rsi_dir = SignalDir.SKIP
                rsi_reason = f"RSI={rsi_val:.1f} 处于超卖区，等反弹"
            elif 45.0 <= rsi_val <= 55.0:
                rsi_dir = SignalDir.SKIP
                rsi_reason = f"RSI={rsi_val:.1f} 中性区(45-55)，跳过"
            elif rsi_val > 55.0:
                rsi_dir = SignalDir.UP
                rsi_agree = False  # 未确认，不计入 Core 共识
                rsi_reason = f"RSI={rsi_val:.1f} 偏多(55-70)，等反转确认"
            elif rsi_val < 45.0:
                rsi_dir = SignalDir.DOWN
                rsi_agree = False  # 未确认，不计入 Core 共识
                rsi_reason = f"RSI={rsi_val:.1f} 偏空(30-45)，等反转确认"

        # ── MACD ───────────────────────────────────────────────────
        macd_line, signal, macd_hist = (0.0, 0.0, [])
        if closes and len(closes) >= 26:
            macd_line, signal, macd_hist = _calc_macd_series(closes)

        macd_dir = SignalDir.SKIP
        macd_agree = False
        macd_reason = ""

        atr_flat_threshold = ATR_DOLLAR_FLAT_RATIO * current_price
        if atr_dollar < atr_flat_threshold:
            macd_dir = SignalDir.SKIP
            macd_reason = f"ATR={atr_dollar:.1f}<${atr_flat_threshold:.1f}({ATR_DOLLAR_FLAT_RATIO*100:.3f}%价格)，横盘忽略MACD"
        elif macd_hist:
            last_h = macd_hist[-1]
            prev_h = macd_hist[-2] if len(macd_hist) >= 2 else 0.0
            if last_h > 0 and last_h > prev_h:
                macd_dir = SignalDir.UP
                macd_agree = True
                macd_reason = "MACD 柱状图绿色放大，动能加速"
            elif last_h > 0 and last_h <= prev_h:
                macd_dir = SignalDir.SKIP
                macd_reason = "MACD 柱状图绿色但收缩，动能衰竭"
            elif last_h < 0 and last_h < prev_h:
                macd_dir = SignalDir.DOWN
                macd_agree = True
                macd_reason = "MACD 柱状图红色放大，动能加速"
            elif last_h < 0 and last_h >= prev_h:
                macd_dir = SignalDir.SKIP
                macd_reason = "MACD 柱状图红色但收缩，动能衰竭"
            else:
                macd_dir = SignalDir.SKIP
                macd_reason = "MACD 柱状图向零轴收缩，横盘预警"
        else:
            macd_reason = "MACD 数据不足"

        # ── VWAP ──────────────────────────────────────────────────
        # 对齐 trading guide：VWAP 需要 RSI/MACD 组合确认，才算 agree
        vwap_val = _calc_vwap(closes, volumes) if closes and volumes else current_price
        vwap_dir = SignalDir.SKIP
        vwap_agree = False
        vwap_reason = ""

        if current_price > vwap_val * 1.001:
            if rsi_val > 50 and macd_agree and macd_dir == SignalDir.UP:
                vwap_dir = SignalDir.UP
                vwap_agree = True
                vwap_reason = "价格>VWAP + RSI>50 + MACD UP，趋势延续"
            elif rsi_val > 70:
                vwap_dir = SignalDir.DOWN
                vwap_agree = False
                vwap_reason = "价格远>VWAP + RSI>70，均值回归"
            elif rsi_val > 50 or macd_agree:
                vwap_dir = SignalDir.UP
                vwap_agree = False
                vwap_reason = "价格在 VWAP 上方，局部偏多，无强确认"
            else:
                vwap_dir = SignalDir.SKIP
                vwap_reason = "价格在 VWAP 上方，但无 RSI/MACD 确认，跳过"
        elif current_price < vwap_val * 0.999:
            if rsi_val < 50 and macd_agree and macd_dir == SignalDir.DOWN:
                vwap_dir = SignalDir.DOWN
                vwap_agree = True
                vwap_reason = "价格<VWAP + RSI<50 + MACD DOWN，趋势延续"
            elif rsi_val < 30:
                vwap_dir = SignalDir.UP
                vwap_agree = False
                vwap_reason = "价格远<VWAP + RSI<30，均值回归"
            elif rsi_val < 50 or macd_agree:
                vwap_dir = SignalDir.DOWN
                vwap_agree = False
                vwap_reason = "价格在 VWAP 下方，局部偏空，无强确认"
            else:
                vwap_dir = SignalDir.SKIP
                vwap_reason = "价格在 VWAP 下方，但无 RSI/MACD 确认，跳过"
        else:
            vwap_dir = SignalDir.SKIP
            vwap_reason = "价格≈VWAP，无明显方向"

        # ── EMA 9/21 ─────────────────────────────────────────────
        # Use the cached series to detect crossovers consistently with get_indicators.
        ema9_full  = self._calc_ema_series(closes, 9)
        ema21_full = self._calc_ema_series(closes, 21)
        ema9_val  = ema9_full[-1]  if closes else current_price
        ema21_val = ema21_full[-1] if closes else current_price
        ema_dir   = SignalDir.SKIP
        ema_agree = False
        ema_reason = ""

        if len(closes) >= 22:
            # Compare last two bars of the cached series to detect a fresh crossover.
            e9p_prev  = ema9_full[-2]
            e21p_prev = ema21_full[-2]
            cross_happened_now = (
                (e9p_prev <= e21p_prev and ema9_val > ema21_val)
                or (e9p_prev >= e21p_prev and ema9_val < ema21_val)
            )
            if cross_happened_now:
                if ema9_val > ema21_val:
                    ema_dir = SignalDir.UP
                    ema_agree = True
                    ema_reason = "EMA 金叉"
                else:
                    ema_dir = SignalDir.DOWN
                    ema_agree = True
                    ema_reason = "EMA 死叉"
            else:
                # 无交叉：给方向但 ema_agree=False（不计入 Core 共识）
                if ema9_val > ema21_val:
                    ema_dir = SignalDir.UP
                    ema_agree = False
                    ema_reason = "EMA 多头排列（无交叉）"
                else:
                    ema_dir = SignalDir.DOWN
                    ema_agree = False
                    ema_reason = "EMA 空头排列（无交叉）"
        else:
            ema_reason = f"数据不足（{len(closes)}根K线），需≥22根"

        # ── Bollinger Bands（均值回归）────────────────────────────
        bb_mid, bb_upper, bb_lower = _calc_bbands(closes) if closes else (0.0, 0.0, 0.0)
        bb_dir   = SignalDir.SKIP
        bb_agree = False
        bb_reason = ""

        if bb_upper > 0 and bb_lower > 0:
            # Upper band: price touching/reaching upper band → momentum UP (ride the move)
            # Lower band: price touching/falling through lower band → momentum DOWN (ride the move)
            # Use symmetric threshold: upper uses * (1 - pct), lower uses * (1 + pct)
            if current_price >= bb_upper * (1 - BBANDS_TOUCH_PCT):
                bb_dir = SignalDir.UP
                bb_agree = True
                bb_reason = "价格触及/突破布林上轨，动量顺势做 UP"
            elif current_price <= bb_lower * (1 + BBANDS_TOUCH_PCT):
                bb_dir = SignalDir.DOWN
                bb_agree = True
                bb_reason = "价格触及/跌破布林下轨，动量顺势做 DOWN"
            else:
                bb_reason = f"价格在布林带内震荡（M={bb_mid:.2f} U={bb_upper:.2f} L={bb_lower:.2f}）"
        else:
            bb_reason = "布林带数据不足"

        # ── Pivot ────────────────────────────────────────────────
        pivot = _calc_pivot(closes) if closes else {
            "high": current_price * 1.01,
            "low": current_price * 0.99,
        }
        piv_dir   = SignalDir.SKIP
        piv_agree = False
        piv_reason = ""

        if current_price > pivot["high"] * 0.9995:
            piv_dir = SignalDir.UP
            piv_agree = True
            piv_reason = "价格突破 HL10 高点，看涨"
        elif current_price < pivot["low"] * 1.0005:
            piv_dir = SignalDir.DOWN
            piv_agree = True
            piv_reason = "价格跌破 HL10 低点，看跌"
        elif pivot["low"] < current_price < pivot["high"]:
            piv_dir = SignalDir.SKIP
            piv_reason = "价格在 HL10 中间区，震荡，跳过"
        else:
            piv_reason = "Pivot 数据不足"

        return CoreSignals(
            rsi=IndicatorSignal(name="RSI", direction=rsi_dir, agree=rsi_agree,
                               reason=rsi_reason, raw_value=f"{rsi_val:.1f}"),
            macd=IndicatorSignal(name="MACD", direction=macd_dir, agree=macd_agree,
                                reason=macd_reason,
                                raw_value=f"hist={macd_hist[-1]:.6f}" if macd_hist else "N/A"),
            vwap=IndicatorSignal(name="VWAP", direction=vwap_dir, agree=vwap_agree,
                               reason=vwap_reason, raw_value=f"{vwap_val:.2f}"),
            ema=IndicatorSignal(name="EMA9/21", direction=ema_dir, agree=ema_agree,
                              reason=ema_reason, raw_value=f"EMA9={ema9_val:.2f} EMA21={ema21_val:.2f}"),
            pivot=IndicatorSignal(name="Pivot", direction=piv_dir, agree=piv_agree,
                                reason=piv_reason,
                                raw_value=f"H={pivot['high']:.2f} L={pivot['low']:.2f}"),
            bbands=IndicatorSignal(name="BBands", direction=bb_dir, agree=bb_agree,
                                  reason=bb_reason,
                                  raw_value=f"M={bb_mid:.2f} U={bb_upper:.2f} L={bb_lower:.2f}"),
        )

    # ── Advanced 5 过滤计算 ───────────────────────────────────────────────
    # P3-13 Fix: 改为实例方法，从自身 OI 历史直接计算 oi_change，
    # 避免与 get_indicators 中的计算路径不一致。
    def _calc_advanced_filters(
        self,
        funding_val: float,
        cvd_val: float,
        cvd_z: float,
        obi_val: float,
        liq: dict,
        current_price: float,
        primary_dir: SignalDir,
        closes: list[float],
    ) -> AdvancedFilters:
        # ── Funding（P0-4 Fix: 正值 = 多头支付空头 = 看涨信号）────────
        # Binance 永续资金费率：
        #   正值 = long pays short（多头给空头钱）= 做多情绪占主导 → UP
        #   负值 = short pays long（空头给多头钱）= 做空情绪占主导 → DOWN
        if funding_val > FUNDING_BEARISH_THRESHOLD:
            fund_dir = SignalDir.UP
            fund_reason = f"资金费率={funding_val*100:.3f}%>+0.05%，多头给空头，趋势延续，做UP"
            fund_agree = (primary_dir == SignalDir.UP)
        elif funding_val < FUNDING_BULLISH_THRESHOLD:
            fund_dir = SignalDir.DOWN
            fund_reason = f"资金费率={funding_val*100:.3f}%<-0.05%，空头给多头，趋势延续，做DOWN"
            fund_agree = (primary_dir == SignalDir.DOWN)
        else:
            fund_dir = SignalDir.SKIP
            fund_reason = f"资金费率={funding_val*100:.3f}%，中性"
            fund_agree = False

        # ── CVD ───────────────────────────────────────────────
        cvd_dir = SignalDir.SKIP
        cvd_agree = False
        cvd_reason = ""

        if cvd_z > 1.0:
            if cvd_val > 0:
                cvd_dir = SignalDir.UP
                cvd_agree = (primary_dir == SignalDir.UP)
                cvd_reason = f"CVD 累计买入，Z={cvd_z:.1f}>1σ，吸筹"
            else:
                cvd_dir = SignalDir.DOWN
                cvd_agree = (primary_dir == SignalDir.DOWN)
                cvd_reason = f"CVD 累计卖出，Z={cvd_z:.1f}>1σ，派发"
        else:
            cvd_dir = SignalDir.SKIP
            cvd_reason = f"CVD 无明显信号（Z={cvd_z:.1f}<1σ）"

        # ── Liquidation ───────────────────────────────────────
        liq_dir = SignalDir.SKIP
        liq_agree = False
        liq_reason = ""
        liq_signal = liq.get("signal", "neutral")

        if liq_signal == "up_magnet":
            liq_dir = SignalDir.UP
            liq_agree = (primary_dir == SignalDir.UP)
            liq_reason = "空头清算密集区在上方，价格被向上吸"
        elif liq_signal == "down_magnet":
            liq_dir = SignalDir.DOWN
            liq_agree = (primary_dir == SignalDir.DOWN)
            liq_reason = "多头清算密集区在下方，价格被向下吸"
        else:
            liq_reason = "清算热力图中性"

        # ── OBI ──────────────────────────────────────────────
        obi_dir = SignalDir.SKIP
        obi_agree = False
        obi_reason = ""

        if obi_val > 1.5:
            obi_dir = SignalDir.UP
            obi_agree = (primary_dir == SignalDir.UP)
            obi_reason = f"OBI={obi_val:.3f}>1.5，买方深度占优"
        elif obi_val < 0.67:
            obi_dir = SignalDir.DOWN
            obi_agree = (primary_dir == SignalDir.DOWN)
            obi_reason = f"OBI={obi_val:.3f}<0.67，卖方深度占优"
        else:
            obi_reason = f"OBI={obi_val:.3f}，订单簿均衡"

        # ── OI（P3-13 Fix: 直接从自身历史计算 OI 变化率）────────────────
        # 与 get_indicators 使用完全相同的计算路径，避免路径不一致
        oi_dir = SignalDir.SKIP
        oi_agree = False
        oi_reason = ""
        oi_h = self._oi_hist()
        oi_change = 0.0
        if len(oi_h) >= 2 and oi_h[-2] > 0:
            oi_change = (oi_h[-1] - oi_h[-2]) / oi_h[-2]

        if len(closes) >= 2 and abs(oi_change) >= OI_CHANGE_SIGNIFICANT:
            price_up = closes[-1] > closes[-2]
            oi_up    = oi_change > 0
            if price_up and oi_up:
                oi_dir = SignalDir.UP
                oi_agree = (primary_dir == SignalDir.UP)
                oi_reason = f"OI 上涨+价格上涨，新多入场，趋势延续"
            elif price_up and not oi_up:
                oi_dir = SignalDir.DOWN
                oi_reason = f"OI 下跌+价格上涨，空头平仓，动力不足"
                oi_agree = False
            elif not price_up and oi_up:
                oi_dir = SignalDir.DOWN
                oi_agree = (primary_dir == SignalDir.DOWN)
                oi_reason = f"OI 上涨+价格下跌，新空入场，趋势延续"
            else:
                oi_dir = SignalDir.UP
                oi_reason = f"OI 下跌+价格下跌，多头投降，可能反弹"
                oi_agree = (primary_dir == SignalDir.UP)
        else:
            oi_reason = f"OI 变化={oi_change*100:.2f}%，不显著"

        # ── 汇总过滤结果 ─────────────────────────────────────────
        filter_result = FilterResult.NEUTRAL
        skip_reason   = ""
        reduce_reason = ""
        confirm_note  = ""

        # CVD 反向背离 = 必须跳过（最强单信号）
        if cvd_dir != SignalDir.SKIP and cvd_dir != primary_dir and primary_dir != SignalDir.SKIP:
            filter_result = FilterResult.SKIP
            skip_reason = f"CVD 信号反向（{cvd_reason}），跳过"
        elif abs(funding_val) >= FUNDING_EXTREME_REDUCE_THRESHOLD:
            # Funding 极端（>0.08%），反向信号强，建议减仓
            filter_result = FilterResult.REDUCE
            reduce_reason = f"Funding 极端（{funding_val*100:.3f}%），建议减仓"
        elif obi_dir != SignalDir.SKIP and obi_dir != primary_dir and primary_dir != SignalDir.SKIP:
            # OBI 反向，建议减仓
            filter_result = FilterResult.REDUCE
            reduce_reason = f"OBI 信号反向（{obi_reason}），建议减仓"
        elif oi_agree or liq_agree or cvd_agree:
            # Advanced 同向确认
            filter_result = FilterResult.CONFIRM
            confirm_parts = []
            if oi_agree: confirm_parts.append("OI")
            if liq_agree: confirm_parts.append("Liquidation")
            if cvd_agree: confirm_parts.append("CVD")
            confirm_note = f"Advanced 确认：{', '.join(confirm_parts)}"

        return AdvancedFilters(
            funding=IndicatorSignal(name="Funding", direction=fund_dir, agree=fund_agree,
                                  reason=fund_reason, raw_value=f"{funding_val*100:.4f}%"),
            cvd=IndicatorSignal(name="CVD", direction=cvd_dir, agree=cvd_agree,
                               reason=cvd_reason, raw_value=f"{cvd_val:.2f} (Z={cvd_z:.2f})"),
            liquidation=IndicatorSignal(name="Liquidation", direction=liq_dir, agree=liq_agree,
                                     reason=liq_reason, raw_value=liq_signal),
            obi=IndicatorSignal(name="OBI", direction=obi_dir, agree=obi_agree,
                               reason=obi_reason, raw_value=f"{obi_val:.3f}"),
            oi=IndicatorSignal(name="OI", direction=oi_dir, agree=oi_agree,
                              reason=oi_reason, raw_value=f"Δ={oi_change*100:.2f}%"),
            filter_result=filter_result,
            reduce_reason=reduce_reason,
            skip_reason=skip_reason,
            confirm_note=confirm_note,
        )

    # ── CVD Z-score ────────────────────────────────────────────────
    async def _cvd_z_score(self, abs_cvd: float) -> float:
        async with _CVD_BASELINE_LOCK:
            baseline = self._cvd_baseline()
            baseline.append(abs_cvd)
            if len(baseline) > _MAX_CVD_BASELINE:
                baseline.pop(0)
            if len(baseline) < 3:
                return 0.0
            mean = sum(baseline) / len(baseline)
            std = (sum((x - mean) ** 2 for x in baseline) / max(len(baseline) - 1, 1)) ** 0.5
            return (abs_cvd - mean) / std if std > 0 else 0.0

    def _cvd_z_score_ro(self, abs_cvd: float) -> float:
        """
        Read-only CVD Z-score — reads the current rolling baseline without modifying it.
        Used by get_indicators() (WS path) where the baseline is already populated
        by prior get_signal() calls; we avoid re-appending here to prevent double-counting.
        """
        baseline = _CVD_BASELINE.get(self.asset, [])
        if len(baseline) < 3:
            return 0.0
        mean = sum(baseline) / len(baseline)
        std = (sum((x - mean) ** 2 for x in baseline) / max(len(baseline) - 1, 1)) ** 0.5
        return (abs_cvd - mean) / std if std > 0 else 0.0

    def _calc_ema_series(self, closes: list[float], period: int) -> list[float]:
        """
        Compute EMA series in O(n) time using the standard recursive formula.
        Returns the EMA value at each index (with warm-up period filled as 0.0).
        """
        if len(closes) < period:
            return [0.0] * len(closes)
        k = 2.0 / (period + 1)
        result: list[float] = [0.0] * (period - 1)
        # Seed with SMA of first `period` bars
        seed = sum(closes[:period]) / period
        result.append(seed)
        for i in range(period, len(closes)):
            val = (closes[i] - result[-1]) * k + result[-1]
            result.append(val)
        return result

    # ── 并发获取 ────────────────────────────────────────────────
    @staticmethod
    async def _gather(*coros, timeout_sec: float = 8.0):
        """
        并发执行多个协程，单个超时控制。
        timeout_sec 内未完成的协程被静默跳过（返回 None）。
        兼容 Python 3.9+（不使用 asyncio.timeout，需要 3.11+）。
        """
        async def _with_timeout(coro, timeout):
            try:
                return await asyncio.wait_for(coro, timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning("Indicator fetch timed out after {:.1f}s", timeout)
                return None
            except Exception:
                return None

        tasks = [_with_timeout(c, timeout_sec) for c in coros]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return tuple(r if not isinstance(r, Exception) else None for r in results)

    # ── 主入口 ────────────────────────────────────────────────
    async def get_signal(
        self,
        up_mid: float,
        down_mid: float,
        window_open_price: float,
        window_start: datetime,
        window_duration_sec: int = 900,
    ) -> SignalResult:
        now = datetime.now(timezone.utc)
        elapsed_sec = (now - window_start).total_seconds()
        remaining_sec = max(0.0, window_duration_sec - elapsed_sec)

        # ── 并发获取所有数据 ─────────────────────────────────────────
        klines_fetch = fetch_binance_klines(self.symbol_spot)
        funding_fetch = fetch_binance_funding(self.symbol_spot)
        oi_fetch = fetch_binance_oi(self.symbol_spot)
        obi_fetch = fetch_binance_obi(self.symbol_spot)
        cvd_fetch = fetch_binance_cvd(self.symbol_spot)

        klines, funding_val, oi_val, obi_data, cvd_val = await self._gather(
            klines_fetch, funding_fetch, oi_fetch, obi_fetch, cvd_fetch,
            timeout_sec=8.0,
        )

        closes   = klines.get("closes", [])   if klines else []
        highs    = klines.get("highs", [])     if klines else []
        lows     = klines.get("lows", [])      if klines else []
        volumes  = klines.get("volumes", [])     if klines else []
        current_price = closes[-1] if closes else window_open_price

        # ── ATR ───────────────────────────────────────────────
        atr_val = _calc_atr(highs, lows, closes) if highs else 0.0

        # ── CVD Z-score ──────────────────────────────────────
        cvd_z = 0.0
        if cvd_val is not None:
            cvd_z = await self._cvd_z_score(abs(cvd_val))

        # ── Liquidation ────────────────────────────────────
        liq = _build_liquidation(self.asset, current_price)

        # ── OI 变化率（使用滚动历史计算）──────────────────────────
        oi_change = 0.0
        if oi_val and oi_val > 0:
            oi_h = self._oi_hist()
            oi_h.append(float(oi_val))
            if len(oi_h) > _MAX_OI_HISTORY:
                oi_h.pop(0)
            if len(oi_h) >= 2:
                prev_oi = oi_h[-2]
                oi_change = (float(oi_val) - prev_oi) / prev_oi if prev_oi > 0 else 0.0

        # ── 计算 Core 6 ─────────────────────────────────────
        core = self._calc_core_signals(
            closes=closes,
            highs=highs,
            lows=lows,
            volumes=volumes,
            up_mid=up_mid,
            down_mid=down_mid,
            window_open_price=window_open_price,
            elapsed_sec=elapsed_sec,
            window_duration_sec=window_duration_sec,
        )

        # ── 计算 Advanced 5 ────────────────────────────────
        # P3-13 Fix: oi_change 由 _calc_advanced_filters 内部直接读取历史计算，不再传入
        primary_dir = core.primary_direction
        advanced = self._calc_advanced_filters(
            funding_val=funding_val or 0.0,
            cvd_val=cvd_val or 0.0,
            cvd_z=cvd_z,
            obi_val=obi_data.get("value", 1.0) if obi_data else 1.0,
            liq=liq,
            current_price=current_price,
            primary_dir=primary_dir,
            closes=closes,
        )

        # ── 价格区间过滤 ─────────────────────────────────
        anchor_deviation = 0.0
        if window_open_price > 0:
            anchor_deviation = (current_price - window_open_price) / window_open_price

        # ── 信号强度与仓位建议 ───────────────────────────────
        core_agree = core.total_agree
        advanced_confirm = advanced.filter_result == FilterResult.CONFIRM

        strength = core_agree
        if advanced.filter_result == FilterResult.SKIP:
            strength = 0

        if strength == 0:
            strength_label = "无信号（必须跳过）"
        elif core_agree >= 5 and advanced_confirm:
            strength_label = "完美信号（5/6 Core，全确认，最大仓）"
        elif core_agree >= 6:
            strength_label = "完美信号（6/6 Core，2×仓）"
        elif core_agree >= 5:
            strength_label = "极强信号（5/6 Core，1.5×仓）"
        elif core_agree >= 4:
            strength_label = "标准信号（4/6 Core，1.0×仓）"
        else:
            strength_label = "试探信号（3/6 Core，0.5×仓）"

        # ── 最终方向 ───────────────────────────────────────
        if advanced.filter_result == FilterResult.SKIP:
            final_dir = SignalDir.SKIP
        else:
            final_dir = primary_dir

        dir_labels = {
            SignalDir.UP:   "做 UP（看涨）",
            SignalDir.DOWN: "做 DOWN（看跌）",
            SignalDir.SKIP: "跳过（无信号）",
            SignalDir.NONE: "无法判断",
        }
        direction_label = dir_labels.get(final_dir, "未知")

        # ── 最终决策 ───────────────────────────────────────
        if final_dir == SignalDir.SKIP or strength == 0:
            suggested_action = "SKIP"
            skip_reason = (
                advanced.skip_reason
                or ("CVD 信号反向，跳过" if advanced.filter_result == FilterResult.SKIP else "")
                or "无有效信号"
            )
        else:
            suggested_action = "ENTER"
            skip_reason = ""

        # RSI was already computed inside _calc_core_signals.
        # Reuse the raw_value stored in core.rsi.raw_value to avoid duplicate calculation.
        _rsi_raw = core.rsi.raw_value
        try:
            raw_rsi_val = float(_rsi_raw)
        except (ValueError, TypeError):
            raw_rsi_val = 50.0

        return SignalResult(
            asset=self.asset,
            timestamp=now,
            elapsed_sec=elapsed_sec,
            window_duration_sec=window_duration_sec,
            window_end_sec=remaining_sec,
            strength=strength,
            strength_label=strength_label,
            direction=final_dir,
            direction_label=direction_label,
            core=core,
            advanced=advanced,
            core_agree=core_agree,
            advanced_confirm=advanced_confirm,
            suggested_position_pct=1.0,
            suggested_action=suggested_action,
            skip_reason=skip_reason,
            up_mid=up_mid,
            down_mid=down_mid,
            spot_price=current_price,
            atr=atr_val,
            atr_dollar=atr_val,
            anchor_deviation_pct=anchor_deviation,
            raw_rsi=raw_rsi_val,
            raw_funding=funding_val or 0.0,
        )

    # ── 指标实时推送专用（用于 WS /ws/indicators）───────────────────────────
    async def get_indicators(
        self,
        closes: list[float],
        highs: list[float],
        lows: list[float],
        volumes: list[float],
        spot_btc: float,
        spot_eth: float,
        funding_val: float = 0.0,
        oi_val: float = 0.0,
        obi_val: float = 1.0,
        cvd_val: float = 0.0,
        cvd_z: float = 0.0,
        liq_data: dict | None = None,
        window_start: datetime | None = None,
        window_duration_sec: int = 900,
    ) -> dict:
        """
        计算并返回所有 10 个指标，格式与 /api/indicators 完全一致。
        由策略主循环每 tick 调用，结果存入 _latest_indicators，供 WS 推送使用。

        所有原始数据由调用方预取后传入，避免重复请求 Binance API。
        """
        liq_data = liq_data or {}
        current_price = closes[-1] if closes else 0.0
        rsi_val = _calc_rsi(closes) if closes else 50.0

        # MACD
        macd_line, macd_signal, macd_hist = (0.0, 0.0, [])
        if closes and len(closes) >= 26:
            macd_line, macd_signal, macd_hist = _calc_macd_series(closes)

        # EMA — use the cached series for both values and crossover detection
        # (consistent with _calc_core_signals which also uses _calc_ema_series).
        ema9_full  = self._calc_ema_series(closes, 9)
        ema21_full = self._calc_ema_series(closes, 21)
        ema9_val  = ema9_full[-1]  if closes else 0.0
        ema21_val = ema21_full[-1] if closes else 0.0

        # VWAP
        vwap_val = _calc_vwap(closes, volumes) if closes and volumes else current_price

        # ATR / Pivot
        atr_val = _calc_atr(highs, lows, closes) if highs else 0.0
        pivot = _calc_pivot(closes) if closes else {"high": 0.0, "low": 0.0, "mid": 0.0}

        # RSI 反转检测
        prev_rsi = self._rsi_hist()[-1] if self._rsi_hist() else 50.0
        rsi_crossed_down = (prev_rsi >= 70.0) and (rsi_val < 70.0)
        rsi_crossed_up   = (prev_rsi <= 30.0) and (rsi_val > 30.0)
        self._rsi_hist().append(rsi_val)
        if len(self._rsi_hist()) > _MAX_RSI_HISTORY:
            self._rsi_hist().pop(0)

        # EMA 交叉 — use cached series, consistent with _calc_core_signals
        ema_cross = "parallel"
        if closes and len(closes) >= 22:
            e9p_prev  = ema9_full[-2]
            e21p_prev = ema21_full[-2]
            if e9p_prev <= e21p_prev and ema9_val > ema21_val:
                ema_cross = "golden"
            elif e9p_prev >= e21p_prev and ema9_val < ema21_val:
                ema_cross = "death"

        # VWAP 信号
        vwap_sig = "at"
        if closes:
            if closes[-1] > vwap_val * 1.001:
                vwap_sig = "above"
            elif closes[-1] < vwap_val * 0.999:
                vwap_sig = "below"

        # Pivot 信号
        pivot_sig = "within"
        if closes and current_price:
            if current_price > pivot["high"] * 0.9995:
                pivot_sig = "above"
            elif current_price < pivot["low"] * 1.0005:
                pivot_sig = "below"

        # OI 变化率
        oi_change = 0.0
        oi_h = self._oi_hist()
        if oi_val and oi_val > 0:
            oi_h.append(float(oi_val))
            if len(oi_h) > _MAX_OI_HISTORY:
                oi_h.pop(0)
            if len(oi_h) >= 2 and oi_h[-2] > 0:
                oi_change = (oi_h[-1] - oi_h[-2]) / oi_h[-2]

        # CVD Z-score 已在 _cvd_z_score() 调用时（_calc_core_signals 路径）追加到基准，
        # 此处不再重复追加，避免同一 tick 内追加两次导致基准消耗加速。

        # 布林带
        bb_mid, bb_upper, bb_lower = _calc_bbands(closes) if closes else (0.0, 0.0, 0.0)

        # Window timing (requires window_start to be provided)
        elapsed_sec = 0.0
        window_end_sec = float(window_duration_sec)
        if window_start is not None:
            now = datetime.now(timezone.utc)
            elapsed_sec = (now - window_start).total_seconds()
            window_end_sec = max(0.0, float(window_duration_sec) - elapsed_sec)

        rsi_hist = self._rsi_hist()

        # CVD history and Z-score for WS dashboard.
        # The baseline is populated by get_signal() calls; read it without modifying.
        cvd_hist = self._cvd_baseline()
        cvd_z_ro = self._cvd_z_score_ro(abs(cvd_val)) if cvd_val != 0.0 else 0.0
        return {
            "asset": self.asset,
            "spots": {"btc": spot_btc, "eth": spot_eth},
            "elapsed_sec": round(elapsed_sec, 1),
            "window_end_sec": round(window_end_sec, 1),
            "rsi": {
                "value": round(rsi_val, 1),
                "signal": (
                    "overbought" if rsi_crossed_down else
                    "oversold" if rsi_crossed_up else
                    "neutral" if 45 <= rsi_val <= 55 else
                    "skewed"
                ),
                "history": [round(v, 1) for v in rsi_hist[-30:]],
            },
            "macd": {
                "line": round(macd_line, 6),
                "signal": round(macd_signal, 6),
                "histogram": [round(v, 6) for v in macd_hist[-30:]],
            },
            "vwap": {
                "value": round(vwap_val, 4),
                "relative": (
                    "价格 > VWAP (偏多)" if vwap_sig == "above"
                    else "价格 < VWAP (偏空)" if vwap_sig == "below"
                    else "价格 ≈ VWAP"
                ),
                "signal": vwap_sig,
                "price": [round(c, 4) for c in closes[-30:]] if closes else [],
                "vwap":  [round(vwap_val, 4)] * min(30, len(closes)) if closes else [],
            },
            "ema": {
                "ema9":  round(ema9_val, 4),
                "ema21": round(ema21_val, 4),
                "cross": ema_cross,
                "ema9_history":  [round(v, 4) for v in self._calc_ema_series(closes, 9)[-30:]] if closes else [],
                "ema21_history": [round(v, 4) for v in self._calc_ema_series(closes, 21)[-30:]] if closes else [],
                "price": [round(c, 4) for c in closes[-30:]] if closes else [],
            },
            "pivot": {
                "high":     round(pivot["high"], 4),
                "low":      round(pivot["low"], 4),
                "mid":      round(pivot["mid"], 4),
                "distHigh": f"{((current_price - pivot['high']) / pivot['high'] * 100):.2f}%" if pivot['high'] else "--",
                "distLow":  f"{((current_price - pivot['low'])  / pivot['low']  * 100):.2f}%" if pivot['low']  else "--",
                "distMid":  f"{((current_price - pivot['mid'])  / pivot['mid']  * 100):.2f}%" if pivot['mid']  else "--",
                "signal":   pivot_sig,
            },
            "bbands": {
                "mid":    round(bb_mid, 4),
                "upper":  round(bb_upper, 4),
                "lower":  round(bb_lower, 4),
                "signal": (
                    "upper_touch" if current_price >= bb_upper * 0.9995
                    else "lower_touch" if current_price <= bb_lower * 1.0005
                    else "within"
                ),
            },
            "funding": {
                "value":     round(funding_val, 6),
                "direction": (
                    "多头支付空头" if funding_val > 0.0005 else
                    "空头支付多头" if funding_val < -0.0005 else
                    "中性"
                ),
                "signal": (
                    "bearish" if funding_val > 0.0005 else
                    "bullish" if funding_val < -0.0005 else
                    "neutral"
                ),
            },
            "cvd": {
                "value":         round(cvd_val, 2),
                "zscore":       round(cvd_z_ro, 3),
                "history":       [round(v, 2) for v in cvd_hist[-30:]],
                "interpretation": (
                    "accumulation" if cvd_z_ro > 1.0 and cvd_val > 0 else
                    "distribution" if cvd_z_ro > 1.0 and cvd_val < 0 else
                    "flat"
                ),
            },
            "liquidation": {
                "long_dist":  round(liq_data.get("long_dist", -0.02), 6),
                "short_dist": round(liq_data.get("short_dist", 0.02), 6),
                "long_pct":   round(liq_data.get("long_pct", 0.5), 4),
                "short_pct":  round(liq_data.get("short_pct", 0.5), 4),
                "signal":     liq_data.get("signal", "neutral") or "neutral",
            },
            "obi": {
                "value":          round(obi_val, 3),
                "signal": (
                    "up" if obi_val > 1.5 else
                    "down" if obi_val < 0.67 else
                    "neutral"
                ),
                "interpretation": (
                    "bid_heavy" if obi_val > 1.5 else
                    "ask_heavy" if obi_val < 0.67 else
                    "balanced"
                ),
            },
            "oi": {
                "current":      round(oi_val, 0),
                "change":       round(oi_change, 4),
                "interpretation": "neutral",
                "history":       [round(v, 0) for v in oi_h[-30:]],
            },
            "atr": round(atr_val, 4),
        }

