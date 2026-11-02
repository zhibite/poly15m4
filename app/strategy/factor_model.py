"""
factor_model.py — 把相关指标压缩成 5 个相对独立的因子

核心原则：
  - 每个因子输出 score ∈ [-1, +1]（正数看 UP）和 confidence ∈ [0, 1]
  - 高度相关的指标先合成一个因子，不再各投一票
  - 合成时使用 score × confidence，并计算 agreement / conflict
  - 因子权重可由 runtime_config 调整

因子：
  F1 trend        Binance MACD/EMA/Pivot/BBands → 趋势/动量
  F2 reversion    RSI 极值/交叉/背离 + VWAP 偏离 → 均值回归/衰竭
  F3 price        Polymarket CLOB 窗口偏离 + 近端斜率 + 反转 → 市场定价行为
  F4 micro        Polymarket CLOB 所交易 token 的 OBI/深度/点差 → 微观结构
  F5 flow         CVD + OI + Liquidation → 资金流/持仓（Funding 暂不参与方向）
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional

from app.strategy.end_window_signal import compute_liquidity_confidence


# ── 数据类 ────────────────────────────────────────────────────────────────────

@dataclass
class FactorScore:
    name: str
    score: float          # -1 ~ +1，正数看 UP
    confidence: float     # 0 ~ 1
    detail: dict = field(default_factory=dict)


@dataclass
class FactorSnapshot:
    trend: FactorScore
    reversion: FactorScore
    price: FactorScore
    micro: FactorScore
    flow: FactorScore

    net_score: float
    agreement: float      # 0 ~ 1，同向因子的加权占比
    conflict: float       # 1 - agreement
    direction: str        # UP / DOWN / NONE
    weights: dict = field(default_factory=dict)

    def entry_ok(
        self,
        min_net: float = 0.25,
        min_agreement: float = 0.60,
        rev_veto_score: float = 0.70,
    ) -> tuple[bool, str]:
        if self.direction == "NONE":
            return False, "factor_no_direction"
        if abs(self.net_score) < min_net:
            return False, f"factor_weak:{self.net_score:+.2f}"
        if self.agreement < min_agreement:
            return False, f"factor_conflict:{self.agreement:.2f}"
        if (
            self.reversion.score * self.net_score < 0
            and abs(self.reversion.score) >= rev_veto_score
        ):
            return False, "factor_reversion_veto"
        return True, "ok"

    def to_dict(self) -> dict:
        def _f(f: FactorScore) -> dict:
            return {
                "score": round(f.score, 4),
                "confidence": round(f.confidence, 4),
                "detail": f.detail,
            }
        return {
            "trend": _f(self.trend),
            "reversion": _f(self.reversion),
            "price": _f(self.price),
            "micro": _f(self.micro),
            "flow": _f(self.flow),
            "net_score": round(self.net_score, 4),
            "agreement": round(self.agreement, 4),
            "conflict": round(self.conflict, 4),
            "direction": self.direction,
            "weights": self.weights,
        }


# ── 工具 ──────────────────────────────────────────────────────────────────────

def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _num(d: dict, key: str, default: float = 0.0) -> float:
    try:
        v = d.get(key, default)
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _slope(values: list[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    den = sum((i - x_mean) ** 2 for i in range(n))
    return num / den if den > 0 else 0.0


def _r2(values: list[float]) -> float:
    n = len(values)
    if n < 3:
        return 0.0
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    den = sum((i - x_mean) ** 2 for i in range(n))
    if den <= 0:
        return 0.0
    slope = num / den
    intercept = y_mean - slope * x_mean
    ss_tot = sum((v - y_mean) ** 2 for v in values)
    if ss_tot <= 1e-12:
        return 0.0
    ss_res = sum((v - (slope * i + intercept)) ** 2 for i, v in enumerate(values))
    return _clamp(1.0 - ss_res / ss_tot, 0.0, 1.0)


def _spread_bps(orderbook: Optional[dict]) -> Optional[float]:
    if not orderbook:
        return None
    try:
        bids = [float(l.get("price", 0)) for l in (orderbook.get("bids") or []) if float(l.get("price", 0)) > 0]
        asks = [float(l.get("price", 0)) for l in (orderbook.get("asks") or []) if float(l.get("price", 0)) > 0]
        if not bids or not asks:
            return None
        best_bid, best_ask = max(bids), min(asks)
        mid = (best_bid + best_ask) / 2.0
        if mid <= 0:
            return None
        return abs(best_ask - best_bid) / mid * 10_000.0
    except Exception:
        return None


# ── F1 趋势/动量 ──────────────────────────────────────────────────────────────

def compute_trend_factor(raw: dict) -> FactorScore:
    atr = _num(raw, "atr")
    close = _num(raw, "close")
    if atr <= 0 or close <= 0:
        return FactorScore("trend", 0.0, 0.0, {"reason": "no_atr"})

    macd_hist = _num(raw, "macd_hist")
    ema9 = _num(raw, "ema9")
    ema21 = _num(raw, "ema21")
    pivot_mid = _num(raw, "pivot_mid", close)
    bb_mid = _num(raw, "bb_mid", close)
    bb_width = _num(raw, "bb_width")

    z_macd = _clamp(macd_hist / atr, -3.0, 3.0)
    z_ema = _clamp((ema9 - ema21) / atr, -3.0, 3.0)
    z_piv = _clamp((close - pivot_mid) / atr, -3.0, 3.0)
    denom = bb_width / 2.0 if bb_width > 0 else atr
    z_bb = _clamp((close - bb_mid) / denom, -3.0, 3.0)

    raw_score = 0.35 * z_macd + 0.25 * z_ema + 0.20 * z_piv + 0.20 * z_bb
    score = math.tanh(raw_score / 2.0)

    # ATR/价格比作为数据质量：过低波动时趋势信号不可信
    atr_ratio = atr / close
    confidence = _clamp(atr_ratio / 0.0005, 0.0, 1.0)

    return FactorScore("trend", score, confidence, {
        "z_macd": round(z_macd, 3),
        "z_ema": round(z_ema, 3),
        "z_pivot": round(z_piv, 3),
        "z_bb": round(z_bb, 3),
        "atr_ratio": round(atr_ratio, 6),
    })


# ── F2 均值回归/衰竭 ─────────────────────────────────────────────────────────

def compute_reversion_factor(raw: dict) -> FactorScore:
    rsi = _num(raw, "rsi", 50.0)
    prev_rsi = _num(raw, "prev_rsi", 50.0)
    close = _num(raw, "close")
    vwap = _num(raw, "vwap", close)
    atr = _num(raw, "atr")
    div = _num(raw, "rsi_divergence")  # +1 bullish, -1 bearish

    level = 0.0
    if rsi >= 70:
        level = (70.0 - rsi) / 30.0
    elif rsi <= 30:
        level = (30.0 - rsi) / 30.0

    event = 0.0
    if prev_rsi <= 30.0 < rsi:
        event = 1.0
    elif prev_rsi >= 70.0 > rsi:
        event = -1.0

    vwap_z = 0.0
    if atr > 0 and close > 0:
        vwap_z = _clamp((close - vwap) / atr, -3.0, 3.0)

    raw_score = (
        0.5 * _clamp(level, -1.0, 1.0)
        + 0.3 * event
        + 0.4 * div
        - 0.3 * math.tanh(vwap_z)
    )
    score = math.tanh(raw_score)
    confidence = 1.0 if close > 0 and atr > 0 else 0.5

    return FactorScore("reversion", score, confidence, {
        "rsi": round(rsi, 2),
        "prev_rsi": round(prev_rsi, 2),
        "level": round(level, 3),
        "event": event,
        "divergence": div,
        "vwap_z": round(vwap_z, 3),
    })


# ── F3 Polymarket 价格行为 ───────────────────────────────────────────────────

def compute_price_factor(
    up_samples: list[float],
    window_open_up: float,
    current_up: float,
) -> FactorScore:
    n = len(up_samples)
    if n < 3 or window_open_up <= 0 or current_up <= 0:
        return FactorScore("price", 0.0, 0.0, {"reason": "insufficient_samples"})

    dev = current_up - window_open_up
    recent = up_samples[-3:]
    slope = _slope(recent)
    r2 = _r2(up_samples)

    # 近端斜率只在趋势稳定时计入
    raw_score = 0.55 * (dev / 0.10) + 0.35 * (slope / 0.02) * r2

    # 方向无关的反转加成：前中期曾越过 0.5，当前又收回
    early = up_samples[: max(1, n * 2 // 3)]
    if current_up > 0.5 and early and min(early) < 0.45:
        raw_score += 0.15 * _clamp((current_up - 0.5) / 0.20, 0.0, 1.0)
    elif current_up < 0.5 and early and max(early) > 0.55:
        raw_score -= 0.15 * _clamp((0.5 - current_up) / 0.20, 0.0, 1.0)

    score = math.tanh(raw_score)
    confidence = 0.5 + 0.5 * r2
    return FactorScore("price", score, confidence, {
        "deviation": round(dev, 4),
        "recent_slope": round(slope, 6),
        "r2": round(r2, 4),
        "samples": n,
    })


# ── F4 微观结构（Polymarket UP token 盘口）──────────────────────────────────

def compute_micro_factor(
    up_orderbook: Optional[dict],
    *,
    max_spread_bps: float = 100.0,
) -> FactorScore:
    if not up_orderbook:
        return FactorScore("micro", 0.0, 0.0, {"reason": "no_orderbook"})

    _D5, obi, depth_factor, depth_ok = compute_liquidity_confidence(
        up_orderbook, "UP",
    )
    spread = _spread_bps(up_orderbook)
    if spread is None:
        spread_penalty = 1.0
    else:
        spread_penalty = _clamp(1.0 - spread / max(max_spread_bps, 1e-9), 0.0, 1.0)

    score = (
        math.tanh(obi / 0.3)
        * _clamp(depth_factor, 0.0, 1.0)
        * spread_penalty
    )
    confidence = _clamp(depth_factor, 0.0, 1.0)
    if not depth_ok:
        confidence *= 0.5

    return FactorScore("micro", score, confidence, {
        "obi": round(obi, 4),
        "depth_factor": round(depth_factor, 3),
        "depth_ok": depth_ok,
        "spread_bps": round(spread, 2) if spread is not None else None,
        "spread_penalty": round(spread_penalty, 3),
    })


# ── F5 资金流/持仓 ───────────────────────────────────────────────────────────

def compute_flow_factor(
    raw: dict,
    *,
    oi_change_threshold: float = 0.02,
) -> FactorScore:
    cvd = _num(raw, "cvd")
    cvd_z = _num(raw, "cvd_z")
    oi_change = _num(raw, "oi_change")
    price_change = _num(raw, "price_change")
    liq_signal = str(raw.get("liq_signal", "neutral") or "neutral")

    # CVD：带符号的 z-score
    cvd_score = (1.0 if cvd > 0 else -1.0 if cvd < 0 else 0.0) * min(abs(cvd_z), 3.0) / 3.0

    # OI：价格与 OI 同向表示新资金延续趋势
    oi_score = 0.0
    if abs(oi_change) >= oi_change_threshold:
        oi_score = (
            (1.0 if price_change > 0 else -1.0 if price_change < 0 else 0.0)
            * (1.0 if oi_change > 0 else -1.0)
            * min(abs(oi_change) / max(oi_change_threshold, 1e-9), 2.0) / 2.0
        )

    liq_score = 1.0 if liq_signal == "up_magnet" else (-1.0 if liq_signal == "down_magnet" else 0.0)

    raw_score = 0.5 * cvd_score + 0.25 * oi_score + 0.25 * liq_score
    score = math.tanh(raw_score)
    confidence = 1.0 if abs(cvd_z) > 0 else 0.6

    return FactorScore("flow", score, confidence, {
        "cvd_score": round(cvd_score, 3),
        "cvd_z": round(cvd_z, 3),
        "oi_change": round(oi_change, 5),
        "oi_score": round(oi_score, 3),
        "liq_signal": liq_signal,
        "liq_score": liq_score,
        # Funding 暂不参与方向，保留原始值供后续分析
        "funding": round(_num(raw, "funding"), 6),
    })


# ── 合成 ──────────────────────────────────────────────────────────────────────

DEFAULT_WEIGHTS = {
    "trend": 0.20,
    "reversion": 0.15,
    "price": 0.35,
    "micro": 0.15,
    "flow": 0.15,
}


def compute_factor_snapshot(
    raw_metrics: dict,
    *,
    up_samples: list[float],
    window_open_up: float,
    current_up: float,
    up_orderbook: Optional[dict],
    weights: Optional[dict] = None,
    max_spread_bps: float = 100.0,
    oi_change_threshold: float = 0.02,
) -> FactorSnapshot:
    w = dict(weights or DEFAULT_WEIGHTS)
    total_w = sum(max(0.0, float(v)) for v in w.values())
    if total_w <= 0:
        w = dict(DEFAULT_WEIGHTS)
        total_w = sum(w.values())
    w = {k: max(0.0, float(v)) / total_w for k, v in w.items()}

    trend = compute_trend_factor(raw_metrics)
    reversion = compute_reversion_factor(raw_metrics)
    price = compute_price_factor(up_samples, window_open_up, current_up)
    micro = compute_micro_factor(up_orderbook, max_spread_bps=max_spread_bps)
    flow = compute_flow_factor(raw_metrics, oi_change_threshold=oi_change_threshold)

    scores = {
        "trend": trend,
        "reversion": reversion,
        "price": price,
        "micro": micro,
        "flow": flow,
    }

    net = 0.0
    abs_weight_sum = 0.0
    agree_weight = 0.0
    for k, f in scores.items():
        eff = f.score * f.confidence
        net += w[k] * eff
        abs_weight_sum += w[k] * abs(eff)

    net = _clamp(net, -1.0, 1.0)
    direction = "UP" if net > 1e-9 else ("DOWN" if net < -1e-9 else "NONE")

    if abs_weight_sum > 1e-9 and direction != "NONE":
        sign = 1.0 if net > 0 else -1.0
        for k, f in scores.items():
            if f.score * sign > 0:
                agree_weight += w[k] * abs(f.score * f.confidence)
        agreement = _clamp(agree_weight / abs_weight_sum, 0.0, 1.0)
    else:
        agreement = 0.0

    return FactorSnapshot(
        trend=trend,
        reversion=reversion,
        price=price,
        micro=micro,
        flow=flow,
        net_score=net,
        agreement=agreement,
        conflict=1.0 - agreement,
        direction=direction,
        weights=w,
    )
