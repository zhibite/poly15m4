"""
end_window_signal.py — 末端顺势策略四维置信度计算

基于 Polymarket CLOB UP/DOWN 价格，完全不需要 threshold。

五维定义（各自独立）：
  D1 信号方向      → Core 6 指标共识（辅助，权重 15%）
  D2 价格偏离度    → current_up - window_open_up（核心，权重 28%）
  D3 趋势动能      → 最近 3 分钟回归斜率 × 全窗口 R²（核心，权重 28%）
  D4 反转可靠性    → 前中期 UP 是否越 0.50 再回升（核心，权重 22%）
  D5 流动性        → 盘口不平衡度 OBI × 深度充足率（辅助，权重 7%）
"""

from dataclasses import dataclass, field


MID_POINT = 0.50


@dataclass
class EndWindowConfidence:
    D1_signal: float      # 信号方向（辅助）
    D2_deviation: float  # 价格偏离度
    D3_momentum: float   # 趋势动能
    D4_reversal: float   # 反转可靠性
    D5_liquidity: float = 50.0  # 流动性（默认中性值，无数据时回退）
    total: float = 0.0   # 加权总分（计算后填充）

    # 流动性相关诊断数据（供日志用）
    _obi: float = 0.0
    _depth_factor: float = 0.0
    _spread_bps: float = 0.0
    _depth_ok: bool = True

    WEIGHTS = {
        "D1_signal":    0.15,
        "D2_deviation": 0.28,
        "D3_momentum":  0.28,
        "D4_reversal":  0.22,
        "D5_liquidity": 0.07,
    }

    def compute_total(self) -> float:
        self.total = (
            self.D1_signal     * self.WEIGHTS["D1_signal"]
          + self.D2_deviation  * self.WEIGHTS["D2_deviation"]
          + self.D3_momentum   * self.WEIGHTS["D3_momentum"]
          + self.D4_reversal    * self.WEIGHTS["D4_reversal"]
          + self.D5_liquidity   * self.WEIGHTS["D5_liquidity"]
        )
        return self.total

    def log_summary(self) -> str:
        """返回一行的置信度摘要（供日志使用）"""
        return (
            f"D1={self.D1_signal:.0f} D2={self.D2_deviation:.0f} "
            f"D3={self.D3_momentum:.0f} D4={self.D4_reversal:.0f} "
            f"D5={self.D5_liquidity:.0f}(obi={self._obi:+.2f} depth={self._depth_factor:.1f}x) "
            f"total={self.total:.1f}"
        )


# ── 方向确定 ──────────────────────────────────────────────────────────────────

MIN_DEVIATION_FROM_MID = 0.05


def determine_direction(up_mid: float) -> str:
    """
    根据当前 UP 中价确定方向。

    up_mid > 0.50  → UP
    up_mid < 0.50  → DOWN
    |up_mid - 0.50| < 0.05 → NONE（无方向）
    """
    deviation = abs(up_mid - MID_POINT)
    if deviation < MIN_DEVIATION_FROM_MID:
        return "NONE"
    return "UP" if up_mid > MID_POINT else "DOWN"


# ── D2: 价格偏离度 ───────────────────────────────────────────────────────────

def compute_price_deviation(window_open_up: float, current_up: float, direction: str) -> float:
    """
    计算 CLOB 价格偏离度（相对于窗口开始）。
    返回 aligned_deviation ∈ [0, 1]。

    D2 衡量整个窗口期的方向幅度，与 D3 的近端动能互补。
    """
    if direction == "UP":
        aligned = current_up - window_open_up
    elif direction == "DOWN":
        aligned = window_open_up - current_up
    else:
        aligned = abs(current_up - window_open_up)
    return max(0.0, aligned)


def deviation_to_confidence(aligned_deviation: float) -> float:
    """偏离度 → D2 置信度映射（分段）"""
    if aligned_deviation >= 0.25: return 100  # 极强
    if aligned_deviation >= 0.15: return 80   # 强
    if aligned_deviation >= 0.10: return 60   # 中等
    if aligned_deviation >= 0.05: return 40   # 弱
    if aligned_deviation >= 0.02: return 20   # 极弱
    return 0                                   # 不满足


# ── D3: 趋势动能 ─────────────────────────────────────────────────────────────

def _linear_regression_slope(x: list[int], y: list[float]) -> float:
    """最小二乘法计算斜率"""
    n = len(x)
    if n < 2:
        return 0.0
    x_mean = sum(x) / n
    y_mean = sum(y) / n
    num = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y))
    den = sum((xi - x_mean) ** 2 for xi in x)
    if den < 1e-12:
        return 0.0
    return num / den  # slope：每单位 x 的 y 变化量


def compute_trend_r2(up_samples: list[float]) -> float:
    """
    全窗口线性回归 R²，衡量从窗口开始方向是否稳定。
    返回 0.0 ~ 1.0。R² 越高，中间反转越少。
    """
    n = len(up_samples)
    if n < 3:
        return 0.0
    x = list(range(n))
    y = up_samples
    y_mean = sum(y) / n
    ss_tot = sum((yi - y_mean) ** 2 for yi in y)
    if ss_tot < 1e-12:
        return 0.0
    slope = _linear_regression_slope(x, y)
    intercept = y_mean - slope * sum(x) / n
    ss_res = sum((yi - (slope * xi + intercept)) ** 2 for xi, yi in zip(x, y))
    return max(0.0, min(1.0, 1.0 - ss_res / ss_tot))


def compute_recent_momentum(up_samples: list[float], scale_factor: float = 2400.0) -> float:
    """
    计算最近 3 分钟（最多取最后 3 个样本）的价格动能。

    使用线性回归斜率（每分钟价格变化）作为动能强度指标，
    与 D2 的全窗口幅度互补：D2 看幅度，D3 看近端速度。

    scale_factor: 斜率放大系数。
      典型 3 分钟 UP 变化 0.05 → slope ≈ 0.025
      目标 momentum ≈ 60 → scale_factor ≈ 60/0.025 = 2400
    """
    n = len(up_samples)
    if n < 2:
        return 0.0
    window = up_samples[max(0, n - 3):]
    if len(window) < 2:
        return 0.0
    slope_per_min = _linear_regression_slope(list(range(len(window))), window)
    momentum = abs(slope_per_min) * scale_factor
    return min(momentum, 100.0)


def momentum_confidence(momentum_raw: float, r_squared: float) -> float:
    """
    D3 = 近端动能 × 全窗口稳定性折扣
    """
    if momentum_raw < 10:
        return momentum_raw
    if r_squared < 0.5:
        return momentum_raw * 0.5
    if r_squared < 0.7:
        return momentum_raw * 0.8
    return momentum_raw


# ── D4: 反转可靠性 ───────────────────────────────────────────────────────────

REVERSAL_THRESHOLD = 0.05  # UP 价格偏离 0.50 超过此值才算反转


def detect_reversal(
    up_samples: list[float],
    direction: str,
    reversal_threshold: float = REVERSAL_THRESHOLD,
) -> tuple[bool, float]:
    """
    检测窗口前中期（取前 2/3 样本）是否出现过反向越界。

    返回：(has_reversed, reversal_depth)
      has_reversed   = 是否出现过反向越界
      reversal_depth = 反转的最大深度（UP 价格从 0.50 偏离的幅度）
    """
    n = len(up_samples)
    if n < 3:
        return False, 0.0
    early_n = n * 2 // 3
    early_prices = up_samples[:early_n]

    has_reversed = False
    reversal_depth = 0.0

    for price in early_prices:
        deviation_from_mid = abs(price - MID_POINT)
        if deviation_from_mid < reversal_threshold:
            continue
        if direction == "UP" and price < MID_POINT:
            has_reversed = True
            reversal_depth = max(reversal_depth, MID_POINT - price)
        elif direction == "DOWN" and price > MID_POINT:
            has_reversed = True
            reversal_depth = max(reversal_depth, price - MID_POINT)

    return has_reversed, reversal_depth


def reversal_reliability(
    has_reversed: bool,
    reversal_depth: float,
    current_deviation: float,
) -> float:
    """
    D4：反转可靠性评分
    """
    if not has_reversed:
        return 30.0  # 全程在 0.50 同侧，无反向确认

    if reversal_depth < 1e-9:
        return 30.0

    rebound_ratio = current_deviation / reversal_depth

    if rebound_ratio >= 1.0:
        base = 100.0
    elif rebound_ratio >= 0.8:
        base = 90.0
    elif rebound_ratio >= 0.5:
        base = 75.0
    else:
        base = 50.0

    if reversal_depth >= 0.15:
        base = min(base + 10, 100.0)

    return base


# ── D5: 流动性置信度 ─────────────────────────────────────────────────────────

# OBI → D5 映射（线性分段）
# OBI = (bid_vol - ask_vol) / (bid_vol + ask_vol)
#  OBI = 0   → 中性，D5 = 50
#  OBI = +1  → 全是买方，D5 = 100（做多 UP 利好）
#  OBI = -1  → 全是卖方，D5 = 100（做多 DOWN 利好）
#  深度不足（depth_factor < 1.0）→ D5 降权，最差 0
OBI_BULL_THRESH = 0.10   # OBI > +0.10 才算买方主导
OBI_BEAR_THRESH = -0.10   # OBI < -0.10 才算卖方主导


def _obi_to_confidence(obi: float, depth_factor: float) -> float:
    """
    把 OBI 和深度因子合并为 0~100 的置信度分数。

    depth_factor: 盘口总量 / 最低期望深度，>1 表示深度充足，<1 表示深度不足。
                  越充足 → depth_multiplier 越高（最多 1.0x）；越浅 → 越低（最少 0.5x）。
    """
    # 深度充足时不额外加分；深度不足时降低 D5
    depth_multiplier = 0.5 + 0.5 * min(depth_factor, 1.0)

    if abs(obi) < OBI_BULL_THRESH:
        # 中性区域：接近 0，D5 趋向 50
        base = 50.0
    elif obi > 0:
        # 买方主导：obi 越接近 +1，D5 越高（最高 100）
        base = 50.0 + 50.0 * min(obi / (1.0 - OBI_BULL_THRESH), 1.0)
    else:
        # 卖方主导：obi 越接近 -1，D5 越高（最高 100）
        base = 50.0 + 50.0 * min(abs(obi) / (1.0 - OBI_BEAR_THRESH), 1.0)

    return max(0.0, min(100.0, base * depth_multiplier))


def compute_liquidity_confidence(
    orderbook: dict | None,
    direction: str,
    *,
    min_depth_tokens: float = 10.0,
    min_levels: int = 3,
) -> tuple[float, float, float, bool]:
    """
    从盘口数据计算 D5 流动性置信度。

    orderbook: pm_client.get_orderbook() 返回的原始 dict
               格式：{"bids": [{"price": "0.46", "size": "150"}, ...],
                      "asks": [{"price": "0.47", "size": "120"}, ...]}
    direction: "UP" / "DOWN" — 决定用哪边档位算深度因子
    min_depth_tokens: 每档最低 token 数量（过滤虚假档位）
    min_levels: 最低档位数（低于此数认为深度不足）

    Returns: (D5, obi, depth_factor, depth_ok)
    """
    if orderbook is None:
        return 50.0, 0.0, 0.0, False

    bids_raw = orderbook.get("bids") or []
    asks_raw = orderbook.get("asks") or []

    def parse_levels(levels: list) -> list[tuple[float, float]]:
        result = []
        for lv in levels:
            price = float(lv.get("price", 0))
            size = float(lv.get("size", 0))
            if price > 0 and size >= min_depth_tokens:
                result.append((price, size))
        return result

    bids = parse_levels(bids_raw)
    asks = parse_levels(asks_raw)

    if len(bids) < min_levels or len(asks) < min_levels:
        return 50.0, 0.0, 0.0, False

    bid_vol = sum(s for _, s in bids)
    ask_vol = sum(s for _, s in asks)
    total_vol = bid_vol + ask_vol

    if total_vol < 1e-9:
        return 50.0, 0.0, 0.0, False

    obi = (bid_vol - ask_vol) / total_vol

    # 深度因子：以两边档位中较小者为基准
    # 正常阈值 = min_depth_tokens * min_levels（每个方向至少 3 档 × 最小量）
    min_expected = min_depth_tokens * min_levels * 2
    depth_factor = total_vol / min_expected if min_expected > 0 else 1.0
    depth_ok = depth_factor >= 1.0

    # OBI 方向检查：若做 UP 但卖方主导（OBI 显著为负），则降低权重
    if direction == "UP":
        if obi < OBI_BEAR_THRESH:
            depth_ok = False   # 降权但不断言失败
    elif direction == "DOWN":
        if obi > OBI_BULL_THRESH:
            depth_ok = False

    D5 = _obi_to_confidence(obi, depth_factor)

    return D5, obi, depth_factor, depth_ok


def compute_confidence(
    indicator_signal_result,   # SignalResult from IndicatorSignalService
    window_open_up: float,
    current_up: float,
    up_samples: list[float],
    direction: str,
    orderbook: dict | None = None,  # 新增：可选盘口数据
) -> EndWindowConfidence:
    """
    计算末端顺势策略的五维置信度（加 D5 流动性）。

    orderbook: pm_client.get_orderbook() 返回的原始 dict，
               传入 None 则 D5 = 50.0（中性回退，不影响总分）。
    """
    # D1: 信号方向（辅助，范围 0~100）
    if isinstance(indicator_signal_result, dict):
        core_agree = int(indicator_signal_result.get("core_agree", 0))
    else:
        core_agree = int(getattr(indicator_signal_result, "core_agree", 0))
    D1 = min(core_agree / 6 * 100.0, 100.0)

    # D2: 全窗口价格偏离度
    aligned_deviation = compute_price_deviation(window_open_up, current_up, direction)
    D2 = deviation_to_confidence(aligned_deviation)

    # D3: 近端动能 × 全窗口稳定性
    momentum_raw = compute_recent_momentum(up_samples)
    r2 = compute_trend_r2(up_samples)
    D3 = momentum_confidence(momentum_raw, r2)

    # D4: 反转可靠性
    current_deviation = abs(current_up - MID_POINT)
    has_rev, rev_depth = detect_reversal(up_samples, direction)
    D4 = reversal_reliability(has_rev, rev_depth, current_deviation)

    # D5: 流动性
    D5, obi, depth_factor, depth_ok = compute_liquidity_confidence(
        orderbook, direction,
    )

    conf = EndWindowConfidence(
        D1_signal=D1,
        D2_deviation=D2,
        D3_momentum=D3,
        D4_reversal=D4,
        D5_liquidity=D5,
        _obi=obi,
        _depth_factor=depth_factor,
        _depth_ok=depth_ok,
    )
    conf.compute_total()
    return conf
