# Polymarket 末端顺势策略 — 逻辑流程文档

> 本策略基于 Polymarket 15m BTC/ETH UP/DOWN 二元市场，专为窗口末端（最后 3 分钟）设计。
>
> 核心逻辑：**末端顺势**，买价格在 0.70+ 方向的高胜率方向，等惯性维持到结算。
>
> 更新时间：2026-05-31
> 版本：v2（以价格行为为核心，信号为辅助参考，重构置信度体系）

---

## 目录

0. [策略流程图](#0-策略流程图)
1. [策略定位](#1-策略定位)
2. [核心逻辑与理论基础](#2-核心逻辑与理论基础)
3. [置信度体系 — 四维评估](#3-置信度体系--四维评估)
4. [入场决策 — 置信度阈值](#4-入场决策--置信度阈值)
5. [仓位管理](#5-仓位管理)
6. [退出机制](#6-退出机制)
7. [与主策略的关系](#7-与主策略的关系)
8. [风险控制](#8-风险控制)
9. [INVALID 风险处理](#9-invalid-风险处理)
10. [配置参数](#10-配置参数)
11. [数据源要求](#11-数据源要求)
12. [模块设计](#12-模块设计)
13. [回测验证清单](#13-回测验证清单)

---

## 0. 策略流程图

```
┌─────────────────────────────────────────────────────────────┐
│                   策略主循环（每个 tick）                      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │  窗口在哪个阶段？  │
                    └────────┬────────┘
                             │
          ┌──────────────────┼──────────────────┐
          ▼                  ▼                  ▼
   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
   │ T=0 ~ 12min │  │ T=12~14.5min│  │ T=14.5~15min│
   │  主策略区间   │  │  末端策略区间 │  │  仅退出不开仓 │
   │  不做末端操作 │  │  检查入场/退出│  │             │
   └──────────────┘  └──────┬───────┘  └──────┬───────┘
                             │
              ┌──────────────┴──────────────┐
              ▼                              ▼
     ┌────────────────┐           ┌─────────────────┐
     │ 有持仓？         │           │  无持仓？         │
     └───────┬────────┘           └────────┬────────┘
             │                              │
      ┌──────▼──────┐            ┌────────▼────────┐
      │   退出检查    │            │    入场检查      │
      │ （优先级最高）│            │ （置信度驱动）   │
      └──────┬──────┘            └────────┬────────┘
             │                              │
      ┌──────▼──────┐               ┌───────▼───────┐
      │ E1 硬性截止  │◄──最高优先级──│ H1 时机窗口    │
      │ E2 方向反转  │               │ H2 Token价格≥0.7│
      │ E3 止盈      │               │ H3 主策略互斥   │
      │ E4 止损      │               └───────┬───────┘
      └──────┬──────┘               ┌───────▼───────┐
             │                       │ 置信度 ≥ 50？  │
             ▼                       └───────┬───────┘
     ┌──────────────┐                       │
     │ 执行退出/结算  │               ┌──────▼──────┐
     └──────────────┘               │  ≥50 → 入场  │
                                     │  <50 → 跳过  │
                                     └──────┬───────┘
                                            │
                                     ┌───────▼───────┐
                                     │ 一次性建仓完毕 │
                                     │  置信度定仓位  │
                                     └────────────────┘


┌─────────────────────────────────────────────────────────────┐
│               四维置信度计算（入场前调用）                    │
│                                                             │
│  D1 信号方向 ──────┐                                        │
│  （Core 6 指标）    │                                        │
│  权重：15%          │                                        │
│                     ▼                                        │
│              综合置信度 =                                     │
│              D1×0.15 + D2×0.30 + D3×0.30 + D4×0.25         │
│                     ▲                                        │
│  D2 价格偏离度 ──────┤                                        │
│  （current_mid - window_open_mid）                          │
│  权重：30%                                            │       │
│                                                      ▼       │
│  D3 趋势动能 ──────┤                           置信度阈值    │
│  （线性回归斜率+R²）                                       │       │
│  权重：30%          │                                       ▼       │
│                     │                              ┌────────────┐
│  D4 反转可靠性 ──────┘                              │  ≥80 → 100%│
│  （前中期是否曾反转再回升）                               │  ≥65 → 80% │
│  权重：25%                                            │  ≥50 → 50% │
│                                                      │  <50 → 不入 │
└──────────────────────────────────────────────────────└────────────┘
```

---

## 1. 策略定位

### 1.1 与主策略的关系

| | 主策略（MainStrategy） | 末端顺势（EndMomentum） |
|---|---|---|
| **窗口时段** | T=0 ~ T=12min | T=12min ~ T=14.5min |
| **决策核心** | Core 6 指标共识 | 四维置信度（价格行为为主，信号为辅） |
| **入场方向** | 趋势方向（价格偏离 0.50 的方向） | 趋势惯性方向（0.70+ 高胜率方向） |
| **持仓时长** | 可能贯穿整个窗口 | 极短（1~3 分钟） |
| **胜率** | ~60% | ~75~85% |
| **赔率** | 中等 | 低（但确定性高） |
| **与对方的关系** | 互斥（同窗口最多一个策略持仓） | 互斥（同窗口最多一个策略持仓） |

### 1.2 策略本质

```
末端顺势 ≠ 趋势预测
末端顺势 = 全窗口价格惯性的延续押注

窗口剩余时间越短，趋势被打破所需的能量越大
所以末端买 0.80 方向的本质是：
  "我赌这股惯性在 3 分钟内不会被打破"
  而不是在赌"BTC 会涨还是会跌"
```

---

## 2. 核心逻辑与理论基础

### 2.1 核心命题

> **Polymarket 二元市场在窗口末端，胜率 0.80+ 的方向，维持到结算的概率是否显著高于其隐含赔率所暗示的概率？**

| 市场定价 | 隐含胜率 | 结算实际胜率（经验） |
|---------|---------|-------------------|
| UP = 0.82 | 82% | ~85~90% |
| UP = 0.78 | 78% | ~82~86% |
| UP = 0.70 | 70% | ~75~80% |
| UP = 0.60 | 60% | ~63~67% |
| UP = 0.50 | 50% | ~50% |

末端 0.80+ 的方向往往存在**惯性溢价**（markets tend to resolve in the direction of recent momentum），实际胜率 > 隐含胜率。

### 2.2 为什么是顺势，不是反转？

```
买 0.25 方向（反转）：
  - 需要 0.25 → YES（概率从 25% 拉到 100%）
  - 剩余 3 分钟内打破市场共识
  - 期望收益 ≈ 25% × 3.0 - 75% × 1.0 ≈ 0（期望为 0 或负）

买 0.80 方向（顺势）：
  - 只需 0.80 → 维持（概率从 80% 维持到 100%）
  - 3 分钟内惯性更难被打破
  - 期望收益 ≈ 80% × 0.25 - 20% × 1.0 > 0（惯性存在时更高）
```

**价格极端（0.25）不代表被低估，更可能是市场知道一些你不知道的事。**

### 2.3 末端惯性的来源

1. **信息不对称**：末端的大额头寸往往由信息更充分的人驱动
2. **流动性清洗**：逆向止损被扫出后，方向更容易延续
3. **做市商中性对冲**：CLOB 做市商倾向于维持当前价格，不主动打破方向
4. **结算摩擦**：结算前退出有手续费 + 滑点，持仓者倾向于持有到期

---

## 3. 置信度体系 — 四维评估

> **核心原则**：价格行为本身是决策的核心依据，指标信号仅作为辅助参考。
>
> 四个维度分别计算各自的置信度分项，最终加权得到总分。某一维度缺失（如无信号数据）不影响其他维度。

### 3.0 方向确定

在计算置信度之前，首先根据当前 CLOB UP/DOWN token 价格确定方向：

```python
MID_POINT = 0.50
MIN_DEVIATION = 0.05   # 价格偏离 0.50 至少 0.05 才算有方向

def determine_direction(up_mid: float) -> str:
    """
    方向 = 顺着 Polymarket CLOB 当前 UP/DOWN 价格隐含的方向

    Polymarket 市场（BTC > $X at T=15:00）：
      up_mid > 0.50  →  方向 = UP（顺势买 UP token）
      up_mid < 0.50  →  方向 = DOWN（顺势买 DOWN token）
      |up_mid - 0.50| < MIN_DEVIATION → 无方向，不做
    """
    deviation = abs(up_mid - MID_POINT)
    if deviation < MIN_DEVIATION:
        return "NONE"
    return "UP" if up_mid > MID_POINT else "DOWN"
```

---

### 3.1 信号方向（D1）— 辅助参考

信号来自主策略的 `IndicatorSignalService`，**不作为主要依据**，但仍提供方向辅助确认。

```python
# Core 6 指标的共识方向，范围 0~6
core_agree = indicator_service.get_signal().core_agree

# 映射到 0~100
D1_score = min(core_agree / 6 * 100, 100)
```

| Core 共识数 | D1 分项 | 说明 |
|-----------|--------|------|
| 0/6 | 0 | 无信号，不反对也不加分 |
| 1~2/6 | 20 | 信号弱 |
| 3/6 | 50 | 中等信号 |
| 4/6 | 70 | 较强信号 |
| 5~6/6 | 90 | 强信号 |

> **关键**：即使 Core 共识 = 0/6（无信号），只要其他三个价格行为维度足够强，末端策略仍可入场。信号缺失 ≠ 反向信号。

---

### 3.2 价格偏离度（D2）— 核心维度

**定义**：当前 CLOB UP token 中价相对于窗口开始时的变化幅度。衡量的是**整个窗口期**方向偏离的幅度，完全基于 CLOB 价格，不需要解析任何 threshold。

```
偏离度 = abs(current_UP_mid - window_open_UP_mid)

方向为 UP 时：偏离度 = current_UP_mid - window_open_UP_mid（正向 = 符合持仓方向）
方向为 DOWN 时：偏离度 = window_open_UP_mid - current_UP_mid（正向 = 符合持仓方向）
```

**偏离度越大 → UP/DOWN 价格越极端 → 方向惯性越强**

```python
def compute_price_deviation(
    window_open_up: float,  # 窗口开始时的 UP 中价（ctx.market.window_open_up）
    current_up: float,       # 当前 UP 中价（ctx.market._up_mid）
    direction: str,          # UP 或 DOWN
) -> float:
    """
    计算 CLOB 价格偏离度（相对于窗口开始）
    返回：aligned_deviation ∈ [0, 1]
    """
    if direction == "UP":
        aligned = current_up - window_open_up
    else:
        aligned = window_open_up - current_up
    return max(0.0, aligned)


def deviation_to_confidence(aligned_deviation: float) -> float:
    """偏离度 → 置信度映射（分段线性）"""
    if aligned_deviation >= 0.25: return 100  # ≥ 0.25 → 极强偏离
    elif aligned_deviation >= 0.15: return 80  # ≥ 0.15 → 强偏离
    elif aligned_deviation >= 0.10: return 60  # ≥ 0.10 → 中等偏离
    elif aligned_deviation >= 0.05: return 40  # ≥ 0.05 → 弱偏离
    elif aligned_deviation >= 0.02: return 20  # ≥ 0.02 → 极弱偏离
    else:                            return 0    # < 0.02 → 不满足
```

| 偏离度 | D2 分项 |
|--------|---------|
| ≥ 0.25 | 100（极强） |
| 0.15 ~ 0.25 | 80（强） |
| 0.10 ~ 0.15 | 60（中等） |
| 0.05 ~ 0.10 | 40（弱） |
| 0.02 ~ 0.05 | 20（极弱） |
| < 0.02 | 0（不满足） |

**举例**：

```
场景A（极强偏离）：
  window_open_UP = 0.50，当前 UP = 0.78
  → 偏离度 = 0.28 → D2 = 100（从头到尾方向明确）

场景B（强偏离）：
  window_open_UP = 0.55，当前 UP = 0.72
  → 偏离度 = 0.17 → D2 = 80（方向持续强化）

场景C（弱偏离，横盘）：
  window_open_UP = 0.52，当前 UP = 0.56
  → 偏离度 = 0.04 → D2 = 20（几乎没动，方向不明确）
```

---

### 3.3 价格变动趋势及动能（D3）— 核心维度

**定义**：最近 3 分钟内 UP 价格的变化强度。与 D2 的"全窗口幅度"不同，D3 关注的是**近端是否有加速**，捕捉的是窗口末期的动能强化信号。

> D2 = 全窗口幅度（起点到当前）；D3 = 近端速度（最近 3 分钟的斜率）。两者互补，各自独立。

#### 3.3.1 近端动能强度

```python
def compute_recent_momentum(
    up_samples: list[float],   # 窗口内按分钟采样的 UP 中价序列（最后一个是当前）
    scale_factor: float = 100.0,  # 斜率放大系数
) -> float:
    """
    计算最近 3 分钟（取最后 3 个样本）的价格动能
    返回值：0 ~ 100
    """
    n = len(up_samples)
    if n < 2:
        return 0.0

    # 取最后 3 个样本（不够则取全部）
    window = up_samples[max(0, n - 3):]
    if len(window) < 2:
        return 0.0

    # 线性回归斜率（每分钟）
    m = len(window)
    x = list(range(m))
    y = window
    x_mean = sum(x) / m
    y_mean = sum(y) / m
    num = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y))
    den = sum((xi - x_mean) ** 2 for xi in x)
    if den == 0:
        return 0.0
    slope_per_min = num / den  # 每分钟价格变化

    # 动能 = 斜率 × scale_factor（放大后映射到 0~100）
    momentum = abs(slope_per_min) * scale_factor
    return min(momentum, 100.0)


# 示例：
# 最近 3 分钟：UP 从 0.70 → 0.75 → 0.78（持续加速）
# slope_per_min ≈ 0.04/2 = 0.02
# momentum = 0.02 * 100 = 2.0 → 截断到 100（实际 2.0 很小，说明近端加速不明显）
# → 实际数值很小，说明这个 scale_factor 可能需要调大

# 修正 scale_factor：
# 典型 3 分钟：UP 变化 0.05 → slope_per_min ≈ 0.025
# 想要 momentum ≈ 60 → scale_factor = 60/0.025 = 2400
```

#### 3.3.2 趋势稳定性（R²）

使用**全窗口线性回归 R²** 来衡量趋势的稳定性，R² 高说明从窗口开始方向就没有反转过，是最强的惯性信号：

```python
def compute_trend_r2(
    up_samples: list[float],  # 窗口内按分钟采样的 UP 中价序列
) -> float:
    """
    计算 UP 中价序列的线性回归 R²
    返回 0.0 ~ 1.0
    R² 越高 = 趋势越稳定，中间反转越少
    """
    n = len(up_samples)
    if n < 3:
        return 0.0

    x = list(range(n))
    y = up_samples

    x_mean = sum(x) / n
    y_mean = sum(y) / n

    ss_tot = sum((yi - y_mean) ** 2 for yi in y)
    if ss_tot == 0:
        return 0.0

    numerator   = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y))
    denominator = sum((xi - x_mean) ** 2 for xi in x)

    if denominator == 0:
        return 0.0

    slope     = numerator / denominator
    intercept = y_mean - slope * x_mean

    ss_res = sum((yi - (slope * xi + intercept)) ** 2 for xi, yi in zip(x, y))
    r_squared = 1 - (ss_res / ss_tot)

    return max(0.0, min(1.0, r_squared))
```

#### 3.3.3 动能置信度汇总

D3 = 近端动能（D3a）× 全窗口稳定性折扣（D3b）

```python
# D3a: 近端动能强度（0 ~ 100）
momentum_raw = compute_recent_momentum(up_samples)

# D3b: 全窗口稳定性折扣（0 ~ 1.0）
r_squared = compute_trend_r2(up_samples)


def momentum_confidence(
    momentum_raw: float,    # 0 ~ 100
    r_squared: float,      # 0.0 ~ 1.0
) -> float:
    """
    D3：动能置信度 = 近端动能 × 稳定性折扣
    """

    if momentum_raw < 10:
        return momentum_raw  # 近端动能不足，分数直接是动能值

    # R² 稳定性折扣
    if r_squared < 0.5:
        return momentum_raw * 0.5   # 趋势不稳定，中间有反转 → 降权 50%
    elif r_squared < 0.7:
        return momentum_raw * 0.8   # 趋势较稳定
    else:
        return momentum_raw


# 示例：
# 近端动能 = 60（全窗口方向一致），r_squared = 0.82
# → D3 = 60

# 近端动能 = 55（末端价格加速），r_squared = 0.35（中途大幅回落）
# → D3 = 55 * 0.5 = 27.5（动能虽正向但趋势不稳，降权）
```

| 近端动能 | R²（全窗口） | D3 分项 | 说明 |
|---------|------------|---------|------|
| ≥ 30 | ≥ 0.7 | 90~100 | 强稳定动能 |
| 20~30 | ≥ 0.7 | 70~90 | 中强稳定动能 |
| 10~20 | ≥ 0.5 | 50~70 | 中等动能 |
| < 10 | — | 0~10 | 近端动能不足 |

---

### 3.4 是否反转过（D4）— 核心维度

**定义**：在当前窗口的前中期（0~T=9min），UP 价格是否曾短暂跌破过 0.50（对 UP 方向而言）或涨过 0.50（对 DOWN 方向而言），形成过反向移动。

```
反转 = 窗口前中期 UP 价格曾短暂越到 0.50 另一侧

反转过 → 末端顺势更可靠（底部/顶部已确认，方向不会再轻易反转）
未反转过 → 末端顺势需谨慎（可能是横盘末期假突破）

举例（方向 = UP）：
  T=5min 时 UP = 0.42（短暂跌破 0.50）← 出现过反转
  T=12min 时 UP = 0.78              ← 末端 UP
  → 这种情况买 UP 的可靠性 > 全程在 0.50 上方横盘的 UP

举例（方向 = DOWN）：
  T=3min 时 UP = 0.58（短暂涨破 0.50）← 出现过反转
  T=12min 时 UP = 0.32              ← 末端 DOWN
  → 反转确认后买 DOWN 更可靠
```

#### 3.4.1 反转检测

```python
def detect_reversal(
    up_samples: list[float],         # 窗口内按时间排序的 UP 中价序列（每分钟一个）
    direction: str,                  # 持仓方向（UP 或 DOWN）
    reversal_threshold: float = 0.05,  # 反转判定阈值（UP 价格偏离 0.50 超过此值才算反转）
) -> tuple[bool, float]:
    """
    检测窗口前中期是否出现过反向越界
    前中期定义：前 2/3 的样本

    返回：(has_reversed, reversal_depth)
    """
    n = len(up_samples)
    if n < 3:
        return False, 0.0

    early_n = n * 2 // 3
    early_prices = up_samples[:early_n]

    has_reversed   = False
    reversal_depth = 0.0

    for price in early_prices:
        deviation_from_mid = abs(price - 0.50)
        if deviation_from_mid < reversal_threshold:
            continue

        if direction == "UP" and price < 0.50:
            has_reversed   = True
            reversal_depth = max(reversal_depth, 0.50 - price)
        elif direction == "DOWN" and price > 0.50:
            has_reversed   = True
            reversal_depth = max(reversal_depth, price - 0.50)

    return has_reversed, reversal_depth
```

#### 3.4.2 反转可靠性评分

```python
def reversal_reliability(
    has_reversed: bool,
    reversal_depth: float,     # 反转深度（UP 价格从 0.50 偏离的最大幅度）
    current_deviation: float,  # 当前 UP 价格偏离 0.50 的幅度
) -> float:
    """D4：反转可靠性评分"""

    if not has_reversed:
        return 30.0  # 未反转：全程在 0.50 同侧

    rebound_ratio = current_deviation / reversal_depth if reversal_depth > 0 else 0

    if rebound_ratio >= 1.0:
        base = 100.0
    elif rebound_ratio >= 0.8:
        base = 90.0
    elif rebound_ratio >= 0.5:
        base = 75.0
    else:
        base = 50.0

    if reversal_depth >= 0.15:
        base = min(base + 10, 100)

    return base
```

| 情况 | D4 分项 | 说明 |
|------|---------|------|
| 未反转（全程在 0.50 同侧） | 30 | 无反向确认，方向依赖横盘 |
| 反转后反弹 < 50% | 50 | 有反转但力度不足 |
| 反转后反弹 50%~80% | 75 | 有效反弹，可靠 |
| 反转后反弹 ≥ 80% | 90 | 强反弹，极可靠 |
| 反转后完全收复（≥100%） | 100 | 极强反转，最可靠 |

---

### 3.5 置信度综合计算

```python
@dataclass
class EndWindowConfidence:
    D1_signal:    float   # 信号方向（辅助）
    D2_deviation: float   # 价格偏离度
    D3_momentum:  float   # 趋势动能
    D4_reversal:  float   # 反转可靠性
    total:        float   # 加权总分

    # 各维度权重
    WEIGHTS = {
        "D1_signal":    0.15,   # 信号仅占 15%（辅助参考）
        "D2_deviation": 0.30,  # 价格偏离度占 30%
        "D3_momentum":  0.30,  # 趋势动能占 30%
        "D4_reversal": 0.25,  # 反转可靠性占 25%
    }

    def compute_total(self) -> float:
        return (
            self.D1_signal    * self.WEIGHTS["D1_signal"]
          + self.D2_deviation * self.WEIGHTS["D2_deviation"]
          + self.D3_momentum  * self.WEIGHTS["D3_momentum"]
          + self.D4_reversal  * self.WEIGHTS["D4_reversal"]
        )


def compute_confidence(
    indicator_service,              # IndicatorSignalService（Core 6 指标）
    window_open_up: float,         # 窗口开始时的 UP 中价（ctx.market.window_open_up）
    current_up: float,             # 当前 UP 中价（ctx.market._up_mid）
    up_samples: list[float],       # 窗口内按分钟采样的 UP 中价序列
    direction: str,                # UP 或 DOWN
) -> EndWindowConfidence:
    """
    计算末端顺势策略的四维置信度
    全部基于 CLOB UP 中价，不需要 threshold
    """

    # D1: 信号方向（辅助，范围 0~100）
    core_agree = indicator_service.get_signal().core_agree
    D1 = min(core_agree / 6 * 100, 100)

    # D2: CLOB 价格偏离度（相对于窗口开始）
    aligned_deviation = compute_price_deviation(window_open_up, current_up, direction)
    D2 = deviation_to_confidence(aligned_deviation)

    # D3: 近端动能（最近 3 分钟斜率）× 全窗口稳定性（R²）
    momentum_raw = compute_recent_momentum(up_samples)
    r_squared   = compute_trend_r2(up_samples)
    D3 = momentum_confidence(momentum_raw, r_squared)

    # D4: 反转可靠性（UP 价格是否曾越 0.50）
    current_deviation = abs(current_up - 0.50)
    has_reversed, reversal_depth = detect_reversal(up_samples, direction)
    D4 = reversal_reliability(has_reversed, reversal_depth, current_deviation)

    conf = EndWindowConfidence(D1=D1, D2=D2, D3=D3, D4=D4)
    conf.total = conf.compute_total()

    return conf


# 输出示例：
# EndWindowConfidence(
#   D1_signal=50,      # Core 3/6 → 50（辅助）
#   D2_deviation=80,   # UP 价格偏离 0.17 → 80
#   D3_momentum=85,    # 动能强 + 趋势稳定 → 85
#   D4_reversal=90,   # 反转确认 + 收复失地 → 90
#   total=77.5           # 加权总分
# )
```

---

## 4. 入场决策 — 置信度阈值

### 4.1 入场等级

| 综合置信度 | 等级 | 入场决策 | 仓位比例 |
|-----------|------|---------|---------|
| ≥ 80 | 强信号 | 正常入场 | 基准仓位 100% |
| 65 ~ 80 | 中强信号 | 正常入场 | 基准仓位 80% |
| 50 ~ 65 | 中等信号 | 轻仓试探 | 基准仓位 50% |
| < 50 | 弱信号 | 不入场 | — |

### 4.2 必须满足的硬条件

即使置信度达标，以下硬条件必须全部满足：

```python
# H1: 时机窗口
def check_hard_timing(elapsed: float) -> tuple[bool, str]:
    WINDOW_START = 12 * 60    # 720 秒（T=12min）
    WINDOW_END    = 14.5 * 60  # 870 秒（T=14.5min）
    HARDCUT       = 14.75 * 60 # 885 秒（T=14.75min，到期前 15 秒）

    if elapsed < WINDOW_START:
        return False, "too_early"
    if elapsed > WINDOW_END:
        return False, "too_late"
    if elapsed > HARDCUT:
        return False, "hardcut"   # 硬性截止，不开新仓
    return True, "ok"


# H2: 最低入场价格（确保是高胜率方向）
# 此处用 Polymarket UP/DOWN token 价格来确认市场定价是否极端
def check_min_entry_price(current_mid: float, direction: str) -> tuple[bool, str]:
    MIN_ENTRY_PRICE = 0.70   # UP token 或 DOWN token 价格至少 0.70

    if direction == "UP" and current_mid < MIN_ENTRY_PRICE:
        return False, "up_price_below_min"
    if direction == "DOWN" and (1 - current_mid) < MIN_ENTRY_PRICE:
        return False, "down_price_below_min"
    return True, "ok"


# H3: 主策略互斥检查
def check_main_strategy_conflict(
    main_has_position: bool,
    main_direction: str,
    direction: str,
) -> tuple[bool, str]:
    """
    main_direction: 主策略当前持仓方向（UP / DOWN / NONE）
    direction: 末端策略方向（UP / DOWN）
    """
    if not main_has_position:
        return True, "ok"  # 主策略未持仓，不冲突

    if main_direction == "NONE":
        return True, "ok"  # 主策略无方向，不冲突

    if main_direction == direction:
        return True, "can_add"  # 方向一致，可以叠加

    # 方向矛盾
    return False, "main_opposite_direction"
```

### 4.3 完整入场决策流程

```python
def check_entry(
    elapsed: float,
    current_mid: float,           # 当前 UP 中价（ctx.market._up_mid）
    direction: str,
    confidence: EndWindowConfidence,
    main_has_position: bool,
) -> tuple[bool, str, float]:
    """
    返回 (是否入场, 入场原因, 仓位比例)
    """

    # 硬条件
    ok, reason = check_hard_timing(elapsed)
    if not ok:
        return False, f"hard_fail:{reason}", 0.0

    ok, reason = check_min_entry_price(current_mid, direction)
    if not ok:
        return False, f"price_reject:{reason}", 0.0

    main_direction = "UP"  # 从主策略持仓中获取
    ok, reason = check_main_strategy_conflict(main_has_position, main_direction, direction)
    if not ok:
        return False, f"conflict:{reason}", 0.0

    # 置信度阈值
    total = confidence.total

    if total >= 80:
        size_pct = 1.0
        reason = "strong_signal"
    elif total >= 65:
        size_pct = 0.8
        reason = "medium_strong_signal"
    elif total >= 50:
        size_pct = 0.5
        reason = "medium_signal"
    else:
        return False, f"low_confidence:{total:.1f}", 0.0

    return True, reason, size_pct
```

### 4.4 持仓期间监控

持有期间不持续监控置信度，只做退出判断。退出条件见第 6 节。

> 为什么不监控置信度？因为 3 分钟的持仓窗口极短，置信度的波动是正常市场噪音，不应作为退出依据。真正需要关注的是**方向反转**（5% 止损）和**时间截止**（硬性平仓）。

---

## 5. 仓位管理

### 5.1 一次性建仓原则

满足入场条件后，一次性建仓完毕，不分批。理由：

```
末端窗口极短（仅 3 分钟），分批只会增加复杂性
方向确认 → 直接入场 → 持仓到结算或触发退出
```

### 5.2 仓位计算

```python
BASE_POSITION_USDC = 4.0

def calculate_position(
    confidence: EndWindowConfidence,
) -> float:
    """
    根据置信度等级计算仓位
    一次性建仓，仓位由置信度决定
    """

    if confidence.total >= 80:
        conf_multiplier = 1.0
    elif confidence.total >= 65:
        conf_multiplier = 0.8
    elif confidence.total >= 50:
        conf_multiplier = 0.5
    else:
        return 0.0  # 不满足置信度，不入场

    return BASE_POSITION_USDC * conf_multiplier
```

---

## 6. 退出机制

### 6.1 退出优先级（从高到低）

```
1. 硬性截止退出  → 到期前 15 秒强制平仓，不允许持仓到结算
2. 方向反转退出  → 末端方向已反转，触发止损
3. 止盈          → 浮盈达到目标
4. 止损          → 浮亏超过容忍
```

### 6.2 方向反转止损（最核心）

```python
# 末端止损原则：不允许扛单，不允许等反弹

# 基准：入场时（T=12min 进入末端窗口时）的价格
PRICE_AT_T12 = price at T=12min

# 反转止损阈值：顺势方向的价格反转超过此幅度 → 立即止损
DIRECTION_REVERSAL_STOP = 0.05  # 5%

def should_exit_on_reversal(
    direction: str,
    price_at_t12: float,
    current_price: float,
) -> bool:
    if direction == "UP":
        # UP 方向：当前价格跌破 T=12min 时的价格 5% → 止损
        if current_price < price_at_t12 * (1 - DIRECTION_REVERSAL_STOP):
            return True
    elif direction == "DOWN":
        # DOWN 方向：当前价格上涨超过 T=12min 时的价格 5% → 止损
        if current_price > price_at_t12 * (1 + DIRECTION_REVERSAL_STOP):
            return True
    return False
```

### 6.3 止盈（宽松）

```python
# 末端 3 分钟不需要太紧的止盈
# 方向正确时，让利润奔跑到结算

TP_ENTRY_MIN = 0.70   # 动态 TP 映射区间
TP_ENTRY_MAX = 0.90
TP_MIN_PCT   = 10.0   # 止盈下限（%）
TP_MAX_PCT   = 30.0   # 止盈上限（%）

def dynamic_tp(entry_price: float) -> float:
    """
    入场价越高（赔率越差），TP 目标越高（需要更多收益来弥补）
    入场价越低（赔率越好），TP 目标可以低一些
    """
    t = (entry_price - TP_ENTRY_MIN) / (TP_ENTRY_MAX - TP_ENTRY_MIN)
    t = max(0.0, min(1.0, t))
    return TP_MAX_PCT - (TP_MAX_PCT - TP_MIN_PCT) * t
```

### 6.4 硬性截止退出

```python
HARDCUT_SECONDS_BEFORE_EXPIRY = 15  # 到期前 15 秒强制平仓

def should_hardcut_exit(elapsed: float, window_duration: float) -> bool:
    time_remaining = window_duration - elapsed
    return time_remaining <= HARDCUT_SECONDS_BEFORE_EXPIRY
```

### 6.5 退出决策汇总

```python
def check_exit(
    elapsed: float,
    window_duration: float,
    direction: str,
    price_at_t12: float,
    current_price: float,
    pnl_pct: float,
    entry_price: float,
) -> tuple[bool, str]:
    """
    返回 (是否退出, 退出原因)
    """

    # E1: 硬性截止 → 强制退出（优先级最高）
    if should_hardcut_exit(elapsed, window_duration):
        return True, "HARDCUT_SETTLEMENT"

    # E2: 方向反转 → 止损（不允许扛单）
    if should_exit_on_reversal(direction, price_at_t12, current_price):
        return True, "DIRECTION_REVERSAL"

    # E3: 止盈
    tp = dynamic_tp(entry_price)
    if pnl_pct >= tp:
        return True, "TAKE_PROFIT"

    # E5: 止损
    if pnl_pct <= -15.0:
        return True, "STOP_LOSS"

    return False, "HOLD"
```

---

## 7. 与主策略的关系

### 7.1 互斥原则

```
同一 15m 窗口内，最多只有一个策略持仓

情况A：主策略已在窗口中段入场
  → 末端策略跳过（不复用，不加仓）
  → 末端只做退出管理（复用主策略持仓的退出逻辑）

情况B：主策略未入场，末端出现信号
  → 末端策略独立开仓
  → 主策略在下一窗口恢复

情况C：主策略和末端策略方向一致（主策略未入场时）
  → 视为一个整体持仓，不叠加仓位（末端独立持仓）

情况D：主策略和末端策略方向矛盾
  → 以主策略为准（主策略信号更早、更全面）
  → 末端策略跳过
```

### 7.2 窗口切换

```
窗口切换时（condition_id 变化）：
1. 清空末端策略的持仓状态
2. 新窗口重新从 T=0 开始判断
3. 主策略优先，末端策略在 T=12min 后激活
```

---

## 8. 风险控制

### 8.1 核心风控原则

| 原则 | 说明 |
|------|------|
| 不扛单 | 方向反转必须止损，不幻想反弹 |
| 不补仓摊平 | 不在错误方向上加仓 |
| 硬性截止 | 到期前 15 秒必须离场 |
| 仓位上限 | 单笔 ≤ 基准仓位 |
| 不叠加 | 同窗口不和主策略叠加 |
| 置信度门槛 | 总分 < 50 不入场 |

### 8.2 最大回撤控制

```python
MAX_DAILY_END_STRATEGY_LOSS = -20.0  # 每日末端策略最大亏损 20 USDC
MAX_SINGLE_TRADE_LOSS       = -15.0  # 单笔最大亏损 15%

def should_pause_end_strategy(daily_loss: float) -> bool:
    return daily_loss <= MAX_DAILY_END_STRATEGY_LOSS
```

### 8.3 动态风险信号

以下情况下末端策略应暂停：

```
1. 市场押注总量异常低（缺乏流动性，CLOB 价格不可靠）
2. 窗口内出现大单反向押注（被清洗风险高）
3. Polymarket Gamma API 响应异常（价格数据不可靠）
4. Binance 合约资金费率极度偏向一方（极端行情预警）
5. OI 变化率在末期窗口内急剧变化（新信息释放风险）
```

---

## 9. INVALID 风险处理

### 9.1 INVALID 是什么

Polymarket 有些市场在到期后被判定为 INVALID（条件无法判定，如 BTC 价格恰好等于阈值），此时：
- 所有 token 按比例分配剩余资金池
- 实际赎回价可能不是 0 或 1，而是中间值

### 9.2 INVALID 风险识别

```python
def is_invalid_risk_high(market: MarketInfo) -> bool:
    """判断市场是否 INVALID 风险较高"""

    # Neg Risk 市场（独立资金池）INVALID 风险较低
    if market.neg_risk:
        return False

    # UP 价格在 0.50 附近（偏离 < 0.03）→ INVALID 高风险
    if abs(market._up_mid - 0.50) < 0.03:
        return True   # 阈值附近，INVALID 风险高

    # 市场押注总量极低（缺乏足够的信息汇聚）
    if market.total_volume < MIN_VOLUME_THRESHOLD:
        return True

    return False
```

### 9.3 INVALID 时的处理

```
INVALID 发生时：
1. 结算价 ≠ 0 或 1，PnL 按实际结算价计算
2. 如果已通过 CLOB 平仓 → PnL 按实际平仓价计算，不受影响
3. 如果持有到结算 → PnL 可能比预期差

预防：INVALID 高风险市场 → 末端策略仓位减半或跳过
```

---

## 10. 配置参数

所有参数可通过 `POST /api/config` 运行时调整。

### 10.1 策略开关

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `end_strategy_enabled` | False | 是否启用末端策略 |
| `end_strategy_asset_btc` | True | BTC 是否启用 |
| `end_strategy_asset_eth` | True | ETH 是否启用 |

### 10.2 时机参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `end_window_start_sec` | 720 | 末端窗口开始（窗口开启后秒数） |
| `end_window_end_sec` | 870 | 末端窗口结束（窗口开启后秒数） |
| `end_hardcut_sec` | 885 | 硬性截止（到期前秒数） |

### 10.3 入场参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `end_min_entry_price` | 0.70 | 最低入场价格 |
| `end_min_deviation` | 0.05 | 最小偏离度（D2 最低门槛） |
| `end_confidence_threshold` | 50.0 | 入场最低置信度总分 |
| `end_strong_confidence` | 80.0 | 强信号置信度阈值 |
| `end_medium_confidence` | 65.0 | 中强信号置信度阈值 |

### 10.4 仓位参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `end_base_position_usdc` | 4.0 | 基准仓位（一次性建仓） |

### 10.5 退出参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `end_reversal_stop_pct` | 5.0 | 方向反转止损幅度（%） |
| `end_tp_min_pct` | 10.0 | 止盈下限（%） |
| `end_tp_max_pct` | 30.0 | 止盈上限（%） |
| `end_sl_pct` | -15.0 | 止损（%） |
| `end_hardcut_sec` | 15 | 硬性截止（秒） |

### 10.6 置信度权重

| 维度 | 默认权重 | 说明 |
|------|---------|------|
| D1 信号方向 | 0.15 | 辅助参考 |
| D2 价格偏离度 | 0.30 | 核心维度 |
| D3 趋势动能 | 0.30 | 核心维度 |
| D4 反转可靠性 | 0.25 | 核心维度 |

---

## 11. 数据源要求

> 所有数据均来自现有接口，无需新增外部 API 调用。

| 数据 | 来源 | 频率 | 用途 |
|------|------|------|------|
| Polymarket CLOB UP 中价 | `ctx.market._up_mid`（`MarketInfo`） | 每 tick | 方向确定 + D2 + D3 + D4 |
| Polymarket CLOB DOWN 中价 | `ctx.market._down_mid`（`MarketInfo`） | 每 tick | H2 入场价格确认 |
| 窗口起始 UP 中价 | `ctx.market.window_open_up`（`MarketInfo`） | 窗口开始时 | D2 + D3 基准 |
| BTC/ETH 现货价格 | `ctx.market.current_spot_price`（`MarketInfo`） | 每 tick | 参考（无需新增） |
| RSI（14-period） | `indicator_signal_service` | 每 tick | D1 信号 |
| Core 6 指标 | `indicator_signal_service` | 每 tick | D1 信号 |
| CVD | `indicator_signal_service` | 每 tick | D1 辅助 |
| Polymarket 结算结果 | `pm_client.get_market_resolution()` | 结算时 | 结算处理 |

> **唯一需要新增**：`WindowContext.up_samples: list[float]` — 在 `WindowManager.tick()` 中每分钟追加一条 `_up_mid`（保留最近 15 条），用于 D3 的 R² 计算和 D4 的反转检测。

> `MarketInfo` 的 `window_open_up`、`window_open_down`、`_up_mid`、`_down_mid` 字段均已有，直接复用。

---

## 12. 模块设计

建议新增以下文件，与现有 `app/strategy/` 模块并行：

```
app/strategy/
├── strategy.py               # 现有：SinglePositionStrategy（主策略）
├── end_window_strategy.py    # 新增：EndWindowStrategy（末端顺势策略，编排层）
├── end_window_signal.py      # 新增：四维置信度计算（信号层）
├── end_window_controller.py  # 新增：入场/出场控制（控制层）
├── window_manager.py         # 现有：复用（市场发现）
├── market_service.py         # 现有：复用（价格 + 现货）
├── position_manager.py       # 现有：复用（平仓 + 结算）
└── ...
```

### 12.1 `end_window_signal.py` — 信号层（核心）

```python
@dataclass
class EndWindowConfidence:
    D1_signal:    float
    D2_deviation: float
    D3_momentum:  float
    D4_reversal:  float
    total:        float

    WEIGHTS = {
        "D1_signal":    0.15,
        "D2_deviation": 0.30,
        "D3_momentum":  0.30,
        "D4_reversal":  0.25,
    }

    def compute_total(self) -> float:
        return sum(getattr(self, k) * v for k, v in self.WEIGHTS.items())


class EndWindowSignal:
    """
    计算末端顺势策略的四维置信度
    """

    def compute(
        self,
        window_open_up: float,
        current_up: float,
        up_samples: list[float],
        direction: str,
        indicator_service,  # IndicatorSignalService
    ) -> EndWindowConfidence:
        # 完整四维计算（见第 3 节）
        ...

    def compute_price_deviation(self, window_open_up, current_up, direction) -> float:
        ...

    def deviation_to_confidence(self, aligned_deviation: float) -> float:
        ...

    def compute_recent_momentum(self, up_samples: list[float]) -> float:
        ...

    def compute_trend_r2(self, up_samples: list[float]) -> float:
        ...

    def detect_reversal(self, up_samples: list[float], direction: str) -> tuple[bool, float]:
        ...

    def reversal_reliability(
        self,
        has_reversed: bool,
        reversal_depth: float,
        current_deviation: float,
    ) -> float:
        ...
```

### 12.2 `end_window_controller.py` — 控制层

```python
class EndWindowController:
    """
    管理末端策略的入场和退出
    一次性建仓，不分批
    """

    def __init__(self):
        self._has_position: dict[str, bool] = {}  # asset → bool

    def check_entry(
        self,
        asset: str,
        direction: str,
        current_mid: float,
        confidence: EndWindowConfidence,
        elapsed: float,
    ) -> Optional[float]:
        """
        根据置信度决定是否入场
        返回：入场金额（USDC），None = 不入场
        """
        if self._has_position.get(asset, False):
            return None  # 已有持仓，不重复入场

        size = calculate_position(confidence)
        return size if size > 0 else None

    def check_exit(
        self,
        elapsed: float,
        window_duration: float,
        direction: str,
        price_at_t12: float,
        current_price: float,
        pnl_pct: float,
        entry_price: float,
    ) -> tuple[bool, str]:
        # E1 硬性截止 → E2 方向反转 → E3 止盈 → E4 止损（见第 6 节）
        ...

    def set_position(self, asset: str, has_pos: bool):
        """设置持仓状态"""
        self._has_position[asset] = has_pos

    def reset(self, asset: str):
        """窗口切换时重置状态"""
        self._has_position[asset] = False
```

### 12.3 `end_window_strategy.py` — 策略编排层

```python
class EndWindowStrategy:
    """
    末端顺势策略：与 SinglePositionStrategy 并行运行
    """

    def __init__(self, main_strategy: SinglePositionStrategy):
        self.main       = main_strategy
        self.signal     = EndWindowSignal()
        self.controller = EndWindowController()

    def tick(self, ctx: WindowContext):
        """
        每个 tick 的执行顺序：
        1. 检查退出（优先级最高：硬性截止 → 方向反转 → 置信度跌破 → 止盈止损）
        2. 检查主策略是否已有持仓 → 有则跳过入场
        3. 检查是否在末端窗口 → 计算置信度 → 一次性建仓
        """
        ...
```

---

## 13. 回测验证清单

实现完成后，建议用历史数据验证以下假设：

| 假设 | 验证方法 |
|------|---------|
| 末端 0.80+ 方向实际胜率 > 隐含胜率 | 统计过去 N 个窗口的结算结果 |
| D2 现货偏离度能有效区分方向 | 对比：现货偏离 ≥ 2.0% vs < 0.5% 的末端入场胜率 |
| D3 趋势动能 + R² 能过滤假惯性 | 对比：有稳定趋势 vs 震荡/反转的末端入场胜率 |
| D4 反转检测能提升置信度 | 对比：有过反转 vs 未反转的末端入场胜率 |
| 分批入场优于一次梭 | 对比分批 vs 一次梭的夏普比率 |
| 方向反转止损有效 | 统计：反转止损 vs 持有到结算的期望收益 |
| INVALID 风险识别有效 | 统计：阈值附近的 INVALID 率 |

> **回测数据源**：Polymarket Gamma API 提供历史市场数据，可通过 `get_markets()` 拉取已结算市场列表。
