# Polymarket 15m 单仓信号驱动策略 — 逻辑流程文档

> 本文档完全基于 `app/strategy/` 目录下各控制器源代码重建，涵盖信号发现逻辑、入场决策流程、止盈止损机制及退出流程。
>
> 更新时间：2026-05-31
> 代码版本：v2（2026-05-22 重构后的单仓版）

---

## 目录

1. [系统概述](#1-系统概述)
2. [主循环架构](#2-主循环架构)
3. [信号发现 — Core 6 指标](#3-信号发现--core-6-指标)
4. [信号发现 — Advanced 5 过滤器](#4-信号发现--advanced-5-过滤器)
5. [入场决策流程](#5-入场决策流程)
6. [仓位确定](#6-仓位确定)
7. [止盈止损阈值计算](#7-止盈止损阈值计算)
8. [退出决策 — 三重门系统](#8-退出决策--三重门系统)
9. [标准止盈止损（兜底）](#9-标准止盈止损兜底)
10. [窗口结算](#10-窗口结算)
11. [配置参数速查](#11-配置参数速查)
12. [状态机与边界条件](#12-状态机与边界条件)

---

## 1. 系统概述

**交易标的**：Polymarket BTC / ETH 15 分钟 Up/Down 二元预测市场（每个窗口 900 秒）

**核心设计**：单单持仓 — 每个品种在一个窗口周期内最多只入场一次，入场后只做止盈止损/窗口结算，不再补仓

**数据源**：
- Polymarket CLOB v2 API — 市场发现、实时价格
- Binance 现货 API（K线、aggTrades）— 技术指标计算
- Binance 永续合约 API（Funding、OpenInterest）— 高级过滤

**主循环频率**：每 `poll_interval_sec` 秒执行一次（默认 1 秒）

---

## 2. 主循环架构

`SinglePositionStrategy.run_forever()` 的每个 tick 执行顺序：

```
┌─────────────────────────────────────────────────────────┐
│  每个 tick                                                 │
│  1. _sync_config()     — 热加载最新参数（支持运行时调参）     │
│  2. _wm.tick()         — 并行发现窗口 + 获取价格 + 现货价     │
│  3. 指标预取            — K线/CVD 实时取，Funding/OI/OBI 缓存 │
│  4. _ind_svc[asset]    — 计算 Core 6 + Advanced 5 信号      │
│  5. EXIT CHECK          — 已有持仓的品种 → check_exit()      │
│  6. ENTRY CHECK         — 无持仓的品种 → check_entry()       │
│  7. FOK RETRY           — 处理 Fill-Or-Kill 重试队列          │
│  8. _push_state()       — 每 0.5s 推送 WS 状态快照           │
│  9. sleep(poll_interval_sec)                               │
└─────────────────────────────────────────────────────────┘
```

**入场和退出的优先级**：退出优先于入场（ops 列表中含持仓品种会排在最前）

---

## 3. 信号发现 — Core 6 指标

Core 6 用于**入场决策时的方向确认**，共识数量决定是否入场及仓位大小。

### 3.1 RSI（带极端反转 + 背离检测）

| 条件 | 信号方向 | agree | 说明 |
|------|---------|-------|------|
| 从 70+ 回落 | `DOWN` | ✅ | 超买区域反转，做空 |
| 从 30- 反弹 | `UP` | ✅ | 超卖区域反弹，做多 |
| 价格↑ RSI 高点↓ | `DOWN` | ✅ | RSI 看跌背离，最强信号 |
| 价格↓ RSI 低点↑ | `UP` | ✅ | RSI 看涨背离，最强信号 |
| RSI ≥ 70 | `SKIP` | ❌ | 超买区，等回落，不入 |
| RSI ≤ 30 | `SKIP` | ❌ | 超卖区，等反弹，不入 |
| 45 ≤ RSI ≤ 55 | `SKIP` | ❌ | 中性区，横盘，跳过 |
| RSI > 55 | `UP` | ❌ | 偏多但未确认，不计入共识 |
| RSI < 45 | `DOWN` | ❌ | 偏空但未确认，不计入共识 |

> 背离检测（`divergence`）：优先级最高，在超买/超卖判断之前检测

### 3.2 MACD（动能柱 + ATR 过滤器）

- **横盘过滤**：`ATR_dollar < 当前价格 × 0.04%` → MACD 视为无效，返回 `SKIP`
- ATR 低于阈值说明价格几乎不动，MACD 信号不可靠

| 条件 | 信号方向 | agree |
|------|---------|-------|
| 直方图绿色 + 放大（> 上一根） | `UP` | ✅ |
| 直方图绿色 + 收缩 | `SKIP` | ❌ |
| 直方图红色 + 放大（< 上一根） | `DOWN` | ✅ |
| 直方图红色 + 收缩 | `SKIP` | ❌ |
| 向零轴收缩 | `SKIP` | ❌ |

### 3.3 VWAP（需 RSI + MACD 组合确认）

VWAP 信号依赖 RSI 和 MACD 的双重确认才能计入 Core 共识。

| 条件 | 信号方向 | agree |
|------|---------|-------|
| 价格 > VWAP + RSI>50 + MACD UP | `UP` | ✅ |
| 价格 < VWAP + RSI<50 + MACD DOWN | `DOWN` | ✅ |
| 价格 > VWAP + RSI>70 | `DOWN` | ❌（均值回归预警） |
| 价格 < VWAP + RSI<30 | `UP` | ❌（均值回归预警） |
| 价格 > VWAP + 有 RSI 或 MACD 信号 | `UP` | ❌（无强确认） |
| 价格 ≈ VWAP | `SKIP` | ❌ |

### 3.4 EMA 9/21（交叉是 Core 信号）

| 条件 | 信号方向 | agree |
|------|---------|-------|
| EMA9 上穿 EMA21（金叉） | `UP` | ✅ |
| EMA9 下穿 EMA21（死叉） | `DOWN` | ✅ |
| EMA9 > EMA21（多头排列，无交叉） | `UP` | ❌ |
| EMA9 < EMA21（空头排列，无交叉） | `DOWN` | ❌ |
| 数据不足（< 22 根 K线） | `SKIP` | ❌ |

> 交叉瞬间是 Core 信号，无交叉但趋势已成型时给方向但不计入共识

### 3.5 Bollinger Bands（动量顺势）

| 条件 | 信号方向 | agree |
|------|---------|-------|
| 价格触及/突破布林上轨 | `UP` | ✅ |
| 价格触及/跌破布林下轨 | `DOWN` | ✅ |
| 价格在布林带内 | `SKIP` | ❌ |

> 与旧版均值回归逻辑不同：现版为动量顺势逻辑（价格触及上轨做多动能顺势）

### 3.6 Pivot Point（10根K线高低点突破）

| 条件 | 信号方向 | agree |
|------|---------|-------|
| 价格 > HL10 高点 | `UP` | ✅ |
| 价格 < HL10 低点 | `DOWN` | ✅ |
| 价格在 HL10 中间 | `SKIP` | ❌ |

---

## 4. 信号发现 — Advanced 5 过滤器

Advanced 5 **不参与 Core 共识计数**，但会触发以下过滤动作：

### 4.1 CVD（累计成交量 delta）

- 基于 Binance aggTrades 实时计算
- Z-score > 1σ 且 CVD 方向与 Core 方向**相反** → **强制跳过（SKIP）**
- Z-score > 1σ 且 CVD 方向与 Core 方向**一致** → **确认信号（CONFIRM）**

> CVD 反向是最强单信号，优先级高于所有其他 Core 指标

### 4.2 Funding（永续资金费率）

| 条件 | 信号方向 | 动作 |
|------|---------|------|
| Funding > +0.05% | `UP` | 多头给空头钱，做多情绪强 |
| Funding < -0.05% | `DOWN` | 空头给多头钱，做空情绪强 |
| Funding > +0.08%（极端） | 反向 | **减仓 50%**（REDUCE） |
| 中性 | `SKIP` | 无动作 |

### 4.3 OBI（订单簿深度比）

| 条件 | 信号方向 | 动作 |
|------|---------|------|
| OBI > 1.5 | `UP` | 买方深度占优 |
| OBI < 0.67 | `DOWN` | 卖方深度占优 |
| OBI 反向与 Core | 反向 | **减仓 50%**（REDUCE） |
| 中性 | `SKIP` | 无动作 |

### 4.4 OI（持仓量变化率）

- 基准：使用**窗口起始时 OI**（历史第一个值），而非 tick-to-tick 差值
- 窗口期累计变化 ≥ 10% 视为显著

| 价格方向 | OI 变化 | 信号 |
|---------|--------|------|
| 价格↑ + OI↑ | UP 顺势 | 确认 |
| 价格↑ + OI↓ | DOWN | 空头平仓，动力不足 |
| 价格↓ + OI↑ | DOWN | 新空入场，趋势延续 |
| 价格↓ + OI↓ | UP | 多头投降，可能反弹 |

### 4.5 Liquidation（清算热力图）

- 基于 Binance aggTrades 模拟清算数据（30分钟滚动窗口）
- 空头清算密集区在上方 → `up_magnet` → UP 信号
- 多头清算密集区在下方 → `down_magnet` → DOWN 信号
- 信号与 Core 同向时确认，反向时提供参考

---

## 5. 入场决策流程

`EntryController.check_entry()` 的完整决策链：

```
入场检查开始
│
├─ [1] 无 MarketInfo → 不入场（no_market）
│
├─ [2] 时间窗口检查
│    ├─ is_fresh（新窗口第一 tick）：
│    │   用 discover_market 时计算的 fresh_elapsed（避免 datetime.min 干扰）
│    └─ elapsed ≥ 540s（窗口开启后 9 分钟）→ 跳过（window_closed）
│
├─ [3] 窗口已标记为跳过 → 不入场（window_skipped）
│
├─ [4] 已有持仓 → 不入场（position_exists）
│
├─ [5] 价格区间检查
│    ├─ up_mid ≤ 0 或 down_mid ≤ 0 → 不入场（数据无效守卫）
│    └─ 实际入场价格 price < 0.20 或 > 0.60 → 不入场（价格区间过滤）
│    ├─ 计算 Core 6 信号
│    ├─ 计算 Advanced 5 过滤
│    └─ 返回 direction / core_agree / filter_result
│
├─ [6] 调用 IndicatorSignalService.get_signal()
│    ├─ 计算 Core 6 信号
│    ├─ 计算 Advanced 5 过滤
│    └─ 返回 direction / core_agree / filter_result
│
├─ [7] Advanced 过滤器 → SKIP
│    └─ CVD 反向 → 不入场（no_signal）
│
├─ [8] Core 共识 < 3/6
│    └─ 不入场（no_signal）
│
├─ [9] 确定方向（UP 或 DOWN）
│    └─ 根据 Core 多数方向
│
├─ [10] 确定仓位（见第 6 节）
│
├─ [11] 实际入场金额 < 1 USDC → 不入场（CLOB 最小交易门槛）
│
└─ [13] 执行入场（live 或 dry）
      ├─ live：调用 live_trader.buy_token（FOK 订单）
      └─ dry：模拟滑点后记录持仓
```

---

## 6. 仓位确定

基于 Core 6 同向共识数量，动态调整入场金额（基准 4 USDC）：

| Core 共识 | 仓位 | 实际金额（基准 4U） |
|---------|------|-------------------|
| 3/6 | 0.5× | 2 USDC |
| 4/6 | 1.0× | 4 USDC |
| 5/6 | 1.5× | 6 USDC |
| 6/6 | 2.0× | 8 USDC |
| < 3/6 | ❌ 不入场 | — |

> Advanced 5 中 CVD 反向 → 强制 SKIP（不论 Core 数量）

---

## 7. 止盈止损阈值计算

入场成功后，`_calc_stop_levels()` 根据入场价计算阈值，写入 `OpenPosition`：

### 7.1 止盈阈值（TP）

**动态公式**：入场价越低，止盈目标越高（线性插值）

```
t = (entry_price - tp_entry_min) / (tp_entry_max - tp_entry_min)
TP = tp_high_min + (tp_high_max - tp_high_min) × t
```

**简化推导**（代入默认值 tp_high_min=30, tp_high_max=10, tp_entry_min=0.20, tp_entry_max=0.60）：

```
TP = 30 - 20 × t
   = 30 - 20 × (entry_price - 0.20) / 0.40
   = 40 - 50 × entry_price
```

| 入场价 | TP 目标 |
|--------|---------|
| 0.20 | +30% |
| 0.30 | +25% |
| 0.40 | +20% |
| 0.50 | +15% |
| 0.60 | +10% |

> **参数说明**：仅需 2 个核心参数即可确定完整线性关系：
> - `tp_high_min`（低价入场时的 TP上限）和 `tp_high_max`（高价入场时的 TP下限）确定**斜率**
> - `tp_entry_min/max` 确定**入场价有效区间**（防止边界外异常定价）

### 7.2 止损阈值（SL）

**固定阈值 + 信号确认**：

```
亏损 <= -20%  → 无条件立即止损
亏损 > -20% 但 <= -10%：
    信号同向 >= 3/6 → 继续持有，等待反转盈利
    信号不足 < 3/6  → 全部止损退出
```

| 亏损幅度 | 信号条件 | 动作 |
|---------|---------|------|
| ≤ -20% | 任意 | 立即止损 |
| > -20% 且 ≤ -10% | 同向 ≥ 3/6 | 继续持有 |
| > -20% 且 ≤ -10% | 同向 < 3/6 | 全部止损 |

---

## 8. 退出决策 — 三重门系统

`ExitController._apply_three_gates()` 在每个 tick 执行，优先级从高到低：

### Gate 1 — 信号反向早平（优先级最高）

**Gate 1a：CVD 反向（最强单信号）**
- 条件：`CVD Z-score > 1.0` 且 CVD 方向与**持仓方向**相反
- 动作：**立即退出**，不等待其他条件
- 触发 `STOP_LOSS_EARLY`

**Gate 1b：Core 6 信号反转**
- 条件：Core 6 中有 **≥ 4/6** 个指标信号方向与**持仓方向相反**
- 动作：**立即退出**
- 触发 `STOP_LOSS_EARLY`

### Gate 2 — TP 命中但信号续持

- 条件：`PnL% ≥ dynamic_tp` 且 Core 6 同向共识 **≥ 3/6**
- 动作：**跳过止盈，继续持仓**
- 理由：信号确认趋势延续，不应在此处止盈
- 触发 `GATE2_SKIP_TP`

### Gate 3 — 信号减弱且盈利 → 退出

- 条件：Core 6 同向共识 < 3/6 **且** `PnL% > 0`（盈利状态）
- 动作：**退出，锁定利润**
- 理由：信号已转弱但仍有浮盈，此时退出优于等待 SL
- 触发 `GATE3_SIGNAL_WEAKEN`

---

## 9. 标准止盈止损（兜底）

当三重门均未触发时，执行 `_apply_standard_exit()`：

### 止损
- `PnL% ≤ dynamic_sl`（ATR 自适应止损）→ **立即退出**，触发 `STOP_LOSS`

### 止盈
- `PnL% ≥ dynamic_tp` 且 Core 同向 < 3/6 → **触发全部止盈**，触发 `TAKE_PROFIT`
- `PnL% ≥ dynamic_tp` 且 Core 同向 ≥ 3/6 → **跳过，继续持有**

---

## 10. 窗口结算

### 窗口末期保护

- 窗口剩余时间 **< 60 秒** → **强制持有至结算**，不执行任何退出
- 理由：末期退出摩擦成本（手续费 + 滑点）可能超过收益

### 结算流程

窗口到期时，`PositionManager.settle_if_due()` 执行：

1. 调用 Polymarket API 轮询窗口结算状态（最多等待 `max_settle_wait_sec`）
2. 获取结算结果（`up_wins` 或 `down_wins`）
3. **UP 胜出** → UP token exit = 1.0，DONE token exit = 0.0
4. **DOWN 胜出** → UP token exit = 0.0，DONE token exit = 1.0

### 平仓执行顺序（按退出原因）

| 退出原因 | 平仓方式 |
|---------|---------|
| TP / SL / 移动止损 / 早平 | CLOB 卖出（最多 3 次重试） |
| CLOB 卖出失败（3次重试后） | 链上赎回兜底（redeem） |
| 窗口结算 | 结算价直接平仓（覆盖价 = 0.0 或 1.0） |

---

## 11. 配置参数速查

所有参数可通过 `/api/config` 运行时调整，也可在 `.env` 文件中设置。

### 入场参数

| 参数名 | 默认值 | 说明 |
|--------|--------|------|
| `entry_base_usdc` | 4.0 | 基准仓位金额 |
| `entry_early_start_elapsed_sec` | 0.0 | 最早可入场时间（窗口开启后秒数） |
| `entry_late_cutoff_elapsed_sec` | 540.0 | 入场截止时间（窗口开启后秒数） |
| `core_agree_threshold` | 3 | Core 6 最低共识门槛 |
| `entry_price_min` | 0.20 | 最低入场价格 |
| `entry_price_max` | 0.60 | 最高入场价格 |

### 止盈止损参数

> **公式**：`TP(%) = (tp_high_min - tp_high_max) / (tp_entry_max - tp_entry_min) × entry_price + tp_high_min - slope × tp_entry_min`
> 代入默认值化简为：`TP = 40 - 50 × entry_price`

| 参数名 | 默认值 | 说明 |
|--------|--------|------|
| `tp_high_min` | 30.0 | 入场价 0.20 时的 TP 上限（%） |
| `tp_high_max` | 10.0 | 入场价 0.60 时的 TP 下限（%） |
| `tp_entry_min` | 0.20 | TP 映射区间下限（入场价低于此不入场） |
| `tp_entry_max` | 0.60 | TP 映射区间上限（入场价高于此不入场） |

### 交易参数

| 参数名 | 默认值 | 说明 |
|--------|--------|------|
| `live_mode` | False | 是否实盘交易 |
| `btc_slippage_pct` | 1.0 | BTC 滑点容忍 |
| `eth_slippage_pct` | 2.0 | ETH 滑点容忍 |
| `default_fee_bps` | 30 | 默认手续费（bps） |
| `min_exit_price` | 0.01 | 最低卖出价格 |
| `sell_max_retry_count` | 3 | 卖出最大重试次数 |

---

## 12. 状态机与边界条件

### PositionState 枚举

| 状态 | 说明 |
|------|------|
| `NO_POSITION` | 无持仓，可入场 |
| `HAS_POSITION` | 持有头寸，等待退出 |
| `PENDING_CLOSE` | 平仓中（FOK 挂单未成交） |

### 孤儿持仓检测（R11 机制）

- 入场成功但数据库写入失败时，pos 上设置 `_is_orphan_position = True`
- 策略重启时，`_check_position_integrity()` 自动将孤儿 Fill 记录关联到最近一笔已平仓 Trade
- 平仓成功后重置孤儿标记

### 窗口切换处理

- 检测到 `condition_id` 变化 → `WindowManager._on_new_window()`
- 重置：traded_sides、position、pending_close、entry_lock
- 重置：`_window_skipped`、`_fresh_elapsed`（正确的 elapsed 值由 discover_market 时计算）
- 取消旧 condition_id 的决策日志轮询

### FOK 重试机制

- 入场 FOK 失败时，将订单加入 RetryManager 队列
- 按指数退避等待（最大 3 次重试）
- 重试时使用最新市场价格
- 重试队列满后放弃并记录错误

---

## 附录：指标数据缓存策略

| 数据 | 缓存 TTL | 说明 |
|------|---------|------|
| BTC/ETH 现货价格 | 30 秒 | 一次请求两品种共用 |
| Funding Rate | 30 秒 | Binance 永续 |
| Open Interest | 30 秒 | Binance 永续 |
| OBI（订单簿深度） | 5 秒 | 更短的刷新频率 |
| K线数据 | 每 tick 实时 | 500 根 1m K线 |
| CVD | 每 tick 实时 | 500 条 aggTrades |
