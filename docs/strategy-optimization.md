# 主策略优化与 A/B 指南

本次优化把主策略从「固定指标 + 固定 TP/SL」升级为可测量、可 A/B 的框架。

## 1. 信号层（P0）

- **Core 共识按方向对齐**：`core_agree` 只统计与 `primary_direction` 同向的指标；平票返回 SKIP。
- **反向信号上限**：`CORE_CONTRADICT_LIMIT`（默认 1），超过则跳过。
- **按已收盘 1m bar 计算**：RSI/MACD/EMA/VWAP/Pivot/BBands 都不再使用未收盘 bar；`get_signal` 每根 bar 只计算一次并缓存。
- **历史按分钟推进**：RSI/OI/CVD 历史每根 bar 只写一次，不再每 tick 重复追加。
- **CVD**：由 1m K 线的 `taker_buy_volume` 推导（`2*taker_buy - volume`），并按分钟维护 10 分钟 baseline。
- **OI**：按分钟维护 30 分钟历史，阈值 `OI_CHANGE_SIGNIFICANT`（默认 2%，可运行时调整）。
- **Liquidation**：策略启动时主动启动 Binance forceOrder WebSocket，不再依赖打开指标页面。

## 2. 出场 A/B（P1）

所有参数都可通过 `POST /api/config` 热更新：

| 参数 | 说明 |
|---|---|
| `exit_hold_to_settlement` | true = 忽略 TP/SL，持有到结算 |
| `exit_tp_enabled` / `exit_sl_enabled` | 分别开关 TP / SL |
| `exit_tp_sl_disable_after_elapsed_sec` | >0 时，窗口 elapsed 超过该秒数后不再 TP/SL，交给结算 |
| `exit_cost_buffer_pct` | TP 上移 / SL 下移的百分点，覆盖手续费+滑点 |
| `exit_trailing_stop_pct` | 移动止损回撤百分点（0=关闭） |
| `exit_trailing_activation_pct` | 移动止损激活的浮盈百分比 |

建议依次测试：
1. 当前 TP/SL（基线）
2. `EXIT_TP_SL_DISABLE_AFTER_ELAPSED_SEC=600`
3. `EXIT_HOLD_TO_SETTLEMENT=true`
4. 加入 `EXIT_COST_BUFFER_PCT=1.0`
5. 移动止损：`EXIT_TRAILING_STOP_PCT=5`、`EXIT_TRAILING_ACTIVATION_PCT=20`

## 3. 入场（P1）

- 时间窗：`ENTRY_EARLY_START_ELAPSED_SEC` / `ENTRY_LATE_CUTOFF_ELAPSED_SEC`
- 价格区间：`ENTRY_PRICE_MIN` / `ENTRY_PRICE_MAX`
- 可选 CLOB 方向确认：`ENTRY_REQUIRE_PRICE_ALIGNMENT=true`，`ENTRY_PRICE_ALIGNMENT_MIN_DEV=0.02`
- 流动性：`MIN_DEPTH_COVERAGE`、`MAX_SLIPPAGE_PCT`
- 实盘深度不足时直接跳过，不再只打日志

## 4. 仓位与风控（P2）

| 参数 | 说明 |
|---|---|
| `STAKE_MULT_CORE3` ~ `STAKE_MULT_CORE6` | 按 Core 共识数量配置仓位倍数 |
| `USE_KELLY_SIZING` | true = 用历史胜率 + 分数 Kelly 计算仓位（默认关闭） |
| `KELLY_FRACTION` | Kelly 折扣（默认 0.25） |
| `KELLY_PRIOR_WEIGHT` | 市场隐含概率先验权重（收缩小样本） |
| `KELLY_MIN_SAMPLES` | 少于此样本数回退到 Core 倍数 |
| `KELLY_MAX_MULT` | Kelly 仓位相对 `ENTRY_BASE_USDC` 的最大倍数 |
| `BANKROLL_USDC` | Kelly 计算使用的账户资金 |
| `MAIN_MAX_DAILY_LOSS_USDC` | 主策略每日已实现亏损上限（负值） |
| `MAIN_MAX_CONSECUTIVE_LOSSES` | 连续亏损次数上限（0=关闭） |
| `MAIN_LOSS_COOLDOWN_SEC` | 触发连续亏损后的冷却时间 |
| `MAX_OPEN_POSITIONS_TOTAL` | 主策略 + 末端策略 + 待结算的全局最大持仓数 |

Kelly 逻辑：`p_est` 用历史胜率按「市场价」做贝叶斯收缩，赔率取 TP/SL 或结算赔率，
`f=(p*b-(1-p))/b`，乘以 `KELLY_FRACTION` 并限制在 `KELLY_MAX_MULT` 以内。
如果 `p_est` 不高于盈亏平衡概率，直接跳过该笔交易。

风控状态会在启动时从当日 `ExitRecord` 恢复，不会因重启重置。

## 5. 因子模型（P0/P1 的进一步压缩）

`app/strategy/factor_model.py` 把原信号压缩为 5 个因子：

| 因子 | 组成 | 作用 |
|---|---|---|
| trend | MACD/EMA/Pivot/BBands（ATR 归一化） | 趋势/动量 |
| reversion | RSI 极值/交叉/背离 + VWAP 偏离 | 均值回归/衰竭 |
| price | CLOB 窗口偏离 + 近端斜率 + 反转 | 市场定价行为 |
| micro | Polymarket UP token OBI/深度/点差 | 微观结构 |
| flow | CVD + OI + Liquidation（Funding 暂不参与方向） | 资金流/持仓 |

每个因子输出 `score ∈ [-1,1]` 和 `confidence ∈ [0,1]`，合成 `net_score`、`agreement`、`conflict`。

### 模式

- `FACTOR_MODE=0`：关闭，保持旧逻辑。
- `FACTOR_MODE=1`：shadow，只记录因子，不改变交易。
- `FACTOR_MODE=2`：live，用因子方向/一致性做入场，仓位由因子强度决定。

权重和阈值均可通过 `/api/config` 调整：

```text
FACTOR_W_TREND / FACTOR_W_REVERSION / FACTOR_W_PRICE / FACTOR_W_MICRO / FACTOR_W_FLOW
FACTOR_MIN_NET / FACTOR_MIN_AGREEMENT / FACTOR_REV_VETO_SCORE
FACTOR_MAX_SPREAD_BPS / FACTOR_OI_CHANGE_THRESHOLD
```

### 因子验证

```bash
GET /api/analysis/factors?days=30
GET /api/analysis/factors?days=30&only_entered=true
```

返回：
- 因子两两相关矩阵（|ρ|>0.7 说明冗余，需要合并/正交化）
- 因子分数与 Trade PnL 的 IC
- 盈利/亏损交易的平均因子分数

建议先跑 **shadow 模式 2~4 周**，确认相关性和 IC 后再切 live。

## 6. 数据记录与分析

新表 `decision_snapshots`：每根 bar 保存一次主策略完整信号快照（Core 6、Advanced 5、价格、方向、共识数、是否入场）。

### 查看表现

```bash
GET /api/analysis/performance?days=30
GET /api/analysis/performance?days=30&strategy=main
GET /api/analysis/signal-outcomes?days=30
GET /api/analysis/decisions?limit=200
GET /api/analysis/decisions?only_entered=true
```

`performance` 返回：
- overall / by_strategy / by_core_agree / by_price_bucket / by_close_reason
- trades、win_rate、total_pnl_usd、avg_pnl_usd、return_pct、profit_factor

### 建议的分析顺序

1. 按 `core_agree` 看 3/6、4/6、5/6、6/6 的真实胜率和 EV，决定仓位倍数是否有依据。
2. 按入场价格桶看哪一段赚钱，调整 `ENTRY_PRICE_MIN/MAX`。
3. 对比 `exit_hold_to_settlement` / `exit_tp_sl_disable_after_elapsed_sec` 的 PnL。
4. 用 `decision_snapshots` 分析单个指标（RSI/MACD/VWAP/...）的增量价值，淘汰无效信号。
5. 用 walk-forward 验证，避免过拟合。

> 注意：`/api/analysis/performance` 依赖 `Trade.strategy` 和 `Trade.entry_core_agree`，
> 旧数据库会在启动时自动补列；旧记录默认归为 `main`。

### 关于完整历史回测

本次提供的是「实盘/干跑数据的表现基线 + 决策快照」。严格的历史 tick 级回测还需要
Polymarket CLOB 的 token 价格历史/盘口快照；当前仓库没有接入这部分数据源。
建议先用真实运行数据积累样本，再决定是否投入完整回测引擎。
