# Polymarket 15m Strategy (v2)

基于 Polymarket CLOB v2 API 的量化交易机器人，交易 BTC/ETH 15 分钟 Up/Down 预测市场。

当前只保留一套主策略：

- **主策略**：窗口前 9 分钟内的信号驱动单仓交易（Core 6 指标共识 + Advanced 5 过滤）

## 主要变更 (v2 vs v1)

| 项目 | V1 | V2 |
|------|-----|-----|
| SDK | `py-clob-client` | `py-clob-client-v2` |
| 抵押品 | USDC.e | pUSD |
| 手续费 | 嵌入订单签名 | 协议匹配时确定 |
| Nonce | 需要 | 不需要（timestamp 替代） |
| EIP-712 域版本 | "1" | "2"（Exchange） |

## 安装

```bash
pip install -r requirements.txt
cp .env.example .env
# JWT_SECRET_KEY 必须通过环境变量提供（不要写进 .env）
export JWT_SECRET_KEY=$(python -c "import secrets; print(secrets.token_urlsafe(64))")
# 编辑 .env 填入钱包 / API 凭证和其他配置
```

## 启动

```bash
python run.py
```

访问 http://localhost:8019

## 功能

- **主策略**: Core 6 指标（RSI / MACD / VWAP / EMA / Pivot / BBands）共识入场，Advanced 5（Funding / CVD / 清算 / OBI / OI）过滤；动态止盈、固定止损、窗口结算
- **观察价止盈**: 止盈触发后以 `tp_price` 作为 FAK 限价卖出；如果盘口跳空到更高 bid，按实际观察到的更高价成交
- **实盘**: CLOB v2 API（FOK 买入、FAK 卖出、链上 redeem 兜底）
- **手续费**: 按官方 `shares × fee_rate × p × (1-p)` 计算，从 PnL 中扣除，并写入 EntryRecord / ExitRecord / Trade
- **WebSocket**: 实时策略状态、盘口价格、技术指标推送
- **认证**: JWT Bearer Token / HttpOnly Cookie
- **配置**: 运行时热更新（`POST /api/config`），实盘切换需二次确认
- **分析**: 决策快照 + 表现统计（`/api/analysis/performance`、`/api/analysis/decisions`）
- **因子模型**: 5 因子压缩（trend/reversion/price/micro/flow），支持 shadow/live 模式与相关性/IC 分析（`/api/analysis/factors`）
- **风控**: 每日亏损上限、连续亏损冷却、单窗口单仓

## 目录结构

```
poly15m4/
├── run.py                   # uvicorn 入口（端口 8019）
├── requirements.txt
├── .env.example
├── app/
│   ├── main.py              # FastAPI lifespan
│   ├── config.py            # 静态设置（pydantic-settings）
│   ├── runtime_config.py    # 运行时配置 + 钱包凭证
│   ├── models.py            # SQLAlchemy 模型
│   ├── database.py          # SQLite / aiosqlite
│   ├── auth.py              # JWT + bcrypt
│   ├── pm_client.py         # Gamma + CLOB 公开 API
│   ├── polygon_usdc.py      # Polygon pUSD/USDC 余额与链上操作
│   ├── live_trader.py       # CLOB v2 实盘交易
│   ├── strategy/            # 策略引擎
│   │   ├── strategy.py          # 主循环 + 到期结算队列
│   │   ├── window_manager.py    # 窗口发现与切换
│   │   ├── market_service.py    # 市场/价格查询
│   │   ├── indicator_signal_service.py  # Core 6 + Advanced 5
│   │   ├── entry_controller.py  # 主策略入场
│   │   ├── exit_controller.py   # 主策略止盈止损 / 移动止损 / 出场策略
│   │   ├── position_manager.py  # 平仓/结算/DB 记录
│   │   ├── risk_manager.py      # 日亏损 / 连续亏损风控
│   │   ├── factor_model.py      # 5 因子压缩模型
│   │   └── liquidity.py         # 盘口 OBI / 深度辅助
│   └── api/                 # REST API + WebSocket
├── templates/               # HTML 页面（login/dashboard/trades/config/indicators）
├── docs/                    # 策略文档
└── data/                    # SQLite 数据库（运行时创建）
```

## 配置说明

- 所有策略参数可通过 `POST /api/config` 热更新，并持久化到 `.env`
- 实盘切换：先 `POST /api/live/confirm`，再在 5 分钟内把 `live_mode` 设为 `true`
- 手续费率：`TAKER_FEE_RATE`（默认 0.07；maker 为 0）
- 观察价止盈：`TP_OBSERVED_EXIT=true`（默认开启）

## 安全说明

- 除 `/api/auth/*` 外，所有 REST API 都需要 JWT（`Authorization: Bearer` 或 `pm_token` Cookie）；WebSocket 同样校验 Cookie / `?token=`
- `python run.py` 默认只监听 `127.0.0.1`；容器中由 Dockerfile / docker-compose 设置 `HOST=0.0.0.0`
- Web 登录账号推荐通过环境变量 `WEB_USERNAME` / `WEB_PASSWORD` 设置（Coolify 面板可直接配置）。启动时会创建或更新对应 admin 用户，密码以环境变量为准
- 未设置 `JWT_SECRET_KEY` 时会从 `WEB_PASSWORD` 派生 JWT 签名密钥；生产环境仍建议单独设置 `JWT_SECRET_KEY`
- 建议设置 `SETUP_TOKEN`，防止首次创建用户（admin）被他人抢占；设置后注册时必须提供 `setup_token`
- 不要把包含 `WALLET_PRIVATE_KEY` / `API_SECRET` 的 `.env`、`.env.docker` 提交到版本库

## 文档

- [策略优化与 A/B 指南](docs/strategy-optimization.md)
- [PnL 计算规则](docs/polymarket-pnl-logic.md)
- [指标用法参考](docs/1.md)
