# Polymarket 5m Strategy (v2)

基于 Polymarket CLOB v2 API 的量化交易机器人，交易 BTC/ETH 5分钟 Up/Down 预测市场。

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
# 编辑 .env 填入 JWT_SECRET_KEY 和其他配置
```

## 启动

```bash
python run.py
```

访问 http://localhost:8002

## 功能

- **策略**: Polymarket 15m Up/Down 马丁加仓 + 单笔止盈百分比 + 窗口结算
- **实盘**: CLOB v2 API (py_clob_client_v2)
- **回测**: Dome API 历史数据
- **WebSocket**: 实时价格和策略状态推送
- **认证**: JWT Bearer Token

## 目录结构

```
polymarket/
├── run.py
├── requirements.txt
├── .env
├── app/
│   ├── main.py              # FastAPI lifespan
│   ├── config.py            # 静态设置
│   ├── runtime_config.py    # 运行时配置
│   ├── models.py           # SQLAlchemy 模型
│   ├── database.py         # 数据库
│   ├── auth.py             # JWT 认证
│   ├── pm_client.py        # Gamma + CLOB 公开 API
│   ├── clob_http.py        # L2 HMAC（v2 SDK 内部处理）
│   ├── clob_l1_auth.py     # L1 钱包认证（v2 SDK）
│   ├── polygon_usdc.py      # Polygon pUSD/USDC 余额
│   ├── live_trader.py      # 实盘交易（v2 SDK）
│   ├── strategy/            # 策略引擎
│   ├── backtest/           # 回测引擎
│   ├── api/                # REST API + WebSocket
│   └── static/
├── templates/               # HTML 页面
└── data/                   # SQLite 数据库（运行时）
```
