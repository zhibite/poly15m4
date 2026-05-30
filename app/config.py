"""
config.py — Static settings from .env (pydantic-settings).
"""

from pathlib import Path
import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_name: str = "Polymarket 15m Single"

    polygon_rpc_url: str = "https://polygon-bor.publicnode.com"

    gamma_api: str = "https://gamma-api.polymarket.com"
    polymarket_host: str = "https://clob.polymarket.com"

    # Auth
    jwt_secret_key: str = ""  # MUST be set via environment variable, NOT in .env file
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 1440

    db_url: str = f"sqlite+aiosqlite:///data/polymarket.db"

    assets: str = "BTC"

    # Gamma：`{asset}-updown-{market_interval_slug}-{ts}`
    market_interval_slug: str = "15m"
    market_duration_sec: int = 900

    trade_asset_btc: bool = True
    trade_asset_eth: bool = False

    # 单持仓首单金额（实际生效值由 runtime_config.py 控制）
    entry_base_usdc: float = 4.0

    # 窗口期：窗口开启后超过此时长则禁止开单（6分钟 = 360s）
    entry_skip_if_late: bool = True
    entry_late_cutoff_elapsed_sec: float = 540.0

    poll_interval_sec: float = 1.0
    settle_grace_sec: int = 120
    max_settle_wait_sec: int = 900
    sell_max_retry_count: int = 3

    live_mode: bool = False
    live_slippage_pct: float = 2.0
    btc_slippage_pct: float = 1.0   # Per-asset slippage: BTC (higher liquidity → tighter)
    eth_slippage_pct: float = 2.0   # Per-asset slippage: ETH (lower liquidity → wider)
    min_exit_price: float = 0.01
    # fee_bps: deprecated — use default_fee_bps instead
    default_fee_bps: int = 30  # Used when fee-rate API call fails

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
        # JWT_SECRET_KEY must come from environment variable — never from .env
        "env_ignore_empty": True,
    }


import logging

logger = logging.getLogger(__name__)

settings = Settings()

# ── JWT_SECRET_KEY: MUST be set via environment variable (never hardcoded in .env) ──
_env_jwt_secret = os.environ.get("JWT_SECRET_KEY", "").strip()
if _env_jwt_secret:
    settings.jwt_secret_key = _env_jwt_secret

if not settings.jwt_secret_key:
    raise ValueError(
        "JWT_SECRET_KEY is not set in the environment — cannot start without it. "
        "Set it via:  export JWT_SECRET_KEY=$(python -c \"import secrets; print(secrets.token_urlsafe(64))\")"
    )
