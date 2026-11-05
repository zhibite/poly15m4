"""
config.py — Static settings from .env (pydantic-settings).
"""

import logging
import os
from pathlib import Path

from pydantic import AliasChoices, Field
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
    # Web 登录管理员（推荐通过 Coolify / 系统环境变量设置）。
    # 设置 WEB_PASSWORD 后，启动时会创建/更新该 admin 用户，并以环境变量为准。
    web_username: str = "admin"
    web_password: str = ""
    # 可选：首次创建用户时要求提供的一次性初始化口令。
    # 为空时保持旧行为（首个注册用户自动成为 admin）。
    setup_token: str = ""

    # Accept both DATABASE_URL (documented/docker) and DB_URL (legacy field name).
    db_url: str = Field(
        default="sqlite+aiosqlite:///data/polymarket.db",
        validation_alias=AliasChoices("DATABASE_URL", "DB_URL"),
    )

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
    # Polymarket crypto taker fee rate: fee = shares * rate * p * (1-p).
    # Makers pay 0; the reference implementation uses 0.07 for these markets.
    taker_fee_rate: float = 0.07

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
        # JWT_SECRET_KEY must come from environment variable — never from .env
        "env_ignore_empty": True,
    }


settings = Settings()

# ── JWT_SECRET_KEY: MUST be set via environment variable (never hardcoded in .env) ──
# Normalize env-provided web credentials (Coolify variables often include
# accidental surrounding whitespace).
settings.web_username = (settings.web_username or "admin").strip() or "admin"
settings.web_password = (settings.web_password or "").strip()
if settings.web_password and len(settings.web_password) < 6:
    raise ValueError(
        "WEB_PASSWORD is too short — minimum 6 characters. "
        "Set a stronger value in the Coolify/environment variables."
    )

_env_jwt_secret = os.environ.get("JWT_SECRET_KEY", "").strip()
if _env_jwt_secret:
    settings.jwt_secret_key = _env_jwt_secret

if not settings.jwt_secret_key:
    if settings.web_password:
        # Convenience for single-operator Coolify deployments: allow the
        # dashboard password to be the only required secret.  A dedicated
        # JWT_SECRET_KEY is still recommended (changing WEB_PASSWORD will
        # invalidate existing sessions).
        import hashlib

        settings.jwt_secret_key = hashlib.sha256(
            f"poly15m4-jwt:{settings.web_password}".encode("utf-8")
        ).hexdigest()
        logging.getLogger(__name__).warning(
            "JWT_SECRET_KEY is not set — deriving it from WEB_PASSWORD. "
            "Set JWT_SECRET_KEY explicitly for production deployments."
        )
    else:
        raise ValueError(
            "JWT_SECRET_KEY is not set in the environment — cannot start without it. "
            "Set JWT_SECRET_KEY or WEB_PASSWORD (the latter derives a JWT secret)."
        )

_jwt_secret = settings.jwt_secret_key.strip()
if len(_jwt_secret) < 32:
    raise ValueError(
        f"JWT_SECRET_KEY is too short ({len(_jwt_secret)} chars) — minimum 32 required. "
        "Generate a strong key: python -c \"import secrets; print(secrets.token_urlsafe(64))\""
    )
if _jwt_secret != settings.jwt_secret_key:
    settings.jwt_secret_key = _jwt_secret
