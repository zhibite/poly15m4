"""
runtime_config.py — In-memory mutable strategy config, hot-updatable via /api/config.
"""

import copy
import os
import pathlib
import threading
from loguru import logger
from app.config import settings

_ENV_PATH: str | None = None
_creds_lock = threading.Lock()
_config_lock = threading.Lock()

# Lock for thread-safe access to the shared _latest state dict
# used by strategy.py (writer) and websocket.py (reader).
_latest_lock = threading.Lock()


def _init_env_path():
    global _ENV_PATH
    if _ENV_PATH is not None:
        return _ENV_PATH
    base = pathlib.Path(__file__).resolve().parent.parent
    candidates = [base / ".env", base.parent / ".env"]
    for p in candidates:
        if p.exists():
            _ENV_PATH = str(p)
            return _ENV_PATH
    _ENV_PATH = None
    return None


def _env(key: str, fallback: str = "") -> str:
    raw = os.environ.get(key)
    if raw is None:
        return fallback
    return raw.strip().strip("'\"").strip()


def _env_bool(key: str, settings_fallback: bool) -> bool:
    raw = os.environ.get(key)
    if raw is None:
        return bool(settings_fallback)
    s = raw.strip().strip("'\"").strip().lower()
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off", ""):
        return False
    return bool(settings_fallback)


def _slug_initial() -> str:
    slug = _env("MARKET_INTERVAL_SLUG", settings.market_interval_slug).strip().lower().replace("/", "")
    return slug if slug else "15m"


def _duration_initial() -> int:
    raw = os.environ.get("MARKET_DURATION_SEC")
    if raw is None:
        return int(settings.market_duration_sec)
    try:
        v = int(float(str(raw).strip()))
        return v if v > 0 else 900
    except ValueError:
        return int(settings.market_duration_sec)


_runtime: dict = {
    "market_interval_slug":            _slug_initial(),
    "market_duration_sec":             _duration_initial(),
    "asset_btc_enabled":              _env_bool("ASSET_BTC_ENABLED", settings.trade_asset_btc),
    "asset_eth_enabled":              _env_bool("ASSET_ETH_ENABLED", True),

    # 单持仓首单金额
    "entry_base_usdc":               float(_env("ENTRY_BASE_USDC", str(settings.entry_base_usdc))),

    # 首单截止：窗口开启后超过此时长则禁止开单
    "entry_skip_if_late":            _env_bool("ENTRY_SKIP_IF_LATE", True),
    "entry_early_start_elapsed_sec": float(_env("ENTRY_EARLY_START_ELAPSED_SEC", "0.0")),
    "entry_late_cutoff_elapsed_sec": float(_env("ENTRY_LATE_CUTOFF_ELAPSED_SEC", "540.0")),

    "poll_interval_sec":              float(_env("POLL_INTERVAL_SEC", str(settings.poll_interval_sec))),
    "settle_grace_sec":               int(float(_env("SETTLE_GRACE_SEC", str(settings.settle_grace_sec)))),
    "max_settle_wait_sec":            int(float(_env("MAX_SETTLE_WAIT_SEC", str(settings.max_settle_wait_sec)))),
    "sell_max_retry_count":           int(float(_env("SELL_MAX_RETRY_COUNT", str(settings.sell_max_retry_count)))),

    "live_mode":                     _env_bool("LIVE_MODE", settings.live_mode),
    "live_slippage_pct":             float(_env("LIVE_SLIPPAGE_PCT", str(settings.live_slippage_pct))),
    "btc_slippage_pct":              float(_env("BTC_SLIPPAGE_PCT", str(settings.btc_slippage_pct))),
    "eth_slippage_pct":              float(_env("ETH_SLIPPAGE_PCT", str(settings.eth_slippage_pct))),
    "default_fee_bps":               int(float(_env("DEFAULT_FEE_BPS", str(settings.default_fee_bps)))),
    "min_exit_price":                float(_env("MIN_EXIT_PRICE", str(settings.min_exit_price))),

    # 入场价格过滤
    "entry_price_min":               float(_env("ENTRY_PRICE_MIN", "0.20")),
    "entry_price_max":               float(_env("ENTRY_PRICE_MAX", "0.60")),

    # ── Core 6 指标同向信号共识门槛 ─────────────────────────────────
    "core_agree_threshold":           int(float(_env("CORE_AGREE_THRESHOLD", "3"))),
    "core_agree_standard":           int(float(_env("CORE_AGREE_STANDARD", "3"))),

    # 止盈（基于入场价的动态 TP：入场价越低 → TP 目标越高）
    # 公式：tp_pct = tp_base_pct + tp_range_pct × (entry_price_max_tp - entry_price) / range
    # 例：tp_base_pct=10 / tp_range_pct=20 / entry_price_min_tp=0.2 / entry_price_max_tp=0.6
    #       入场 0.2 → TP=30%  |  入场 0.4 → TP=20%  |  入场 0.6 → TP=10%
    "tp_high_min":                 float(_env("TP_HIGH_MIN", "30.0")),
    "tp_high_max":                 float(_env("TP_HIGH_MAX", "10.0")),
    "tp_entry_min":                float(_env("TP_ENTRY_MIN", "0.20")),
    "tp_entry_max":                float(_env("TP_ENTRY_MAX", "0.60")),
    # 止损
    "sl_base_pct":                 float(_env("SL_BASE_PCT", "-10.0")),
}

_ENV_KEYS: dict[str, str] = {
    "market_interval_slug":            "MARKET_INTERVAL_SLUG",
    "market_duration_sec":             "MARKET_DURATION_SEC",
    "asset_btc_enabled":              "ASSET_BTC_ENABLED",
    "asset_eth_enabled":              "ASSET_ETH_ENABLED",
    "entry_base_usdc":                "ENTRY_BASE_USDC",
    "entry_skip_if_late":             "ENTRY_SKIP_IF_LATE",
    "entry_early_start_elapsed_sec":  "ENTRY_EARLY_START_ELAPSED_SEC",
    "entry_late_cutoff_elapsed_sec":  "ENTRY_LATE_CUTOFF_ELAPSED_SEC",
    "poll_interval_sec":              "POLL_INTERVAL_SEC",
    "settle_grace_sec":               "SETTLE_GRACE_SEC",
    "max_settle_wait_sec":            "MAX_SETTLE_WAIT_SEC",
    "sell_max_retry_count":           "SELL_MAX_RETRY_COUNT",
    "live_mode":                      "LIVE_MODE",
    "live_slippage_pct":              "LIVE_SLIPPAGE_PCT",
    "btc_slippage_pct":               "BTC_SLIPPAGE_PCT",
    "eth_slippage_pct":               "ETH_SLIPPAGE_PCT",
    "default_fee_bps":                "DEFAULT_FEE_BPS",
    "min_exit_price":                 "MIN_EXIT_PRICE",
    "core_agree_threshold":           "CORE_AGREE_THRESHOLD",
    "core_agree_standard":           "CORE_AGREE_STANDARD",
    "entry_price_min":               "ENTRY_PRICE_MIN",
    "entry_price_max":               "ENTRY_PRICE_MAX",
    "tp_base_pct":                   "TP_BASE_PCT",
    "tp_range_pct":                  "TP_RANGE_PCT",
    "entry_price_min_tp":             "TP_ENTRY_PRICE_MIN",
    "entry_price_max_tp":             "TP_ENTRY_PRICE_MAX",
    "sl_base_pct":               "SL_BASE_PCT",
    "tp_high_min":              "TP_HIGH_MIN",
    "tp_high_max":              "TP_HIGH_MAX",
    "tp_entry_min":             "TP_ENTRY_MIN",
    "tp_entry_max":             "TP_ENTRY_MAX",
}

# 缓存上次返回的深拷贝，仅当 _runtime 实际更新后才重新生成。
_config_cache: dict | None = None
_config_cache_version: int = 0
_config_runtime_version: int = 0


def get_config() -> dict:
    global _config_cache, _config_cache_version
    global _config_runtime_version
    with _config_lock:
        if _config_cache is None or _config_runtime_version != _config_cache_version:
            _config_cache = copy.deepcopy(dict(_runtime))
            _config_cache_version = _config_runtime_version
        # Return a fresh deep copy so callers cannot mutate the cached state.
        return copy.deepcopy(_config_cache)


def update_config(patch: dict, *, env_path: str | None = None) -> dict:
    global _config_runtime_version
    with _config_lock:
        _runtime.update(patch)
        _config_runtime_version += 1
    target = env_path or _init_env_path()
    if target:
        try:
            from dotenv import set_key as _set_key
            for k, v in patch.items():
                env_key = _ENV_KEYS.get(k)
                if env_key:
                    if isinstance(v, bool):
                        persisted = str(v).lower()
                    else:
                        persisted = str(v)
                    _set_key(target, env_key, persisted)
        except Exception as exc:
            logger.warning("Failed to persist config patch to .env: {}", exc)
    return get_config()


def _initial_signature_type() -> int:
    raw = _env("SIGNATURE_TYPE", "0")
    try:
        st = int(raw)
        return st if st in (0, 1, 2) else 0
    except ValueError:
        return 0


def _build_creds_store() -> dict:
    return {
        "private_key":       _env("WALLET_PRIVATE_KEY", ""),
        "api_key":           _env("API_KEY", ""),
        "api_secret":        _env("API_SECRET", ""),
        "api_passphrase":    _env("API_PASSPHRASE", ""),
        "signer_address":    _env("SIGNER_ADDRESS", ""),
        "funder_address":    _env("FUNDER_ADDRESS", ""),
        "signature_type":    _initial_signature_type(),
    }


_creds_store: dict | None = None


def get_creds() -> dict:
    global _creds_store
    with _creds_lock:
        if _creds_store is None:
            _creds_store = _build_creds_store()
        return {
            "private_key":       _creds_store["private_key"],
            "api_key":           _creds_store["api_key"],
            "api_secret":        _creds_store["api_secret"],
            "api_passphrase":    _creds_store["api_passphrase"],
            "signer_address":    _creds_store.get("signer_address", ""),
            "funder_address":    _creds_store.get("funder_address", ""),
            "signature_type":     _creds_store.get("signature_type", 0),
            "host":              settings.polymarket_host,
            "live_mode":        _runtime.get("live_mode", False),
            "live_slippage_pct": _runtime.get("live_slippage_pct", 2.0),
        }


def clear_wallet() -> None:
    global _creds_store
    with _creds_lock:
        if _creds_store is None:
            _creds_store = _build_creds_store()
        _creds_store["private_key"] = ""
        _creds_store["api_key"] = ""
        _creds_store["api_secret"] = ""
        _creds_store["api_passphrase"] = ""
        logger.debug("Wallet credentials cleared from memory.")


def set_wallet(
    private_key: str,
    *,
    api_key: str = "",
    api_secret: str = "",
    api_passphrase: str = "",
    signer_address: str = "",
    funder_address: str = "",
    signature_type: int = 0,
) -> None:
    global _creds_store
    with _creds_lock:
        if _creds_store is None:
            _creds_store = _build_creds_store()
        _creds_store["private_key"]    = private_key
        _creds_store["api_key"]        = api_key
        _creds_store["api_secret"]    = api_secret
        _creds_store["api_passphrase"] = api_passphrase
        _creds_store["signer_address"] = signer_address
        _creds_store["funder_address"] = funder_address
        _creds_store["signature_type"] = signature_type
