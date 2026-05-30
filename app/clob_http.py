"""
clob_http.py — CLOB v2 L2 HMAC helpers.

In v2, ClobClient handles all L2 HMAC authentication internally.
This module is a placeholder for any raw HTTP L2 operations that may
be needed outside the SDK (e.g. custom balance queries with extra params).
"""

# Re-export helpers from polygon_usdc for convenience
from app.polygon_usdc import _erc20_balance_raw, sync_clob_balance_cache
