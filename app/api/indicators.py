"""
indicators.py -- REST API: /api/indicators
Aggregates all 10 technical indicators for BTC/ETH from real Binance & Polymarket data.
"""

import asyncio
import threading
import time
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter
from loguru import logger

from app.pm_client import PolymarketClient

# Singleton PolymarketClient and asyncio lock (initialized lazily).
_pm_client: PolymarketClient | None = None
_pm_init_lock: asyncio.Lock | None = None


def _get_pm_lock() -> asyncio.Lock:
    global _pm_init_lock
    if _pm_init_lock is None:
        _pm_init_lock = asyncio.Lock()
    return _pm_init_lock


async def _get_pm_client() -> PolymarketClient:
    """Get or create the singleton PolymarketClient."""
    global _pm_client
    lock = _get_pm_lock()
    async with lock:
        if _pm_client is None:
            _pm_client = PolymarketClient()
        return _pm_client

router = APIRouter(prefix="/api", tags=["indicators"])

HISTORY_SIZE = 60
KLINES_LIMIT = 500

# ── Rolling chart history (per asset) ─────────────────────────────────────────
_rsi_history: dict[str, list[float]] = {}
_macd_hist: dict[str, list[float]] = {}
_macd_signal: dict[str, list[float]] = {}
_vwap_prices: dict[str, list[float]] = {}
_vwap_values: dict[str, list[float]] = {}
_ema9: dict[str, list[float]] = {}
_ema21: dict[str, list[float]] = {}
_ema_prices: dict[str, list[float]] = {}
_cvd_history: dict[str, list[float]] = {}
_oi_history: dict[str, list[float]] = {}

# ── Spot prices ───────────────────────────────────────────────────────────────
_spot_btc: float = 0.0
_spot_eth: float = 0.0

# ── Caches (ttl, value) ──────────────────────────────────────────────────────
_cache_ttl = 60   # seconds for funding / OI / OBI
_cache: dict[str, tuple[float, float]] = {}

# ── CVD raw window delta ──────────────────────────────────────────────────
_cvd_cum: dict[str, float] = {}
# ── CVD rolling baseline (last 10 window deltas) ───────────────────────────
_cvd_baseline: dict[str, list[float]] = {}
_MAX_BASELINE = 10
# ── Rolling history lock — protects all per-asset chart history dicts ─────────
_history_lock: asyncio.Lock = asyncio.Lock()

# ── Liquidation buckets (rolling 30-min window) ───────────────────────────────
_liq_buckets: dict[str, list[dict]] = {}
_liq_buckets_lock = threading.Lock()

# ── Force-order WebSocket per asset ───────────────────────────────────────────
_liq_ws: dict[str, asyncio.Task | None] = {}
_liq_ws_running: dict[str, bool] = {}
_liq_ws_lock: asyncio.Lock = asyncio.Lock()   # guards task creation


# ════════════════════════════════════════════════════════════════════════════════
# Binance helpers
# ════════════════════════════════════════════════════════════════════════════════

async def _binance_get(url: str, params: dict | None = None) -> dict | None:
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, params=params or {})
            r.raise_for_status()
            return r.json()
    except Exception as exc:
        logger.debug("Binance request failed [{}]: {}", url, exc)
        return None


async def fetch_binance_klines(symbol: str, interval: str = "1m", limit: int = KLINES_LIMIT) -> dict | None:
    """Fetch OHLCV klines from Binance spot API. symbol e.g. 'BTCUSDT'."""
    data = await _binance_get(
        "https://api.binance.com/api/v3/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
    )
    if not data or not isinstance(data, list):
        return None
    try:
        return {
            "closes":  [float(r[4]) for r in data],
            "highs":   [float(r[2]) for r in data],
            "lows":    [float(r[3]) for r in data],
            "volumes": [float(r[5]) for r in data],
        }
    except (IndexError, ValueError) as exc:
        logger.debug("Failed to parse klines: {}", exc)
        return None


async def fetch_binance_funding(symbol: str) -> float | None:
    """Current funding rate from Binance USDT-M futures."""
    data = await _binance_get(
        "https://fapi.binance.com/fapi/v1/premiumIndex",
        params={"symbol": symbol},
    )
    if not data:
        return None
    try:
        return float(data["lastFundingRate"])
    except (KeyError, ValueError):
        return None


async def fetch_binance_oi(symbol: str) -> float | None:
    """Open interest from Binance USDT-M futures."""
    data = await _binance_get(
        "https://fapi.binance.com/fapi/v1/openInterest",
        params={"symbol": symbol},
    )
    if not data:
        return None
    try:
        return float(data["openInterest"])
    except (KeyError, ValueError):
        return None


async def fetch_binance_obi(symbol: str, levels: int = 10) -> dict | None:
    """Order-book imbalance from Binance spot depth."""
    data = await _binance_get(
        "https://api.binance.com/api/v3/depth",
        params={"symbol": symbol, "limit": levels},
    )
    if not data:
        return None
    try:
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        bid_vol = sum(float(q) for _, q in bids)
        ask_vol = sum(float(q) for _, q in asks)
        if ask_vol == 0:
            return None
        obi = bid_vol / ask_vol
        return {"bid_vol": bid_vol, "ask_vol": ask_vol, "value": obi}
    except (KeyError, ValueError, TypeError):
        return None


async def fetch_binance_cvd(symbol: str, limit: int = 500) -> float | None:
    """Cumulative Volume Delta from Binance aggTrades.

    m=false means taker bought (buy pressure, +qty).
    m=true  means taker sold  (sell pressure, -qty).
    """
    data = await _binance_get(
        "https://api.binance.com/api/v3/aggTrades",
        params={"symbol": symbol, "limit": limit},
    )
    if not data or not isinstance(data, list):
        return None
    try:
        delta = 0.0
        for t in data:
            qty = float(t.get("q", 0))
            maker = t.get("m", True)
            delta += qty if not maker else -qty
        return delta
    except (KeyError, ValueError, TypeError):
        return None


# ════════════════════════════════════════════════════════════════════════════════
# Liquidation WebSocket listener
# ════════════════════════════════════════════════════════════════════════════════

async def _liq_ws_listener(asset: str):
    """Subscribe to Binance futures force-order stream and accumulate into buckets."""
    import websockets
    symbol_fut = f"{asset}USDT"
    ws_url = "wss://fstream.binance.com:9443/ws/!forceOrder@arr"
    bucket_ttl = 30 * 60   # 30 minutes
    reconnect_delay = 5.0    # seconds before reconnecting
    max_reconnect_attempts = 5
    reconnect_attempts = 0

    while True:
        try:
            async with websockets.connect(ws_url, ping_timeout=20) as ws:
                _liq_ws_running[asset] = True
                reconnect_attempts = 0  # Reset on successful connection
                logger.info("Liquidation WS connected for {}", asset)

                while _liq_ws_running.get(asset, False):
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        payload = __import__("json").loads(msg)

                        orders = payload if isinstance(payload, list) else [payload]
                        for order in orders:
                            try:
                                o = order.get("o", {})
                                if o.get("s") != symbol_fut:
                                    continue
                                filled_qty = float(o.get("l", 0))
                                if filled_qty <= 0:
                                    continue
                                price = float(o.get("ap", 0))
                                side = o.get("S", "")
                                direction = "long" if side == "SELL" else "short"
                                bucket = {
                                    "price": price,
                                    "qty": filled_qty,
                                    "direction": direction,
                                    "ts": time.time(),
                                }

                                with _liq_buckets_lock:
                                    buckets = _liq_buckets.setdefault(asset, [])
                                    buckets.append(bucket)
                                    cutoff = time.time() - bucket_ttl
                                    _liq_buckets[asset] = [b for b in buckets if b["ts"] > cutoff]

                            except (TypeError, ValueError, KeyError):
                                continue

                    except asyncio.TimeoutError:
                        try:
                            await ws.ping()
                        except Exception:
                            break
                    except websockets.ConnectionClosed:
                        logger.warning("Liquidation WS for {} connection closed, reconnecting...", asset)
                        break
                    except Exception:
                        break

        except websockets.ConnectionClosed:
            logger.warning("Liquidation WS for {} failed to connect, retrying in {:.0f}s...", asset, reconnect_delay)
        except Exception as exc:
            logger.warning("Liquidation WS for {} error: {}, retrying in {:.0f}s...", asset, exc, reconnect_delay)

        _liq_ws_running[asset] = False

        # Reconnect with exponential backoff
        reconnect_attempts += 1
        if max_reconnect_attempts > 0 and reconnect_attempts > max_reconnect_attempts:
            logger.error(
                "Liquidation WS for {} exhausted {} reconnect attempts — giving up. "
                "Liquidation data will be unavailable until server restart.",
                asset, max_reconnect_attempts,
            )
            break
        await asyncio.sleep(reconnect_delay)


def _build_liquidation(asset: str, current_price: float) -> dict:
    """Aggregate _liq_buckets into the heatmap format."""
    with _liq_buckets_lock:
        buckets = list(_liq_buckets.get(asset, []))
    long_qty = 0.0
    short_qty = 0.0
    long_prices = []
    short_prices = []

    for b in buckets:
        if b["direction"] == "long":
            long_qty += b["qty"]
            long_prices.append(b["price"])
        else:
            short_qty += b["qty"]
            short_prices.append(b["price"])

    long_dist = (min(long_prices) - current_price) / current_price if long_prices else -0.02
    short_dist = (max(short_prices) - current_price) / current_price if short_prices else 0.02

    total = long_qty + short_qty
    if total <= 0:
        return {
            "long_dist": -0.02, "short_dist": 0.02,
            "long_pct": 0.5, "short_pct": 0.5,
            "signal": "neutral",
        }

    return {
        "long_dist":  long_dist,
        "short_dist": short_dist,
        "long_pct":   long_qty / total,
        "short_pct":  short_qty / total,
        "signal": "down_magnet" if abs(long_dist) < abs(short_dist) else "up_magnet",
    }


async def _ensure_liq_ws(asset: str):
    """Start liquidation WS if not already running. Guards against concurrent duplicate task creation."""
    async with _liq_ws_lock:
        if _liq_ws.get(asset) is None or _liq_ws_running.get(asset, False) is False:
            t = asyncio.create_task(_liq_ws_listener(asset))
            _liq_ws[asset] = t


# ════════════════════════════════════════════════════════════════════════════════
# Polymarket helpers
# ════════════════════════════════════════════════════════════════════════════════

async def _get_market_prices(asset: str) -> tuple[float, float]:
    """Fetch UP/DOWN mid-prices from Polymarket CLOB, with fallback."""
    try:
        pm = await _get_pm_client()
        markets = await pm.get_asset_updown_markets(asset, limit=1)
        if markets:
            m = markets[0]
            tokens = m.get("tokens", [])
            if len(tokens) >= 2:
                up_id = tokens[0].get("token_id", "")
                dn_id = tokens[1].get("token_id", "")
                up_raw = await pm.get_midpoint(up_id)
                dn_raw = await pm.get_midpoint(dn_id)
                if up_raw is not None and dn_raw is not None:
                    return float(up_raw), float(dn_raw)
    except Exception as exc:
        logger.debug("_get_market_prices({}) failed: {}", asset, exc)
    return 0.5, 0.5


async def _get_both_market_prices() -> dict[str, dict[str, float]]:
    """Fetch UP/DOWN mids for both BTC and ETH concurrently."""
    (btc_up, btc_dn), (eth_up, eth_dn) = await asyncio.gather(
        _get_market_prices("BTC"),
        _get_market_prices("ETH"),
    )
    return {
        "BTC": {"up_mid": btc_up, "down_mid": btc_dn},
        "ETH": {"up_mid": eth_up, "down_mid": eth_dn},
    }


# ════════════════════════════════════════════════════════════════════════════════
# Indicator computation
# ════════════════════════════════════════════════════════════════════════════════

def _ema(series: list[float], period: int) -> float:
    if len(series) < period:
        return series[-1] if series else 0.0
    k = 2.0 / (period + 1)
    val = sum(series[:period]) / period
    for v in series[period:]:
        val = v * k + val * (1 - k)
    return val


def _calc_rsi(closes: list[float], period: int = 14) -> float:
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    if len(gains) < period:
        return 50.0
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    return 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))


def _calc_macd_series(closes: list[float]):
    """Return (macd_line, signal_line, histogram_series) for the last 30 bars."""
    if len(closes) < 26:
        return None, None, []
    # Build full MACD line first
    macd_line_arr = []
    for i in range(26, len(closes)):
        e12 = _ema(closes[max(0, i - 12):i + 1], 12)
        e26 = _ema(closes[max(0, i - 26):i + 1], 26)
        macd_line_arr.append(e12 - e26)

    if not macd_line_arr:
        return None, None, []

    # Standard recursive EMA for signal line over the entire MACD line
    k = 2.0 / (9 + 1)
    sig_val = sum(macd_line_arr[:9]) / 9 if len(macd_line_arr) >= 9 else macd_line_arr[-1]
    sig_arr = [sig_val]
    for v in macd_line_arr[9:]:
        sig_val = v * k + sig_val * (1 - k)
        sig_arr.append(sig_val)

    # histogram = MACD line - signal line, last 30 bars
    macd_start = max(0, len(macd_line_arr) - 30)
    sig_start = max(0, len(sig_arr) - 30)
    hist = [macd_line_arr[macd_start + i] - sig_arr[sig_start + i]
            for i in range(min(len(macd_line_arr) - macd_start, len(sig_arr) - sig_start))]

    return macd_line_arr[-1], sig_arr[-1], hist


def _calc_vwap(closes: list[float], volumes: list[float]) -> float:
    if not volumes:
        return closes[-1] if closes else 0.0
    return sum(c * v for c, v in zip(closes, volumes)) / sum(volumes)


def _calc_pivot(prices: list[float]) -> dict:
    if len(prices) < 10:
        pad_val = prices[-1] if prices else 0.0
        pad = [pad_val] * (10 - len(prices))
        prices = pad + prices
    recent = prices[-10:]
    high = max(recent)
    low = min(recent)
    return {"high": high, "low": low, "mid": (high + low) / 2}


def _calc_bbands(closes: list[float], period: int = 20, std_mult: float = 2.0) -> tuple[float, float, float]:
    """计算布林带：返回 (中轨, 上轨, 下轨)"""
    if len(closes) < period:
        return 0.0, 0.0, 0.0
    recent = closes[-period:]
    mid = sum(recent) / period
    variance = sum((x - mid) ** 2 for x in recent) / period
    std = variance ** 0.5
    return mid, mid + std_mult * std, mid - std_mult * std


def _calc_atr(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float:
    if len(highs) < 2:
        return 0.0
    trs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(tr)
    if len(trs) < period:
        return sum(trs) / len(trs) if trs else 0.0
    return sum(trs[-period:]) / period


# ════════════════════════════════════════════════════════════════════════════════
# Signal helpers
# ════════════════════════════════════════════════════════════════════════════════

def _signal_funding(v: float) -> tuple[str, str]:
    if v > 0.0005: return "bearish", "多头支付空头"
    if v < -0.0005: return "bullish", "空头支付多头"
    return "neutral", "中性"



def _signal_obi(obi: float) -> tuple[str, str]:
    if obi > 1.5: return "up", "bid_heavy"
    if obi < 0.67: return "down", "ask_heavy"
    return "neutral", "balanced"


def _signal_oi(closes: list[float], oi_list: list[float]) -> str:
    if len(closes) < 2 or len(oi_list) < 2:
        return "neutral"
    p_up = closes[-1] > closes[-2]
    o_up = oi_list[-1] > oi_list[-2]
    if p_up and o_up: return "new_long_fuel"
    if p_up and not o_up: return "short_squeeze"
    if not p_up and o_up: return "new_short_fuel"
    if not p_up and not o_up: return "long_capitulation"
    return "neutral"


# ════════════════════════════════════════════════════════════════════════════════
# Main endpoint
# ════════════════════════════════════════════════════════════════════════════════

@router.get("/indicators")
async def get_indicators(asset: str = "BTC"):
    """
    Returns all 10 indicators with rolling history for chart rendering.
    Asset: BTC or ETH (default BTC).
    Data sources:
      - Core 6:  Binance spot klines (real OHLCV)
      - Funding/OI: Binance USDT-M futures
      - CVD/OBI:    Binance spot aggTrades + depth
      - Liquidation: Binance futures force-order WebSocket
      - Markets:     Polymarket CLOB
    """
    asset = asset.upper()
    if asset not in ("BTC", "ETH"):
        asset = "BTC"

    symbol_spot = f"{asset}USDT"
    now = time.time()

    # ── 1. Fetch Binance klines ──────────────────────────────────────────────
    klines = await fetch_binance_klines(symbol_spot)
    if klines:
        closes  = klines["closes"]
        highs   = klines["highs"]
        lows    = klines["lows"]
        volumes = klines["volumes"]
        current_price = closes[-1] if closes else 0.0
    else:
        closes  = []
        highs   = []
        lows    = []
        volumes = []
        current_price = 0.0

    # ── 2. Fetch Polymarket market prices ────────────────────────────────────
    up_mid, dn_mid = await _get_market_prices(asset)
    all_markets = await _get_both_market_prices()

    # ── 3. Spot prices from Binance ticker ───────────────────────────────────
    # NOTE: `_spot_btc` / `_spot_eth` are module-level globals. Do NOT assign
    # to them with a bare `=` inside an async function — Python treats the name
    # as a local variable from the point of the first assignment, shadowing the
    # global. Use a temporary local variable and assign to the global once only.
    try:
        _tmp_btc: float = 0.0
        _tmp_eth: float = 0.0
        async with httpx.AsyncClient(timeout=5.0) as client:
            btc_r, eth_r = await asyncio.gather(
                client.get(
                    "https://api.binance.com/api/v3/ticker/price",
                    params={"symbol": "BTCUSDT"},
                ),
                client.get(
                    "https://api.binance.com/api/v3/ticker/price",
                    params={"symbol": "ETHUSDT"},
                ),
            )
            if btc_r.status_code == 200:
                _tmp_btc = float(btc_r.json().get("price", 0))
            if eth_r.status_code == 200:
                _tmp_eth = float(eth_r.json().get("price", 0))
        # Assign once to the module-level global (outside the scope where the
        # local name `_spot_btc` / `_spot_eth` is visible).
        global _spot_btc, _spot_eth
        _spot_btc = _tmp_btc
        _spot_eth = _tmp_eth
    except Exception:
        pass

    # ── 4. Cached: Funding Rate ──────────────────────────────────────────────
    fund_key = f"funding:{asset}"
    if fund_key not in _cache or (now - _cache[fund_key][1]) > _cache_ttl:
        val = await fetch_binance_funding(symbol_spot)
        if val is not None:
            _cache[fund_key] = (val, now)
    funding_val = _cache.get(fund_key, (0.0, 0))[0]

    # ── 5. Cached: Open Interest ─────────────────────────────────────────────
    oi_key = f"oi:{asset}"
    if oi_key not in _cache or (now - _cache[oi_key][1]) > _cache_ttl:
        val = await fetch_binance_oi(symbol_spot)
        if val is not None:
            _cache[oi_key] = (val, now)
    oi_val = _cache.get(oi_key, (0.0, 0))[0]

    # ── 6. Cached: OBI ───────────────────────────────────────────────────────
    obi_key = f"obi:{asset}"
    if obi_key not in _cache or (now - _cache[obi_key][1]) > 5:
        val = await fetch_binance_obi(symbol_spot)
        if val is not None:
            _cache[obi_key] = (val, now)
    obi_data = _cache.get(obi_key, ({"value": 1.0, "bid_vol": 0.0, "ask_vol": 0.0}, 0))[0]

    # ── 7. CVD: net buy/sell pressure from last 500 aggTrades ─────────────────
    cvd_val = await fetch_binance_cvd(symbol_spot)

    # ── 8. Liquidation WebSocket ─────────────────────────────────────────────
    await _ensure_liq_ws(asset)
    liq = _build_liquidation(asset, current_price or (_spot_btc if asset == "BTC" else _spot_eth))

    # ── 9. Compute indicators ────────────────────────────────────────────────
    rsi_val = _calc_rsi(closes) if closes else 50.0

    macd_line, signal, macd_hist = (None, None, [])
    if closes and len(closes) >= 26:
        macd_line, signal, macd_hist = _calc_macd_series(closes)
    if macd_line is None:
        macd_line = 0.0
    if signal is None:
        signal = 0.0

    vwap_val = _calc_vwap(closes[-HISTORY_SIZE:], volumes[-HISTORY_SIZE:]) if closes else 0.0
    ema9_val = _ema(closes, 9) if closes else 0.0
    ema21_val = _ema(closes, 21) if closes else 0.0
    atr = _calc_atr(highs, lows, closes) if highs else 0.0
    pivot = _calc_pivot(closes) if closes else {"high": 0.0, "low": 0.0, "mid": 0.0}
    bb_mid, bb_upper, bb_lower = _calc_bbands(closes) if closes else (0.0, 0.0, 0.0)

    # ── 10. Signal computation ───────────────────────────────────────────────
    # CVD Z-score + interpretation
    _cvd_baseline_local = _cvd_baseline.setdefault(asset, [])
    if len(_cvd_baseline_local) >= 3:
        _mean = sum(_cvd_baseline_local) / len(_cvd_baseline_local)
        _std = (sum((x - _mean) ** 2 for x in _cvd_baseline_local) / max(len(_cvd_baseline_local) - 1, 1)) ** 0.5
        cvd_z_local = (abs(cvd_val) - _mean) / _std if _std > 0 else 0.0
    else:
        cvd_z_local = 0.0
    cvd_sig = (
        "吸筹" if cvd_z_local > 1.0 and cvd_val > 0
        else "派发" if cvd_z_local > 1.0 and cvd_val < 0
        else "中性"
    )

    fund_sig, fund_dir = _signal_funding(funding_val)
    obi_sig, obi_interp = _signal_obi(obi_data.get("value", 1.0))
    oi_sig = _signal_oi(closes, _oi_history.get(asset, []))

    # EMA cross: require at least 21 closes before detecting a real cross
    if len(closes) >= 21:
        e9p = _ema(closes[:-1], 9)
        e21p = _ema(closes[:-1], 21)
        if e9p <= e21p and ema9_val > ema21_val:
            ema_cross = "golden"
        elif e9p >= e21p and ema9_val < ema21_val:
            ema_cross = "death"
        else:
            ema_cross = "parallel"
    else:
        ema_cross = "parallel"

    # VWAP signal
    if closes:
        if closes[-1] > vwap_val * 1.001: vwap_sig = "above"
        elif closes[-1] < vwap_val * 0.999: vwap_sig = "below"
        else: vwap_sig = "at"
    else:
        vwap_sig = "at"

    # Pivot signal
    if closes and current_price:
        if current_price > pivot["high"] * 0.9995: pivot_sig = "above"
        elif current_price < pivot["low"] * 1.0005: pivot_sig = "below"
        else: pivot_sig = "within"
    else:
        pivot_sig = "within"

    # RSI signal
    if rsi_val > 70: rsi_sig = "overbought"
    elif rsi_val < 30: rsi_sig = "oversold"
    elif 45 <= rsi_val <= 55: rsi_sig = "neutral"
    else: rsi_sig = "skewed"

    # MACD signal
    if macd_hist:
        last_h = macd_hist[-1]
        prev_h = macd_hist[-2] if len(macd_hist) >= 2 else 0.0
        if last_h > 0 and last_h > prev_h: macd_sig = "bullish"
        elif last_h < 0 and last_h < prev_h: macd_sig = "bearish"
        else: macd_sig = "contracting"
    else:
        macd_sig = "contracting"

    # ── 11. Append to rolling history ────────────────────────────────────────
    # Hold _history_lock for the entire batch of writes to avoid partial reads
    # from concurrent requests.  Within the lock, update ALL history dicts atomically.
    async with _history_lock:
        def _app(hist: dict, key: str, val: float):
            lst = hist.setdefault(key, [])
            lst.append(val)
            if len(lst) > HISTORY_SIZE:
                lst.pop(0)

        _app(_rsi_history,   asset, rsi_val)
        _app(_macd_hist,     asset, macd_line - signal)
        _app(_macd_signal,   asset, signal)
        _app(_vwap_prices,   asset, closes[-1] if closes else 0.0)
        _app(_vwap_values,   asset, vwap_val)
        _app(_ema9,          asset, ema9_val)
        _app(_ema21,         asset, ema21_val)
        _app(_ema_prices,    asset, closes[-1] if closes else 0.0)
        _app(_cvd_history,   asset, cvd_val)
        # OI: write ONLY to module-level _oi_history (shared with get_indicators).
        # Service-level oi_hist is maintained separately inside IndicatorSignalService.
        # Guard: only append valid positive OI values to avoid polluting history.
        if oi_val and oi_val > 0:
            async with _history_lock:
                oi_list = _oi_history.setdefault(asset, [])
                oi_list.append(float(oi_val))
                if len(oi_list) > HISTORY_SIZE:
                    oi_list.pop(0)

        rsi_hist     = _rsi_history.get(asset, [])
        macd_h_list  = _macd_hist.get(asset, [])
        macd_s_list  = _macd_signal.get(asset, [])
        vwap_p_list  = _vwap_prices.get(asset, [])
        vwap_v_list  = _vwap_values.get(asset, [])
        ema9_list    = _ema9.get(asset, [])
        ema21_list   = _ema21.get(asset, [])
        ema_pr_list  = _ema_prices.get(asset, [])
        cvd_hist     = _cvd_history.get(asset, [])
    # OI history read AFTER the lock to avoid partial-read races.
    oi_hist = _oi_history.get(asset, [])

    oi_change = 0.0
    if len(oi_hist) >= 2 and oi_hist[-1] and oi_hist[-2] and oi_hist[-2] != 0:
        oi_change = (oi_hist[-1] - oi_hist[-2]) / oi_hist[-2]

    # ── 12. Build response ───────────────────────────────────────────────────
    price_for_pivot = current_price or up_mid

    return {
        "asset": asset,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "markets": all_markets,
        "spots": {"btc": _spot_btc, "eth": _spot_eth},

        "rsi": {
            "value": round(rsi_val, 1),
            "signal": rsi_sig,
            "history": [round(v, 1) for v in rsi_hist[-30:]],
        },
        "macd": {
            "line": round(macd_line, 6),
            "signal": round(signal, 6),
            "histogram": [round(v, 6) for v in macd_hist[-30:]],
        },
        "vwap": {
            "value": round(vwap_val, 4),
            "relative": (
                "价格 > VWAP (偏多)" if vwap_sig == "above"
                else "价格 < VWAP (偏空)" if vwap_sig == "below"
                else "价格 ≈ VWAP"
            ),
            "signal": vwap_sig,
            "price": [round(v, 4) for v in vwap_p_list[-30:]],
            "vwap":  [round(v, 4) for v in vwap_v_list[-30:]],
        },
        "ema": {
            "ema9": round(ema9_val, 4),
            "ema21": round(ema21_val, 4),
            "cross": ema_cross,
            "ema9_history":  [round(v, 4) for v in ema9_list[-30:]],
            "ema21_history": [round(v, 4) for v in ema21_list[-30:]],
            "price":         [round(v, 4) for v in ema_pr_list[-30:]],
        },
        "pivot": {
            "high":     round(pivot["high"], 4),
            "low":      round(pivot["low"], 4),
            "mid":      round(pivot["mid"], 4),
            "distHigh": f"{((price_for_pivot - pivot['high']) / pivot['high'] * 100):.2f}%" if pivot["high"] else "--",
            "distLow":  f"{((price_for_pivot - pivot['low'])  / pivot['low']  * 100):.2f}%" if pivot["low"]  else "--",
            "distMid":  f"{((price_for_pivot - pivot['mid'])  / pivot['mid']  * 100):.2f}%" if pivot["mid"]  else "--",
            "signal":   pivot_sig,
        },
        "bbands": {
            "mid":    round(bb_mid, 4),
            "upper":  round(bb_upper, 4),
            "lower":  round(bb_lower, 4),
            "signal": (
                "upper_touch" if current_price >= bb_upper * 0.9995
                else "lower_touch" if current_price <= bb_lower * 1.0005
                else "within"
            ),
        },
        "funding": {
            "value":     round(funding_val, 6),
            "direction": fund_dir,
            "signal":    fund_sig,
        },
        "cvd": {
            "value":        round(cvd_val, 2),
            "history":      [round(v, 2) for v in cvd_hist[-30:]],
            "interpretation": cvd_sig,
        },
        "liquidation": {
            "long_dist":  round(liq["long_dist"], 6),
            "short_dist": round(liq["short_dist"], 6),
            "long_pct":   round(liq["long_pct"], 4),
            "short_pct":  round(liq["short_pct"], 4),
            "signal":     liq["signal"],
        },
        "obi": {
            "value":          round(obi_data.get("value", 1.0), 3),
            "bidVol":         round(obi_data.get("bid_vol", 0.0), 2),
            "askVol":         round(obi_data.get("ask_vol", 0.0), 2),
            "signal":         obi_sig,
            "interpretation": obi_interp,
        },
        "oi": {
            "current":      round(oi_val, 0),
            "change":        round(oi_change, 4),
            "interpretation": oi_sig,
            "history":       [round(v, 0) for v in oi_hist[-30:]],
        },
        "atr": round(atr, 4),
    }


@router.get("/signal")
async def get_signal_recommendation(asset: str = "BTC"):
    """
    返回交易信号推荐（用于自动交易和监控面板）。

    整合 Core 6 + Advanced 信号，输出：
    - 信号强度（0-6）
    - 建议方向和操作
    - 各指标详情
    """
    asset = asset.upper()
    if asset not in ("BTC", "ETH"):
        asset = "BTC"

    # ── 内联指标数据获取（避免跨函数 await 问题）────────────────────────
    import time as _time
    symbol_spot = f"{asset}USDT"
    now_ts = _time.time()

    klines = await fetch_binance_klines(symbol_spot)
    closes  = klines["closes"]  if klines else []
    highs   = klines["highs"]   if klines else []
    lows    = klines["lows"]    if klines else []
    volumes = klines["volumes"] if klines else []
    current_price = closes[-1] if closes else 0.0

    # Polymarket 价格
    all_markets = await _get_both_market_prices()
    mkt_prices = all_markets.get(asset.upper(), {})
    up_mid   = float(mkt_prices.get("up_mid", 0.5))
    down_mid = float(mkt_prices.get("down_mid", 0.5))

    # 现货价格
    spot_price = current_price
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(
                "https://api.binance.com/api/v3/ticker/price",
                params={"symbol": symbol_spot},
            )
            if r.status_code == 200:
                spot_price = float(r.json().get("price", 0)) or current_price
    except Exception:
        pass

    # 高级指标缓存
    fund_key = f"funding:{asset}"
    if fund_key not in _cache or (now_ts - _cache[fund_key][1]) > _cache_ttl:
        val = await fetch_binance_funding(symbol_spot)
        if val is not None:
            _cache[fund_key] = (val, now_ts)
    funding_val = _cache.get(fund_key, (0.0, 0))[0]

    oi_key = f"oi:{asset}"
    if oi_key not in _cache or (now_ts - _cache[oi_key][1]) > _cache_ttl:
        val = await fetch_binance_oi(symbol_spot)
        if val is not None:
            _cache[oi_key] = (val, now_ts)
    oi_val = _cache.get(oi_key, (0.0, 0))[0]

    obi_key = f"obi:{asset}"
    if obi_key not in _cache or (now_ts - _cache[obi_key][1]) > 5:
        val = await fetch_binance_obi(symbol_spot)
        if val is not None:
            _cache[obi_key] = (val, now_ts)
    obi_data = _cache.get(obi_key, ({"value": 1.0}, 0))[0]

    cvd_val = await fetch_binance_cvd(symbol_spot) or 0.0

    # Liquidation
    await _ensure_liq_ws(asset)
    liq = _build_liquidation(asset, spot_price)

    # ATR / RSI
    atr = _calc_atr(highs, lows, closes) if highs else 0.0
    rsi = _calc_rsi(closes) if closes else 50.0

    # OI 变化 — read from shared _oi_history (written only by get_indicators).
    # Guard: ensure history is long enough for change calculation.
    oi_change = 0.0
    oi_hist_read = _oi_history.get(asset, [])
    if len(oi_hist_read) >= 2 and oi_hist_read[-1] and oi_hist_read[-2] and oi_hist_read[-2] != 0:
        oi_change = (oi_hist_read[-1] - oi_hist_read[-2]) / oi_hist_read[-2]

    # 窗口时间（15min 对齐）
    from datetime import datetime, timezone as _tz
    now_dt = datetime.now(_tz.utc)
    minute = now_dt.minute
    if minute < 15:       aligned_min = 0
    elif minute < 30:     aligned_min = 15
    elif minute < 45:      aligned_min = 30
    else:                  aligned_min = 45
    window_start = now_dt.replace(minute=aligned_min, second=0, microsecond=0)
    if aligned_min == 0 and minute >= 45:
        from datetime import timedelta
        window_start = window_start + timedelta(hours=1)
    elapsed_sec = (now_dt - window_start).total_seconds()
    remaining_sec = max(0.0, 900 - elapsed_sec)

    # 调用信号服务（其内部会追加 CVD 到 baseline 并计算 Z-score）
    from app.strategy.indicator_signal_service import IndicatorSignalService
    svc = IndicatorSignalService(asset)
    result = await svc.get_signal(
        up_mid=up_mid,
        down_mid=down_mid,
        window_open_price=spot_price,
        window_start=window_start,
        window_duration_sec=900,
    )

    # CVD Z-score 由服务内部计算；提取 raw_value 中的 Z 用于响应
    # 格式如 "1234.56 (Z=2.34)"
    cvd_z = 0.0
    try:
        raw = result.advanced.cvd.raw_value
        import re as _re
        m = _re.search(r"Z=([-\d.]+)", raw)
        if m:
            cvd_z = float(m.group(1))
    except Exception:
        pass

    # 构建响应
    core_items = []
    for s in [result.core.rsi, result.core.macd, result.core.vwap,
              result.core.ema, result.core.pivot]:
        core_items.append({
            "name": s.name, "direction": s.direction.value,
            "agree": s.agree, "reason": s.reason, "raw_value": s.raw_value,
        })

    advanced_items = []
    for s in [result.advanced.funding, result.advanced.cvd,
              result.advanced.liquidation, result.advanced.obi, result.advanced.oi]:
        advanced_items.append({
            "name": s.name, "direction": s.direction.value,
            "agree": s.agree, "reason": s.reason, "raw_value": s.raw_value,
        })

    action_labels = {
        "ENTER_FULL":    "建议全仓入场",
        "ENTER_HALF":   "建议半仓入场",
        "ENTER_QUARTER": "建议1/4仓试探",
        "SKIP":         "建议跳过",
    }
    strength_colors = {
        0: "#6b7280", 1: "#9ca3af", 2: "#fbbf24",
        3: "#34d399", 4: "#10b981", 5: "#059669",
    }

    return {
        "asset": asset.upper(),
        "timestamp": result.timestamp.isoformat(),
        "elapsed_sec": round(result.elapsed_sec, 1),
        "window_remaining_sec": round(result.window_end_sec, 1),
        "strength": result.strength,
        "strength_label": result.strength_label,
        "strength_color": strength_colors.get(result.strength, "#6b7280"),
        "direction": result.direction.value,
        "direction_label": result.direction_label,
        "suggested_action": result.suggested_action,
        "action_label": action_labels.get(result.suggested_action, "未知"),
        "suggested_position_pct": round(result.suggested_position_pct, 2),
        "skip_reason": result.skip_reason,
        "core_agree": result.core_agree,
        "advanced_confirm": result.advanced_confirm,
        "advanced_filter": result.advanced.filter_result.value,
        "advanced_note": (
            result.advanced.skip_reason
            or result.advanced.reduce_reason
            or result.advanced.confirm_note
            or ""
        ),
        "up_mid": round(result.up_mid, 4),
        "down_mid": round(result.down_mid, 4),
        "spot_price": round(result.spot_price, 2),
        "anchor_deviation_pct": round(result.anchor_deviation_pct * 100, 3),
        # 注意：此处的 dynamic_tp/sl_pct 是估算值（strength-based），
        # 实际执行时的止盈止损阈值由 entry_controller._calc_stop_levels 计算（基于入场价线性映射），
        # 两者可能存在差异，前端应标注为「估算参考值」而非「实际执行值」。
        "dynamic_tp_pct": round(result.strength * 5 + 20, 1) if result.strength > 0 else 0,
        "dynamic_sl_pct": round(min(5.0, max(5.0, 15.0 - result.strength)), 1) if result.strength > 0 else 0,
        "core_signals": core_items,
        "advanced_signals": advanced_items,
        # 原始指标数据
        "rsi": {"value": round(rsi, 1)},
        "funding": {"value": round(funding_val * 100, 4)},
        "atr": round(atr, 4),
        "cvd_z": round(cvd_z, 3),
        "up_mid_raw": up_mid,
        "down_mid_raw": down_mid,
    }
