"""
websocket.py — WebSocket endpoints:
  /ws/live   — pushes strategy state every 1s (same data as /api/live/dashboard)
  /ws/prices — pushes BTC+ETH Polymarket mids + Binance spot prices every 1s
"""

import asyncio
import copy
import threading
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from loguru import logger

from app.pm_client import PolymarketClient
from app.runtime_config import get_config, _latest_lock


def _serialize_for_json(obj):
    """Deep-convert non-JSON-native objects to JSON-safe types."""
    if isinstance(obj, dict):
        return {k: _serialize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize_for_json(item) for item in obj]
    if isinstance(obj, float) and (obj != obj):  # NaN
        return None
    return obj

router = APIRouter(tags=["websocket"])

PUSH_INTERVAL_SEC = 1.0

# Shared singleton client for ws/prices — avoids creating a new httpx client per connection
_ws_prices_pm: PolymarketClient | None = None
_ws_prices_pm_lock = asyncio.Lock()

# Shared httpx client for Binance spot prices (fetched every second)
_ws_http: httpx.AsyncClient | None = None
_ws_http_lock = asyncio.Lock()


@router.websocket("/ws/live")
async def ws_live(ws: WebSocket):
    await ws.accept()
    strategy_ref = _get_strategy_ref()
    last_pong_at = datetime.now(timezone.utc)
    try:
        while True:
            if strategy_ref is None:
                await asyncio.sleep(PUSH_INTERVAL_SEC)
                continue
            # Heartbeat: send a ping frame every 30s if no activity
            if (datetime.now(timezone.utc) - last_pong_at).total_seconds() > 30:
                try:
                    await ws.send_ping(b"ping")
                    last_pong_at = datetime.now(timezone.utc)
                except Exception:
                    break
            # Thread-safe read of _latest to avoid racing with strategy loop updates
            with _latest_lock:
                raw_latest = getattr(strategy_ref, "_latest", None)
                snapshot = copy.deepcopy(raw_latest) if raw_latest else {}
            if snapshot:
                payload = _serialize_for_json(snapshot)
                payload["ts"] = datetime.now(timezone.utc).isoformat()
                await ws.send_json(payload)
                last_pong_at = datetime.now(timezone.utc)
            await asyncio.sleep(PUSH_INTERVAL_SEC)
    except WebSocketDisconnect:
        pass


@router.websocket("/ws/prices")
async def ws_prices(ws: WebSocket):
    global _ws_prices_pm
    await ws.accept()
    async with _ws_prices_pm_lock:
        if _ws_prices_pm is None:
            _ws_prices_pm = PolymarketClient()
    pm = _ws_prices_pm
    assets = ["BTC", "ETH"]
    try:
        while True:
            prices = {}
            for asset in assets:
                m = await _fetch_latest_price(pm, asset)
                if m:
                    prices[asset] = m
            spots = await _fetch_spot_prices()
            payload = {
                "_type": "prices",
                "ts": datetime.now(timezone.utc).isoformat(),
                "prices": prices,
                "spots": spots,
            }
            await ws.send_json(payload)
            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        pass


async def _fetch_spot_prices() -> dict:
    """Fetch BTC and ETH spot prices from Binance ticker API."""
    global _ws_http
    async with _ws_http_lock:
        if _ws_http is None:
            _ws_http = httpx.AsyncClient(timeout=5.0, limits=httpx.Limits(max_keepalive_connections=2, max_connections=5))
    try:
        btc_r = await _ws_http.get(
            "https://api.binance.com/api/v3/ticker/price",
            params={"symbol": "BTCUSDT"},
        )
        eth_r = await _ws_http.get(
            "https://api.binance.com/api/v3/ticker/price",
            params={"symbol": "ETHUSDT"},
        )
        btc = float(btc_r.json().get("price", 0)) if btc_r.status_code == 200 else 0.0
        eth = float(eth_r.json().get("price", 0)) if eth_r.status_code == 200 else 0.0
        return {"btc": btc, "eth": eth}
    except Exception as e:
        logger.debug("ws_prices spot fetch error: {}", e)
        return {"btc": 0.0, "eth": 0.0}


async def _fetch_latest_price(pm: PolymarketClient, asset: str) -> dict | None:
    cfg = get_config()
    slug_iv = str(cfg.get("market_interval_slug", "15m")).strip().lower()
    win_sec = int(float(cfg.get("market_duration_sec", 900)))
    markets = await pm.get_asset_updown_markets(
        asset, slug_interval=slug_iv, window_sec=win_sec, limit=2,
    )
    logger.debug("ws_prices _fetch_latest_price({}): {} markets returned", asset, len(markets))
    if not markets:
        return None
    m = markets[0]
    tokens = m.get("tokens", [])
    logger.debug("ws_prices {}: tokens={}", asset, [t.get("token_id") for t in tokens])
    if len(tokens) < 2:
        return None
    up_id = tokens[0].get("token_id", "")
    down_id = tokens[1].get("token_id", "")
    up_mid = await _safe_midpoint(pm, up_id)
    down_mid = await _safe_midpoint(pm, down_id)

    # CLOB fallback: use Gamma API outcomePrices (available immediately, no round-trip)
    if up_mid <= 0.0:
        up_mid = float(tokens[0].get("price", "0.5"))
    if down_mid <= 0.0:
        down_mid = float(tokens[1].get("price", "0.5"))

    logger.debug("ws_prices {} mids: up={} down={}", asset, up_mid, down_mid)
    slug = ""
    try:
        slug = m.get("_event_slug", "")
        logger.debug("ws_prices {}: slug={}", asset, slug)
        window_ts = int(slug.split("-")[-1])
        window_end = datetime.fromtimestamp(window_ts + win_sec, tz=timezone.utc)
    except Exception as e:
        logger.warning("ws_prices {}: window_end parse failed, slug={!r}: {}", asset, slug, e)
        now_utc = datetime.now(timezone.utc)
        offset = (
            now_utc.hour * 3600 + now_utc.minute * 60 + now_utc.second
        ) % win_sec if win_sec > 0 else 0
        window_end = now_utc + timedelta(seconds=win_sec - offset if win_sec > 0 else 900)
    return {
        "up_mid": up_mid,
        "down_mid": down_mid,
        "window_end": window_end.isoformat() if window_end else None,
        "condition_id": m.get("conditionId", ""),
    }


async def _safe_midpoint(pm: PolymarketClient, token_id: str) -> float:
    if not token_id:
        return 0.5
    try:
        return await pm.get_midpoint(token_id)
    except Exception as e:
        logger.debug("ws_prices _safe_midpoint({}) CLOB failed: {}, will use Gamma outcomePrices fallback", token_id[:12], e)
        return 0.0  # signal: use Gamma fallback in caller


_strategy_ref = None


@router.websocket("/ws/indicators")
async def ws_indicators(ws: WebSocket):
    """
    实时推送所有 10 个技术指标。

    数据来源：strategy._latest_indicators（策略主循环每 tick 预取计算）
    前端在连接后可通过发送 {"asset":"BTC"} 或 {"asset":"ETH"} 切换资产。
    """
    await ws.accept()
    strategy_ref = _get_strategy_ref()
    asset = "BTC"

    try:
        while True:
            # 非阻塞接收前端消息（切换资产）
            try:
                raw = await asyncio.wait_for(ws.receive_text(), timeout=0.1)
                import json
                msg = json.loads(raw)
                if isinstance(msg, dict) and msg.get("asset") in ("BTC", "ETH"):
                    asset = msg["asset"]
            except (asyncio.TimeoutError, Exception):
                pass

            if strategy_ref is not None:
                ind_data = getattr(strategy_ref, "_latest_indicators", {})
                data = ind_data.get(asset) or ind_data.get("BTC", {})
                if data:
                    out = _serialize_for_json(data)
                    out["_type"] = "indicators"
                    out["asset"] = asset
                    out["ts"] = datetime.now(timezone.utc).isoformat()
                    try:
                        await ws.send_json(out)
                    except Exception:
                        break

            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        pass


def close_ws_clients():
    """
    Close all shared clients held by the websocket module.
    Returns a list of pending close tasks that the caller MUST await before exiting.
    Call during application shutdown after the event loop is still running.

    Example:
        tasks = close_ws_clients()
        if tasks:
            asyncio.get_running_loop().run_until_complete(asyncio.gather(*tasks))
    """
    global _ws_prices_pm, _ws_http
    pending_tasks: list[asyncio.Task] = []

    if _ws_prices_pm is not None:
        try:
            loop = asyncio.get_running_loop()
            pending_tasks.append(loop.create_task(_ws_prices_pm.close()))
        except RuntimeError:
            # No running loop during late shutdown — close synchronously as last resort.
            try:
                asyncio.run(_ws_prices_pm.close())
            except Exception:
                pass
        _ws_prices_pm = None

    if _ws_http is not None:
        try:
            loop = asyncio.get_running_loop()
            pending_tasks.append(loop.create_task(_ws_http.aclose()))
        except RuntimeError:
            try:
                asyncio.run(_ws_http.aclose())
            except Exception:
                pass
        _ws_http = None

    return pending_tasks


def set_strategy_ref(ref):
    global _strategy_ref
    _strategy_ref = ref


def _get_strategy_ref():
    return _strategy_ref
