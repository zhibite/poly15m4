"""
market_service.py — Polymarket 市场服务
负责：发现窗口市场、获取实时价格、选择入场方向。
不含交易逻辑。
"""

from datetime import datetime, timezone
from typing import Optional

import httpx

from loguru import logger

from app.models import Side
from app.pm_client import PolymarketClient, parse_window_end
from app.strategy.models import MarketInfo


class MarketService:
    """
    封装 Polymarket 市场相关的只读操作：
    - 发现当前活跃的 up/down 市场（周期可配置，如 5m / 15m）
    - 获取 UP/DOWN 实时中间价
    - 根据阈值选择入场方向（UP 或 DOWN）
    """

    def __init__(
        self,
        pm: PolymarketClient,
        *,
        slug_interval: str = PolymarketClient.DEFAULT_UPDOWN_INTERVAL_SLUG,
        window_sec: int = PolymarketClient.DEFAULT_UPDOWN_WINDOW_SEC,
    ):
        self._pm = pm
        default_w = PolymarketClient.DEFAULT_UPDOWN_WINDOW_SEC
        self._slug_interval = slug_interval
        self._window_sec = window_sec if window_sec > 0 else default_w
        # Cache neg_risk per condition_id — neg_risk is immutable within a window lifecycle
        self._neg_risk_cache: dict[str, bool] = {}
        # Discovery cache: asset -> (slot_start_epoch, MarketInfo).
        # Game markets are immutable within a slot, so we only query Gamma once
        # per window instead of every strategy tick.
        self._discovery_cache: dict[str, tuple[int, MarketInfo]] = {}
        # Shared client for Binance spot-price lookups (avoid one client per tick).
        self._spot_client: httpx.AsyncClient | None = None

    def update_interval(self, slug_interval: str, window_sec: int):
        dslug = PolymarketClient.DEFAULT_UPDOWN_INTERVAL_SLUG
        default_w = PolymarketClient.DEFAULT_UPDOWN_WINDOW_SEC
        new_slug = slug_interval or dslug
        new_window = window_sec if window_sec > 0 else default_w
        # Only invalidate when the interval actually changed — this method is
        # called on every strategy tick by _sync_config().
        if new_slug != self._slug_interval or new_window != self._window_sec:
            self._discovery_cache.clear()
        self._slug_interval = new_slug
        self._window_sec = new_window

    def clear_neg_risk_cache(self, condition_id: str | None = None) -> None:
        """
        Clear neg_risk / discovery cache entries.
        If condition_id is provided, only that entry is removed.
        If None, the entire cache is cleared.
        """
        if condition_id is not None:
            self._neg_risk_cache.pop(condition_id, None)
            for asset, (_slot, mkt) in list(self._discovery_cache.items()):
                if mkt.condition_id == condition_id:
                    self._discovery_cache.pop(asset, None)
        else:
            self._neg_risk_cache.clear()
            self._discovery_cache.clear()

    def clear_discovery_cache(self, asset: str | None = None) -> None:
        if asset is None:
            self._discovery_cache.clear()
        else:
            self._discovery_cache.pop(asset, None)

    def _get_spot_client(self) -> httpx.AsyncClient:
        client = self._spot_client
        if client is None or client.is_closed:
            client = httpx.AsyncClient(
                timeout=5.0,
                limits=httpx.Limits(max_connections=4, max_keepalive_connections=2),
            )
            self._spot_client = client
        return client

    async def close(self) -> None:
        if self._spot_client is not None and not self._spot_client.is_closed:
            await self._spot_client.aclose()
        self._spot_client = None

    async def discover_market(self, asset: str) -> Optional[MarketInfo]:
        """发现当前还未结束的市场，返回 MarketInfo 或 None.

        Results are cached per asset for the duration of one window slot, so the
        strategy loop does not hit Gamma + two midpoint endpoints every second.
        """
        now = datetime.now(timezone.utc)
        window_sec = self._window_sec if self._window_sec > 0 else 900
        slot = int(now.timestamp()) // window_sec * window_sec
        cached = self._discovery_cache.get(asset)
        if cached is not None and cached[0] == slot:
            if cached[1].window_end > now:
                return cached[1]
            # Defensive: never serve an already-expired cached market.
            self._discovery_cache.pop(asset, None)

        markets = await self._pm.get_asset_updown_markets(
            asset,
            slug_interval=self._slug_interval,
            window_sec=self._window_sec,
            limit=3,
        )
        # Slot may have rolled over while the HTTP request was in flight.
        now = datetime.now(timezone.utc)
        slot = int(now.timestamp()) // window_sec * window_sec
        for m in markets:
            end_dt = parse_window_end(m)
            if end_dt is None:
                continue
            if end_dt <= now:
                continue
            tokens = m.get("tokens", [])
            if len(tokens) < 2:
                continue
            try:
                window_ts = int(m.get("_event_slug", "").split("-")[-1])
                window_start = datetime.fromtimestamp(window_ts, tz=timezone.utc)
            except Exception:
                window_start = None

            # window_end comes from the authoritative API field endDate
            window_end = end_dt
            # Guard: if window_start is invalid (slug parse failed) or > window_end, derive it
            # from window_end since window_end - window_start == window_sec == 900s (for 15m)
            if window_start is None or window_start >= window_end:
                window_start = datetime.fromtimestamp(
                    window_end.timestamp() - self._window_sec, tz=timezone.utc,
                )
            # Do not switch to a future window before the current one is indexed;
            # the cached discovery entry would otherwise pin the wrong market for
            # the whole slot.
            if window_start > now:
                continue
            condition_id = m.get("conditionId", "")
            neg_risk = self._neg_risk_cache.get(condition_id, None)
            if neg_risk is None:
                neg_risk = False
                token_id_for_check = tokens[0].get("token_id", "")
                try:
                    neg_risk = await self._pm.get_neg_risk(token_id_for_check)
                except Exception as exc:
                    logger.warning(
                        "get_neg_risk failed for {} ({}): {} — defaulting to False; "
                        "if this market is actually neg_risk=True, redeem will use the wrong contract address",
                        condition_id[:12], token_id_for_check[:12], exc,
                    )
                self._neg_risk_cache[condition_id] = neg_risk

            # 窗口开启时刻的 Binary Option 挂单价
            window_open_up = window_open_down = 0.5
            try:
                window_open_up = await self._pm.get_midpoint(tokens[0].get("token_id", ""))
            except Exception as exc_up:
                logger.warning(
                    "get_midpoint failed for up token {} ({}): {} — using fallback 0.5",
                    condition_id[:12], tokens[0].get("token_id", "")[:12], exc_up,
                )
            try:
                window_open_down = await self._pm.get_midpoint(tokens[1].get("token_id", ""))
            except Exception as exc_dn:
                logger.warning(
                    "get_midpoint failed for down token {} ({}): {} — using fallback 0.5",
                    condition_id[:12], tokens[1].get("token_id", "")[:12], exc_dn,
                )

            market_info = MarketInfo(
                asset=asset,
                condition_id=m.get("conditionId", ""),
                token_up_id=tokens[0].get("token_id", ""),
                token_down_id=tokens[1].get("token_id", ""),
                window_start=window_start,
                window_end=window_end,
                neg_risk=neg_risk,
                window_open_up=window_open_up,
                window_open_down=window_open_down,
                window_spot_price=await self.fetch_spot_price(asset),
            )
            self._discovery_cache[asset] = (slot, market_info)
            return market_info
        return None

    async def fetch_prices(self, market: MarketInfo | None, asset: str = "") -> tuple[float, float]:
        """获取 UP 和 DOWN 的实时中间价.

        若 market 为 None（窗口未激活），先 discover 市场再取价，
        确保未启用品种（如 ETH）的价格也能刷新供前端仪表盘使用。

        Returns:
          (up_mid, down_mid) — 始终返回有效价格，失败时返回 (0.5, 0.5)。
          调用方必须用 getattr(mkt, "_up_mid", ...) 而非直接访问属性。
        """
        up_mid = down_mid = 0.5
        try:
            mkt = market
            if mkt is None and asset:
                mkt = await self.discover_market(asset)
            if mkt is not None:
                up_mid = await self._pm.get_midpoint(mkt.token_up_id)
                down_mid = await self._pm.get_midpoint(mkt.token_down_id)
        except Exception:
            pass
        # 回退：若 API 返回异常值，用保守值而非 0
        if up_mid <= 0 or up_mid >= 1:
            up_mid = 0.5
        if down_mid <= 0 or down_mid >= 1:
            down_mid = 0.5
        return up_mid, down_mid

    async def fetch_spot_price(self, asset: str) -> float:
        """
        通过 Binance 公开 REST API 获取 BTC / ETH 美元现货最新价。
        API: GET https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT
        Returns 0.0 on failure (不影响主流程，回退为不触发过滤).
        """
        symbol_map = {"BTC": "BTCUSDT", "ETH": "ETHUSDT"}
        symbol = symbol_map.get(asset)
        if not symbol:
            return 0.0
        try:
            resp = await self._get_spot_client().get(
                "https://api.binance.com/api/v3/ticker/price",
                params={"symbol": symbol},
            )
            resp.raise_for_status()
            data = resp.json()
            return float(data.get("price", 0.0))
        except Exception as exc:
            logger.debug("fetch_spot_price {} failed: {}", symbol, exc)
            return 0.0

