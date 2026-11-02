"""
pm_client.py — Polymarket Gamma + CLOB v2 public API client.
Handles market discovery, midpoint prices, fee rates, and resolution queries.
"""

import asyncio
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import httpx
from loguru import logger


def _parse_end_dt(raw: str) -> Optional[datetime]:
    if not raw:
        return None
    raw = raw.strip()
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


@dataclass(frozen=True)
class ClientSettings:
    gamma_api: str
    polymarket_host: str


class PolymarketClient:
    """
    v2 Gamma + CLOB public API client.
    No authentication required for all methods.
    """

    def __init__(
        self,
        *,
        gamma_api: Optional[str] = None,
        polymarket_host: Optional[str] = None,
    ):
        self.settings = ClientSettings(
            gamma_api=(gamma_api or os.getenv("GAMMA_API") or "https://gamma-api.polymarket.com"),
            polymarket_host=(polymarket_host or os.getenv("POLYMARKET_HOST") or "https://clob.polymarket.com"),
        )
        self._http: Optional[httpx.AsyncClient] = None

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=httpx.Timeout(15.0, connect=5.0),
                limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            )
        return self._http

    async def close(self):
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    # ── Market discovery ──────────────────────────────────────────────────────────

    # Default aligns with prod 15m Polymarket crypto up/down ladders.
    DEFAULT_UPDOWN_INTERVAL_SLUG: str = "15m"
    DEFAULT_UPDOWN_WINDOW_SEC: int = 900

    async def get_asset_updown_markets(
        self,
        asset: str,
        *,
        slug_interval: str | None = None,
        window_sec: int | None = None,
        limit: int = 5,
    ) -> list[dict]:
        """
        Fetch active up/down ladder markets for the given asset (15m default, …).
        Slug shape: `{asset.lower()}-updown-{slug_interval}-{unix_slot}`.
        """
        if slug_interval is None:
            slug_interval = self.DEFAULT_UPDOWN_INTERVAL_SLUG
        if window_sec is None:
            window_sec = self.DEFAULT_UPDOWN_WINDOW_SEC
        asset = asset.upper()
        c = await self._client()
        prefix = f"{asset.lower()}-updown-{slug_interval}"
        now = int(time.time())
        if window_sec <= 0:
            window_sec = self.DEFAULT_UPDOWN_WINDOW_SEC
        base = (now // window_sec) * window_sec
        cand_ts = [base - 2 * window_sec, base - window_sec, base, base + window_sec, base + 2 * window_sec]
        seen: set[int] = set()
        candidates = [ts for ts in cand_ts if ts > 0 and not (ts in seen or seen.add(ts))]

        markets: list[dict] = []
        for ts in candidates:
            # Expired windows can never be selected by discover_market — skip
            # them to avoid pointless Gamma requests on every discovery.
            if ts + window_sec <= now:
                continue
            slug = f"{prefix}-{ts}"
            try:
                resp = await c.get(
                    f"{self.settings.gamma_api}/events",
                    params={"slug": slug},
                    timeout=8,
                )
                if resp.status_code != 200:
                    continue
                events = resp.json()
                if isinstance(events, dict):
                    events = [events] if events else []

                for ev in events:
                    if prefix not in (ev.get("slug") or ""):
                        continue
                    for m in ev.get("markets", []):
                        raw_ids = m.get("clobTokenIds") or "[]"
                        try:
                            token_ids = json.loads(raw_ids)
                        except Exception:
                            token_ids = []

                        raw_prices = m.get("outcomePrices") or '["0.5", "0.5"]'
                        try:
                            prices = json.loads(raw_prices)
                        except Exception:
                            prices = ["0.5", "0.5"]

                        m["tokens"] = [
                            {
                                "token_id": token_ids[0] if len(token_ids) > 0 else "",
                                "outcome": "Up",
                                "price": prices[0] if prices else "0.5",
                            },
                            {
                                "token_id": token_ids[1] if len(token_ids) > 1 else "",
                                "outcome": "Down",
                                "price": prices[1] if len(prices) > 1 else "0.5",
                            },
                        ]

                        # Validate outcome order (M1 — Polymarket always uses [UP, DOWN]
                        # but we check explicitly to catch any API changes)
                        outcomes = [t.get("outcome", "").lower() for t in m["tokens"]]
                        if outcomes[0] != "up" or outcomes[1] != "down":
                            logger.warning(
                                "get_asset_updown_markets({}): unexpected outcome order {!r}, skipping",
                                asset, outcomes,
                            )
                            continue

                        m["_event_slug"] = ev.get("slug", "")
                        m["_up_price"] = float(prices[0]) if prices else 0.5
                        m["_down_price"] = float(prices[1]) if len(prices) > 1 else 0.5
                        # M4: store neg_risk flag (default False; per-market CLOB call resolves it)
                        m["_neg_risk"] = False
                        markets.append(m)
                # The current window's event is authoritative and sufficient —
                # stop querying later slots.
                if markets:
                    break
                if asset:
                    logger.debug(
                        "get_asset_updown_markets({} {} {}s) returned 0 markets for slug prefix {}",
                        asset, slug_interval, window_sec, prefix,
                    )
            except Exception as e:
                logger.debug("Slug {} query failed: {}", slug, e)

        return markets[:limit]

    async def get_asset_5min_markets(self, asset: str, limit: int = 5) -> list[dict]:
        """Backward-compatible alias: 5-minute windows (300s)."""
        return await self.get_asset_updown_markets(
            asset,
            slug_interval="5m",
            window_sec=300,
            limit=limit,
        )

    async def get_markets_by_condition_id(self, condition_id: str) -> Optional[dict]:
        """Fetch a single market by its condition_id (CLOB market info)."""
        c = await self._client()
        try:
            resp = await c.get(
                f"{self.settings.polymarket_host}/markets/{condition_id}",
                timeout=8,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.debug("get_markets_by_condition_id({}) failed: {}", condition_id[:12], e)
            return None

    # ── CLOB price data ─────────────────────────────────────────────────────────

    async def get_midpoint(self, token_id: str) -> float:
        """Get current midpoint price for a token from CLOB."""
        c = await self._client()
        resp = await c.get(
            f"{self.settings.polymarket_host}/midpoint",
            params={"token_id": token_id},
            timeout=3.0,
        )
        if resp.status_code == 404:
            raise ValueError(
                f"midpoint for {token_id[:12]} → CLOB 404 "
                "(token not indexed yet — new window, normal during first ~30s)"
            )
        resp.raise_for_status()
        data = resp.json()
        val = float(data.get("mid", 0.0))
        if val <= 0.0:
            raise ValueError(
                f"midpoint for {token_id[:12]} returned {val} — "
                "CLOB has no trading data (token may not be listed)"
            )
        return val

    async def get_tick_size(self, token_id: str) -> str:
        """Get minimum tick size for a token (v2 CLOB API)."""
        c = await self._client()
        resp = await c.get(
            f"{self.settings.polymarket_host}/tick-size",
            params={"token_id": token_id},
            timeout=5.0,
        )
        resp.raise_for_status()
        data = resp.json()
        return str(data.get("minimum_tick_size", "0.0001"))

    async def get_neg_risk(self, token_id: str) -> bool:
        """Check if token uses Neg Risk exchange."""
        c = await self._client()
        resp = await c.get(
            f"{self.settings.polymarket_host}/neg-risk",
            params={"token_id": token_id},
            timeout=5.0,
        )
        resp.raise_for_status()
        data = resp.json()
        return bool(data.get("neg_risk", False))

    async def get_clob_market_info(self, condition_id: str) -> Optional[dict]:
        """
        Fetch all CLOB market parameters in one call (v2).
        Returns: {mts, mos, fd: {r, e, to}, t: [{t, o}], ...}
        """
        c = await self._client()
        try:
            resp = await c.get(
                f"{self.settings.polymarket_host}/markets/{condition_id}",
                timeout=8,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.debug("get_clob_market_info({}) failed: {}", condition_id[:12], e)
            return None

    # ── Fee rate ────────────────────────────────────────────────────────────────

    async def get_fee_rate_bps(self, token_id: str) -> int:
        """Get taker fee rate in basis points for a token."""
        c = await self._client()
        try:
            resp = await c.get(
                f"{self.settings.polymarket_host}/fee-rate",
                params={"token_id": token_id},
                timeout=8,
            )
            resp.raise_for_status()
            data = resp.json()
            rate = int(data.get("base_fee") or data.get("fee_rate_bps") or -1)
            if rate < 0:
                raise ValueError(f"fee-rate response has no base_fee/fee_rate_bps: {data}")
            return rate
        except Exception as e:
            raise RuntimeError(f"get_fee_rate_bps({token_id[:12]}) failed: {e}") from e

    # ── Market resolution ───────────────────────────────────────────────────────

    async def get_market_resolution(self, condition_id: str) -> Optional[dict]:
        """Query Gamma for market resolution result."""
        c = await self._client()
        try:
            resp = await c.get(f"{self.settings.gamma_api}/markets/{condition_id}")
            resp.raise_for_status()
            data = resp.json()
            resolved = bool(data.get("resolved", False))
            if not resolved:
                return {"resolved": False, "up_wins": None, "outcome": ""}

            raw = data.get("outcomePrices") or '["0","0"]'
            try:
                prices = json.loads(raw)
            except Exception:
                prices = ["0", "0"]

            up_price = float(prices[0]) if prices else 0.0
            up_wins = up_price >= 0.5
            outcome_text = (data.get("outcome") or "").strip()
            if outcome_text.lower() in ("up", "yes"):
                up_wins = True
            elif outcome_text.lower() in ("down", "no"):
                up_wins = False
            return {"resolved": True, "up_wins": up_wins, "outcome": outcome_text}
        except Exception as e:
            # HTTPStatusError has no .status_code directly; the actual status code is on
            # the response object.  httpx returns 422 when Gamma doesn't know the result
            # yet (market not settled) — this is expected, so log at debug level.
            resp = getattr(e, "response", None)
            status_code = getattr(resp, "status_code", None)
            is_422 = status_code == 422
            level = "debug" if is_422 else "warning"
            getattr(logger, level)("get_market_resolution({}) failed: {}", condition_id[:12], e)
            return None

    async def get_market_info(self, token_id: str) -> Optional[dict]:
        """Get market details by token_id (v2 CLOB)."""
        c = await self._client()
        try:
            resp = await c.get(
                f"{self.settings.polymarket_host}/markets/{token_id}",
                timeout=8,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.debug("get_market_info({}) failed: {}", token_id[:12], e)
            return None

    # ── Orderbook ─────────────────────────────────────────────────────────────

    async def get_orderbook(self, token_id: str) -> Optional[dict]:
        """
        获取单个 token 的完整盘口（bids / asks）。

        REST: GET /book?token_id=<token_id>
        公开接口，无需认证。

        返回结构示例：
        {
            "market": "0x...",
            "asset_id": "0x...",
            "timestamp": "1234567890",
            "hash": "...",
            "bids": [{"price": "0.45", "size": "100"}, ...],
            "asks": [{"price": "0.46", "size": "150"}, ...],
            "min_order_size": "1",
            "tick_size": "0.01",
            "neg_risk": False,
            "last_trade_price": "0.45",
        }
        """
        c = await self._client()
        try:
            resp = await c.get(
                f"{self.settings.polymarket_host}/book",
                params={"token_id": token_id},
                timeout=5.0,
            )
            if resp.status_code == 404:
                logger.debug("get_orderbook({}) → 404, no orderbook yet", token_id[:12])
                return None
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.debug("get_orderbook({}) failed: {}", token_id[:12], e)
            return None

    async def get_orderbooks_batch(self, token_ids: list[str]) -> list[dict]:
        """
        批量获取多个 token 的盘口。

        REST: POST /books
        公开接口，无需认证。
        最多一次 500 个 token。

        返回: [orderbook, ...] 列表，无数据的 token 对应空 dict 或 None。
        """
        if not token_ids:
            return []
        c = await self._client()
        try:
            resp = await c.post(
                f"{self.settings.polymarket_host}/books",
                json=[{"token_id": tid} for tid in token_ids],
                timeout=10.0,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.debug("get_orderbooks_batch({} tokens) failed: {}", len(token_ids), e)
            return []


def parse_window_end(market: dict | None) -> Optional[datetime]:
    if not market:
        return None
    raw = market.get("endDate") or market.get("end_date") or ""
    return _parse_end_dt(raw)


async def safe_gather(*coros):
    return await asyncio.gather(*coros, return_exceptions=True)


# ── 滑点估算 ───────────────────────────────────────────────────────────────

@dataclass
class SlippageEstimate:
    """
    滑点估算结果。

    所有价格均为概率值（0~1），如 0.78 表示 78% 胜率。

    fill_price  基于盘口档位估算的实际成交价
    slippage_pct  滑点百分比（相对决策价的百分比）
    pct_of_depth  盘口可用深度 / 本次投入的覆盖倍数
                  （买入按 USDC、卖出按 token 计；<1 表示深度不足以完全成交）
    sufficient  盘口深度是否至少覆盖 min_coverage × 本次投入
    """
    fill_price: float       # 估算成交价
    slippage_pct: float     # 滑点（%，正数 = 对买方不利）
    pct_of_depth: float      # 深度覆盖倍数（available_depth / requested）
    sufficient: bool        # 深度是否足够（coverage >= min_coverage）
    depth_at_fill: float    # 盘口可用 token 总数
    levels_used: int        # 消耗的档位数
    error: str = ""         # 估算失败原因


async def estimate_buy_slippage(
    token_id: str,
    stake_usdc: float,
    *,
    pm_client: "PolymarketClient",
    min_coverage: float = 2.0,
) -> SlippageEstimate:
    """
    估算市价买入指定 token 的滑点。

    遍历盘口 asks（从低到高），逐档消耗直到覆盖 stake_usdc，
    估算加权平均成交价和实际滑点。

    min_coverage: 要求盘口深度至少是投入的多少倍（默认 2x）
    """
    book = await pm_client.get_orderbook(token_id)
    if not book:
        return SlippageEstimate(
            fill_price=0.5, slippage_pct=0.0,
            pct_of_depth=0.0, sufficient=False,
            depth_at_fill=0.0, levels_used=0,
            error="no_orderbook",
        )

    asks = book.get("asks") or []
    if not asks:
        return SlippageEstimate(
            fill_price=0.5, slippage_pct=0.0,
            pct_of_depth=0.0, sufficient=False,
            depth_at_fill=0.0, levels_used=0,
            error="no_asks",
        )

    # 解析 asks: [{"price": "0.46", "size": "150"}, ...]
    # size 是 token 数量（概率单位），不是美元
    parsed: list[tuple[float, float]] = []
    for level in asks:
        price = float(level.get("price", 0))
        size = float(level.get("size", 0))
        if price > 0 and size > 0:
            parsed.append((price, size))

    if not parsed:
        return SlippageEstimate(
            fill_price=0.5, slippage_pct=0.0,
            pct_of_depth=0.0, sufficient=False,
            depth_at_fill=0.0, levels_used=0,
            error="empty_book",
        )

    # 盘口总深度（用于计算覆盖倍数，而不是只看本次成交消耗）
    total_ask_tokens = sum(size for _, size in parsed)
    total_ask_usdc = sum(price * size for price, size in parsed)

    filled_cost = 0.0
    filled_tokens = 0.0
    levels_used = 0

    for price, size in parsed:
        level_cost = price * size
        if filled_cost + level_cost <= stake_usdc:
            filled_cost += level_cost
            filled_tokens += size
            levels_used += 1
        else:
            remaining = stake_usdc - filled_cost
            remaining_tokens = remaining / price if price > 0 else 0
            filled_cost += remaining
            filled_tokens += remaining_tokens
            levels_used += 1
            break

    if filled_tokens <= 0:
        return SlippageEstimate(
            fill_price=0.5, slippage_pct=0.0,
            pct_of_depth=0.0, sufficient=False,
            depth_at_fill=total_ask_tokens, levels_used=levels_used,
            error="empty_book",
        )

    fill_price = filled_cost / filled_tokens

    # 滑点 = 相对最佳卖价的损耗（%）
    best_ask = parsed[0][0]
    slippage_pct = abs(fill_price - best_ask) / best_ask * 100.0 if best_ask > 1e-9 else 0.0

    # 覆盖倍数：可用深度 / 本次投入。>= min_coverage 才算深度充足。
    pct_of_depth = total_ask_usdc / stake_usdc if stake_usdc > 0 else 0.0
    sufficient = pct_of_depth >= min_coverage

    return SlippageEstimate(
        fill_price=fill_price,
        slippage_pct=slippage_pct,
        pct_of_depth=pct_of_depth,
        sufficient=sufficient,
        depth_at_fill=total_ask_tokens,
        levels_used=levels_used,
    )


async def estimate_sell_slippage(
    token_id: str,
    tokens: float,
    *,
    pm_client: "PolymarketClient",
    min_coverage: float = 2.0,
) -> SlippageEstimate:
    """
    估算市价卖出 token 的滑点。

    遍历盘口 bids（从高到低），逐档消耗直到卖出 tokens，
    估算加权平均成交价和实际滑点。
    """
    book = await pm_client.get_orderbook(token_id)
    if not book:
        return SlippageEstimate(
            fill_price=0.5, slippage_pct=0.0,
            pct_of_depth=0.0, sufficient=False,
            depth_at_fill=0.0, levels_used=0,
            error="no_orderbook",
        )

    bids = book.get("bids") or []
    if not bids:
        return SlippageEstimate(
            fill_price=0.5, slippage_pct=0.0,
            pct_of_depth=0.0, sufficient=False,
            depth_at_fill=0.0, levels_used=0,
            error="no_bids",
        )

    parsed: list[tuple[float, float]] = []
    for level in bids:
        price = float(level.get("price", 0))
        size = float(level.get("size", 0))
        if price > 0 and size > 0:
            parsed.append((price, size))

    if not parsed:
        return SlippageEstimate(
            fill_price=0.5, slippage_pct=0.0,
            pct_of_depth=0.0, sufficient=False,
            depth_at_fill=0.0, levels_used=0,
            error="empty_book",
        )

    # 盘口总买盘深度（token 数）
    total_bid_tokens = sum(size for _, size in parsed)

    filled_cost = 0.0
    filled_tokens = 0.0
    levels_used = 0

    for price, size in parsed:
        consume = min(size, tokens - filled_tokens)
        if consume <= 0:
            break
        filled_cost += price * consume
        filled_tokens += consume
        levels_used += 1

        if filled_tokens >= tokens - 1e-9:
            break

    if filled_tokens <= 0:
        return SlippageEstimate(
            fill_price=0.5, slippage_pct=0.0,
            pct_of_depth=0.0, sufficient=False,
            depth_at_fill=total_bid_tokens, levels_used=0,
            error="empty_book",
        )

    fill_price = filled_cost / filled_tokens

    # 滑点 = 相对最佳买价的损耗（%）
    best_bid = parsed[0][0]
    slippage_pct = abs(best_bid - fill_price) / best_bid * 100.0 if best_bid > 1e-9 else 0.0

    # 覆盖倍数：可用买盘 token 数 / 本次卖出 token 数
    pct_of_depth = total_bid_tokens / tokens if tokens > 0 else 0.0
    sufficient = pct_of_depth >= min_coverage

    return SlippageEstimate(
        fill_price=fill_price,
        slippage_pct=slippage_pct,
        pct_of_depth=pct_of_depth,
        sufficient=sufficient,
        depth_at_fill=total_bid_tokens,
        levels_used=levels_used,
    )
