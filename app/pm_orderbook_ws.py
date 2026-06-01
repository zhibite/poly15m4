"""
pm_orderbook_ws.py — Polymarket CLOB 盘口 WebSocket 实时客户端

WebSocket 端点：wss://ws-subscriptions-clob.polymarket.com/ws/market
公开接口，无需认证。

消息类型：
  book          — 完整盘口快照（订阅时 + 重大变化时推送）
  price_change  — 单个价格档位变化（新单 / 撤销）
  best_bid_ask  — 盘口顶部变化（需 custom_feature_enabled）
  tick_size_change — tick size 变化（价格达到 >0.96 或 <0.04 时触发）

心跳：服务端无自动 ping，需客户端每 10s 发一次 PING

使用方式：
  ws = PMOrderbookWS(token_ids=["token_up_id", "token_down_id"])
  ws.on_orderbook(lambda ob: print(ob.bids, ob.asks))
  asyncio.run(ws.connect())
"""

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

import websockets
from loguru import logger


@dataclass
class OrderbookLevel:
    price: float
    size: float


@dataclass
class OrderbookSnapshot:
    """
    盘口快照。

    bids / asks 均按价格降序排列（bids 高→低，asks 低→高）。
    price / size 均为 float 类型（API 返回字符串）。
    """
    asset_id: str
    bids: list[OrderbookLevel] = field(default_factory=list)
    asks: list[OrderbookLevel] = field(default_factory=list)
    timestamp: int = 0
    hash: str = ""
    tick_size: float = 0.01
    min_order_size: float = 1.0
    neg_risk: bool = False
    last_trade_price: float = 0.5

    @property
    def best_bid(self) -> Optional[OrderbookLevel]:
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> Optional[OrderbookLevel]:
        return self.asks[0] if self.asks else None

    @property
    def spread(self) -> float:
        if self.best_bid and self.best_ask:
            return self.best_ask.price - self.best_bid.price
        return 0.0

    @property
    def spread_pct(self) -> float:
        mid = self.midpoint
        if mid > 0:
            return self.spread / mid
        return 0.0

    @property
    def midpoint(self) -> float:
        if self.best_bid and self.best_ask:
            return (self.best_bid.price + self.best_ask.price) / 2.0
        return 0.5

    @property
    def bid_vol_total(self) -> float:
        return sum(l.size for l in self.bids)

    @property
    def ask_vol_total(self) -> float:
        return sum(l.size for l in self.asks)

    @property
    def imbalance(self) -> float:
        """
        盘口失衡度 ∈ [-1, 1]。
        > 0 → 买方深度占优（bids 更多更大）
        < 0 → 卖方深度占优
        0  → 均衡
        """
        total = self.bid_vol_total + self.ask_vol_total
        if total <= 0:
            return 0.0
        return (self.bid_vol_total - self.ask_vol_total) / total

    @classmethod
    def from_dict(cls, data: dict, asset_id: str) -> "OrderbookSnapshot":
        def parse_levels(levels: list) -> list[OrderbookLevel]:
            return [
                OrderbookLevel(price=float(l.get("price", 0)), size=float(l.get("size", 0)))
                for l in (levels or [])
            ]

        return cls(
            asset_id=asset_id,
            bids=parse_levels(data.get("bids", [])),
            asks=parse_levels(data.get("asks", [])),
            timestamp=int(data.get("timestamp", 0)),
            hash=str(data.get("hash", "")),
            tick_size=float(data.get("tick_size", 0.01)),
            min_order_size=float(data.get("min_order_size", 1.0)),
            neg_risk=bool(data.get("neg_risk", False)),
            last_trade_price=float(data.get("last_trade_price", 0.5)),
        )


def _level_from_pc(pc: dict) -> Optional[OrderbookLevel]:
    """从 price_change 条目构造 OrderbookLevel，size=0 表示撤销该档位"""
    price = float(pc.get("price", 0))
    size = float(pc.get("size", 0))
    if price <= 0:
        return None
    return OrderbookLevel(price=price, size=size)


class PMOrderbookWS:
    """
    Polymarket CLOB 盘口 WebSocket 客户端。

    使用方式：
        ws = PMOrderbookWS(token_ids=[up_token_id, down_token_id])
        ws.on_orderbook(lambda ob: print(ob.imbalance))
        await ws.connect()

    内部维护每个 token_id 的本地盘口快照（初始为空），
    收到 book 快照时全量替换，收到 price_change 时增量更新。
    """

    PING_INTERVAL_SEC: float = 10.0

    def __init__(
        self,
        token_ids: list[str],
        ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    ):
        self._token_ids = token_ids
        self._ws_url = ws_url
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False
        self._books: dict[str, OrderbookSnapshot] = {}   # asset_id → snapshot
        self._lock = asyncio.Lock()
        self._recv_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._handlers: list[Callable[[str, OrderbookSnapshot], None]] = []

    # ── 事件回调 ─────────────────────────────────────────────────────────

    def on_orderbook(
        self, handler: Callable[[str, OrderbookSnapshot], None]
    ) -> "PMOrderbookWS":
        """注册盘口更新回调：handler(asset_id, snapshot)。"""
        self._handlers.append(handler)
        return self

    # ── 连接管理 ─────────────────────────────────────────────────────

    async def connect(self) -> None:
        """建立 WebSocket 连接并开始收发消息。"""
        if self._running:
            return
        self._running = True
        self._recv_task = asyncio.create_task(self._recv_loop())
        self._ping_task = asyncio.create_task(self._ping_loop())

    async def close(self) -> None:
        """优雅关闭连接。"""
        self._running = False
        for t in [self._recv_task, self._ping_task]:
            if t:
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass
        if self._ws:
            await self._ws.close()
            self._ws = None

    async def wait_closed(self) -> None:
        """等待连接完全关闭。"""
        if self._ws:
            try:
                await self._ws.wait_closed()
            except Exception:
                pass

    # ── 快照查询 ─────────────────────────────────────────────────────

    def get_snapshot(self, asset_id: str) -> Optional[OrderbookSnapshot]:
        """返回当前本地缓存的盘口快照。首次连接时可能为空。"""
        return self._books.get(asset_id)

    def get_imbalance(self, asset_id: str) -> float:
        """返回当前盘口失衡度，snapshot 不存在时返回 0。"""
        ob = self._books.get(asset_id)
        return ob.imbalance if ob else 0.0

    # ── 内部：消息收发 ───────────────────────────────────────────────

    async def _send_subscription(self, ws) -> None:
        """连接建立后立即发送订阅消息。"""
        payload = {
            "assets_ids": self._token_ids,
            "type": "market",
            "custom_feature_enabled": True,
        }
        await ws.send(json.dumps(payload))
        logger.info(
            "PMOrderbookWS subscribed to {} tokens: {}",
            len(self._token_ids),
            [tid[:12] + "..." for tid in self._token_ids],
        )

    async def _recv_loop(self) -> None:
        while self._running:
            try:
                async with websockets.connect(
                    self._ws_url,
                    ping_interval=None,
                ) as ws:
                    self._ws = ws
                    await self._send_subscription(ws)

                    while self._running:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        except asyncio.TimeoutError:
                            continue
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        await self._dispatch(msg)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if not self._running:
                    break
                logger.warning(
                    "PMOrderbookWS connection error: {} — reconnecting in 3s", exc
                )
                await asyncio.sleep(3.0)

    async def _ping_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self.PING_INTERVAL_SEC)
            if self._ws and self._running:
                try:
                    await self._ws.send('{"type":"PING"}')
                except Exception:
                    pass

    async def _dispatch(self, msg: dict) -> None:
        event_type = msg.get("event_type") or msg.get("type", "")
        data = msg.get("data", msg)

        if event_type in ("book", "snapshot"):
            asset_id = data.get("asset_id", "")
            if not asset_id:
                return
            snap = OrderbookSnapshot.from_dict(data, asset_id)
            async with self._lock:
                self._books[asset_id] = snap
            self._notify(asset_id, snap)

        elif event_type == "price_change":
            asset_id = data.get("asset_id", "")
            if not asset_id:
                return
            changes = data.get("price_changes", [])
            async with self._lock:
                snap = self._books.get(asset_id)
                if snap is None:
                    return
                for pc in changes:
                    level = _level_from_pc(pc)
                    if level is None:
                        continue
                    side = pc.get("side", "").lower()
                    if side == "bid":
                        snap = self._apply_level(snap, level, is_bid=True)
                    elif side == "ask":
                        snap = self._apply_level(snap, level, is_bid=False)
                self._books[asset_id] = snap
            self._notify(asset_id, snap)

        elif event_type == "tick_size_change":
            asset_id = data.get("asset_id", "")
            if not asset_id:
                return
            new_tick = float(data.get("new_tick_size", 0.01))
            async with self._lock:
                if asset_id in self._books:
                    self._books[asset_id].tick_size = new_tick

        elif event_type in ("PONG", "pong", ""):
            pass

    def _apply_level(
        self, snap: OrderbookSnapshot, level: OrderbookLevel, is_bid: bool
    ) -> OrderbookSnapshot:
        """
        增量更新盘口快照中的单个档位。

        size = 0 → 撤销该档位
        size > 0 → 插入或更新该档位
        """
        target = snap.bids if is_bid else snap.asks
        if level.size == 0:
            new_levels = [l for l in target if abs(l.price - level.price) > 1e-12]
        else:
            replaced = False
            new_levels = []
            for l in target:
                if abs(l.price - level.price) < 1e-12:
                    new_levels.append(level)
                    replaced = True
                else:
                    new_levels.append(l)
            if not replaced:
                new_levels.append(level)
                new_levels.sort(key=lambda l: l.price, reverse=is_bid)
        if is_bid:
            snap.bids = new_levels
        else:
            snap.asks = new_levels
        return snap

    def _notify(self, asset_id: str, snap: OrderbookSnapshot) -> None:
        for h in self._handlers:
            try:
                h(asset_id, snap)
            except Exception as exc:
                logger.warning("orderbook handler error: {}", exc)
