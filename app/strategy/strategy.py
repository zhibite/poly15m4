"""
strategy.py — Polymarket BTC/ETH 信号驱动策略（15m Up/Down）

重构说明（2026-05-22）：
  简化为「薄调度层」，所有具体逻辑委托给独立控制器：

  - EntryController    → 信号驱动单仓入场
  - ExitController     → 移动止损止盈 / 窗口结算
  - PositionManager    → 平仓执行

  策略层的职责（仅）：
  1. run_forever() 主循环 → 编排各控制器的调用顺序
  2. _sync_config()       → 同步配置到所有子控制器
  3. _push_state()         → 聚合状态供 WebSocket / API 使用
"""

import asyncio
import httpx
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from loguru import logger

from app.config import settings
from app.models import Side
from app.pm_client import PolymarketClient
from app.runtime_config import get_config, _latest_lock
from app.strategy.decision_logger import DecisionLogger
from app.strategy.entry_controller import EntryController
from app.strategy.exit_controller import ExitController
from app.strategy.market_service import MarketService
from app.strategy.position_manager import PositionManager
from app.strategy.position_state import PositionState
from app.strategy.window_context import WindowContext
from app.strategy.window_manager import WindowManager
from app.strategy.models import MarketInfo, OpenPosition
from app.strategy.indicator_signal_service import IndicatorSignalService
from app.strategy.end_window_strategy import EndWindowStrategy


class SinglePositionStrategy:
    """
    薄调度层：只负责编排各控制器，不含具体业务逻辑。
    """

    ALL_SYMBOLS = ("BTC", "ETH")

    def __init__(self):
        self._live_trader = None
        self.pm: PolymarketClient = PolymarketClient()

        self._market_svc: Optional[MarketService] = None
        self._entry_ctrl: Optional[EntryController] = None
        self._exit_ctrl: Optional[ExitController] = None
        self._pos_mgr: Optional[PositionManager] = None
        self._logger: Optional[DecisionLogger] = None
        self._wm: Optional[WindowManager] = None
        # 类型：dict[str, IndicatorSignalService]，key 为 "BTC" / "ETH"
        self._ind_svc: dict[str, IndicatorSignalService] = {}
        self._end_strategy: Optional[EndWindowStrategy] = None

        self._running: bool = False
        self._running_lock = asyncio.Lock()
        self._prev_running: bool = False

        self._latest: dict = {}
        self._latest_indicators: dict[str, dict] = {}
        self._ind_cvd_baseline: dict[str, list[float]] = {}
        self._poll_counter = 0
        self._last_push_ts: float = 0.0

        # 运行时参数（由 _sync_config 同步到子控制器）
        self.market_interval_slug: str = "15m"
        self.market_duration_sec: int = 900
        self.entry_base_usdc: float = 4.0   # 单仓首单金额
        self.entry_early_start_elapsed_sec: float = 0.0
        self.entry_late_cutoff_elapsed_sec: float = 540.0
        self.poll_interval_sec: float = 1.0
        self.live_mode: bool = settings.live_mode
        self.live_slippage_pct: float = 2.0
        self.btc_slippage_pct: float = 1.0
        self.eth_slippage_pct: float = 2.0
        self.min_exit_price: float = 0.01
        self.default_fee_bps: int = 30
        self.sell_max_retry_count: int = 3

        # ── Binance 数据缓存（缓变数据，避免每 tick 重复请求）────────────────
        self._http_cache: dict[str, tuple[float, float]] = {}  # (value, timestamp)
        self._http_cache_ttl: float = 30.0  # seconds
        # Shared httpx client to avoid creating a new client per tick (reduces memory pressure)
        self._http_client: httpx.AsyncClient = httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, connect=5.0),
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )

        # 清算热力图滚动桶（策略自维护，不依赖 indicators.py 的 WS）
        self._liq_buckets: dict[str, list[dict]] = {}

    async def start(self) -> bool:
        async with self._running_lock:
            if self._running:
                return True
            self._running = True
            self._prev_running = True
            logger.info("Strategy STARTED")
            return True

    async def stop(self) -> bool:
        async with self._running_lock:
            was = self._running
            self._running = False
            self._prev_running = False
            if was:
                await self._http_client.aclose()
                logger.info("Strategy STOPPED")
            return was

    def is_running(self) -> bool:
        return self._running

    def set_live_trader(self, trader):
        self._live_trader = trader
        if trader is not None:
            trader.set_pm_client(self.pm)
            # Controllers were initialized with the old (possibly None) trader reference.
            # Inject the new trader so live-mode entries/exits work immediately.
            if self._entry_ctrl is not None:
                self._entry_ctrl._live_trader = trader
            if self._exit_ctrl is not None:
                self._exit_ctrl._live_trader = trader
            # end_strategy.tick() receives live_trader as a parameter from run_forever,
            # but we also store it here so _do_close can access it asynchronously.
            if self._end_strategy is not None:
                self._end_strategy._live_trader = trader

    def _symbols_enabled(self) -> list[str]:
        cfg = get_config()
        out = []
        if cfg.get("asset_btc_enabled", True):
            out.append("BTC")
        if cfg.get("asset_eth_enabled", False):
            out.append("ETH")
        return out

    def _symbols_operational(self) -> list[str]:
        need = set(self._symbols_enabled())
        for sym in self.ALL_SYMBOLS:
            ctx = self._wm.get(sym) if self._wm else None
            if ctx and ctx.has_position:
                need.add(sym)
        return sorted(need)

    def _init_services(self):
        self._market_svc = MarketService(
            self.pm,
            slug_interval=self.market_interval_slug,
            window_sec=self.market_duration_sec,
        )
        self._ind_svc: dict[str, IndicatorSignalService] = {
            a: IndicatorSignalService(asset=a) for a in ("BTC", "ETH")
        }
        self._logger = DecisionLogger(self.pm)
        self._pos_mgr = PositionManager(self.pm)
        self._exit_ctrl = ExitController(
            pos_mgr=self._pos_mgr,
            wm=self._wm,
            decision_logger=self._logger,
        )
        # 末端策略必须在 _exit_ctrl 之后创建，以便注入 pos_mgr
        self._end_strategy = EndWindowStrategy(pos_mgr=self._pos_mgr, pm=self.pm)
        self._entry_ctrl = EntryController(
            pm=self.pm,
            market_svc=self._market_svc,
            live_trader=self._live_trader,
            logger=self._logger,
            signal_svc=self._ind_svc,
        )
        self._wm = WindowManager(
            self._market_svc, self.ALL_SYMBOLS,
            entry_ctrl=self._entry_ctrl,
            decision_logger=self._logger,
            end_strategy=self._end_strategy,
        )

    def _sync_config(self):
        cfg = get_config()

        self.market_interval_slug = str(cfg.get("market_interval_slug", "15m")).strip().lower()
        self.market_duration_sec = int(float(cfg.get("market_duration_sec", 900)))
        self.entry_base_usdc = float(cfg.get("entry_base_usdc", 4.0))
        self.entry_early_start_elapsed_sec = float(cfg.get("entry_early_start_elapsed_sec", 0.0))
        self.entry_late_cutoff_elapsed_sec = float(cfg.get("entry_late_cutoff_elapsed_sec", 540.0))
        self.poll_interval_sec = float(cfg.get("poll_interval_sec", 1.0))
        lm = bool(cfg.get("live_mode", False))
        self.live_mode = lm
        self.live_slippage_pct = float(cfg.get("live_slippage_pct", 2.0))
        self.btc_slippage_pct = float(cfg.get("btc_slippage_pct", 1.0))
        self.eth_slippage_pct = float(cfg.get("eth_slippage_pct", 2.0))
        self.min_exit_price = float(cfg.get("min_exit_price", 0.01))
        self.sell_max_retry_count = int(cfg.get("sell_max_retry_count", 3))
        self.default_fee_bps = int(cfg.get("default_fee_bps", 30))

        # 信号驱动入场参数
        self.core_agree_threshold = int(float(cfg.get("core_agree_threshold", 3)))
        self.core_agree_standard = int(float(cfg.get("core_agree_standard", 3)))
        self.entry_price_min = float(cfg.get("entry_price_min", 0.20))
        self.entry_price_max = float(cfg.get("entry_price_max", 0.60))
        self.signal_fixed_stop_loss_pct = float(cfg.get("sl_base_pct", -10.0))

        if self._entry_ctrl:
            self._entry_ctrl.sync_params(cfg)
        if self._exit_ctrl:
            self._exit_ctrl.sync_params(cfg)
        if self._end_strategy:
            self._end_strategy.sync_params(cfg)

        if self._live_trader is not None:
            self._live_trader.slippage_pct = float(self.live_slippage_pct)
            self._live_trader._min_exit_price = float(self.min_exit_price)

        if self._market_svc:
            self._market_svc.update_interval(self.market_interval_slug, self.market_duration_sec)

    # ── R11 孤儿 Fill 检测 ─────────────────────────────────────────────

    async def _check_position_integrity(self):
        """
        R11 startup fix: detect orphaned fill records (trade_id=NULL) from crashed runs.
        Batch-grafts orphans to any closed Trade with the same condition_id.
        Orphan fills with no closed Trade are logged as warnings (DB-backed only).
        """
        try:
            from app.database import async_session
            from app.models import Fill, Trade
            from sqlalchemy import select, update

            async with async_session() as db:
                result = await db.execute(
                    select(Fill).where(Fill.trade_id.is_(None)).order_by(Fill.condition_id, Fill.fill_index)
                )
                orphaned_fills = result.scalars().all()

            if not orphaned_fills:
                return

            orphans_by_condition: dict[str, list] = {}
            for fill in orphaned_fills:
                orphans_by_condition.setdefault(fill.condition_id, []).append(fill)

            async with async_session() as db:
                for cid, orphans in orphans_by_condition.items():
                    fill_ids = [f.id for f in orphans]
                    trade_result = await db.execute(
                        select(Trade).where(Trade.condition_id == cid).order_by(Trade.closed_at.desc()).limit(1)
                    )
                    trade = trade_result.scalar_one_or_none()
                    if trade is not None:
                        await db.execute(
                            update(Fill).where(Fill.id.in_(fill_ids)).values(trade_id=trade.id)
                        )
                        logger.info(
                            "R11 cleanup: linked {} orphaned fill(s) to trade id={} (condition_id={})",
                            len(fill_ids), trade.id, cid[:12],
                        )
                    else:
                        fill_indices = [f.fill_index for f in orphans]
                        logger.warning(
                            "R11 WARNING: {} orphaned fill(s) for condition={} have NO closed trade "
                            "(fill_indices={}). Manual resolution required before live trading.",
                            len(orphans), cid[:12], fill_indices,
                        )
                await db.commit()
        except Exception as exc:
            logger.warning("R11 _check_position_integrity failed: {}. Continuing startup.", exc)

    # ── 主循环 ──────────────────────────────────────────────────────────

    async def run_forever(self):
        self._sync_config()
        self._init_services()
        self._sync_config()   # 首次同步：确保子控制器立即拿到运行时参数（而非硬编码默认值）
        logger.info(
            "Single-position loop: enabled={} slug={}/{}s entry_window={:.0f}s~{:.0f}s",
            self._symbols_enabled(), self.market_interval_slug, self.market_duration_sec,
            self.entry_early_start_elapsed_sec, self.entry_late_cutoff_elapsed_sec,
        )
        await self._check_position_integrity()

        self._running = True

        while True:
            try:
                if not self._running:
                    await asyncio.sleep(1.0)
                    continue

                if not getattr(self, "_prev_running", True):
                    logger.info("Strategy resumed — refreshing market prices")
                    self._prev_running = True
                    self._sync_config()
                    ctxs = await self._wm.tick(list(self.ALL_SYMBOLS))
                    await self._push_state(ctxs)
                    self._last_push_ts = datetime.now(timezone.utc).timestamp()
                    await asyncio.sleep(self.poll_interval_sec)
                    continue

                self._sync_config()

                ctxs = await self._wm.tick(list(self.ALL_SYMBOLS))

                # ── 指标预取（缓变数据：现货/ Funding / OI / OBI 带 30s 缓存）──
                now_ts = datetime.now(timezone.utc).timestamp()

                # 现货价格（BTC + ETH，在循环外取一次，两品种共用）
                spot_btc, spot_eth = await self._fetch_spot_prices_cached(now_ts)

                # 对每个品种：klines / CVD 实时取，Funding / OI / OBI 走缓存
                for asset in self.ALL_SYMBOLS:
                    try:
                        symbol = f"{asset}USDT"

                        funding, oi, obi = await self._fetch_cached_indicators(symbol, now_ts)
                        klines, cvd = await asyncio.gather(
                            self._fetch_klines(symbol),
                            self._fetch_cvd(symbol),
                        )
                        closes  = klines.get("closes", [])  if klines  else []
                        highs   = klines.get("highs", [])   if klines  else []
                        lows    = klines.get("lows", [])    if klines  else []
                        volumes = klines.get("volumes", []) if klines  else []

                        spot_price = closes[-1] if closes else 0.0
                        liq = self._build_liquidation(asset, spot_price)

                        # _ind_cvd_baseline 保留用于 _push_state 中的本地 Z-score 计算，
                        # 内部更新由 IndicatorSignalService.get_indicators() 完成（单次写入），
                        # strategy 层不再重复追加，避免滚动基准被加速消耗（每 tick 写 2 次）。
                        # 当前 cvd_val 传入供 get_indicators() 内部使用。
                        cvd_val = cvd if cvd is not None else 0.0

                        ind = await self._ind_svc[asset].get_indicators(
                            closes=closes, highs=highs, lows=lows, volumes=volumes,
                            spot_btc=spot_btc, spot_eth=spot_eth,
                            funding_val=funding or 0.0,
                            oi_val=oi or 0.0,
                            obi_val=(obi.get("value", 1.0) if obi else 1.0),
                            cvd_val=cvd_val,
                            liq_data=liq,
                            window_start=ctxs[asset].market.window_start if ctxs[asset].market else None,
                            window_duration_sec=int(self.market_duration_sec),
                        )
                        self._latest_indicators[asset] = ind
                    except Exception as exc:
                        logger.warning("Indicator calc failed for {}: {}", asset, exc)

                # 监控日志：每5秒输出所有活跃窗口的价格
                self._poll_counter += 1
                if self._poll_counter % 5 == 0:
                    for a, ctx in ctxs.items():
                        if ctx and ctx.market:
                            up = ctx.market._up_mid
                            dn = ctx.market._down_mid
                            elapsed = ctx.elapsed()
                            pos_tag = "HAS_POS" if ctx.position else "NO_POS"
                            logger.info(
                                "ENTRY MONITOR [{}] up={:.4f} down={:.4f} elapsed={:.1f}s/window{:.0f}~{:.0f}s | {}",
                                a, up, dn,
                                elapsed, self.entry_early_start_elapsed_sec, self.entry_late_cutoff_elapsed_sec, pos_tag,
                            )

                ops = self._symbols_operational()

                # 退出检查
                for asset in ops:
                    ctx = ctxs.get(asset)
                    if not ctx or not ctx.position:
                        continue
                    exit_signal = self._exit_ctrl.check_exit(ctx)
                    if exit_signal.should_exit:
                        await self._exit_ctrl.handle_exit(
                            asset, ctx, exit_signal,
                            live_trader=self._live_trader if self.live_mode else None,
                        )

                # 2. 入场检查（仅无持仓时）
                for asset in ops:
                    ctx = ctxs.get(asset)
                    if not ctx:
                        continue
                    if self._entry_ctrl.is_locked(asset):
                        continue
                    if ctx.position is not None and getattr(ctx.position, "_pending_close", False):
                        continue
                    await self._entry_ctrl.check_entry(asset, ctx)

                # 3. 末端策略 tick（T=12min~14.5min，独立入场/退出）
                if self._end_strategy:
                    cfg = get_config()
                    if cfg.get("end_enabled", False):
                        end_ops = [
                            a for a in ("BTC", "ETH")
                            if cfg.get(f"end_asset_{a.lower()}_enabled", True)
                        ]
                        for asset in end_ops:
                            ctx = ctxs.get(asset)
                            if not ctx or not ctx.market:
                                continue
                            sig_result = self._latest_indicators.get(asset)
                            main_has_pos = (
                                ctx.position is not None
                                if ctxs.get(asset) else False
                            )
                            main_dir = (
                                ctx.position.side.value
                                if ctx.position else "NONE"
                            )
                            await self._end_strategy.tick(
                                asset=asset,
                                ctx=ctx,
                                indicator_signal_result=sig_result,
                                main_has_position=main_has_pos,
                                main_direction=main_dir,
                                live_trader=self._live_trader if self.live_mode else None,
                                market_duration_sec=float(self.market_duration_sec),
                            )

                # 3. FOK 重试
                await self._entry_ctrl.fire_entry_retries(ctxs)

                # 5. 推送状态
                now_ts = datetime.now(timezone.utc).timestamp()
                if now_ts - self._last_push_ts >= 0.5:
                    await self._push_state(ctxs)
                    self._last_push_ts = now_ts

                if self._poll_counter % 30 == 0:
                    logger.debug(
                        "tick {} alive: ops={} active_windows={}",
                        self._poll_counter, ops,
                        {a: ctxs.get(a) is not None for a in ops},
                    )
                await asyncio.sleep(self.poll_interval_sec)

            except asyncio.CancelledError:
                logger.info("Strategy loop cancelled.")
                break
            except Exception as exc:
                logger.exception("Strategy loop error: {}", exc)
                await asyncio.sleep(5)

    # ── 辅助 ──────────────────────────────────────────────────────────

    async def _push_state(self, ctxs: dict[str, WindowContext]):
        outs = {}
        for a, ctx in ctxs.items():
            pos = ctx.position
            if not pos:
                continue
            um = ctx.market._up_mid if ctx.market else 0.5
            dn = ctx.market._down_mid if ctx.market else 0.5
            cur = um if pos.side == Side.UP else dn
            pnl = self._exit_ctrl.get_unrealized_pnl_pct(ctx)
            slip_pct = self.btc_slippage_pct if a == "BTC" else self.eth_slippage_pct
            cur_slip = cur * (1.0 - slip_pct / 100.0)
            outs[a] = {
                "side": pos.side.value,
                "tokens": pos.tokens_bought,
                "vwap": pos.effective_entry_price,
                "actual_cost_usdc": pos.actual_cost_usdc,
                "window_end": pos.window_end.isoformat() if pos.window_end else "",
                "unrealized_pnl_pct": pnl,
                "unrealized_pnl": round(pos.tokens_bought * cur_slip - pos.actual_cost_usdc, 4),
                "take_profit_pct": pos.tp_pct,
                "stop_loss_pct": pos.sl_pct,
                "entry_core_agree": pos.entry_core_agree,
                "mid": cur,
            }

        cfg = get_config()
        new_state = {
            "_type": "state",
            "strategy": "single_position",
            "now": datetime.now(timezone.utc).isoformat(),
            "live_mode": self.live_mode,
            "strategy_running": self._running,
            "market_interval_slug": self.market_interval_slug,
            "market_duration_sec": self.market_duration_sec,
            "asset_btc_enabled": bool(cfg.get("asset_btc_enabled", True)),
            "asset_eth_enabled": bool(cfg.get("asset_eth_enabled", False)),
            "entry": {
                "base_usdc": self.entry_base_usdc,
                "core_agree_threshold": self.core_agree_threshold,
            },
            "exit": {
                "fixed_stop_loss_pct": self.signal_fixed_stop_loss_pct,
            },
            "markets": {
                a: {
                    "condition_id": (ctx.condition_id[:12] + "...") if ctx.condition_id else "",
                    "window_start": ctx.window_start.isoformat() if ctx.window_start else "",
                    "window_end": ctx.window_end.isoformat() if ctx.window_end else "",
                    # H2 fix: guard against ctx.market being None (ETH disabled, window not discovered yet)
                    "up_mid": ctx.market._up_mid if ctx.market and hasattr(ctx.market, '_up_mid') else (
                        ctx.price_snapshot.up_mid if ctx.price_snapshot else 0.0
                    ),
                    "down_mid": ctx.market._down_mid if ctx.market and hasattr(ctx.market, '_down_mid') else (
                        ctx.price_snapshot.down_mid if ctx.price_snapshot else 0.0
                    ),
                }
                for a, ctx in self._wm._ctxs.items()
            },
            "open_positions": outs,
            # 实时指标（策略 tick 时预取计算，通过 /ws/live 和 /ws/indicators 推送）
            "indicators": dict(self._latest_indicators),
        }
        # Thread-safe write: serialize the dict under lock so ws_live readers get a consistent snapshot
        with _latest_lock:
            self._latest = new_state

    # ── Binance 数据预取辅助 ──────────────────────────────────────────────

    async def _fetch_klines(self, symbol: str, limit: int = 500) -> dict | None:
        try:
            r = await self._http_client.get(
                "https://api.binance.com/api/v3/klines",
                params={"symbol": symbol, "interval": "1m", "limit": limit},
            )
            r.raise_for_status()
            data = r.json()
            return {
                "closes":  [float(row[4]) for row in data],
                "highs":   [float(row[2]) for row in data],
                "lows":    [float(row[3]) for row in data],
                "volumes": [float(row[5]) for row in data],
            }
        except Exception:
            return None

    async def _fetch_funding(self, symbol: str) -> float | None:
        try:
            r = await self._http_client.get(
                "https://fapi.binance.com/fapi/v1/premiumIndex",
                params={"symbol": symbol},
            )
            r.raise_for_status()
            return float(r.json().get("lastFundingRate", 0))
        except Exception:
            return None

    async def _fetch_oi(self, symbol: str) -> float | None:
        try:
            r = await self._http_client.get(
                "https://fapi.binance.com/fapi/v1/openInterest",
                params={"symbol": symbol},
            )
            r.raise_for_status()
            return float(r.json().get("openInterest", 0))
        except Exception:
            return None

    async def _fetch_obi(self, symbol: str) -> dict | None:
        try:
            r = await self._http_client.get(
                "https://api.binance.com/api/v3/depth",
                params={"symbol": symbol, "limit": 10},
            )
            r.raise_for_status()
            data = r.json()
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            bv = sum(float(q) for _, q in bids)
            av = sum(float(q) for _, q in asks)
            obi = bv / av if av > 0 else 1.0
            return {"bid_vol": bv, "ask_vol": av, "value": obi}
        except Exception:
            return None

    async def _fetch_cvd(self, symbol: str) -> float | None:
        try:
            r = await self._http_client.get(
                "https://api.binance.com/api/v3/aggTrades",
                params={"symbol": symbol, "limit": 500},
            )
            r.raise_for_status()
            data = r.json()
            delta = 0.0
            for t in data:
                qty = float(t.get("q", 0))
                maker = t.get("m", True)
                delta += qty if not maker else -qty
            return delta
        except Exception:
            return None

    async def _fetch_spot_prices(self) -> tuple[float, float]:
        try:
            btc_r = await self._http_client.get(
                "https://api.binance.com/api/v3/ticker/price",
                params={"symbol": "BTCUSDT"},
            )
            eth_r = await self._http_client.get(
                "https://api.binance.com/api/v3/ticker/price",
                params={"symbol": "ETHUSDT"},
            )
            btc = float(btc_r.json().get("price", 0)) if btc_r.status_code == 200 else 0.0
            eth = float(eth_r.json().get("price", 0)) if eth_r.status_code == 200 else 0.0
            return btc, eth
        except Exception:
            return 0.0, 0.0

    # ── 现货价格缓存版本（BTC + ETH 一次请求，两品种共用）──────────────────
    async def _fetch_spot_prices_cached(self, now_ts: float) -> tuple[float, float]:
        cache_key = "spots"
        cached = self._http_cache.get(cache_key)
        if cached and (now_ts - cached[1]) < self._http_cache_ttl:
            return cached[0]  # cached[0] = (btc, eth)
        spot_btc, spot_eth = await self._fetch_spot_prices()
        self._http_cache[cache_key] = ((spot_btc, spot_eth), now_ts)
        return spot_btc, spot_eth

    # ── 带缓存的缓变数据请求（Funding / OI / OBI）─────────────────────────
    async def _fetch_cached_indicators(
        self, symbol: str, now_ts: float,
    ) -> tuple[float | None, float | None, dict | None]:
        funding = None
        oi = None
        obi = None

        fk = f"funding:{symbol}"
        fk_cached = self._http_cache.get(fk)
        if fk_cached and (now_ts - fk_cached[1]) < self._http_cache_ttl:
            funding = fk_cached[0]
        else:
            val = await self._fetch_funding(symbol)
            if val is not None:
                self._http_cache[fk] = (val, now_ts)
            funding = val

        oik = f"oi:{symbol}"
        oik_cached = self._http_cache.get(oik)
        if oik_cached and (now_ts - oik_cached[1]) < self._http_cache_ttl:
            oi = oik_cached[0]
        else:
            val = await self._fetch_oi(symbol)
            if val is not None:
                self._http_cache[oik] = (val, now_ts)
            oi = val

        obik = f"obi:{symbol}"
        obik_cached = self._http_cache.get(obik)
        # OBI TTL 更短（5s），因为它是实时订单簿数据
        if obik_cached and (now_ts - obik_cached[1]) < 5.0:
            obi = obik_cached[0]
        else:
            val = await self._fetch_obi(symbol)
            if val is not None:
                self._http_cache[obik] = (val, now_ts)
            obi = val

        return funding, oi, obi

    # ── 策略自维护的清算热力图（不依赖 WS）───────────────────────────────
    def _build_liquidation(self, asset: str, current_price: float) -> dict:
        buckets = self._liq_buckets.get(asset, [])
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

    async def _push_liquidation_tick(self, asset: str, price: float, qty: float, direction: str) -> None:
        """每次 Binance aggTrade 推送时由主循环追加清算数据（每 5s 批量更新一次）"""
        bucket = {"price": price, "qty": qty, "direction": direction, "ts": datetime.now(timezone.utc).timestamp()}
        self._liq_buckets.setdefault(asset, []).append(bucket)
        cutoff = datetime.now(timezone.utc).timestamp() - 30 * 60
        self._liq_buckets[asset] = [b for b in self._liq_buckets[asset] if b["ts"] > cutoff]

    async def close(self):
        await self.pm.close()
        if self._live_trader:
            self._live_trader.close()
