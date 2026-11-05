"""
window_manager.py — 窗口生命周期管理器（简化版）

核心职责：
1. 窗口发现（discover_market）
2. 窗口切换检测（condition_id 变化 → 重置 WindowContext）
3. 价格同步（MarketInfo._up_mid / _down_mid）

简化说明（2026-05-22）：
- 无市场预判检测（已移除 MarketRegimeDetector）
- 无 on_take_profit 热启动激活
"""

import asyncio
from datetime import datetime, timezone
from typing import Dict, Optional

from loguru import logger

from app.strategy.market_service import MarketService
from app.strategy.models import MarketInfo
from app.strategy.window_context import WindowContext, PriceSnapshot


class WindowManager:
    """
    统一管理所有品种的窗口生命周期。
    """

    def __init__(self, market_svc: MarketService, symbols: list[str], entry_ctrl=None, decision_logger=None):
        self._svc = market_svc
        self._symbols = symbols
        self._entry_ctrl = entry_ctrl
        self._decision_logger = decision_logger
        self._ctxs: Dict[str, WindowContext] = {
            s: WindowContext(
                asset=s,
                condition_id="",
                window_start=datetime.min.replace(tzinfo=timezone.utc),
                window_end=datetime.min.replace(tzinfo=timezone.utc),
            )
            for s in symbols
            }
    # ── 公开 API ────────────────────────────────────────────────────────────────

    def get(self, asset: str) -> Optional[WindowContext]:
        ctx = self._ctxs.get(asset)
        if ctx is None or ctx.condition_id == "":
            return None
        return ctx

    async def tick(self, assets: list[str]) -> Dict[str, WindowContext]:
        """
        主循环每 tick 调用一次：
        1. discover_market 所有品种
        2. 检测窗口切换，重置状态
        3. 获取最新价格
        返回 {asset: ctx} 字典
        """
        # ── 1. 并行发现所有品种的窗口 ──────────────────────────────────
        tasks = [self._svc.discover_market(a) for a in self._symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Track which assets got a new window in step 1 so we can log with real prices
        new_window_assets: list[str] = []

        for i, result in enumerate(results):
            asset = self._symbols[i]
            ctx = self._ctxs[asset]

            if isinstance(result, BaseException):
                logger.warning("discover_market {} failed: {}", asset, result)
                continue

            if result is None:
                logger.debug("discover_market {} returned None (no active window)", asset)
                continue

            mkt = result

            prev_id = ctx.condition_id
            is_new = (
                prev_id is None or prev_id == ""
                or mkt.condition_id != prev_id
            )

            if is_new:
                self._on_new_window(ctx, mkt)
                new_window_assets.append(asset)
                logger.info(
                    "window_mgr {} discovered: cid={} end={}",
                    asset, mkt.condition_id[:12], mkt.window_end,
                )

        # ── 2. 并行获取所有品种的价格 ─────────────────────────────────
        price_tasks = {a: self._svc.fetch_prices(self._ctxs[a].market, asset=a) for a in self._symbols}
        price_results = await asyncio.gather(*price_tasks.values(), return_exceptions=True)
        for asset, price_result in zip(price_tasks.keys(), price_results):
            if isinstance(price_result, BaseException):
                logger.warning("fetch_prices {} failed: {}", asset, price_result)
                ctx = self._ctxs[asset]
                if ctx.market:
                    ctx.market._up_mid = 0.5
                    ctx.market._down_mid = 0.5
                continue
            up_mid, down_mid = price_result
            ctx = self._ctxs[asset]
            if ctx.market:
                ctx.market._up_mid = up_mid
                ctx.market._down_mid = down_mid
            ctx.price_snapshot = PriceSnapshot(up_mid=up_mid, down_mid=down_mid)

            # 每分钟追加 UP 价格采样（供因子模型 price 因子使用）
            ctx.append_up_sample(up_mid)

            # Log new-window discovery WITH real CLOB mid prices (after fetch_prices)
            if asset in new_window_assets and ctx.condition_id and ctx.market:
                logger.info(
                    "window_mgr {} new_window: open_up={:.4f} open_down={:.4f} | "
                    "mid_up={:.4f} mid_down={:.4f} | end={}",
                    ctx.asset,
                    ctx.market.window_open_up,
                    ctx.market.window_open_down,
                    up_mid,
                    down_mid,
                    ctx.window_end,
                )

        # ── 3. 获取 BTC/ETH 美元现货价 ──────────────────────────────────
        spot_tasks = {a: self._svc.fetch_spot_price(a) for a in self._symbols}
        spot_results = await asyncio.gather(*spot_tasks.values(), return_exceptions=True)
        for asset, spot_result in zip(spot_tasks.keys(), spot_results):
            ctx = self._ctxs[asset]
            if ctx.market and not isinstance(spot_result, BaseException) and spot_result > 0:
                ctx.market.current_spot_price = spot_result

        active_assets = [a for a in assets if self.get(a) is not None]
        return {a: self._ctxs[a] for a in active_assets}

    # ── 内部方法 ───────────────────────────────────────────────────────────────

    def _on_new_window(self, ctx: WindowContext, mkt: "MarketInfo") -> None:
        prev_id = ctx.condition_id

        ctx.condition_id = mkt.condition_id
        ctx.window_start = mkt.window_start
        ctx.window_end = mkt.window_end
        ctx.market = mkt

        if prev_id and prev_id != mkt.condition_id:
            self._svc.clear_neg_risk_cache(prev_id)

        ctx.reset_for_new_window()

        if prev_id and prev_id != mkt.condition_id:
            logger.info(
                "window_mgr {} window changed: {} -> {}",
                ctx.asset, prev_id[:12], mkt.condition_id[:12],
            )
            if self._decision_logger is not None:
                self._decision_logger.cancel_tracking(prev_id)
        # NOTE: window metadata (open_up/down) is available immediately,
        # but _up_mid/_down_mid are only populated after fetch_prices in tick().
        # The combined "new_window" log is printed in tick() after fetch_prices
        # so it shows real CLOB mid prices, not the 0.0 default.
        if self._entry_ctrl is not None:
            self._entry_ctrl.reset_window_skipped(ctx.asset)
            now = datetime.now(timezone.utc)
            fresh_elapsed = (now - mkt.window_start).total_seconds()
            if fresh_elapsed < 0:
                fresh_elapsed = 0.0
            self._entry_ctrl.set_fresh_elapsed(ctx.asset, fresh_elapsed)
