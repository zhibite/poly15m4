"""
entry_controller.py — 入场控制器（单单持仓版）

规则（per trading-guide.md）：
- 窗口开启后 T=3~7min 内入场（噪音期后、危险期前）
- 超过 6 分钟 → 整个窗口期不开单
- Core 5 指标同向才入场，< 3/5 必须跳过
- 根据信号强度确定仓位（3/5 半仓，4/5 全仓，5/5 最大仓）
- 无补仓：只入场一次，之后只做止盈止损/窗口结算
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from loguru import logger

from app.models import Side
from app.pm_client import PolymarketClient
from app.strategy.models import MarketInfo, OpenPosition
from app.strategy.decision_logger import DecisionLogger
from app.strategy.window_context import WindowContext
from app.strategy.position_state import PositionState
from app.strategy.retry_manager import RetryManager
from app.strategy.indicator_signal_service import (
    IndicatorSignalService,
    SignalDir,
    SignalResult,
)


"""
entry_controller.py — 入场控制器（单单持仓版）
"""


@dataclass
class EntryParams:
    """入场参数包"""

    base_usdc: float = 4.0           # 基准仓位（100% 时使用）
    live_mode: bool = False
    btc_slippage_pct: float = 1.0
    eth_slippage_pct: float = 2.0
    default_fee_bps: int = 30
    min_exit_price: float = 0.01
    market_duration_sec: int = 900

    # ── 入场时间窗口（窗口开启后 N 秒内允许入场）─────────────────────
    entry_early_start_elapsed_sec: float = 0.0    # 起始：0=窗口开启即允许
    entry_late_cutoff_elapsed_sec: float = 540.0   # 截止：超过此秒数后禁止开单

    # ── 信号驱动入场参数 ─────────────────────────────────────────────
    # Core 6 指标同向数量门槛（>= 此值才入场）
    core_agree_threshold: int = 3
    # 绝对价格区间（入场时 UP/DOWN 价格必须在此范围内）
    entry_price_min: float = 0.20
    entry_price_max: float = 0.60

    # ── 止盈止损阈值（per trading-guide.md）─────────────────────────
    # 止盈基础值（概率%，入场价低时更高）
    tp_base_pct: float = 10.0
    tp_range_pct: float = 20.0       # 范围：10% ~ 30% TP
    entry_price_min_tp: float = 0.20
    entry_price_max_tp: float = 0.60
    # 止损基础值（概率%）
    sl_base_pct: float = -10.0


@dataclass
class EntryResult:
    """入场检查结果"""

    triggered: bool = False
    reason: str = ""
    side: Optional[Side] = None
    direction: str = ""
    price: float = 0.0
    stake: float = 0.0
    signal_result: Optional[SignalResult] = None


# ── 控制器 ────────────────────────────────────────────────────────────────

class EntryController:
    """
    单单持仓入场控制器。

    对外 API：
      check_entry()  — 检查是否应发起入场（主循环每 tick 调用一次）
      sync_params()  — 从 runtime_config 同步参数
    """

    def __init__(
        self,
        pm: PolymarketClient,
        market_svc,
        live_trader,
        logger: DecisionLogger,
        signal_svc: dict[str, "IndicatorSignalService"] | None = None,
    ):
        self._pm = pm
        self._market_svc = market_svc
        self._live_trader = live_trader
        self._decision_logger = logger

        self._entry_locks: dict[str, asyncio.Lock] = {}
        self._opening_pos_by_asset: dict[str, bool] = {}
        self._retry_mgr = RetryManager(mode="entry")
        self._params = EntryParams()
        self._prev_live_mode: Optional[bool] = None

        # 窗口入场标记：一旦某窗口超过截止时间仍未入场，整个窗口禁止开单
        self._window_skipped: dict[str, bool] = {}
        self._window_skipped_lock = asyncio.Lock()

        # 在窗口切换时存储正确的 elapsed 值（用于 is_fresh 跳过逻辑）。
        # 原因：ctx.elapsed() 在新窗口的第一个 tick 仍使用旧的 datetime.min
        # 作为 window_start，导致返回极大的值并触发 window_closed。
        # 正确值在 _on_new_window 时通过 discover_market 的 window_start 计算。
        self._fresh_elapsed: dict[str, float] = {}

        self._signal_svc: dict[str, IndicatorSignalService] = (
            signal_svc if signal_svc is not None
            else {a: IndicatorSignalService(a) for a in ("BTC", "ETH")}
        )

    def sync_params(self, cfg: dict) -> None:
        """从传入的 cfg dict 同步所有入场参数"""
        p = self._params
        p.base_usdc = float(cfg.get("entry_base_usdc", 4.0))
        p.market_duration_sec = int(float(cfg.get("market_duration_sec", 900)))
        lm = bool(cfg.get("live_mode", False))
        self._prev_live_mode = lm
        p.live_mode = lm
        p.btc_slippage_pct = float(cfg.get("btc_slippage_pct", 1.0))
        p.eth_slippage_pct = float(cfg.get("eth_slippage_pct", 2.0))
        p.default_fee_bps = int(cfg.get("default_fee_bps", 30))
        p.min_exit_price = float(cfg.get("min_exit_price", 0.01))

        # 信号驱动参数
        p.core_agree_threshold = int(float(cfg.get("core_agree_threshold", 3)))
        p.entry_early_start_elapsed_sec = float(cfg.get("entry_early_start_elapsed_sec", 0.0))
        p.entry_late_cutoff_elapsed_sec = float(cfg.get("entry_late_cutoff_elapsed_sec", 540.0))
        p.entry_price_min = float(cfg.get("entry_price_min", 0.20))
        p.entry_price_max = float(cfg.get("entry_price_max", 0.60))

        # 止盈止损阈值
        p.tp_base_pct = float(cfg.get("tp_base_pct", 65.0))
        p.tp_range_pct = float(cfg.get("tp_range_pct", 20.0))
        p.entry_price_min_tp = float(cfg.get("entry_price_min_tp", 0.20))
        p.entry_price_max_tp = float(cfg.get("entry_price_max_tp", 0.60))
        p.sl_base_pct = float(cfg.get("sl_base_pct", -10.0))

        for sym in ("BTC", "ETH"):
            if sym not in self._entry_locks:
                self._entry_locks[sym] = asyncio.Lock()
            if sym not in self._opening_pos_by_asset:
                self._opening_pos_by_asset[sym] = False

    # ── 公开 API ───────────────────────────────────────────────────────────

    def is_locked(self, asset: str) -> bool:
        return self._opening_pos_by_asset.get(asset, False)

    def ensure_lock(self, asset: str) -> asyncio.Lock:
        if asset not in self._entry_locks:
            self._entry_locks[asset] = asyncio.Lock()
        return self._entry_locks[asset]

    def reset_window_skipped(self, asset: str) -> None:
        """窗口切换时重置跳过标记"""
        self._window_skipped[asset] = False

    def set_fresh_elapsed(self, asset: str, elapsed: float) -> None:
        """窗口切换时存储正确的 elapsed 值（供 is_fresh 逻辑使用）"""
        self._fresh_elapsed[asset] = elapsed

    # ── 计算止盈止损阈值（per trading-guide.md）──────────────────────────

    def _calc_stop_levels(self, entry_price: float, pos: OpenPosition) -> None:
        """
        根据入场价计算止盈止损阈值，写入 pos。

        止盈阈值（tp_pct）：基于入场价的线性映射
          TP = tp_base_pct + tp_range_pct × (entry_price_max_tp - entry_price) / range
          入场价越低 → scale 越大 → TP 目标越高（低价入、高目标）

        止损阈值（sl_pct）：固定值 sl_base_pct
          亏损达到 sl_base_pct 时进入信号确认检查（见 exit_controller）

        不含追踪止损逻辑：TP 触发时检查信号同向性，同向则继续持有，否则止盈退出。
        """
        p = self._params
        price_range = p.entry_price_max_tp - p.entry_price_min_tp
        if price_range > 1e-9:
            clamped = max(p.entry_price_min_tp, min(p.entry_price_max_tp, entry_price))
            scale = (p.entry_price_max_tp - clamped) / price_range
            tp_pct = p.tp_base_pct + p.tp_range_pct * scale
        else:
            tp_pct = p.tp_base_pct
        tp_pct = max(tp_pct, 0.0)

        pos.tp_pct = tp_pct
        pos.sl_pct = p.sl_base_pct
        # 锚点价：入场时等于有效入场价，用于 PnL 百分比的基准计算
        pos.anchor_price = entry_price

    # ── 入场决策 ────────────────────────────────────────────────────────

    async def check_entry(self, asset: str, ctx: WindowContext) -> EntryResult:
        """
        入场决策流程（单单持仓版）：

        1. 检查时间窗口：必须在窗口开启后 6min 内
        2. 检查 Core 5 指标共识：>= 3/5 才入场，< 3/5 必须跳过
        3. 根据信号强度确定仓位：3/5 半仓，4/5 全仓，5/5 最大仓
        4. 检查绝对价格区间：UP/DOWN 价格必须在 0.20~0.60 范围内
        5. 执行入场（live 或 dry）
        """
        mkt = ctx.market
        if not mkt:
            return EntryResult(reason="no_market")

        is_fresh = getattr(ctx, "is_fresh", False)
        if is_fresh:
            ctx.is_fresh = False

        # 在窗口切换的第一个 tick，使用 _on_new_window 时计算的正确 elapsed 值。
        # 原因：ctx.elapsed() 仍使用旧 context 的 window_start（datetime.min 或上一个窗口），
        # 会返回错误的大值（如 ~86400s），导致 entry_late_cutoff_elapsed_sec 检查失败。
        elapsed = self._fresh_elapsed.get(asset, ctx.elapsed()) if is_fresh else ctx.elapsed()

        p = self._params

        # 超过入场截止时间：整个窗口禁止开单
        if not is_fresh and elapsed >= p.entry_late_cutoff_elapsed_sec - 1e-9:
            async with self._window_skipped_lock:
                self._window_skipped[asset] = True
            logger.info(
                "check_entry [{}] elapsed={:.1f}s >= {:.0f}s — SKIP (window_closed)",
                asset, elapsed, p.entry_late_cutoff_elapsed_sec,
            )
            return EntryResult(reason="window_closed")

        # 尚未进入入场时间窗口：等待
        if elapsed < p.entry_early_start_elapsed_sec - 1e-9:
            return EntryResult(reason="before_entry_window")

        async with self._window_skipped_lock:
            is_skipped = self._window_skipped.get(asset, False)
        if is_skipped:
            return EntryResult(reason="window_skipped")

        # 有持仓则跳过
        if ctx.position is not None:
            return EntryResult(reason="position_exists")

        up_mid = mkt._up_mid
        down_mid = mkt._down_mid

        # Guard: reject invalid price data
        if up_mid <= 0 or down_mid <= 0:
            logger.warning(
                "ENTRY GUARD [{}] price_fetch_failed: up={:.4f} down={:.4f}",
                asset, up_mid, down_mid,
            )
            return EntryResult(reason="no_signal")

        # ── Step 1: 调用指标信号服务 ────────────────────────────────────
        svc = self._signal_svc.get(asset, self._signal_svc["BTC"])
        window_open_price = getattr(mkt, "window_open_up", 0.0) or 0.0
        if window_open_price <= 0:
            window_open_price = getattr(mkt, "window_open_down", 0.0) or 0.0
        if window_open_price <= 0:
            window_open_price = (
                ctx.market.window_spot_price
                if ctx.market and ctx.market.window_spot_price
                else 0.0
            )
        if window_open_price <= 0:
            window_open_price = (
                ctx.market.current_spot_price
                if ctx.market and ctx.market.current_spot_price
                else 0.0
            )

        sig: SignalResult = await svc.get_signal(
            up_mid=up_mid,
            down_mid=down_mid,
            window_open_price=window_open_price,
            window_start=mkt.window_start,
            window_duration_sec=p.market_duration_sec,
        )

        mode_label = "LIVE" if p.live_mode else "DRY"
        logger.info(
            "INDICATOR SIGNAL [{}][{}] elapsed={:.0f}s core={}/6 direction={} price={:.4f} action={}",
            mode_label, asset, elapsed, sig.core_agree,
            sig.direction_label,
            sig.up_mid if sig.direction == SignalDir.UP else sig.down_mid,
            sig.suggested_action,
        )

        # ── Step 2: Core 6 共识门槛 ───────────────────────────────────
        # 3/6 → 半仓，4/6 → 全仓，5/6 → 1.5×仓，6/6 → 2×仓，< 3/6 → 必须跳过
        if sig.suggested_action == "SKIP":
            return EntryResult(reason="no_signal", signal_result=sig)

        if sig.core_agree < p.core_agree_threshold:
            logger.info(
                "check_entry [{}] SKIP: Core 共识 {}/6 < {}/6（不足）",
                asset, sig.core_agree, p.core_agree_threshold,
            )
            return EntryResult(reason="no_signal", signal_result=sig)

        # ── Step 3: 确定方向 ────────────────────────────────────────────
        if sig.direction == SignalDir.UP:
            side = Side.UP
            direction = "UP"
            price = up_mid
        elif sig.direction == SignalDir.DOWN:
            side = Side.DOWN
            direction = "DOWN"
            price = down_mid
        else:
            return EntryResult(reason="no_signal", signal_result=sig)

        # ── Step 4: 绝对价格区间检查 ────────────────────────────────────
        if price < p.entry_price_min:
            logger.info(
                "check_entry [{}] SKIP: {} price={:.4f} < {:.2f}（价格太低）",
                asset, direction, price, p.entry_price_min,
            )
            return EntryResult(reason="no_signal", signal_result=sig)
        if price > p.entry_price_max:
            logger.info(
                "check_entry [{}] SKIP: {} price={:.4f} > {:.2f}（价格太高）",
                asset, direction, price, p.entry_price_max,
            )
            return EntryResult(reason="no_signal", signal_result=sig)

        # ── Step 5: 确定仓位（按 Core 6 共识数量，基准 4U）─────────────────
        # 6/6 → 8U（2×），5/6 → 6U（1.5×），4/6 → 4U（1×），3/6 → 2U（0.5×）
        if sig.core_agree >= 6:
            stake = p.base_usdc * 2.0   # 8U
        elif sig.core_agree >= 5:
            stake = p.base_usdc * 1.5   # 6U
        elif sig.core_agree >= 4:
            stake = p.base_usdc * 1.0   # 4U
        else:
            stake = p.base_usdc * 0.5   # 2U
        if stake < 1.0:
            logger.info(
                "check_entry [{}] SKIP: stake={:.4f} < min $1",
                asset, stake,
            )
            return EntryResult(reason="stake_too_small", signal_result=sig)

        logger.info(
            "check_entry READY [{}] {} {} @ {:.4f} stake={:.1f}u (core={}/6)",
            mode_label, asset, direction, price, stake, sig.core_agree,
        )

        # ── Step 6: 序列化信号供日志记录 ───────────────────────────────
        ctx.entry_signal_meta = {
            "core_agree": sig.core_agree,
            "strength": sig.strength,
            "strength_label": sig.strength_label,
            "direction": sig.direction.value,
            "direction_label": sig.direction_label,
            "anchor_deviation_pct": round(sig.anchor_deviation_pct, 6),
            "advanced_filter": sig.advanced.filter_result.value,
            "advanced_note": (
                sig.advanced.skip_reason
                or sig.advanced.reduce_reason
                or sig.advanced.confirm_note
                or ""
            ),
            "rsi": sig.raw_rsi,
            "funding": sig.raw_funding,
            "window_remaining_sec": round(sig.window_end_sec, 1),
            "skip_reason": sig.skip_reason,
        }

        async with self.ensure_lock(asset):
            # 双重检查持仓
            if ctx.position is not None:
                return EntryResult(reason="position_exists")

            self._opening_pos_by_asset[asset] = True
            try:
                success = await self._do_entry(
                    asset, ctx, side, direction, price, stake, elapsed,
                )
                if success:
                    return EntryResult(
                        triggered=True,
                        side=side,
                        direction=direction,
                        price=price,
                        stake=stake,
                        signal_result=sig,
                    )
                else:
                    return EntryResult(reason="order_failed", signal_result=sig)
            finally:
                self._opening_pos_by_asset[asset] = False

    # ── FOK 重试 ───────────────────────────────────────────────────────

    async def fire_entry_retries(self, assets_with_ctx: dict[str, WindowContext]) -> None:
        """每 tick 调用：处理 FOK 重试"""
        now_ts = datetime.now(timezone.utc).timestamp()
        for asset in await self._retry_mgr.snapshot_keys():
            items = await self._retry_mgr.pop_ready(asset, now_ts)
            for item in items:
                ctx = assets_with_ctx.get(asset)
                if ctx is None or ctx.position is not None:
                    await self._retry_mgr.clear(asset)
                    continue
                await self._fire_retry(asset, ctx, item)

    # ── 入场执行 ──────────────────────────────────────────────────────

    async def _do_entry(
        self,
        asset: str,
        ctx: WindowContext,
        side: Side,
        direction: str,
        price: float,
        stake: float,
        elapsed: float,
    ) -> bool:
        """实际执行入场（live 或 dry）"""
        mkt = ctx.market
        p = self._params
        slip = p.btc_slippage_pct if asset == "BTC" else p.eth_slippage_pct
        fee_rate_bps = p.default_fee_bps

        try:
            token_id = mkt.token_up_id if side == Side.UP else mkt.token_down_id
            fee_rate_bps = await self._pm.get_fee_rate_bps(token_id)
        except Exception:
            logger.warning("fee_rate_bps unavailable — using default {} bps", fee_rate_bps)

        if p.live_mode and self._live_trader:
            return await self._live_entry(
                asset, ctx, side, direction, price, stake, elapsed, slip, fee_rate_bps,
            )
        else:
            await self._dry_entry(
                asset, ctx, side, direction, price, stake, elapsed, slip, fee_rate_bps,
            )
            return True

    async def _live_entry(
        self,
        asset: str,
        ctx: WindowContext,
        side: Side,
        direction: str,
        price: float,
        stake: float,
        elapsed: float,
        slip: float,
        fee_rate_bps: int,
    ) -> bool:
        """实盘入场"""
        mkt = ctx.market
        fee_factor = 1.0 + fee_rate_bps / 10_000
        actual_stake = stake * fee_factor
        token_id = mkt.token_up_id if side == Side.UP else mkt.token_down_id

        self._live_trader.invalidate_balance_cache()
        bal_micro, alw_micro = await self._live_trader.get_balance_and_allowance("COLLATERAL")
        need_micro = int(Decimal(str(actual_stake)) * Decimal("1_000_000"))

        if actual_stake < 1.0:
            logger.warning("ENTRY skip {}: actual cost {:.4f} below CLOB minimum $1", asset, actual_stake)
            return False
        if alw_micro < need_micro:
            logger.warning("ENTRY skip {}: allowance {:.4f} < {:.4f}", asset, alw_micro / 1e6, actual_stake)
            return False
        if bal_micro < need_micro:
            logger.warning("ENTRY skip {}: balance {:.4f} < {:.4f}", asset, bal_micro / 1e6, actual_stake)
            return False

        try:
            result = await self._live_trader.buy_token(
                token_id, stake, price, fee_rate_bps,
                user_usdc_balance=bal_micro / 1_000_000,
                neg_risk=mkt.neg_risk,
                slippage_pct=slip,
                max_entry_price=price,
            )
        except Exception as exc:
            logger.warning("ENTRY {} buy_token exception: {} — retrying", asset, exc)
            if not await self._retry_mgr.schedule(asset, stake=stake, direction=direction, price=price):
                logger.error(
                    "ENTRY CRITICAL: buy_token exception and retries exhausted for {} "
                    "(direction={}, stake={}, price={:.4f}, exc={!r})",
                    asset, direction, stake, price, exc,
                )
            return False

        if result and result.success:
            await self._on_entry_success(
                asset, ctx, side, direction, elapsed,
                float(result.tokens), float(result.fill_price),
                float(result.usdc), fee_rate_bps, "live",
                estimated_price=getattr(result, "estimated_price", False),
            )
            return True
        else:
            err = result.error if result else "unknown"
            if not await self._retry_mgr.schedule(asset, stake=stake, direction=direction, price=price):
                logger.error(
                    "ENTRY CRITICAL: FOK fill failed and retries exhausted for {} "
                    "(direction={}, stake={}, price={:.4f}, error={!r})",
                    asset, direction, stake, price, err,
                )
            return False

    async def _dry_entry(
        self,
        asset: str,
        ctx: WindowContext,
        side: Side,
        direction: str,
        price: float,
        stake: float,
        elapsed: float,
        slip: float,
        fee_rate_bps: int,
    ) -> None:
        """模拟入场"""
        slip_factor = Decimal("1") + Decimal(str(slip)) / Decimal("100")
        fill_price = float(Decimal(str(price)) * slip_factor)
        fill_price = min(fill_price, 0.9999)
        tokens = float(Decimal(str(stake)) / Decimal(str(fill_price))) if fill_price > 1e-9 else 0.0
        await self._on_entry_success(
            asset, ctx, side, direction, elapsed,
            tokens, fill_price, stake, fee_rate_bps, "dry",
            estimated_price=False,
        )

    async def _fire_retry(self, asset: str, ctx: WindowContext, item) -> None:
        stake = item.stake
        direction = item.direction
        price = item.price
        mkt = ctx.market
        side = Side.UP if direction == "UP" else Side.DOWN
        elapsed = ctx.elapsed()
        slip = self._params.btc_slippage_pct if asset == "BTC" else self._params.eth_slippage_pct

        retry_price = mkt._up_mid if side == Side.UP else mkt._down_mid
        if retry_price <= 0.001 or retry_price >= 0.999:
            retry_price = price

        self._live_trader.invalidate_balance_cache()
        try:
            bal_micro = await self._live_trader.get_usdc_balance_micro()
        except Exception:
            bal_micro = 0

        result = await self._live_trader.buy_token(
            mkt.token_up_id if side == Side.UP else mkt.token_down_id,
            stake, retry_price, self._params.default_fee_bps,
            user_usdc_balance=bal_micro / 1_000_000,
            neg_risk=mkt.neg_risk,
            slippage_pct=slip,
            max_entry_price=retry_price,
        )
        if result and result.success:
            await self._on_entry_success(
                asset, ctx, side, direction, elapsed,
                float(result.tokens), float(result.fill_price),
                float(result.usdc), self._params.default_fee_bps, "live",
                estimated_price=getattr(result, "estimated_price", False),
            )
            await self._retry_mgr.clear(asset)
        else:
            err = getattr(result, "error", "unknown") if result else "unknown"
            logger.warning("ENTRY retry failed {}: {}", asset, err)
            if not await self._retry_mgr.schedule(asset, stake=stake, direction=direction, price=price):
                logger.error("ENTRY retry {}: FOK retries exhausted", asset)
                await self._retry_mgr.clear(asset)

    # ── 入场成功回调 ─────────────────────────────────────────────────

    async def _on_entry_success(
        self,
        asset: str,
        ctx: WindowContext,
        side: Side,
        direction: str,
        elapsed: float,
        tokens: float,
        fill_price: float,
        actual_cost: float,
        fee_rate_bps: int,
        mode: str,
        estimated_price: bool = False,
    ) -> None:
        """入场成功后：构建 OpenPosition，保存 Fill，设置止盈止损阈值"""
        mkt = ctx.market
        px_decision = mkt._up_mid if side == Side.UP else mkt._down_mid
        fp = float(fill_price)
        p = self._params

        pos = OpenPosition(
            asset=asset,
            condition_id=mkt.condition_id,
            side=side,
            token_id_bought=mkt.token_up_id if side == Side.UP else mkt.token_down_id,
            entry_price=fp,
            effective_entry_price=fp,
            tokens_bought=tokens,
            bet_usdc=actual_cost,
            actual_cost_usdc=actual_cost,
            taker_fee_bps=fee_rate_bps,
            min_exit_price=p.min_exit_price,
            opened_at=datetime.now(timezone.utc),
            window_start=mkt.window_start,
            window_end=mkt.window_end,
            market=mkt,
        )

        # 计算止盈止损阈值（直接写入 pos）
        # 用 effective_entry_price（含手续费影响）作为 TP 计算基准，最接近实际盈亏
        self._calc_stop_levels(fp, pos)

        sig_meta = getattr(ctx, "entry_signal_meta", None) or {}
        pos.entry_core_agree = int(sig_meta.get("core_agree", 0))
        pos.entry_anchor_deviation_pct = float(sig_meta.get("anchor_deviation_pct", 0.0))

        ctx.position = pos
        ctx.mark_traded(side)

        # 保存 Fill 记录
        fill_ok = await self._save_fill_record(ctx, pos, mkt, fp, tokens, actual_cost, fee_rate_bps, px_decision, mode)
        if not fill_ok:
            pos._is_orphan_position = True
            ctx.state = PositionState.HAS_POSITION
            logger.warning(
                "ENTRY [{}] {} {} recorded as ORPHAN (fill save failed)",
                mode.upper(), asset, direction,
            )
            return

        if self._live_trader:
            self._live_trader.invalidate_token_balance_cache(
                mkt.token_up_id if side == Side.UP else mkt.token_down_id
            )

        ctx.state = PositionState.HAS_POSITION

        if estimated_price:
            logger.warning(
                "ENTER [{}] {} {} @ {} (tokens={:.4f}, cost={:.4f}) [estimated fill price]",
                mode.upper(), asset, direction, fp, tokens, actual_cost,
            )
        else:
            logger.info(
                "ENTER [{}] {} {} @ {} (tokens={:.4f}, cost={:.4f}, TP={:.1f}%, SL={:.1f}%)",
                mode.upper(), asset, direction, fp, tokens, actual_cost,
                pos.tp_pct, pos.sl_pct,
            )

    # ── DB 记录 ────────────────────────────────────────────────────

    async def _save_fill_record(
        self,
        ctx,
        pos,
        mkt,
        fp: float,
        tokens: float,
        actual_cost: float,
        fee_rate_bps: int,
        px_decision: float,
        mode: str,
    ) -> bool:
        from app.database import async_session
        from app.models import EntryRecord, Fill

        try:
            async with async_session() as db:
                fee_usdc = actual_cost * fee_rate_bps / 10_000
                entry_record = EntryRecord(
                    asset=ctx.asset,
                    condition_id=mkt.condition_id,
                    side=pos.side,
                    entry_price=float(fp),
                    effective_entry_price=float(fp),
                    tokens_bought=float(tokens),
                    cost_usdc=float(actual_cost),
                    fee_usdc=fee_usdc,
                    mode=mode,
                    entry_core_agree=pos.entry_core_agree,
                )
                db.add(entry_record)
                await db.flush()

                fill = Fill(
                    trade_id=None,
                    asset=ctx.asset,
                    condition_id=mkt.condition_id,
                    side=pos.side,
                    fill_price=float(fp),
                    tokens=float(tokens),
                    cost_usdc=float(actual_cost),
                    fee_usdc=fee_usdc,
                    fill_index=0,
                    vwap_after=float(fp),
                    mid_price_at_fill=float(px_decision),
                    mode=mode,
                )
                db.add(fill)
                await db.flush()
                await db.commit()

                # 回填 pos 上的 entry_record_id，供后续出场时关联
                pos.entry_record_id = entry_record.id

                logger.info("SAVED entry_record id={} fill id={} for {}", entry_record.id, fill.id, ctx.asset)
                return True
        except Exception as exc:
            logger.error("Failed to save entry/fill record for {}: {}", ctx.asset, exc)
            return False
