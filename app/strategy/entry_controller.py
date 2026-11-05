"""
entry_controller.py — 入场控制器（单单持仓版）

规则：
- 入场时间窗由 entry_early_start_elapsed_sec ~ entry_late_cutoff_elapsed_sec 控制
  （默认 0~540s，超过截止时间整个窗口不再开单）
- Core 6 指标同向共识 >= core_agree_threshold（默认 3）才入场
- 根据共识数量确定仓位：3/6 半仓、4/6 全仓、5/6 1.5×、6/6 2×
- Advanced 过滤要求 REDUCE 时仓位减半；SKIP 时禁止入场
- 无补仓：同一窗口只入场一次，之后只做止盈止损/窗口结算
"""

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

from loguru import logger

from app.models import Side
from app.pm_client import PolymarketClient, estimate_buy_slippage
from app.strategy.liquidity import compute_liquidity_confidence
from app.strategy.models import MarketInfo, OpenPosition
from app.strategy.decision_logger import DecisionLogger
from app.strategy.factor_model import FactorSnapshot, compute_factor_snapshot
from app.strategy.window_context import WindowContext
from app.strategy.position_state import PositionState
from app.strategy.retry_manager import RetryManager
from app.strategy.indicator_signal_service import (
    FilterResult,
    IndicatorSignalService,
    SignalDir,
    SignalResult,
)


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
    # 与主方向相反的 Core 信号数量上限（超过则跳过）
    core_contradict_limit: int = 1
    # 绝对价格区间（入场时 UP/DOWN 价格必须在此范围内）
    entry_price_min: float = 0.20
    entry_price_max: float = 0.60

    # 是否要求信号方向与 CLOB 价格方向一致（可选的额外确认）
    require_price_alignment: bool = False
    price_alignment_min_dev: float = 0.02

    # 按 Core 共识数量配置仓位倍数（相对 base_usdc）
    stake_mult_core3: float = 0.5
    stake_mult_core4: float = 1.0
    stake_mult_core5: float = 1.5
    stake_mult_core6: float = 2.0

    # 可选：基于历史胜率的分数 Kelly 仓位
    use_kelly_sizing: bool = False
    kelly_fraction: float = 0.25
    kelly_prior_weight: float = 20.0
    kelly_min_samples: int = 20
    kelly_max_mult: float = 2.0
    bankroll_usdc: float = 100.0
    hold_to_settlement: bool = False

    # ── 因子模型 ──────────────────────────────────────────────────────
    # 0 = off, 1 = shadow（只记录）, 2 = live（用因子做决策）
    factor_mode: int = 0
    factor_w_trend: float = 0.20
    factor_w_reversion: float = 0.15
    factor_w_price: float = 0.35
    factor_w_micro: float = 0.15
    factor_w_flow: float = 0.15
    factor_min_net: float = 0.25
    factor_min_agreement: float = 0.60
    factor_rev_veto_score: float = 0.70
    factor_max_spread_bps: float = 100.0
    factor_oi_change_threshold: float = 0.02

    # ── 流动性过滤 ───────────────────────────────────────────────────
    min_depth_coverage: float = 2.0    # 盘口深度至少是投入的多少倍（默认 2x）
    max_slippage_pct: float = 3.0      # 最大容忍滑点（%，超出则跳过）
    obi_contradict_skip: float = 0.25  # OBI 与方向矛盾时跳过（默认 0.25：做 UP 但 OBI<-0.25 则跳过）

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
        risk_manager=None,
    ):
        self._pm = pm
        self._market_svc = market_svc
        self._live_trader = live_trader
        self._decision_logger = logger
        self._risk_manager = risk_manager

        self._entry_locks: dict[str, asyncio.Lock] = {}
        self._opening_pos_by_asset: dict[str, bool] = {}
        self._retry_mgr = RetryManager(mode="entry")
        self._params = EntryParams()

        # 决策快照：每个 asset 每个 bar 只写一次；记录最近一条的 id 供入场回填
        self._last_snapshot_bar: dict[str, int] = {}
        self._last_snapshot_id: dict[str, int] = {}

        # 历史胜率缓存：(core_agree, mode) -> (wins, total, ts)
        self._winrate_cache: dict[tuple[int, str], tuple[int, int, float]] = {}

        # 因子快照缓存：asset -> (cache_key, snapshot, up_orderbook)
        self._factor_cache: dict[str, tuple[tuple, FactorSnapshot, dict | None]] = {}

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
        p.live_mode = lm
        p.btc_slippage_pct = float(cfg.get("btc_slippage_pct", 1.0))
        p.eth_slippage_pct = float(cfg.get("eth_slippage_pct", 2.0))
        p.default_fee_bps = int(cfg.get("default_fee_bps", 30))
        p.min_exit_price = float(cfg.get("min_exit_price", 0.01))

        # 信号驱动参数
        p.core_agree_threshold = int(float(cfg.get("core_agree_threshold", 3)))
        p.core_contradict_limit = int(float(cfg.get("core_contradict_limit", 1)))
        p.entry_early_start_elapsed_sec = float(cfg.get("entry_early_start_elapsed_sec", 0.0))
        p.entry_late_cutoff_elapsed_sec = float(cfg.get("entry_late_cutoff_elapsed_sec", 540.0))
        p.entry_price_min = float(cfg.get("entry_price_min", 0.20))
        p.entry_price_max = float(cfg.get("entry_price_max", 0.60))

        p.require_price_alignment = bool(cfg.get("entry_require_price_alignment", False))
        p.price_alignment_min_dev = float(cfg.get("entry_price_alignment_min_dev", 0.02))

        p.stake_mult_core3 = float(cfg.get("stake_mult_core3", 0.5))
        p.stake_mult_core4 = float(cfg.get("stake_mult_core4", 1.0))
        p.stake_mult_core5 = float(cfg.get("stake_mult_core5", 1.5))
        p.stake_mult_core6 = float(cfg.get("stake_mult_core6", 2.0))

        p.use_kelly_sizing = bool(cfg.get("use_kelly_sizing", False))
        p.kelly_fraction = float(cfg.get("kelly_fraction", 0.25))
        p.kelly_prior_weight = float(cfg.get("kelly_prior_weight", 20.0))
        p.kelly_min_samples = int(float(cfg.get("kelly_min_samples", 20)))
        p.kelly_max_mult = float(cfg.get("kelly_max_mult", 2.0))
        p.bankroll_usdc = float(cfg.get("bankroll_usdc", 100.0))
        p.hold_to_settlement = bool(cfg.get("exit_hold_to_settlement", False))

        # 因子模型
        p.factor_mode = int(float(cfg.get("factor_mode", 0)))
        p.factor_w_trend = float(cfg.get("factor_w_trend", 0.20))
        p.factor_w_reversion = float(cfg.get("factor_w_reversion", 0.15))
        p.factor_w_price = float(cfg.get("factor_w_price", 0.35))
        p.factor_w_micro = float(cfg.get("factor_w_micro", 0.15))
        p.factor_w_flow = float(cfg.get("factor_w_flow", 0.15))
        p.factor_min_net = float(cfg.get("factor_min_net", 0.25))
        p.factor_min_agreement = float(cfg.get("factor_min_agreement", 0.60))
        p.factor_rev_veto_score = float(cfg.get("factor_rev_veto_score", 0.70))
        p.factor_max_spread_bps = float(cfg.get("factor_max_spread_bps", 100.0))
        p.factor_oi_change_threshold = float(cfg.get("factor_oi_change_threshold", 0.02))

        # 流动性过滤参数
        p.min_depth_coverage = float(cfg.get("min_depth_coverage", 2.0))
        p.max_slippage_pct = float(cfg.get("max_slippage_pct", 3.0))
        p.obi_contradict_skip = float(cfg.get("obi_contradict_skip", 0.25))

        # 止盈止损阈值
        p.tp_base_pct = float(cfg.get("tp_base_pct", 10.0))
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

    def _tp_pct_for_price(self, entry_price: float) -> float:
        """根据入场价计算止盈阈值（%）。"""
        p = self._params
        price_range = p.entry_price_max_tp - p.entry_price_min_tp
        if price_range > 1e-9:
            clamped = max(p.entry_price_min_tp, min(p.entry_price_max_tp, entry_price))
            scale = (p.entry_price_max_tp - clamped) / price_range
            tp_pct = p.tp_base_pct + p.tp_range_pct * scale
        else:
            tp_pct = p.tp_base_pct
        return max(tp_pct, 0.0)

    def _calc_stop_levels(self, entry_price: float, pos: OpenPosition) -> None:
        """
        根据入场价计算止盈止损阈值，写入 pos。

        止盈阈值（tp_pct）：基于入场价的线性映射
          TP = tp_base_pct + tp_range_pct × (entry_price_max_tp - entry_price) / range
          入场价越低 → scale 越大 → TP 目标越高（低价入、高目标）

        止损阈值（sl_pct）：固定值 sl_base_pct
          亏损达到 sl_base_pct 时进入信号确认检查（见 exit_controller）
        """
        p = self._params
        pos.tp_pct = self._tp_pct_for_price(entry_price)
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

        # 单单持仓：同一窗口只允许开仓一次（平仓后也不允许再次入场）
        if ctx.is_traded:
            return EntryResult(reason="already_traded")

        # 账户级风控：每日亏损上限 / 连续亏损冷却
        if self._risk_manager is not None:
            allowed, risk_reason = self._risk_manager.can_enter()
            if not allowed:
                return EntryResult(reason=f"risk:{risk_reason}")

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
        # 锚点价必须是窗口开启时的现货价（美元）。此前误用了 window_open_up
        # （Polymarket 概率价 0~1），导致 anchor_deviation_pct 变成无意义的大数。
        window_open_price = getattr(mkt, "window_spot_price", 0.0) or 0.0
        if window_open_price <= 0:
            window_open_price = getattr(mkt, "current_spot_price", 0.0) or 0.0

        sig: SignalResult = await svc.get_signal(
            up_mid=up_mid,
            down_mid=down_mid,
            window_open_price=window_open_price,
            window_start=mkt.window_start,
            window_duration_sec=p.market_duration_sec,
        )

        mode_label = "LIVE" if p.live_mode else "DRY"

        factor_snap: FactorSnapshot | None = None
        factor_up_book: dict | None = None
        if p.factor_mode > 0:
            factor_snap, factor_up_book = await self._get_factor_snapshot(
                asset, ctx, sig,
            )

        await self._record_decision_snapshot(
            asset, ctx, sig, elapsed, up_mid, down_mid, factors=factor_snap,
        )

        # factor_mode=2：用因子分数替代 Core 共识做决策
        if p.factor_mode == 2:
            return await self._check_factor_entry(
                asset, ctx, sig, factor_snap, factor_up_book, elapsed,
            )

        logger.info(
            "INDICATOR SIGNAL [{}][{}] elapsed={:.0f}s core={}/6 contradict={} direction={} price={:.4f} action={}",
            mode_label, asset, elapsed, sig.core_agree, sig.core_contradict,
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
                "check_entry [{}] SKIP: Core 同向共识 {}/6 < {}/6（不足）",
                asset, sig.core_agree, p.core_agree_threshold,
            )
            return EntryResult(reason="no_signal", signal_result=sig)

        if sig.core_contradict > p.core_contradict_limit:
            logger.info(
                "check_entry [{}] SKIP: Core 反向信号 {}/6 > 上限 {}（方向分歧过大）",
                asset, sig.core_contradict, p.core_contradict_limit,
            )
            return EntryResult(reason="core_contradiction", signal_result=sig)

        # ── Step 3: 确定方向和边 ────────────────────────────────────────────
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

        # ── Step 4a: 可选的 CLOB 价格方向确认 ──────────────────────────
        # 只有当市场本身也偏向同一方向时才入场，过滤“指标看多但市场看空”的逆势单。
        if p.require_price_alignment:
            up_implied = mkt._up_mid
            aligned = (
                (direction == "UP" and up_implied >= 0.5 + p.price_alignment_min_dev)
                or (direction == "DOWN" and up_implied <= 0.5 - p.price_alignment_min_dev)
            )
            if not aligned:
                logger.info(
                    "check_entry [{}] SKIP: {} 信号与 CLOB 方向不一致 (up_mid={:.4f}, min_dev={:.3f})",
                    asset, direction, up_implied, p.price_alignment_min_dev,
                )
                return EntryResult(reason="price_alignment", signal_result=sig)

        # ── Step 4b: OBI 方向过滤 ──────────────────────────────────────
        book = await self._pm.get_orderbook(
            mkt.token_up_id if side == Side.UP else mkt.token_down_id
        )
        if book:
            _D5, obi, _depth_factor, _depth_ok = compute_liquidity_confidence(
                book, direction,
            )
            # book 是所买 token（UP 或 DOWN）的盘口：
            # 卖方主导（OBI < -threshold）与持仓方向矛盾，跳过；买方主导是支持信号。
            if obi < -abs(p.obi_contradict_skip):
                logger.info(
                    "check_entry [{}] SKIP: {} OBI={:+.2f}（卖方主导，与 {} 方向矛盾）",
                    asset, direction, obi, direction,
                )
                return EntryResult(reason="obi_contradict", signal_result=sig)

        # ── Step 5: 确定仓位（按 Core 6 共识数量，基准 4U）─────────────────
        # 6/6 → 8U（2×），5/6 → 6U（1.5×），4/6 → 4U（1×），3/6 → 2U（0.5×）
        if sig.core_agree >= 6:
            stake = p.base_usdc * p.stake_mult_core6
        elif sig.core_agree >= 5:
            stake = p.base_usdc * p.stake_mult_core5
        elif sig.core_agree >= 4:
            stake = p.base_usdc * p.stake_mult_core4
        else:
            stake = p.base_usdc * p.stake_mult_core3

        # 可选：用历史胜率 + Kelly 覆盖固定倍数仓位
        if p.use_kelly_sizing:
            kelly_stake = await self._kelly_stake(sig.core_agree, price)
            if kelly_stake is not None:
                if kelly_stake <= 0:
                    logger.info(
                        "check_entry [{}] SKIP: Kelly 无正 edge (core={}/6, price={:.4f})",
                        asset, sig.core_agree, price,
                    )
                    return EntryResult(reason="kelly_no_edge", signal_result=sig)
                stake = kelly_stake
                logger.info(
                    "check_entry [{}] KELLY stake={:.2f}u (core={}/6, price={:.4f})",
                    asset, stake, sig.core_agree, price,
                )

        # Advanced 过滤要求减仓（Funding 极端反向 / OBI 反向）→ 仓位减半
        if sig.advanced.filter_result == FilterResult.REDUCE:
            stake *= 0.5
            logger.info(
                "check_entry [{}] REDUCE: {} — stake halved to {:.1f}u",
                asset,
                sig.advanced.reduce_reason or "advanced filter",
                stake,
            )

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
            "core_contradict": sig.core_contradict,
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
                    orderbook=book,
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

    # ── 基于历史胜率的分数 Kelly 仓位（可选）───────────────────────────

    async def _historical_win_stats(self, core_agree: int) -> tuple[int, int]:
        """返回该 Core 共识桶的历史 (wins, total)；5 分钟缓存。"""
        now_ts = datetime.now(timezone.utc).timestamp()
        mode = "live" if self._params.live_mode else "dry"
        cache_key = (int(core_agree), mode)
        cached = self._winrate_cache.get(cache_key)
        if cached and now_ts - cached[2] < 300:
            return cached[0], cached[1]
        try:
            from sqlalchemy import case, func, select

            from app.database import async_session
            from app.models import Trade

            cutoff = datetime.now(timezone.utc) - timedelta(days=180)
            async with async_session() as db:
                row = await db.execute(
                    select(
                        func.count(Trade.id),
                        func.coalesce(
                            func.sum(case((Trade.pnl_usd > 0, 1), else_=0)), 0,
                        ),
                    ).where(
                        Trade.strategy == "main",
                        Trade.mode == ("live" if self._params.live_mode else "dry"),
                        Trade.entry_core_agree == int(core_agree),
                        Trade.closed_at >= cutoff,
                    )
                )
                total, wins = row.one()
            wins_i = int(wins or 0)
            total_i = int(total or 0)
            self._winrate_cache[cache_key] = (wins_i, total_i, now_ts)
            return wins_i, total_i
        except Exception as exc:
            logger.debug("historical win stats failed: {}", exc)
            return 0, 0

    async def _kelly_stake(self, core_agree: int, price: float) -> Optional[float]:
        """
        用历史胜率 + 市场隐含概率先验计算分数 Kelly 仓位。

        返回 None  = 样本不足，调用方回退到 Core 倍数仓位
        返回 0.0   = 历史数据没有正 edge，调用方跳过
        返回 >0    = 建议仓位（USDC）
        """
        p = self._params
        if not p.use_kelly_sizing or price <= 0 or price >= 1:
            return None

        wins, total = await self._historical_win_stats(core_agree)
        if total < p.kelly_min_samples:
            return None

        # 以市场隐含概率为先验做收缩，避免小样本极端胜率
        prior = max(0.01, min(0.99, float(price)))
        p_est = (wins + p.kelly_prior_weight * prior) / (
            total + p.kelly_prior_weight
        )

        # 赔率 b：win 时收益 / loss 时亏损
        if p.hold_to_settlement:
            b = (1.0 - price) / price
        else:
            tp = self._tp_pct_for_price(price)
            loss_pct = abs(p.sl_base_pct)
            b = tp / loss_pct if loss_pct > 0 else 0.0
        if b <= 0:
            return None

        kelly = (p_est * (b + 1.0) - 1.0) / b
        if kelly <= 0:
            return 0.0

        stake = p.bankroll_usdc * p.kelly_fraction * kelly
        max_stake = p.base_usdc * p.kelly_max_mult
        return max(0.0, min(stake, max_stake))

    # ── 因子模型 ──────────────────────────────────────────────────────

    def latest_factor_snapshot(self, asset: str) -> dict | None:
        cached = self._factor_cache.get(asset)
        return cached[1].to_dict() if cached else None

    async def _get_factor_snapshot(
        self,
        asset: str,
        ctx: WindowContext,
        sig: SignalResult,
    ) -> tuple[FactorSnapshot | None, dict | None]:
        """按 bar 缓存因子快照；返回 (snapshot, UP token orderbook)。"""
        p = self._params
        if p.factor_mode <= 0:
            return None, None

        mkt = ctx.market
        if mkt is None:
            return None, None

        bar_ts = int(
            sig.bar_ts
            or (int(datetime.now(timezone.utc).timestamp()) // 60 * 60)
        )
        cache_key = (bar_ts, ctx.condition_id)
        cached = self._factor_cache.get(asset)
        if cached and cached[0] == cache_key:
            return cached[1], cached[2]

        # 因子模型统一使用 UP token 盘口：OBI 正数代表看 UP
        up_book = await self._pm.get_orderbook(mkt.token_up_id)
        weights = {
            "trend": p.factor_w_trend,
            "reversion": p.factor_w_reversion,
            "price": p.factor_w_price,
            "micro": p.factor_w_micro,
            "flow": p.factor_w_flow,
        }
        snapshot = compute_factor_snapshot(
            sig.raw_metrics or {},
            up_samples=list(getattr(ctx, "up_samples", []) or []),
            window_open_up=float(getattr(mkt, "window_open_up", 0.5) or 0.5),
            current_up=float(getattr(mkt, "_up_mid", 0.5) or 0.5),
            up_orderbook=up_book,
            weights=weights,
            max_spread_bps=p.factor_max_spread_bps,
            oi_change_threshold=p.factor_oi_change_threshold,
        )
        self._factor_cache[asset] = (cache_key, snapshot, up_book)
        return snapshot, up_book

    async def _check_factor_entry(
        self,
        asset: str,
        ctx: WindowContext,
        sig: SignalResult,
        factor_snap: FactorSnapshot | None,
        up_book: dict | None,
        elapsed: float,
    ) -> EntryResult:
        """factor_mode=2 时用因子分数替代 Core 共识做入场决策。"""
        mkt = ctx.market
        p = self._params
        if mkt is None or factor_snap is None:
            return EntryResult(reason="factor_unavailable", signal_result=sig)

        # 保留现有 Advanced 的硬否决（CVD 反向背离），因子模式不覆盖该安全条件
        if sig.advanced.filter_result == FilterResult.SKIP:
            logger.info(
                "FACTOR SKIP [{}] advanced_hard_skip: {}",
                asset, sig.advanced.skip_reason or "advanced filter",
            )
            return EntryResult(
                reason="advanced_hard_skip", signal_result=sig,
            )

        ok, reason = factor_snap.entry_ok(
            min_net=p.factor_min_net,
            min_agreement=p.factor_min_agreement,
            rev_veto_score=p.factor_rev_veto_score,
        )
        if not ok:
            logger.info(
                "FACTOR SKIP [{}] {} net={:+.2f} agree={:.2f} reason={}",
                asset, factor_snap.direction, factor_snap.net_score,
                factor_snap.agreement, reason,
            )
            return EntryResult(reason=reason, signal_result=sig)

        direction = factor_snap.direction
        side = Side.UP if direction == "UP" else Side.DOWN
        price = mkt._up_mid if side == Side.UP else mkt._down_mid

        if price < p.entry_price_min or price > p.entry_price_max:
            logger.info(
                "FACTOR SKIP [{}] {} price={:.4f} outside [{:.2f}, {:.2f}]",
                asset, direction, price, p.entry_price_min, p.entry_price_max,
            )
            return EntryResult(reason="price_range", signal_result=sig)

        if p.require_price_alignment:
            up_implied = mkt._up_mid
            aligned = (
                (direction == "UP" and up_implied >= 0.5 + p.price_alignment_min_dev)
                or (direction == "DOWN" and up_implied <= 0.5 - p.price_alignment_min_dev)
            )
            if not aligned:
                logger.info(
                    "FACTOR SKIP [{}] {} 与 CLOB 方向不一致 (up_mid={:.4f})",
                    asset, direction, up_implied,
                )
                return EntryResult(reason="price_alignment", signal_result=sig)

        # 所买 token 的盘口（因子模型里的 up_book 可复用）
        if side == Side.UP:
            book = up_book or await self._pm.get_orderbook(mkt.token_up_id)
        else:
            book = await self._pm.get_orderbook(mkt.token_down_id)

        if book:
            _D5, obi, _depth_factor, _depth_ok = compute_liquidity_confidence(
                book, direction,
            )
            if obi < -abs(p.obi_contradict_skip):
                logger.info(
                    "FACTOR SKIP [{}] {} OBI={:+.2f}（卖方主导，与方向矛盾）",
                    asset, direction, obi,
                )
                return EntryResult(reason="obi_contradict", signal_result=sig)

        # 因子强度 → 仓位倍数
        strength = abs(factor_snap.net_score) * factor_snap.agreement
        mult = max(0.5, min(2.0, 0.5 + 1.5 * strength))
        stake = p.base_usdc * mult

        # Advanced REDUCE（Funding 极端等）仍作为保守减仓条件
        if sig.advanced.filter_result == FilterResult.REDUCE:
            stake *= 0.5
            logger.info(
                "FACTOR REDUCE [{}] {} — stake halved to {:.2f}u",
                asset, sig.advanced.reduce_reason or "advanced filter", stake,
            )

        if stake < 1.0:
            logger.info(
                "FACTOR SKIP [{}] stake={:.4f} < min $1", asset, stake,
            )
            return EntryResult(reason="stake_too_small", signal_result=sig)

        logger.info(
            "FACTOR READY [{}][{}] {} @ {:.4f} net={:+.2f} agree={:.2f} "
            "stake={:.1f}u trend={:+.2f} rev={:+.2f} price={:+.2f} micro={:+.2f} flow={:+.2f}",
            "LIVE" if p.live_mode else "DRY", asset, direction, price,
            factor_snap.net_score, factor_snap.agreement, stake,
            factor_snap.trend.score, factor_snap.reversion.score,
            factor_snap.price.score, factor_snap.micro.score,
            factor_snap.flow.score,
        )

        ctx.entry_signal_meta = {
            "core_agree": sig.core_agree,
            "core_contradict": sig.core_contradict,
            "strength": sig.strength,
            "strength_label": sig.strength_label,
            "direction": direction,
            "direction_label": (
                "做 UP（看涨）" if direction == "UP" else "做 DOWN（看跌）"
            ),
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
            "skip_reason": "",
            "factor_mode": "live",
            "factor_net": round(factor_snap.net_score, 4),
            "factor_agreement": round(factor_snap.agreement, 4),
            "factor_conflict": round(factor_snap.conflict, 4),
            "factor_direction": direction,
            "factors": factor_snap.to_dict(),
        }

        async with self.ensure_lock(asset):
            if ctx.position is not None:
                return EntryResult(reason="position_exists")
            self._opening_pos_by_asset[asset] = True
            try:
                success = await self._do_entry(
                    asset, ctx, side, direction, price, stake, elapsed,
                    orderbook=book,
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
                return EntryResult(reason="order_failed", signal_result=sig)
            finally:
                self._opening_pos_by_asset[asset] = False

    # ── 决策快照 ───────────────────────────────────────────────────────

    @staticmethod
    def _signal_to_dict(s) -> dict:
        return {
            "name": s.name,
            "direction": s.direction.value,
            "agree": bool(s.agree),
            "reason": s.reason,
            "raw": s.raw_value,
        }

    async def _record_decision_snapshot(
        self,
        asset: str,
        ctx: WindowContext,
        sig: SignalResult,
        elapsed: float,
        up_mid: float,
        down_mid: float,
        *,
        factors: FactorSnapshot | None = None,
    ) -> None:
        """每个 asset 每根 bar 写一条完整决策快照，供后续信号有效性分析。"""
        try:
            bar_key = int(
                sig.bar_ts
                or (int(datetime.now(timezone.utc).timestamp()) // 60 * 60)
            )
            if self._last_snapshot_bar.get(asset) == bar_key:
                return

            from app.database import async_session
            from app.models import DecisionSnapshot

            direction = sig.direction.value
            decision_price = (
                up_mid if direction == "UP"
                else down_mid if direction == "DOWN"
                else 0.0
            )
            core = {
                "rsi": self._signal_to_dict(sig.core.rsi),
                "macd": self._signal_to_dict(sig.core.macd),
                "vwap": self._signal_to_dict(sig.core.vwap),
                "ema": self._signal_to_dict(sig.core.ema),
                "pivot": self._signal_to_dict(sig.core.pivot),
                "bbands": self._signal_to_dict(sig.core.bbands),
            }
            advanced = {
                "funding": self._signal_to_dict(sig.advanced.funding),
                "cvd": self._signal_to_dict(sig.advanced.cvd),
                "liquidation": self._signal_to_dict(sig.advanced.liquidation),
                "obi": self._signal_to_dict(sig.advanced.obi),
                "oi": self._signal_to_dict(sig.advanced.oi),
                "filter_result": sig.advanced.filter_result.value,
                "skip_reason": sig.advanced.skip_reason,
                "reduce_reason": sig.advanced.reduce_reason,
                "confirm_note": sig.advanced.confirm_note,
            }

            snapshot = DecisionSnapshot(
                asset=asset,
                condition_id=ctx.condition_id,
                bar_ts=bar_key,
                window_start=ctx.window_start,
                elapsed_sec=float(elapsed),
                mode="live" if self._params.live_mode else "dry",
                up_mid=float(up_mid),
                down_mid=float(down_mid),
                decision_price=float(decision_price),
                direction=direction,
                core_agree=int(sig.core_agree),
                core_contradict=int(sig.core_contradict),
                strength=int(sig.strength),
                advanced_filter=sig.advanced.filter_result.value,
                suggested_action=sig.suggested_action,
                skip_reason=sig.skip_reason or "",
                core_json=json.dumps(core, ensure_ascii=False),
                advanced_json=json.dumps(advanced, ensure_ascii=False),
                factor_mode=int(self._params.factor_mode),
                factor_net=float(factors.net_score) if factors else 0.0,
                factor_agreement=float(factors.agreement) if factors else 0.0,
                factor_conflict=float(factors.conflict) if factors else 0.0,
                factor_direction=factors.direction if factors else "NONE",
                factors_json=(
                    json.dumps(factors.to_dict(), ensure_ascii=False)
                    if factors else ""
                ),
                entered=0,
            )
            async with async_session() as db:
                db.add(snapshot)
                await db.commit()
                await db.refresh(snapshot)
                self._last_snapshot_id[asset] = snapshot.id
                self._last_snapshot_bar[asset] = bar_key
        except Exception as exc:
            # 分析数据不能影响交易主流程
            logger.debug("decision snapshot failed for {}: {}", asset, exc)

    async def _mark_snapshot_entered(
        self, asset: str, entry_record_id: int | None,
    ) -> None:
        snapshot_id = self._last_snapshot_id.get(asset)
        if not snapshot_id:
            return
        try:
            from sqlalchemy import update

            from app.database import async_session
            from app.models import DecisionSnapshot

            async with async_session() as db:
                await db.execute(
                    update(DecisionSnapshot)
                    .where(DecisionSnapshot.id == snapshot_id)
                    .values(entered=1, entry_record_id=entry_record_id)
                )
                await db.commit()
        except Exception as exc:
            logger.debug("decision snapshot update failed for {}: {}", asset, exc)

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
        *,
        orderbook: dict | None = None,
    ) -> bool:
        """实际执行入场（live 或 dry）"""
        mkt = ctx.market
        if mkt is None:
            logger.error("ENTRY [{}] skipped: no market info", asset)
            return False
        p = self._params
        slip = p.btc_slippage_pct if asset == "BTC" else p.eth_slippage_pct
        fee_rate_bps = p.default_fee_bps
        token_id = mkt.token_up_id if side == Side.UP else mkt.token_down_id

        try:
            fee_rate_bps = await self._pm.get_fee_rate_bps(token_id)
        except Exception:
            logger.warning("fee_rate_bps unavailable — using default {} bps", fee_rate_bps)

        # 实盘模式必须真的有 LiveTrader；否则绝不能静默降级为 dry 模拟，
        # 否则 UI 显示 live_mode=True 但实际只记录了模拟成交。
        if p.live_mode and self._live_trader is None:
            logger.error(
                "ENTRY [{}] live_mode=True but LiveTrader is unavailable — skipping entry "
                "(check wallet/API credentials)",
                asset,
            )
            return False

        if p.live_mode:
            # 用盘口估算动态滑点，超限则跳过
            if orderbook:
                slip_est = await estimate_buy_slippage(
                    token_id, stake, pm_client=self._pm,
                    min_coverage=p.min_depth_coverage,
                )
                if not slip_est.error:
                    slip = max(slip_est.slippage_pct, 0.5)
                    if not slip_est.sufficient:
                        logger.info(
                            "ENTRY skip [{}] {} depth coverage={:.1f}x < {:.1f}x "
                            "(available={:.1f} tokens, stake={:.1f}U)",
                            asset, direction, slip_est.pct_of_depth,
                            p.min_depth_coverage, slip_est.depth_at_fill, stake,
                        )
                        return False
                    if slip > p.max_slippage_pct:
                        logger.info(
                            "ENTRY skip [{}] {} slip_est={:.2f}% > max={:.1f}%",
                            asset, direction, slip, p.max_slippage_pct,
                        )
                        return False
                    logger.debug(
                        "ENTRY [{}] {} slip_est={:.2f}% depth={:.1f}x sufficient={}",
                        asset, direction, slip, slip_est.pct_of_depth, slip_est.sufficient,
                    )
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
                fee_usdc=float(getattr(result, "fee_usdc", 0.0) or 0.0),
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
        entry_fee = stake * fee_rate_bps / 10_000 if fee_rate_bps > 0 else 0.0
        await self._on_entry_success(
            asset, ctx, side, direction, elapsed,
            tokens, fill_price, stake, fee_rate_bps, "dry",
            estimated_price=False,
            fee_usdc=entry_fee,
        )

    async def _fire_retry(self, asset: str, ctx: WindowContext, item) -> None:
        if self._live_trader is None:
            logger.warning("ENTRY retry [{}] skipped: LiveTrader unavailable", asset)
            await self._retry_mgr.clear(asset)
            return
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
                fee_usdc=float(getattr(result, "fee_usdc", 0.0) or 0.0),
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
        fee_usdc: float = 0.0,
    ) -> None:
        """入场成功后：构建 OpenPosition，保存 Fill，设置止盈止损阈值"""
        mkt = ctx.market
        px_decision = mkt._up_mid if side == Side.UP else mkt._down_mid
        fp = float(fill_price)
        p = self._params

        # Keep the position and DB fee consistent.  Live fills normally carry an
        # explicit fee; if the exchange omitted it, estimate from the fee rate.
        fee_usdc = float(fee_usdc or 0.0)
        if fee_usdc <= 0 and fee_rate_bps > 0:
            fee_usdc = float(actual_cost) * float(fee_rate_bps) / 10_000.0

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
            execution_mode=mode,
            entry_fee_usdc=fee_usdc,
            market=mkt,
        )

        # 计算止盈止损阈值（直接写入 pos）
        # 用 effective_entry_price（含手续费影响）作为 TP 计算基准，最接近实际盈亏
        self._calc_stop_levels(fp, pos)
        pos.peak_price = fp
        pos.peak_pnl_pct = 0.0

        sig_meta = getattr(ctx, "entry_signal_meta", None) or {}
        pos.entry_core_agree = int(sig_meta.get("core_agree", 0))
        pos.entry_anchor_deviation_pct = float(sig_meta.get("anchor_deviation_pct", 0.0))

        ctx.position = pos
        ctx.mark_traded(side)

        # 保存 Fill 记录
        fill_ok = await self._save_fill_record(
            ctx, pos, mkt, fp, tokens, actual_cost, fee_rate_bps,
            px_decision, mode, fee_usdc=fee_usdc,
        )
        if not fill_ok:
            pos._is_orphan_position = True
            ctx.state = PositionState.HAS_POSITION
            logger.warning(
                "ENTRY [{}] {} {} recorded as ORPHAN (fill save failed)",
                mode.upper(), asset, direction,
            )
            return

        await self._mark_snapshot_entered(asset, pos.entry_record_id)

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
        fee_usdc: float = 0.0,
    ) -> bool:
        from app.database import async_session
        from app.models import EntryRecord, Fill

        try:
            async with async_session() as db:
                if fee_usdc <= 0 and fee_rate_bps > 0:
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
