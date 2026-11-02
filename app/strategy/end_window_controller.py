"""
end_window_controller.py — 末端顺势策略入场/退出控制

一次性建仓，不分批。

入场条件（H 优先级递减）：
  H1 时机窗口     → 必须在 T=12min~T=14.5min 之间
  H2 最低价格     → UP/DOWN 价格 ≥ 0.70
  H3 主策略互斥   → 主策略无反向持仓

退出条件（E 优先级递减）：
  E1 硬性截止     → 到期前 15 秒强制平仓
  E2 方向反转     → 价格跌破入场价 5%
  E3 止盈         → 动态 TP（入场价越低，TP 目标越低）
  E4 止损         → 固定 -15%
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class EndWindowExitSignal:
    """退出信号"""
    should_exit: bool = False
    reason: str = ""
    entry_price: float = 0.0
    pnl_pct: float = 0.0
    current_price: float = 0.0


@dataclass
class EndWindowParams:
    """末端策略运行时参数"""
    enabled: bool = False
    asset_btc_enabled: bool = True
    asset_eth_enabled: bool = True

    window_start_sec: float = 720.0    # T=12min
    window_end_sec: float = 870.0     # T=14.5min
    hardcut_sec: float = 885.0        # 到期前 15 秒

    min_entry_price: float = 0.70     # H2：最低入场价格
    confidence_threshold: float = 50.0  # 入场最低置信度总分

    reversal_stop_pct: float = 0.05   # E2：方向反转止损（5%）
    tp_min_pct: float = 10.0         # E3：动态 TP 下限
    tp_max_pct: float = 30.0        # E3：动态 TP 上限
    tp_entry_min: float = 0.70      # TP 动态映射区间
    tp_entry_max: float = 0.90
    sl_pct: float = -15.0           # E4：固定止损

    base_position_usdc: float = 4.0  # 基准仓位

    # ── 每日最大亏损控制 ─────────────────────────────────────
    max_daily_loss: float = -20.0


class EndWindowController:
    """
    管理末端策略的入场和退出（一次性建仓）。
    """

    def __init__(self):
        self._has_position: dict[str, bool] = {}
        self._params = EndWindowParams()
        self._daily_pnl: dict[str, float] = {}
        self._last_trade_date: dict[str, str] = {}

    # ── 参数同步 ───────────────────────────────────────────────────────────────

    def sync_params(self, cfg: dict) -> None:
        p = self._params
        p.enabled = bool(cfg.get("end_enabled", False))
        p.asset_btc_enabled = bool(cfg.get("end_asset_btc_enabled", True))
        p.asset_eth_enabled = bool(cfg.get("end_asset_eth_enabled", True))

        p.window_start_sec = float(cfg.get("end_window_start_sec", 720.0))
        p.window_end_sec = float(cfg.get("end_window_end_sec", 870.0))
        p.hardcut_sec = float(cfg.get("end_hardcut_sec", 885.0))

        p.min_entry_price = float(cfg.get("end_min_entry_price", 0.70))
        p.confidence_threshold = float(cfg.get("end_confidence_threshold", 50.0))

        p.reversal_stop_pct = float(cfg.get("end_reversal_stop_pct", 0.05))
        p.tp_min_pct = float(cfg.get("end_tp_min_pct", 10.0))
        p.tp_max_pct = float(cfg.get("end_tp_max_pct", 30.0))
        p.tp_entry_min = float(cfg.get("end_tp_entry_min", 0.70))
        p.tp_entry_max = float(cfg.get("end_tp_entry_max", 0.90))
        p.sl_pct = float(cfg.get("end_sl_pct", -15.0))
        p.base_position_usdc = float(cfg.get("end_base_position_usdc", 4.0))
        p.max_daily_loss = float(cfg.get("end_max_daily_loss", -20.0))

    # ── 持仓状态管理 ─────────────────────────────────────────────────────────

    def has_position(self, asset: str) -> bool:
        return self._has_position.get(asset, False)

    def set_position(self, asset: str, has_pos: bool) -> None:
        self._has_position[asset] = has_pos

    def reset(self, asset: str) -> None:
        self._has_position[asset] = False

    # ── 每日亏损重置 ────────────────────────────────────────────────────────
    # 注意：_daily_pnl 以 USDC 计，与 max_daily_loss（负值 USDC）同单位。

    def _check_daily_reset(self, asset: str) -> None:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self._last_trade_date.get(asset) != today:
            self._daily_pnl[asset] = 0.0
            self._last_trade_date[asset] = today

    def record_pnl(self, asset: str, pnl_usd: float) -> None:
        """记录一笔已实现盈亏（USDC）。"""
        self._check_daily_reset(asset)
        self._daily_pnl[asset] = self._daily_pnl.get(asset, 0.0) + pnl_usd

    def is_paused(self, asset: str) -> bool:
        self._check_daily_reset(asset)
        return self._daily_pnl.get(asset, 0.0) <= self._params.max_daily_loss

    # ── 硬条件检查 ────────────────────────────────────────────────────────────

    def check_hard_timing(self, elapsed: float) -> tuple[bool, str]:
        """H1：时机窗口"""
        p = self._params
        if elapsed < p.window_start_sec:
            return False, "too_early"
        if elapsed >= p.window_end_sec:
            return False, "too_late"
        if elapsed >= p.hardcut_sec:
            return False, "hardcut"
        return True, "ok"

    def check_min_entry_price(self, current_mid: float, direction: str) -> tuple[bool, str]:
        """H2：最低入场价格（确保是高胜率方向）"""
        p = self._params
        if direction == "UP" and current_mid < p.min_entry_price:
            return False, "up_price_below_min"
        if direction == "DOWN" and (1.0 - current_mid) < p.min_entry_price:
            return False, "down_price_below_min"
        return True, "ok"

    def check_main_strategy_conflict(
        self,
        main_has_position: bool,
        main_direction: str,
        direction: str,
    ) -> tuple[bool, str]:
        """
        H3：主策略互斥检查。

        同一窗口内最多只允许一个策略持仓：只要主策略在该窗口有持仓
        （或已经交易过），末端策略都跳过，避免叠加仓位。
        """
        if not main_has_position:
            return True, "ok"
        return False, "main_has_position"

    def check_entry(
        self,
        elapsed: float,
        current_mid: float,
        direction: str,
        confidence_total: float,
        main_has_position: bool,
        main_direction: str,
        asset: str = "BTC",
    ) -> tuple[bool, str, float]:
        """
        返回 (是否入场, 入场原因, 仓位 USDC)
        """
        p = self._params

        if not p.enabled:
            return False, "disabled", 0.0
        if self.is_paused(asset):
            return False, "daily_loss_paused", 0.0
        if self.has_position(asset):
            return False, "already_has_position", 0.0

        # H1
        ok, reason = self.check_hard_timing(elapsed)
        if not ok:
            return False, f"hard_fail:{reason}", 0.0

        # H2
        ok, reason = self.check_min_entry_price(current_mid, direction)
        if not ok:
            return False, f"price_reject:{reason}", 0.0

        # H3
        ok, reason = self.check_main_strategy_conflict(
            main_has_position, main_direction, direction,
        )
        if not ok:
            return False, f"conflict:{reason}", 0.0

        # 置信度
        if confidence_total < p.confidence_threshold:
            return False, f"low_confidence:{confidence_total:.1f}", 0.0

        # 仓位
        if confidence_total >= 80:
            multiplier = 1.0
            reason = "strong_signal"
        elif confidence_total >= 65:
            multiplier = 0.8
            reason = "medium_strong_signal"
        else:  # 50 <= total < 65
            multiplier = 0.5
            reason = "medium_signal"

        size = p.base_position_usdc * multiplier
        return True, reason, size

    # ── 退出条件 ──────────────────────────────────────────────────────────────

    def dynamic_tp(self, entry_price: float) -> float:
        """
        动态止盈：入场价越高（赔率越差），TP 目标越高。
        """
        p = self._params
        span = p.tp_entry_max - p.tp_entry_min
        if abs(span) < 1e-9:
            return p.tp_max_pct
        t = (entry_price - p.tp_entry_min) / span
        t = max(0.0, min(1.0, t))
        return p.tp_max_pct - (p.tp_max_pct - p.tp_min_pct) * t

    def should_exit_on_reversal(
        self,
        direction: str,
        entry_price: float,
        current_price: float,
    ) -> bool:
        """
        E2：方向反转止损。

        entry_price / current_price 都是「所持有 token」的价格（UP 持仓用 UP
        token，DOWN 持仓用 DOWN token）。无论方向如何，token 价格从入场价
        下跌超过 reversal_stop_pct 即视为方向反转，立即止损。
        """
        p = self._params
        if direction not in ("UP", "DOWN"):
            return False
        return current_price < entry_price * (1.0 - p.reversal_stop_pct)

    def check_exit(
        self,
        elapsed: float,
        window_duration: float,
        direction: str,
        entry_price: float,
        current_price: float,
        pnl_pct: float,
    ) -> EndWindowExitSignal:
        """
        返回 (是否退出, 退出原因)
        按优先级逐级检查：E1 > E2 > E3 > E4
        """
        p = self._params

        # E1: 硬性截止（优先级最高）
        # elapsed >= hardcut_sec → 到期前 p.hardcut_sec 秒强制平仓
        if elapsed >= p.hardcut_sec:
            return EndWindowExitSignal(
                should_exit=True,
                reason="HARDCUT",
                entry_price=entry_price,
                pnl_pct=pnl_pct,
                current_price=current_price,
            )

        # E2: 方向反转
        if self.should_exit_on_reversal(direction, entry_price, current_price):
            return EndWindowExitSignal(
                should_exit=True,
                reason="DIRECTION_REVERSAL",
                entry_price=entry_price,
                pnl_pct=pnl_pct,
                current_price=current_price,
            )

        # E3: 止盈
        tp = self.dynamic_tp(entry_price)
        if pnl_pct >= tp:
            return EndWindowExitSignal(
                should_exit=True,
                reason="TAKE_PROFIT",
                entry_price=entry_price,
                pnl_pct=pnl_pct,
                current_price=current_price,
            )

        # E4: 止损
        if pnl_pct <= p.sl_pct:
            return EndWindowExitSignal(
                should_exit=True,
                reason="STOP_LOSS",
                entry_price=entry_price,
                pnl_pct=pnl_pct,
                current_price=current_price,
            )

        return EndWindowExitSignal(
            should_exit=False,
            reason="HOLD",
            entry_price=entry_price,
            pnl_pct=pnl_pct,
            current_price=current_price,
        )
