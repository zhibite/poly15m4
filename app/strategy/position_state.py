"""
position_state.py — 仓位状态机 + 平仓原因常量
"""

from enum import Enum, auto


class PositionState(Enum):
    NO_POSITION = auto()
    HAS_POSITION = auto()


class ExitReason:
    """平仓原因字符串常量。"""

    # 平仓结果（写入 DB）
    TAKE_PROFIT      = "止盈"
    STOP_LOSS        = "止损"
    TRAILING_STOP    = "移动止损"
    SETTLE           = "窗口结算"
    INVALID_POSITION = "无效持仓数量"

    # check_exit 返回的 reason（handle_exit 入口参数）
    REASON_TP       = "TP"
    REASON_SL       = "SL"
    REASON_TRAILING = "TRAILING"
    REASON_SETTLE   = "settle"
    REASON_INVALID  = "invalid_tokens"

    # 平仓重试兜底
    SETTLE_EXHAUSTED = "settle_exhausted"
    ALREADY_CLOSED   = "已全部卖出"
    DUST_CLEARED     = "小额残差已清"

    # ── 卖出失败 / 链上赎回兜底（PositionManager._close_loop）───────────
    CLOB_REDEEM        = "CLOB兜底卖出"
    REDEEM_SUCCESS     = "链上赎回"
    REDEEM_FAIL        = "赎回失败-按结算价"
    REDEEM_ZERO        = "赎回零回收"
    RESIDUAL_EXHAUSTED = "残差轮次耗尽"

    # ── 辅助方法 ──────────────────────────────────────────────────────

    @staticmethod
    def is_take_profit(reason: str) -> bool:
        return "止盈" in reason if reason else False

    @staticmethod
    def is_stop_loss(reason: str) -> bool:
        return "止损" in reason if reason else False

    @staticmethod
    def is_settlement(reason: str) -> bool:
        return "结算" in reason if reason else False

    @staticmethod
    def is_dust(reason: str) -> bool:
        if not reason:
            return False
        return any(kw in reason for kw in (ExitReason.ALREADY_CLOSED, ExitReason.DUST_CLEARED))
