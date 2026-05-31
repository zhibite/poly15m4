"""
position_state.py — 仓位状态机 + 平仓原因常量

状态：
- NO_POSITION：等待入场
- HAS_POSITION：持有仓位

ExitReason（M1 fix）：
用类常量替代字符串字面量（"止盈" in close_reason），避免：
- 字符串拼写变化导致逻辑失效
- 难以维护的 magic strings
- 中文/英文混淆

用法：
    if ExitReason.is_take_profit(reason):
        ...
    elif ExitReason.is_stop_loss(reason):
        ...
"""

from enum import Enum, auto


class PositionState(Enum):
    """仓位状态枚举"""

    # 无持仓，可以发起入场
    NO_POSITION = auto()

    # 持有仓位，监听止盈 / 补仓 / 结算
    HAS_POSITION = auto()


class ExitReason:
    """
    平仓原因字符串常量 + 类型安全辅助方法。
    不继承 Enum 以避免字符串转换开销，直接用 class-level 常量。
    """

    # ── 主要退出原因（与 exit_controller.handle_exit 分支一一对应）────────────
    TAKE_PROFIT      = "止盈"
    TAKE_PROFIT_2    = "第二止盈"     # +30% 第二阶段止盈（剩余仓位的 60%）
    STOP_LOSS        = "止损"
    STOP_LOSS_EARLY  = "信号早平"     # Gate1：信号反转提前止损
    GATE3_EXIT        = "信号减弱退出"  # Gate3：信号减弱且盈利退出
    SETTLE           = "窗口结算"
    INVALID_POSITION = "无效持仓数量"

    # ── _close_loop / _close_with_override 中的具体原因 ───────────────
    ALREADY_CLOSED    = "已全部卖出"
    DUST_CLEARED     = "小额残差已清"
    RESIDUAL_EXHAUSTED = "残差轮次耗尽"
    SETTLE_EXHAUSTED  = "settle_exhausted"
    CLOB_REDEEM       = "CLOB拒绝_赎回兜底"
    REDEEM_ZERO       = "赎回价格为零"
    REDEEM_FAIL       = "链上赎回失败"
    REDEEM_SUCCESS    = "链上赎回兜底"

    # ── 别名（与 position_manager 中的实际字符串保持一致）───────────────
    STOP_LOSS_FULL = "固定止损（全部）"

    # ── 中间状态（handle_exit -> spawn_close 的 reason 参数）─────────
    # handle_exit 收到的 reason（来自 check_exit 返回值）
    REASON_TP        = "TP"
    REASON_SL_FULL   = "SL_FULL"
    REASON_SETTLE    = "settle"
    REASON_INVALID   = "invalid_tokens"

    # ── tp_hit kind 别名（与 close_reason 写入 DB 的值对应）────────
    # spawn_close 的 reason 参数经 _do_close 传入 close_reason，
    # 最终由 _record_trade 写入 Trade.close_reason，同时用作 tp_hit["kind"]
    # 以下常量使 _do_close 中的 tp_hit.kind 检查与 check_exit 返回值对齐
    KIND_TP          = "止盈"
    KIND_TP2         = "第二止盈"
    KIND_SL          = "止损"
    KIND_SL_EARLY    = "信号早平"
    KIND_GATE3       = "信号减弱退出"
    KIND_SL_FULL     = "固定止损（全部）"

    # ── 辅助方法 ──────────────────────────────────────────────────────

    @staticmethod
    def is_take_profit(reason: str) -> bool:
        return "止盈" in reason if reason else False

    @staticmethod
    def is_stop_loss(reason: str) -> bool:
        if not reason:
            return False
        return any(kw in reason for kw in (ExitReason.STOP_LOSS, ExitReason.STOP_LOSS_FULL, ExitReason.INVALID_POSITION))

    @staticmethod
    def is_settlement(reason: str) -> bool:
        if not reason:
            return False
        return "结算" in reason or reason == ExitReason.SETTLE_EXHAUSTED

    @staticmethod
    def is_dust(reason: str) -> bool:
        """小额残差已清——PnL 已由主退出路径记录，无需单独记录"""
        if not reason:
            return False
        return any(kw in reason for kw in (ExitReason.DUST_CLEARED, ExitReason.ALREADY_CLOSED, ExitReason.RESIDUAL_EXHAUSTED))
