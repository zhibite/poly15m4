"""
db_unit_of_work.py — 统一数据库事务上下文管理器

入场/加仓的 Fill 保存，以及平仓的 Trade 保存，
全部通过此模块封装，确保：
1. 失败时自动回滚内存状态（通过传入的回滚回调）
2. 成功时自动 commit
3. 异常时统一日志记录

使用方式：
    await db_transaction(
        db_session,
        fill_ops=[...],
        rollback_on_error=lambda: rollback_memory_state(),
    )
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Sequence

from loguru import logger

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


async def db_transaction(
    db: "AsyncSession",
    *,
    commit_on_success: bool = True,
    on_commit: Callable[[], Any] | None = None,
    on_rollback: Callable[[Exception], Any] | None = None,
    label: str = "db_transaction",
) -> None:
    """
    基础事务上下文。
    传入的 callback 均在事务内部执行。

    用法：
        async with db_transaction(db, label="save_fill"):
            db.add(fill)
    """
    try:
        yield
        if commit_on_success:
            await db.commit()
        if on_commit:
            on_commit()
    except Exception as exc:
        await db.rollback()
        logger.debug("{} rolled back on error: {}", label, exc)
        if on_rollback:
            on_rollback(exc)
        raise


async def save_fills_in_transaction(
    db: "AsyncSession",
    fills: Sequence[Any],
) -> list[int]:
    """
    在一个事务内批量保存 Fill 记录，返回写入后的 ID 列表。
    全部成功才 commit，任一失败则 rollback 并重新抛出。
    """
    try:
        for f in fills:
            db.add(f)
        await db.flush()
        ids = [f.id for f in fills]
        await db.commit()
        return ids
    except Exception:
        await db.rollback()
        raise


async def save_trade_and_link_fills(
    db: "AsyncSession",
    trade,
    fill_ids: list[int],
    fill_ids_lock,
    pending_fill_ids_ref: list[int],
) -> None:
    """
    在一个事务内写入 Trade 记录，并将 fill_ids 关联到该 Trade。
    commit 后自动更新引用持有的 pending_fill_ids 列表。
    """
    from sqlalchemy import update
    from app.models import Fill

    try:
        db.add(trade)
        await db.flush()

        if fill_ids and fill_ids_lock is not None:
            async with fill_ids_lock:
                pending_fill_ids_ref.extend(fill_ids)
                await db.execute(
                    update(Fill)
                    .where(Fill.id.in_(fill_ids))
                    .values(trade_id=trade.id)
                )

        await db.commit()
    except Exception:
        await db.rollback()
        raise
