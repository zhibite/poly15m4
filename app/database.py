"""
database.py — Async SQLAlchemy setup.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from app.config import settings

engine = create_async_engine(settings.db_url, echo=False)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def _migrate_if_needed():
    """Add missing columns to existing tables (simple zero-downtime migration)."""
    async with engine.begin() as conn:
        result = await conn.execute(text("PRAGMA table_info(trades)"))
        columns = [row[1] for row in result.fetchall()]
        if 'fills' not in columns:
            await conn.execute(text("ALTER TABLE trades ADD COLUMN fills INTEGER DEFAULT 1 NOT NULL"))
        if 'strategy' not in columns:
            await conn.execute(text("ALTER TABLE trades ADD COLUMN strategy VARCHAR(8) DEFAULT 'main' NOT NULL"))
            # 旧数据回填：末端策略的 close_reason 以 END_ 开头
            await conn.execute(text(
                "UPDATE trades SET strategy = 'end' WHERE close_reason LIKE 'END_%'"
            ))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_trades_strategy ON trades (strategy)"))
        if 'entry_core_agree' not in columns:
            await conn.execute(text("ALTER TABLE trades ADD COLUMN entry_core_agree INTEGER DEFAULT 0 NOT NULL"))

        # decision_snapshots: 因子模型列（旧库自动补列）
        ds_result = await conn.execute(text("PRAGMA table_info(decision_snapshots)"))
        ds_columns = [row[1] for row in ds_result.fetchall()]
        if 'factor_mode' not in ds_columns:
            await conn.execute(text("ALTER TABLE decision_snapshots ADD COLUMN factor_mode INTEGER DEFAULT 0"))
        if 'factor_net' not in ds_columns:
            await conn.execute(text("ALTER TABLE decision_snapshots ADD COLUMN factor_net FLOAT DEFAULT 0"))
        if 'factor_agreement' not in ds_columns:
            await conn.execute(text("ALTER TABLE decision_snapshots ADD COLUMN factor_agreement FLOAT DEFAULT 0"))
        if 'factor_conflict' not in ds_columns:
            await conn.execute(text("ALTER TABLE decision_snapshots ADD COLUMN factor_conflict FLOAT DEFAULT 0"))
        if 'factor_direction' not in ds_columns:
            await conn.execute(text("ALTER TABLE decision_snapshots ADD COLUMN factor_direction VARCHAR(8) DEFAULT 'NONE'"))
        if 'factors_json' not in ds_columns:
            await conn.execute(text("ALTER TABLE decision_snapshots ADD COLUMN factors_json TEXT DEFAULT ''"))


async def init_db():
    # Ensure all ORM models are imported/registered on Base.metadata before create_all.
    import app.models  # noqa: F401

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await _migrate_if_needed()


async def get_db() -> AsyncSession:
    async with async_session() as session:
        yield session
