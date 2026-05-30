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


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await _migrate_if_needed()


async def get_db() -> AsyncSession:
    async with async_session() as session:
        yield session
