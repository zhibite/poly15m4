"""
main.py — FastAPI application with lifespan management.
"""

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI
from fastapi.staticfiles import StaticFiles
from loguru import logger

from app.api.analysis_routes import router as analysis_router
from app.api.auth_routes import router as auth_router
from app.api.pages import router as pages_router
from app.api.routes import (
    router as api_router,
    set_live_trader_ref,
    set_reload_fn,
    set_strategy_ref,
)
from app.api.websocket import router as ws_router
from app.api.indicators import router as indicators_router


from app.auth import get_current_user, sync_env_admin
from app.database import init_db
from app.config import settings
from app.runtime_config import get_creds
from app.strategy import SinglePositionStrategy

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"

strategy: SinglePositionStrategy | None = None


async def _reload_live_trader():
    global strategy
    creds = get_creds()
    if not creds.get("private_key"):
        set_live_trader_ref(None)
        if strategy:
            strategy.set_live_trader(None)
        return

    try:
        from app.live_trader import LiveTrader
        trader = LiveTrader(
            host=creds["host"],
            private_key=creds["private_key"],
            api_key=creds.get("api_key", ""),
            api_secret=creds.get("api_secret", ""),
            api_passphrase=creds.get("api_passphrase", ""),
            slippage_pct=float(creds.get("live_slippage_pct", 2.0)),
            signature_type=creds.get("signature_type", 0),
            signer_address=creds.get("signer_address", ""),
            funder_address=creds.get("funder_address", ""),
        )
        set_live_trader_ref(trader)
        if strategy:
            strategy.set_live_trader(trader)
            if trader:
                trader.set_pm_client(strategy.pm)
        logger.info("LiveTrader ready (live_mode={})", creds.get("live_mode"))
    except Exception as exc:
        logger.warning("LiveTrader init failed: {}", exc)
        set_live_trader_ref(None)
        if strategy:
            strategy.set_live_trader(None)


def _handle_task_exception(task: asyncio.Task) -> None:
    """Log uncaught exceptions from fire-and-forget async tasks."""
    try:
        exc = task.exception()
        if exc is not None:
            logger.error("Uncaught exception in background task: {}", exc)
    except asyncio.InvalidStateError:
        pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    global strategy

    logger.info("Starting {} ...", settings.app_name)
    await init_db()
    # Coolify/deployment env may define WEB_USERNAME/WEB_PASSWORD as the
    # authoritative dashboard credentials.
    await sync_env_admin()

    strategy = SinglePositionStrategy()
    set_strategy_ref(strategy)
    set_reload_fn(_reload_live_trader)
    try:
        await _reload_live_trader()
    except Exception as exc:
        logger.warning("LiveTrader init failed on startup (will retry on wallet connect): {}", exc)

    bot_task = asyncio.create_task(strategy.run_forever())
    bot_task.add_done_callback(_handle_task_exception)
    app.state.bot_task = bot_task
    logger.info("Strategy loop started (background).")
    await strategy.start()

    yield

    try:
        task = getattr(app.state, "bot_task", None)
        if task:
            task.remove_done_callback(_handle_task_exception)
            task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=10.0)
            except asyncio.CancelledError:
                logger.debug("Shutdown: strategy task cancelled.")
            except asyncio.TimeoutError:
                logger.warning("Shutdown: strategy task did not exit in 10s — forcing.")
    except Exception:
        pass
    try:
        if strategy:
            await strategy.close()
            # Also close the DecisionLogger (cancels background resolution-tracking tasks)
            if strategy._logger:
                await strategy._logger.close()
    except Exception:
        pass
    # Close shared WebSocket clients (httpx connection pools).
    # close_ws_clients() returns pending tasks that must be awaited so the connections
    # have a chance to close cleanly before the process exits.
    try:
        from app.api.websocket import close_ws_clients
        pending = close_ws_clients()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
    except Exception:
        pass
    logger.info("Shutdown complete.")


app = FastAPI(title=settings.app_name, lifespan=lifespan)

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
else:
    # 前端由 templates/ 提供，static/ 仅用于可选的静态资源
    logger.debug("Static directory {} not found — skipping static mount", STATIC_DIR)

# All /api routes require a valid JWT (Bearer header or HttpOnly cookie),
# except the auth endpoints (login/register/logout/me) and WebSocket handlers
# (ws_router validates the token itself on connect).
app.include_router(api_router, dependencies=[Depends(get_current_user)])
app.include_router(pages_router)
app.include_router(ws_router)
app.include_router(auth_router)
app.include_router(indicators_router, dependencies=[Depends(get_current_user)])
app.include_router(analysis_router, dependencies=[Depends(get_current_user)])
