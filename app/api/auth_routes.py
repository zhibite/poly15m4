"""
auth_routes.py — Authentication API: register / login / logout / me.
"""

import threading
from collections import deque
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request, Response

from app.auth import (
    create_access_token, hash_password, verify_password, verify_token,
    _set_cookie,
)
from app.database import async_session
from app.models import User

router = APIRouter(prefix="/api/auth", tags=["auth"])

_rate_limit_lock = threading.Lock()
_rate_limit: dict[str, tuple[deque, datetime]] = {}


def _check_rate_limit(username: str):
    now = datetime.now(timezone.utc)
    with _rate_limit_lock:
        entry = _rate_limit.get(username)
        if entry is None:
            d = deque(maxlen=5)
            d.append(now)
            _rate_limit[username] = (d, now)
            return
        failures, window_start = entry
        if (now - window_start).total_seconds() > 900:
            failures.clear()
            _rate_limit[username] = (failures, now)
            return
        if len(failures) >= 5:
            raise HTTPException(status_code=429, detail="Too many failed attempts, try again in 15 minutes")
        failures.append(now)
        _rate_limit[username] = (failures, window_start)


def _client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def _record_failure(username: str):
    with _rate_limit_lock:
        entry = _rate_limit.get(username)
        if entry:
            failures, window_start = entry
            failures.append(datetime.now(timezone.utc))
            _rate_limit[username] = (failures, window_start)


def _is_https(request: Request) -> bool:
    """Detect if the request arrived over HTTPS."""
    if request.url.scheme == "https":
        return True
    forwarded = request.headers.get("X-Forwarded-Proto", "").lower()
    if forwarded in ("https", "wss"):
        return True
    return False


@router.get("/has-users")
async def has_users():
    async with async_session() as db:
        from sqlalchemy import select, func
        row = await db.execute(select(func.count(User.id)))
        count = row.scalar() or 0
    return {"has_users": count > 0}


@router.post("/register")
async def register(request: Request, body: dict):
    # Per-IP rate limit: max 5 registrations per 15 minutes per IP
    _check_generic_rate_limit(_client_ip(request), max_requests=5, window_sec=900)

    username = (body.get("username") or "").strip()
    password = body.get("password") or ""
    if len(username) < 3 or len(username) > 64:
        raise HTTPException(status_code=400, detail="Username must be 3-64 characters")
    if len(password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")

    async with async_session() as db:
        from sqlalchemy import select
        row = await db.execute(select(User).where(User.username == username))
        if row.scalar_one_or_none():
            raise HTTPException(status_code=409, detail="Username already exists")

        from sqlalchemy import func
        count_row = await db.execute(select(func.count(User.id)))
        total = count_row.scalar() or 0

        user = User(
            username=username,
            password_hash=hash_password(password),
            is_admin=1 if total == 0 else 0,
        )
        db.add(user)
        await db.commit()
        await db.refresh(user)

    token = create_access_token({"sub": str(user.id), "username": username})
    return {"username": username, "is_admin": user.is_admin, "token": token}


@router.post("/login")
async def login(request: Request, response: Response, body: dict):
    # Per-IP rate limit: max 20 login attempts per minute per IP
    _check_generic_rate_limit(_client_ip(request), max_requests=20, window_sec=60)

    username = (body.get("username") or "").strip()
    password = body.get("password") or ""

    _check_rate_limit(username)

    async with async_session() as db:
        from sqlalchemy import select, func
        count_row = await db.execute(select(func.count(User.id)))
        total = count_row.scalar() or 0

        # First time login: auto-register as admin
        if total == 0:
            user = User(
                username=username,
                password_hash=hash_password(password),
                is_admin=1,
            )
            db.add(user)
            await db.commit()
            await db.refresh(user)
            token = create_access_token({"sub": str(user.id), "username": username, "is_admin": 1})
            _set_cookie(response, token, secure=_is_https(request))
            return {"username": username, "is_admin": 1, "token": token, "first_time": True}

        row = await db.execute(select(User).where(User.username == username))
        user = row.scalar_one_or_none()

    if user is None or not verify_password(password, user.password_hash):
        _record_failure(username)
        raise HTTPException(status_code=401, detail="Incorrect username or password")

    token = create_access_token({"sub": str(user.id), "username": username, "is_admin": user.is_admin})
    _set_cookie(response, token, secure=_is_https(request))
    return {"username": user.username, "is_admin": user.is_admin, "token": token}


@router.post("/logout")
async def logout(response: Response):
    response.delete_cookie("pm_token")
    return {"ok": True}


@router.get("/me")
async def me(request: Request):
    from app.auth import _extract_token, verify_token
    token = _extract_token(request)
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    payload = verify_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid token")
    return {"username": payload.get("username", ""), "is_admin": payload.get("is_admin", 0)}


# ── Generic API rate limiter ─────────────────────────────────────────────────────

_generic_rate_limit_lock = threading.Lock()
_generic_rate_limit: dict[str, tuple[deque, datetime]] = {}


def _check_generic_rate_limit(key: str, *, max_requests: int = 60, window_sec: int = 60):
    """
    Lightweight per-IP rate limiter for config and wallet endpoints.
    key: identifier (e.g. IP address or "config_api")
    """
    now = datetime.now(timezone.utc)
    with _generic_rate_limit_lock:
        entry = _generic_rate_limit.get(key)
        if entry is None:
            d = deque(maxlen=max_requests)
            d.append(now)
            _generic_rate_limit[key] = (d, now)
            return
        d, window_start = entry
        if (now - window_start).total_seconds() > window_sec:
            _generic_rate_limit[key] = (deque(maxlen=max_requests), now)
            return
        if len(d) >= max_requests:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded ({max_requests} requests per {window_sec}s). Try again later.",
            )
        d.append(now)
