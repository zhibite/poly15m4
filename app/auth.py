"""
auth.py — JWT Bearer Token + bcrypt password hashing.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
import bcrypt

from app.config import settings
from app.database import async_session
from app.models import User

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login", auto_error=False)
_COOKIE_NAME = "pm_token"


def hash_password(password: str) -> str:
    pwd_bytes = password.encode("utf-8")[:72]
    return bcrypt.hashpw(pwd_bytes, bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode("utf-8")[:72], hashed.encode("utf-8"))
    except Exception:
        return False


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (
        expires_delta or timedelta(minutes=settings.access_token_expire_minutes)
    )
    to_encode["exp"] = expire
    return jwt.encode(to_encode, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


def verify_token(token: str) -> dict | None:
    try:
        payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
        return payload
    except JWTError:
        return None


def _extract_token(request: Request) -> str | None:
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        token = auth[7:].strip()
        if token:
            return token
    # Fall back to the HttpOnly cookie when no Bearer token was supplied
    # (including the case of an empty "Authorization: Bearer " header).
    return request.cookies.get(_COOKIE_NAME)


async def get_current_user(request: Request) -> User:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="认证失败，请重新登录",
        headers={"WWW-Authenticate": "Bearer"},
    )
    token = _extract_token(request)
    if not token:
        raise credentials_exception
    payload = verify_token(token)
    if payload is None:
        raise credentials_exception
    sub = payload.get("sub")
    if sub is None:
        raise credentials_exception
    try:
        user_id = int(sub) if not isinstance(sub, int) else sub
    except (TypeError, ValueError):
        raise credentials_exception

    async with async_session() as db:
        from sqlalchemy import select
        row = await db.execute(select(User).where(User.id == user_id))
        user = row.scalar_one_or_none()
    if user is None:
        raise credentials_exception
    return user


def _set_cookie(response, token: str, *, secure: bool = True):
    response.set_cookie(
        key=_COOKIE_NAME,
        value=token,
        httponly=True,
        samesite="lax",
        secure=secure,
        max_age=settings.access_token_expire_minutes * 60,
    )
