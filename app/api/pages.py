"""
pages.py — Static HTML page routing with JWT auth.
"""

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["pages"])

_TEMPLATES_DIR = None


def _get_templates_dir():
    global _TEMPLATES_DIR
    if _TEMPLATES_DIR is None:
        from pathlib import Path
        base = Path(__file__).resolve().parent.parent.parent
        _TEMPLATES_DIR = base / "templates"
    return _TEMPLATES_DIR


def _verify_token(request: Request) -> bool:
    from app.auth import _extract_token, verify_token
    token = _extract_token(request)
    if not token:
        return False
    return verify_token(token) is not None


def _read_template(name: str) -> str:
    path = _get_templates_dir() / name
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return f.read()
    return f"<h1>{name} not found</h1>"


@router.get("/login")
async def login_page(request: Request):
    if _verify_token(request):
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url="/", status_code=302)
    return HTMLResponse(content=_read_template("login.html"))


@router.get("/")
async def dashboard_page(request: Request):
    if not _verify_token(request):
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url="/login", status_code=302)
    return HTMLResponse(content=_read_template("dashboard.html"))


@router.get("/trades")
async def trades_page(request: Request):
    if not _verify_token(request):
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url="/login", status_code=302)
    return HTMLResponse(content=_read_template("trades.html"))


@router.get("/config")
async def config_page(request: Request):
    if not _verify_token(request):
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url="/login", status_code=302)
    return HTMLResponse(content=_read_template("config.html"))


@router.get("/indicators")
async def indicators_page(request: Request):
    if not _verify_token(request):
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url="/login", status_code=302)
    return HTMLResponse(content=_read_template("indicators.html"))
