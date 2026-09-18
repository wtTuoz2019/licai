from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import time
from collections import defaultdict

from dotenv import load_dotenv
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from .config import ROOT

COOKIE_NAME = "licai_session"
COOKIE_TTL = 7 * 24 * 3600
OPEN_PATHS = {
    ("/", "GET"),
    ("/", "HEAD"),
    ("/api/login", "POST"),
    ("/api/logout", "POST"),
}

_login_fails: dict[str, list[float]] = defaultdict(list)


def dashboard_password() -> str:
    load_dotenv(ROOT / ".env")
    return os.getenv("LICAI_DASHBOARD_PASSWORD", "").strip()


def require_dashboard_password() -> str:
    password = dashboard_password()
    if not password:
        raise SystemExit("请先在 .env 设置 LICAI_DASHBOARD_PASSWORD，操作台必须有访问密码")
    return password


def _secret() -> bytes:
    extra = os.getenv("LICAI_DASHBOARD_SECRET", "").strip()
    material = f"{dashboard_password()}|{extra}".encode("utf-8")
    return hashlib.sha256(material).digest()


def make_session_token() -> str:
    issued = str(int(time.time()))
    sig = hmac.new(_secret(), issued.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{issued}.{sig}"


def valid_session(token: str | None) -> bool:
    if not token or "." not in token or not dashboard_password():
        return False
    issued_raw, sig = token.split(".", 1)
    try:
        issued = int(issued_raw)
    except ValueError:
        return False
    now = time.time()
    if issued > now + 60 or now - issued > COOKIE_TTL:
        return False
    expect = hmac.new(_secret(), issued_raw.encode("utf-8"), hashlib.sha256).hexdigest()
    return hmac.compare_digest(expect, sig)


def password_ok(value: str) -> bool:
    expected = dashboard_password()
    if not expected or not value:
        return False
    return secrets.compare_digest(value.encode("utf-8"), expected.encode("utf-8"))


def login_blocked(ip: str) -> bool:
    now = time.time()
    recent = [t for t in _login_fails[ip] if now - t < 600]
    _login_fails[ip] = recent
    return len(recent) >= 8


def mark_login_fail(ip: str) -> None:
    _login_fails[ip].append(time.time())


def clear_login_fails(ip: str) -> None:
    _login_fails.pop(ip, None)


def session_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        COOKIE_NAME,
        token,
        max_age=COOKIE_TTL,
        httponly=True,
        samesite="strict",
        path="/",
    )


def clear_session_cookie(response: Response) -> None:
    response.delete_cookie(COOKIE_NAME, path="/")


def client_ip(request: Request) -> str:
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        method = request.method.upper()
        if (path, method) in OPEN_PATHS or (path == "/api/logout" and method == "POST"):
            return await call_next(request)
        if valid_session(request.cookies.get(COOKIE_NAME)):
            return await call_next(request)
        if path.startswith("/api/") or path.startswith("/static/"):
            return JSONResponse({"detail": "未登录"}, status_code=401)
        return JSONResponse({"detail": "未登录"}, status_code=401)
