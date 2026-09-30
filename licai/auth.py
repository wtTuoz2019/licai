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


ROLES = ("admin", "super")


def _load_env() -> None:
    load_dotenv(ROOT / ".env")


def dashboard_password() -> str:
    _load_env()
    return os.getenv("LICAI_DASHBOARD_PASSWORD", "").strip()


def super_password() -> str:
    _load_env()
    return os.getenv("LICAI_SUPER_PASSWORD", "").strip()


def require_dashboard_password() -> str:
    password = dashboard_password()
    if not password:
        raise SystemExit("请先在 .env 设置 LICAI_DASHBOARD_PASSWORD，操作台必须有访问密码")
    return password


def _same(left: str, right: str) -> bool:
    if not left or not right:
        return False
    a = left.encode("utf-8")
    b = right.encode("utf-8")
    if len(a) != len(b):
        return False
    return secrets.compare_digest(a, b)


def _secret() -> bytes:
    extra = os.getenv("LICAI_DASHBOARD_SECRET", "").strip()
    material = f"{dashboard_password()}|{super_password()}|{extra}".encode("utf-8")
    return hashlib.sha256(material).digest()


def make_session_token(role: str = "admin") -> str:
    if role not in ROLES:
        raise ValueError(role)
    issued = str(int(time.time()))
    msg = f"{issued}.{role}".encode("utf-8")
    sig = hmac.new(_secret(), msg, hashlib.sha256).hexdigest()
    return f"{issued}.{role}.{sig}"


def session_role(token: str | None) -> str | None:
    if not token or token.count(".") < 2 or not dashboard_password():
        return None
    issued_raw, role, sig = token.split(".", 2)
    if role not in ROLES:
        return None
    if role == "super" and not super_password():
        return None
    try:
        issued = int(issued_raw)
    except ValueError:
        return None
    now = time.time()
    if issued > now + 60 or now - issued > COOKIE_TTL:
        return None
    expect = hmac.new(_secret(), f"{issued_raw}.{role}".encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expect, sig):
        return None
    return role


def valid_session(token: str | None) -> bool:
    return session_role(token) is not None


def login_role(value: str) -> str | None:
    """超级管理员密码优先，但和普通密码相同时只当作普通管理员。"""
    if not value:
        return None
    admin_pw = dashboard_password()
    super_pw = super_password()
    if super_pw and admin_pw and _same(super_pw, admin_pw):
        return "admin" if _same(value, admin_pw) else None
    if super_pw and _same(value, super_pw):
        return "super"
    if admin_pw and _same(value, admin_pw):
        return "admin"
    return None


def password_ok(value: str) -> bool:
    return login_role(value) is not None


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
        role = session_role(request.cookies.get(COOKIE_NAME))
        if role:
            request.state.role = role
            return await call_next(request)
        if path.startswith("/api/") or path.startswith("/static/"):
            return JSONResponse({"detail": "未登录"}, status_code=401)
        return JSONResponse({"detail": "未登录"}, status_code=401)
