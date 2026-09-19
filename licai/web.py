from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .auth import (
    AuthMiddleware,
    clear_login_fails,
    clear_session_cookie,
    client_ip,
    login_blocked,
    make_session_token,
    mark_login_fail,
    password_ok,
    session_cookie,
    valid_session,
    COOKIE_NAME,
)
from .autoharvest import AutoHarvestWorker
from .config import normalize_hedge_symbol
from .ops import OpsService

STATIC_DIR = Path(__file__).resolve().parent / "static"
NO_STORE = {"Cache-Control": "no-store"}
ops = OpsService()
store = ops.store
_auto_worker = AutoHarvestWorker(ops)


class AccountIn(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    api_key: str = Field(min_length=8)
    api_secret: str = Field(min_length=8)
    proxy: str = ""
    hedge_symbol: str = "ETHUSDT"


class AccountPatch(BaseModel):
    name: str | None = None
    api_key: str | None = None
    api_secret: str | None = None
    proxy: str | None = None
    hedge_symbol: str | None = None
    take_profit_usdt: float | None = None
    take_profit_custom: bool | None = None
    auto_harvest: bool | None = None


class ActionIn(BaseModel):
    action: str
    force: bool = False
    mode: str | None = None


class LoginIn(BaseModel):
    password: str = ""


@asynccontextmanager
async def lifespan(_app: FastAPI):
    _auto_worker.start()
    try:
        yield
    finally:
        _auto_worker.stop()


def create_app() -> FastAPI:
    app = FastAPI(title="理财对冲操作台", docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)
    app.add_middleware(AuthMiddleware)

    @app.get("/")
    def index(request: Request):
        if valid_session(request.cookies.get(COOKIE_NAME)):
            return FileResponse(STATIC_DIR / "index.html", headers=NO_STORE)
        return FileResponse(STATIC_DIR / "login.html", headers=NO_STORE)

    @app.post("/api/login")
    def login(request: Request, body: LoginIn):
        ip = client_ip(request)
        if login_blocked(ip):
            raise HTTPException(429, "尝试次数过多，请稍后再试")
        if not password_ok(body.password):
            mark_login_fail(ip)
            raise HTTPException(401, "密码错误")
        clear_login_fails(ip)
        response = JSONResponse({"ok": True})
        session_cookie(response, make_session_token())
        return response

    @app.post("/api/logout")
    def logout():
        response = JSONResponse({"ok": True})
        clear_session_cookie(response)
        return response

    @app.get("/api/session")
    def session():
        return {"ok": True}

    @app.get("/api/accounts")
    def list_accounts():
        return {
            "accounts": [item.public_dict() for item in store.list_accounts()],
            "hedge_symbols": list(ops.base.hedge_symbols),
            "live_poll_seconds": int(ops.base.live_poll_seconds),
        }

    @app.post("/api/accounts")
    def add_account(body: AccountIn):
        try:
            symbol = normalize_hedge_symbol(body.hedge_symbol, ops.base.hedge_symbols)
            account = store.add(body.name, body.api_key, body.api_secret, body.proxy, symbol)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        return account.public_dict()

    @app.patch("/api/accounts/{account_id}")
    def patch_account(account_id: int, body: AccountPatch):
        try:
            symbol = None
            if body.hedge_symbol is not None:
                symbol = normalize_hedge_symbol(body.hedge_symbol, ops.base.hedge_symbols)
            account = store.update(
                account_id,
                name=body.name,
                api_key=body.api_key,
                api_secret=body.api_secret,
                proxy=body.proxy,
                hedge_symbol=symbol,
                take_profit_usdt=body.take_profit_usdt,
                take_profit_custom=body.take_profit_custom,
                auto_harvest=body.auto_harvest,
            )
        except KeyError:
            raise HTTPException(404, "账号不存在") from None
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        ops.invalidate_snapshot(account_id)
        return account.public_dict()

    @app.delete("/api/accounts/{account_id}")
    def delete_account(account_id: int):
        try:
            store.delete(account_id)
        except KeyError:
            raise HTTPException(404, "账号不存在") from None
        ops.invalidate_snapshot(account_id)
        return {"ok": True}

    @app.get("/api/accounts/{account_id}/snapshot")
    def snapshot(account_id: int, force: bool = False):
        try:
            account = store.get(account_id)
        except KeyError:
            raise HTTPException(404, "账号不存在") from None
        try:
            return ops.snapshot(account, force=force)
        except Exception as exc:
            raise HTTPException(500, str(exc)) from exc

    @app.get("/api/accounts/{account_id}/monitor")
    def monitor(account_id: int):
        try:
            account = store.get(account_id)
        except KeyError:
            raise HTTPException(404, "账号不存在") from None
        try:
            return ops.monitor(account)
        except Exception as exc:
            raise HTTPException(500, str(exc)) from exc

    @app.post("/api/accounts/{account_id}/actions")
    def run_action(account_id: int, body: ActionIn):
        try:
            account = store.get(account_id)
        except KeyError:
            raise HTTPException(404, "账号不存在") from None
        try:
            return ops.run_action(account, body.action, force=body.force, mode=body.mode)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        except Exception as exc:
            raise HTTPException(500, str(exc)) from exc

    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
    return app


app = create_app()
