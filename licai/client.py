from __future__ import annotations

import hashlib
import hmac
import threading
import time
from typing import Any, Callable
from urllib.parse import urlencode

import requests


class BinanceAPIError(RuntimeError):
    def __init__(self, message: str, status: int | None = None, payload: Any = None):
        super().__init__(message)
        self.status = status
        self.payload = payload

    @property
    def code(self) -> int | None:
        if isinstance(self.payload, dict) and self.payload.get("code") is not None:
            try:
                return int(self.payload["code"])
            except (TypeError, ValueError):
                return None
        return None


# 代理失效时回调：传入旧代理与异常，返回新代理 URL；返回 None 表示改直连
ProxyRotator = Callable[[str, BaseException], str | None]


class BinanceClient:
    SPOT = "https://api.binance.com"
    FUTURES = "https://fapi.binance.com"
    PAPI = "https://papi.binance.com"
    _time_lock = threading.Lock()
    _shared_offset_ms = 0
    _shared_offset_at = 0.0
    _max_proxy_rotations = 5

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        recv_window: int = 5000,
        timeout: int = 20,
        proxy: str | None = None,
        proxy_rotator: ProxyRotator | None = None,
    ):
        self.api_key = api_key
        self.api_secret = api_secret.encode("utf-8")
        self.recv_window = recv_window
        self.timeout = timeout
        self.proxy = (proxy or "").strip() or None
        self.proxy_rotator = proxy_rotator
        self._tls = threading.local()
        self._time_offset_ms = BinanceClient._shared_offset_ms
        if api_key:
            try:
                self.sync_time()
            except BinanceAPIError:
                self._time_offset_ms = BinanceClient._shared_offset_ms

    def bind_proxy_rotator(self, rotator: ProxyRotator | None) -> None:
        self.proxy_rotator = rotator

    def apply_proxy(self, proxy: str | None) -> None:
        """切换代理并丢弃旧 Session，下次请求用新出口。"""
        next_proxy = (proxy or "").strip() or None
        if next_proxy == self.proxy:
            return
        self.proxy = next_proxy
        if getattr(self._tls, "session", None) is not None:
            self._tls.session = None

    def sync_time(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - BinanceClient._shared_offset_at < 300:
            self._time_offset_ms = BinanceClient._shared_offset_ms
            return
        with BinanceClient._time_lock:
            now = time.monotonic()
            if not force and now - BinanceClient._shared_offset_at < 300:
                self._time_offset_ms = BinanceClient._shared_offset_ms
                return
            data = self.public("GET", "/api/v3/time")
            BinanceClient._shared_offset_ms = int(data["serverTime"]) - int(time.time() * 1000)
            BinanceClient._shared_offset_at = time.monotonic()
            self._time_offset_ms = BinanceClient._shared_offset_ms

    def _http(self) -> requests.Session:
        """每个线程自己的连接，刷新时资金和仓位可以同时打。"""
        sess = getattr(self._tls, "session", None)
        if sess is not None:
            return sess
        sess = requests.Session()
        sess.trust_env = False
        sess.headers.update({"X-MBX-APIKEY": self.api_key, "Accept": "application/json"})
        if self.proxy:
            sess.proxies.update({"http": self.proxy, "https": self.proxy})
        self._tls.session = sess
        return sess

    @property
    def session(self) -> requests.Session:
        return self._http()

    @session.setter
    def session(self, value: requests.Session) -> None:
        self._tls.session = value

    def timestamp(self) -> int:
        return int(time.time() * 1000) + self._time_offset_ms

    def public(self, method: str, path: str, params: dict | None = None, futures: bool = False, papi: bool = False) -> Any:
        return self._send(method, path, params or {}, signed=False, futures=futures, papi=papi)

    def signed(self, method: str, path: str, params: dict | None = None, futures: bool = False, papi: bool = False) -> Any:
        if not self.api_key or not self.api_secret:
            raise BinanceAPIError("缺少 BINANCE_API_KEY / BINANCE_API_SECRET")
        payload = {k: v for k, v in (params or {}).items() if v is not None}
        payload["timestamp"] = self.timestamp()
        payload["recvWindow"] = self.recv_window
        query = urlencode(payload, doseq=True)
        signature = hmac.new(self.api_secret, query.encode("utf-8"), hashlib.sha256).hexdigest()
        payload["signature"] = signature
        try:
            return self._send(method, path, payload, signed=True, futures=futures, papi=papi)
        except BinanceAPIError as exc:
            if self._is_timestamp_error(exc):
                self.sync_time(force=True)
                payload["timestamp"] = self.timestamp()
                query = urlencode({k: v for k, v in payload.items() if k != "signature"}, doseq=True)
                payload["signature"] = hmac.new(self.api_secret, query.encode("utf-8"), hashlib.sha256).hexdigest()
                return self._send(method, path, payload, signed=True, futures=futures, papi=papi)
            raise

    def _send(self, method: str, path: str, params: dict, signed: bool, futures: bool, papi: bool = False) -> Any:
        if papi:
            base = self.PAPI
        elif futures:
            base = self.FUTURES
        else:
            base = self.SPOT
        url = base + path
        last_net: BaseException | None = None
        for attempt in range(self._max_proxy_rotations + 2):
            try:
                response = self.session.request(method, url, params=params, timeout=self.timeout)
            except requests.RequestException as exc:
                last_net = exc
                if not self._should_rotate_proxy(exc) or attempt >= self._max_proxy_rotations:
                    break
                if not self.proxy or not self.proxy_rotator:
                    break
                try:
                    nxt = self.proxy_rotator(self.proxy, exc)
                except Exception as rot_exc:
                    last_net = rot_exc
                    self.apply_proxy(None)
                    continue
                # nxt 为新代理，或 None=改直连再试一次
                self.apply_proxy(nxt)
                continue
            try:
                data = response.json()
            except ValueError as exc:
                raise BinanceAPIError(f"无法解析响应: {response.text[:300]}", status=response.status_code) from exc
            if response.status_code >= 400:
                msg = data.get("msg") if isinstance(data, dict) else str(data)
                code = data.get("code") if isinstance(data, dict) else response.status_code
                raise BinanceAPIError(f"Binance 错误 {code}: {msg}", status=response.status_code, payload=data)
            if isinstance(data, dict) and "code" in data and "msg" in data:
                try:
                    code_int = int(data["code"])
                except (TypeError, ValueError):
                    code_int = 0
                if code_int < 0:
                    raise BinanceAPIError(
                        f"Binance 错误 {data['code']}: {data['msg']}",
                        status=response.status_code,
                        payload=data,
                    )
            return data
        raise BinanceAPIError(f"网络错误: {last_net}") from last_net

    @staticmethod
    def _should_rotate_proxy(exc: BaseException) -> bool:
        """连接/隧道/超时等代理层失败才换线；业务 HTTP 错误不换。"""
        if isinstance(
            exc,
            (
                requests.exceptions.ProxyError,
                requests.exceptions.ConnectTimeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.SSLError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ChunkedEncodingError,
            ),
        ):
            return True
        text = str(exc).lower()
        keys = (
            "proxy",
            "tunnel",
            "timed out",
            "timeout",
            "connection reset",
            "connection aborted",
            "network is unreachable",
            "name or service not known",
            "nodename nor servname",
            "max retries exceeded",
        )
        return any(k in text for k in keys)

    @staticmethod
    def _is_timestamp_error(exc: BinanceAPIError) -> bool:
        payload = exc.payload if isinstance(exc.payload, dict) else {}
        return payload.get("code") in (-1021, "-1021")
