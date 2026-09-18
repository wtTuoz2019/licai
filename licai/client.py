from __future__ import annotations

import hashlib
import hmac
import threading
import time
from typing import Any
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


class BinanceClient:
    SPOT = "https://api.binance.com"
    FUTURES = "https://fapi.binance.com"
    PAPI = "https://papi.binance.com"
    _time_lock = threading.Lock()
    _shared_offset_ms = 0
    _shared_offset_at = 0.0

    def __init__(self, api_key: str, api_secret: str, recv_window: int = 5000, timeout: int = 20, proxy: str | None = None):
        self.api_key = api_key
        self.api_secret = api_secret.encode("utf-8")
        self.recv_window = recv_window
        self.timeout = timeout
        self.proxy = (proxy or "").strip() or None
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update({"X-MBX-APIKEY": api_key, "Accept": "application/json"})
        if self.proxy:
            self.session.proxies.update({"http": self.proxy, "https": self.proxy})
        self._time_offset_ms = BinanceClient._shared_offset_ms
        if api_key:
            try:
                self.sync_time()
            except BinanceAPIError:
                self._time_offset_ms = BinanceClient._shared_offset_ms

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
        try:
            response = self.session.request(method, url, params=params, timeout=self.timeout)
        except requests.RequestException as exc:
            raise BinanceAPIError(f"网络错误: {exc}") from exc
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
                raise BinanceAPIError(f"Binance 错误 {data['code']}: {data['msg']}", status=response.status_code, payload=data)
        return data

    @staticmethod
    def _is_timestamp_error(exc: BinanceAPIError) -> bool:
        payload = exc.payload if isinstance(exc.payload, dict) else {}
        return payload.get("code") in (-1021, "-1021")
