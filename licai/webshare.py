"""Webshare 代理列表：https://apidocs.webshare.io/proxy-list/list"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from urllib.parse import quote

import requests

API_LIST = "https://proxy.webshare.io/api/v2/proxy/list/"
CHECK_URL = "https://ipv4.webshare.io/"


@dataclass(frozen=True)
class WebshareProxy:
    id: str
    username: str
    password: str
    host: str
    port: int
    valid: bool
    country: str
    city: str

    @property
    def url(self) -> str:
        user = quote(self.username, safe="")
        pwd = quote(self.password, safe="")
        return f"http://{user}:{pwd}@{self.host}:{self.port}"

    @property
    def endpoint(self) -> str:
        return f"{self.host}:{self.port}"


class WebshareError(RuntimeError):
    pass


class WebshareClient:
    def __init__(
        self,
        token: str,
        *,
        mode: str = "direct",
        country: str | None = None,
        timeout: int = 20,
    ):
        self.token = (token or "").strip()
        self.mode = (mode or "direct").strip() or "direct"
        self.country = (country or "").strip().upper() or None
        self.timeout = timeout
        self._lock = threading.Lock()
        self._cache: list[WebshareProxy] | None = None
        self._cache_at = 0.0

    @property
    def configured(self) -> bool:
        return bool(self.token)

    def _headers(self) -> dict[str, str]:
        if not self.token:
            raise WebshareError("未配置 WEBSHARE_API_TOKEN")
        return {"Authorization": f"Token {self.token}"}

    def list_proxies(self, *, force: bool = False, page_size: int = 100) -> list[WebshareProxy]:
        import time

        now = time.monotonic()
        with self._lock:
            if not force and self._cache is not None and now - self._cache_at < 60:
                return list(self._cache)

        out: list[WebshareProxy] = []
        page = 1
        while True:
            params: dict[str, str | int] = {
                "mode": self.mode,
                "page": page,
                "page_size": max(1, min(int(page_size), 100)),
            }
            if self.country:
                params["country_code__in"] = self.country
            if self.mode == "direct":
                params["valid"] = "true"
            try:
                resp = requests.get(
                    API_LIST,
                    params=params,
                    headers=self._headers(),
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                raise WebshareError(f"Webshare 请求失败: {exc}") from exc
            if resp.status_code == 401:
                raise WebshareError("Webshare Token 无效（401）")
            if resp.status_code >= 400:
                raise WebshareError(f"Webshare HTTP {resp.status_code}: {resp.text[:300]}")
            try:
                data = resp.json()
            except ValueError as exc:
                raise WebshareError("Webshare 返回非 JSON") from exc
            for row in data.get("results") or []:
                item = self._parse(row)
                if item is not None:
                    out.append(item)
            if not data.get("next"):
                break
            page += 1
            if page > 50:
                break

        with self._lock:
            self._cache = list(out)
            self._cache_at = time.monotonic()
        return out

    @staticmethod
    def _parse(row: dict) -> WebshareProxy | None:
        host = str(row.get("proxy_address") or "").strip()
        user = str(row.get("username") or "").strip()
        pwd = str(row.get("password") or "").strip()
        try:
            port = int(row.get("port") or 0)
        except (TypeError, ValueError):
            port = 0
        if not host or not user or not pwd or port <= 0:
            return None
        return WebshareProxy(
            id=str(row.get("id") or f"{host}:{port}"),
            username=user,
            password=pwd,
            host=host,
            port=port,
            valid=bool(row.get("valid", True)),
            country=str(row.get("country_code") or ""),
            city=str(row.get("city_name") or ""),
        )

    def pick(
        self,
        *,
        used_urls: set[str] | None = None,
        used_endpoints: set[str] | None = None,
        force: bool = False,
        verify: bool = True,
    ) -> WebshareProxy:
        """选一个未被其它账号占用的可用代理；可选探测 ipv4.webshare.io。"""
        used_urls = {u.strip() for u in (used_urls or set()) if u and u.strip()}
        used_endpoints = {e.strip() for e in (used_endpoints or set()) if e and e.strip()}
        candidates = self.list_proxies(force=force)
        if not candidates:
            raise WebshareError("Webshare 列表为空，检查套餐或 country 过滤")
        preferred = [
            p
            for p in candidates
            if p.valid and p.url not in used_urls and p.endpoint not in used_endpoints
        ]
        if not preferred:
            preferred = [p for p in candidates if p.valid]
        if not preferred:
            preferred = list(candidates)

        last_err: Exception | None = None
        for proxy in preferred:
            if not verify:
                return proxy
            try:
                self.verify(proxy)
                return proxy
            except Exception as exc:
                last_err = exc
                continue
        if last_err:
            raise WebshareError(f"代理均探测失败：{last_err}") from last_err
        raise WebshareError("没有可用代理")

    def verify(self, proxy: WebshareProxy, timeout: float = 8.0) -> str:
        """经该代理访问 Webshare 校验页，返回出口 IP。"""
        try:
            resp = requests.get(
                CHECK_URL,
                proxies={"http": proxy.url, "https": proxy.url},
                timeout=timeout,
            )
            resp.raise_for_status()
            ip = (resp.text or "").strip()
            if not ip:
                raise WebshareError("校验页无出口 IP")
            return ip
        except requests.RequestException as exc:
            raise WebshareError(f"代理探测失败 {proxy.endpoint}: {exc}") from exc


def proxy_endpoint(url: str | None) -> str:
    """从 http://user:pass@host:port 取出 host:port。"""
    text = (url or "").strip()
    if not text:
        return ""
    try:
        from urllib.parse import urlparse

        parsed = urlparse(text if "://" in text else f"http://{text}")
        if parsed.hostname and parsed.port:
            return f"{parsed.hostname}:{parsed.port}"
        if parsed.hostname:
            return parsed.hostname
    except Exception:
        pass
    if "@" in text:
        text = text.rsplit("@", 1)[-1]
    return text.strip().rstrip("/")
