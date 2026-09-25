from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import os

import yaml
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent.parent

DEFAULT_HEDGE_SYMBOLS = ["ETHUSDT", "BTCUSDT", "ETHUSDC", "BTCUSDC", "SOLUSDT", "BNBUSDT"]


def normalize_hedge_symbol(value: str, allowed: list[str] | None = None) -> str:
    raw = (value or "").upper().replace("/", "").replace("-", "").replace("_", "").replace(" ", "")
    allowed = allowed or DEFAULT_HEDGE_SYMBOLS
    if raw not in allowed:
        raise ValueError(f"对冲币对只支持 {', '.join(allowed)}，当前是 {value or '(空)'}")
    return raw


def settle_asset_of(symbol: str) -> str:
    """合约结算币：ETHUSDC -> USDC，ETHUSDT -> USDT。"""
    name = (symbol or "").upper().replace("/", "").replace("-", "").replace("_", "")
    for quote in ("USDC", "FDUSD", "BUSD", "USDT"):
        if name.endswith(quote):
            return quote
    return "USDT"


def d(value: object, default: str = "0") -> Decimal:
    if value is None or value == "":
        return Decimal(default)
    return Decimal(str(value))


UNI_MMR_SENTINEL = Decimal("999999")


def is_mmr_sentinel(mmr: Decimal) -> bool:
    return mmr <= 0 or mmr >= UNI_MMR_SENTINEL


def load_uni_mmr_floor(raw: dict, key: str, default: str, *legacy: str) -> Decimal:
    # 币安 uniMMR = 权益 / 维持保证金，越大越安全，爆仓约 1.05。
    # 旧配置把 0.4/0.5 当成「越大越危险」，小于 1 的值一律改回默认安全线。
    val = None
    if key in raw:
        val = d(raw.get(key))
    else:
        for old in legacy:
            if old in raw:
                val = d(raw.get(old))
                break
    if val is None:
        return d(default)
    if val < Decimal("1"):
        return d(default)
    return val


def fmt_amount(value: Decimal, places: int | None = None) -> str:
    if places is not None:
        q = Decimal(10) ** -places
        value = value.quantize(q)
    normalized = value.quantize(Decimal("0.00000001")) if value != value.to_integral_value() else value
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


@dataclass
class Settings:
    api_key: str
    api_secret: str
    dry_run: bool
    source_asset: str = "USDT"
    amount: Decimal = Decimal("20")
    max_amount: Decimal = Decimal("50")
    source_account: str = "SPOT"
    margin_asset: str = "BFUSD"
    margin_asset_fallbacks: list[str] = field(default_factory=lambda: ["USDT", "USDC", "FDUSD"])
    unified_account: bool = True
    transfer_to_futures: bool = False
    enable_multi_assets: bool = False
    prefer_margin_assets: bool = True
    keep_earn: bool = True
    min_apr: Decimal = Decimal("0")
    max_apr: Decimal = Decimal("200")
    asset_allowlist: list[str] = field(default_factory=lambda: ["USDT", "BFUSD"])
    asset_denylist: list[str] = field(default_factory=list)
    margin_earn_assets: list[str] = field(default_factory=lambda: ["USDT", "BFUSD"])
    settle_seconds: int = 3
    recv_window: int = 5000
    hedge_symbol: str = "ETHUSDT"
    hedge_symbols: list[str] = field(default_factory=lambda: list(DEFAULT_HEDGE_SYMBOLS))
    hedge_leverage: int = 100
    hedge_leverage_target: int = 100
    leverage_cap: int | None = None
    leverage_unlock_at: str | None = None
    hedge_qty: Decimal = Decimal("0")
    hedge_margin_use_pct: Decimal = Decimal("0.85")
    take_profit_usdt: Decimal = Decimal("0")
    take_profit_custom: bool = False
    maker_only: bool = False
    quote_refresh_seconds: float = 0.5
    hedge_quote_retries: int = 8
    maker_improve_bps: Decimal = Decimal("1")
    maker_passive_bps: Decimal = Decimal("2")
    maker_passive_ticks: int = 2
    hedge_max_wait_seconds: float = 0.8
    hedge_max_slippage_bps: Decimal = Decimal("3.5")
    watch_seconds: int = 60
    min_free_usdt_keep: Decimal = Decimal("0")
    cycle_sweep_all: bool = True
    min_uni_mmr: Decimal = Decimal("2")
    scale_min_add_pct: Decimal = Decimal("0.03")
    proxy: str | None = None
    maker_fee_rate: Decimal = Decimal("0.0002")
    taker_fee_rate: Decimal = Decimal("0.0005")
    min_profit_fee_multiple: Decimal = Decimal("8")
    harvest_principal_pct: Decimal = Decimal("0.01")
    harvest_pos_pct: Decimal = Decimal("0.03")
    harvest_sqrt_coeff: Decimal = Decimal("0.5")
    harvest_min_usdt: Decimal = Decimal("1")
    live_poll_seconds: int = 120
    auto_harvest_seconds: int = 60
    macd_indicator_url: str = "https://c7.pro/capi/indicator"
    macd_auto_harvest: bool = True
    cooldown_minutes: int = 15
    stable_range_pct: Decimal = Decimal("0.0025")
    stable_spread_pct: Decimal = Decimal("0.00025")
    # 收利专用更宽平稳（入场仍用上面更严的）；默认约 0.6% / 0.06%
    harvest_stable_range_pct: Decimal = Decimal("0.006")
    harvest_stable_spread_pct: Decimal = Decimal("0.0006")
    harvest_stable_wait_seconds: int = 90
    switch_safe_uni_mmr: Decimal = Decimal("2.5")
    switch_batch_pct: Decimal = Decimal("0.15")
    switch_batch_usdt: Decimal = Decimal("200")
    switch_min_batch: Decimal = Decimal("10")
    switch_max_batches: int = 5
    webshare_api_token: str = ""
    webshare_mode: str = "direct"
    webshare_country: str = ""
    webshare_auto_assign: bool = True

    @property
    def target_margin_assets(self) -> list[str]:
        ordered = [self.margin_asset, *self.margin_asset_fallbacks]
        seen: list[str] = []
        for asset in ordered:
            name = asset.upper()
            if name and name not in seen:
                seen.append(name)
        return seen


def load_settings(config_path: str | Path | None = None, dry_run_override: bool | None = None) -> Settings:
    load_dotenv(ROOT / ".env")
    path = Path(config_path) if config_path else ROOT / "config.yaml"
    raw: dict = {}
    if path.exists():
        loaded = yaml.safe_load(path.read_text()) or {}
        if not isinstance(loaded, dict):
            raise ValueError(f"配置文件格式错误: {path}")
        raw = loaded

    dry_run_env = os.getenv("LICAI_DRY_RUN", "true").strip().lower() in {"1", "true", "yes", "on"}
    dry_run = dry_run_env if dry_run_override is None else dry_run_override

    unified_account = bool(raw.get("unified_account", True))
    keep_earn = True if unified_account else bool(raw.get("keep_earn", False))
    transfer_to_futures = False if unified_account else bool(raw.get("transfer_to_futures", True))
    enable_multi_assets = False if unified_account else bool(raw.get("enable_multi_assets", True))
    allowed = []
    for item in raw.get("hedge_symbols") or DEFAULT_HEDGE_SYMBOLS:
        symbol = str(item).upper().replace("/", "").replace("-", "").replace("_", "")
        if symbol and symbol not in allowed:
            allowed.append(symbol)
    if not allowed:
        allowed = list(DEFAULT_HEDGE_SYMBOLS)

    return Settings(
        api_key=os.getenv("BINANCE_API_KEY", "").strip(),
        api_secret=os.getenv("BINANCE_API_SECRET", "").strip(),
        dry_run=dry_run,
        source_asset=str(raw.get("source_asset", "USDT")).upper(),
        amount=d(raw.get("amount", "20")),
        max_amount=d(raw.get("max_amount", "50")),
        source_account=str(raw.get("source_account", "SPOT")).upper(),
        margin_asset=str(raw.get("margin_asset", "BFUSD")).upper(),
        margin_asset_fallbacks=[str(x).upper() for x in (raw.get("margin_asset_fallbacks") or ["USDT", "USDC", "FDUSD"])],
        unified_account=unified_account,
        transfer_to_futures=transfer_to_futures,
        enable_multi_assets=enable_multi_assets,
        prefer_margin_assets=True,
        keep_earn=keep_earn,
        min_apr=d(raw.get("min_apr", "0")),
        max_apr=d(raw.get("max_apr", "200")),
        asset_allowlist=[str(x).upper() for x in (raw.get("asset_allowlist") or raw.get("margin_earn_assets") or ["USDT", "BFUSD"])],
        asset_denylist=[str(x).upper() for x in (raw.get("asset_denylist") or [])],
        margin_earn_assets=[str(x).upper() for x in (raw.get("margin_earn_assets") or ["USDT", "BFUSD"])],
        settle_seconds=int(raw.get("settle_seconds", 3)),
        hedge_symbol=normalize_hedge_symbol(str(raw.get("hedge_symbol", "ETHUSDT")), allowed),
        hedge_symbols=allowed,
        hedge_leverage=int(raw.get("hedge_leverage", 100)),
        hedge_leverage_target=int(raw.get("hedge_leverage", 100)),
        hedge_qty=d(raw.get("hedge_qty", "0")),
        hedge_margin_use_pct=d(raw.get("hedge_margin_use_pct", "0.85")),
        take_profit_usdt=d(raw.get("take_profit_usdt", "0")),
        take_profit_custom=d(raw.get("take_profit_usdt", "0")) > 0,
        maker_only=bool(raw.get("maker_only", False)),
        quote_refresh_seconds=float(raw.get("quote_refresh_seconds", 0.5)),
        hedge_quote_retries=int(raw.get("hedge_quote_retries", 8)),
        maker_improve_bps=d(raw.get("maker_improve_bps", "1")),
        maker_passive_bps=d(raw.get("maker_passive_bps", raw.get("maker_improve_bps", "2"))),
        maker_passive_ticks=int(raw.get("maker_passive_ticks", 2)),
        hedge_max_wait_seconds=float(raw.get("hedge_max_wait_seconds", 0.8)),
        hedge_max_slippage_bps=d(raw.get("hedge_max_slippage_bps", "3.5")),
        watch_seconds=int(raw.get("watch_seconds", raw.get("live_poll_seconds", 60))),
        min_free_usdt_keep=d(raw.get("min_free_usdt_keep", "0")),
        cycle_sweep_all=bool(raw.get("cycle_sweep_all", True)),
        min_uni_mmr=load_uni_mmr_floor(raw, "min_uni_mmr", "2", "max_uni_mmr"),
        scale_min_add_pct=d(raw.get("scale_min_add_pct", "0.03")),
        proxy=(str(raw.get("proxy") or "").strip() or None),
        maker_fee_rate=d(raw.get("maker_fee_rate", "0.0002")),
        taker_fee_rate=d(raw.get("taker_fee_rate", "0.0005")),
        min_profit_fee_multiple=d(raw.get("min_profit_fee_multiple", "8")),
        harvest_principal_pct=d(raw.get("harvest_principal_pct", "0.01")),
        harvest_pos_pct=d(raw.get("harvest_pos_pct", "0.03")),
        harvest_sqrt_coeff=d(raw.get("harvest_sqrt_coeff", "0.5")),
        harvest_min_usdt=d(raw.get("harvest_min_usdt", "1")),
        live_poll_seconds=int(raw.get("live_poll_seconds", 120)),
        auto_harvest_seconds=int(raw.get("auto_harvest_seconds", 60)),
        macd_indicator_url=str(raw.get("macd_indicator_url") or "https://c7.pro/capi/indicator").strip(),
        macd_auto_harvest=bool(raw.get("macd_auto_harvest", True)),
        cooldown_minutes=int(raw.get("cooldown_minutes", 15)),
        stable_range_pct=d(raw.get("stable_range_pct", "0.0025")),
        stable_spread_pct=d(raw.get("stable_spread_pct", "0.00025")),
        harvest_stable_range_pct=d(raw.get("harvest_stable_range_pct", "0.006")),
        harvest_stable_spread_pct=d(raw.get("harvest_stable_spread_pct", "0.0006")),
        harvest_stable_wait_seconds=int(raw.get("harvest_stable_wait_seconds", 90)),
        switch_safe_uni_mmr=load_uni_mmr_floor(raw, "switch_safe_uni_mmr", "2.5"),
        switch_batch_pct=d(raw.get("switch_batch_pct", "0.15")),
        switch_batch_usdt=d(raw.get("switch_batch_usdt", "200")),
        switch_min_batch=d(raw.get("switch_min_batch", "10")),
        switch_max_batches=int(raw.get("switch_max_batches", 5)),
        webshare_api_token=(
            os.getenv("WEBSHARE_API_TOKEN", "").strip()
            or str(raw.get("webshare_api_token") or "").strip()
        ),
        webshare_mode=str(raw.get("webshare_mode") or os.getenv("WEBSHARE_MODE") or "direct").strip() or "direct",
        webshare_country=(
            str(raw.get("webshare_country") or os.getenv("WEBSHARE_COUNTRY") or "").strip().upper()
        ),
        webshare_auto_assign=bool(
            raw.get(
                "webshare_auto_assign",
                os.getenv("WEBSHARE_AUTO_ASSIGN", "true").strip().lower() in {"1", "true", "yes", "on"},
            )
        ),
    )


def settings_for_account(
    base: Settings,
    *,
    api_key: str,
    api_secret: str,
    proxy: str | None,
    dry_run: bool,
    hedge_symbol: str | None = None,
    hedge_leverage: int | None = None,
    leverage_cap: int | None = None,
    leverage_unlock_at: str | None = None,
    take_profit_usdt: float | None = None,
    take_profit_custom: bool = False,
) -> Settings:
    symbol = normalize_hedge_symbol(hedge_symbol or base.hedge_symbol, base.hedge_symbols)
    target = int(hedge_leverage or base.hedge_leverage_target or base.hedge_leverage)
    if target < 1:
        target = int(base.hedge_leverage_target or base.hedge_leverage or 1)
    effective = target
    # 目标倍数用于尝试上调；有仓时算仓以交易所实际杠杆为准（ops/cycle 会覆盖）。
    # 仅在交易所明确限时（有未到期的解禁时间）时，预览按上限压一档。
    cap = int(leverage_cap) if leverage_cap else 0
    unlock = _unlock_dt(leverage_unlock_at)
    if cap > 0 and unlock is not None and datetime.now(timezone.utc) < unlock:
        effective = min(target, cap)
    custom_tp = Decimal("0")
    if take_profit_custom and take_profit_usdt:
        custom_tp = d(take_profit_usdt)
    return replace(
        base,
        api_key=api_key,
        api_secret=api_secret,
        proxy=(proxy or "").strip() or None,
        dry_run=dry_run,
        hedge_symbol=symbol,
        hedge_leverage=effective,
        hedge_leverage_target=target,
        leverage_cap=cap or None,
        leverage_unlock_at=leverage_unlock_at,
        take_profit_usdt=custom_tp if custom_tp > 0 else Decimal("0"),
        take_profit_custom=bool(take_profit_custom and custom_tp > 0),
    )


def _unlock_dt(value: str | None):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed

