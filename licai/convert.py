from __future__ import annotations

import time
from decimal import Decimal, ROUND_DOWN

from .client import BinanceClient
from .config import fmt_amount


class ConvertAPI:
    def __init__(self, client: BinanceClient):
        self.client = client

    def convert(self, from_asset: str, to_asset: str, from_amount: Decimal) -> dict:
        from_asset = from_asset.upper()
        to_asset = to_asset.upper()
        if from_asset == to_asset:
            return {"skipped": True, "fromAsset": from_asset, "toAsset": to_asset, "fromAmount": fmt_amount(from_amount)}
        amount = self._truncate(from_asset, from_amount)
        quote = self._get_quote(from_asset, to_asset, amount)
        quote_id = str((quote or {}).get("quoteId") or "").strip()
        if not quote_id:
            raise RuntimeError(f"兑换询价失败: {quote}")
        accepted = self.client.signed("POST", "/sapi/v1/convert/acceptQuote", {"quoteId": quote_id})
        return {"quote": quote, "result": accepted}

    def _get_quote(self, from_asset: str, to_asset: str, amount: Decimal) -> dict:
        """币安偶发返回比率但无 quoteId，短重试后再失败。"""
        amount_text = fmt_amount(amount)
        last: dict | list | str | None = None
        for attempt in range(4):
            params_list = [
                {
                    "fromAsset": from_asset,
                    "toAsset": to_asset,
                    "fromAmount": amount_text,
                    "walletType": "SPOT",
                    "validTime": "30s",
                },
            ]
            # 第二次起再试去掉 walletType，兼容偶发 SPOT 钱包未就绪
            if attempt > 0:
                params_list.append(
                    {
                        "fromAsset": from_asset,
                        "toAsset": to_asset,
                        "fromAmount": amount_text,
                        "validTime": "30s",
                    }
                )
            for params in params_list:
                quote = self.client.signed("POST", "/sapi/v1/convert/getQuote", params)
                last = quote
                if isinstance(quote, dict) and str(quote.get("quoteId") or "").strip():
                    return quote
            time.sleep(0.45 * (attempt + 1))
        return last if isinstance(last, dict) else {"raw": last}

    def _truncate(self, asset: str, amount: Decimal) -> Decimal:
        try:
            info = self.client.signed("GET", "/sapi/v1/convert/assetInfo")
        except Exception:
            return amount.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
        decimals = 8
        rows = info if isinstance(info, list) else []
        for row in rows:
            if str(row.get("asset", "")).upper() == asset.upper():
                decimals = int(row.get("fraction") or 8)
                break
        quant = Decimal("1").scaleb(-decimals)
        return amount.quantize(quant, rounding=ROUND_DOWN)
