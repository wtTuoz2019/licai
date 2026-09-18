from __future__ import annotations

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
        quote = self.client.signed(
            "POST",
            "/sapi/v1/convert/getQuote",
            {
                "fromAsset": from_asset,
                "toAsset": to_asset,
                "fromAmount": fmt_amount(amount),
                "walletType": "SPOT",
                "validTime": "30s",
            },
        )
        quote_id = quote.get("quoteId")
        if not quote_id:
            raise RuntimeError(f"兑换询价失败: {quote}")
        accepted = self.client.signed("POST", "/sapi/v1/convert/acceptQuote", {"quoteId": quote_id})
        return {"quote": quote, "result": accepted}

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
