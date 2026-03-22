from __future__ import annotations

from quant_demo.adapters.qmt.quote_client import LocalParquetQuoteClient, QuoteClient, XtQuantQuoteClient
from quant_demo.adapters.qmt.trade_client import SimulatedTradeClient, TradeClient, XtQuantTradeClient
from quant_demo.core.config import AppSettings
from quant_demo.core.enums import Environment


class QmtGateway:
    def __init__(self, quote_client: QuoteClient, trade_client: TradeClient) -> None:
        self.quote_client = quote_client
        self.trade_client = trade_client


def _build_quote_client(app_settings: AppSettings) -> QuoteClient:
    if app_settings.environment == Environment.LIVE or app_settings.history_source.lower() == "qmt":
        return XtQuantQuoteClient(app_settings)
    return LocalParquetQuoteClient()


def create_gateway(app_settings: AppSettings) -> QmtGateway:
    quote_client = _build_quote_client(app_settings)
    if app_settings.environment == Environment.LIVE:
        return QmtGateway(quote_client=quote_client, trade_client=XtQuantTradeClient(app_settings))
    return QmtGateway(quote_client=quote_client, trade_client=SimulatedTradeClient())
