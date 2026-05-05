from __future__ import annotations

from typing import Any, Protocol

from quant_demo.adapters.qmt.bridge_client import QmtBridgeClient
from quant_demo.adapters.qmt.quote_client import LocalParquetQuoteClient, QuoteClient, XtQuantQuoteClient
from quant_demo.adapters.qmt.trade_client import SimulatedTradeClient, TradeClient, XtQuantTradeClient
from quant_demo.core.config import AppSettings
from quant_demo.core.enums import Environment


class QmtBrokerClient(Protocol):
    def healthcheck(self) -> dict[str, Any]: ...
    def get_quotes(self, symbols: list[str]) -> dict[str, Any]: ...
    def get_latest_prices(self, symbols: list[str]) -> dict[str, Any]: ...
    def get_instrument_details(self, symbols: list[str]) -> dict[str, Any]: ...
    def get_sector_symbols(self, sector_name: str, only_a_share: bool = True, limit: int = 0) -> list[str]: ...
    def get_industry_map(self, symbols: list[str], sector_prefix: str = "GICS2", only_a_share: bool = True) -> list[dict[str, Any]]: ...
    def get_history(
        self,
        symbols: list[str],
        period: str,
        start_time: str,
        end_time: str,
        dividend_type: str,
        fill_data: bool,
    ) -> Any: ...
    def get_financial_data(
        self,
        symbols: list[str],
        tables: list[str],
        start_time: str,
        end_time: str,
        report_type: str = "announce_time",
    ) -> dict[str, Any]: ...
    def get_account_snapshot(self) -> dict[str, Any]: ...
    def get_order_status(self, order_id: str | int) -> dict[str, Any]: ...
    def cancel_order(self, order_id: str | int) -> dict[str, Any]: ...
    def submit_order(
        self,
        symbol: str,
        side: str,
        qty: int,
        price: Any,
        *,
        strategy_name: str = "quant-demo-live",
        order_remark: str = "quant-demo",
    ) -> dict[str, Any]: ...


class QmtGateway:
    def __init__(self, quote_client: QuoteClient, trade_client: TradeClient) -> None:
        self.quote_client = quote_client
        self.trade_client = trade_client


def create_bridge_client(app_settings: AppSettings) -> QmtBrokerClient:
    return QmtBridgeClient(app_settings)


def _build_quote_client(app_settings: AppSettings, bridge: QmtBrokerClient | None = None) -> QuoteClient:
    if app_settings.environment == Environment.LIVE or app_settings.history_source.lower() == "qmt":
        return XtQuantQuoteClient(app_settings, bridge=bridge)
    return LocalParquetQuoteClient()


def create_gateway(app_settings: AppSettings) -> QmtGateway:
    bridge = create_bridge_client(app_settings) if (app_settings.environment == Environment.LIVE or app_settings.history_source.lower() == "qmt") else None
    quote_client = _build_quote_client(app_settings, bridge=bridge)
    if app_settings.environment == Environment.LIVE:
        return QmtGateway(quote_client=quote_client, trade_client=XtQuantTradeClient(app_settings, bridge=bridge))
    return QmtGateway(quote_client=quote_client, trade_client=SimulatedTradeClient())
