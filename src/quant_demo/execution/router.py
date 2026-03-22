from __future__ import annotations

from quant_demo.adapters.qmt.trade_client import SimulatedTradeClient, TradeClient, XtQuantTradeClient
from quant_demo.core.config import AppSettings
from quant_demo.core.enums import Environment


def build_trade_client(app_settings: AppSettings) -> TradeClient:
    if app_settings.environment == Environment.LIVE:
        return XtQuantTradeClient(app_settings)
    return SimulatedTradeClient()
