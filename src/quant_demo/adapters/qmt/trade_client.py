from __future__ import annotations

from decimal import Decimal
from typing import Any

from quant_demo.adapters.qmt.bridge_client import QmtBridgeClient
from quant_demo.core.config import AppSettings
from quant_demo.core.events import OrderRecord, TradeFill
from quant_demo.core.exceptions import QmtUnavailableError


class TradeClient:
    def submit_order(self, order: OrderRecord, market_price: Decimal) -> TradeFill:
        raise NotImplementedError

    def get_account_snapshot(self) -> dict[str, Any]:
        raise NotImplementedError


class SimulatedTradeClient(TradeClient):
    def __init__(self, commission_rate: Decimal = Decimal("0.0003")) -> None:
        self.commission_rate = commission_rate

    def submit_order(self, order: OrderRecord, market_price: Decimal) -> TradeFill:
        commission = Decimal(order.qty) * market_price * self.commission_rate
        return TradeFill(
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            fill_qty=order.qty,
            fill_price=market_price,
            commission=commission.quantize(Decimal("0.0001")),
        )

    def get_account_snapshot(self) -> dict[str, Any]:
        return {
            "mode": "simulated",
            "asset": {"cash": None, "total_asset": None},
            "positions": [],
            "orders": [],
            "trades": [],
        }


class XtQuantTradeClient(TradeClient):
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.bridge = QmtBridgeClient(settings)

    def submit_order(self, order: OrderRecord, market_price: Decimal) -> TradeFill:
        if not self.settings.qmt_trade_enabled:
            raise QmtUnavailableError("live 配置未开启 qmt_trade_enabled，当前仅允许程序联调与只读探测")
        result = self.bridge.submit_order(order.symbol, order.side.value, order.qty, market_price)
        broker_order_id = result.get("order_id")
        if not broker_order_id:
            raise QmtUnavailableError(f"QMT 委托未返回订单号: {result}")
        raise QmtUnavailableError(
            "QMT 已返回真实委托编号，但当前执行引擎仍按同步成交建模。"
            "如需实盘自动成交回写，需要把 OMS 改为异步回报驱动。"
        )

    def get_account_snapshot(self) -> dict[str, Any]:
        return self.bridge.get_account_snapshot()
