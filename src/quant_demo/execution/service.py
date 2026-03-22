from __future__ import annotations

from decimal import Decimal

from quant_demo.adapters.qmt.trade_client import TradeClient
from quant_demo.core.events import AccountState, OrderRecord, TradeFill


class ExecutionService:
    def __init__(self, trade_client: TradeClient) -> None:
        self.trade_client = trade_client

    def execute(self, order: OrderRecord, market_price: Decimal) -> TradeFill:
        return self.trade_client.submit_order(order, market_price)

    def apply_fill(self, account_state: AccountState, fill: TradeFill) -> None:
        cost = fill.fill_price * fill.fill_qty
        if fill.side.value == "buy":
            account_state.cash -= cost + fill.commission
            position = account_state.positions.get(fill.symbol)
            if position is None:
                from quant_demo.core.events import Position

                account_state.positions[fill.symbol] = Position(
                    symbol=fill.symbol,
                    qty=fill.fill_qty,
                    available_qty=fill.fill_qty,
                    cost_price=fill.fill_price,
                    last_price=fill.fill_price,
                )
            else:
                total_cost = position.cost_price * position.qty + cost
                position.qty += fill.fill_qty
                position.available_qty += fill.fill_qty
                position.cost_price = total_cost / position.qty
                position.last_price = fill.fill_price
        else:
            account_state.cash += cost - fill.commission
            position = account_state.positions[fill.symbol]
            position.qty -= fill.fill_qty
            position.available_qty = max(0, position.available_qty - fill.fill_qty)
            position.last_price = fill.fill_price
            if position.qty == 0:
                del account_state.positions[fill.symbol]
        account_state.turnover += cost
