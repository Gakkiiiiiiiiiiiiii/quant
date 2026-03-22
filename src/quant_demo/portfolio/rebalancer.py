from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from quant_demo.core.enums import OrderSide
from quant_demo.core.events import AccountState
from quant_demo.portfolio.constraints import clamp_weight
from quant_demo.portfolio.sizing import target_quantity


@dataclass(slots=True)
class RebalanceInstruction:
    symbol: str
    side: OrderSide
    qty: int
    price: Decimal
    target_weight: float


class Rebalancer:
    def __init__(self, lot_size: int = 100, max_position_ratio: float = 0.35) -> None:
        self.lot_size = lot_size
        self.max_position_ratio = max_position_ratio

    def build_instructions(
        self,
        account_state: AccountState,
        target_weights: dict[str, float],
        prices: dict[str, Decimal],
    ) -> list[RebalanceInstruction]:
        total_asset = account_state.total_asset(prices)
        instructions: list[RebalanceInstruction] = []
        symbols = set(target_weights) | set(account_state.positions)
        for symbol in sorted(symbols):
            price = prices.get(symbol)
            if price is None or price <= 0:
                continue
            target_weight = clamp_weight(target_weights.get(symbol, 0.0), self.max_position_ratio)
            current_qty = account_state.positions.get(symbol).qty if symbol in account_state.positions else 0
            desired_qty = target_quantity(total_asset, target_weight, price, self.lot_size)
            delta = desired_qty - current_qty
            if delta == 0:
                continue
            side = OrderSide.BUY if delta > 0 else OrderSide.SELL
            instructions.append(
                RebalanceInstruction(
                    symbol=symbol,
                    side=side,
                    qty=abs(delta),
                    price=price,
                    target_weight=target_weight,
                )
            )
        return instructions
