from __future__ import annotations

from datetime import date

from quant_demo.core.events import OrderIntent
from quant_demo.core.enums import IntentSource, OrderSide
from quant_demo.portfolio.rebalancer import RebalanceInstruction


def build_order_intents(account_id: str, trading_date: date, instructions: list[RebalanceInstruction]) -> list[OrderIntent]:
    intents: list[OrderIntent] = []
    for instruction in instructions:
        intents.append(
            OrderIntent(
                account_id=account_id,
                trading_date=trading_date,
                symbol=instruction.symbol,
                side=instruction.side,
                qty=instruction.qty,
                reference_price=instruction.price,
                source=IntentSource.STRATEGY,
                metadata={"target_weight": instruction.target_weight},
            )
        )
    return intents
