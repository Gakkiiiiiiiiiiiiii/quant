from __future__ import annotations

from decimal import Decimal

from quant_demo.portfolio.constraints import round_lot


def target_quantity(total_asset: Decimal, target_weight: float, price: Decimal, lot_size: int) -> int:
    if price <= 0:
        return 0
    target_value = total_asset * Decimal(str(target_weight))
    quantity = target_value / price
    return round_lot(quantity, lot_size)
