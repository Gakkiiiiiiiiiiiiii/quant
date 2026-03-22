from __future__ import annotations

from decimal import Decimal


def clamp_weight(weight: float, max_weight: float) -> float:
    return max(0.0, min(weight, max_weight))


def round_lot(quantity: Decimal, lot_size: int) -> int:
    raw = int(quantity)
    return max(0, raw // lot_size * lot_size)
