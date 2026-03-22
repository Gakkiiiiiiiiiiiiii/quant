from __future__ import annotations

from quant_demo.core.enums import OrderStatus


_ALLOWED_TRANSITIONS = {
    OrderStatus.CREATED: {OrderStatus.SUBMITTED, OrderStatus.REJECTED, OrderStatus.CANCELLED},
    OrderStatus.SUBMITTED: {OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED},
    OrderStatus.PARTIALLY_FILLED: {OrderStatus.FILLED, OrderStatus.CANCELLED},
}


def validate_transition(current: OrderStatus, target: OrderStatus) -> None:
    allowed = _ALLOWED_TRANSITIONS.get(current, set())
    if target not in allowed and current != target:
        raise ValueError(f"非法订单状态迁移: {current} -> {target}")
