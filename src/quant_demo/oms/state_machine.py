from __future__ import annotations

from quant_demo.core.enums import OrderStatus


# 详细修改方案 §15：全量状态迁移表（未列出的目标状态均为非法迁移）。
_ALLOWED_TRANSITIONS = {
    OrderStatus.CREATED: {
        OrderStatus.RISK_REJECTED,
        OrderStatus.APPROVED,
        OrderStatus.SUBMITTED,
        OrderStatus.REJECTED,
        OrderStatus.CANCELLED,
    },
    OrderStatus.RISK_REJECTED: set(),  # 终态
    OrderStatus.APPROVED: {OrderStatus.SUBMITTED, OrderStatus.CANCELLED, OrderStatus.REJECTED},
    OrderStatus.SUBMITTED: {
        OrderStatus.ACKNOWLEDGED,
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
        OrderStatus.CANCEL_PENDING,
        OrderStatus.CANCELLED,
        OrderStatus.REJECTED,
        OrderStatus.UNKNOWN,
    },
    OrderStatus.ACKNOWLEDGED: {
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
        OrderStatus.CANCEL_PENDING,
        OrderStatus.CANCELLED,
        OrderStatus.REJECTED,
        OrderStatus.UNKNOWN,
    },
    OrderStatus.PARTIALLY_FILLED: {
        OrderStatus.FILLED,
        OrderStatus.CANCEL_PENDING,
        OrderStatus.CANCELLED,
        OrderStatus.UNKNOWN,
    },
    OrderStatus.CANCEL_PENDING: {
        OrderStatus.CANCELLED,
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
        OrderStatus.UNKNOWN,
    },
    # UNKNOWN：券商状态失联后的恢复通道，只允许回到已确认状态。
    OrderStatus.UNKNOWN: {
        OrderStatus.ACKNOWLEDGED,
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.REJECTED,
    },
    OrderStatus.FILLED: set(),  # 终态
    OrderStatus.CANCELLED: set(),  # 终态
    OrderStatus.REJECTED: set(),  # 终态
}

TERMINAL_STATES = frozenset(
    {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.RISK_REJECTED}
)


def validate_transition(current: OrderStatus, target: OrderStatus) -> None:
    allowed = _ALLOWED_TRANSITIONS.get(current, set())
    if target not in allowed and current != target:
        raise ValueError(f"非法订单状态迁移: {current} -> {target}")
