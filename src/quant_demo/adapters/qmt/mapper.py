from __future__ import annotations

from decimal import Decimal

from quant_demo.core.enums import OrderSide, OrderStatus
from quant_demo.core.events import OrderRecord


def to_order_record(payload: dict) -> OrderRecord:
    return OrderRecord(
        account_id=payload["account_id"],
        symbol=payload["symbol"],
        side=OrderSide(payload["side"]),
        qty=int(payload["qty"]),
        order_intent_id=payload["order_intent_id"],
        risk_decision_id=payload.get("risk_decision_id"),
        broker_order_id=payload.get("broker_order_id"),
        status=OrderStatus(payload.get("status", "created")),
        filled_qty=int(payload.get("filled_qty", 0)),
        avg_price=Decimal(str(payload["avg_price"])) if payload.get("avg_price") else None,
    )
