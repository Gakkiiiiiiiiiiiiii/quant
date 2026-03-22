from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.orm import Session

from quant_demo.core.events import OrderIntent, OrderRecord
from quant_demo.db.models import OrderEventModel, OrderIntentModel, OrderModel


class OrdersRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add_intent(self, intent: OrderIntent, strategy_version_id: str | None = None) -> OrderIntentModel:
        model = OrderIntentModel(
            order_intent_id=intent.order_intent_id,
            strategy_version_id=strategy_version_id,
            account_id=intent.account_id,
            trading_date=intent.trading_date,
            symbol=intent.symbol,
            side=intent.side.value,
            qty=intent.qty,
            limit_price=intent.limit_price,
            reference_price=intent.reference_price,
            source=intent.source.value,
            metadata_json=intent.metadata,
        )
        self.session.add(model)
        return model

    def add_order(self, order: OrderRecord) -> OrderModel:
        model = OrderModel(
            order_id=order.order_id,
            order_intent_id=order.order_intent_id,
            risk_decision_id=order.risk_decision_id,
            broker_order_id=order.broker_order_id,
            status=order.status.value,
            account_id=order.account_id,
            symbol=order.symbol,
            side=order.side.value,
            qty=order.qty,
            filled_qty=order.filled_qty,
            avg_price=order.avg_price,
            updated_at=datetime.utcnow(),
        )
        self.session.add(model)
        return model

    def add_order_event(self, order_id: str, status: str, source: str, payload: dict) -> None:
        self.session.add(
            OrderEventModel(
                order_id=order_id,
                status=status,
                source=source,
                payload=payload,
            )
        )

    def mark_filled(self, order_id: str, filled_qty: int, avg_price: Decimal) -> None:
        model = self.session.get(OrderModel, order_id)
        if model is None:
            return
        model.filled_qty = filled_qty
        model.avg_price = avg_price
        model.status = "filled"
        model.updated_at = datetime.utcnow()

    def list_orders(self) -> list[OrderModel]:
        return list(self.session.scalars(select(OrderModel).order_by(OrderModel.created_at)))
