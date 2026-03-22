from __future__ import annotations

from sqlalchemy.orm import Session

from quant_demo.core.enums import OrderStatus
from quant_demo.core.events import OrderIntent, OrderRecord, RiskDecision
from quant_demo.db.repositories.orders import OrdersRepository
from quant_demo.db.repositories.risk import RiskRepository
from quant_demo.oms.state_machine import validate_transition


class OmsService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.orders_repo = OrdersRepository(session)
        self.risk_repo = RiskRepository(session)

    def register_intent(self, intent: OrderIntent, decision: RiskDecision, strategy_version_id: str | None = None) -> None:
        self.orders_repo.add_intent(intent, strategy_version_id=strategy_version_id)
        self.risk_repo.add_decision(decision)
        self.session.flush()

    def create_order(self, intent: OrderIntent, decision: RiskDecision) -> OrderRecord:
        order = OrderRecord(
            account_id=intent.account_id,
            symbol=intent.symbol,
            side=intent.side,
            qty=intent.qty,
            order_intent_id=intent.order_intent_id,
            risk_decision_id=decision.risk_decision_id,
        )
        self.orders_repo.add_order(order)
        self.session.flush()
        self.orders_repo.add_order_event(order.order_id, OrderStatus.CREATED.value, "oms", {"intent_id": intent.order_intent_id})
        return order

    def submit_order(self, order: OrderRecord) -> None:
        validate_transition(order.status, OrderStatus.SUBMITTED)
        order.status = OrderStatus.SUBMITTED
        self.orders_repo.add_order_event(order.order_id, order.status.value, "execution", {})

    def fill_order(self, order: OrderRecord, filled_qty: int, avg_price) -> None:
        validate_transition(order.status, OrderStatus.FILLED)
        order.status = OrderStatus.FILLED
        order.filled_qty = filled_qty
        order.avg_price = avg_price
        self.orders_repo.mark_filled(order.order_id, filled_qty, avg_price)
        self.orders_repo.add_order_event(order.order_id, order.status.value, "execution", {"avg_price": float(avg_price)})
