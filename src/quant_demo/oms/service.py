from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from quant_demo.core.enums import OrderStatus
from quant_demo.core.events import OrderIntent, OrderRecord, RiskDecision
from quant_demo.db.models import OrderModel
from quant_demo.db.repositories.orders import OrdersRepository
from quant_demo.db.repositories.risk import RiskRepository
from quant_demo.oms.kill_switch import KillSwitchManager
from quant_demo.oms.state_machine import validate_transition

# 详细修改方案 §15：任何下单必须具备的身份字段。
REQUIRED_ORDER_FIELDS = ("client_order_id", "idempotency_key", "decision_id", "portfolio_id")


class OrderBlockedError(RuntimeError):
    """下单被阻断（Kill Switch / 必备字段缺失）。"""


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

    def place_order(
        self,
        intent: OrderIntent,
        decision: RiskDecision,
        *,
        client_order_id: str,
        idempotency_key: str,
        decision_id: str,
        portfolio_id: str,
        kill_switch: KillSwitchManager | None = None,
        strategy_id: str | None = None,
    ) -> tuple[OrderRecord, bool]:
        """详细修改方案 §15/§17：受控下单入口。

        强制 client_order_id/idempotency_key/decision_id/portfolio_id；
        Kill Switch 命中即阻断；同 idempotency_key 重试幂等返回既有订单。
        返回 (order, reused)。
        """
        identity = {
            "client_order_id": client_order_id,
            "idempotency_key": idempotency_key,
            "decision_id": decision_id,
            "portfolio_id": portfolio_id,
        }
        missing = [field for field, value in identity.items() if not value]
        if missing:
            raise OrderBlockedError(f"下单缺少必备身份字段: {', '.join(missing)}")
        if kill_switch is not None:
            blocked, scope = kill_switch.is_blocked(intent.account_id, strategy_id)
            if blocked:
                raise OrderBlockedError(f"KILL_SWITCH 熔断中，拒绝新订单 (scope={scope})")
        existing = self.session.scalar(select(OrderModel).where(OrderModel.idempotency_key == idempotency_key))
        if existing is not None:
            reused = OrderRecord(
                account_id=existing.account_id,
                symbol=existing.symbol,
                side=existing.side,
                qty=existing.qty,
                order_intent_id=existing.order_intent_id,
                risk_decision_id=existing.risk_decision_id,
                broker_order_id=existing.broker_order_id,
                status=OrderStatus(existing.status),
                filled_qty=existing.filled_qty,
                avg_price=existing.avg_price,
                client_order_id=existing.client_order_id,
                idempotency_key=existing.idempotency_key,
                decision_id=existing.decision_id,
                portfolio_id=existing.portfolio_id,
                order_id=existing.order_id,
            )
            return reused, True
        order = OrderRecord(
            account_id=intent.account_id,
            symbol=intent.symbol,
            side=intent.side,
            qty=intent.qty,
            order_intent_id=intent.order_intent_id,
            risk_decision_id=decision.risk_decision_id,
            client_order_id=client_order_id,
            idempotency_key=idempotency_key,
            decision_id=decision_id,
            portfolio_id=portfolio_id,
        )
        self.orders_repo.add_order(order)
        self.session.flush()
        self.orders_repo.add_order_event(
            order.order_id, OrderStatus.CREATED.value, "oms", {"intent_id": intent.order_intent_id, "decision_id": decision_id}
        )
        return order, False

    def transition(self, order: OrderRecord, target: OrderStatus, source: str = "execution", payload: dict | None = None) -> None:
        """受状态机约束的通用迁移入口（§15）。"""
        validate_transition(order.status, target)
        order.status = target
        model = self.session.get(OrderModel, order.order_id)
        if model is not None:
            model.status = target.value
        self.orders_repo.add_order_event(order.order_id, target.value, source, payload or {})

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
