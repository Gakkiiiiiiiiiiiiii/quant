from __future__ import annotations

from dataclasses import asdict

from sqlalchemy import select
from sqlalchemy.orm import Session

from quant_demo.core.events import RiskDecision
from quant_demo.db.models import RiskDecisionModel


class RiskRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add_decision(self, decision: RiskDecision) -> RiskDecisionModel:
        model = RiskDecisionModel(
            risk_decision_id=decision.risk_decision_id,
            order_intent_id=decision.order_intent_id,
            status=decision.status.value,
            rule_results=[asdict(result) for result in decision.rule_results],
        )
        self.session.add(model)
        return model

    def list_decisions(self) -> list[RiskDecisionModel]:
        return list(self.session.scalars(select(RiskDecisionModel).order_by(RiskDecisionModel.decided_at)))
