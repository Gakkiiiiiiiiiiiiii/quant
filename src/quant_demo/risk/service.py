from __future__ import annotations

from quant_demo.core.enums import RiskStatus
from quant_demo.core.events import AccountState, OrderIntent, RiskDecision
from quant_demo.risk.base import RiskRule


class RiskService:
    def __init__(self, rules: list[RiskRule]) -> None:
        self.rules = rules

    def evaluate(self, intent: OrderIntent, account_state: AccountState, prices: dict) -> RiskDecision:
        results = [rule.evaluate(intent, account_state, prices) for rule in self.rules]
        status = RiskStatus.APPROVED if all(result.passed for result in results) else RiskStatus.REJECTED
        return RiskDecision(order_intent_id=intent.order_intent_id, status=status, rule_results=results)
