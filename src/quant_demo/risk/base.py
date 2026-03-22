from __future__ import annotations

from typing import Protocol

from quant_demo.core.events import AccountState, OrderIntent, RuleResult


class RiskRule(Protocol):
    name: str

    def evaluate(self, intent: OrderIntent, account_state: AccountState, prices: dict) -> RuleResult:
        ...
