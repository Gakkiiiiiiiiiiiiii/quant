from __future__ import annotations

from decimal import Decimal

from quant_demo.core.enums import OrderSide
from quant_demo.core.events import AccountState, OrderIntent, RuleResult


class CashCheckRule:
    name = "cash_check"

    def evaluate(self, intent: OrderIntent, account_state: AccountState, prices: dict) -> RuleResult:
        if intent.side == OrderSide.SELL:
            return RuleResult(self.name, True, "卖出单不检查现金")
        required = intent.reference_price * intent.qty
        passed = account_state.cash >= required
        return RuleResult(self.name, passed, "现金充足" if passed else "现金不足", {"required": float(required), "cash": float(account_state.cash)})
