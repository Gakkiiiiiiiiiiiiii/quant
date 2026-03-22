from __future__ import annotations

from decimal import Decimal

from quant_demo.core.enums import OrderSide
from quant_demo.core.events import AccountState, OrderIntent, RuleResult


class PositionLimitRule:
    name = "position_limit"

    def __init__(self, max_position_ratio: float) -> None:
        self.max_position_ratio = max_position_ratio

    def evaluate(self, intent: OrderIntent, account_state: AccountState, prices: dict) -> RuleResult:
        total_asset = account_state.total_asset(prices)
        current_qty = account_state.positions.get(intent.symbol).qty if intent.symbol in account_state.positions else 0
        projected_qty = current_qty + intent.qty if intent.side == OrderSide.BUY else max(0, current_qty - intent.qty)
        projected_value = Decimal(projected_qty) * intent.reference_price
        ratio = float(projected_value / total_asset) if total_asset else 0.0
        passed = ratio <= self.max_position_ratio + 1e-9
        return RuleResult(self.name, passed, "仓位限制通过" if passed else "超过单标的仓位限制", {"ratio": ratio, "limit": self.max_position_ratio})
