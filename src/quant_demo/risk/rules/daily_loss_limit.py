from __future__ import annotations

from quant_demo.core.events import AccountState, OrderIntent, RuleResult


class DailyLossLimitRule:
    name = "daily_loss_limit"

    def __init__(self, loss_limit: float) -> None:
        self.loss_limit = loss_limit

    def evaluate(self, intent: OrderIntent, account_state: AccountState, prices: dict) -> RuleResult:
        total_asset = float(account_state.total_asset(prices))
        peak = float(account_state.peak_total_asset) if account_state.peak_total_asset else total_asset
        drawdown = (total_asset - peak) / peak if peak else 0.0
        passed = drawdown >= self.loss_limit
        return RuleResult(self.name, passed, "当日亏损限制通过" if passed else "触发当日亏损限制", {"drawdown": drawdown, "limit": self.loss_limit})
