from __future__ import annotations

from datetime import datetime, time

from quant_demo.core.enums import Environment
from quant_demo.core.events import AccountState, OrderIntent, RuleResult


class TradingWindowRule:
    name = "trading_window"

    def __init__(self, environment: Environment, start: str, end: str) -> None:
        self.environment = environment
        self.start = time.fromisoformat(start)
        self.end = time.fromisoformat(end)

    def evaluate(self, intent: OrderIntent, account_state: AccountState, prices: dict) -> RuleResult:
        if self.environment == Environment.BACKTEST:
            return RuleResult(self.name, True, "回测环境放行")
        now = datetime.now().time()
        passed = self.start <= now <= self.end
        return RuleResult(self.name, passed, "交易时间窗口通过" if passed else "不在允许交易时间窗口", {"now": now.isoformat()})
