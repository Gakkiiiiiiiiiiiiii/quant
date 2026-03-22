from __future__ import annotations

from datetime import date
from decimal import Decimal

from quant_demo.core.enums import OrderSide
from quant_demo.core.events import AccountState, OrderIntent
from quant_demo.risk.rules.cash_check import CashCheckRule


def test_cash_check_rejects_insufficient_cash() -> None:
    rule = CashCheckRule()
    account_state = AccountState(account_id="demo", cash=Decimal("1000"))
    intent = OrderIntent(
        account_id="demo",
        trading_date=date.today(),
        symbol="510300.SH",
        side=OrderSide.BUY,
        qty=1000,
        reference_price=Decimal("3.5"),
    )
    result = rule.evaluate(intent, account_state, {"510300.SH": Decimal("3.5")})
    assert result.passed is False
    assert result.payload["required"] > result.payload["cash"]
