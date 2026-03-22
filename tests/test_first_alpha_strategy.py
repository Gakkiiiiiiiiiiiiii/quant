from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pandas as pd

from quant_demo.core.events import AccountState, MarketBar
from quant_demo.strategy.base import StrategyContext
from quant_demo.strategy.implementations.first_alpha import FirstAlphaStrategy


def test_first_alpha_strategy_returns_normalized_weights() -> None:
    strategy = FirstAlphaStrategy(lookback_days=6, top_n=2)
    dates = [date(2026, 1, 1) + timedelta(days=index) for index in range(6)]
    rows = []
    for symbol, closes in {
        "AAA.SZ": [10, 10.2, 10.3, 10.5, 10.7, 10.9],
        "BBB.SZ": [10, 9.9, 9.8, 9.7, 9.6, 9.5],
        "CCC.SZ": [10, 10.1, 10.15, 10.2, 10.25, 10.4],
    }.items():
        for trading_date, close in zip(dates, closes):
            rows.append({"trading_date": trading_date, "symbol": symbol, "open": close, "high": close, "low": close, "close": close, "volume": 1_000_000})
    history = pd.DataFrame(rows)
    context = StrategyContext(
        trading_date=dates[-1],
        account_state=AccountState(account_id="demo", cash=Decimal("100000")),
        history=history,
        bars=[MarketBar(trading_date=dates[-1], symbol="AAA.SZ", open_price=Decimal("10.9"), high_price=Decimal("10.9"), low_price=Decimal("10.9"), close_price=Decimal("10.9"), volume=1000000)],
        prices={"AAA.SZ": Decimal("10.9"), "BBB.SZ": Decimal("9.5"), "CCC.SZ": Decimal("10.4")},
    )

    weights = strategy.target_weights(context)

    assert set(weights) <= {"AAA.SZ", "CCC.SZ"}
    assert all(weight > 0 for weight in weights.values())
    assert abs(sum(weights.values()) - 1) < 0.02
