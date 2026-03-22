from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Protocol

import pandas as pd

from quant_demo.core.events import AccountState, MarketBar


@dataclass(slots=True)
class StrategyContext:
    trading_date: date
    account_state: AccountState
    history: pd.DataFrame
    bars: list[MarketBar]
    prices: dict[str, Decimal]


class BaseStrategy(Protocol):
    name: str

    def target_weights(self, context: StrategyContext) -> dict[str, float]:
        ...
