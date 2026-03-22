from __future__ import annotations

from decimal import Decimal

import pandas as pd

from quant_demo.core.events import MarketBar


def group_bars_by_date(frame: pd.DataFrame) -> dict:
    grouped: dict = {}
    for trading_date, sub_frame in frame.groupby("trading_date"):
        grouped[trading_date] = [MarketBar.from_row(row) for row in sub_frame.to_dict("records")]
    return grouped


def prices_from_bars(bars: list[MarketBar]) -> dict[str, Decimal]:
    return {bar.symbol: bar.close_price for bar in bars}
