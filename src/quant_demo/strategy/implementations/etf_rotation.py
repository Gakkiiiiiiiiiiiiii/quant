from __future__ import annotations

import pandas as pd

from quant_demo.strategy.base import StrategyContext


class EtfRotationStrategy:
    name = "etf_rotation"

    def __init__(self, lookback_days: int = 20, top_n: int = 2) -> None:
        self.lookback_days = lookback_days
        self.top_n = top_n

    def target_weights(self, context: StrategyContext) -> dict[str, float]:
        history = context.history[context.history["trading_date"] <= context.trading_date]
        if history.empty:
            return {}
        scores: list[tuple[str, float]] = []
        for symbol, frame in history.groupby("symbol"):
            ordered = frame.sort_values("trading_date")
            closes = ordered["close"].tail(self.lookback_days + 1)
            if len(closes) < 2:
                continue
            score = float(closes.iloc[-1] / closes.iloc[0] - 1)
            scores.append((symbol, score))
        scores.sort(key=lambda item: item[1], reverse=True)
        selected = [symbol for symbol, score in scores[: self.top_n] if score > 0]
        if not selected:
            return {}
        weight = round(1 / len(selected), 4)
        return {symbol: weight for symbol in selected}
