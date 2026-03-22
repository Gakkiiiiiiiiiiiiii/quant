from __future__ import annotations

from quant_demo.strategy.base import StrategyContext


class StockRankingStrategy:
    name = "stock_ranking"

    def __init__(self, lookback_days: int = 15, top_n: int = 3) -> None:
        self.lookback_days = lookback_days
        self.top_n = top_n

    def target_weights(self, context: StrategyContext) -> dict[str, float]:
        history = context.history[context.history["trading_date"] <= context.trading_date]
        scores: list[tuple[str, float]] = []
        for symbol, frame in history.groupby("symbol"):
            ordered = frame.sort_values("trading_date")
            window = ordered.tail(self.lookback_days)
            if window.empty:
                continue
            ret = float(window["close"].iloc[-1] / window["close"].iloc[0] - 1)
            liquidity = float(window["volume"].mean()) / 1_000_000
            score = ret * 0.7 + liquidity * 0.3
            scores.append((symbol, score))
        scores.sort(key=lambda item: item[1], reverse=True)
        selected = [symbol for symbol, _ in scores[: self.top_n]]
        if not selected:
            return {}
        weight = round(1 / len(selected), 4)
        return {symbol: weight for symbol in selected}
