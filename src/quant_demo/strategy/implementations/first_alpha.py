from __future__ import annotations

from quant_demo.strategy.base import StrategyContext


class FirstAlphaStrategy:
    name = "first_alpha_v1"

    def __init__(self, lookback_days: int = 20, top_n: int = 3) -> None:
        self.lookback_days = lookback_days
        self.top_n = top_n

    def target_weights(self, context: StrategyContext) -> dict[str, float]:
        history = context.history[context.history["trading_date"] <= context.trading_date]
        scores: list[tuple[str, float]] = []
        for symbol, frame in history.groupby("symbol"):
            ordered = frame.sort_values("trading_date")
            window = ordered.tail(max(self.lookback_days, 6))
            if len(window) < 6:
                continue
            returns = window["close"].pct_change(fill_method=None).dropna()
            if returns.empty:
                continue
            momentum = float(window["close"].iloc[-1] / window["close"].iloc[0] - 1)
            win_rate = float((returns > 0).mean())
            volatility = float(returns.std() or 0.0)
            liquidity = float(window["volume"].mean()) / 1_000_000
            score = momentum * 0.55 + win_rate * 0.25 - volatility * 0.15 + liquidity * 0.05
            if momentum > 0 and score > 0:
                scores.append((symbol, score))
        scores.sort(key=lambda item: item[1], reverse=True)
        selected = scores[: self.top_n]
        if not selected:
            return {}
        total_score = sum(score for _, score in selected)
        if total_score <= 0:
            return {}
        return {symbol: round(score / total_score, 4) for symbol, score in selected}
