from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(slots=True)
class EvaluationResult:
    total_return: float
    annualized_return: float
    max_drawdown: float
    turnover: float


class Evaluator:
    def evaluate(self, equity_curve: pd.DataFrame) -> EvaluationResult:
        if equity_curve.empty:
            return EvaluationResult(0.0, 0.0, 0.0, 0.0)
        start = float(equity_curve["equity"].iloc[0])
        end = float(equity_curve["equity"].iloc[-1])
        total_return = end / start - 1 if start else 0.0
        rolling_max = equity_curve["equity"].cummax()
        drawdowns = equity_curve["equity"] / rolling_max - 1
        annualized = (1 + total_return) ** (252 / max(1, len(equity_curve))) - 1
        turnover = float(equity_curve["turnover"].iloc[-1]) if "turnover" in equity_curve else 0.0
        return EvaluationResult(
            total_return=total_return,
            annualized_return=annualized,
            max_drawdown=float(drawdowns.min()),
            turnover=turnover,
        )
