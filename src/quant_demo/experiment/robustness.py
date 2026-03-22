from __future__ import annotations

import itertools

from quant_demo.core.config import StrategySettings
from quant_demo.experiment.evaluator import EvaluationResult


class RobustnessRunner:
    def parameter_grid(self, strategy_settings: StrategySettings) -> list[dict]:
        lookbacks = sorted({max(5, strategy_settings.lookback_days - 5), strategy_settings.lookback_days, strategy_settings.lookback_days + 5})
        top_ns = sorted({max(1, strategy_settings.top_n - 1), strategy_settings.top_n, strategy_settings.top_n + 1})
        return [{"lookback_days": lookback, "top_n": top_n} for lookback, top_n in itertools.product(lookbacks, top_ns)]

    def summarize(self, results: list[tuple[dict, EvaluationResult]]) -> dict:
        if not results:
            return {"best": None, "count": 0}
        best = max(results, key=lambda item: item[1].total_return)
        return {"best": {"params": best[0], "total_return": best[1].total_return}, "count": len(results)}
