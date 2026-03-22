from __future__ import annotations

from quant_demo.strategy.base import BaseStrategy


class StrategyRegistry:
    def __init__(self) -> None:
        self._strategies: dict[str, BaseStrategy] = {}

    def register(self, strategy: BaseStrategy) -> None:
        self._strategies[strategy.name] = strategy

    def get(self, name: str) -> BaseStrategy:
        return self._strategies[name]

    def names(self) -> list[str]:
        return sorted(self._strategies)
