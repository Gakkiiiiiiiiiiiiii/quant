from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import pandas as pd

from quant_demo.strategy.base import StrategyContext


@dataclass(slots=True)
class JqRuntimeContext:
    universe: list[str] = field(default_factory=list)
    top_n: int = 3
    short_window: int = 5
    long_window: int = 20


class JoinQuantStyleStrategy:
    name = "joinquant_style"

    def __init__(self, lookback_days: int = 20, top_n: int = 3, extra: dict[str, Any] | None = None) -> None:
        payload = extra or {}
        self.runtime = JqRuntimeContext(
            top_n=max(1, int(payload.get("top_n", top_n))),
            short_window=max(2, int(payload.get("short_window", max(3, lookback_days // 4)))),
            long_window=max(5, int(payload.get("long_window", lookback_days))),
        )
        self._initialized = False

    def initialize(self, context: JqRuntimeContext, symbols: list[str]) -> None:
        context.universe = symbols

    def before_trading_start(self, context: JqRuntimeContext, history: pd.DataFrame) -> None:
        symbols = sorted(history["symbol"].dropna().unique().tolist())
        if symbols:
            context.universe = symbols

    def handle_data(self, context: JqRuntimeContext, history: pd.DataFrame) -> dict[str, float]:
        rows: list[tuple[str, float]] = []
        for symbol, frame in history.groupby("symbol"):
            ordered = frame.sort_values("trading_date")
            if len(ordered) < context.long_window:
                continue
            short_ma = ordered["close"].tail(context.short_window).mean()
            long_ma = ordered["close"].tail(context.long_window).mean()
            if long_ma <= 0:
                continue
            trend = float(short_ma / long_ma - 1)
            liquidity = float(ordered["volume"].tail(context.long_window).mean()) / 1_000_000
            score = trend * 0.85 + liquidity * 0.15
            if score > 0:
                rows.append((symbol, score))
        rows.sort(key=lambda item: item[1], reverse=True)
        selected = rows[: context.top_n]
        if not selected:
            return {}
        total = sum(score for _, score in selected)
        if total <= 0:
            return {}
        return {symbol: round(score / total, 4) for symbol, score in selected}

    def after_trading_end(self, context: JqRuntimeContext) -> None:
        _ = context

    def target_weights(self, context: StrategyContext) -> dict[str, float]:
        history = context.history[context.history["trading_date"] <= context.trading_date]
        if history.empty:
            return {}
        if not self._initialized:
            self.initialize(self.runtime, sorted(history["symbol"].dropna().unique().tolist()))
            self._initialized = True
        self.before_trading_start(self.runtime, history)
        target = self.handle_data(self.runtime, history)
        self.after_trading_end(self.runtime)
        return target
