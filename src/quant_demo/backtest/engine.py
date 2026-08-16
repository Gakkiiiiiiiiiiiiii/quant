"""事件驱动回测引擎（设计文档 §16.2 / §53）。

执行链路：TargetPortfolio -> Time Contract Validation -> Rebalancer ->
Execution Risk -> OMS(简化) -> Execution Model -> Position Book -> Account。

严格语义（自 stock_agent 迁入）：
- T+1：当日买入股份当日不可卖出
- 停牌阻断
- 涨跌停：实际涨跌停价 > 本地规则回退
- 统一 TransactionCostModel
- Lookahead Guard：available_at 必须在执行日之前
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime
from typing import Callable

from quant_demo.backtest.execution_model import ExecutionModel, resolve_fill_price
from quant_demo.backtest.target_portfolio import TargetPortfolio
from quant_demo.backtest.time_contract import LookaheadViolation
from quant_demo.backtest.transaction_cost import TransactionCostModel

DEFAULT_LIMIT_RATE = 0.10


@dataclass
class BacktestExecutionConfig:
    execution_model: ExecutionModel = ExecutionModel.NEXT_OPEN
    cost: TransactionCostModel = field(default_factory=TransactionCostModel)
    t1: bool = True
    price_limit: bool = True
    suspension: bool = True
    lot_size: int = 100


@dataclass
class _PositionState:
    qty: int = 0
    sellable_qty: int = 0
    avg_cost: float = 0.0
    pending_qty: int = 0  # 当日买入，T+1 后可卖


@dataclass
class BacktestRunResult:
    equity: list[dict] = field(default_factory=list)
    trades: list[dict] = field(default_factory=list)
    positions: list[dict] = field(default_factory=list)
    daily_actions: list[dict] = field(default_factory=list)
    diagnostics: dict = field(default_factory=dict)
    quality_flags: list[str] = field(default_factory=list)


class EventDrivenBacktester:
    """日线事件驱动回测器。"""

    def __init__(
        self,
        bars_by_day: dict[date, dict[str, dict]],
        trading_days: list[date],
        initial_cash: float,
        config: BacktestExecutionConfig | None = None,
        status_map: dict | None = None,
        limit_map: dict | None = None,
        prev_close_lookup: Callable[[str, date], float | None] | None = None,
    ) -> None:
        self._bars_by_day = bars_by_day
        self._trading_days = trading_days
        self._config = config or BacktestExecutionConfig()
        self._cash = float(initial_cash)
        self._initial_cash = float(initial_cash)
        self._positions: dict[str, _PositionState] = {}
        self._status_map = status_map or {}
        self._limit_map = limit_map or {}
        self._prev_close_lookup = prev_close_lookup
        self._limit_metadata_missing = False
        self._status_metadata_missing = bool(self._status_map == {} and self._config.suspension)

    # ------------------------------------------------------------------- run
    def run(self, portfolios: list[TargetPortfolio], progress_cb: Callable[[int, str], None] | None = None) -> BacktestRunResult:
        result = BacktestRunResult()
        by_execution_day: dict[date, TargetPortfolio] = {}
        for portfolio in portfolios:
            portfolio.validate_time_contract()
            by_execution_day[portfolio.executable_from.date()] = portfolio

        total_days = max(1, len(self._trading_days))
        for index, day in enumerate(self._trading_days):
            if progress_cb is not None and index % 20 == 0:
                progress_cb(int(index / total_days * 100), "execution")
            self._release_t1()
            bars = self._bars_by_day.get(day, {})
            portfolio = by_execution_day.get(day)
            if portfolio is not None:
                self._rebalance(day, portfolio, bars, result)
            self._mark_to_market(day, bars, result)
            self._snapshot_positions(day, result)
        if progress_cb is not None:
            progress_cb(100, "finalized")
        result.diagnostics = {
            "trading_days": len(self._trading_days),
            "initial_cash": self._initial_cash,
            "final_cash": round(self._cash, 2),
            "execution_model": self._config.execution_model.value,
        }
        result.quality_flags = self._quality_flags()
        return result

    # -------------------------------------------------------------- internals
    def _release_t1(self) -> None:
        if not self._config.t1:
            return
        for state in self._positions.values():
            state.sellable_qty += state.pending_qty
            state.pending_qty = 0

    def _rebalance(self, day: date, portfolio: TargetPortfolio, bars: dict[str, dict], result: BacktestRunResult) -> None:
        decision_deadline = datetime.combine(day, dtime(9, 15))
        if portfolio.available_at >= decision_deadline:
            raise LookaheadViolation(
                f"调仓信号 available_at={portfolio.available_at.isoformat()} 在执行日 "
                f"{day.isoformat()} 开盘前不可用：LOOKAHEAD_VIOLATION"
            )
        equity = self._equity_value(bars)
        targets = {target.symbol: target.target_weight for target in portfolio.targets}

        # 先卖出（回笼现金），再买入
        for symbol, state in sorted(self._positions.items()):
            target_weight = targets.get(symbol, 0.0)
            bar = bars.get(symbol)
            if state.qty <= 0:
                continue
            if bar is None or self._is_suspended(day, symbol):
                result.daily_actions.append(
                    {"date": day.isoformat(), "symbol": symbol, "action": "SELL_BLOCKED", "reason": "SUSPENDED_OR_NO_BAR"}
                )
                continue
            target_qty = self._target_qty(equity, target_weight, bar)
            delta = state.sellable_qty - max(target_qty, 0)
            if delta > 0:
                self._execute(day, symbol, "sell", delta, bar, result)

        equity = self._equity_value(bars)
        for symbol, target_weight in sorted(targets.items()):
            if target_weight <= 0:
                continue
            bar = bars.get(symbol)
            if bar is None or self._is_suspended(day, symbol):
                result.daily_actions.append(
                    {"date": day.isoformat(), "symbol": symbol, "action": "BUY_BLOCKED", "reason": "SUSPENDED_OR_NO_BAR"}
                )
                continue
            state = self._positions.setdefault(symbol, _PositionState())
            target_qty = self._target_qty(equity, target_weight, bar)
            delta = target_qty - state.qty
            if delta > 0:
                self._execute(day, symbol, "buy", delta, bar, result)

    def _execute(self, day: date, symbol: str, side: str, qty: int, bar: dict, result: BacktestRunResult) -> None:
        if qty <= 0:
            return
        if self._config.price_limit and self._blocked_by_price_limit(day, symbol, side, bar):
            result.daily_actions.append(
                {"date": day.isoformat(), "symbol": symbol, "action": f"{side.upper()}_BLOCKED", "reason": "PRICE_LIMIT"}
            )
            return
        raw_price = resolve_fill_price(self._config.execution_model, bar)
        if raw_price is None:
            result.daily_actions.append(
                {"date": day.isoformat(), "symbol": symbol, "action": f"{side.upper()}_BLOCKED", "reason": "NO_FILL_PRICE"}
            )
            return
        fill_price = self._config.cost.apply_slippage(round(float(raw_price), 6), side)
        state = self._positions.setdefault(symbol, _PositionState())
        if side == "buy":
            cost = self._config.cost.buy_cost(fill_price, qty)
            total_cost = fill_price * qty + self._config.cost.total(cost)
            if total_cost > self._cash:
                affordable = int(self._cash / (fill_price * 1.002)) // self._config.lot_size * self._config.lot_size
                if affordable <= 0:
                    result.daily_actions.append(
                        {"date": day.isoformat(), "symbol": symbol, "action": "BUY_BLOCKED", "reason": "INSUFFICIENT_CASH"}
                    )
                    return
                qty = affordable
                cost = self._config.cost.buy_cost(fill_price, qty)
                total_cost = fill_price * qty + self._config.cost.total(cost)
            self._cash -= total_cost
            new_qty = state.qty + qty
            state.avg_cost = (state.avg_cost * state.qty + fill_price * qty) / new_qty if new_qty else 0.0
            state.qty = new_qty
            if self._config.t1:
                state.pending_qty += qty
            else:
                state.sellable_qty += qty
        else:
            qty = min(qty, state.sellable_qty)
            if qty <= 0:
                return
            cost = self._config.cost.sell_cost(fill_price, qty)
            proceeds = fill_price * qty - self._config.cost.total(cost)
            self._cash += proceeds
            state.qty -= qty
            state.sellable_qty -= qty
        result.trades.append(
            {
                "date": day.isoformat(),
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "price": round(fill_price, 6),
                "commission": cost["commission"],
                "stamp_duty": cost["stamp_duty"],
                "execution_model": self._config.execution_model.value,
            }
        )
        result.daily_actions.append(
            {"date": day.isoformat(), "symbol": symbol, "action": side.upper(), "qty": qty, "price": round(fill_price, 6)}
        )

    def _blocked_by_price_limit(self, day: date, symbol: str, side: str, bar: dict) -> bool:
        limit_row = self._limit_map.get((day, symbol))
        fill_price = resolve_fill_price(self._config.execution_model, bar)
        if fill_price is None:
            return True
        if limit_row is not None:
            upper = limit_row.upper_limit_price
            lower = limit_row.lower_limit_price
            if upper is None or lower is None:
                prev_close = self._prev_close(symbol, day)
                if prev_close is None:
                    self._limit_metadata_missing = True
                    return False
                upper = upper if upper is not None else round(prev_close * (1 + limit_row.limit_rate), 6)
                lower = lower if lower is not None else round(prev_close * (1 - limit_row.limit_rate), 6)
            if side == "buy" and fill_price >= upper:
                return True
            return side == "sell" and fill_price <= lower
        # 无 PIT 涨跌停元数据：回退本地规则并记录质量标志。
        self._limit_metadata_missing = True
        prev_close = self._prev_close(symbol, day)
        if prev_close is None:
            return False
        upper = prev_close * (1 + DEFAULT_LIMIT_RATE)
        lower = prev_close * (1 - DEFAULT_LIMIT_RATE)
        if side == "buy" and fill_price >= upper:
            return True
        return side == "sell" and fill_price <= lower

    def _prev_close(self, symbol: str, day: date) -> float | None:
        if self._prev_close_lookup is not None:
            return self._prev_close_lookup(symbol, day)
        return None

    def _is_suspended(self, day: date, symbol: str) -> bool:
        if not self._config.suspension:
            return False
        status = self._status_map.get((day, symbol))
        return bool(status.is_suspended) if status is not None else False

    def _target_qty(self, equity: float, weight: float, bar: dict) -> int:
        price = resolve_fill_price(self._config.execution_model, bar) or bar.get("close")
        if not price:
            return 0
        amount = equity * weight
        raw_qty = int(amount / float(price))
        return raw_qty // self._config.lot_size * self._config.lot_size

    def _equity_value(self, bars: dict[str, dict]) -> float:
        market_value = 0.0
        for symbol, state in self._positions.items():
            if state.qty <= 0:
                continue
            bar = bars.get(symbol)
            price = float(bar["close"]) if bar and bar.get("close") is not None else state.avg_cost
            market_value += price * state.qty
        return self._cash + market_value

    def _mark_to_market(self, day: date, bars: dict[str, dict], result: BacktestRunResult) -> None:
        market_value = 0.0
        for symbol, state in self._positions.items():
            if state.qty <= 0:
                continue
            bar = bars.get(symbol)
            price = float(bar["close"]) if bar and bar.get("close") is not None else state.avg_cost
            market_value += price * state.qty
        result.equity.append(
            {
                "date": day.isoformat(),
                "total_asset": round(self._cash + market_value, 2),
                "cash": round(self._cash, 2),
                "market_value": round(market_value, 2),
            }
        )

    def _snapshot_positions(self, day: date, result: BacktestRunResult) -> None:
        for symbol, state in sorted(self._positions.items()):
            if state.qty <= 0:
                continue
            result.positions.append(
                {
                    "date": day.isoformat(),
                    "symbol": symbol,
                    "qty": state.qty,
                    "sellable_qty": state.sellable_qty,
                    "avg_cost": round(state.avg_cost, 6),
                }
            )

    def _quality_flags(self) -> list[str]:
        flags: list[str] = []
        if self._config.execution_model == ExecutionModel.VWAP:
            flags.append("VWAP_APPROXIMATED")
        if self._config.price_limit and self._limit_metadata_missing:
            flags.append("PRICE_LIMIT_METADATA_MISSING")
        if self._status_metadata_missing:
            flags.append("PIT_SECURITY_STATUS_MISSING")
        return flags


__all__ = ["BacktestExecutionConfig", "BacktestRunResult", "EventDrivenBacktester"]
