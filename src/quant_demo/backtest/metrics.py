"""回测标准指标（设计文档 §19）。"""
from __future__ import annotations

import math

TRADING_DAYS_PER_YEAR = 252


def compute_metrics(
    equity: list[dict],
    trades: list[dict],
    initial_cash: float,
    benchmark_equity: list[dict] | None = None,
) -> dict:
    if not equity:
        return {"total_return": 0.0, "annualized_return": 0.0}
    totals = [point["total_asset"] for point in equity]
    returns = [
        (totals[i] / totals[i - 1] - 1.0) if totals[i - 1] else 0.0 for i in range(1, len(totals))
    ]
    final_asset = totals[-1]
    total_return = final_asset / initial_cash - 1.0 if initial_cash else 0.0
    n_days = len(totals)
    annualized = (
        (final_asset / initial_cash) ** (TRADING_DAYS_PER_YEAR / max(n_days, 1)) - 1.0
        if initial_cash > 0 and final_asset > 0
        else -1.0
    )
    max_drawdown = _max_drawdown(totals)
    volatility = _std(returns) * math.sqrt(TRADING_DAYS_PER_YEAR) if returns else 0.0
    mean_return = sum(returns) / len(returns) if returns else 0.0
    sharpe = (mean_return * TRADING_DAYS_PER_YEAR / volatility) if volatility > 0 else 0.0
    downside = [r for r in returns if r < 0]
    downside_vol = _std(downside) * math.sqrt(TRADING_DAYS_PER_YEAR) if downside else 0.0
    sortino = (mean_return * TRADING_DAYS_PER_YEAR / downside_vol) if downside_vol > 0 else 0.0
    calmar = (annualized / abs(max_drawdown)) if max_drawdown < 0 else 0.0

    sell_trades = [trade for trade in trades if trade.get("side") == "sell"]
    win_rate = _win_rate(trades)
    trading_cost = round(
        sum(float(trade.get("commission", 0.0)) + float(trade.get("stamp_duty", 0.0)) for trade in trades), 2
    )
    turnover = _turnover(trades, totals)
    cash_ratio = round(equity[-1]["cash"] / final_asset, 6) if final_asset else 0.0
    exposure = round(equity[-1]["market_value"] / final_asset, 6) if final_asset else 0.0

    benchmark_return = None
    excess_return = None
    if benchmark_equity:
        benchmark_totals = [point["total_asset"] for point in benchmark_equity]
        if benchmark_totals and benchmark_totals[0]:
            benchmark_return = benchmark_totals[-1] / benchmark_totals[0] - 1.0
            excess_return = total_return - benchmark_return

    return {
        "total_return": round(total_return, 6),
        "annualized_return": round(annualized, 6),
        "benchmark_return": round(benchmark_return, 6) if benchmark_return is not None else None,
        "excess_return": round(excess_return, 6) if excess_return is not None else None,
        "max_drawdown": round(max_drawdown, 6),
        "sharpe": round(sharpe, 4),
        "sortino": round(sortino, 4),
        "calmar": round(calmar, 4),
        "volatility": round(volatility, 6),
        "win_rate": round(win_rate, 4) if win_rate is not None else None,
        "turnover": round(turnover, 4),
        "trading_cost": trading_cost,
        "average_holding_period": _average_holding_period(trades),
        "exposure": exposure,
        "cash_ratio": cash_ratio,
        "trade_count": len(trades),
        "sell_trade_count": len(sell_trades),
    }


def _max_drawdown(totals: list[float]) -> float:
    peak = totals[0]
    worst = 0.0
    for value in totals:
        peak = max(peak, value)
        if peak > 0:
            worst = min(worst, value / peak - 1.0)
    return worst


def _std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(variance)


def _win_rate(trades: list[dict]) -> float | None:
    """按标的配对（FIFO 近似）估算胜率。"""
    open_lots: dict[str, list[tuple[int, float]]] = {}
    wins = 0
    closed = 0
    for trade in trades:
        symbol = trade["symbol"]
        if trade["side"] == "buy":
            open_lots.setdefault(symbol, []).append((trade["qty"], trade["price"]))
            continue
        remaining = trade["qty"]
        lots = open_lots.get(symbol, [])
        while remaining > 0 and lots:
            lot_qty, lot_price = lots[0]
            consumed = min(lot_qty, remaining)
            closed += 1
            if trade["price"] > lot_price:
                wins += 1
            remaining -= consumed
            if consumed >= lot_qty:
                lots.pop(0)
            else:
                lots[0] = (lot_qty - consumed, lot_price)
    return wins / closed if closed else None


def _turnover(trades: list[dict], totals: list[float]) -> float:
    if not totals:
        return 0.0
    average_asset = sum(totals) / len(totals)
    traded_value = sum(trade["qty"] * trade["price"] for trade in trades)
    return traded_value / average_asset if average_asset else 0.0


def _average_holding_period(trades: list[dict]) -> float | None:
    """简化持仓周期：卖出次数 / 买入次数的反向近似（交易日）。"""
    from datetime import date as _date

    buy_dates: dict[str, list[_date]] = {}
    periods: list[int] = []
    for trade in trades:
        day = _date.fromisoformat(trade["date"])
        if trade["side"] == "buy":
            buy_dates.setdefault(trade["symbol"], []).append(day)
            continue
        queue = buy_dates.get(trade["symbol"], [])
        if queue:
            periods.append(max((day - queue.pop(0)).days, 0))
    return round(sum(periods) / len(periods), 2) if periods else None


__all__ = ["compute_metrics"]
