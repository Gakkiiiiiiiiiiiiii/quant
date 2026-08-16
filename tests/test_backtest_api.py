"""异步 Backtest API 与严格执行语义测试（设计文档 §18/§19/§43/§44/§48/§52/§69）。"""
from __future__ import annotations

import time
from datetime import date, datetime, time as dtime

import pytest

from quant_demo.api.v1_app import dispatch
from quant_demo.backtest.engine import BacktestExecutionConfig, EventDrivenBacktester
from quant_demo.backtest.target_portfolio import TargetPortfolio, TargetWeight
from quant_demo.backtest.time_contract import LookaheadViolation

from v1_fixtures import SAMPLE_SYMBOLS, build_state, history_dates


def _wait_completion(backtest_id: str, timeout: float = 30.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, payload = dispatch("GET", f"/api/v1/backtests/{backtest_id}", {}, {}, {})
        assert status == 200
        data = payload["data"]
        if data["status"] in {"COMPLETED", "FAILED"}:
            return data
        assert data["status"] in {"QUEUED", "RUNNING"}
        time.sleep(0.05)
    raise TimeoutError("backtest did not finish")


def test_async_backtest_lifecycle_and_metrics(tmp_path):
    state = build_state(tmp_path, days=260)
    dates = history_dates(state)
    start, end = dates[0], dates[-1]
    create_payload = {
        "strategy": {"type": "equal_weight", "version": "golden-v1", "rebalance_every_days": 20},
        "symbols": SAMPLE_SYMBOLS,
        "start": start,
        "end": end,
        "initial_cash": 500_000,
        "benchmark": SAMPLE_SYMBOLS[0],
        "execution_config": {
            "execution_model": "next_open",
            "commission_rate": 0.0003,
            "min_commission": 5,
            "stamp_duty_rate": 0.001,
            "slippage": 0.001,
            "t1": True,
            "price_limit": True,
            "suspension": True,
        },
    }
    status, payload = dispatch("POST", "/api/v1/backtests", {}, create_payload, {"Idempotency-Key": "bt-1"})
    assert status == 202
    data = payload["data"]
    assert data["status"] == "QUEUED"
    assert data["config_hash"]
    backtest_id = data["backtest_id"]

    job = _wait_completion(backtest_id)
    assert job["status"] == "COMPLETED", job
    assert job["started_at"] and job["completed_at"]

    status, metrics_payload = dispatch("GET", f"/api/v1/backtests/{backtest_id}/metrics", {}, {}, {})
    assert status == 200
    metrics = metrics_payload["data"]["metrics"]
    for key in (
        "total_return", "annualized_return", "benchmark_return", "excess_return",
        "max_drawdown", "sharpe", "sortino", "calmar", "volatility", "win_rate",
        "turnover", "trading_cost", "average_holding_period", "exposure", "cash_ratio",
    ):
        assert key in metrics
    assert metrics_payload["data"]["quality_flags"] is not None

    for section in ("equity", "trades", "positions", "daily-actions", "diagnostics"):
        status, section_payload = dispatch("GET", f"/api/v1/backtests/{backtest_id}/{section}", {}, {}, {})
        assert status == 200, section
        assert section_payload["data"] is not None
    status, diagnostics = dispatch("GET", f"/api/v1/backtests/{backtest_id}/diagnostics", {}, {}, {})
    assert diagnostics["data"]["data_snapshot_id"].startswith("mds-")


def test_reproducibility_hash_reuses_completed_backtest(tmp_path):
    state = build_state(tmp_path, days=260)
    dates = history_dates(state)
    create_payload = {
        "strategy": {"type": "equal_weight", "version": "v1"},
        "symbols": SAMPLE_SYMBOLS[:2],
        "start": dates[0],
        "end": dates[40],
        "initial_cash": 300_000,
    }
    _, first = dispatch("POST", "/api/v1/backtests", {}, create_payload, {})
    _wait_completion(first["data"]["backtest_id"])
    _, second = dispatch("POST", "/api/v1/backtests", {}, create_payload, {})
    assert second["data"]["backtest_id"] == first["data"]["backtest_id"]
    assert second["data"]["reused"] is True
    assert second["data"]["reuse_reason"] == "REPRODUCIBILITY_HASH_HIT"


def test_idempotency_key_prevents_duplicate_jobs(tmp_path):
    state = build_state(tmp_path, days=260)
    dates = history_dates(state)
    create_payload = {
        "strategy": {"type": "equal_weight", "version": "v1"},
        "symbols": SAMPLE_SYMBOLS[:2],
        "start": dates[0],
        "end": dates[30],
    }
    _, first = dispatch("POST", "/api/v1/backtests", {}, create_payload, {"Idempotency-Key": "dup-key"})
    _, second = dispatch("POST", "/api/v1/backtests", {}, create_payload, {"Idempotency-Key": "dup-key"})
    assert second["data"]["backtest_id"] == first["data"]["backtest_id"]
    assert second["data"].get("reused") is True


def test_t1_blocks_same_day_sell_and_suspension_blocks_trade(tmp_path):
    """T+1 / 停牌 / 涨跌停语义（设计文档 §52 / 验收标准 Backtest 部分）。"""
    state = build_state(tmp_path, days=60)
    data = state.market.bars_batch(SAMPLE_SYMBOLS, "2020-01-01", "2099-12-31")
    dates = [date.fromisoformat(day) for day in data["dates"]]
    trading_days = sorted({day for day in dates})
    symbol = SAMPLE_SYMBOLS[0]
    bars_by_day: dict[date, dict[str, dict]] = {}
    for symbol_index, sym in enumerate(data["symbols"]):
        for date_index, day in enumerate(data["dates"]):
            bars_by_day.setdefault(date.fromisoformat(day), {})[sym] = {
                field: data["bars"][field][symbol_index][date_index] for field in data["bars"]
            }
    buy_day, sell_day = trading_days[5], trading_days[6]
    day_before_buy = trading_days[4]
    portfolios = [
        TargetPortfolio(
            strategy_id="t1-test", strategy_version="v1",
            signal_time=datetime.combine(day_before_buy, dtime(15, 0)),
            available_at=datetime.combine(day_before_buy, dtime(15, 5)),
            executable_from=datetime.combine(buy_day, dtime(9, 30)),
            targets=[TargetWeight(symbol=symbol, target_weight=0.5)],
        ),
        TargetPortfolio(
            strategy_id="t1-test", strategy_version="v1",
            signal_time=datetime.combine(buy_day, dtime(15, 0)),
            available_at=datetime.combine(buy_day, dtime(15, 5)),
            executable_from=datetime.combine(sell_day, dtime(9, 30)),
            targets=[],
        ),
    ]
    engine = EventDrivenBacktester(bars_by_day, trading_days, 500_000, BacktestExecutionConfig())
    result = engine.run(portfolios)
    buy_trades = [trade for trade in result.trades if trade["side"] == "buy"]
    sell_trades = [trade for trade in result.trades if trade["side"] == "sell"]
    assert buy_trades and buy_trades[0]["date"] == buy_day.isoformat()
    # T+1：买入当日不可卖出，卖出只能发生在 buy_day 之后
    assert all(trade["date"] > buy_day.isoformat() for trade in sell_trades)
    assert sell_trades and sell_trades[0]["date"] == sell_day.isoformat()

    # 停牌阻断：在买入日标记停牌
    class _Suspended:
        is_suspended = True

    engine2 = EventDrivenBacktester(
        bars_by_day, trading_days, 500_000, BacktestExecutionConfig(),
        status_map={(buy_day, symbol): _Suspended()},
    )
    result2 = engine2.run(portfolios[:1])
    assert not result2.trades
    assert any(action["reason"] == "SUSPENDED_OR_NO_BAR" for action in result2.daily_actions)
    assert "PIT_SECURITY_STATUS_MISSING" not in result2.quality_flags


def test_price_limit_blocks_buy_at_limit_up(tmp_path):
    state = build_state(tmp_path, days=60)
    data = state.market.bars_batch(SAMPLE_SYMBOLS, "2020-01-01", "2099-12-31")
    dates = sorted(date.fromisoformat(day) for day in data["dates"])
    symbol = SAMPLE_SYMBOLS[0]
    bars_by_day: dict[date, dict[str, dict]] = {}
    for symbol_index, sym in enumerate(data["symbols"]):
        for date_index, day in enumerate(data["dates"]):
            bars_by_day.setdefault(date.fromisoformat(day), {})[sym] = {
                field: data["bars"][field][symbol_index][date_index] for field in data["bars"]
            }
    buy_day, signal_day = dates[5], dates[4]

    class _Limit:
        limit_rate = 0.1
        upper_limit_price = bars_by_day[buy_day][symbol]["open"] - 0.01  # 开盘价即超涨停
        lower_limit_price = 0.01

    portfolio = TargetPortfolio(
        strategy_id="limit-test", strategy_version="v1",
        signal_time=datetime.combine(signal_day, dtime(15, 0)),
        available_at=datetime.combine(signal_day, dtime(15, 5)),
        executable_from=datetime.combine(buy_day, dtime(9, 30)),
        targets=[TargetWeight(symbol=symbol, target_weight=0.5)],
    )
    engine = EventDrivenBacktester(
        bars_by_day, dates, 500_000, BacktestExecutionConfig(),
        limit_map={(buy_day, symbol): _Limit()},
    )
    result = engine.run([portfolio])
    assert not result.trades
    assert any(action["reason"] == "PRICE_LIMIT" for action in result.daily_actions)


def test_lookahead_violation_raised(tmp_path):
    state = build_state(tmp_path, days=60)
    data = state.market.bars_batch([SAMPLE_SYMBOLS[0]], "2020-01-01", "2099-12-31")
    dates = sorted(date.fromisoformat(day) for day in data["dates"])
    execution_day = dates[5]
    bars_by_day = {
        date.fromisoformat(day): {
            data["symbols"][i]: {field: data["bars"][field][i][j] for field in data["bars"]}
            for i in range(len(data["symbols"]))
        }
        for j, day in enumerate(data["dates"])
    }
    # available_at 晚于执行日开盘前 -> LOOKAHEAD_VIOLATION（§48）
    bad_portfolio = TargetPortfolio(
        strategy_id="bad", strategy_version="v1",
        signal_time=datetime.combine(dates[4], dtime(15, 0)),
        available_at=datetime.combine(execution_day, dtime(10, 0)),
        executable_from=datetime.combine(execution_day, dtime(9, 30)),
        targets=[TargetWeight(symbol=SAMPLE_SYMBOLS[0], target_weight=0.5)],
    )
    engine = EventDrivenBacktester(bars_by_day, dates, 500_000, BacktestExecutionConfig())
    with pytest.raises(LookaheadViolation):
        engine.run([bad_portfolio])


def test_golden_backtest_deterministic(tmp_path):
    """Golden Backtest（§69）：固定 Snapshot + Strategy + Execution Config 必须 deterministic。"""
    state = build_state(tmp_path, days=260)
    dates = history_dates(state)
    payload = {
        "strategy": {"type": "equal_weight", "version": "golden-v1", "rebalance_every_days": 10},
        "symbols": SAMPLE_SYMBOLS,
        "start": dates[0],
        "end": dates[-1],
        "initial_cash": 400_000,
        "execution_config": {"execution_model": "next_open", "t1": True, "price_limit": True, "suspension": True},
    }
    _, first = dispatch("POST", "/api/v1/backtests", {}, dict(payload, strategy={"type": "equal_weight", "version": "golden-a", "rebalance_every_days": 10}), {})
    _, second = dispatch("POST", "/api/v1/backtests", {}, dict(payload, strategy={"type": "equal_weight", "version": "golden-b", "rebalance_every_days": 10}), {})
    first_id, second_id = first["data"]["backtest_id"], second["data"]["backtest_id"]
    _wait_completion(first_id)
    _wait_completion(second_id)
    _, equity_a = dispatch("GET", f"/api/v1/backtests/{first_id}/equity", {}, {}, {})
    _, equity_b = dispatch("GET", f"/api/v1/backtests/{second_id}/equity", {}, {}, {})
    _, trades_a = dispatch("GET", f"/api/v1/backtests/{first_id}/trades", {}, {}, {})
    _, trades_b = dispatch("GET", f"/api/v1/backtests/{second_id}/trades", {}, {}, {})
    _, metrics_a = dispatch("GET", f"/api/v1/backtests/{first_id}/metrics", {}, {}, {})
    _, metrics_b = dispatch("GET", f"/api/v1/backtests/{second_id}/metrics", {}, {}, {})
    assert equity_a["data"] == equity_b["data"]
    assert trades_a["data"] == trades_b["data"]
    assert metrics_a["data"]["metrics"] == metrics_b["data"]["metrics"]
