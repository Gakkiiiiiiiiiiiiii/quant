from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _load_strategy_module():
    qlib_source = ROOT / "runtime" / "qlib_source"
    if str(qlib_source) not in sys.path:
        sys.path.insert(0, str(qlib_source))
    return _load_module("test_brick_strategy_module", ROOT / "strategy" / "strategy.py")


def _load_backtest_script():
    script_dir = ROOT / "scripts"
    if str(script_dir) not in sys.path:
        sys.path.insert(0, str(script_dir))
    if str(ROOT / "src") not in sys.path:
        sys.path.insert(0, str(ROOT / "src"))
    return _load_module("test_brick_backtest_script", script_dir / "run_user_pattern_backtests.py")


def _run_stubbed_pattern_simulation(
    backtest_script,
    *,
    mode: str,
    ohlcv: pd.DataFrame,
    signal: pd.DataFrame,
    dates: pd.DatetimeIndex,
    start_time: str,
    end_time: str,
):
    user_module = SimpleNamespace(
        build_indicators=lambda frame: frame,
        build_pattern_signals=lambda frame: signal,
    )
    app_settings = SimpleNamespace(history_parquet="", qlib_benchmark_symbol="", qlib_benchmark="")

    original_load = backtest_script._load_local_ohlcv
    original_warm = backtest_script._resolve_warmup_start
    original_bench = backtest_script._benchmark_returns
    original_env = backtest_script._build_b1_env_filter
    original_st = backtest_script.resolve_st_symbols
    original_names = backtest_script.resolve_instrument_names
    try:
        backtest_script._load_local_ohlcv = lambda *args, **kwargs: ohlcv
        backtest_script._resolve_warmup_start = lambda *args, **kwargs: dates[0].date().isoformat()
        backtest_script._benchmark_returns = lambda *args, **kwargs: pd.Series(0.0, index=dates)
        backtest_script._build_b1_env_filter = lambda *args, **kwargs: (pd.Series(True, index=dates), "000852.SH")
        backtest_script.resolve_st_symbols = lambda *args, **kwargs: set()
        backtest_script.resolve_instrument_names = lambda *args, **kwargs: {}
        return backtest_script.simulate_pattern_backtest(
            user_module=user_module,
            app_settings=app_settings,
            provider_uri=ROOT,
            mode=mode,
            start_time=start_time,
            end_time=end_time,
            account=500000.0,
            max_holdings=8,
            risk_degree=0.95,
            max_holding_days=15,
            buy_price_field="open",
            sell_price_field="open",
            open_cost=0.0005,
            close_cost=0.0015,
            min_cost=5.0,
            slippage_rate=0.005,
            min_position_ratio=0.04,
            min_swap_score_gap=15.0,
        )
    finally:
        backtest_script._load_local_ohlcv = original_load
        backtest_script._resolve_warmup_start = original_warm
        backtest_script._benchmark_returns = original_bench
        backtest_script._build_b1_env_filter = original_env
        backtest_script.resolve_st_symbols = original_st
        backtest_script.resolve_instrument_names = original_names


def test_brick_formula_signal_matches_doc_baseline() -> None:
    strategy_module = _load_strategy_module()
    closes = [10.0 + 4.0 * idx / 64 for idx in range(65)] + [14.0, 13.4, 13.1, 13.5, 13.9]
    dates = pd.bdate_range("2025-01-02", periods=len(closes))
    opens = [close_value * 0.995 for close_value in closes]
    highs = [max(open_value, close_value) * 1.01 for open_value, close_value in zip(opens, closes)]
    lows = [min(open_value, close_value) * 0.99 for open_value, close_value in zip(opens, closes)]
    volumes = [10_000_000.0] * len(closes)
    amounts = [close_value * volume for close_value, volume in zip(closes, volumes)]
    index = pd.MultiIndex.from_product([["000001.SZ"], dates], names=["instrument", "datetime"])
    frame = pd.DataFrame(
        {
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volumes,
            "amount": amounts,
        },
        index=index,
    )

    features = strategy_module.build_indicators(frame)
    signal = strategy_module.build_pattern_signals(features)
    row = signal.iloc[-1]

    assert int(row["brick_buy_signal"]) == 1
    assert int(row["brick_strong_buy"]) == 1
    assert int(row["brick_filter_pass"]) == 1
    assert int(row["brick"]) == 1
    assert int(row["brick_sell_flag"]) == 0
    assert row["brick_divergence_ratio"] > 2.618


def test_select_candidates_respects_brick_env_filter() -> None:
    backtest_script = _load_backtest_script()
    day_df = pd.DataFrame(
        {
            "brick": [1, 1],
            "volume": [1000.0, 1000.0],
            "priority_score": [130.0, 120.0],
            "brick_env_ok": [1, 0],
        },
        index=pd.Index(["A", "B"], name="instrument"),
    )

    candidate_df = backtest_script._select_candidates(day_df, "BRICK", set())

    assert candidate_df.index.tolist() == ["A"]


def test_brick_backtest_reduces_once_then_exits_on_red_to_green() -> None:
    backtest_script = _load_backtest_script()
    dates = pd.bdate_range("2026-01-05", periods=6)
    index = pd.MultiIndex.from_product([["000001.SZ"], dates], names=["instrument", "datetime"])
    ohlcv = pd.DataFrame(
        {
            "open": [10.0, 10.2, 10.35, 10.5, 10.4, 10.0],
            "high": [10.2, 10.4, 10.55, 10.7, 10.5, 10.1],
            "low": [9.9, 10.1, 10.2, 10.3, 10.1, 9.8],
            "close": [10.1, 10.3, 10.45, 10.55, 10.2, 9.95],
            "volume": [2_000_000.0] * 6,
            "amount": [50_000_000.0] * 6,
        },
        index=index,
    )
    signal = pd.DataFrame(
        {
            "open": [10.0, 10.2, 10.35, 10.5, 10.4, 10.0],
            "high": [10.2, 10.4, 10.55, 10.7, 10.5, 10.1],
            "low": [9.9, 10.1, 10.2, 10.3, 10.1, 9.8],
            "close": [10.1, 10.3, 10.45, 10.55, 10.2, 9.95],
            "volume": [2_000_000.0] * 6,
            "st": [9.8] * 6,
            "lt": [9.5] * 6,
            "ma10": [10.0] * 6,
            "ma20": [9.9] * 6,
            "ma60": [9.7] * 6,
            "vol_ma5": [2_000_000.0] * 6,
            "amount_ma5": [120_000_000.0] * 6,
            "brick_value": [20.0, 24.0, 28.0, 31.0, 27.0, 24.0],
            "brick_red_bar": [1, 1, 1, 1, 0, 0],
            "brick_green_to_red": [1, 0, 0, 0, 0, 0],
            "brick_red_to_green": [0, 0, 0, 0, 1, 0],
            "brick_red_h": [4.0, 4.0, 4.0, 3.0, 4.0, 3.0],
            "brick_green_h": [2.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            "brick_height_ratio": [2.0, 4.0, 4.0, 3.0, 4.0, 3.0],
            "brick_divergence_ratio": [3.0, 3.0, 3.0, 2.9, 1.0, 1.0],
            "brick_buy_signal": [1, 0, 0, 0, 0, 0],
            "brick_strong_buy": [1, 0, 0, 0, 0, 0],
            "brick_four_red": [0, 0, 1, 1, 0, 0],
            "brick_reduce_flag": [0, 0, 1, 1, 0, 0],
            "brick_filter_pass": [1, 1, 1, 1, 1, 1],
            "brick_quality_score": [80.0, 60.0, 60.0, 60.0, 20.0, 20.0],
            "brick_sell_flag": [0, 0, 0, 0, 1, 0],
            "b1": [0] * 6,
            "b2": [0] * 6,
            "b3": [0] * 6,
            "brick": [1, 0, 0, 0, 0, 0],
            "entry_flag": [1, 0, 0, 0, 0, 0],
            "exit_flag": [0, 0, 0, 0, 1, 0],
            "stop_price": [None] * 6,
            "priority_score": [150.0, 10.0, 20.0, 20.0, 5.0, 5.0],
            "rule_priority_score": [150.0, 10.0, 20.0, 20.0, 5.0, 5.0],
            "quality_score": [80.0, 10.0, 20.0, 20.0, 5.0, 5.0],
            "model_score_raw": [None] * 6,
            "model_score": [None] * 6,
            "b1_exit_score_tp1": [None] * 6,
            "b1_exit_score_tp2": [None] * 6,
            "b1_exit_score_tp3": [None] * 6,
            "b1_exit_score_tail": [None] * 6,
            "final_score": [150.0, 10.0, 20.0, 20.0, 5.0, 5.0],
            "score_source": ["rule_only"] * 6,
            "b1_rank_active": [0] * 6,
            "pattern": ["BRICK", "", "", "", "", ""],
            "score": [150.0, 10.0, 20.0, 20.0, 5.0, 5.0],
        },
        index=index,
    )

    result = _run_stubbed_pattern_simulation(
        backtest_script,
        mode="BRICK",
        ohlcv=ohlcv,
        signal=signal,
        dates=dates,
        start_time="2026-01-06",
        end_time="2026-01-12",
    )

    ledger = result["ledger"]
    assert ledger["卖出原因"].tolist() == ["brick_four_red_reduce", "brick_red_to_green"]
    assert ledger["SELL日期"].tolist() == ["2026-01-08", "2026-01-12"]
    assert len(ledger) == 2
