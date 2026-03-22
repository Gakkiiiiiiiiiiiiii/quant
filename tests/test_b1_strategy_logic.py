from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
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
    return _load_module("test_user_strategy_module", ROOT / "strategy" / "strategy.py")


def _load_backtest_script():
    script_dir = ROOT / "scripts"
    if str(script_dir) not in sys.path:
        sys.path.insert(0, str(script_dir))
    if str(ROOT / "src") not in sys.path:
        sys.path.insert(0, str(ROOT / "src"))
    return _load_module("test_user_pattern_backtest_script", script_dir / "run_user_pattern_backtests.py")


def _make_b1_v5_feature_frame(dates: pd.DatetimeIndex) -> pd.DataFrame:
    n = len(dates)
    open_prices = [10.18] * n
    high_prices = [10.35] * n
    low_prices = [10.05] * n
    close_prices = [10.22] * n
    volumes = [1000.0] * n

    open_prices[30] = 10.00
    high_prices[30] = 10.85
    low_prices[30] = 9.98
    close_prices[30] = 10.76
    volumes[30] = 1800.0

    open_prices[34] = 10.55
    high_prices[34] = 11.20
    low_prices[34] = 10.40
    close_prices[34] = 10.60
    volumes[34] = 1300.0

    for idx in range(35, n - 1):
        open_prices[idx] = 10.44
        high_prices[idx] = 10.50
        low_prices[idx] = 10.30
        close_prices[idx] = 10.45
        volumes[idx] = 900.0

    open_prices[-1] = 10.33
    high_prices[-1] = 10.42
    low_prices[-1] = 10.24
    close_prices[-1] = 10.35
    volumes[-1] = 500.0

    close_series = pd.Series(close_prices)
    prev_close = close_series.shift(1).fillna(close_series.iloc[0])
    lt = np.linspace(9.60, 10.00, n)
    st = lt + 0.25
    hh20 = [10.90] * n
    hh20[34] = 11.20
    for idx in range(35, n):
        hh20[idx] = 11.40

    index = pd.MultiIndex.from_product([["000001.SZ"], dates], names=["instrument", "datetime"])
    return pd.DataFrame(
        {
            "open": open_prices,
            "high": high_prices,
            "low": low_prices,
            "close": close_prices,
            "volume": volumes,
            "st": st,
            "lt": lt,
            "vol_ma5": [1000.0] * n,
            "vol_ma20": [1000.0] * n,
            "ret1": close_series.pct_change().fillna(0.0).tolist(),
            "k": [24.0] * n,
            "d": [20.0] * n,
            "j": [15.0] * n,
            "listed_days": list(range(200, 200 + n)),
            "ll6": [10.00] * n,
            "hh8": [10.75] * n,
            "ll8": [9.95] * n,
            "hh6": [10.80] * n,
            "ll10": [9.90] * n,
            "hh6_prev": [10.70] * n,
            "hh10_prev": [10.90] * n,
            "hh20": hh20,
            "hh25": [11.50] * n,
            "hh30": [11.60] * n,
            "hh40": [12.40] * n,
            "ll20": [9.70] * n,
            "ll40": [9.50] * n,
            "vol_hh60": [2200.0] * n,
            "range_pos_3": [40.0] * n,
            "range_pos_21": [60.0] * n,
            "count_above_lt_20": [18.0] * n,
            "count_above_lt_30": [24.0] * n,
            "count_above_lt_10": [8.0] * n,
            "count_above_st_20": [15.0] * n,
            "count_above_st_10": [5.0] * n,
            "small_move_5": [4.0] * n,
            "st_slope_up": [True] * n,
            "lt_slope_up": [True] * n,
            "close_pos": [0.50] * n,
            "body_ratio": [0.01] * n,
            "body_pct": (((pd.Series(close_prices) - pd.Series(open_prices)).abs()) / (pd.Series(open_prices) + 1e-12)).tolist(),
            "vol_ratio": (pd.Series(volumes) / 1000.0).tolist(),
            "structure_range_6": [0.06] * n,
            "range_pct_prev_close": (((pd.Series(high_prices) - pd.Series(low_prices)) / (prev_close + 1e-12))).tolist(),
            "amount_proxy": [50_000_000.0] * n,
            "amount_ma20": [50_000_000.0] * n,
            "upper_shadow_ratio": [0.20] * n,
            "down_vol_8": [1_000.0] * n,
            "up_vol_8": [8_000.0] * n,
            "down_body_8": [0.01] * n,
            "up_body_8": [0.10] * n,
        },
        index=index,
    )


def _default_user_module(signal: pd.DataFrame):
    return SimpleNamespace(
        build_indicators=lambda frame: frame,
        build_pattern_signals=lambda frame: signal,
        resolve_b1_position_ratio=lambda row: 0.15 if float(row.get("priority_score", 0.0)) >= 125.0 or bool(row.get("b1_confirm", 0)) else 0.12,
        B1_ACTIVE_MAIN_POSITION_LIMIT=8,
        B1_EXPLORATORY_POSITION_RATIO=0.12,
        B1_PROBE_RATIO_DIVISOR=3.0,
    )


def _run_stubbed_b1_simulation(
    backtest_script,
    *,
    ohlcv: pd.DataFrame,
    signal: pd.DataFrame,
    dates: pd.DatetimeIndex,
    start_time: str,
    end_time: str,
    user_module=None,
):
    if user_module is None:
        user_module = _default_user_module(signal)
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
        backtest_script._build_b1_env_filter = lambda *args, **kwargs: (pd.Series(True, index=dates), "000300.SH")
        backtest_script.resolve_st_symbols = lambda *args, **kwargs: set()
        backtest_script.resolve_instrument_names = lambda *args, **kwargs: {}
        return backtest_script.simulate_pattern_backtest(
            user_module=user_module,
            app_settings=app_settings,
            provider_uri=ROOT,
            mode="B1",
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


def test_b1_formula_signal_matches_user_rule() -> None:
    strategy_module = _load_strategy_module()
    dates = pd.bdate_range("2024-01-02", periods=40)
    frame = _make_b1_v5_feature_frame(dates)

    signal = strategy_module.build_pattern_signals(frame)
    row = signal.loc[("000001.SZ", dates[-1])]

    assert int(row["b1_trigger_entry"]) == 1
    assert int(row["b1"]) == 1
    assert int(row["b1_confirm"]) == 0
    assert int(row["b1_probe_invalid"]) == 0
    assert int(row["b1_forbidden"]) == 0
    assert int(row["b1_exit_flag"]) == 0
    assert pd.isna(row["stop_price"])


def test_b1_recent_distribution_blocks_reentry_without_repair() -> None:
    strategy_module = _load_strategy_module()
    dates = pd.bdate_range("2024-01-02", periods=40)
    frame = _make_b1_v5_feature_frame(dates)
    exit_row = ("000001.SZ", dates[10])
    frame.loc[exit_row, ["open", "high", "low", "close", "volume", "st", "lt", "vol_ma20", "vol_hh60", "hh20", "ll20"]] = [
        12.0,
        12.2,
        10.8,
        11.0,
        3000.0,
        10.0,
        9.0,
        1000.0,
        3200.0,
        11.5,
        9.0,
    ]

    signal = strategy_module.build_pattern_signals(frame)
    row = signal.loc[("000001.SZ", dates[-1])]

    assert int(signal.loc[exit_row, "b1_exit_flag"]) == 1
    assert int(row["b1_forbidden"]) == 1
    assert int(row["b1"]) == 0


def test_select_target_codes_prefers_high_score_replacement() -> None:
    backtest_script = _load_backtest_script()
    score_df = pd.DataFrame(
        {"priority_score": [70.0, 65.0, 90.0, 68.0]},
        index=pd.Index(["A", "B", "C", "D"], name="instrument"),
    )
    candidate_df = score_df.loc[["C", "D"]].copy()

    final_codes, final_keep, buy_codes = backtest_script._select_target_codes(
        candidate_df=candidate_df,
        score_df=score_df,
        keep_codes=["A", "B"],
        max_holdings=2,
        min_swap_score_gap=8.0,
    )

    assert final_codes == ["C", "A"]
    assert final_keep == ["A"]
    assert buy_codes == ["C"]


def test_select_candidates_respects_b1_env_filter() -> None:
    backtest_script = _load_backtest_script()
    day_df = pd.DataFrame(
        {
            "b1": [1, 1],
            "volume": [1000.0, 1000.0],
            "priority_score": [120.0, 110.0],
            "b1_env_ok": [1, 0],
        },
        index=pd.Index(["A", "B"], name="instrument"),
    )

    candidate_df = backtest_script._select_candidates(day_df, "B1", set())

    assert candidate_df.index.tolist() == ["A"]


def test_limit_new_buys_avoids_small_positions() -> None:
    backtest_script = _load_backtest_script()

    limited = backtest_script._limit_new_buys(
        buy_codes=["A", "B", "C", "D", "E", "F"],
        total_equity_before_buy=500000.0,
        current_value=390000.0,
        risk_degree=0.95,
        min_position_ratio=0.06,
    )

    assert limited == ["A", "B"]


def test_shift_start_by_trading_days_uses_preceding_history() -> None:
    backtest_script = _load_backtest_script()
    trading_dates = pd.bdate_range("2025-01-02", periods=220)

    warmup_start = backtest_script._shift_start_by_trading_days(
        trading_dates,
        "2025-09-20",
        114,
    )

    target_start = pd.Timestamp("2025-09-22")
    target_position = trading_dates.get_loc(target_start)
    assert warmup_start == trading_dates[target_position - 114]


def test_exit_decision_uses_signal_bar_only() -> None:
    backtest_script = _load_backtest_script()
    signal_row = pd.Series({"exit_flag": 0, "close": 9.8, "st": 10.1})
    meta = {"stop_price": 10.0, "hold_days": 3}

    exit_now, reason = backtest_script._decide_exit_from_signal(
        signal_row,
        meta,
        max_holding_days=15,
    )

    assert exit_now is True
    assert reason == "stop_loss"


def test_b1_probe_action_accepts_lt_st_distribution_and_timeout() -> None:
    backtest_script = _load_backtest_script()

    hold_action, hold_reason = backtest_script._decide_b1_probe_action(
        pd.Series({"b1_probe_invalid": 0, "b1_watch_days": 3}),
        {"pattern": "B1"},
    )
    invalid_action, invalid_reason = backtest_script._decide_b1_probe_action(
        pd.Series({"b1_probe_invalid": 1}),
        {"pattern": "B1"},
    )
    lt_action, lt_reason = backtest_script._decide_b1_probe_action(
        pd.Series({"b1_probe_invalid": 0, "b1_lt_hard_stop_flag": 1}),
        {"pattern": "B1"},
    )
    st_action, st_reason = backtest_script._decide_b1_probe_action(
        pd.Series({"b1_probe_invalid": 0, "b1_st_stop_flag": 1}),
        {"pattern": "B1"},
    )
    dist_action, dist_reason = backtest_script._decide_b1_probe_action(
        pd.Series({"b1_probe_invalid": 0, "b1_soft_exit_flag": 1}),
        {"pattern": "B1"},
    )
    timeout_action, timeout_reason = backtest_script._decide_b1_probe_action(
        pd.Series({"b1_probe_invalid": 0, "b1_watch_days": 6}),
        {"pattern": "B1"},
    )

    assert hold_action == "hold"
    assert hold_reason == ""
    assert invalid_action == "full_exit"
    assert invalid_reason == "b1_probe_invalid"
    assert lt_action == "full_exit"
    assert lt_reason == "b1_lt_hard_stop"
    assert st_action == "full_exit"
    assert st_reason == "b1_st_stop"
    assert dist_action == "full_exit"
    assert dist_reason == "b1_distribution_soft"
    assert timeout_action == "full_exit"
    assert timeout_reason == "b1_probe_timeout"


def test_b1_exit_decision_uses_v2_defense_stops() -> None:
    backtest_script = _load_backtest_script()
    signal_row = pd.Series(
        {
            "close": 9.7,
            "st": 10.0,
            "b1_lt_hard_stop_flag": 1,
            "b1_st_stop_flag": 0,
            "b1_hard_distribution_flag": 0,
            "b1_soft_exit_flag": 0,
        }
    )
    meta = {
        "buy_price": 10.0,
        "hold_days": 3,
        "position_stage": 0,
        "entry_signal_low": 10.0,
        "entry_signal_close": 10.2,
        "entry_platform_established": True,
        "entry_platform_low": 9.9,
        "max_high_since_entry": 10.3,
        "min_low_since_entry": 9.8,
        "last_soft_exit_signal_date": "",
    }

    action, reason, next_stage = backtest_script._decide_b1_position_action(
        signal_row,
        meta,
        signal_date=pd.Timestamp("2026-01-10"),
    )

    assert action == "full_exit"
    assert reason == "b1_lt_hard_stop"
    assert next_stage is None


def test_b1_st_stop_requires_volume_expansion_and_weaker_close() -> None:
    strategy_module = _load_strategy_module()
    dates = pd.bdate_range("2024-01-02", periods=40)
    frame = _make_b1_v5_feature_frame(dates)
    frame.loc[("000001.SZ", dates[-2]), ["close", "st", "volume", "vol_ma5"]] = [10.00, 10.20, 1000.0, 1000.0]
    frame.loc[("000001.SZ", dates[-1]), ["open", "close", "st", "volume", "vol_ma5"]] = [10.05, 10.02, 10.20, 1100.0, 1000.0]

    signal = strategy_module.build_pattern_signals(frame)

    assert int(signal.iloc[-1]["b1_st_stop_flag"]) == 0


def test_b1_exit_decision_uses_v2_time_stop() -> None:
    backtest_script = _load_backtest_script()
    signal_row = pd.Series(
        {
            "close": 9.9,
            "st": 10.0,
            "b1_lt_hard_stop_flag": 0,
            "b1_st_stop_flag": 0,
            "b1_hard_distribution_flag": 0,
            "b1_soft_exit_flag": 0,
        }
    )
    meta = {
        "buy_price": 10.0,
        "hold_days": 8,
        "position_stage": 0,
        "entry_signal_low": 9.6,
        "entry_signal_close": 10.0,
        "entry_platform_established": False,
        "entry_platform_low": 9.5,
        "max_high_since_entry": 10.3,
        "min_low_since_entry": 9.7,
        "last_soft_exit_signal_date": "",
    }

    action, reason, next_stage = backtest_script._decide_b1_position_action(
        signal_row,
        meta,
        signal_date=pd.Timestamp("2026-01-11"),
    )

    assert action == "full_exit"
    assert reason == "b1_time_stop_a"
    assert next_stage is None


def test_b1_v3_partial_exit_triggers_at_10_percent_mfe() -> None:
    backtest_script = _load_backtest_script()
    signal_row = pd.Series(
        {
            "close": 10.8,
            "st": 10.2,
            "b1_lt_hard_stop_flag": 0,
            "b1_st_stop_flag": 0,
            "b1_hard_distribution_flag": 0,
            "b1_soft_exit_flag": 0,
        }
    )
    meta = {
        "buy_price": 10.0,
        "hold_days": 3,
        "position_stage": 0,
        "entry_signal_low": 9.6,
        "entry_signal_close": 10.0,
        "entry_platform_established": False,
        "entry_platform_low": 9.5,
        "max_high_since_entry": 11.2,
        "min_low_since_entry": 9.9,
        "last_soft_exit_signal_date": "",
    }

    action, reason, next_stage = backtest_script._decide_b1_position_action(
        signal_row,
        meta,
        signal_date=pd.Timestamp("2026-01-10"),
    )

    assert action == "partial_exit"
    assert reason == "b1_profit_take_10"
    assert next_stage == 1


def test_b1_v3_hard_exit_has_priority_over_partial_exit() -> None:
    backtest_script = _load_backtest_script()
    signal_row = pd.Series(
        {
            "close": 9.7,
            "st": 10.0,
            "b1_lt_hard_stop_flag": 1,
            "b1_st_stop_flag": 0,
            "b1_hard_distribution_flag": 0,
            "b1_soft_exit_flag": 1,
        }
    )
    meta = {
        "buy_price": 10.0,
        "hold_days": 3,
        "position_stage": 1,
        "entry_signal_low": 9.6,
        "entry_signal_close": 10.0,
        "entry_platform_established": False,
        "entry_platform_low": 9.5,
        "max_high_since_entry": 12.0,
        "min_low_since_entry": 9.5,
        "last_soft_exit_signal_date": "",
    }

    action, reason, next_stage = backtest_script._decide_b1_position_action(
        signal_row,
        meta,
        signal_date=pd.Timestamp("2026-01-11"),
    )

    assert action == "full_exit"
    assert reason == "b1_lt_hard_stop"
    assert next_stage is None


def test_resolve_trade_price_prefers_open_execution() -> None:
    backtest_script = _load_backtest_script()
    execution_row = pd.Series({"open": 8.0, "close": 7.76})

    assert backtest_script._resolve_trade_price(execution_row, "open", 0.0) == 8.0
    assert backtest_script._resolve_trade_price(execution_row, "missing", 1.0) == 8.0


def test_resolve_b1_position_ratio_uses_v5_bands() -> None:
    strategy_module = _load_strategy_module()

    assert strategy_module.resolve_b1_position_ratio(pd.Series({"priority_score": 118.0, "quality_score": 48.0, "b1_confirm": 0})) == 0.12
    assert strategy_module.resolve_b1_position_ratio(pd.Series({"priority_score": 125.0, "quality_score": 60.0, "b1_confirm": 0})) == 0.15
    assert strategy_module.resolve_b1_position_ratio(pd.Series({"priority_score": 140.0, "quality_score": 72.0, "b1_confirm": 1})) == 0.18


def test_plan_b1_new_entries_uses_probe_ratio_and_main_slots() -> None:
    backtest_script = _load_backtest_script()
    strategy_module = _load_strategy_module()
    candidate_df = pd.DataFrame(
        {
            "priority_score": [125.0, 118.0],
            "quality_score": [60.0, 48.0],
            "b1_confirm": [0, 0],
        },
        index=pd.Index(["C", "D"], name="instrument"),
    )
    holdings = {
        "A": {"shares": 1000, "position_stage": 0, "b1_phase": "confirmed"},
        "B": {"shares": 700, "position_stage": 1, "b1_phase": "confirmed"},
    }

    planned = backtest_script._plan_b1_new_entries(
        user_module=strategy_module,
        candidate_df=candidate_df,
        keep_codes=["A", "B"],
        holdings=holdings,
        max_holdings=2,
        total_equity_before_buy=500000.0,
        current_value=250000.0,
        risk_degree=0.95,
        min_position_ratio=0.04,
    )

    assert [item["code"] for item in planned] == ["C"]
    assert round(float(planned[0]["target_ratio"]), 4) == 0.05


def test_b1_existing_position_sells_on_same_day_close_signal() -> None:
    backtest_script = _load_backtest_script()
    dates = pd.bdate_range("2026-01-05", periods=5)
    index = pd.MultiIndex.from_product([["000001.SZ"], dates], names=["instrument", "datetime"])
    ohlcv = pd.DataFrame(
        {
            "open": [10.0, 10.2, 10.35, 9.95, 9.8],
            "high": [10.2, 10.4, 10.5, 10.05, 9.9],
            "low": [9.9, 10.0, 10.2, 9.7, 9.6],
            "close": [10.1, 10.25, 10.3, 9.85, 9.7],
            "volume": [1000.0, 1100.0, 1400.0, 1300.0, 1000.0],
            "amount": [50_000_000.0] * 5,
        },
        index=index,
    )
    signal = pd.DataFrame(
        {
            "open": [10.0, 10.2, 10.35, 9.95, 9.8],
            "high": [10.2, 10.4, 10.5, 10.05, 9.9],
            "low": [9.9, 10.0, 10.2, 9.7, 9.6],
            "close": [10.1, 10.25, 10.3, 9.85, 9.7],
            "volume": [1000.0, 1100.0, 1400.0, 1300.0, 1000.0],
            "st": [9.9, 10.0, 10.1, 10.0, 9.8],
            "lt": [9.7, 9.7, 9.8, 9.8, 9.7],
            "vol_ma5": [1000.0] * 5,
            "b1_trigger_raw": [1, 0, 0, 0, 0],
            "b1_watch_days": [None] * 5,
            "b1_trigger_entry": [1, 0, 0, 0, 0],
            "b1_pullback_entry": [0, 0, 0, 0, 0],
            "b1_core": [1, 0, 0, 0, 0],
            "b1_confirm": [0, 1, 0, 0, 0],
            "b1_probe_invalid": [0, 0, 0, 0, 0],
            "b1_signal_high": [10.2, 10.4, 10.5, 10.05, 9.9],
            "b1_signal_low": [9.9, 10.0, 10.2, 9.7, 9.6],
            "b1_forbidden": [0, 0, 0, 0, 0],
            "b1_exit_flag": [0, 0, 0, 1, 0],
            "b1_soft_exit_flag": [0, 0, 0, 0, 0],
            "b1_hard_distribution_flag": [0, 0, 0, 0, 0],
            "b1_lt_hard_stop_flag": [0, 0, 0, 0, 0],
            "b1_st_stop_flag": [0, 0, 0, 1, 0],
            "b1_platform_established": [0, 0, 0, 0, 0],
            "b1_platform_low": [9.8, 9.8, 9.8, 9.8, 9.8],
            "b1": [1, 0, 0, 0, 0],
            "b2": [0, 0, 0, 0, 0],
            "b3": [0, 0, 0, 0, 0],
            "entry_flag": [1, 0, 0, 0, 0],
            "exit_flag": [0, 0, 0, 1, 0],
            "stop_price": [None] * 5,
            "priority_score": [126.0, 140.0, 50.0, 30.0, 30.0],
            "quality_score": [60.0, 72.0, 50.0, 30.0, 30.0],
            "pattern": ["B1", "", "", "", ""],
            "score": [126.0, 140.0, 50.0, 30.0, 30.0],
        },
        index=index,
    )

    result = _run_stubbed_b1_simulation(
        backtest_script,
        ohlcv=ohlcv,
        signal=signal,
        dates=dates,
        start_time="2026-01-06",
        end_time="2026-01-09",
    )

    ledger = result["ledger"]
    assert len(ledger) == 1
    row = ledger.iloc[0]
    assert row["SELL日期"] == "2026-01-08"
    assert row["卖出原因"] == "b1_st_stop"
    assert row["卖出价格"] == round(9.85 * 0.995, 4)


def test_b1_same_day_buy_exit_defers_to_next_open() -> None:
    backtest_script = _load_backtest_script()
    dates = pd.bdate_range("2026-01-05", periods=4)
    index = pd.MultiIndex.from_product([["000001.SZ"], dates], names=["instrument", "datetime"])
    ohlcv = pd.DataFrame(
        {
            "open": [10.0, 10.2, 9.7, 9.4],
            "high": [10.2, 10.4, 9.9, 9.5],
            "low": [9.9, 9.9, 9.5, 9.2],
            "close": [10.1, 9.6, 9.45, 9.3],
            "volume": [1000.0, 1000.0, 1000.0, 1000.0],
            "amount": [50_000_000.0] * 4,
        },
        index=index,
    )
    signal = pd.DataFrame(
        {
            "open": [10.0, 10.2, 9.7, 9.4],
            "high": [10.2, 10.4, 9.9, 9.5],
            "low": [9.9, 9.9, 9.5, 9.2],
            "close": [10.1, 9.6, 9.45, 9.3],
            "volume": [1000.0] * 4,
            "st": [9.9, 10.0, 9.8, 9.6],
            "lt": [9.7] * 4,
            "vol_ma5": [1000.0] * 4,
            "b1_trigger_raw": [1, 0, 0, 0],
            "b1_watch_days": [None] * 4,
            "b1_trigger_entry": [1, 0, 0, 0],
            "b1_pullback_entry": [0, 0, 0, 0],
            "b1_core": [1, 0, 0, 0],
            "b1_confirm": [0, 0, 0, 0],
            "b1_probe_invalid": [0, 1, 0, 0],
            "b1_signal_high": [10.2, 10.4, 9.9, 9.5],
            "b1_signal_low": [9.9, 9.9, 9.5, 9.2],
            "b1_forbidden": [0, 0, 0, 0],
            "b1_exit_flag": [0, 0, 0, 0],
            "b1_soft_exit_flag": [0, 0, 0, 0],
            "b1_hard_distribution_flag": [0, 0, 0, 0],
            "b1_lt_hard_stop_flag": [0, 0, 0, 0],
            "b1_st_stop_flag": [0, 0, 0, 0],
            "b1_platform_established": [0, 0, 0, 0],
            "b1_platform_low": [9.8, 9.8, 9.8, 9.8],
            "b1": [1, 0, 0, 0],
            "b2": [0, 0, 0, 0],
            "b3": [0, 0, 0, 0],
            "entry_flag": [1, 0, 0, 0],
            "exit_flag": [0, 0, 0, 0],
            "stop_price": [None] * 4,
            "priority_score": [120.0, 30.0, 30.0, 30.0],
            "quality_score": [60.0, 30.0, 30.0, 30.0],
            "pattern": ["B1", "", "", ""],
            "score": [120.0, 30.0, 30.0, 30.0],
        },
        index=index,
    )

    result = _run_stubbed_b1_simulation(
        backtest_script,
        ohlcv=ohlcv,
        signal=signal,
        dates=dates,
        start_time="2026-01-06",
        end_time="2026-01-08",
    )

    ledger = result["ledger"]
    assert len(ledger) == 1
    row = ledger.iloc[0]
    assert row["日期"] == "2026-01-06"
    assert row["SELL日期"] == "2026-01-07"
    assert row["卖出原因"] == "b1_probe_invalid"
    assert row["卖出价格"] == round(9.7 * 0.995, 4)


def test_b1_probe_timeout_sells_on_same_day_close_after_five_days() -> None:
    backtest_script = _load_backtest_script()
    dates = pd.bdate_range("2026-01-05", periods=7)
    index = pd.MultiIndex.from_product([["000001.SZ"], dates], names=["instrument", "datetime"])
    ohlcv = pd.DataFrame(
        {
            "open": [10.0, 10.2, 10.1, 10.0, 9.95, 9.98, 9.9],
            "high": [10.2, 10.3, 10.2, 10.1, 10.0, 10.02, 9.95],
            "low": [9.9, 10.0, 9.95, 9.9, 9.85, 9.9, 9.8],
            "close": [10.1, 10.15, 10.05, 9.98, 9.96, 9.97, 9.9],
            "volume": [1000.0] * 7,
            "amount": [50_000_000.0] * 7,
        },
        index=index,
    )
    signal = pd.DataFrame(
        {
            "open": [10.0, 10.2, 10.1, 10.0, 9.95, 9.98, 9.9],
            "high": [10.2, 10.3, 10.2, 10.1, 10.0, 10.02, 9.95],
            "low": [9.9, 10.0, 9.95, 9.9, 9.85, 9.9, 9.8],
            "close": [10.1, 10.15, 10.05, 9.98, 9.96, 9.97, 9.9],
            "volume": [1000.0] * 7,
            "st": [9.9, 10.0, 10.0, 9.95, 9.92, 9.93, 9.9],
            "lt": [9.7] * 7,
            "vol_ma5": [1000.0] * 7,
            "b1_trigger_raw": [1, 0, 0, 0, 0, 0, 0],
            "b1_watch_days": [0, 1, 2, 3, 4, 5, 6],
            "b1_trigger_entry": [1, 0, 0, 0, 0, 0, 0],
            "b1_pullback_entry": [0, 0, 0, 0, 0, 0, 0],
            "b1_core": [1, 0, 0, 0, 0, 0, 0],
            "b1_confirm": [0, 0, 0, 0, 0, 0, 0],
            "b1_probe_invalid": [0, 0, 0, 0, 0, 0, 0],
            "b1_signal_high": [10.2, 10.2, 10.2, 10.2, 10.2, 10.2, 10.2],
            "b1_signal_low": [9.9, 9.9, 9.9, 9.9, 9.9, 9.9, 9.9],
            "b1_forbidden": [0, 0, 0, 0, 0, 0, 0],
            "b1_exit_flag": [0, 0, 0, 0, 0, 0, 0],
            "b1_soft_exit_flag": [0, 0, 0, 0, 0, 0, 0],
            "b1_hard_distribution_flag": [0, 0, 0, 0, 0, 0, 0],
            "b1_lt_hard_stop_flag": [0, 0, 0, 0, 0, 0, 0],
            "b1_st_stop_flag": [0, 0, 0, 0, 0, 0, 0],
            "b1_platform_established": [0, 0, 0, 0, 0, 0, 0],
            "b1_platform_low": [9.8] * 7,
            "b1": [1, 0, 0, 0, 0, 0, 0],
            "b2": [0, 0, 0, 0, 0, 0, 0],
            "b3": [0, 0, 0, 0, 0, 0, 0],
            "entry_flag": [1, 0, 0, 0, 0, 0, 0],
            "exit_flag": [0, 0, 0, 0, 0, 0, 0],
            "stop_price": [None] * 7,
            "priority_score": [120.0, 30.0, 30.0, 30.0, 30.0, 30.0, 30.0],
            "quality_score": [60.0, 30.0, 30.0, 30.0, 30.0, 30.0, 30.0],
            "pattern": ["B1", "", "", "", "", "", ""],
            "score": [120.0, 30.0, 30.0, 30.0, 30.0, 30.0, 30.0],
        },
        index=index,
    )

    result = _run_stubbed_b1_simulation(
        backtest_script,
        ohlcv=ohlcv,
        signal=signal,
        dates=dates,
        start_time="2026-01-06",
        end_time="2026-01-13",
    )

    ledger = result["ledger"]
    assert len(ledger) == 1
    row = ledger.iloc[0]
    assert row["SELL日期"] == "2026-01-13"
    assert row["卖出原因"] == "b1_probe_timeout"
    assert row["卖出价格"] == round(9.9 * 0.995, 4)
