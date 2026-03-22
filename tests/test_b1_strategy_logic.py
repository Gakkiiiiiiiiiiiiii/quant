from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

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


def test_b1_formula_signal_matches_user_rule() -> None:
    strategy_module = _load_strategy_module()
    index = pd.MultiIndex.from_tuples(
        [("000001.SZ", pd.Timestamp("2024-01-0" + str(day))) for day in range(2, 10)],
        names=["instrument", "datetime"],
    )
    frame = pd.DataFrame(
        {
            "open": [10.1, 10.1, 10.1, 10.1, 10.1, 10.1, 10.1, 10.0],
            "high": [10.3, 10.3, 10.3, 10.3, 10.3, 10.3, 10.3, 10.22],
            "low": [9.95, 9.95, 9.95, 9.95, 9.95, 9.95, 9.95, 9.92],
            "close": [10.05, 10.05, 10.05, 10.05, 10.05, 10.05, 10.05, 10.08],
            "volume": [900.0, 880.0, 860.0, 840.0, 820.0, 800.0, 780.0, 600.0],
            "st": [10.0] * 8,
            "lt": [9.95] * 8,
            "vol_ma5": [900.0] * 8,
            "vol_ma20": [1000.0] * 8,
            "ret1": [-0.01] * 8,
            "k": [12.0] * 8,
            "d": [16.0] * 8,
            "j": [4.0] * 8,
            "listed_days": [120.0] * 8,
            "ll6": [9.8] * 8,
            "ll8": [600.0] * 8,
            "hh8": [10.3] * 8,
            "hh6": [10.4] * 8,
            "ll10": [9.7] * 8,
            "hh6_prev": [10.35] * 8,
            "hh10_prev": [10.5] * 8,
            "hh20": [12.0] * 8,
            "hh25": [12.2] * 8,
            "hh30": [12.5] * 8,
            "ll20": [9.5] * 8,
            "vol_hh60": [1_500.0] * 8,
            "range_pos_3": [30.0] * 8,
            "range_pos_21": [55.0] * 8,
            "count_above_lt_20": [15.0] * 8,
            "count_above_lt_10": [8.0] * 8,
            "count_above_st_10": [8.0] * 8,
            "small_move_5": [4.0] * 8,
            "st_slope_up": [True] * 8,
            "lt_slope_up": [True] * 8,
            "close_pos": [0.4] * 7 + [0.53],
            "body_ratio": [0.01] * 8,
            "body_pct": [0.01] * 8,
            "vol_ratio": [0.9, 0.88, 0.86, 0.84, 0.82, 0.8, 0.78, 0.66],
            "structure_range_6": [0.06] * 8,
            "range_pct_prev_close": [0.035] * 7 + [0.03],
            "amount_proxy": [9_045.0] * 7 + [6_048.0],
            "amount_ma20": [50_000_000.0] * 8,
            "upper_shadow_ratio": [0.5] * 7 + [0.46],
            "down_vol_8": [1_000.0] * 8,
            "up_vol_8": [6_000.0] * 8,
            "down_body_8": [0.03] * 8,
            "up_body_8": [0.08] * 8,
        },
        index=index,
    )

    signal = strategy_module.build_pattern_signals(frame)
    row = signal.loc[("000001.SZ", pd.Timestamp("2024-01-09"))]

    assert int(row["b1_trigger_entry"]) == 1
    assert int(row["b1_pullback_entry"]) == 0
    assert int(row["b1"]) == 1
    assert int(row["b1_forbidden"]) == 0
    assert int(row["b1_exit_flag"]) == 0
    assert pd.isna(row["stop_price"])


def test_b1_recent_distribution_blocks_reentry_without_repair() -> None:
    strategy_module = _load_strategy_module()
    dates = pd.bdate_range("2024-01-02", periods=35)
    index = pd.MultiIndex.from_product([["000001.SZ"], dates], names=["instrument", "datetime"])
    frame = pd.DataFrame(
        {
            "open": [10.0] * 35,
            "high": [10.2] * 35,
            "low": [9.9] * 35,
            "close": [10.0] * 35,
            "volume": [900.0] * 35,
            "st": [10.0] * 35,
            "lt": [9.9] * 35,
            "vol_ma5": [950.0] * 35,
            "vol_ma20": [1_000.0] * 35,
            "ret1": [0.0] * 35,
            "k": [20.0] * 35,
            "d": [22.0] * 35,
            "j": [18.0] * 35,
            "listed_days": [200.0] * 35,
            "ll6": [9.8] * 35,
            "hh8": [10.3] * 35,
            "ll8": [9.8] * 35,
            "hh6": [10.3] * 35,
            "ll10": [9.7] * 35,
            "hh6_prev": [10.3] * 35,
            "hh10_prev": [10.4] * 35,
            "hh20": [12.0] * 35,
            "hh25": [12.2] * 35,
            "hh30": [12.5] * 35,
            "ll20": [9.5] * 35,
            "vol_hh60": [3_200.0] * 35,
            "range_pos_3": [30.0] * 35,
            "range_pos_21": [55.0] * 35,
            "count_above_lt_20": [15.0] * 35,
            "count_above_lt_10": [8.0] * 35,
            "count_above_st_10": [2.0] * 35,
            "small_move_5": [4.0] * 35,
            "st_slope_up": [True] * 35,
            "lt_slope_up": [True] * 35,
            "close_pos": [0.5] * 35,
            "body_ratio": [0.01] * 35,
            "body_pct": [0.01] * 35,
            "vol_ratio": [0.9] * 35,
            "structure_range_6": [0.06] * 35,
            "range_pct_prev_close": [0.03] * 35,
            "amount_proxy": [50_000_000.0] * 35,
            "amount_ma20": [50_000_000.0] * 35,
            "upper_shadow_ratio": [0.2] * 35,
            "down_vol_8": [1_000.0] * 35,
            "up_vol_8": [6_000.0] * 35,
            "down_body_8": [0.03] * 35,
            "up_body_8": [0.08] * 35,
        },
        index=index,
    )
    exit_row = ("000001.SZ", dates[10])
    frame.loc[exit_row, ["open", "high", "low", "close", "volume", "st", "lt", "vol_ma20"]] = [12.0, 12.2, 10.8, 11.0, 3_000.0, 10.0, 9.0, 1_000.0]
    final_row = ("000001.SZ", dates[-1])
    frame.loc[final_row, ["open", "high", "low", "close", "volume", "j", "st", "lt"]] = [10.0, 10.12, 9.95, 10.02, 600.0, 5.0, 10.0, 9.9]

    signal = strategy_module.build_pattern_signals(frame)
    row = signal.loc[final_row]

    assert int(signal.loc[exit_row, "b1_exit_flag"]) == 1
    assert int(row["b1_core"]) == 1
    assert int(row["b1_forbidden"]) == 1
    assert int(row["b1"]) == 0


def test_select_target_codes_prefers_high_score_replacement() -> None:
    backtest_script = _load_backtest_script()
    score_df = pd.DataFrame(
        {
            "priority_score": [70.0, 65.0, 90.0, 68.0],
        },
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

    unchanged_codes, unchanged_keep, unchanged_buy = backtest_script._select_target_codes(
        candidate_df=candidate_df,
        score_df=score_df,
        keep_codes=["A", "B"],
        max_holdings=2,
        min_swap_score_gap=30.0,
    )

    assert unchanged_codes == ["A", "B"]
    assert unchanged_keep == ["A", "B"]
    assert unchanged_buy == []


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


def test_b1_exit_decision_only_uses_distribution_signal() -> None:
    backtest_script = _load_backtest_script()
    signal_row = pd.Series({"exit_flag": 0, "b1_exit_flag": 0, "close": 9.8, "st": 10.1})
    meta = {"pattern": "B1", "stop_price": 10.0, "hold_days": 20}

    exit_now, reason = backtest_script._decide_exit_from_signal(
        signal_row,
        meta,
        max_holding_days=15,
    )

    assert exit_now is False
    assert reason == ""


def test_b1_exit_decision_uses_v2_defense_stops() -> None:
    backtest_script = _load_backtest_script()
    signal_row = pd.Series(
        {
            "exit_flag": 0,
            "b1_exit_flag": 0,
            "b1_lt_hard_stop_flag": 1,
            "b1_st_stop_flag": 0,
            "close": 9.7,
            "st": 10.0,
        }
    )
    meta = {
        "pattern": "B1",
        "hold_days": 3,
        "entry_signal_low": 10.0,
        "entry_signal_close": 10.2,
        "entry_platform_established": True,
        "entry_platform_low": 9.9,
        "max_high_since_entry": 10.3,
        "min_low_since_entry": 9.8,
    }

    exit_now, reason = backtest_script._decide_exit_from_signal(
        signal_row,
        meta,
        max_holding_days=15,
    )

    assert exit_now is True
    assert reason == "b1_lt_hard_stop"


def test_b1_st_stop_requires_volume_expansion_and_weaker_close() -> None:
    strategy_module = _load_strategy_module()
    dates = pd.bdate_range("2024-01-02", periods=3)
    index = pd.MultiIndex.from_product([["000001.SZ"], dates], names=["instrument", "datetime"])
    frame = pd.DataFrame(
        {
            "open": [10.0, 10.0, 10.0],
            "high": [10.2, 10.2, 10.2],
            "low": [9.8, 9.8, 9.8],
            "close": [9.9, 9.85, 9.84],
            "volume": [1000.0, 1000.0, 1000.0],
            "st": [10.1, 10.1, 10.1],
            "lt": [9.7, 9.7, 9.7],
            "vol_ma5": [1000.0, 1000.0, 1000.0],
            "vol_ma20": [1000.0, 1000.0, 1000.0],
            "ret1": [0.0, -0.005, -0.001],
            "k": [20.0, 20.0, 20.0],
            "d": [22.0, 22.0, 22.0],
            "j": [18.0, 18.0, 18.0],
            "listed_days": [200.0, 201.0, 202.0],
            "ll6": [9.7, 9.7, 9.7],
            "hh8": [10.3, 10.3, 10.3],
            "ll8": [9.7, 9.7, 9.7],
            "hh6": [10.3, 10.3, 10.3],
            "ll10": [9.7, 9.7, 9.7],
            "hh6_prev": [10.2, 10.2, 10.2],
            "hh10_prev": [10.2, 10.2, 10.2],
            "hh20": [10.4, 10.4, 10.4],
            "hh25": [10.4, 10.4, 10.4],
            "hh30": [10.4, 10.4, 10.4],
            "ll20": [9.6, 9.6, 9.6],
            "vol_hh60": [1500.0, 1500.0, 1500.0],
            "range_pos_3": [30.0, 30.0, 30.0],
            "range_pos_21": [55.0, 55.0, 55.0],
            "count_above_lt_20": [15.0, 15.0, 15.0],
            "count_above_lt_10": [8.0, 8.0, 8.0],
            "count_above_st_10": [2.0, 2.0, 2.0],
            "small_move_5": [4.0, 4.0, 4.0],
            "st_slope_up": [True, True, True],
            "lt_slope_up": [True, True, True],
            "close_pos": [0.5, 0.5, 0.5],
            "body_ratio": [0.01, 0.01, 0.01],
            "body_pct": [0.01, 0.01, 0.01],
            "vol_ratio": [1.0, 1.0, 1.0],
            "structure_range_6": [0.06, 0.06, 0.06],
            "range_pct_prev_close": [0.03, 0.03, 0.03],
            "amount_proxy": [50_000_000.0, 50_000_000.0, 50_000_000.0],
            "amount_ma20": [50_000_000.0, 50_000_000.0, 50_000_000.0],
            "upper_shadow_ratio": [0.2, 0.2, 0.2],
            "down_vol_8": [1_000.0, 1_000.0, 1_000.0],
            "up_vol_8": [6_000.0, 6_000.0, 6_000.0],
            "down_body_8": [0.03, 0.03, 0.03],
            "up_body_8": [0.08, 0.08, 0.08],
        },
        index=index,
    )

    signal = strategy_module.build_pattern_signals(frame)

    assert int(signal.iloc[1]["b1_st_stop_flag"]) == 0
    assert int(signal.iloc[2]["b1_st_stop_flag"]) == 0


def test_b1_exit_decision_uses_v2_time_stop() -> None:
    backtest_script = _load_backtest_script()
    signal_row = pd.Series(
        {
            "exit_flag": 0,
            "b1_exit_flag": 0,
            "b1_lt_hard_stop_flag": 0,
            "b1_st_stop_flag": 0,
            "close": 9.9,
            "st": 10.0,
        }
    )
    meta = {
        "pattern": "B1",
        "hold_days": 8,
        "entry_signal_low": 9.6,
        "entry_signal_close": 10.0,
        "entry_platform_established": False,
        "entry_platform_low": 9.5,
        "max_high_since_entry": 10.3,
        "min_low_since_entry": 9.7,
    }

    exit_now, reason = backtest_script._decide_exit_from_signal(
        signal_row,
        meta,
        max_holding_days=15,
    )

    assert exit_now is True
    assert reason == "b1_time_stop_a"


def test_resolve_trade_price_prefers_open_execution() -> None:
    backtest_script = _load_backtest_script()
    execution_row = pd.Series({"open": 8.0, "close": 7.76})

    assert backtest_script._resolve_trade_price(execution_row, "open", 0.0) == 8.0
    assert backtest_script._resolve_trade_price(execution_row, "missing", 1.0) == 8.0
