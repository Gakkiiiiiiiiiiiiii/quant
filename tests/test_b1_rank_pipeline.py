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
    return _load_module("test_user_strategy_rank_module", ROOT / "strategy" / "strategy.py")


def _load_backtest_script():
    script_dir = ROOT / "scripts"
    if str(script_dir) not in sys.path:
        sys.path.insert(0, str(script_dir))
    if str(ROOT / "src") not in sys.path:
        sys.path.insert(0, str(ROOT / "src"))
    return _load_module("test_user_pattern_rank_backtest_script", script_dir / "run_user_pattern_backtests.py")


def _make_feature_frame(dates: pd.DatetimeIndex) -> pd.DataFrame:
    n = len(dates)
    index = pd.MultiIndex.from_product([["000001.SZ"], dates], names=["instrument", "datetime"])
    close_prices = pd.Series([10.2] * n)
    open_prices = pd.Series([10.1] * n)
    high_prices = pd.Series([10.3] * n)
    low_prices = pd.Series([10.0] * n)
    volume = pd.Series([1000.0] * n)
    prev_close = close_prices.shift(1).fillna(close_prices.iloc[0])
    lt = pd.Series([9.8 + idx * 0.01 for idx in range(n)])
    st = lt + 0.2

    return pd.DataFrame(
        {
            "open": open_prices.to_numpy(),
            "high": high_prices.to_numpy(),
            "low": low_prices.to_numpy(),
            "close": close_prices.to_numpy(),
            "volume": volume.to_numpy(),
            "st": st.to_numpy(),
            "lt": lt.to_numpy(),
            "vol_ma20": [1000.0] * n,
            "vol_ma5": [1000.0] * n,
            "hh20": [10.6] * n,
            "hh40": [11.2] * n,
            "ll40": [9.5] * n,
            "count_above_lt_30": [24.0] * n,
            "count_above_st_20": [15.0] * n,
            "j": [15.0] * n,
            "down_vol_8": [1000.0] * n,
            "up_vol_8": [8000.0] * n,
            "amount_proxy": [50_000_000.0] * n,
            "amount_ma20": [50_000_000.0] * n,
            "body_pct": (((close_prices - open_prices).abs()) / (open_prices + 1e-12)).to_numpy(),
            "range_pct_prev_close": (((high_prices - low_prices) / (prev_close + 1e-12))).to_numpy(),
        },
        index=index,
    )


def test_apply_b1_model_scores_blends_priority_score_for_b1_only() -> None:
    strategy_module = _load_strategy_module()
    date = pd.Timestamp("2026-01-05")
    index = pd.MultiIndex.from_tuples(
        [("A", date), ("B", date), ("C", date)],
        names=["instrument", "datetime"],
    )
    signal = pd.DataFrame(
        {
            "b1": [1, 1, 0],
            "quality_score": [40.0, 80.0, 30.0],
            "priority_score": [100.0, 140.0, 130.0],
            "rule_priority_score": [100.0, 140.0, 130.0],
            "b1_confirm": [0, 0, 0],
            "score": [100.0, 140.0, 130.0],
        },
        index=index,
    )
    score_frame = pd.DataFrame(
        {
            "instrument": ["A", "B"],
            "datetime": [date, date],
            "model_score_raw": [0.2, 0.8],
        }
    )

    enriched = strategy_module.apply_b1_model_scores(signal, score_frame)

    assert round(float(enriched.loc[("A", date), "model_score"]), 4) == 50.0
    assert round(float(enriched.loc[("B", date), "model_score"]), 4) == 100.0
    assert round(float(enriched.loc[("A", date), "priority_score"]), 4) == 47.0
    assert round(float(enriched.loc[("B", date), "priority_score"]), 4) == 94.0
    assert round(float(enriched.loc[("C", date), "priority_score"]), 4) == 130.0
    assert enriched.loc[("B", date), "score_source"] == "model_blend"
    assert enriched.loc[("C", date), "score_source"] == "rule_only"


def test_resolve_b1_position_ratio_prefers_model_score_bins() -> None:
    strategy_module = _load_strategy_module()

    assert strategy_module.resolve_b1_position_ratio(pd.Series({"model_score": 49.0, "priority_score": 200.0})) == 0.12
    assert strategy_module.resolve_b1_position_ratio(pd.Series({"model_score": 50.0, "priority_score": 0.0})) == 0.15
    assert strategy_module.resolve_b1_position_ratio(pd.Series({"model_score": 80.0, "priority_score": 0.0})) == 0.18


def test_plan_b1_new_entries_filters_to_probe_topk_and_score_floor() -> None:
    backtest_script = _load_backtest_script()
    strategy_module = _load_strategy_module()
    candidate_df = pd.DataFrame(
        {
            "priority_score": [90.0, 88.0, 86.0, 84.0],
            "quality_score": [65.0, 60.0, 58.0, 50.0],
            "model_score": [90.0, 55.0, 40.0, 30.0],
            "b1_confirm": [0, 0, 0, 0],
        },
        index=pd.Index(["A", "B", "C", "D"], name="instrument"),
    )

    planned = backtest_script._plan_b1_new_entries(
        user_module=strategy_module,
        candidate_df=candidate_df,
        keep_codes=[],
        holdings={},
        max_holdings=8,
        total_equity_before_buy=500000.0,
        current_value=0.0,
        risk_degree=0.95,
        min_position_ratio=0.04,
    )

    assert [item["code"] for item in planned] == ["A", "B"]


def test_allow_b1_confirm_uses_model_threshold_when_present() -> None:
    strategy_module = _load_strategy_module()

    assert strategy_module.allow_b1_confirm(pd.Series({"b1_confirm": 1, "model_score": 59.9})) is False
    assert strategy_module.allow_b1_confirm(pd.Series({"b1_confirm": 1, "model_score": 60.0})) is True
    assert strategy_module.allow_b1_confirm(pd.Series({"b1_confirm": 1})) is True


def test_build_b1_rank_event_frame_deduplicates_unresolved_candidates() -> None:
    strategy_module = _load_strategy_module()
    dates = pd.bdate_range("2024-01-02", periods=40)
    features = _make_feature_frame(dates)
    signal = pd.DataFrame(
        {
            "b1": 0,
            "b1_confirm": 0,
            "b1_probe_invalid": 0,
            "b1_lt_hard_stop_flag": 0,
            "b1_st_stop_flag": 0,
            "b1_soft_exit_flag": 0,
            "b1_watch_days": 0,
        },
        index=features.index,
    )
    signal.loc[("000001.SZ", dates[20]), "b1"] = 1
    signal.loc[("000001.SZ", dates[25]), "b1"] = 1

    events = strategy_module.build_b1_rank_event_frame(features, signal, dedup_window=8)

    assert events["datetime"].dt.normalize().tolist() == [dates[20]]
