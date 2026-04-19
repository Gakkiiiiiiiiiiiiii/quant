from __future__ import annotations

import pandas as pd

from quant_demo.experiment.joinquant_microcap_engine import (
    MicrocapStrategyConfig,
    _build_segmented_special_treatment_for_symbol,
    _build_sz_special_treatment_for_symbol,
    _monster_exit_reason,
    _special_treatment_flags_from_name,
    _surge_exit_reason,
    _should_rebalance_on_date,
    adjust_target_amount_for_rules,
    available_trade_shares,
    build_target_portfolio,
    calendar_hedge_ratio,
    execution_price,
    fit_target_count_by_cash,
    volume_units_to_shares,
)


def _base_day_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": "000001.SZ",
                "open": 10.0,
                "prev_close": 10.0,
                "volume": 1_000_000,
                "listed_days": 300,
                "avg_amount_20_prev": 20_000_000.0,
                "market_cap_prev": 100.0,
                "is_st_name": False,
                "is_beijing_stock": False,
                "is_b_share": False,
            },
            {
                "symbol": "000002.SZ",
                "open": 9.0,
                "prev_close": 9.0,
                "volume": 1_000_000,
                "listed_days": 300,
                "avg_amount_20_prev": 20_000_000.0,
                "market_cap_prev": 120.0,
                "is_st_name": False,
                "is_beijing_stock": False,
                "is_b_share": False,
            },
            {
                "symbol": "000003.SZ",
                "open": 8.0,
                "prev_close": 8.0,
                "volume": 1_000_000,
                "listed_days": 300,
                "avg_amount_20_prev": 20_000_000.0,
                "market_cap_prev": 140.0,
                "is_st_name": False,
                "is_beijing_stock": False,
                "is_b_share": False,
            },
        ]
    )


def test_adjust_target_amount_for_rules_respects_board_lots() -> None:
    assert adjust_target_amount_for_rules("000001.SZ", 0, 135) == 100
    assert adjust_target_amount_for_rules("688981.SH", 0, 199) == 0
    assert adjust_target_amount_for_rules("688981.SH", 0, 205) == 205
    assert adjust_target_amount_for_rules("000001.SZ", 250, 30) == 50


def test_build_target_portfolio_keeps_existing_holding_inside_keep_rank() -> None:
    cfg = MicrocapStrategyConfig(target_hold_num=2, buy_rank=1, keep_rank=2, query_limit=10)
    target, ranked_count = build_target_portfolio(
        _base_day_frame(),
        holdings=["000002.SZ"],
        total_value_open=100_000.0,
        cfg=cfg,
    )

    assert ranked_count == 3
    assert target == ["000002.SZ", "000001.SZ"]


def test_fit_target_count_by_cash_shrinks_when_star_board_is_too_expensive() -> None:
    cfg = MicrocapStrategyConfig(target_hold_num=2)
    selected = fit_target_count_by_cash(
        candidates=["000001.SZ", "688981.SH"],
        price_lookup={"000001.SZ": 10.0, "688981.SH": 50.0},
        invest_value=15_000.0,
        cfg=cfg,
    )

    assert selected == ["000001.SZ"]


def test_build_target_portfolio_filters_out_front_adjusted_tiny_prices() -> None:
    cfg = MicrocapStrategyConfig(target_hold_num=1, min_price_floor=1.0)
    frame = _base_day_frame()
    frame.loc[0, "open"] = 0.04
    frame.loc[0, "prev_close"] = 0.04
    frame.loc[1, "prev_close"] = 9.0
    frame.loc[2, "prev_close"] = 8.0
    target, _ = build_target_portfolio(frame, holdings=[], total_value_open=100_000.0, cfg=cfg)

    assert target == ["000002.SZ"]


def test_build_target_portfolio_allows_low_amount_when_threshold_disabled() -> None:
    cfg = MicrocapStrategyConfig(target_hold_num=1, min_avg_money_20=0)
    frame = _base_day_frame()
    frame.loc[0, "avg_amount_20_prev"] = 1.0
    frame.loc[1, "market_cap_prev"] = 200.0
    frame.loc[2, "market_cap_prev"] = 300.0

    target, ranked_count = build_target_portfolio(frame, holdings=[], total_value_open=100_000.0, cfg=cfg)

    assert ranked_count == 3
    assert target == ["000001.SZ"]


def test_build_target_portfolio_filters_new_zhuang_candidate_only_for_new_buys() -> None:
    cfg = MicrocapStrategyConfig(target_hold_num=2, buy_rank=2, keep_rank=2, query_limit=10, zhuang_filter_enabled=True)
    frame = _base_day_frame()
    frame["is_old_zhuang_suspect_prev"] = [True, False, False]

    target, ranked_count = build_target_portfolio(frame, holdings=[], total_value_open=100_000.0, cfg=cfg)

    assert ranked_count == 2
    assert target == ["000002.SZ", "000003.SZ"]


def test_build_target_portfolio_keeps_existing_holding_even_when_zhuang_flagged() -> None:
    cfg = MicrocapStrategyConfig(target_hold_num=2, buy_rank=2, keep_rank=2, query_limit=10, zhuang_filter_enabled=True)
    frame = _base_day_frame()
    frame["is_old_zhuang_suspect_prev"] = [True, False, False]

    target, ranked_count = build_target_portfolio(frame, holdings=["000001.SZ"], total_value_open=100_000.0, cfg=cfg)

    assert ranked_count == 3
    assert target == ["000001.SZ", "000002.SZ"]


def test_build_target_portfolio_allows_st_and_beijing_but_still_excludes_star_st() -> None:
    cfg = MicrocapStrategyConfig(
        target_hold_num=2,
        buy_rank=2,
        keep_rank=2,
        query_limit=10,
        allow_st_buy=True,
        allow_beijing_stock_buy=True,
        exclude_star_st_buy=True,
        exclude_delisting_buy=True,
    )
    frame = pd.DataFrame(
        [
            {
                "symbol": "000001.SZ",
                "open": 10.0,
                "prev_close": 10.0,
                "volume": 1_000_000,
                "listed_days": 300,
                "avg_amount_20_prev": 20_000_000.0,
                "market_cap_prev": 100.0,
                "is_st_name": True,
                "is_star_st_name": False,
                "is_delisting_name": False,
                "is_beijing_stock": False,
                "is_b_share": False,
            },
            {
                "symbol": "430001.BJ",
                "open": 9.0,
                "prev_close": 9.0,
                "volume": 1_000_000,
                "listed_days": 300,
                "avg_amount_20_prev": 20_000_000.0,
                "market_cap_prev": 120.0,
                "is_st_name": False,
                "is_star_st_name": False,
                "is_delisting_name": False,
                "is_beijing_stock": True,
                "is_b_share": False,
            },
            {
                "symbol": "000003.SZ",
                "open": 8.0,
                "prev_close": 8.0,
                "volume": 1_000_000,
                "listed_days": 300,
                "avg_amount_20_prev": 20_000_000.0,
                "market_cap_prev": 80.0,
                "is_st_name": True,
                "is_star_st_name": True,
                "is_delisting_name": False,
                "is_beijing_stock": False,
                "is_b_share": False,
            },
        ]
    )

    target, ranked_count = build_target_portfolio(frame, holdings=[], total_value_open=100_000.0, cfg=cfg)

    assert ranked_count == 2
    assert target == ["000001.SZ", "430001.BJ"]


def test_build_sz_special_treatment_for_symbol_uses_change_dates() -> None:
    trading_dates = pd.Series(pd.to_datetime(["2022-01-03", "2022-01-04", "2022-01-05"]))
    change_rows = pd.DataFrame(
        [
            {"change_date": pd.Timestamp("2022-01-04"), "old_name": "普通简称", "new_name": "*ST测试"},
            {"change_date": pd.Timestamp("2022-01-05"), "old_name": "*ST测试", "new_name": "ST测试"},
        ]
    )

    result = _build_sz_special_treatment_for_symbol(trading_dates, "ST测试", change_rows)

    assert result["is_st_name"].tolist() == [False, True, True]
    assert result["is_star_st_name"].tolist() == [False, True, False]
    assert result["is_delisting_name"].tolist() == [False, False, False]


def test_build_segmented_special_treatment_for_symbol_maps_star_st_period_to_first_st_segment() -> None:
    trading_dates = pd.Series(pd.to_datetime(["2022-01-03", "2022-01-04", "2022-01-05", "2022-01-06"]))
    is_st_history = pd.Series([False, True, True, False])
    ordered_names = ["普通简称", "*ST测试", "ST测试", "普通简称"]

    result = _build_segmented_special_treatment_for_symbol(trading_dates, "普通简称", is_st_history, ordered_names)

    assert result["is_st_name"].tolist() == [False, True, True, False]
    assert result["is_star_st_name"].tolist() == [False, True, True, False]
    assert result["is_delisting_name"].tolist() == [False, False, False, False]


def test_special_treatment_flags_from_name_identifies_delisting() -> None:
    assert _special_treatment_flags_from_name("退市整理") == (True, False, True)


def test_build_target_portfolio_monster_prelude_prefers_accumulation_breakout_setup() -> None:
    cfg = MicrocapStrategyConfig(
        target_hold_num=1,
        buy_rank=1,
        keep_rank=2,
        query_limit=10,
        monster_prelude_enabled=True,
    )
    frame = pd.DataFrame(
        [
            {
                "symbol": "GOOD",
                "open": 10.0,
                "prev_close": 9.7,
                "volume": 1_000_000,
                "listed_days": 300,
                "avg_amount_20_prev": 20_000_000.0,
                "market_cap_prev": 2_000_000_000.0,
                "is_st_name": False,
                "is_beijing_stock": False,
                "is_b_share": False,
                "ret_5_prev": -0.03,
                "ret_20_prev": 0.02,
                "ret_60_prev": -0.08,
                "ret_120_prev": -0.05,
                "amount_ratio_5_20_prev": 0.35,
                "amount_ratio_1_20_prev": 2.10,
                "amount_ratio_3_20_prev": 0.25,
                "amount_ratio_5_60_prev": 0.40,
                "breakout_gap_20_prev": -0.02,
                "range_120_prev": 0.55,
                "drawdown_from_high_120_prev": -0.15,
                "ddx_proxy_prev": 0.02,
                "ddx_proxy_3_prev": 0.012,
                "ddx_burst_z_prev": 2.1,
                "monster_recent_spike_count_prev": 2.0,
                "monster_recent_spike_strength_prev": 2.4,
                "monster_recent_ddx_burst_max_prev": 2.3,
                "monster_recent_strong_spike_count_prev": 1.0,
                "monster_recent_strong_spike_amount_max_prev": 3.4,
                "monster_recent_strong_ddx_burst_max_prev": 2.8,
            },
            {
                "symbol": "BAD",
                "open": 10.0,
                "prev_close": 11.0,
                "volume": 1_000_000,
                "listed_days": 300,
                "avg_amount_20_prev": 20_000_000.0,
                "market_cap_prev": 8_000_000_000.0,
                "is_st_name": False,
                "is_beijing_stock": False,
                "is_b_share": False,
                "ret_5_prev": 0.12,
                "ret_20_prev": 0.20,
                "ret_60_prev": 0.35,
                "ret_120_prev": 0.50,
                "amount_ratio_5_20_prev": -0.10,
                "amount_ratio_1_20_prev": 0.80,
                "amount_ratio_3_20_prev": -0.20,
                "amount_ratio_5_60_prev": -0.30,
                "breakout_gap_20_prev": 0.18,
                "range_120_prev": 2.0,
                "drawdown_from_high_120_prev": 0.0,
                "ddx_proxy_prev": -0.01,
                "ddx_proxy_3_prev": -0.02,
                "ddx_burst_z_prev": -0.5,
                "monster_recent_spike_count_prev": 0.0,
                "monster_recent_spike_strength_prev": 0.9,
                "monster_recent_ddx_burst_max_prev": 0.1,
                "monster_recent_strong_spike_count_prev": 0.0,
                "monster_recent_strong_spike_amount_max_prev": 1.1,
                "monster_recent_strong_ddx_burst_max_prev": 0.1,
            },
        ]
    )

    target, ranked_count = build_target_portfolio(frame, holdings=[], total_value_open=100_000.0, cfg=cfg)

    assert ranked_count == 1
    assert target == ["GOOD"]


def test_monster_exit_reason_detects_big_bear_and_stagnation() -> None:
    cfg = MicrocapStrategyConfig(monster_prelude_enabled=True, min_holding_days=5)
    big_bear_row = type(
        "Row",
        (),
        {
            "open": 13.0,
            "close_ret_1_prev": -0.08,
            "amount_ratio_1_20_prev": 2.2,
            "close_location_prev": -0.5,
            "ma5_prev": 12.0,
            "prev_close": 11.8,
            "ret_5_prev": 0.02,
        },
    )()
    big_bear_meta = {"avg_cost": 10.0, "hold_since": pd.Timestamp("2024-01-01"), "peak_close": 13.5}
    assert _monster_exit_reason(big_bear_row, big_bear_meta, pd.Timestamp("2024-01-10"), cfg) == "monster_exit_big_bear"

    stagnation_row = type(
        "Row",
        (),
        {
            "open": 10.1,
            "close_ret_1_prev": 0.0,
            "amount_ratio_1_20_prev": 1.0,
            "close_location_prev": 0.0,
            "ma5_prev": 9.9,
            "prev_close": 10.0,
            "ret_5_prev": 0.01,
        },
    )()
    stagnation_meta = {"avg_cost": 10.0, "hold_since": pd.Timestamp("2024-01-01"), "peak_close": 10.3}
    assert _monster_exit_reason(stagnation_row, stagnation_meta, pd.Timestamp("2024-01-20"), cfg) == "monster_exit_stagnation"


def test_monster_exit_reason_only_uses_ma5_after_acceleration() -> None:
    cfg = MicrocapStrategyConfig(monster_prelude_enabled=True, min_holding_days=5)
    row = type(
        "Row",
        (),
        {
            "open": 10.2,
            "close_ret_1_prev": -0.01,
            "amount_ratio_1_20_prev": 1.0,
            "close_location_prev": -0.1,
            "ma5_prev": 10.6,
            "ma10_prev": 10.1,
            "prev_close": 10.3,
            "ret_5_prev": -0.02,
            "breakout_gap_20_prev": -0.02,
            "ddx_proxy_3_prev": 0.01,
            "amount_ratio_5_20_prev": 0.2,
        },
    )()
    non_accel_meta = {"avg_cost": 10.0, "hold_since": pd.Timestamp("2024-01-01"), "peak_close": 10.7}
    accel_meta = {"avg_cost": 10.0, "hold_since": pd.Timestamp("2024-01-01"), "peak_close": 12.5}

    assert _monster_exit_reason(row, non_accel_meta, pd.Timestamp("2024-01-10"), cfg) is None
    assert _monster_exit_reason(row, accel_meta, pd.Timestamp("2024-01-10"), cfg) == "monster_exit_ma5_break"


def test_surge_exit_reason_requires_large_gain_then_exits_on_big_bear_or_ma5_break() -> None:
    cfg = MicrocapStrategyConfig(
        surge_exit_enabled=True,
        surge_exit_gain_min=0.30,
        surge_ma5_break_gain_min=0.35,
        surge_ma5_break_amount_ratio_min=0.70,
        surge_ma5_break_close_ret_max=0.0,
    )
    row = type(
        "Row",
        (),
        {
            "open": 12.2,
            "close_ret_1_prev": -0.08,
            "amount_ratio_1_20_prev": 2.2,
            "close_location_prev": -0.4,
            "ma5_prev": 12.5,
            "prev_close": 12.2,
        },
    )()
    no_gain_meta = {"avg_cost": 10.0, "peak_close": 12.6}
    big_bear_meta = {"avg_cost": 10.0, "peak_close": 13.4}
    ma5_break_row = type(
        "Row",
        (),
        {
            "open": 13.1,
            "close_ret_1_prev": -0.01,
            "amount_ratio_1_20_prev": 0.8,
            "close_location_prev": 0.2,
            "ma5_prev": 12.5,
            "prev_close": 12.2,
        },
    )()
    ma5_break_meta = {"avg_cost": 10.0, "peak_close": 13.8}

    assert _surge_exit_reason(row, no_gain_meta, cfg) is None
    assert _surge_exit_reason(row, big_bear_meta, cfg) == "surge_exit_big_bear"
    assert _surge_exit_reason(ma5_break_row, ma5_break_meta, cfg) == "surge_exit_ma5_break"


def test_surge_exit_reason_uses_stricter_ma5_break_filters() -> None:
    cfg = MicrocapStrategyConfig(
        surge_exit_enabled=True,
        surge_exit_gain_min=0.30,
        surge_ma5_break_gain_min=0.45,
        surge_ma5_break_amount_ratio_min=1.20,
        surge_ma5_break_close_ret_max=0.0,
    )
    weak_ma5_row = type(
        "Row",
        (),
        {
            "open": 13.0,
            "close_ret_1_prev": 0.01,
            "amount_ratio_1_20_prev": 0.9,
            "close_location_prev": 0.0,
            "ma5_prev": 12.5,
            "prev_close": 12.2,
        },
    )()
    strong_ma5_row = type(
        "Row",
        (),
        {
            "open": 15.0,
            "close_ret_1_prev": -0.02,
            "amount_ratio_1_20_prev": 1.5,
            "close_location_prev": 0.0,
            "ma5_prev": 14.8,
            "prev_close": 14.4,
        },
    )()
    meta = {"avg_cost": 10.0, "peak_close": 15.2}

    assert _surge_exit_reason(weak_ma5_row, meta, cfg) is None
    assert _surge_exit_reason(strong_ma5_row, meta, cfg) == "surge_exit_ma5_break"


def test_build_target_portfolio_final_replace_swaps_flagged_new_buy_only() -> None:
    cfg = MicrocapStrategyConfig(
        target_hold_num=2,
        buy_rank=2,
        keep_rank=2,
        query_limit=4,
        zhuang_filter_enabled=True,
        zhuang_filter_mode="final_replace",
        zhuang_final_replace_max_count=1,
    )
    frame = _base_day_frame()
    extra = {
        "symbol": "000004.SZ",
        "open": 7.0,
        "prev_close": 7.0,
        "volume": 1_000_000,
        "listed_days": 300,
        "avg_amount_20_prev": 20_000_000.0,
        "market_cap_prev": 160.0,
        "is_st_name": False,
        "is_beijing_stock": False,
        "is_b_share": False,
    }
    frame = pd.concat([frame, pd.DataFrame([extra])], ignore_index=True)
    frame["is_old_zhuang_suspect_prev"] = [True, False, False, False]

    target, ranked_count = build_target_portfolio(frame, holdings=[], total_value_open=100_000.0, cfg=cfg)

    assert ranked_count == 4
    assert target == ["000003.SZ", "000002.SZ"]


def test_build_target_portfolio_final_replace_keeps_existing_flagged_holding() -> None:
    cfg = MicrocapStrategyConfig(
        target_hold_num=2,
        buy_rank=2,
        keep_rank=2,
        query_limit=4,
        zhuang_filter_enabled=True,
        zhuang_filter_mode="final_replace",
        zhuang_final_replace_max_count=1,
    )
    frame = _base_day_frame()
    extra = {
        "symbol": "000004.SZ",
        "open": 7.0,
        "prev_close": 7.0,
        "volume": 1_000_000,
        "listed_days": 300,
        "avg_amount_20_prev": 20_000_000.0,
        "market_cap_prev": 160.0,
        "is_st_name": False,
        "is_beijing_stock": False,
        "is_b_share": False,
    }
    frame = pd.concat([frame, pd.DataFrame([extra])], ignore_index=True)
    frame["is_old_zhuang_suspect_prev"] = [True, False, False, False]

    target, _ = build_target_portfolio(frame, holdings=["000001.SZ"], total_value_open=100_000.0, cfg=cfg)

    assert target == ["000001.SZ", "000002.SZ"]


def test_build_target_portfolio_excludes_overlay_asset_from_microcap_ranking() -> None:
    cfg = MicrocapStrategyConfig(target_hold_num=1)
    frame = _base_day_frame()
    overlay = {
        "symbol": "515450.SH",
        "open": 1.2,
        "prev_close": 1.2,
        "volume": 10_000_000,
        "listed_days": 3000,
        "avg_amount_20_prev": 50_000_000.0,
        "market_cap_prev": 1.0,
        "is_st_name": False,
        "is_beijing_stock": False,
        "is_b_share": False,
        "is_overlay_asset": True,
    }
    frame = pd.concat([frame, pd.DataFrame([overlay])], ignore_index=True)

    target, _ = build_target_portfolio(frame, holdings=[], total_value_open=100_000.0, cfg=cfg)

    assert target == ["000001.SZ"]


def test_build_target_portfolio_allocates_more_slots_to_stronger_industries() -> None:
    cfg = MicrocapStrategyConfig(
        target_hold_num=5,
        buy_rank=5,
        keep_rank=6,
        query_limit=20,
        industry_weighted_enabled=True,
        industry_top_k=2,
        industry_keep_top_k=3,
        industry_min_slots=1,
        industry_max_slots=4,
        industry_query_limit_multiplier=3,
        industry_min_candidate_count=3,
    )
    frame = pd.DataFrame(
        [
            {"symbol": "A1", "open": 10.0, "prev_close": 10.0, "volume": 1_000_000, "listed_days": 300, "avg_amount_20_prev": 20_000_000.0, "market_cap_prev": 100.0, "is_st_name": False, "is_beijing_stock": False, "is_b_share": False, "industry_code": "GICS2_STRONG", "industry_name": "STRONG", "ret_5_prev": 0.08, "ret_20_prev": 0.30, "amount_ratio_5_20_prev": 0.40},
            {"symbol": "A2", "open": 9.8, "prev_close": 9.8, "volume": 1_000_000, "listed_days": 300, "avg_amount_20_prev": 20_000_000.0, "market_cap_prev": 110.0, "is_st_name": False, "is_beijing_stock": False, "is_b_share": False, "industry_code": "GICS2_STRONG", "industry_name": "STRONG", "ret_5_prev": 0.07, "ret_20_prev": 0.28, "amount_ratio_5_20_prev": 0.35},
            {"symbol": "A3", "open": 9.6, "prev_close": 9.6, "volume": 1_000_000, "listed_days": 300, "avg_amount_20_prev": 20_000_000.0, "market_cap_prev": 120.0, "is_st_name": False, "is_beijing_stock": False, "is_b_share": False, "industry_code": "GICS2_STRONG", "industry_name": "STRONG", "ret_5_prev": 0.06, "ret_20_prev": 0.25, "amount_ratio_5_20_prev": 0.30},
            {"symbol": "C1", "open": 8.8, "prev_close": 8.8, "volume": 1_000_000, "listed_days": 300, "avg_amount_20_prev": 20_000_000.0, "market_cap_prev": 130.0, "is_st_name": False, "is_beijing_stock": False, "is_b_share": False, "industry_code": "GICS2_NEUTRAL", "industry_name": "NEUTRAL", "ret_5_prev": 0.03, "ret_20_prev": 0.12, "amount_ratio_5_20_prev": 0.10},
            {"symbol": "C2", "open": 8.6, "prev_close": 8.6, "volume": 1_000_000, "listed_days": 300, "avg_amount_20_prev": 20_000_000.0, "market_cap_prev": 140.0, "is_st_name": False, "is_beijing_stock": False, "is_b_share": False, "industry_code": "GICS2_NEUTRAL", "industry_name": "NEUTRAL", "ret_5_prev": 0.02, "ret_20_prev": 0.10, "amount_ratio_5_20_prev": 0.08},
            {"symbol": "B1", "open": 8.4, "prev_close": 8.4, "volume": 1_000_000, "listed_days": 300, "avg_amount_20_prev": 20_000_000.0, "market_cap_prev": 90.0, "is_st_name": False, "is_beijing_stock": False, "is_b_share": False, "industry_code": "GICS2_WEAK", "industry_name": "WEAK", "ret_5_prev": -0.04, "ret_20_prev": -0.12, "amount_ratio_5_20_prev": -0.15},
            {"symbol": "B2", "open": 8.2, "prev_close": 8.2, "volume": 1_000_000, "listed_days": 300, "avg_amount_20_prev": 20_000_000.0, "market_cap_prev": 95.0, "is_st_name": False, "is_beijing_stock": False, "is_b_share": False, "industry_code": "GICS2_WEAK", "industry_name": "WEAK", "ret_5_prev": -0.03, "ret_20_prev": -0.10, "amount_ratio_5_20_prev": -0.12},
        ]
    )

    target, ranked_count = build_target_portfolio(frame, holdings=[], total_value_open=100_000.0, cfg=cfg)

    assert ranked_count == 7
    assert len(target) == 5
    assert len([symbol for symbol in target if symbol.startswith("A")]) == 3
    assert len([symbol for symbol in target if symbol.startswith("C")]) == 2
    assert "B1" not in target
    assert "B2" not in target


def test_should_rebalance_on_date_honors_weekly_frequency() -> None:
    cfg = MicrocapStrategyConfig(rebalance_frequency="weekly")

    assert _should_rebalance_on_date(pd.Timestamp("2024-01-02"), None, cfg) is True
    assert _should_rebalance_on_date(pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-02"), cfg) is False
    assert _should_rebalance_on_date(pd.Timestamp("2024-01-08"), pd.Timestamp("2024-01-02"), cfg) is True


def test_industry_weighted_selection_uses_industry_local_microcap_ranking() -> None:
    cfg = MicrocapStrategyConfig(
        target_hold_num=4,
        buy_rank=4,
        keep_rank=6,
        query_limit=3,
        industry_weighted_enabled=True,
        industry_top_k=2,
        industry_keep_top_k=2,
        industry_min_slots=1,
        industry_max_slots=3,
        industry_query_limit_multiplier=2,
        industry_min_candidate_count=2,
    )
    frame = pd.DataFrame(
        [
            {"symbol": "A1", "open": 10.0, "prev_close": 10.0, "volume": 1_000_000, "listed_days": 300, "avg_amount_20_prev": 20_000_000.0, "market_cap_prev": 100.0, "is_st_name": False, "is_beijing_stock": False, "is_b_share": False, "industry_code": "GICS2_STRONG", "industry_name": "STRONG", "ret_5_prev": 0.09, "ret_20_prev": 0.30, "amount_ratio_5_20_prev": 0.30},
            {"symbol": "A2", "open": 9.8, "prev_close": 9.8, "volume": 1_000_000, "listed_days": 300, "avg_amount_20_prev": 20_000_000.0, "market_cap_prev": 110.0, "is_st_name": False, "is_beijing_stock": False, "is_b_share": False, "industry_code": "GICS2_STRONG", "industry_name": "STRONG", "ret_5_prev": 0.08, "ret_20_prev": 0.28, "amount_ratio_5_20_prev": 0.28},
            {"symbol": "W1", "open": 9.6, "prev_close": 9.6, "volume": 1_000_000, "listed_days": 300, "avg_amount_20_prev": 20_000_000.0, "market_cap_prev": 115.0, "is_st_name": False, "is_beijing_stock": False, "is_b_share": False, "industry_code": "GICS2_WEAK", "industry_name": "WEAK", "ret_5_prev": -0.03, "ret_20_prev": -0.12, "amount_ratio_5_20_prev": -0.10},
            {"symbol": "B1", "open": 9.4, "prev_close": 9.4, "volume": 1_000_000, "listed_days": 300, "avg_amount_20_prev": 20_000_000.0, "market_cap_prev": 180.0, "is_st_name": False, "is_beijing_stock": False, "is_b_share": False, "industry_code": "GICS2_NEUTRAL", "industry_name": "NEUTRAL", "ret_5_prev": 0.05, "ret_20_prev": 0.16, "amount_ratio_5_20_prev": 0.18},
            {"symbol": "B2", "open": 9.2, "prev_close": 9.2, "volume": 1_000_000, "listed_days": 300, "avg_amount_20_prev": 20_000_000.0, "market_cap_prev": 190.0, "is_st_name": False, "is_beijing_stock": False, "is_b_share": False, "industry_code": "GICS2_NEUTRAL", "industry_name": "NEUTRAL", "ret_5_prev": 0.04, "ret_20_prev": 0.15, "amount_ratio_5_20_prev": 0.16},
        ]
    )

    target, ranked_count = build_target_portfolio(frame, holdings=[], total_value_open=100_000.0, cfg=cfg)

    assert ranked_count == 5
    assert "B1" in target
    assert "B2" in target
    assert "W1" not in target


def test_layer_rotation_allocates_more_slots_to_stronger_small_cap_layers() -> None:
    cfg = MicrocapStrategyConfig(
        target_hold_num=5,
        buy_rank=5,
        keep_rank=6,
        query_limit=20,
        layer_rotation_enabled=True,
        layer_market_cap_bounds=[0, 2_000_000_000, 4_000_000_000, 6_000_000_000],
        layer_base_slots=[1, 1, 1],
        layer_max_slots=[3, 3, 3],
    )
    frame = pd.DataFrame(
        [
            {"symbol": "L1A", "open": 10.0, "prev_close": 10.0, "volume": 1_000_000, "listed_days": 300, "avg_amount_20_prev": 20_000_000.0, "market_cap_prev": 1_000_000_000.0, "is_st_name": False, "is_beijing_stock": False, "is_b_share": False, "ret_5_prev": 0.08, "ret_20_prev": 0.30, "amount_ratio_5_20_prev": 0.30},
            {"symbol": "L1B", "open": 9.8, "prev_close": 9.8, "volume": 1_000_000, "listed_days": 300, "avg_amount_20_prev": 20_000_000.0, "market_cap_prev": 1_200_000_000.0, "is_st_name": False, "is_beijing_stock": False, "is_b_share": False, "ret_5_prev": 0.07, "ret_20_prev": 0.28, "amount_ratio_5_20_prev": 0.28},
            {"symbol": "L1C", "open": 9.6, "prev_close": 9.6, "volume": 1_000_000, "listed_days": 300, "avg_amount_20_prev": 20_000_000.0, "market_cap_prev": 1_400_000_000.0, "is_st_name": False, "is_beijing_stock": False, "is_b_share": False, "ret_5_prev": 0.06, "ret_20_prev": 0.25, "amount_ratio_5_20_prev": 0.25},
            {"symbol": "L2A", "open": 9.4, "prev_close": 9.4, "volume": 1_000_000, "listed_days": 300, "avg_amount_20_prev": 20_000_000.0, "market_cap_prev": 2_500_000_000.0, "is_st_name": False, "is_beijing_stock": False, "is_b_share": False, "ret_5_prev": 0.03, "ret_20_prev": 0.12, "amount_ratio_5_20_prev": 0.10},
            {"symbol": "L2B", "open": 9.2, "prev_close": 9.2, "volume": 1_000_000, "listed_days": 300, "avg_amount_20_prev": 20_000_000.0, "market_cap_prev": 2_800_000_000.0, "is_st_name": False, "is_beijing_stock": False, "is_b_share": False, "ret_5_prev": 0.02, "ret_20_prev": 0.10, "amount_ratio_5_20_prev": 0.08},
            {"symbol": "L3A", "open": 9.0, "prev_close": 9.0, "volume": 1_000_000, "listed_days": 300, "avg_amount_20_prev": 20_000_000.0, "market_cap_prev": 4_500_000_000.0, "is_st_name": False, "is_beijing_stock": False, "is_b_share": False, "ret_5_prev": -0.03, "ret_20_prev": -0.10, "amount_ratio_5_20_prev": -0.10},
            {"symbol": "L3B", "open": 8.8, "prev_close": 8.8, "volume": 1_000_000, "listed_days": 300, "avg_amount_20_prev": 20_000_000.0, "market_cap_prev": 4_800_000_000.0, "is_st_name": False, "is_beijing_stock": False, "is_b_share": False, "ret_5_prev": -0.04, "ret_20_prev": -0.12, "amount_ratio_5_20_prev": -0.12},
            {"symbol": "OUT", "open": 8.6, "prev_close": 8.6, "volume": 1_000_000, "listed_days": 300, "avg_amount_20_prev": 20_000_000.0, "market_cap_prev": 12_000_000_000.0, "is_st_name": False, "is_beijing_stock": False, "is_b_share": False, "ret_5_prev": 0.10, "ret_20_prev": 0.35, "amount_ratio_5_20_prev": 0.40},
        ]
    )

    target, ranked_count = build_target_portfolio(frame, holdings=[], total_value_open=100_000.0, cfg=cfg)

    assert ranked_count == 7
    assert len(target) == 5
    assert len([symbol for symbol in target if symbol.startswith("L1")]) >= len([symbol for symbol in target if symbol.startswith("L3")])
    assert len([symbol for symbol in target if symbol.startswith("L1")]) >= 2
    assert len([symbol for symbol in target if symbol.startswith("L2")]) >= 1
    assert len([symbol for symbol in target if symbol.startswith("L3")]) >= 1
    assert "OUT" not in target


def test_execution_price_applies_slippage_in_expected_direction() -> None:
    assert execution_price("000001.SZ", pd.Timestamp("2026-04-03"), 10.0, 9.8, is_buy=True, slippage_bps=35) == 10.035
    assert execution_price("000001.SZ", pd.Timestamp("2026-04-03"), 10.0, 9.8, is_buy=False, slippage_bps=35) == 9.965


def test_available_trade_shares_respects_capacity_and_lot_rules() -> None:
    assert volume_units_to_shares(1234) == 123400
    assert available_trade_shares("000001.SZ", desired_shares=1200, remaining_shares=550, current_amount=1200, is_buy=False) == 500
    assert available_trade_shares("688981.SH", desired_shares=300, remaining_shares=180, current_amount=0, is_buy=True) == 0


def test_calendar_hedge_ratio_reads_schedule_by_month() -> None:
    cfg = MicrocapStrategyConfig(seasonal_hedge_schedule={1: 0.2, 4: 0.5})

    assert calendar_hedge_ratio(pd.Timestamp("2026-01-15"), cfg) == 0.2
    assert calendar_hedge_ratio(pd.Timestamp("2026-04-15"), cfg) == 0.5
    assert calendar_hedge_ratio(pd.Timestamp("2026-03-15"), cfg) == 0.0
