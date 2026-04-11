from __future__ import annotations

import pandas as pd

from quant_demo.experiment.joinquant_microcap_engine import (
    MicrocapStrategyConfig,
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
