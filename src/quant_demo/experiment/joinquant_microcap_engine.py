from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
from sqlalchemy import delete
from sqlalchemy.orm import sessionmaker

try:
    import akshare as ak
except ImportError:  # pragma: no cover - 依赖缺失时走降级逻辑
    ak = None

try:
    import baostock as bs
except ImportError:  # pragma: no cover - 依赖缺失时走降级逻辑
    bs = None

from quant_demo.adapters.qmt.bridge_client import QmtBridgeClient
from quant_demo.audit.report_service import AuditReportService
from quant_demo.core.config import AppSettings, StrategySettings
from quant_demo.db.models import (
    AssetSnapshotModel,
    AuditLogModel,
    OrderEventModel,
    OrderIntentModel,
    OrderModel,
    PositionSnapshotModel,
    RiskDecisionModel,
    TradeModel,
)
from quant_demo.experiment.evaluator import EvaluationResult, Evaluator

PROJECT_ROOT = Path(__file__).resolve().parents[3]


@dataclass(slots=True)
class MicrocapStrategyConfig:
    rebalance_frequency: str = "weekly"
    target_hold_num: int = 35
    buy_rank: int = 35
    keep_rank: int = 60
    query_limit: int = 1500
    min_list_days: int = 60
    min_avg_money_20: float = 8_000_000.0
    cash_buffer: float = 0.01
    min_price_floor: float = 1.0
    max_price_normal_hard: float = 50.0
    max_price_star_hard: float = 25.0
    max_overweight_ratio: float = 1.15
    benchmark_symbol: str = ""
    benchmark_name: str = ""
    hedge_symbol: str = ""
    hedge_name: str = ""
    hedge_history_adjustment: str = "none"
    seasonal_hedge_schedule: dict[int, float] = field(default_factory=dict)
    buy_slippage_bps: float = 35.0
    sell_slippage_bps: float = 35.0
    max_trade_volume_ratio: float = 0.03
    instrument_cache_path: str = "data/parquet/joinquant_microcap_instruments.parquet"
    capital_cache_path: str = "data/parquet/joinquant_microcap_capital.parquet"
    historical_st_cache_path: str = "data/parquet/joinquant_microcap_st_status.parquet"
    historical_name_cache_path: str = "data/parquet/joinquant_microcap_name_history.parquet"
    historical_sz_change_name_cache_path: str = "data/parquet/joinquant_microcap_sz_change_name.parquet"
    historical_status_rank_buffer: int = 3000
    layer_rotation_enabled: bool = False
    layer_market_cap_bounds: list[float] = field(default_factory=lambda: [0.0, 2_000_000_000.0, 4_000_000_000.0, 6_000_000_000.0, 8_000_000_000.0, 10_000_000_000.0])
    layer_base_slots: list[int] = field(default_factory=lambda: [4, 4, 3, 2, 2])
    layer_max_slots: list[int] = field(default_factory=lambda: [10, 10, 8, 6, 6])
    layer_holding_score_bonus: float = 0.05
    layer_score_momentum_weight: float = 0.5
    layer_score_breadth_weight: float = 0.3
    layer_score_liquidity_weight: float = 0.2
    industry_weighted_enabled: bool = False
    industry_sector_prefix: str = "GICS2"
    industry_cache_path: str = "data/parquet/joinquant_microcap_industries_gics2.parquet"
    industry_top_k: int = 8
    industry_keep_top_k: int = 10
    industry_min_slots: int = 1
    industry_max_slots: int = 6
    industry_lookback_days: int = 20
    industry_breadth_lookback_days: int = 5
    industry_liquidity_lookback_days: int = 5
    industry_score_momentum_weight: float = 0.5
    industry_score_breadth_weight: float = 0.3
    industry_score_liquidity_weight: float = 0.2
    industry_query_limit_multiplier: int = 3
    industry_min_candidate_count: int = 10
    min_holding_days: int = 10
    industry_holding_score_bonus: float = 0.08
    zhuang_filter_enabled: bool = False
    zhuang_filter_mode: str = "hard_filter"
    zhuang_final_replace_max_count: int = 5
    zhuang_amp120_min: float = 1.0
    zhuang_ret120_max: float = 0.25
    zhuang_shadow_amp_min: float = 0.06
    zhuang_upper_shadow_ratio: float = 0.45
    zhuang_upper_shadow_count60: int = 4
    zhuang_intraday_attack_min: float = 0.07
    zhuang_close_ret_max: float = 0.02
    zhuang_failed_attack_count120: int = 3
    zhuang_score_threshold: int = 2
    allow_st_buy: bool = False
    allow_beijing_stock_buy: bool = False
    exclude_star_st_buy: bool = True
    exclude_delisting_buy: bool = True
    surge_exit_enabled: bool = False
    surge_exit_gain_min: float = 0.30
    surge_big_bear_ret_max: float = -0.05
    surge_big_bear_amount_ratio_min: float = 1.80
    surge_big_bear_close_location_max: float = -0.20
    surge_ma5_break_buffer: float = 0.01
    surge_ma5_break_gain_min: float = 0.45
    surge_ma5_break_amount_ratio_min: float = 1.20
    surge_ma5_break_close_ret_max: float = 0.0
    monster_prelude_enabled: bool = False
    monster_market_cap_max: float = 5_000_000_000.0
    monster_ret60_max: float = 0.12
    monster_ret120_max: float = 0.20
    monster_range120_max: float = 1.10
    monster_drawdown_high120_max: float = -0.08
    monster_spike_lookback_days: int = 20
    monster_spike_amount_ratio_min: float = 1.60
    monster_ddx_burst_z_min: float = 1.20
    monster_strong_spike_lookback_days: int = 8
    monster_strong_spike_amount_ratio_min: float = 3.0
    monster_strong_spike_ddx_z_min: float = 2.5
    monster_strong_spike_close_ret_min: float = 0.03
    monster_strong_spike_close_location_min: float = 0.35
    monster_pullback_ret_min: float = -0.12
    monster_pullback_ret_max: float = 0.03
    monster_pullback_amount_ratio_min: float = 0.15
    monster_breakout_gap_min: float = -0.12
    monster_breakout_gap_max: float = 0.03
    monster_breakout_target_gap: float = -0.03
    monster_pre_attack_amount_ratio_min: float = 1.15
    monster_pre_attack_ddx_z_min: float = 0.30
    monster_holding_score_bonus: float = 0.08
    monster_buy_score_min: float = 0.68
    monster_acceleration_gain_min: float = 0.18
    monster_big_bear_profit_min: float = 0.18
    monster_big_bear_ret_max: float = -0.05
    monster_big_bear_amount_ratio_min: float = 1.80
    monster_big_bear_close_location_max: float = -0.20
    monster_ma5_break_buffer: float = 0.01
    monster_ma5_break_ret5_max: float = 0.0
    monster_setup_fail_hold_days: int = 8
    monster_pre_accel_ma10_break_buffer: float = 0.015
    monster_pre_accel_breakout_gap_max: float = -0.08
    monster_pre_accel_ddx_3_max: float = 0.0
    monster_pre_accel_amount_ratio_max: float = 0.05
    monster_stagnation_days: int = 12
    monster_stagnation_gain_max: float = 0.06

    @classmethod
    def from_strategy_settings(cls, settings: StrategySettings) -> "MicrocapStrategyConfig":
        payload = settings.extra or {}
        return cls(
            rebalance_frequency=str(settings.rebalance_frequency or "weekly").strip().lower() or "weekly",
            target_hold_num=max(1, int(payload.get("target_hold_num", 35))),
            buy_rank=max(1, int(payload.get("buy_rank", 35))),
            keep_rank=max(1, int(payload.get("keep_rank", 60))),
            query_limit=max(50, int(payload.get("query_limit", 1500))),
            min_list_days=max(0, int(payload.get("min_list_days", 60))),
            min_avg_money_20=float(payload.get("min_avg_money_20", 8_000_000.0)),
            cash_buffer=max(0.0, min(float(payload.get("cash_buffer", 0.01)), 0.2)),
            min_price_floor=max(0.01, float(payload.get("min_price_floor", 1.0))),
            max_price_normal_hard=max(0.01, float(payload.get("max_price_normal_hard", 50.0))),
            max_price_star_hard=max(0.01, float(payload.get("max_price_star_hard", 25.0))),
            max_overweight_ratio=max(1.0, float(payload.get("max_overweight_ratio", 1.15))),
            benchmark_symbol=str(payload.get("benchmark_symbol", "") or "").strip(),
            benchmark_name=str(payload.get("benchmark_name", "") or "").strip(),
            hedge_symbol=str(payload.get("hedge_symbol", "") or "").strip(),
            hedge_name=str(payload.get("hedge_name", "") or "").strip(),
            hedge_history_adjustment=str(payload.get("hedge_history_adjustment", "none") or "none").strip(),
            seasonal_hedge_schedule=_parse_seasonal_hedge_schedule(payload),
            buy_slippage_bps=max(0.0, min(float(payload.get("buy_slippage_bps", 35.0)), 500.0)),
            sell_slippage_bps=max(0.0, min(float(payload.get("sell_slippage_bps", 35.0)), 500.0)),
            max_trade_volume_ratio=max(0.0, min(float(payload.get("max_trade_volume_ratio", 0.03)), 1.0)),
            instrument_cache_path=str(payload.get("instrument_cache_path", "data/parquet/joinquant_microcap_instruments.parquet")),
            capital_cache_path=str(payload.get("capital_cache_path", "data/parquet/joinquant_microcap_capital.parquet")),
            historical_st_cache_path=str(payload.get("historical_st_cache_path", "data/parquet/joinquant_microcap_st_status.parquet")),
            historical_name_cache_path=str(payload.get("historical_name_cache_path", "data/parquet/joinquant_microcap_name_history.parquet")),
            historical_sz_change_name_cache_path=str(payload.get("historical_sz_change_name_cache_path", "data/parquet/joinquant_microcap_sz_change_name.parquet")),
            historical_status_rank_buffer=max(500, int(payload.get("historical_status_rank_buffer", 3000))),
            layer_rotation_enabled=bool(payload.get("layer_rotation_enabled")) or settings.implementation == "microcap_100b_layer_rot",
            layer_market_cap_bounds=_parse_float_list(payload.get("layer_market_cap_bounds"), [0.0, 2_000_000_000.0, 4_000_000_000.0, 6_000_000_000.0, 8_000_000_000.0, 10_000_000_000.0]),
            layer_base_slots=_parse_int_list(payload.get("layer_base_slots"), [4, 4, 3, 2, 2]),
            layer_max_slots=_parse_int_list(payload.get("layer_max_slots"), [10, 10, 8, 6, 6]),
            layer_holding_score_bonus=max(0.0, float(payload.get("layer_holding_score_bonus", 0.05))),
            layer_score_momentum_weight=max(0.0, float(payload.get("layer_score_momentum_weight", 0.5))),
            layer_score_breadth_weight=max(0.0, float(payload.get("layer_score_breadth_weight", 0.3))),
            layer_score_liquidity_weight=max(0.0, float(payload.get("layer_score_liquidity_weight", 0.2))),
            industry_weighted_enabled=bool(payload.get("industry_weighted_enabled")) or settings.implementation == "industry_weighted_microcap_alpha",
            industry_sector_prefix=str(payload.get("industry_sector_prefix", "GICS2") or "GICS2").strip() or "GICS2",
            industry_cache_path=str(payload.get("industry_cache_path", "data/parquet/joinquant_microcap_industries_gics2.parquet")),
            industry_top_k=max(1, int(payload.get("industry_top_k", 8))),
            industry_keep_top_k=max(1, int(payload.get("industry_keep_top_k", 10))),
            industry_min_slots=max(1, int(payload.get("industry_min_slots", 1))),
            industry_max_slots=max(1, int(payload.get("industry_max_slots", 6))),
            industry_lookback_days=max(2, int(payload.get("industry_lookback_days", 20))),
            industry_breadth_lookback_days=max(2, int(payload.get("industry_breadth_lookback_days", 5))),
            industry_liquidity_lookback_days=max(2, int(payload.get("industry_liquidity_lookback_days", 5))),
            industry_score_momentum_weight=max(0.0, float(payload.get("industry_score_momentum_weight", 0.5))),
            industry_score_breadth_weight=max(0.0, float(payload.get("industry_score_breadth_weight", 0.3))),
            industry_score_liquidity_weight=max(0.0, float(payload.get("industry_score_liquidity_weight", 0.2))),
            industry_query_limit_multiplier=max(1, int(payload.get("industry_query_limit_multiplier", 3))),
            industry_min_candidate_count=max(1, int(payload.get("industry_min_candidate_count", 10))),
            min_holding_days=max(0, int(payload.get("min_holding_days", 10))),
            industry_holding_score_bonus=max(0.0, float(payload.get("industry_holding_score_bonus", 0.08))),
            zhuang_filter_enabled=bool(payload.get("zhuang_filter_enabled")),
            zhuang_filter_mode=str(payload.get("zhuang_filter_mode", "hard_filter") or "hard_filter").strip().lower() or "hard_filter",
            zhuang_final_replace_max_count=max(0, int(payload.get("zhuang_final_replace_max_count", 5))),
            zhuang_amp120_min=max(0.0, float(payload.get("zhuang_amp120_min", 1.0))),
            zhuang_ret120_max=float(payload.get("zhuang_ret120_max", 0.25)),
            zhuang_shadow_amp_min=max(0.0, float(payload.get("zhuang_shadow_amp_min", 0.06))),
            zhuang_upper_shadow_ratio=max(0.0, min(float(payload.get("zhuang_upper_shadow_ratio", 0.45)), 1.0)),
            zhuang_upper_shadow_count60=max(1, int(payload.get("zhuang_upper_shadow_count60", 4))),
            zhuang_intraday_attack_min=max(0.0, float(payload.get("zhuang_intraday_attack_min", 0.07))),
            zhuang_close_ret_max=float(payload.get("zhuang_close_ret_max", 0.02)),
            zhuang_failed_attack_count120=max(1, int(payload.get("zhuang_failed_attack_count120", 3))),
            zhuang_score_threshold=max(1, int(payload.get("zhuang_score_threshold", 2))),
            allow_st_buy=bool(payload.get("allow_st_buy")),
            allow_beijing_stock_buy=bool(payload.get("allow_beijing_stock_buy")),
            exclude_star_st_buy=bool(payload.get("exclude_star_st_buy", True)),
            exclude_delisting_buy=bool(payload.get("exclude_delisting_buy", True)),
            surge_exit_enabled=bool(payload.get("surge_exit_enabled")) or settings.implementation == "joinquant_microcap_alpha_zfe",
            surge_exit_gain_min=max(0.0, float(payload.get("surge_exit_gain_min", 0.30))),
            surge_big_bear_ret_max=float(payload.get("surge_big_bear_ret_max", -0.05)),
            surge_big_bear_amount_ratio_min=max(0.0, float(payload.get("surge_big_bear_amount_ratio_min", 1.80))),
            surge_big_bear_close_location_max=float(payload.get("surge_big_bear_close_location_max", -0.20)),
            surge_ma5_break_buffer=max(0.0, float(payload.get("surge_ma5_break_buffer", 0.01))),
            surge_ma5_break_gain_min=max(0.0, float(payload.get("surge_ma5_break_gain_min", 0.45))),
            surge_ma5_break_amount_ratio_min=max(0.0, float(payload.get("surge_ma5_break_amount_ratio_min", 1.20))),
            surge_ma5_break_close_ret_max=float(payload.get("surge_ma5_break_close_ret_max", 0.0)),
            monster_prelude_enabled=bool(payload.get("monster_prelude_enabled")) or settings.implementation == "monster_prelude_alpha",
            monster_market_cap_max=max(0.0, float(payload.get("monster_market_cap_max", 5_000_000_000.0))),
            monster_ret60_max=float(payload.get("monster_ret60_max", 0.12)),
            monster_ret120_max=float(payload.get("monster_ret120_max", 0.20)),
            monster_range120_max=max(0.0, float(payload.get("monster_range120_max", 1.10))),
            monster_drawdown_high120_max=float(payload.get("monster_drawdown_high120_max", -0.08)),
            monster_spike_lookback_days=max(5, int(payload.get("monster_spike_lookback_days", 20))),
            monster_spike_amount_ratio_min=max(0.0, float(payload.get("monster_spike_amount_ratio_min", 1.60))),
            monster_ddx_burst_z_min=float(payload.get("monster_ddx_burst_z_min", 1.20)),
            monster_strong_spike_lookback_days=max(3, int(payload.get("monster_strong_spike_lookback_days", 8))),
            monster_strong_spike_amount_ratio_min=max(0.0, float(payload.get("monster_strong_spike_amount_ratio_min", 3.0))),
            monster_strong_spike_ddx_z_min=float(payload.get("monster_strong_spike_ddx_z_min", 2.5)),
            monster_strong_spike_close_ret_min=float(payload.get("monster_strong_spike_close_ret_min", 0.03)),
            monster_strong_spike_close_location_min=float(payload.get("monster_strong_spike_close_location_min", 0.35)),
            monster_pullback_ret_min=float(payload.get("monster_pullback_ret_min", -0.12)),
            monster_pullback_ret_max=float(payload.get("monster_pullback_ret_max", 0.03)),
            monster_pullback_amount_ratio_min=float(payload.get("monster_pullback_amount_ratio_min", 0.15)),
            monster_breakout_gap_min=float(payload.get("monster_breakout_gap_min", -0.12)),
            monster_breakout_gap_max=float(payload.get("monster_breakout_gap_max", 0.03)),
            monster_breakout_target_gap=float(payload.get("monster_breakout_target_gap", -0.03)),
            monster_pre_attack_amount_ratio_min=float(payload.get("monster_pre_attack_amount_ratio_min", 1.15)),
            monster_pre_attack_ddx_z_min=float(payload.get("monster_pre_attack_ddx_z_min", 0.30)),
            monster_holding_score_bonus=max(0.0, float(payload.get("monster_holding_score_bonus", 0.08))),
            monster_buy_score_min=float(payload.get("monster_buy_score_min", 0.68)),
            monster_acceleration_gain_min=float(payload.get("monster_acceleration_gain_min", 0.18)),
            monster_big_bear_profit_min=float(payload.get("monster_big_bear_profit_min", 0.18)),
            monster_big_bear_ret_max=float(payload.get("monster_big_bear_ret_max", -0.05)),
            monster_big_bear_amount_ratio_min=max(0.0, float(payload.get("monster_big_bear_amount_ratio_min", 1.80))),
            monster_big_bear_close_location_max=float(payload.get("monster_big_bear_close_location_max", -0.20)),
            monster_ma5_break_buffer=max(0.0, float(payload.get("monster_ma5_break_buffer", 0.01))),
            monster_ma5_break_ret5_max=float(payload.get("monster_ma5_break_ret5_max", 0.0)),
            monster_setup_fail_hold_days=max(1, int(payload.get("monster_setup_fail_hold_days", 8))),
            monster_pre_accel_ma10_break_buffer=max(0.0, float(payload.get("monster_pre_accel_ma10_break_buffer", 0.015))),
            monster_pre_accel_breakout_gap_max=float(payload.get("monster_pre_accel_breakout_gap_max", -0.08)),
            monster_pre_accel_ddx_3_max=float(payload.get("monster_pre_accel_ddx_3_max", 0.0)),
            monster_pre_accel_amount_ratio_max=float(payload.get("monster_pre_accel_amount_ratio_max", 0.05)),
            monster_stagnation_days=max(1, int(payload.get("monster_stagnation_days", 12))),
            monster_stagnation_gain_max=float(payload.get("monster_stagnation_gain_max", 0.06)),
        )


def _batched(items: Iterable[str], size: int) -> Iterable[list[str]]:
    batch: list[str] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _resolve_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return (PROJECT_ROOT / path).resolve()


def _clip_ratio(value: Any) -> float:
    return max(0.0, min(float(value), 1.0))


def _parse_float_list(raw: Any, default: list[float]) -> list[float]:
    if not isinstance(raw, (list, tuple)):
        return list(default)
    values: list[float] = []
    for item in raw:
        try:
            values.append(float(item))
        except (TypeError, ValueError):
            continue
    return values or list(default)


def _parse_int_list(raw: Any, default: list[int]) -> list[int]:
    if not isinstance(raw, (list, tuple)):
        return list(default)
    values: list[int] = []
    for item in raw:
        try:
            values.append(int(item))
        except (TypeError, ValueError):
            continue
    return values or list(default)


def _parse_seasonal_hedge_schedule(payload: dict[str, Any]) -> dict[int, float]:
    schedule: dict[int, float] = {}
    raw_schedule = payload.get("seasonal_hedge_schedule")
    if isinstance(raw_schedule, dict):
        for raw_month, raw_ratio in raw_schedule.items():
            try:
                month = int(raw_month)
            except (TypeError, ValueError):
                continue
            if 1 <= month <= 12:
                ratio = _clip_ratio(raw_ratio)
                if ratio > 0:
                    schedule[month] = ratio
        return dict(sorted(schedule.items()))

    raw_months = payload.get("seasonal_hedge_months") or []
    if not isinstance(raw_months, (list, tuple, set)):
        return {}
    ratio = _clip_ratio(payload.get("seasonal_hedge_ratio", 0.0))
    if ratio <= 0:
        return {}
    for raw_month in raw_months:
        try:
            month = int(raw_month)
        except (TypeError, ValueError):
            continue
        if 1 <= month <= 12:
            schedule[month] = ratio
    return dict(sorted(schedule.items()))


def _special_treatment_flags_from_name(name: Any) -> tuple[bool, bool, bool]:
    raw_name = str(name or "")
    normalized_name = raw_name.upper()
    is_star_st = "*ST" in normalized_name
    is_delisting = "退" in raw_name
    is_st = ("ST" in normalized_name) or is_delisting
    return is_st, is_star_st, is_delisting


def _infer_special_treatment_frame_from_names(names: pd.Series) -> pd.DataFrame:
    values = names.fillna("").map(_special_treatment_flags_from_name)
    return pd.DataFrame(
        values.tolist(),
        index=names.index,
        columns=["is_st_name", "is_star_st_name", "is_delisting_name"],
    )


def _symbol_market(symbol: str) -> str:
    raw_symbol = str(symbol or "").upper()
    if raw_symbol.endswith(".SZ"):
        return "SZ"
    if raw_symbol.endswith(".SH"):
        return "SH"
    if raw_symbol.endswith(".BJ") or raw_symbol.endswith(".XBJ"):
        return "BJ"
    return ""


def _symbol_code(symbol: str) -> str:
    return str(symbol or "").split(".", 1)[0]


def _to_baostock_symbol(symbol: str) -> str | None:
    market = _symbol_market(symbol)
    code = _symbol_code(symbol)
    if market == "SZ":
        return f"sz.{code}"
    if market == "SH":
        return f"sh.{code}"
    return None


def _true_segments(flags: Iterable[bool]) -> list[tuple[int, int]]:
    values = [bool(item) for item in flags]
    segments: list[tuple[int, int]] = []
    start: int | None = None
    for index, value in enumerate(values):
        if bool(value):
            if start is None:
                start = index
        elif start is not None:
            segments.append((start, index - 1))
            start = None
    if start is not None:
        segments.append((start, len(values) - 1))
    return segments


def _special_name_segments(names: list[str]) -> list[dict[str, bool]]:
    segments: list[dict[str, bool]] = []
    current: dict[str, bool] | None = None
    for raw_name in names:
        is_st, is_star_st, is_delisting = _special_treatment_flags_from_name(raw_name)
        if is_st:
            if current is None:
                current = {"is_star_st_name": False, "is_delisting_name": False}
            current["is_star_st_name"] = bool(current["is_star_st_name"] or is_star_st)
            current["is_delisting_name"] = bool(current["is_delisting_name"] or is_delisting)
        elif current is not None:
            segments.append(current)
            current = None
    if current is not None:
        segments.append(current)
    return segments


def _build_sz_special_treatment_for_symbol(
    trading_dates: pd.Series,
    current_name: str,
    change_rows: pd.DataFrame,
) -> pd.DataFrame:
    dates = pd.Series(pd.to_datetime(trading_dates, errors="coerce")).dropna().sort_values().drop_duplicates().reset_index(drop=True)
    result = pd.DataFrame({"trading_date": dates})
    if result.empty:
        result["is_st_name"] = pd.Series(dtype=bool)
        result["is_star_st_name"] = pd.Series(dtype=bool)
        result["is_delisting_name"] = pd.Series(dtype=bool)
        return result
    if change_rows.empty:
        names = pd.Series([current_name] * len(result), index=result.index, dtype=str)
        flags = _infer_special_treatment_frame_from_names(names)
        return pd.concat([result, flags.reset_index(drop=True)], axis=1)
    ordered_changes = change_rows.sort_values("change_date").reset_index(drop=True).copy()
    base_name = str(ordered_changes["old_name"].dropna().iloc[0] if ordered_changes["old_name"].notna().any() else current_name)
    effective = ordered_changes.loc[:, ["change_date", "new_name"]].rename(columns={"change_date": "effective_date"}).copy()
    merged = pd.merge_asof(
        result.sort_values("trading_date"),
        effective.sort_values("effective_date"),
        left_on="trading_date",
        right_on="effective_date",
        direction="backward",
    )
    names = merged["new_name"].fillna(base_name).fillna(current_name).astype(str)
    flags = _infer_special_treatment_frame_from_names(names)
    return pd.concat([result, flags.reset_index(drop=True)], axis=1)


def _build_segmented_special_treatment_for_symbol(
    trading_dates: pd.Series,
    current_name: str,
    is_st_history: pd.Series,
    ordered_names: list[str],
) -> pd.DataFrame:
    dates = pd.Series(pd.to_datetime(trading_dates, errors="coerce")).dropna().sort_values().drop_duplicates().reset_index(drop=True)
    is_st_series = pd.Series(is_st_history, index=range(len(is_st_history))).astype(bool)
    result = pd.DataFrame({"trading_date": dates})
    if result.empty:
        result["is_st_name"] = pd.Series(dtype=bool)
        result["is_star_st_name"] = pd.Series(dtype=bool)
        result["is_delisting_name"] = pd.Series(dtype=bool)
        return result
    current_is_st, current_is_star_st, current_is_delisting = _special_treatment_flags_from_name(current_name)
    result["is_st_name"] = is_st_series.reindex(result.index, fill_value=False).astype(bool)
    result["is_star_st_name"] = False
    result["is_delisting_name"] = False
    special_segments = _special_name_segments([str(item or "") for item in ordered_names])
    fallback_segment = {
        "is_star_st_name": bool(current_is_star_st or any(segment["is_star_st_name"] for segment in special_segments)),
        "is_delisting_name": bool(current_is_delisting or any(segment["is_delisting_name"] for segment in special_segments)),
    }
    true_segments = _true_segments(result["is_st_name"].tolist())
    for index, (start, end) in enumerate(true_segments):
        segment_flags = special_segments[index] if index < len(special_segments) else fallback_segment
        if segment_flags["is_star_st_name"]:
            result.loc[start:end, "is_star_st_name"] = True
        if segment_flags["is_delisting_name"]:
            result.loc[start:end, "is_delisting_name"] = True
    if current_is_st and not true_segments:
        result["is_st_name"] = current_is_st
        result["is_star_st_name"] = current_is_star_st
        result["is_delisting_name"] = current_is_delisting
    return result


def is_star_stock(symbol: str) -> bool:
    return str(symbol or "").upper().startswith("688")


def is_beijing_stock(symbol: str) -> bool:
    raw = str(symbol or "").upper()
    if raw.startswith(("4", "8")):
        return True
    return raw.endswith(".BJ") or raw.endswith(".XBJ")


def is_b_share(symbol: str) -> bool:
    raw = str(symbol or "").upper()
    return raw.startswith("200") or raw.startswith("900")


def get_min_buy_amount(symbol: str) -> int:
    return 200 if is_star_stock(symbol) else 100


def estimate_min_order_cost(symbol: str, price: float) -> float:
    return float(price) * get_min_buy_amount(symbol) * 1.002


def normalize_buy_amount(symbol: str, raw_amount: int) -> int:
    raw_amount = int(max(0, raw_amount))
    if raw_amount <= 0:
        return 0
    if is_star_stock(symbol):
        return raw_amount if raw_amount >= 200 else 0
    return (raw_amount // 100) * 100


def normalize_sell_amount(symbol: str, raw_amount: int, current_amount: int) -> int:
    raw_amount = int(max(0, raw_amount))
    current_amount = int(max(0, current_amount))
    if raw_amount <= 0 or current_amount <= 0:
        return 0
    if is_star_stock(symbol):
        if raw_amount >= 200:
            return min(raw_amount, current_amount)
        if current_amount < 200 and raw_amount >= current_amount:
            return current_amount
        return 0
    lot_amount = (raw_amount // 100) * 100
    if lot_amount >= 100:
        return min(lot_amount, current_amount)
    if current_amount < 100 and raw_amount >= current_amount:
        return current_amount
    return 0


def adjust_target_amount_for_rules(symbol: str, current_amount: int, raw_target_amount: int) -> int:
    current_amount = int(max(0, current_amount))
    raw_target_amount = int(max(0, raw_target_amount))
    if raw_target_amount == current_amount:
        return current_amount
    if raw_target_amount > current_amount:
        buy_delta = raw_target_amount - current_amount
        valid_buy_delta = normalize_buy_amount(symbol, buy_delta)
        if valid_buy_delta <= 0:
            return current_amount
        return current_amount + valid_buy_delta
    sell_delta = current_amount - raw_target_amount
    valid_sell_delta = normalize_sell_amount(symbol, sell_delta, current_amount)
    if valid_sell_delta <= 0:
        return current_amount
    return max(0, current_amount - valid_sell_delta)


def calc_target_amount_by_value(symbol: str, target_value: float, price: float) -> int:
    if price <= 0:
        return 0
    raw_amount = int(float(target_value) / float(price))
    if is_star_stock(symbol):
        return raw_amount if raw_amount >= 200 else 0
    return (raw_amount // 100) * 100


def calc_amount_by_cash(symbol: str, cash: float, price: float) -> int:
    if price <= 0 or cash <= 0:
        return 0
    effective_cash = max(float(cash) - 5.0, 0.0)
    raw_amount = int(effective_cash / (float(price) * 1.0003))
    return normalize_buy_amount(symbol, raw_amount)


def buy_fee(amount: float) -> float:
    if amount <= 0:
        return 0.0
    return max(amount * 0.0003, 5.0)


def sell_fee(amount: float) -> float:
    if amount <= 0:
        return 0.0
    return max(amount * 0.0003, 5.0) + amount * 0.001


def board_limit_ratio(symbol: str, trade_date: pd.Timestamp) -> float:
    raw = str(symbol or "").upper()
    if is_star_stock(raw):
        return 0.20
    if raw.startswith(("300", "301")):
        return 0.20 if trade_date >= pd.Timestamp("2020-08-24") else 0.10
    return 0.10


def resolve_price_limits(symbol: str, trade_date: pd.Timestamp, prev_close: float) -> tuple[float | None, float | None]:
    if pd.isna(prev_close) or float(prev_close) <= 0:
        return None, None
    ratio = board_limit_ratio(symbol, trade_date)
    high_limit = round(float(prev_close) * (1.0 + ratio), 2)
    low_limit = round(float(prev_close) * (1.0 - ratio), 2)
    return high_limit, low_limit


def can_trade(symbol: str, trade_date: pd.Timestamp, open_price: float, volume: float, prev_close: float, *, is_buy: bool) -> bool:
    if pd.isna(open_price) or float(open_price) <= 0:
        return False
    if pd.isna(volume) or float(volume) <= 0:
        return False
    if pd.isna(prev_close) or float(prev_close) <= 0:
        return True
    high_limit, low_limit = resolve_price_limits(symbol, trade_date, prev_close)
    if is_buy and float(open_price) >= high_limit * 0.995:
        return False
    if (not is_buy) and float(open_price) <= low_limit * 1.005:
        return False
    return True


def execution_price(symbol: str, trade_date: pd.Timestamp, open_price: float, prev_close: float, *, is_buy: bool, slippage_bps: float) -> float:
    if pd.isna(open_price) or float(open_price) <= 0:
        return 0.0
    adjusted = float(open_price) * (1.0 + float(slippage_bps) / 10000.0) if is_buy else float(open_price) * (1.0 - float(slippage_bps) / 10000.0)
    high_limit, low_limit = resolve_price_limits(symbol, trade_date, prev_close)
    if high_limit is not None:
        adjusted = min(adjusted, high_limit)
    if low_limit is not None:
        adjusted = max(adjusted, low_limit)
    return round(max(adjusted, 0.01), 4)


def volume_units_to_shares(volume: float) -> int:
    if pd.isna(volume):
        return 0
    return int(max(float(volume or 0.0), 0.0) * 100.0)


def available_trade_shares(symbol: str, desired_shares: int, remaining_shares: int, current_amount: int, *, is_buy: bool) -> int:
    capped = min(int(max(desired_shares, 0)), int(max(remaining_shares, 0)))
    if capped <= 0:
        return 0
    if is_buy:
        return normalize_buy_amount(symbol, capped)
    return normalize_sell_amount(symbol, capped, current_amount)


def calendar_hedge_ratio(trade_date: pd.Timestamp, cfg: MicrocapStrategyConfig) -> float:
    return float(cfg.seasonal_hedge_schedule.get(int(pd.Timestamp(trade_date).month), 0.0))


def get_dynamic_price_cap(symbol: str, slot_value: float, cfg: MicrocapStrategyConfig) -> float:
    min_amount = get_min_buy_amount(symbol)
    dynamic_cap = float(slot_value) / float(min_amount) * 0.98 if min_amount > 0 else 0.0
    if is_star_stock(symbol):
        return min(cfg.max_price_star_hard, dynamic_cap)
    return min(cfg.max_price_normal_hard, dynamic_cap)


def fit_target_count_by_cash(
    candidates: list[str],
    price_lookup: dict[str, float],
    invest_value: float,
    cfg: MicrocapStrategyConfig,
) -> list[str]:
    if not candidates:
        return []
    max_n = min(cfg.target_hold_num, len(candidates))
    for count in range(max_n, 0, -1):
        each_value = float(invest_value) / float(count)
        ok = True
        for symbol in candidates[:count]:
            price = float(price_lookup.get(symbol, 0.0) or 0.0)
            if price <= 0 or each_value < estimate_min_order_cost(symbol, price):
                ok = False
                break
        if ok:
            return candidates[:count]
    return []


def _scaled_rank_limit(base_rank: int, target_count: int, total_target_count: int) -> int:
    if target_count <= 0:
        return 0
    if total_target_count <= 0:
        return target_count
    scaled = int(np.ceil(float(base_rank) * float(target_count) / float(total_target_count)))
    return max(target_count, scaled)


def _select_from_ranked_symbols(ranked: list[str], holdings: list[str], target_count: int, cfg: MicrocapStrategyConfig) -> list[str]:
    if target_count <= 0 or not ranked:
        return []
    keep_rank = min(len(ranked), _scaled_rank_limit(cfg.keep_rank, target_count, cfg.target_hold_num))
    buy_rank = min(len(ranked), _scaled_rank_limit(cfg.buy_rank, target_count, cfg.target_hold_num))
    target: list[str] = []
    protected = [symbol for symbol in holdings if symbol in ranked]
    for symbol in protected:
        if symbol not in target:
            target.append(symbol)
        if len(target) >= target_count:
            return target[:target_count]
    for symbol in ranked[:keep_rank]:
        if symbol in holdings and symbol not in target:
            target.append(symbol)
        if len(target) >= target_count:
            return target[:target_count]
    for symbol in ranked[:buy_rank]:
        if symbol not in target:
            target.append(symbol)
        if len(target) >= target_count:
            return target[:target_count]
    for symbol in ranked:
        if symbol not in target:
            target.append(symbol)
        if len(target) >= target_count:
            break
    return target[:target_count]


def _apply_zhuang_buy_filter(
    working: pd.DataFrame,
    holdings: list[str],
    cfg: MicrocapStrategyConfig,
) -> tuple[pd.DataFrame, int]:
    if (
        working.empty
        or not cfg.zhuang_filter_enabled
        or str(cfg.zhuang_filter_mode or "hard_filter").strip().lower() != "hard_filter"
        or "is_old_zhuang_suspect_prev" not in working.columns
    ):
        return working, 0
    holding_set = {str(symbol) for symbol in holdings}
    symbol_series = working["symbol"].astype(str)
    holding_mask = symbol_series.isin(holding_set)
    suspect_mask = pd.Series(working["is_old_zhuang_suspect_prev"], index=working.index, dtype="boolean").fillna(False)
    filtered_count = int((~holding_mask & suspect_mask).sum())
    filtered = working[holding_mask | (~suspect_mask)].copy()
    return filtered, filtered_count


def _apply_zhuang_final_replacements(
    targets: list[str],
    ranked: list[str],
    holdings: list[str],
    suspect_lookup: dict[str, bool],
    cfg: MicrocapStrategyConfig,
) -> tuple[list[str], int]:
    if (
        not targets
        or not cfg.zhuang_filter_enabled
        or str(cfg.zhuang_filter_mode or "").strip().lower() != "final_replace"
        or cfg.zhuang_final_replace_max_count <= 0
    ):
        return list(targets), 0
    holding_set = {str(symbol) for symbol in holdings}
    selected = [str(symbol) for symbol in targets]
    selected_set = set(selected)
    candidate_pool = [
        str(symbol)
        for symbol in ranked
        if str(symbol) not in selected_set and not bool(suspect_lookup.get(str(symbol), False))
    ]
    replaced = 0
    for index, symbol in enumerate(list(selected)):
        if replaced >= cfg.zhuang_final_replace_max_count:
            break
        if symbol in holding_set:
            continue
        if not bool(suspect_lookup.get(symbol, False)):
            continue
        replacement = None
        while candidate_pool:
            candidate = candidate_pool.pop(0)
            if candidate not in selected_set:
                replacement = candidate
                break
        if replacement is None:
            break
        selected[index] = replacement
        selected_set.remove(symbol)
        selected_set.add(replacement)
        replaced += 1
    return selected[: len(targets)], replaced


def _rank_percentile(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    result = pd.Series(0.5, index=values.index, dtype=float)
    valid = numeric.notna()
    if valid.any():
        result.loc[valid] = numeric.loc[valid].rank(method="average", pct=True).astype(float)
    return result.fillna(0.5)


def _inverse_rank_percentile(values: pd.Series) -> pd.Series:
    return 1.0 - _rank_percentile(values)


def _proximity_rank(values: pd.Series, target: float) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    distance = (numeric - float(target)).abs()
    return _inverse_rank_percentile(distance)


def _build_monster_prelude_ranked(
    working: pd.DataFrame,
    holdings: list[str],
    cfg: MicrocapStrategyConfig,
) -> tuple[list[str], int]:
    if working.empty:
        return [], 0
    pool = working.copy()
    pool = pool[
        pd.to_numeric(pool["market_cap_prev"], errors="coerce").fillna(float("inf")).le(cfg.monster_market_cap_max)
    ].copy()
    if pool.empty:
        return [], 0
    range_120 = pd.to_numeric(pool.get("range_120_prev"), errors="coerce")
    drawdown_120 = pd.to_numeric(pool.get("drawdown_from_high_120_prev"), errors="coerce")
    regime_mask = (
        pd.to_numeric(pool.get("ret_60_prev"), errors="coerce").fillna(float("inf")).le(cfg.monster_ret60_max)
        & pd.to_numeric(pool.get("ret_120_prev"), errors="coerce").fillna(float("inf")).le(cfg.monster_ret120_max)
        & (
            range_120.fillna(float("inf")).le(cfg.monster_range120_max)
            | drawdown_120.fillna(0.0).le(cfg.monster_drawdown_high120_max)
        )
    )
    setup_mask = (
        pd.to_numeric(pool.get("monster_recent_strong_spike_count_prev"), errors="coerce").fillna(0.0).ge(1.0)
        & pd.to_numeric(pool.get("monster_recent_strong_spike_amount_max_prev"), errors="coerce").fillna(0.0).ge(cfg.monster_strong_spike_amount_ratio_min)
        & pd.to_numeric(pool.get("monster_recent_strong_ddx_burst_max_prev"), errors="coerce").fillna(-99.0).ge(cfg.monster_strong_spike_ddx_z_min)
        & pd.to_numeric(pool.get("ret_5_prev"), errors="coerce").fillna(float("inf")).between(
            cfg.monster_pullback_ret_min,
            cfg.monster_pullback_ret_max,
        )
        & pd.to_numeric(pool.get("amount_ratio_5_20_prev"), errors="coerce").fillna(-1.0).ge(cfg.monster_pullback_amount_ratio_min)
        & pd.to_numeric(pool.get("breakout_gap_20_prev"), errors="coerce").fillna(float("inf")).between(
            cfg.monster_breakout_gap_min,
            cfg.monster_breakout_gap_max,
        )
        & (
            pd.to_numeric(pool.get("amount_ratio_1_20_prev"), errors="coerce").fillna(-1.0).ge(cfg.monster_pre_attack_amount_ratio_min)
            | pd.to_numeric(pool.get("ddx_burst_z_prev"), errors="coerce").fillna(-99.0).ge(cfg.monster_pre_attack_ddx_z_min)
        )
    )
    pool = pool[regime_mask & setup_mask].copy()
    if pool.empty:
        return [], 0
    holding_set = {str(symbol) for symbol in holdings}
    pool["has_holding"] = pool["symbol"].astype(str).isin(holding_set)
    size_score = _inverse_rank_percentile(pool["market_cap_prev"])
    spike_count_score = _rank_percentile(pool["monster_recent_strong_spike_count_prev"])
    spike_strength_score = _rank_percentile(pool["monster_recent_strong_spike_amount_max_prev"])
    ddx_score = _rank_percentile(pool["monster_recent_strong_ddx_burst_max_prev"])
    pullback_support_score = _rank_percentile(pool["amount_ratio_5_20_prev"])
    ddx_keep_score = _rank_percentile(pool["ddx_proxy_3_prev"])
    breakout_score = _proximity_rank(pool["breakout_gap_20_prev"], cfg.monster_breakout_target_gap)
    quiet_base_score = (
        _inverse_rank_percentile(pool["ret_120_prev"].clip(lower=-1.0, upper=1.0))
        + _inverse_rank_percentile(pool["range_120_prev"])
    ) / 2.0
    pool["monster_score"] = (
        0.18 * size_score
        + 0.18 * spike_count_score
        + 0.17 * spike_strength_score
        + 0.17 * ddx_score
        + 0.10 * pullback_support_score
        + 0.10 * ddx_keep_score
        + 0.10 * breakout_score
        + 0.10 * quiet_base_score
    )
    if cfg.monster_holding_score_bonus > 0:
        pool["monster_score"] = pool["monster_score"] + pool["has_holding"].astype(float) * float(cfg.monster_holding_score_bonus)
    if cfg.monster_buy_score_min > 0:
        pool = pool[pool["monster_score"].fillna(-1.0) >= cfg.monster_buy_score_min].copy()
    if pool.empty:
        return [], 0
    ranked = (
        pool.sort_values(["monster_score", "market_cap_prev", "symbol"], ascending=[False, True, True])["symbol"]
        .astype(str)
        .head(max(cfg.query_limit, cfg.keep_rank + 30, cfg.target_hold_num * 6))
        .tolist()
    )
    return ranked, int(len(pool))


def _monster_exit_reason(
    row: Any,
    meta: dict[str, Any],
    trade_date: pd.Timestamp,
    cfg: MicrocapStrategyConfig,
) -> str | None:
    if not cfg.monster_prelude_enabled:
        return None
    hold_since = pd.Timestamp(meta.get("hold_since", trade_date)).normalize()
    holding_days = int((pd.Timestamp(trade_date).normalize() - hold_since).days)
    if holding_days < cfg.min_holding_days:
        return None
    avg_cost = float(meta.get("avg_cost", 0.0) or 0.0)
    open_price = float(getattr(row, "open", 0.0) or 0.0)
    if avg_cost <= 0.0 or open_price <= 0.0:
        return None
    gain_open = open_price / avg_cost - 1.0
    peak_close = float(meta.get("peak_close", avg_cost) or avg_cost)
    gain_peak = peak_close / avg_cost - 1.0 if avg_cost > 0.0 else 0.0
    accelerated = max(gain_open, gain_peak) >= cfg.monster_acceleration_gain_min
    close_ret_prev = float(getattr(row, "close_ret_1_prev", np.nan) or np.nan)
    amount_ratio_prev = float(getattr(row, "amount_ratio_1_20_prev", np.nan) or np.nan)
    close_location_prev = float(getattr(row, "close_location_prev", np.nan) or np.nan)
    if (
        max(gain_open, gain_peak) >= cfg.monster_big_bear_profit_min
        and pd.notna(close_ret_prev)
        and pd.notna(amount_ratio_prev)
        and pd.notna(close_location_prev)
        and close_ret_prev <= cfg.monster_big_bear_ret_max
        and amount_ratio_prev >= cfg.monster_big_bear_amount_ratio_min
        and close_location_prev <= cfg.monster_big_bear_close_location_max
    ):
        return "monster_exit_big_bear"
    ma5_prev = float(getattr(row, "ma5_prev", np.nan) or np.nan)
    ma10_prev = float(getattr(row, "ma10_prev", np.nan) or np.nan)
    prev_close = float(getattr(row, "prev_close", np.nan) or np.nan)
    ret_5_prev = float(getattr(row, "ret_5_prev", np.nan) or np.nan)
    if (
        accelerated
        and pd.notna(ma5_prev)
        and ma5_prev > 0.0
        and pd.notna(prev_close)
        and prev_close < ma5_prev * (1.0 - cfg.monster_ma5_break_buffer)
        and pd.notna(ret_5_prev)
        and ret_5_prev <= cfg.monster_ma5_break_ret5_max
    ):
        return "monster_exit_ma5_break"
    breakout_gap_prev = float(getattr(row, "breakout_gap_20_prev", np.nan) or np.nan)
    ddx_proxy_3_prev = float(getattr(row, "ddx_proxy_3_prev", np.nan) or np.nan)
    amount_ratio_5_20_prev = float(getattr(row, "amount_ratio_5_20_prev", np.nan) or np.nan)
    if (
        not accelerated
        and holding_days >= cfg.monster_setup_fail_hold_days
        and pd.notna(ma10_prev)
        and ma10_prev > 0.0
        and pd.notna(prev_close)
        and prev_close < ma10_prev * (1.0 - cfg.monster_pre_accel_ma10_break_buffer)
        and pd.notna(breakout_gap_prev)
        and breakout_gap_prev <= cfg.monster_pre_accel_breakout_gap_max
        and pd.notna(ddx_proxy_3_prev)
        and ddx_proxy_3_prev <= cfg.monster_pre_accel_ddx_3_max
        and pd.notna(amount_ratio_5_20_prev)
        and amount_ratio_5_20_prev <= cfg.monster_pre_accel_amount_ratio_max
    ):
        return "monster_exit_setup_fail"
    if (
        pd.notna(ma5_prev)
        and holding_days >= cfg.monster_stagnation_days
        and peak_close < avg_cost * (1.0 + cfg.monster_stagnation_gain_max)
    ):
        return "monster_exit_stagnation"
    return None


def _surge_exit_reason(
    row: Any,
    meta: dict[str, Any],
    cfg: MicrocapStrategyConfig,
) -> str | None:
    if not cfg.surge_exit_enabled:
        return None
    avg_cost = float(meta.get("avg_cost", 0.0) or 0.0)
    if avg_cost <= 0.0:
        return None
    open_price = float(getattr(row, "open", np.nan) or np.nan)
    peak_close = float(meta.get("peak_close", avg_cost) or avg_cost)
    gain_open = open_price / avg_cost - 1.0 if pd.notna(open_price) and avg_cost > 0.0 else 0.0
    gain_peak = peak_close / avg_cost - 1.0 if avg_cost > 0.0 else 0.0
    if max(gain_open, gain_peak) < cfg.surge_exit_gain_min:
        return None
    close_ret_prev = float(getattr(row, "close_ret_1_prev", np.nan) or np.nan)
    amount_ratio_prev = float(getattr(row, "amount_ratio_1_20_prev", np.nan) or np.nan)
    close_location_prev = float(getattr(row, "close_location_prev", np.nan) or np.nan)
    if (
        pd.notna(close_ret_prev)
        and pd.notna(amount_ratio_prev)
        and pd.notna(close_location_prev)
        and close_ret_prev <= cfg.surge_big_bear_ret_max
        and amount_ratio_prev >= cfg.surge_big_bear_amount_ratio_min
        and close_location_prev <= cfg.surge_big_bear_close_location_max
    ):
        return "surge_exit_big_bear"
    ma5_prev = float(getattr(row, "ma5_prev", np.nan) or np.nan)
    prev_close = float(getattr(row, "prev_close", np.nan) or np.nan)
    if (
        max(gain_open, gain_peak) >= cfg.surge_ma5_break_gain_min
        and
        pd.notna(ma5_prev)
        and ma5_prev > 0.0
        and pd.notna(prev_close)
        and prev_close < ma5_prev * (1.0 - cfg.surge_ma5_break_buffer)
        and pd.notna(amount_ratio_prev)
        and amount_ratio_prev >= cfg.surge_ma5_break_amount_ratio_min
        and pd.notna(close_ret_prev)
        and close_ret_prev <= cfg.surge_ma5_break_close_ret_max
    ):
        return "surge_exit_ma5_break"
    return None


def _resolve_layer_specs(cfg: MicrocapStrategyConfig) -> list[dict[str, Any]]:
    bounds = [float(item) for item in cfg.layer_market_cap_bounds if float(item) >= 0.0]
    if len(bounds) < 2:
        bounds = [0.0, 2_000_000_000.0, 4_000_000_000.0, 6_000_000_000.0, 8_000_000_000.0, 10_000_000_000.0]
    bounds = sorted(set(bounds))
    if len(bounds) < 2:
        return []
    layer_count = len(bounds) - 1
    base_slots = list(cfg.layer_base_slots or [])
    max_slots = list(cfg.layer_max_slots or [])
    if len(base_slots) < layer_count:
        base_slots.extend([base_slots[-1] if base_slots else 1] * (layer_count - len(base_slots)))
    if len(max_slots) < layer_count:
        max_slots.extend([max(max_slots[-1], 1) if max_slots else max(cfg.target_hold_num, 1)] * (layer_count - len(max_slots)))
    specs: list[dict[str, Any]] = []
    for index in range(layer_count):
        lower = float(bounds[index])
        upper = float(bounds[index + 1])
        specs.append(
            {
                "layer_code": f"CAP_{index + 1}",
                "layer_name": f"{int(lower / 100000000)}-{int(upper / 100000000)}亿",
                "lower_cap": lower,
                "upper_cap": upper,
                "base_slots": max(0, int(base_slots[index])),
                "max_slots": max(1, int(max_slots[index])),
                "layer_order": index,
            }
        )
    return specs


def _apply_market_cap_layers(working: pd.DataFrame, cfg: MicrocapStrategyConfig) -> pd.DataFrame:
    specs = _resolve_layer_specs(cfg)
    if working.empty or not specs:
        return pd.DataFrame(columns=list(working.columns) + ["layer_code", "layer_name", "layer_order", "base_slots", "max_slots"])
    layered_frames: list[pd.DataFrame] = []
    market_caps = pd.to_numeric(working["market_cap_prev"], errors="coerce")
    for spec in specs:
        mask = market_caps.gt(spec["lower_cap"]) & market_caps.le(spec["upper_cap"])
        layer_frame = working[mask].copy()
        if layer_frame.empty:
            continue
        layer_frame["layer_code"] = spec["layer_code"]
        layer_frame["layer_name"] = spec["layer_name"]
        layer_frame["layer_order"] = spec["layer_order"]
        layer_frame["base_slots"] = spec["base_slots"]
        layer_frame["max_slots"] = spec["max_slots"]
        layered_frames.append(layer_frame)
    if not layered_frames:
        return pd.DataFrame(columns=list(working.columns) + ["layer_code", "layer_name", "layer_order", "base_slots", "max_slots"])
    return pd.concat(layered_frames, ignore_index=True)


def _should_rebalance_on_date(
    trade_date: pd.Timestamp,
    last_rebalance_date: pd.Timestamp | None,
    cfg: MicrocapStrategyConfig,
) -> bool:
    frequency = str(cfg.rebalance_frequency or "weekly").strip().lower()
    if frequency in {"", "daily"}:
        return True
    if last_rebalance_date is None:
        return True
    current = pd.Timestamp(trade_date).normalize()
    previous = pd.Timestamp(last_rebalance_date).normalize()
    if frequency == "weekly":
        current_week = current.isocalendar()
        previous_week = previous.isocalendar()
        return (current_week.year, current_week.week) != (previous_week.year, previous_week.week)
    if frequency == "monthly":
        return (current.year, current.month) != (previous.year, previous.month)
    return True


def _build_industry_slot_table(
    working: pd.DataFrame,
    holdings: list[str],
    cfg: MicrocapStrategyConfig,
) -> pd.DataFrame:
    if working.empty or "industry_code" not in working.columns:
        return pd.DataFrame()
    grouped = working.groupby(["industry_code", "industry_name"], dropna=False, sort=False)
    summary = grouped.agg(
        candidate_count=("symbol", "count"),
        momentum=("ret_20_prev", "median"),
        breadth=("ret_5_prev", lambda series: float(pd.to_numeric(series, errors="coerce").gt(0).mean())),
        liquidity=("amount_ratio_5_20_prev", "median"),
    ).reset_index()
    summary = summary[summary["industry_code"].fillna("").astype(str).ne("")].copy()
    if summary.empty:
        return summary
    summary["momentum_score"] = _rank_percentile(summary["momentum"])
    summary["breadth_score"] = _rank_percentile(summary["breadth"])
    summary["liquidity_score"] = _rank_percentile(summary["liquidity"])
    weight_sum = float(cfg.industry_score_momentum_weight + cfg.industry_score_breadth_weight + cfg.industry_score_liquidity_weight)
    if weight_sum <= 0:
        weight_sum = 1.0
    summary["industry_score"] = (
        summary["momentum_score"] * float(cfg.industry_score_momentum_weight)
        + summary["breadth_score"] * float(cfg.industry_score_breadth_weight)
        + summary["liquidity_score"] * float(cfg.industry_score_liquidity_weight)
    ) / weight_sum
    holding_set = set(str(item) for item in holdings)
    held_frame = working[working["symbol"].astype(str).isin(holding_set)].copy()
    held_industries = set(held_frame["industry_code"].fillna("").astype(str))
    summary["has_holding"] = summary["industry_code"].astype(str).isin(held_industries)
    if cfg.industry_holding_score_bonus > 0:
        summary["industry_score"] = summary["industry_score"] + summary["has_holding"].astype(float) * float(cfg.industry_holding_score_bonus)
    summary = summary.sort_values(["industry_score", "candidate_count", "industry_code"], ascending=[False, False, True]).reset_index(drop=True)
    selected_codes = summary["industry_code"].astype(str).head(cfg.industry_top_k).tolist()
    keep_codes = set(summary["industry_code"].astype(str).head(max(cfg.industry_keep_top_k, cfg.industry_top_k)).tolist())
    for code in summary.loc[summary["has_holding"], "industry_code"].astype(str):
        if code in keep_codes and code not in selected_codes:
            selected_codes.append(code)
    capacity_map = {
        str(row["industry_code"]): max(0, min(int(row["candidate_count"]), cfg.industry_max_slots))
        for row in summary.to_dict(orient="records")
    }
    selected_codes = [code for code in selected_codes if capacity_map.get(code, 0) > 0]
    total_capacity = sum(capacity_map.get(code, 0) for code in selected_codes)
    if total_capacity < cfg.target_hold_num:
        for code in summary["industry_code"].astype(str):
            if code in selected_codes:
                continue
            if capacity_map.get(code, 0) <= 0:
                continue
            selected_codes.append(code)
            total_capacity += capacity_map.get(code, 0)
            if total_capacity >= cfg.target_hold_num:
                break
    if not selected_codes:
        return pd.DataFrame()
    selected = summary[summary["industry_code"].astype(str).isin(selected_codes)].copy()
    selected["selected_rank"] = selected["industry_code"].astype(str).map({code: index for index, code in enumerate(selected_codes)})
    selected = selected.sort_values(["selected_rank", "industry_score", "industry_code"]).reset_index(drop=True)
    selected["capacity"] = selected["candidate_count"].clip(upper=cfg.industry_max_slots).astype(int)
    selected["slots"] = 0
    if cfg.target_hold_num <= 0:
        return selected
    base_candidates = selected.index[selected["capacity"] > 0].tolist()
    if base_candidates:
        if len(base_candidates) * cfg.industry_min_slots <= cfg.target_hold_num:
            selected.loc[base_candidates, "slots"] = np.minimum(selected.loc[base_candidates, "capacity"], cfg.industry_min_slots)
        else:
            ranked_candidates = selected.loc[base_candidates].sort_values(["industry_score", "selected_rank"], ascending=[False, True]).index.tolist()
            for index in ranked_candidates[: cfg.target_hold_num]:
                selected.at[index, "slots"] = 1
    remaining = int(cfg.target_hold_num - int(selected["slots"].sum()))
    if remaining > 0:
        scores = selected["industry_score"].clip(lower=0.0)
        if float(scores.sum()) <= 0:
            scores = pd.Series(1.0, index=selected.index, dtype=float)
        while remaining > 0:
            quotients = []
            for index in selected.index.tolist():
                slots = int(selected.at[index, "slots"])
                capacity = int(selected.at[index, "capacity"])
                if slots >= capacity:
                    continue
                quotients.append((float(scores.loc[index]) / float(slots + 1), int(selected.at[index, "selected_rank"]), index))
            if not quotients:
                break
            _, _, winner = max(quotients, key=lambda item: (item[0], -item[1]))
            selected.at[winner, "slots"] = int(selected.at[winner, "slots"]) + 1
            remaining -= 1
    return selected[selected["slots"] > 0].copy()


def _build_market_cap_layer_slot_table(
    working: pd.DataFrame,
    holdings: list[str],
    cfg: MicrocapStrategyConfig,
) -> pd.DataFrame:
    if working.empty or "layer_code" not in working.columns:
        return pd.DataFrame()
    grouped = working.groupby(["layer_code", "layer_name", "layer_order", "base_slots", "max_slots"], dropna=False, sort=False)
    summary = grouped.agg(
        candidate_count=("symbol", "count"),
        momentum=("ret_20_prev", "median"),
        breadth=("ret_5_prev", lambda series: float(pd.to_numeric(series, errors="coerce").gt(0).mean())),
        liquidity=("amount_ratio_5_20_prev", "median"),
    ).reset_index()
    if summary.empty:
        return summary
    summary["momentum_score"] = _rank_percentile(summary["momentum"])
    summary["breadth_score"] = _rank_percentile(summary["breadth"])
    summary["liquidity_score"] = _rank_percentile(summary["liquidity"])
    weight_sum = float(cfg.layer_score_momentum_weight + cfg.layer_score_breadth_weight + cfg.layer_score_liquidity_weight)
    if weight_sum <= 0:
        weight_sum = 1.0
    summary["layer_score"] = (
        summary["momentum_score"] * float(cfg.layer_score_momentum_weight)
        + summary["breadth_score"] * float(cfg.layer_score_breadth_weight)
        + summary["liquidity_score"] * float(cfg.layer_score_liquidity_weight)
    ) / weight_sum
    holding_set = set(str(item) for item in holdings)
    held_frame = working[working["symbol"].astype(str).isin(holding_set)].copy()
    held_layers = set(held_frame["layer_code"].fillna("").astype(str))
    summary["has_holding"] = summary["layer_code"].astype(str).isin(held_layers)
    if cfg.layer_holding_score_bonus > 0:
        summary["layer_score"] = summary["layer_score"] + summary["has_holding"].astype(float) * float(cfg.layer_holding_score_bonus)
    summary = summary.sort_values(["layer_order", "layer_code"]).reset_index(drop=True)
    summary["capacity"] = np.minimum(summary["candidate_count"].astype(int), summary["max_slots"].astype(int))
    summary["slots"] = np.minimum(summary["capacity"], summary["base_slots"].astype(int))
    allocated = int(summary["slots"].sum())
    if allocated > cfg.target_hold_num:
        summary["slots"] = 0
        ranked = summary.sort_values(["layer_score", "layer_order"], ascending=[False, True]).index.tolist()
        for index in ranked[: cfg.target_hold_num]:
            summary.at[index, "slots"] = 1
        return summary[summary["slots"] > 0].copy()
    remaining = int(cfg.target_hold_num - allocated)
    if remaining > 0:
        scores = summary["layer_score"].clip(lower=0.0)
        if float(scores.sum()) <= 0:
            scores = pd.Series(1.0, index=summary.index, dtype=float)
        while remaining > 0:
            quotients = []
            for index in summary.index.tolist():
                slots = int(summary.at[index, "slots"])
                capacity = int(summary.at[index, "capacity"])
                if slots >= capacity:
                    continue
                quotients.append((float(scores.loc[index]) / float(slots + 1), int(summary.at[index, "layer_order"]), index))
            if not quotients:
                break
            _, _, winner = max(quotients, key=lambda item: (item[0], -item[1]))
            summary.at[winner, "slots"] = int(summary.at[winner, "slots"]) + 1
            remaining -= 1
    return summary[summary["slots"] > 0].copy()


def build_portfolio_selection(
    day_frame: pd.DataFrame,
    holdings: list[str],
    total_value_open: float,
    cfg: MicrocapStrategyConfig,
) -> dict[str, Any]:
    if day_frame.empty:
        return {"targets": [], "ranked_count": 0, "industry_allocations": [], "layer_allocations": [], "zhuang_filtered_count": 0, "zhuang_replaced_count": 0}
    invest_value = float(total_value_open) * (1.0 - cfg.cash_buffer)
    slot_value = invest_value / float(cfg.target_hold_num)
    working = day_frame
    if "is_overlay_asset" in working.columns:
        overlay_mask = pd.Series(working["is_overlay_asset"], index=working.index, dtype="boolean").fillna(False)
    else:
        overlay_mask = pd.Series(False, index=working.index, dtype=bool)
    amount_mask = pd.Series(True, index=working.index, dtype=bool)
    if cfg.min_avg_money_20 > 0:
        amount_mask = working["avg_amount_20_prev"].fillna(0.0).ge(cfg.min_avg_money_20)
    beijing_mask = pd.Series(working["is_beijing_stock"], index=working.index, dtype="boolean").fillna(False)
    st_mask = pd.Series(working["is_st_name"], index=working.index, dtype="boolean").fillna(False)
    star_st_mask = pd.Series(working.get("is_star_st_name", False), index=working.index, dtype="boolean").fillna(False)
    delisting_mask = pd.Series(working.get("is_delisting_name", False), index=working.index, dtype="boolean").fillna(False)
    if cfg.allow_st_buy:
        st_buy_mask = ~star_st_mask if cfg.exclude_star_st_buy else pd.Series(True, index=working.index, dtype=bool)
        if cfg.exclude_delisting_buy:
            st_buy_mask = st_buy_mask & (~delisting_mask)
    else:
        st_buy_mask = ~st_mask
    if cfg.allow_beijing_stock_buy:
        beijing_buy_mask = pd.Series(True, index=working.index, dtype=bool)
    else:
        beijing_buy_mask = ~beijing_mask
    working = working[
        (~overlay_mask)
        & (~working["is_b_share"])
        & beijing_buy_mask
        & st_buy_mask
        & working["open"].fillna(0.0).ge(cfg.min_price_floor)
        & working["volume"].fillna(0.0).gt(0.0)
        & working["prev_close"].fillna(0.0).ge(cfg.min_price_floor)
        & working["listed_days"].fillna(-1).ge(cfg.min_list_days)
        & amount_mask
        & working["market_cap_prev"].fillna(float("inf")).gt(0.0)
    ].copy()
    if working.empty:
        return {"targets": [], "ranked_count": 0, "industry_allocations": [], "layer_allocations": [], "zhuang_filtered_count": 0, "zhuang_replaced_count": 0}
    working["price_cap"] = working["symbol"].map(lambda symbol: get_dynamic_price_cap(symbol, slot_value, cfg))
    working = working[working["open"].le(working["price_cap"])].copy()
    if working.empty:
        return {"targets": [], "ranked_count": 0, "industry_allocations": [], "layer_allocations": [], "zhuang_filtered_count": 0, "zhuang_replaced_count": 0}
    if cfg.layer_rotation_enabled:
        working = _apply_market_cap_layers(working, cfg)
        if working.empty:
            return {"targets": [], "ranked_count": 0, "industry_allocations": [], "layer_allocations": [], "zhuang_filtered_count": 0, "zhuang_replaced_count": 0}
    working, zhuang_filtered_count = _apply_zhuang_buy_filter(working, holdings, cfg)
    if working.empty:
        return {"targets": [], "ranked_count": 0, "industry_allocations": [], "layer_allocations": [], "zhuang_filtered_count": zhuang_filtered_count, "zhuang_replaced_count": 0}
    suspect_lookup = (
        working.set_index(working["symbol"].astype(str))["is_old_zhuang_suspect_prev"].fillna(False).astype(bool).to_dict()
        if "is_old_zhuang_suspect_prev" in working.columns
        else {}
    )
    ranked_cap = max(cfg.keep_rank + 30, cfg.target_hold_num * 4)
    if cfg.layer_rotation_enabled and "layer_code" in working.columns:
        slot_table = _build_market_cap_layer_slot_table(working, holdings, cfg)
        if not slot_table.empty:
            target: list[str] = []
            layer_allocations: list[dict[str, Any]] = []
            replace_ranked = working.sort_values(["market_cap_prev", "symbol"], ascending=[True, True])["symbol"].astype(str).tolist()
            for row in slot_table.to_dict(orient="records"):
                layer_code = str(row.get("layer_code") or "")
                layer_name = str(row.get("layer_name") or layer_code)
                slot_count = int(row.get("slots") or 0)
                if slot_count <= 0:
                    continue
                pool = working[working["layer_code"].fillna("").astype(str) == layer_code].copy()
                if pool.empty:
                    continue
                pool = pool.sort_values(["market_cap_prev", "symbol"], ascending=[True, True]).reset_index(drop=True)
                local_keep_rank = _scaled_rank_limit(cfg.keep_rank, slot_count, cfg.target_hold_num)
                local_buy_rank = _scaled_rank_limit(cfg.buy_rank, slot_count, cfg.target_hold_num)
                layer_rank_limit = max(slot_count * 3, local_keep_rank, local_buy_rank, slot_count + 5)
                ranked = pool["symbol"].astype(str).head(layer_rank_limit).tolist()
                held_layer = [symbol for symbol in holdings if symbol in set(pool["symbol"].astype(str))]
                selected_symbols = _select_from_ranked_symbols(ranked, held_layer, slot_count, cfg)
                for symbol in selected_symbols:
                    if symbol not in target:
                        target.append(symbol)
                    if len(target) >= cfg.target_hold_num:
                        break
                layer_allocations.append(
                    {
                        "layer_code": layer_code,
                        "layer_name": layer_name,
                        "slots": slot_count,
                        "candidate_count": int(row.get("candidate_count") or 0),
                        "layer_score": round(float(row.get("layer_score") or 0.0), 6),
                        "selected_symbols": selected_symbols,
                    }
                )
                if len(target) >= cfg.target_hold_num:
                    break
            if target:
                target, zhuang_replaced_count = _apply_zhuang_final_replacements(target[: cfg.target_hold_num], replace_ranked, holdings, suspect_lookup, cfg)
                return {
                    "targets": target[: cfg.target_hold_num],
                    "ranked_count": int(len(working)),
                    "industry_allocations": [],
                    "layer_allocations": layer_allocations,
                    "zhuang_filtered_count": zhuang_filtered_count,
                    "zhuang_replaced_count": zhuang_replaced_count,
                }
    if cfg.industry_weighted_enabled and "industry_code" in working.columns:
        slot_table = _build_industry_slot_table(working, holdings, cfg)
        if not slot_table.empty:
            target: list[str] = []
            allocations: list[dict[str, Any]] = []
            replace_ranked = working.sort_values(["market_cap_prev", "symbol"], ascending=[True, True])["symbol"].astype(str).tolist()
            for row in slot_table.to_dict(orient="records"):
                industry_code = str(row.get("industry_code") or "")
                industry_name = str(row.get("industry_name") or industry_code)
                slot_count = int(row.get("slots") or 0)
                if slot_count <= 0:
                    continue
                pool = working[working["industry_code"].fillna("").astype(str) == industry_code].copy()
                if pool.empty:
                    continue
                pool = pool.sort_values(["market_cap_prev", "symbol"], ascending=[True, True]).reset_index(drop=True)
                local_keep_rank = _scaled_rank_limit(cfg.keep_rank, slot_count, cfg.target_hold_num)
                local_buy_rank = _scaled_rank_limit(cfg.buy_rank, slot_count, cfg.target_hold_num)
                industry_rank_limit = max(
                    slot_count * cfg.industry_query_limit_multiplier,
                    cfg.industry_min_candidate_count,
                    local_keep_rank,
                    local_buy_rank,
                )
                ranked = pool["symbol"].astype(str).head(industry_rank_limit).tolist()
                held_industry = [symbol for symbol in holdings if symbol in set(pool["symbol"].astype(str))]
                selected_symbols = _select_from_ranked_symbols(ranked, held_industry, slot_count, cfg)
                for symbol in selected_symbols:
                    if symbol not in target:
                        target.append(symbol)
                    if len(target) >= cfg.target_hold_num:
                        break
                allocations.append(
                    {
                        "industry_code": industry_code,
                        "industry_name": industry_name,
                        "slots": slot_count,
                        "candidate_count": int(row.get("candidate_count") or 0),
                        "industry_score": round(float(row.get("industry_score") or 0.0), 6),
                        "selected_symbols": selected_symbols,
                    }
                )
                if len(target) >= cfg.target_hold_num:
                    break
            if target:
                target, zhuang_replaced_count = _apply_zhuang_final_replacements(target[: cfg.target_hold_num], replace_ranked, holdings, suspect_lookup, cfg)
                return {
                    "targets": target[: cfg.target_hold_num],
                    "ranked_count": int(len(working)),
                    "industry_allocations": allocations,
                    "layer_allocations": [],
                    "zhuang_filtered_count": zhuang_filtered_count,
                    "zhuang_replaced_count": zhuang_replaced_count,
                }
    if cfg.monster_prelude_enabled:
        ranked, ranked_count = _build_monster_prelude_ranked(working, holdings, cfg)
        if ranked:
            targets = _select_from_ranked_symbols(ranked, holdings, cfg.target_hold_num, cfg)
            targets, zhuang_replaced_count = _apply_zhuang_final_replacements(targets, ranked, holdings, suspect_lookup, cfg)
            return {
                "targets": targets,
                "ranked_count": ranked_count,
                "industry_allocations": [],
                "layer_allocations": [],
                "zhuang_filtered_count": zhuang_filtered_count,
                "zhuang_replaced_count": zhuang_replaced_count,
            }
    working = working.sort_values(["market_cap_prev", "symbol"], ascending=[True, True]).head(cfg.query_limit).reset_index(drop=True)
    ranked = working["symbol"].astype(str).tolist()[:ranked_cap]
    targets = _select_from_ranked_symbols(ranked, holdings, cfg.target_hold_num, cfg)
    targets, zhuang_replaced_count = _apply_zhuang_final_replacements(targets, ranked, holdings, suspect_lookup, cfg)
    return {
        "targets": targets,
        "ranked_count": len(ranked),
        "industry_allocations": [],
        "layer_allocations": [],
        "zhuang_filtered_count": zhuang_filtered_count,
        "zhuang_replaced_count": zhuang_replaced_count,
    }


def build_target_portfolio(
    day_frame: pd.DataFrame,
    holdings: list[str],
    total_value_open: float,
    cfg: MicrocapStrategyConfig,
) -> tuple[list[str], int]:
    selection = build_portfolio_selection(day_frame, holdings, total_value_open, cfg)
    return list(selection["targets"]), int(selection["ranked_count"])


class JoinQuantMicrocapBacktestEngine:
    def __init__(self, session_factory: sessionmaker, app_settings: AppSettings, strategy_settings: StrategySettings) -> None:
        self.session_factory = session_factory
        self.app_settings = app_settings
        self.strategy_settings = strategy_settings
        self.cfg = MicrocapStrategyConfig.from_strategy_settings(strategy_settings)
        self.account_id = self.strategy_settings.implementation
        self.bridge = QmtBridgeClient(app_settings)

    def run(self, initial_cash: Decimal) -> tuple[Path, EvaluationResult, pd.DataFrame]:
        self._emit_progress(1, 5, "加载日线历史")
        history = self._load_history()
        self._emit_progress(2, 5, "加载证券元数据")
        symbols = history["symbol"].dropna().astype(str).unique().tolist()
        instrument_frame = self._load_instrument_frame(symbols)
        capital_frame = self._load_capital_frame(symbols)
        self._emit_progress(3, 5, "构建回测截面")
        prepared = self._prepare_history(history, instrument_frame, capital_frame)
        overlay_history = self._load_overlay_history()
        overlay_frame = self._prepare_overlay_history(overlay_history)
        if not overlay_frame.empty:
            prepared = pd.concat([prepared, overlay_frame], ignore_index=True).sort_values(["trading_date", "symbol"]).reset_index(drop=True)
        benchmark = self._load_benchmark()
        self._emit_progress(4, 5, "执行微盘策略回测")
        summary = self._simulate(prepared, benchmark, float(initial_cash), instrument_frame)
        self._emit_progress(5, 5, "写入回测结果")
        return self._persist_results(summary, initial_cash)

    def _emit_progress(self, current: int, total: int, phase: str) -> None:
        print(f"[JoinQuantMicrocap] backtest {current}/{total} phase={phase}", flush=True)

    def _load_history(self) -> pd.DataFrame:
        history_path = _resolve_path(self.app_settings.history_parquet)
        frame = pd.read_parquet(history_path, columns=["trading_date", "symbol", "open", "high", "low", "close", "volume", "amount"])
        frame["trading_date"] = pd.to_datetime(frame["trading_date"]).dt.normalize()
        start_dt = pd.Timestamp(self.app_settings.history_start or "2020-01-01").normalize()
        if self.app_settings.history_end:
            end_dt = pd.Timestamp(self.app_settings.history_end).normalize()
            frame = frame[(frame["trading_date"] >= start_dt) & (frame["trading_date"] <= end_dt)].copy()
        else:
            frame = frame[frame["trading_date"] >= start_dt].copy()
        frame["symbol"] = frame["symbol"].astype(str)
        return frame.sort_values(["symbol", "trading_date"]).reset_index(drop=True)

    def _load_benchmark(self) -> pd.DataFrame:
        symbol = self.cfg.benchmark_symbol or str(self.app_settings.qlib_benchmark_symbol or "").strip()
        if not symbol:
            return pd.DataFrame(columns=["trading_date", "close"])
        try:
            frame = self.bridge.get_history(
                [symbol],
                period=self.app_settings.history_period,
                start_time=self.app_settings.history_start,
                end_time=self.app_settings.history_end,
                dividend_type=self.app_settings.history_adjustment,
                fill_data=self.app_settings.history_fill_data,
            )
        except Exception:
            return pd.DataFrame(columns=["trading_date", "close"])
        if frame.empty:
            return pd.DataFrame(columns=["trading_date", "close"])
        frame["trading_date"] = pd.to_datetime(frame["trading_date"]).dt.normalize()
        return frame.loc[:, ["trading_date", "close"]].sort_values("trading_date").reset_index(drop=True)

    def _load_overlay_history(self) -> pd.DataFrame:
        symbol = str(self.cfg.hedge_symbol or "").strip()
        if not symbol or not self.cfg.seasonal_hedge_schedule:
            return pd.DataFrame(columns=["trading_date", "symbol", "open", "high", "low", "close", "volume", "amount"])
        try:
            frame = self.bridge.get_history(
                [symbol],
                period=self.app_settings.history_period,
                start_time=self.app_settings.history_start,
                end_time=self.app_settings.history_end,
                dividend_type=self.cfg.hedge_history_adjustment,
                fill_data=self.app_settings.history_fill_data,
            )
        except Exception:
            return pd.DataFrame(columns=["trading_date", "symbol", "open", "high", "low", "close", "volume", "amount"])
        if frame.empty:
            return pd.DataFrame(columns=["trading_date", "symbol", "open", "high", "low", "close", "volume", "amount"])
        frame["trading_date"] = pd.to_datetime(frame["trading_date"]).dt.normalize()
        frame["symbol"] = frame["symbol"].astype(str)
        return frame.loc[:, ["trading_date", "symbol", "open", "high", "low", "close", "volume", "amount"]].sort_values(["symbol", "trading_date"]).reset_index(drop=True)

    def _load_instrument_frame(self, symbols: list[str]) -> pd.DataFrame:
        cache_path = _resolve_path(self.cfg.instrument_cache_path)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cached = pd.DataFrame()
        if cache_path.exists():
            cached = pd.read_parquet(cache_path)
            cached["symbol"] = cached["symbol"].astype(str)
        known = set(cached["symbol"].astype(str)) if not cached.empty else set()
        missing = sorted(set(symbols) - known)
        rows: list[dict[str, Any]] = []
        for batch in _batched(missing, 200):
            details = self.bridge.get_instrument_details(batch)
            for symbol in batch:
                payload = details.get(symbol) or {}
                rows.append(
                    {
                        "symbol": symbol,
                        "instrument_name": str(payload.get("InstrumentName") or payload.get("instrument_name") or symbol),
                        "open_date": str(payload.get("OpenDate") or ""),
                        "total_capital_current": float(payload.get("TotalVolume") or payload.get("total_capital") or 0.0),
                        "float_capital_current": float(payload.get("FloatVolume") or payload.get("float_capital") or 0.0),
                    }
                )
        if rows:
            fresh = pd.DataFrame(rows)
            combined = pd.concat([cached, fresh], ignore_index=True) if not cached.empty else fresh
            combined = combined.drop_duplicates("symbol", keep="last").sort_values("symbol").reset_index(drop=True)
            combined.to_parquet(cache_path, index=False)
            cached = combined
        if cached.empty:
            cached = pd.DataFrame(columns=["symbol", "instrument_name", "open_date", "total_capital_current", "float_capital_current"])
        cached["open_date"] = pd.to_datetime(cached["open_date"], format="%Y%m%d", errors="coerce").dt.normalize()
        cached["instrument_name"] = cached["instrument_name"].fillna("")
        cached = cached.drop_duplicates("symbol", keep="last").reset_index(drop=True)
        if self.cfg.industry_weighted_enabled:
            industry_frame = self._load_industry_frame(symbols)
            if not industry_frame.empty:
                cached = cached.merge(industry_frame, on="symbol", how="left")
        if "industry_code" not in cached.columns:
            cached["industry_code"] = ""
        if "industry_name" not in cached.columns:
            cached["industry_name"] = ""
        cached["industry_code"] = cached["industry_code"].fillna("").astype(str)
        cached["industry_name"] = cached["industry_name"].fillna("").astype(str)
        return cached

    def _load_industry_frame(self, symbols: list[str]) -> pd.DataFrame:
        cache_path = _resolve_path(self.cfg.industry_cache_path)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cached = pd.DataFrame()
        if cache_path.exists():
            cached = pd.read_parquet(cache_path)
            if "symbol" in cached.columns:
                cached["symbol"] = cached["symbol"].astype(str)
        known = set(cached["symbol"].astype(str)) if not cached.empty and "symbol" in cached.columns else set()
        missing = sorted(set(symbols) - known)
        rows: list[dict[str, Any]] = []
        if missing:
            # 行业映射首次预热时优先走一次全量抓取，避免每个批次都重复扫描整个行业列表。
            if len(missing) >= 200:
                target_set = set(missing)
                full_map = self.bridge.get_industry_map([], sector_prefix=self.cfg.industry_sector_prefix)
                for item in full_map:
                    symbol = str(item.get("symbol") or "")
                    if symbol not in target_set:
                        continue
                    rows.append(
                        {
                            "symbol": symbol,
                            "industry_code": str(item.get("industry_code") or ""),
                            "industry_name": str(item.get("industry_name") or ""),
                            "industry_level": str(item.get("industry_level") or self.cfg.industry_sector_prefix),
                        }
                    )
            else:
                for batch in _batched(missing, 400):
                    for item in self.bridge.get_industry_map(batch, sector_prefix=self.cfg.industry_sector_prefix):
                        rows.append(
                            {
                                "symbol": str(item.get("symbol") or ""),
                                "industry_code": str(item.get("industry_code") or ""),
                                "industry_name": str(item.get("industry_name") or ""),
                                "industry_level": str(item.get("industry_level") or self.cfg.industry_sector_prefix),
                            }
                        )
            unresolved = sorted(set(missing) - {row["symbol"] for row in rows})
            rows.extend(
                {
                    "symbol": symbol,
                    "industry_code": "",
                    "industry_name": "",
                    "industry_level": self.cfg.industry_sector_prefix,
                }
                for symbol in unresolved
            )
        if rows:
            fresh = pd.DataFrame(rows)
            combined = pd.concat([cached, fresh], ignore_index=True) if not cached.empty else fresh
            combined = combined.drop_duplicates("symbol", keep="last").sort_values("symbol").reset_index(drop=True)
            combined.to_parquet(cache_path, index=False)
            cached = combined
        if cached.empty:
            return pd.DataFrame(columns=["symbol", "industry_code", "industry_name", "industry_level"])
        for column in ["industry_code", "industry_name", "industry_level"]:
            if column not in cached.columns:
                cached[column] = ""
            cached[column] = cached[column].fillna("").astype(str)
        return cached.loc[:, ["symbol", "industry_code", "industry_name", "industry_level"]].drop_duplicates("symbol", keep="last").reset_index(drop=True)

    def _load_sz_change_name_frame(self) -> pd.DataFrame:
        cache_path = _resolve_path(self.cfg.historical_sz_change_name_cache_path)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        if cache_path.exists():
            cached = pd.read_parquet(cache_path)
        elif ak is None:
            cached = pd.DataFrame()
        else:
            raw = ak.stock_info_sz_change_name(symbol="简称变更")
            cached = raw.rename(
                columns={
                    "变更日期": "change_date",
                    "证券代码": "code",
                    "变更前简称": "old_name",
                    "变更后简称": "new_name",
                }
            )
            cached["code"] = cached["code"].astype(str).str.zfill(6)
            cached["symbol"] = cached["code"] + ".SZ"
            cached["change_date"] = pd.to_datetime(cached["change_date"], errors="coerce").dt.normalize()
            cached = cached.loc[:, ["symbol", "change_date", "old_name", "new_name"]].dropna(subset=["change_date"])
            cached = cached.sort_values(["symbol", "change_date"]).reset_index(drop=True)
            cached.to_parquet(cache_path, index=False)
        if cached.empty:
            return pd.DataFrame(columns=["symbol", "change_date", "old_name", "new_name"])
        cached["symbol"] = cached["symbol"].astype(str)
        cached["change_date"] = pd.to_datetime(cached["change_date"], errors="coerce").dt.normalize()
        for column in ["old_name", "new_name"]:
            if column not in cached.columns:
                cached[column] = ""
            cached[column] = cached[column].fillna("").astype(str)
        return cached.loc[:, ["symbol", "change_date", "old_name", "new_name"]].dropna(subset=["change_date"]).reset_index(drop=True)

    def _load_name_history_frame(self, symbols: list[str]) -> pd.DataFrame:
        cache_path = _resolve_path(self.cfg.historical_name_cache_path)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cached = pd.DataFrame()
        if cache_path.exists():
            cached = pd.read_parquet(cache_path)
            if "symbol" in cached.columns:
                cached["symbol"] = cached["symbol"].astype(str)
        known = set(cached["symbol"].astype(str)) if not cached.empty and "symbol" in cached.columns else set()
        missing = sorted(set(symbols) - known)
        rows: list[dict[str, Any]] = []
        if missing and ak is not None:
            for symbol in missing:
                code = _symbol_code(symbol)
                try:
                    history = ak.stock_info_change_name(symbol=code)
                except Exception:
                    history = pd.DataFrame()
                if history.empty:
                    rows.append({"symbol": symbol, "name_order": 1, "instrument_name": ""})
                    continue
                history = history.rename(columns={"index": "name_order", "name": "instrument_name"}).copy()
                history["symbol"] = symbol
                history["name_order"] = pd.to_numeric(history["name_order"], errors="coerce").fillna(0).astype(int)
                history["instrument_name"] = history["instrument_name"].fillna("").astype(str)
                rows.extend(history.loc[:, ["symbol", "name_order", "instrument_name"]].to_dict("records"))
        if rows:
            fresh = pd.DataFrame(rows)
            combined = pd.concat([cached, fresh], ignore_index=True) if not cached.empty else fresh
            combined = combined.drop_duplicates(["symbol", "name_order"], keep="last").sort_values(["symbol", "name_order"]).reset_index(drop=True)
            combined.to_parquet(cache_path, index=False)
            cached = combined
        if cached.empty:
            return pd.DataFrame(columns=["symbol", "name_order", "instrument_name"])
        cached["symbol"] = cached["symbol"].astype(str)
        cached["name_order"] = pd.to_numeric(cached["name_order"], errors="coerce").fillna(0).astype(int)
        cached["instrument_name"] = cached["instrument_name"].fillna("").astype(str)
        return cached.loc[:, ["symbol", "name_order", "instrument_name"]].drop_duplicates(["symbol", "name_order"], keep="last").reset_index(drop=True)

    def _load_historical_st_status_frame(self, symbols: list[str], start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        cache_path = _resolve_path(self.cfg.historical_st_cache_path)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cached = pd.DataFrame()
        if cache_path.exists():
            cached = pd.read_parquet(cache_path)
            if "symbol" in cached.columns:
                cached["symbol"] = cached["symbol"].astype(str)
            if "trading_date" in cached.columns:
                cached["trading_date"] = pd.to_datetime(cached["trading_date"], errors="coerce").dt.normalize()
        coverage: dict[str, tuple[pd.Timestamp, pd.Timestamp]] = {}
        if not cached.empty:
            summary = cached.groupby("symbol", sort=False)["trading_date"].agg(["min", "max"]).reset_index()
            coverage = {
                str(row.symbol): (pd.Timestamp(row.min).normalize(), pd.Timestamp(row.max).normalize())
                for row in summary.itertuples(index=False)
            }
        fetch_symbols = [
            symbol
            for symbol in sorted(set(symbols))
            if _to_baostock_symbol(symbol)
            and (
                symbol not in coverage
                or coverage[symbol][0] > start_date.normalize()
                or coverage[symbol][1] < end_date.normalize()
            )
        ]
        rows: list[dict[str, Any]] = []
        if fetch_symbols and bs is not None:
            login_result = bs.login()
            if str(getattr(login_result, "error_code", "0")) == "0":
                try:
                    for symbol in fetch_symbols:
                        query_symbol = _to_baostock_symbol(symbol)
                        if not query_symbol:
                            continue
                        rs = bs.query_history_k_data_plus(
                            query_symbol,
                            "date,code,isST",
                            start_date=start_date.strftime("%Y-%m-%d"),
                            end_date=end_date.strftime("%Y-%m-%d"),
                            frequency="d",
                            adjustflag="3",
                        )
                        while rs.error_code == "0" and rs.next():
                            current_row = rs.get_row_data()
                            rows.append(
                                {
                                    "symbol": symbol,
                                    "trading_date": pd.Timestamp(current_row[0]).normalize(),
                                    "is_st_history": str(current_row[2]).strip() == "1",
                                }
                            )
                finally:
                    bs.logout()
        if rows:
            fresh = pd.DataFrame(rows)
            if not cached.empty:
                cached = cached[~cached["symbol"].isin(fetch_symbols)].copy()
                combined = pd.concat([cached, fresh], ignore_index=True)
            else:
                combined = fresh
            combined["symbol"] = combined["symbol"].astype(str)
            combined["trading_date"] = pd.to_datetime(combined["trading_date"], errors="coerce").dt.normalize()
            combined["is_st_history"] = combined["is_st_history"].fillna(False).astype(bool)
            combined = combined.drop_duplicates(["symbol", "trading_date"], keep="last").sort_values(["symbol", "trading_date"]).reset_index(drop=True)
            combined.to_parquet(cache_path, index=False)
            cached = combined
        if cached.empty:
            return pd.DataFrame(columns=["symbol", "trading_date", "is_st_history"])
        cached["is_st_history"] = cached["is_st_history"].fillna(False).astype(bool)
        return cached[
            cached["symbol"].isin(set(symbols))
            & cached["trading_date"].between(start_date.normalize(), end_date.normalize())
        ].loc[:, ["symbol", "trading_date", "is_st_history"]].reset_index(drop=True)

    def _load_historical_special_treatment_frame(
        self,
        frame: pd.DataFrame,
        instrument_frame: pd.DataFrame,
    ) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame(columns=["symbol", "trading_date", "is_st_name", "is_star_st_name", "is_delisting_name"])
        rank_buffer = max(int(self.cfg.historical_status_rank_buffer), int(self.cfg.query_limit) * 2)
        ranking = frame.loc[frame["market_cap_prev"].fillna(float("inf")).gt(0.0), ["trading_date", "symbol", "market_cap_prev"]].copy()
        if ranking.empty:
            return pd.DataFrame(columns=["symbol", "trading_date", "is_st_name", "is_star_st_name", "is_delisting_name"])
        ranking["market_cap_rank"] = ranking.groupby("trading_date", sort=False)["market_cap_prev"].rank(method="first")
        candidate_symbols = sorted(ranking.loc[ranking["market_cap_rank"] <= rank_buffer, "symbol"].astype(str).unique())
        candidate_index = frame.loc[frame["symbol"].isin(candidate_symbols), ["symbol", "trading_date"]].drop_duplicates().sort_values(["symbol", "trading_date"]).reset_index(drop=True)
        if candidate_index.empty:
            return pd.DataFrame(columns=["symbol", "trading_date", "is_st_name", "is_star_st_name", "is_delisting_name"])
        instrument_base = instrument_frame.drop_duplicates("symbol", keep="last").set_index("symbol")
        current_name_map = instrument_base["instrument_name"].fillna("").astype(str).to_dict() if "instrument_name" in instrument_base.columns else {}
        result = candidate_index.copy()
        fallback_names = result["symbol"].map(lambda symbol: current_name_map.get(symbol, ""))
        fallback_flags = _infer_special_treatment_frame_from_names(fallback_names)
        result[["is_st_name", "is_star_st_name", "is_delisting_name"]] = fallback_flags.reset_index(drop=True)

        sz_symbols = [symbol for symbol in candidate_symbols if _symbol_market(symbol) == "SZ"]
        if sz_symbols:
            sz_change_frame = self._load_sz_change_name_frame()
            if not sz_change_frame.empty:
                sz_updates: list[pd.DataFrame] = []
                for symbol in sz_symbols:
                    trade_dates = candidate_index.loc[candidate_index["symbol"] == symbol, "trading_date"]
                    change_rows = sz_change_frame[sz_change_frame["symbol"] == symbol]
                    symbol_flags = _build_sz_special_treatment_for_symbol(trade_dates, current_name_map.get(symbol, ""), change_rows)
                    symbol_flags["symbol"] = symbol
                    sz_updates.append(symbol_flags)
                if sz_updates:
                    sz_frame = pd.concat(sz_updates, ignore_index=True)
                    result = result.merge(
                        sz_frame,
                        on=["symbol", "trading_date"],
                        how="left",
                        suffixes=("", "_sz"),
                    )
                    result["is_st_name"] = np.where(result["is_st_name_sz"].notna(), result["is_st_name_sz"], result["is_st_name"]).astype(bool)
                    result["is_star_st_name"] = np.where(
                        result["is_star_st_name_sz"].notna(),
                        result["is_star_st_name_sz"],
                        result["is_star_st_name"],
                    ).astype(bool)
                    result["is_delisting_name"] = np.where(
                        result["is_delisting_name_sz"].notna(),
                        result["is_delisting_name_sz"],
                        result["is_delisting_name"],
                    ).astype(bool)
                    result = result.drop(columns=["is_st_name_sz", "is_star_st_name_sz", "is_delisting_name_sz"])

        sh_symbols = [symbol for symbol in candidate_symbols if _symbol_market(symbol) == "SH"]
        if sh_symbols:
            name_history = self._load_name_history_frame(sh_symbols)
            special_history_symbols = []
            for symbol in sh_symbols:
                ordered_names = name_history.loc[name_history["symbol"] == symbol, "instrument_name"].astype(str).tolist()
                if any(_special_treatment_flags_from_name(name)[0] for name in ordered_names) or _special_treatment_flags_from_name(current_name_map.get(symbol, ""))[0]:
                    special_history_symbols.append(symbol)
            if special_history_symbols:
                start_date = pd.Timestamp(candidate_index["trading_date"].min()).normalize()
                end_date = pd.Timestamp(candidate_index["trading_date"].max()).normalize()
                st_status = self._load_historical_st_status_frame(special_history_symbols, start_date, end_date)
                sh_updates: list[pd.DataFrame] = []
                for symbol in special_history_symbols:
                    trade_dates = candidate_index.loc[candidate_index["symbol"] == symbol, "trading_date"].sort_values().reset_index(drop=True)
                    history_flags = (
                        trade_dates.to_frame(name="trading_date")
                        .merge(st_status[st_status["symbol"] == symbol], on="trading_date", how="left")["is_st_history"]
                        .fillna(False)
                        .astype(bool)
                    )
                    ordered_names = name_history.loc[name_history["symbol"] == symbol].sort_values("name_order")["instrument_name"].astype(str).tolist()
                    symbol_flags = _build_segmented_special_treatment_for_symbol(
                        trade_dates,
                        current_name_map.get(symbol, ""),
                        history_flags,
                        ordered_names,
                    )
                    symbol_flags["symbol"] = symbol
                    sh_updates.append(symbol_flags)
                if sh_updates:
                    sh_frame = pd.concat(sh_updates, ignore_index=True)
                    result = result.merge(
                        sh_frame,
                        on=["symbol", "trading_date"],
                        how="left",
                        suffixes=("", "_sh"),
                    )
                    result["is_st_name"] = np.where(result["is_st_name_sh"].notna(), result["is_st_name_sh"], result["is_st_name"]).astype(bool)
                    result["is_star_st_name"] = np.where(
                        result["is_star_st_name_sh"].notna(),
                        result["is_star_st_name_sh"],
                        result["is_star_st_name"],
                    ).astype(bool)
                    result["is_delisting_name"] = np.where(
                        result["is_delisting_name_sh"].notna(),
                        result["is_delisting_name_sh"],
                        result["is_delisting_name"],
                    ).astype(bool)
                    result = result.drop(columns=["is_st_name_sh", "is_star_st_name_sh", "is_delisting_name_sh"])

        return result.loc[:, ["symbol", "trading_date", "is_st_name", "is_star_st_name", "is_delisting_name"]].drop_duplicates(["symbol", "trading_date"], keep="last")

    def _load_capital_frame(self, symbols: list[str]) -> pd.DataFrame:
        cache_path = _resolve_path(self.cfg.capital_cache_path)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cached = pd.DataFrame()
        if cache_path.exists():
            cached = pd.read_parquet(cache_path)
            cached["symbol"] = cached["symbol"].astype(str)
        known = set(cached["symbol"].astype(str)) if not cached.empty else set()
        missing = sorted(set(symbols) - known)
        rows: list[dict[str, Any]] = []
        for batch in _batched(missing, 100):
            payload = self.bridge.get_financial_data(
                batch,
                ["Capital"],
                start_time=self.app_settings.history_start,
                end_time=self.app_settings.history_end,
                report_type="announce_time",
            )
            for symbol, table_map in payload.items():
                for item in table_map.get("Capital", []) or []:
                    rows.append(
                        {
                            "symbol": symbol,
                            "announce_date": str(item.get("m_anntime") or ""),
                            "report_date": str(item.get("m_timetag") or ""),
                            "total_capital": float(item.get("total_capital") or 0.0),
                            "circulating_capital": float(item.get("circulating_capital") or 0.0),
                            "free_float_capital": float(item.get("freeFloatCapital") or 0.0),
                        }
                    )
        if rows:
            fresh = pd.DataFrame(rows)
            combined = pd.concat([cached, fresh], ignore_index=True) if not cached.empty else fresh
            combined = combined.drop_duplicates(["symbol", "announce_date", "report_date"], keep="last")
            combined = combined.sort_values(["symbol", "announce_date", "report_date"]).reset_index(drop=True)
            combined.to_parquet(cache_path, index=False)
            cached = combined
        if cached.empty:
            return pd.DataFrame(columns=["symbol", "effective_date", "total_capital"])
        cached["announce_date"] = pd.to_datetime(cached["announce_date"], format="%Y%m%d", errors="coerce").dt.normalize()
        cached["report_date"] = pd.to_datetime(cached["report_date"], format="%Y%m%d", errors="coerce").dt.normalize()
        cached["effective_date"] = cached["announce_date"].fillna(cached["report_date"])
        cached = cached.dropna(subset=["effective_date"]).copy()
        cached = cached[cached["total_capital"].fillna(0.0) > 0.0].copy()
        return cached.loc[:, ["symbol", "effective_date", "total_capital", "circulating_capital", "free_float_capital"]]

    def _prepare_history(
        self,
        history: pd.DataFrame,
        instrument_frame: pd.DataFrame,
        capital_frame: pd.DataFrame,
    ) -> pd.DataFrame:
        frame = history.sort_values(["symbol", "trading_date"]).reset_index(drop=True).copy()
        grouped = frame.groupby("symbol", sort=False)
        frame["prev_close"] = grouped["close"].shift(1)
        frame["avg_amount_20_prev"] = grouped["amount"].transform(lambda series: series.rolling(20, min_periods=20).mean().shift(1))
        frame["avg_volume_20_prev"] = grouped["volume"].transform(lambda series: series.rolling(20, min_periods=20).mean().shift(1))

        instrument_base = instrument_frame.drop_duplicates("symbol", keep="last").set_index("symbol")
        frame["open_date"] = frame["symbol"].map(instrument_base["open_date"])
        first_seen = frame.groupby("symbol", sort=False)["trading_date"].transform("min")
        frame["open_date"] = frame["open_date"].fillna(first_seen)
        frame["listed_days"] = (frame["trading_date"] - frame["open_date"]).dt.days
        industry_codes = instrument_base["industry_code"].fillna("") if "industry_code" in instrument_base.columns else pd.Series(dtype=str)
        industry_names = instrument_base["industry_name"].fillna("") if "industry_name" in instrument_base.columns else pd.Series(dtype=str)
        frame["industry_code"] = frame["symbol"].map(industry_codes).fillna("").astype(str)
        frame["industry_name"] = frame["symbol"].map(industry_names).fillna("").astype(str)
        names = instrument_base["instrument_name"].fillna("")
        frame["is_st_name"] = frame["symbol"].map(
            lambda symbol: any(token in str(names.get(symbol, "")).upper() for token in ["ST", "*", "退"])
        )
        frame["is_beijing_stock"] = frame["symbol"].map(is_beijing_stock)
        frame["is_b_share"] = frame["symbol"].map(is_b_share)
        frame["is_star_st_name"] = frame["symbol"].map(lambda symbol: "*ST" in str(names.get(symbol, "")).upper())
        frame["is_delisting_name"] = frame["symbol"].map(lambda symbol: "退" in str(names.get(symbol, "")))
        frame["is_st_name"] = frame["symbol"].map(
            lambda symbol: ("ST" in str(names.get(symbol, "")).upper()) or ("退" in str(names.get(symbol, "")))
        )
        current_capital = instrument_base["total_capital_current"].fillna(0.0)
        fallback_flags = _infer_special_treatment_frame_from_names(frame["symbol"].map(lambda symbol: names.get(symbol, "")))
        frame["is_st_name"] = fallback_flags["is_st_name"].astype(bool)
        frame["is_star_st_name"] = fallback_flags["is_star_st_name"].astype(bool)
        frame["is_delisting_name"] = fallback_flags["is_delisting_name"].astype(bool)

        if not capital_frame.empty:
            capital_groups = {
                symbol: group.sort_values("effective_date").reset_index(drop=True)
                for symbol, group in capital_frame.groupby("symbol", sort=False)
            }
            capital_values = np.full(len(frame), np.nan, dtype=float)
            for symbol, positions in frame.groupby("symbol", sort=False).indices.items():
                capital_rows = capital_groups.get(symbol)
                if capital_rows is None or capital_rows.empty:
                    continue
                trade_dates = frame.loc[positions, "trading_date"].to_numpy(dtype="datetime64[ns]")
                effective_dates = capital_rows["effective_date"].to_numpy(dtype="datetime64[ns]")
                total_capital_values = capital_rows["total_capital"].to_numpy(dtype=float)
                indexer = effective_dates.searchsorted(trade_dates, side="right") - 1
                valid = indexer >= 0
                if valid.any():
                    capital_values[np.asarray(positions)[valid]] = total_capital_values[indexer[valid]]
            frame["total_capital"] = capital_values
        else:
            frame["total_capital"] = pd.NA

        frame["total_capital"] = frame["total_capital"].fillna(frame["symbol"].map(current_capital))
        frame["total_capital"] = frame.groupby("symbol", sort=False)["total_capital"].ffill().bfill()
        frame["total_capital_prev"] = frame.groupby("symbol", sort=False)["total_capital"].shift(1).fillna(frame["total_capital"])
        frame["raw_vwap"] = np.where(
            frame["volume"].fillna(0.0) > 0.0,
            frame["amount"].fillna(0.0) / (frame["volume"].fillna(0.0) * 100.0),
            np.nan,
        )
        frame["raw_vwap_prev"] = frame.groupby("symbol", sort=False)["raw_vwap"].shift(1)
        frame["ranking_price_prev"] = frame["raw_vwap_prev"].fillna(frame["prev_close"])
        frame["market_cap_prev"] = frame["ranking_price_prev"].fillna(0.0) * frame["total_capital_prev"].fillna(0.0)
        historical_special_treatment = self._load_historical_special_treatment_frame(frame, instrument_frame)
        if not historical_special_treatment.empty:
            frame = frame.drop(columns=["is_st_name", "is_star_st_name", "is_delisting_name"]).merge(
                historical_special_treatment,
                on=["symbol", "trading_date"],
                how="left",
            )
            frame["is_st_name"] = frame["is_st_name"].fillna(fallback_flags["is_st_name"]).astype(bool)
            frame["is_star_st_name"] = frame["is_star_st_name"].fillna(fallback_flags["is_star_st_name"]).astype(bool)
            frame["is_delisting_name"] = frame["is_delisting_name"].fillna(fallback_flags["is_delisting_name"]).astype(bool)
            grouped = frame.groupby("symbol", sort=False)
        frame["close_prev_5"] = grouped["close"].shift(self.cfg.industry_breadth_lookback_days)
        frame["close_prev_20"] = grouped["close"].shift(self.cfg.industry_lookback_days)
        frame["ret_5_prev"] = np.where(
            pd.to_numeric(frame["close_prev_5"], errors="coerce").fillna(0.0) > 0.0,
            pd.to_numeric(frame["prev_close"], errors="coerce") / pd.to_numeric(frame["close_prev_5"], errors="coerce") - 1.0,
            np.nan,
        )
        frame["ret_20_prev"] = np.where(
            pd.to_numeric(frame["close_prev_20"], errors="coerce").fillna(0.0) > 0.0,
            pd.to_numeric(frame["prev_close"], errors="coerce") / pd.to_numeric(frame["close_prev_20"], errors="coerce") - 1.0,
            np.nan,
        )
        frame["avg_amount_5_prev"] = grouped["amount"].transform(
            lambda series: series.rolling(self.cfg.industry_liquidity_lookback_days, min_periods=self.cfg.industry_liquidity_lookback_days).mean().shift(1)
        )
        frame["avg_amount_3_prev"] = grouped["amount"].transform(lambda series: series.rolling(3, min_periods=3).mean().shift(1))
        frame["avg_amount_60_prev"] = grouped["amount"].transform(lambda series: series.rolling(60, min_periods=60).mean().shift(1))
        amount_prev = grouped["amount"].shift(1)
        frame["amount_ratio_1_20_prev"] = np.where(
            pd.to_numeric(frame["avg_amount_20_prev"], errors="coerce").fillna(0.0) > 0.0,
            pd.to_numeric(amount_prev, errors="coerce") / pd.to_numeric(frame["avg_amount_20_prev"], errors="coerce"),
            np.nan,
        )
        frame["amount_ratio_3_20_prev"] = np.where(
            pd.to_numeric(frame["avg_amount_20_prev"], errors="coerce").fillna(0.0) > 0.0,
            pd.to_numeric(frame["avg_amount_3_prev"], errors="coerce") / pd.to_numeric(frame["avg_amount_20_prev"], errors="coerce") - 1.0,
            np.nan,
        )
        frame["amount_ratio_5_60_prev"] = np.where(
            pd.to_numeric(frame["avg_amount_60_prev"], errors="coerce").fillna(0.0) > 0.0,
            pd.to_numeric(frame["avg_amount_5_prev"], errors="coerce") / pd.to_numeric(frame["avg_amount_60_prev"], errors="coerce") - 1.0,
            np.nan,
        )
        frame["amount_ratio_5_20_prev"] = np.where(
            pd.to_numeric(frame["avg_amount_20_prev"], errors="coerce").fillna(0.0) > 0.0,
            pd.to_numeric(frame["avg_amount_5_prev"], errors="coerce") / pd.to_numeric(frame["avg_amount_20_prev"], errors="coerce") - 1.0,
            np.nan,
        )
        frame["close_prev_10"] = grouped["close"].shift(10)
        frame["close_prev_60"] = grouped["close"].shift(60)
        frame["close_prev_120"] = grouped["close"].shift(120)
        frame["ret_10_prev"] = np.where(
            pd.to_numeric(frame["close_prev_10"], errors="coerce").fillna(0.0) > 0.0,
            pd.to_numeric(frame["prev_close"], errors="coerce") / pd.to_numeric(frame["close_prev_10"], errors="coerce") - 1.0,
            np.nan,
        )
        frame["ma5_prev"] = grouped["close"].transform(lambda series: series.rolling(5, min_periods=5).mean().shift(1))
        frame["ma10_prev"] = grouped["close"].transform(lambda series: series.rolling(10, min_periods=10).mean().shift(1))
        frame["ret_60_prev"] = np.where(
            pd.to_numeric(frame["close_prev_60"], errors="coerce").fillna(0.0) > 0.0,
            pd.to_numeric(frame["prev_close"], errors="coerce") / pd.to_numeric(frame["close_prev_60"], errors="coerce") - 1.0,
            np.nan,
        )
        frame["ret_120_prev"] = np.where(
            pd.to_numeric(frame["close_prev_120"], errors="coerce").fillna(0.0) > 0.0,
            pd.to_numeric(frame["prev_close"], errors="coerce") / pd.to_numeric(frame["close_prev_120"], errors="coerce") - 1.0,
            np.nan,
        )
        frame["high_20_prev"] = grouped["high"].transform(lambda series: series.shift(1).rolling(20, min_periods=20).max())
        frame["high_120_prev"] = grouped["high"].transform(lambda series: series.shift(1).rolling(120, min_periods=120).max())
        frame["low_120_prev"] = grouped["low"].transform(lambda series: series.shift(1).rolling(120, min_periods=120).min())
        frame["close_120_prev"] = grouped["close"].shift(120)
        frame["breakout_gap_20_prev"] = np.where(
            pd.to_numeric(frame["high_20_prev"], errors="coerce").fillna(0.0) > 0.0,
            pd.to_numeric(frame["prev_close"], errors="coerce") / pd.to_numeric(frame["high_20_prev"], errors="coerce") - 1.0,
            np.nan,
        )
        frame["range_120_prev"] = np.where(
            pd.to_numeric(frame["low_120_prev"], errors="coerce").fillna(0.0) > 0.0,
            pd.to_numeric(frame["high_120_prev"], errors="coerce") / pd.to_numeric(frame["low_120_prev"], errors="coerce") - 1.0,
            np.nan,
        )
        frame["drawdown_from_high_120_prev"] = np.where(
            pd.to_numeric(frame["high_120_prev"], errors="coerce").fillna(0.0) > 0.0,
            pd.to_numeric(frame["prev_close"], errors="coerce") / pd.to_numeric(frame["high_120_prev"], errors="coerce") - 1.0,
            np.nan,
        )
        frame["zhuang_amp120_prev"] = np.where(
            pd.to_numeric(frame["low_120_prev"], errors="coerce").fillna(0.0) > 0.0,
            pd.to_numeric(frame["high_120_prev"], errors="coerce") / pd.to_numeric(frame["low_120_prev"], errors="coerce") - 1.0,
            np.nan,
        )
        frame["zhuang_ret120_prev"] = np.where(
            pd.to_numeric(frame["close_120_prev"], errors="coerce").fillna(0.0) > 0.0,
            pd.to_numeric(frame["prev_close"], errors="coerce") / pd.to_numeric(frame["close_120_prev"], errors="coerce") - 1.0,
            np.nan,
        )
        day_range = pd.to_numeric(frame["high"], errors="coerce") - pd.to_numeric(frame["low"], errors="coerce")
        upper_shadow = pd.to_numeric(frame["high"], errors="coerce") - np.maximum(
            pd.to_numeric(frame["open"], errors="coerce"),
            pd.to_numeric(frame["close"], errors="coerce"),
        )
        close_in_lower_half = pd.to_numeric(frame["close"], errors="coerce") <= (
            pd.to_numeric(frame["low"], errors="coerce") + 0.5 * day_range
        )
        upper_shadow_ratio = np.where(day_range > 0.0, upper_shadow / day_range, np.nan)
        amp_daily = np.where(
            pd.to_numeric(frame["low"], errors="coerce").fillna(0.0) > 0.0,
            pd.to_numeric(frame["high"], errors="coerce") / pd.to_numeric(frame["low"], errors="coerce") - 1.0,
            np.nan,
        )
        frame["zhuang_upper_shadow_flag"] = (
            (pd.Series(amp_daily, index=frame.index).fillna(-1.0) > self.cfg.zhuang_shadow_amp_min)
            & (pd.Series(upper_shadow_ratio, index=frame.index).fillna(-1.0) >= self.cfg.zhuang_upper_shadow_ratio)
            & pd.Series(close_in_lower_half, index=frame.index).fillna(False)
        ).astype(int)
        frame["zhuang_upper_shadow_count60_prev"] = grouped["zhuang_upper_shadow_flag"].transform(
            lambda series: series.shift(1).rolling(60, min_periods=60).sum()
        )
        intraday_attack = np.where(
            pd.to_numeric(frame["prev_close"], errors="coerce").fillna(0.0) > 0.0,
            pd.to_numeric(frame["high"], errors="coerce") / pd.to_numeric(frame["prev_close"], errors="coerce") - 1.0,
            np.nan,
        )
        close_ret = np.where(
            pd.to_numeric(frame["prev_close"], errors="coerce").fillna(0.0) > 0.0,
            pd.to_numeric(frame["close"], errors="coerce") / pd.to_numeric(frame["prev_close"], errors="coerce") - 1.0,
            np.nan,
        )
        frame["zhuang_failed_attack_flag"] = (
            (pd.Series(intraday_attack, index=frame.index).fillna(-1.0) >= self.cfg.zhuang_intraday_attack_min)
            & (
                (pd.Series(close_ret, index=frame.index).fillna(float("inf")) <= self.cfg.zhuang_close_ret_max)
                | pd.Series(close_in_lower_half, index=frame.index).fillna(False)
            )
        ).astype(int)
        frame["zhuang_failed_attack_count120_prev"] = grouped["zhuang_failed_attack_flag"].transform(
            lambda series: series.shift(1).rolling(120, min_periods=120).sum()
        )
        close_location = np.where(
            day_range > 0.0,
            ((pd.to_numeric(frame["close"], errors="coerce") - pd.to_numeric(frame["low"], errors="coerce")) / day_range) * 2.0 - 1.0,
            0.0,
        )
        frame["close_location_prev"] = pd.Series(close_location, index=frame.index).groupby(frame["symbol"], sort=False).shift(1)
        frame["close_ret_1"] = close_ret
        frame["close_ret_1_prev"] = grouped["close_ret_1"].shift(1)
        effective_cap = (pd.to_numeric(frame["close"], errors="coerce") * pd.to_numeric(frame["total_capital"], errors="coerce")).replace(0.0, np.nan)
        frame["ddx_proxy_raw"] = np.where(
            pd.to_numeric(effective_cap, errors="coerce").fillna(0.0) > 0.0,
            pd.Series(close_location, index=frame.index).fillna(0.0) * pd.to_numeric(frame["amount"], errors="coerce").fillna(0.0) / pd.to_numeric(effective_cap, errors="coerce"),
            np.nan,
        )
        frame["ddx_proxy_prev"] = grouped["ddx_proxy_raw"].shift(1)
        frame["ddx_proxy_3_prev"] = grouped["ddx_proxy_raw"].transform(lambda series: series.rolling(3, min_periods=3).mean().shift(1))
        frame["ddx_proxy_mean_20_prev"] = grouped["ddx_proxy_raw"].transform(lambda series: series.rolling(20, min_periods=20).mean().shift(1))
        frame["ddx_proxy_std_20_prev"] = grouped["ddx_proxy_raw"].transform(lambda series: series.rolling(20, min_periods=20).std().shift(1))
        frame["ddx_burst_z_prev"] = np.where(
            pd.to_numeric(frame["ddx_proxy_std_20_prev"], errors="coerce").fillna(0.0) > 0.0,
            (
                pd.to_numeric(frame["ddx_proxy_prev"], errors="coerce")
                - pd.to_numeric(frame["ddx_proxy_mean_20_prev"], errors="coerce")
            ) / pd.to_numeric(frame["ddx_proxy_std_20_prev"], errors="coerce"),
            np.nan,
        )
        frame["monster_burst_flag_prev"] = (
            (pd.to_numeric(frame["amount_ratio_1_20_prev"], errors="coerce").fillna(0.0) >= self.cfg.monster_spike_amount_ratio_min)
            & (pd.to_numeric(frame["ddx_burst_z_prev"], errors="coerce").fillna(-99.0) >= self.cfg.monster_ddx_burst_z_min)
        ).astype(int)
        frame["monster_strong_spike_flag_prev"] = (
            (pd.to_numeric(frame["amount_ratio_1_20_prev"], errors="coerce").fillna(0.0) >= self.cfg.monster_strong_spike_amount_ratio_min)
            & (pd.to_numeric(frame["ddx_burst_z_prev"], errors="coerce").fillna(-99.0) >= self.cfg.monster_strong_spike_ddx_z_min)
            & (pd.to_numeric(frame["close_ret_1_prev"], errors="coerce").fillna(-99.0) >= self.cfg.monster_strong_spike_close_ret_min)
            & (pd.to_numeric(frame["close_location_prev"], errors="coerce").fillna(-99.0) >= self.cfg.monster_strong_spike_close_location_min)
        ).astype(int)
        frame["monster_recent_spike_count_prev"] = grouped["monster_burst_flag_prev"].transform(
            lambda series: series.rolling(self.cfg.monster_spike_lookback_days, min_periods=1).sum()
        )
        frame["monster_recent_spike_strength_prev"] = grouped["amount_ratio_1_20_prev"].transform(
            lambda series: series.rolling(self.cfg.monster_spike_lookback_days, min_periods=1).max()
        )
        frame["monster_recent_ddx_burst_max_prev"] = grouped["ddx_burst_z_prev"].transform(
            lambda series: series.rolling(self.cfg.monster_spike_lookback_days, min_periods=1).max()
        )
        strong_flag = grouped["monster_strong_spike_flag_prev"]
        frame["monster_recent_strong_spike_count_prev"] = strong_flag.transform(
            lambda series: series.rolling(self.cfg.monster_strong_spike_lookback_days, min_periods=1).sum()
        )
        frame["monster_recent_strong_spike_amount_max_prev"] = grouped["amount_ratio_1_20_prev"].transform(
            lambda series: series.rolling(self.cfg.monster_strong_spike_lookback_days, min_periods=1).max()
        )
        frame["monster_recent_strong_ddx_burst_max_prev"] = grouped["ddx_burst_z_prev"].transform(
            lambda series: series.rolling(self.cfg.monster_strong_spike_lookback_days, min_periods=1).max()
        )
        frame["zhuang_score_prev"] = (
            (
                (pd.to_numeric(frame["zhuang_amp120_prev"], errors="coerce").fillna(-1.0) > self.cfg.zhuang_amp120_min)
                & (pd.to_numeric(frame["zhuang_ret120_prev"], errors="coerce").fillna(float("inf")) < self.cfg.zhuang_ret120_max)
            ).astype(int)
            + (pd.to_numeric(frame["zhuang_upper_shadow_count60_prev"], errors="coerce").fillna(0.0) >= self.cfg.zhuang_upper_shadow_count60).astype(int)
            + (pd.to_numeric(frame["zhuang_failed_attack_count120_prev"], errors="coerce").fillna(0.0) >= self.cfg.zhuang_failed_attack_count120).astype(int)
        )
        frame["is_old_zhuang_suspect_prev"] = frame["zhuang_score_prev"].fillna(0).astype(int) >= self.cfg.zhuang_score_threshold
        columns = [
            "trading_date",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "prev_close",
            "avg_volume_20_prev",
            "avg_amount_20_prev",
            "listed_days",
            "is_st_name",
            "is_star_st_name",
            "is_delisting_name",
            "is_beijing_stock",
            "is_b_share",
            "is_overlay_asset",
            "industry_code",
            "industry_name",
            "ret_5_prev",
            "ret_20_prev",
            "ma5_prev",
            "ma10_prev",
            "ret_60_prev",
            "ret_120_prev",
            "amount_ratio_5_20_prev",
            "amount_ratio_1_20_prev",
            "amount_ratio_3_20_prev",
            "amount_ratio_5_60_prev",
            "close_ret_1_prev",
            "close_location_prev",
            "breakout_gap_20_prev",
            "range_120_prev",
            "drawdown_from_high_120_prev",
            "ddx_proxy_prev",
            "ddx_proxy_3_prev",
            "ddx_burst_z_prev",
            "monster_recent_spike_count_prev",
            "monster_recent_spike_strength_prev",
            "monster_recent_ddx_burst_max_prev",
            "monster_recent_strong_spike_count_prev",
            "monster_recent_strong_spike_amount_max_prev",
            "monster_recent_strong_ddx_burst_max_prev",
            "market_cap_prev",
            "zhuang_score_prev",
            "is_old_zhuang_suspect_prev",
        ]
        frame["is_overlay_asset"] = False
        return frame.loc[:, columns].sort_values(["trading_date", "symbol"]).reset_index(drop=True)

    def _prepare_overlay_history(self, history: pd.DataFrame) -> pd.DataFrame:
        if history.empty:
            return pd.DataFrame(columns=[
                "trading_date",
                "symbol",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "prev_close",
                "avg_volume_20_prev",
                "avg_amount_20_prev",
                "listed_days",
                "is_st_name",
                "is_star_st_name",
                "is_delisting_name",
                "is_beijing_stock",
                "is_b_share",
                "is_overlay_asset",
                "industry_code",
                "industry_name",
                "ret_5_prev",
                "ret_20_prev",
                "amount_ratio_5_20_prev",
                "market_cap_prev",
                "zhuang_score_prev",
                "is_old_zhuang_suspect_prev",
            ])
        frame = history.sort_values(["symbol", "trading_date"]).reset_index(drop=True).copy()
        grouped = frame.groupby("symbol", sort=False)
        frame["prev_close"] = grouped["close"].shift(1)
        frame["avg_volume_20_prev"] = grouped["volume"].transform(lambda series: series.rolling(20, min_periods=20).mean().shift(1))
        frame["avg_amount_20_prev"] = grouped["amount"].transform(lambda series: series.rolling(20, min_periods=20).mean().shift(1))
        frame["listed_days"] = 9999
        frame["is_st_name"] = False
        frame["is_star_st_name"] = False
        frame["is_delisting_name"] = False
        frame["is_beijing_stock"] = False
        frame["is_b_share"] = False
        frame["is_overlay_asset"] = True
        frame["industry_code"] = ""
        frame["industry_name"] = ""
        frame["ret_5_prev"] = np.nan
        frame["ret_20_prev"] = np.nan
        frame["amount_ratio_5_20_prev"] = np.nan
        frame["market_cap_prev"] = float("inf")
        frame["zhuang_score_prev"] = 0
        frame["is_old_zhuang_suspect_prev"] = False
        return frame.loc[:, [
            "trading_date",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "prev_close",
            "avg_volume_20_prev",
            "avg_amount_20_prev",
            "listed_days",
            "is_st_name",
            "is_star_st_name",
            "is_delisting_name",
            "is_beijing_stock",
            "is_b_share",
            "is_overlay_asset",
            "industry_code",
            "industry_name",
            "ret_5_prev",
            "ret_20_prev",
            "amount_ratio_5_20_prev",
            "market_cap_prev",
            "zhuang_score_prev",
            "is_old_zhuang_suspect_prev",
        ]].sort_values(["trading_date", "symbol"]).reset_index(drop=True)

    def _simulate(
        self,
        prepared: pd.DataFrame,
        benchmark: pd.DataFrame,
        initial_cash: float,
        instrument_frame: pd.DataFrame,
    ) -> dict[str, Any]:
        prepared = prepared.sort_values(["trading_date", "market_cap_prev", "symbol"], ascending=[True, True, True]).reset_index(drop=True)
        cash = float(initial_cash)
        holdings: dict[str, dict[str, float]] = {}
        trade_rows: list[dict[str, Any]] = []
        daily_target_rows: list[dict[str, Any]] = []
        position_rows: list[dict[str, Any]] = []
        equity_rows: list[dict[str, Any]] = []
        benchmark_close = benchmark.set_index("trading_date")["close"].astype(float) if not benchmark.empty else pd.Series(dtype=float)
        benchmark_returns = benchmark_close.pct_change().fillna(0.0) if not benchmark_close.empty else pd.Series(dtype=float)
        benchmark_equity = float(initial_cash)
        cumulative_turnover = 0.0
        peak_equity = float(initial_cash)
        last_rebalance_date: pd.Timestamp | None = None
        name_map = (
            instrument_frame.drop_duplicates("symbol", keep="last").set_index("symbol")["instrument_name"].fillna("").to_dict()
            if not instrument_frame.empty
            else {}
        )
        overlay_symbol = str(self.cfg.hedge_symbol or "").strip()
        if overlay_symbol and overlay_symbol not in name_map:
            name_map[overlay_symbol] = self.cfg.hedge_name or overlay_symbol

        for trade_date, day_frame in prepared.groupby("trading_date", sort=False):
            day_rows: dict[str, Any] = {}
            remaining_trade_capacity: dict[str, int] = {}
            price_lookup: dict[str, float] = {}
            for row in day_frame.itertuples(index=False):
                symbol = str(row.symbol)
                day_rows[symbol] = row
                remaining_trade_capacity[symbol] = int(
                    volume_units_to_shares(float(getattr(row, "avg_volume_20_prev", 0.0) or 0.0)) * self.cfg.max_trade_volume_ratio
                )
                price_lookup[symbol] = float(getattr(row, "open", 0.0) or 0.0)
            open_value = cash
            for symbol, meta in holdings.items():
                day_row = day_rows.get(symbol)
                open_price = float(day_row.open) if day_row is not None else float(meta.get("last_close", meta["avg_cost"]))
                open_value += int(meta["shares"]) * open_price

            stock_holdings = [symbol for symbol in holdings.keys() if symbol != overlay_symbol]
            exit_signal_reasons: dict[str, str] = {}
            for symbol in stock_holdings:
                row = day_rows.get(symbol)
                meta = holdings.get(symbol)
                if row is None or meta is None:
                    continue
                reason = None
                if self.cfg.monster_prelude_enabled:
                    reason = _monster_exit_reason(row, meta, trade_date, self.cfg)
                elif self.cfg.surge_exit_enabled:
                    reason = _surge_exit_reason(row, meta, self.cfg)
                if reason:
                    exit_signal_reasons[symbol] = reason
            protected_stock_holdings = [
                symbol
                for symbol in stock_holdings
                if symbol not in exit_signal_reasons
                and (trade_date.normalize() - pd.Timestamp(holdings[symbol].get("hold_since", trade_date)).normalize()).days < self.cfg.min_holding_days
            ]
            rebalance_today = _should_rebalance_on_date(trade_date, last_rebalance_date, self.cfg)
            hedge_ratio = calendar_hedge_ratio(trade_date, self.cfg)
            invest_value = open_value * (1.0 - self.cfg.cash_buffer)
            stock_invest_value = invest_value * (1.0 - hedge_ratio)
            hedge_target_value = invest_value * hedge_ratio if overlay_symbol else 0.0
            if rebalance_today:
                prioritized_holdings = protected_stock_holdings + [symbol for symbol in stock_holdings if symbol not in set(protected_stock_holdings)]
                selection = build_portfolio_selection(
                    day_frame,
                    prioritized_holdings,
                    open_value * (1.0 - hedge_ratio),
                    self.cfg,
                )
                target_stocks = list(selection["targets"])
                ranked_count = int(selection["ranked_count"])
                industry_allocations = list(selection.get("industry_allocations") or [])
                layer_allocations = list(selection.get("layer_allocations") or [])
                zhuang_filtered_count = int(selection.get("zhuang_filtered_count") or 0)
                zhuang_replaced_count = int(selection.get("zhuang_replaced_count") or 0)
                fitted_targets = fit_target_count_by_cash(
                    target_stocks,
                    price_lookup=price_lookup,
                    invest_value=stock_invest_value,
                    cfg=self.cfg,
                )
                if self.cfg.monster_prelude_enabled or self.cfg.surge_exit_enabled:
                    surviving_holdings = [symbol for symbol in stock_holdings if symbol not in exit_signal_reasons]
                    open_slots = max(0, self.cfg.target_hold_num - len(surviving_holdings))
                    add_candidates = [symbol for symbol in fitted_targets if symbol not in set(surviving_holdings) and symbol not in exit_signal_reasons]
                    fitted_targets = surviving_holdings + add_candidates[:open_slots]
                last_rebalance_date = trade_date
            else:
                target_stocks = [symbol for symbol in stock_holdings if symbol not in exit_signal_reasons]
                ranked_count = len(target_stocks)
                industry_allocations = []
                layer_allocations = []
                zhuang_filtered_count = 0
                zhuang_replaced_count = 0
                fitted_targets = list(target_stocks)
            target_set = set(fitted_targets)
            target_num = len(fitted_targets)
            each_target_value = (stock_invest_value / float(target_num)) if target_num > 0 else 0.0
            daily_turnover = 0.0
            daily_fees = 0.0

            if exit_signal_reasons:
                for symbol, exit_reason in list(exit_signal_reasons.items()):
                    row = day_rows.get(symbol)
                    if row is None or symbol not in holdings:
                        continue
                    if not can_trade(symbol, trade_date, float(row.open), float(row.volume), float(row.prev_close or 0.0), is_buy=False):
                        continue
                    shares = int(holdings[symbol]["shares"])
                    if shares <= 0:
                        holdings.pop(symbol, None)
                        continue
                    sell_shares = available_trade_shares(
                        symbol,
                        shares,
                        remaining_trade_capacity.get(symbol, 0),
                        shares,
                        is_buy=False,
                    )
                    if sell_shares <= 0:
                        continue
                    price = execution_price(
                        symbol,
                        trade_date,
                        float(row.open),
                        float(row.prev_close or 0.0),
                        is_buy=False,
                        slippage_bps=self.cfg.sell_slippage_bps,
                    )
                    amount = sell_shares * price
                    fee = sell_fee(amount)
                    cash += amount - fee
                    daily_turnover += amount
                    daily_fees += fee
                    cumulative_turnover += amount
                    remaining_trade_capacity[symbol] = max(0, remaining_trade_capacity.get(symbol, 0) - sell_shares)
                    trade_rows.append(
                        {
                            "trading_date": trade_date.date().isoformat(),
                            "symbol": symbol,
                            "instrument_name": name_map.get(symbol, symbol),
                            "side": "SELL",
                            "shares": sell_shares,
                            "price": round(price, 4),
                            "amount": round(amount, 2),
                            "fee": round(fee, 2),
                            "reason": exit_reason,
                        }
                    )
                    holdings.pop(symbol, None)
            if rebalance_today and not self.cfg.monster_prelude_enabled:
                for symbol in list(holdings.keys()):
                    row = day_rows.get(symbol)
                    if symbol == overlay_symbol or symbol in target_set or row is None:
                        continue
                    if not can_trade(symbol, trade_date, float(row.open), float(row.volume), float(row.prev_close or 0.0), is_buy=False):
                        continue
                    shares = int(holdings[symbol]["shares"])
                    if shares <= 0:
                        holdings.pop(symbol, None)
                        continue
                    sell_shares = available_trade_shares(
                        symbol,
                        shares,
                        remaining_trade_capacity.get(symbol, 0),
                        shares,
                        is_buy=False,
                    )
                    if sell_shares <= 0:
                        continue
                    price = execution_price(
                        symbol,
                        trade_date,
                        float(row.open),
                        float(row.prev_close or 0.0),
                        is_buy=False,
                        slippage_bps=self.cfg.sell_slippage_bps,
                    )
                    amount = sell_shares * price
                    fee = sell_fee(amount)
                    cash += amount - fee
                    daily_turnover += amount
                    daily_fees += fee
                    cumulative_turnover += amount
                    remaining_trade_capacity[symbol] = max(0, remaining_trade_capacity.get(symbol, 0) - sell_shares)
                    trade_rows.append(
                        {
                            "trading_date": trade_date.date().isoformat(),
                            "symbol": symbol,
                            "instrument_name": name_map.get(symbol, symbol),
                            "side": "SELL",
                            "shares": sell_shares,
                            "price": round(price, 4),
                            "amount": round(amount, 2),
                            "fee": round(fee, 2),
                            "reason": "not_in_target",
                        }
                    )
                    remaining_shares = shares - sell_shares
                    if remaining_shares > 0:
                        holdings[symbol]["shares"] = remaining_shares
                    else:
                        holdings.pop(symbol, None)

                for symbol in fitted_targets:
                    row = day_rows.get(symbol)
                    if symbol not in holdings or row is None:
                        continue
                    if not can_trade(symbol, trade_date, float(row.open), float(row.volume), float(row.prev_close or 0.0), is_buy=False):
                        continue
                    open_price = float(row.open)
                    current_shares = int(holdings[symbol]["shares"])
                    current_value = current_shares * open_price
                    if current_value <= each_target_value * self.cfg.max_overweight_ratio:
                        continue
                    target_amount = calc_target_amount_by_value(symbol, each_target_value, open_price)
                    adjusted_target = adjust_target_amount_for_rules(symbol, current_shares, target_amount)
                    desired_sell = current_shares - adjusted_target
                    sell_shares = available_trade_shares(
                        symbol,
                        desired_sell,
                        remaining_trade_capacity.get(symbol, 0),
                        current_shares,
                        is_buy=False,
                    )
                    if sell_shares <= 0:
                        continue
                    sell_price = execution_price(
                        symbol,
                        trade_date,
                        float(row.open),
                        float(row.prev_close or 0.0),
                        is_buy=False,
                        slippage_bps=self.cfg.sell_slippage_bps,
                    )
                    amount = sell_shares * sell_price
                    fee = sell_fee(amount)
                    cash += amount - fee
                    daily_turnover += amount
                    daily_fees += fee
                    cumulative_turnover += amount
                    remaining_trade_capacity[symbol] = max(0, remaining_trade_capacity.get(symbol, 0) - sell_shares)
                    holdings[symbol]["shares"] = current_shares - sell_shares
                    trade_rows.append(
                        {
                            "trading_date": trade_date.date().isoformat(),
                            "symbol": symbol,
                            "instrument_name": name_map.get(symbol, symbol),
                            "side": "SELL",
                            "shares": sell_shares,
                            "price": round(sell_price, 4),
                            "amount": round(amount, 2),
                            "fee": round(fee, 2),
                            "reason": "overweight_trim",
                        }
                    )
                    if holdings[symbol]["shares"] <= 0:
                        holdings.pop(symbol, None)

            overlay_row = day_rows.get(overlay_symbol) if overlay_symbol else None
            if overlay_symbol and overlay_row is not None:
                row = overlay_row
                current_shares = int(holdings.get(overlay_symbol, {}).get("shares", 0))
                open_price = float(row.open)
                current_value = current_shares * open_price
                target_amount = calc_target_amount_by_value(overlay_symbol, hedge_target_value, open_price)
                adjusted_target = adjust_target_amount_for_rules(overlay_symbol, current_shares, target_amount)
                if adjusted_target < current_shares and can_trade(
                    overlay_symbol,
                    trade_date,
                    float(row.open),
                    float(row.volume),
                    float(row.prev_close or 0.0),
                    is_buy=False,
                ):
                    desired_sell = current_shares - adjusted_target
                    sell_shares = available_trade_shares(
                        overlay_symbol,
                        desired_sell,
                        remaining_trade_capacity.get(overlay_symbol, 0),
                        current_shares,
                        is_buy=False,
                    )
                    if sell_shares > 0:
                        sell_price = execution_price(
                            overlay_symbol,
                            trade_date,
                            float(row.open),
                            float(row.prev_close or 0.0),
                            is_buy=False,
                            slippage_bps=self.cfg.sell_slippage_bps,
                        )
                        amount = sell_shares * sell_price
                        fee = sell_fee(amount)
                        cash += amount - fee
                        daily_turnover += amount
                        daily_fees += fee
                        cumulative_turnover += amount
                        remaining_trade_capacity[overlay_symbol] = max(0, remaining_trade_capacity.get(overlay_symbol, 0) - sell_shares)
                        trade_rows.append(
                            {
                                "trading_date": trade_date.date().isoformat(),
                                "symbol": overlay_symbol,
                                "instrument_name": name_map.get(overlay_symbol, overlay_symbol),
                                "side": "SELL",
                                "shares": sell_shares,
                                "price": round(sell_price, 4),
                                "amount": round(amount, 2),
                                "fee": round(fee, 2),
                                "reason": "seasonal_hedge_reduce",
                            }
                        )
                        remaining_shares = current_shares - sell_shares
                        if remaining_shares > 0:
                            holdings[overlay_symbol]["shares"] = remaining_shares
                        else:
                            holdings.pop(overlay_symbol, None)
                elif adjusted_target > current_shares and can_trade(
                    overlay_symbol,
                    trade_date,
                    float(row.open),
                    float(row.volume),
                    float(row.prev_close or 0.0),
                    is_buy=True,
                ):
                    buy_price = execution_price(
                        overlay_symbol,
                        trade_date,
                        float(row.open),
                        float(row.prev_close or 0.0),
                        is_buy=True,
                        slippage_bps=self.cfg.buy_slippage_bps,
                    )
                    desired_buy = adjusted_target - current_shares
                    buy_shares = available_trade_shares(
                        overlay_symbol,
                        desired_buy,
                        remaining_trade_capacity.get(overlay_symbol, 0),
                        current_shares,
                        is_buy=True,
                    )
                    if buy_shares > 0:
                        amount = buy_shares * buy_price
                        fee = buy_fee(amount)
                        total_cash = amount + fee
                        if total_cash <= cash:
                            position = holdings.setdefault(overlay_symbol, {"shares": 0, "avg_cost": 0.0, "last_close": float(row.open), "hold_since": trade_date})
                            prev_shares = int(position["shares"])
                            new_shares = prev_shares + buy_shares
                            avg_cost_numerator = float(position["avg_cost"]) * prev_shares + amount + fee
                            position["shares"] = new_shares
                            position["avg_cost"] = avg_cost_numerator / float(new_shares)
                            if prev_shares <= 0:
                                position["hold_since"] = trade_date
                            cash -= total_cash
                            daily_turnover += amount
                            daily_fees += fee
                            cumulative_turnover += amount
                            remaining_trade_capacity[overlay_symbol] = max(0, remaining_trade_capacity.get(overlay_symbol, 0) - buy_shares)
                            trade_rows.append(
                                {
                                    "trading_date": trade_date.date().isoformat(),
                                    "symbol": overlay_symbol,
                                    "instrument_name": name_map.get(overlay_symbol, overlay_symbol),
                                    "side": "BUY",
                                    "shares": buy_shares,
                                    "price": round(buy_price, 4),
                                    "amount": round(amount, 2),
                                    "fee": round(fee, 2),
                                    "reason": "seasonal_hedge_buy",
                                }
                            )

            if rebalance_today:
                buy_plan: list[tuple[str, float]] = []
                for symbol in fitted_targets:
                    row = day_rows.get(symbol)
                    if row is None:
                        continue
                    if not can_trade(symbol, trade_date, float(row.open), float(row.volume), float(row.prev_close or 0.0), is_buy=True):
                        continue
                    open_price = float(row.open)
                    current_value = int(holdings.get(symbol, {}).get("shares", 0)) * open_price
                    gap_value = each_target_value - current_value
                    if gap_value <= 0:
                        continue
                    buy_plan.append((symbol, gap_value))
                buy_plan.sort(key=lambda item: item[1], reverse=True)

                for index, (symbol, gap_value) in enumerate(buy_plan):
                    remaining = len(buy_plan) - index
                    row = day_rows.get(symbol)
                    if remaining <= 0 or row is None:
                        continue
                    buy_price = execution_price(
                        symbol,
                        trade_date,
                        float(row.open),
                        float(row.prev_close or 0.0),
                        is_buy=True,
                        slippage_bps=self.cfg.buy_slippage_bps,
                    )
                    budget = min(float(gap_value), cash / float(remaining) * 0.98)
                    if budget < estimate_min_order_cost(symbol, buy_price):
                        continue
                    desired_buy = calc_amount_by_cash(symbol, budget, buy_price)
                    buy_shares = available_trade_shares(
                        symbol,
                        desired_buy,
                        remaining_trade_capacity.get(symbol, 0),
                        int(holdings.get(symbol, {}).get("shares", 0)),
                        is_buy=True,
                    )
                    if buy_shares <= 0:
                        continue
                    amount = buy_shares * buy_price
                    fee = buy_fee(amount)
                    total_cash = amount + fee
                    if total_cash > cash:
                        continue
                    position = holdings.setdefault(
                        symbol,
                        {"shares": 0, "avg_cost": 0.0, "last_close": float(row.open), "hold_since": trade_date, "peak_close": float(row.open)},
                    )
                    prev_shares = int(position["shares"])
                    new_shares = prev_shares + buy_shares
                    avg_cost_numerator = float(position["avg_cost"]) * prev_shares + amount + fee
                    position["shares"] = new_shares
                    position["avg_cost"] = avg_cost_numerator / float(new_shares)
                    if prev_shares <= 0:
                        position["hold_since"] = trade_date
                        position["peak_close"] = float(row.open)
                    cash -= total_cash
                    daily_turnover += amount
                    daily_fees += fee
                    cumulative_turnover += amount
                    remaining_trade_capacity[symbol] = max(0, remaining_trade_capacity.get(symbol, 0) - buy_shares)
                    trade_rows.append(
                        {
                            "trading_date": trade_date.date().isoformat(),
                            "symbol": symbol,
                            "instrument_name": name_map.get(symbol, symbol),
                            "side": "BUY",
                            "shares": buy_shares,
                            "price": round(buy_price, 4),
                            "amount": round(amount, 2),
                            "fee": round(fee, 2),
                            "reason": "rebalance_buy",
                        }
                    )

            end_value = cash
            for symbol, meta in list(holdings.items()):
                day_row = day_rows.get(symbol)
                close_price = float(day_row.close) if day_row is not None else float(meta.get("last_close", meta["avg_cost"]))
                meta["last_close"] = close_price
                meta["peak_close"] = max(float(meta.get("peak_close", close_price) or close_price), close_price)
                if int(meta["shares"]) <= 0:
                    holdings.pop(symbol, None)
                    continue
                end_value += int(meta["shares"]) * close_price
                position_rows.append(
                    {
                        "trading_date": trade_date,
                        "symbol": symbol,
                        "qty": int(meta["shares"]),
                        "cost_price": float(meta["avg_cost"]),
                        "market_price": close_price,
                    }
                )

            benchmark_equity *= 1.0 + float(benchmark_returns.get(trade_date, 0.0))
            peak_equity = max(peak_equity, end_value)
            drawdown = end_value / peak_equity - 1.0 if peak_equity > 0 else 0.0
            equity_rows.append(
                {
                    "trading_date": trade_date.date(),
                    "equity": float(end_value),
                    "cash": float(cash),
                    "turnover": float(cumulative_turnover),
                    "benchmark_equity": float(benchmark_equity),
                    "fees": float(daily_fees),
                    "max_drawdown": float(drawdown),
                }
            )
            daily_target_rows.append(
                {
                    "trading_date": trade_date.date().isoformat(),
                    "ranked_count": int(ranked_count),
                    "target_count": int(target_num),
                    "target_symbols": ",".join(fitted_targets),
                    "holding_count": int(len(holdings)),
                    "hedge_ratio": round(hedge_ratio, 4),
                    "hedge_symbol": overlay_symbol,
                    "hedge_target_value": round(hedge_target_value, 2),
                    "hedge_holding_shares": int(holdings.get(overlay_symbol, {}).get("shares", 0)) if overlay_symbol else 0,
                    "daily_turnover": round(daily_turnover, 2),
                    "daily_fees": round(daily_fees, 2),
                    "industry_selection_mode": (
                        "monster_prelude"
                        if self.cfg.monster_prelude_enabled
                        else ("layer_rotation" if self.cfg.layer_rotation_enabled else ("industry_weighted" if self.cfg.industry_weighted_enabled else "microcap_global"))
                    ),
                    "industry_allocations": json.dumps(industry_allocations, ensure_ascii=False),
                    "layer_allocations": json.dumps(layer_allocations, ensure_ascii=False),
                    "zhuang_filtered_count": zhuang_filtered_count,
                    "zhuang_replaced_count": zhuang_replaced_count,
                }
            )

        return {
            "equity_curve": pd.DataFrame(equity_rows),
            "positions": pd.DataFrame(position_rows),
            "trades": pd.DataFrame(trade_rows),
            "daily_targets": pd.DataFrame(daily_target_rows),
        }

    def _persist_results(self, summary: dict[str, Any], initial_cash: Decimal) -> tuple[Path, EvaluationResult, pd.DataFrame]:
        equity_curve = summary["equity_curve"].copy()
        positions = summary["positions"].copy()
        trades = summary["trades"].copy()
        daily_targets = summary["daily_targets"].copy()
        report_dir = _resolve_path(self.app_settings.report_dir)
        report_dir.mkdir(parents=True, exist_ok=True)
        equity_path = report_dir / "joinquant_microcap_equity.csv"
        trade_path = report_dir / "joinquant_microcap_trades.csv"
        target_path = report_dir / "joinquant_microcap_daily_targets.csv"
        summary_path = report_dir / "joinquant_microcap_summary.json"
        equity_curve.to_csv(equity_path, index=False, encoding="utf-8-sig")
        trades.to_csv(trade_path, index=False, encoding="utf-8-sig")
        daily_targets.to_csv(target_path, index=False, encoding="utf-8-sig")
        metrics = Evaluator().evaluate(equity_curve)

        with self.session_factory() as session:
            self._reset_non_live_state(session)
            asset_rows = []
            for row in equity_curve.to_dict(orient="records"):
                snapshot_time = datetime.combine(pd.Timestamp(row["trading_date"]).date(), datetime.min.time())
                total_asset = Decimal(str(row["equity"]))
                asset_rows.append(
                    {
                        "account_id": self.account_id,
                        "cash": Decimal(str(row.get("cash", 0.0))),
                        "frozen_cash": Decimal("0"),
                        "total_asset": total_asset,
                        "total_pnl": total_asset - initial_cash,
                        "turnover": Decimal(str(row.get("turnover", 0.0))),
                        "max_drawdown": Decimal(str(row.get("max_drawdown", 0.0))),
                        "snapshot_time": snapshot_time,
                    }
                )
            if asset_rows:
                session.bulk_insert_mappings(AssetSnapshotModel, asset_rows)
            if not positions.empty:
                position_rows = []
                for row in positions.to_dict(orient="records"):
                    snapshot_time = datetime.combine(pd.Timestamp(row["trading_date"]).date(), datetime.min.time())
                    position_rows.append(
                        {
                            "account_id": self.account_id,
                            "symbol": str(row["symbol"]),
                            "qty": int(row["qty"]),
                            "available_qty": int(row["qty"]),
                            "cost_price": Decimal(str(row["cost_price"])),
                            "market_price": Decimal(str(row["market_price"])),
                            "snapshot_time": snapshot_time,
                        }
                    )
                if position_rows:
                    session.bulk_insert_mappings(PositionSnapshotModel, position_rows)
            summary_payload = {
                "strategy": self.strategy_settings.implementation,
                "history_start": self.app_settings.history_start,
                "history_end": self.app_settings.history_end or (
                    str(equity_curve["trading_date"].iloc[-1]) if not equity_curve.empty else ""
                ),
                "report_dir": str(report_dir),
                "equity_path": str(equity_path),
                "trade_path": str(trade_path),
                "target_path": str(target_path),
                "benchmark_symbol": self.cfg.benchmark_symbol or self.app_settings.qlib_benchmark_symbol,
                "benchmark_name": self.cfg.benchmark_name or self.cfg.benchmark_symbol or self.app_settings.qlib_benchmark_symbol,
                "hedge_symbol": self.cfg.hedge_symbol,
                "hedge_name": self.cfg.hedge_name or self.cfg.hedge_symbol,
                "seasonal_hedge_schedule": self.cfg.seasonal_hedge_schedule,
                "industry_weighted_enabled": self.cfg.industry_weighted_enabled,
                "layer_rotation_enabled": self.cfg.layer_rotation_enabled,
                "monster_prelude_enabled": self.cfg.monster_prelude_enabled,
                "monster_market_cap_max": self.cfg.monster_market_cap_max,
                "monster_spike_lookback_days": self.cfg.monster_spike_lookback_days,
                "monster_spike_amount_ratio_min": self.cfg.monster_spike_amount_ratio_min,
                "monster_ddx_burst_z_min": self.cfg.monster_ddx_burst_z_min,
                "layer_market_cap_bounds": self.cfg.layer_market_cap_bounds,
                "layer_base_slots": self.cfg.layer_base_slots,
                "layer_max_slots": self.cfg.layer_max_slots,
                "industry_sector_prefix": self.cfg.industry_sector_prefix,
                "industry_top_k": self.cfg.industry_top_k,
                "industry_keep_top_k": self.cfg.industry_keep_top_k,
                "industry_min_slots": self.cfg.industry_min_slots,
                "industry_max_slots": self.cfg.industry_max_slots,
                "zhuang_filter_enabled": self.cfg.zhuang_filter_enabled,
                "zhuang_filter_mode": self.cfg.zhuang_filter_mode,
                "zhuang_final_replace_max_count": self.cfg.zhuang_final_replace_max_count,
                "zhuang_score_threshold": self.cfg.zhuang_score_threshold,
                "total_return": metrics.total_return,
                "annualized_return": metrics.annualized_return,
                "max_drawdown": metrics.max_drawdown,
                "turnover": metrics.turnover,
                "execution_assumptions": {
                    "buy_slippage_bps": self.cfg.buy_slippage_bps,
                    "sell_slippage_bps": self.cfg.sell_slippage_bps,
                    "max_trade_volume_ratio_pct": round(self.cfg.max_trade_volume_ratio * 100.0, 2),
                    "volume_window": "20d_avg_prev",
                    "ranking_price_source": "raw_vwap_prev",
                    "hedge_history_adjustment": self.cfg.hedge_history_adjustment,
                },
                "known_limitations": [
                    "历史股票池仍由当前板块成分回推，仍存在生存者偏差。",
                    "停牌与当日无成交的识别仍依赖日线结果，无法完全复原 09:35 盘中可成交状态。",
                ],
            }
            session.bulk_insert_mappings(
                AuditLogModel,
                [
                    {
                        "object_type": "backtest_engine",
                        "object_id": "joinquant_microcap",
                        "message": "JoinQuant microcap backtest finished",
                        "payload": summary_payload,
                    }
                ],
            )
            session.flush()
            report_path = AuditReportService().write_daily_report(session, report_dir)
            session.commit()

        summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return report_path, metrics, equity_curve

    def _reset_non_live_state(self, session) -> None:
        for model in [
            AuditLogModel,
            TradeModel,
            OrderEventModel,
            OrderModel,
            RiskDecisionModel,
            OrderIntentModel,
            PositionSnapshotModel,
            AssetSnapshotModel,
        ]:
            session.execute(delete(model))
        session.flush()
