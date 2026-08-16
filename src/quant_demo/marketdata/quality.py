"""Market Data Quality Flags（详细修改方案 §18）。

Market API 必须随数据返回 quality_flags；生产 Backtest 对 critical quality flag
默认拒绝（除非显式 allow_critical_quality=true）。
"""
from __future__ import annotations

from datetime import date

import pandas as pd

MISSING_BAR = "MISSING_BAR"
DUPLICATE_BAR = "DUPLICATE_BAR"
OUT_OF_ORDER = "OUT_OF_ORDER"
STALE_DATA = "STALE_DATA"
UNRESOLVED_CORPORATE_ACTION = "UNRESOLVED_CORPORATE_ACTION"
MEMBERSHIP_GAP = "MEMBERSHIP_GAP"
CALENDAR_MISMATCH = "CALENDAR_MISMATCH"
UNKNOWN_PRICE_LIMIT = "UNKNOWN_PRICE_LIMIT"

STANDARD_QUALITY_FLAGS = (
    MISSING_BAR,
    DUPLICATE_BAR,
    OUT_OF_ORDER,
    STALE_DATA,
    UNRESOLVED_CORPORATE_ACTION,
    MEMBERSHIP_GAP,
    CALENDAR_MISMATCH,
    UNKNOWN_PRICE_LIMIT,
)

# 生产 Backtest 默认拒绝的 critical 标志。
CRITICAL_QUALITY_FLAGS = frozenset({DUPLICATE_BAR, OUT_OF_ORDER, CALENDAR_MISMATCH})


def evaluate_frame_quality(frame: pd.DataFrame, symbols: list[str], end_d: date) -> list[str]:
    """对窗口行情帧执行标准质量检查，返回命中的 flag 列表。"""
    flags: list[str] = []
    if frame.empty:
        return [MISSING_BAR]
    if frame.duplicated(subset=["symbol", "trading_date"]).any():
        flags.append(DUPLICATE_BAR)
    for symbol in symbols:
        series = frame.loc[frame["symbol"] == symbol, "trading_date"]
        if series.empty:
            flags.append(MISSING_BAR)
            break
        if series.is_monotonic_increasing is False:
            flags.append(OUT_OF_ORDER)
            break
    max_day = frame["trading_date"].max()
    if isinstance(max_day, pd.Timestamp):
        max_day = max_day.date()
    if max_day < end_d:
        flags.append(STALE_DATA)
    return flags


def critical_flags(quality_flags: list[str] | None) -> list[str]:
    return [flag for flag in (quality_flags or []) if flag in CRITICAL_QUALITY_FLAGS]


def merge_flags(*flag_lists: list[str] | None) -> list[str]:
    merged: list[str] = []
    for flags in flag_lists:
        for flag in flags or []:
            if flag not in merged:
                merged.append(flag)
    return merged


__all__ = [
    "MISSING_BAR",
    "DUPLICATE_BAR",
    "OUT_OF_ORDER",
    "STALE_DATA",
    "UNRESOLVED_CORPORATE_ACTION",
    "MEMBERSHIP_GAP",
    "CALENDAR_MISMATCH",
    "UNKNOWN_PRICE_LIMIT",
    "STANDARD_QUALITY_FLAGS",
    "CRITICAL_QUALITY_FLAGS",
    "evaluate_frame_quality",
    "critical_flags",
    "merge_flags",
]
