"""执行模型（设计文档 §52）。

每种模型必须明确 Signal Time / Order Time / Execution Time，
禁止出现"T 日收盘生成信号又 T 日收盘成交"而无明确可执行时间定义。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time as dtime
from enum import Enum


class ExecutionModel(str, Enum):
    NEXT_OPEN = "next_open"
    CLOSE = "close"
    VWAP = "vwap"
    LIMIT_PRICE = "limit_price"


@dataclass(frozen=True)
class ExecutionSchedule:
    """一个调仓信号的三段时间契约。"""

    signal_time: datetime
    order_time: datetime
    execution_time: datetime


def schedule_for(model: ExecutionModel, signal_day: date, execution_day: date) -> ExecutionSchedule:
    """按执行模型生成时间契约。

    - NEXT_OPEN：T 日收盘出信号，T+1 开盘成交。
    - CLOSE：T 日收盘出信号，T+1 收盘成交（严禁 T 日收盘自成交）。
    - VWAP：T+1 全天 VWAP 近似成交。
    - LIMIT_PRICE：T+1 限价单。
    """
    signal_time = datetime.combine(signal_day, dtime(15, 0))
    if model == ExecutionModel.NEXT_OPEN:
        return ExecutionSchedule(
            signal_time=signal_time,
            order_time=datetime.combine(execution_day, dtime(9, 15)),
            execution_time=datetime.combine(execution_day, dtime(9, 30)),
        )
    if model == ExecutionModel.CLOSE:
        return ExecutionSchedule(
            signal_time=signal_time,
            order_time=datetime.combine(execution_day, dtime(9, 30)),
            execution_time=datetime.combine(execution_day, dtime(15, 0)),
        )
    if model == ExecutionModel.VWAP:
        return ExecutionSchedule(
            signal_time=signal_time,
            order_time=datetime.combine(execution_day, dtime(9, 30)),
            execution_time=datetime.combine(execution_day, dtime(11, 30)),
        )
    return ExecutionSchedule(
        signal_time=signal_time,
        order_time=datetime.combine(execution_day, dtime(9, 30)),
        execution_time=datetime.combine(execution_day, dtime(14, 57)),
    )


def resolve_fill_price(model: ExecutionModel, bar: dict) -> float | None:
    """按执行模型确定成交价（VWAP 为近似值，会打 VWAP_APPROXIMATED 质量标志）。"""
    open_price = bar.get("open")
    close_price = bar.get("close")
    high = bar.get("high")
    low = bar.get("low")
    if model == ExecutionModel.NEXT_OPEN:
        return open_price
    if model == ExecutionModel.CLOSE:
        return close_price
    if model == ExecutionModel.VWAP:
        if None in (high, low, close):
            return close_price
        return round((high + low + close) / 3.0, 6)
    return close_price


__all__ = ["ExecutionModel", "ExecutionSchedule", "resolve_fill_price", "schedule_for"]
