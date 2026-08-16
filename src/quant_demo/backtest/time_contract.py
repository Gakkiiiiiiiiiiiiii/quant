"""Feature Time Contract 与 Lookahead Guard（设计文档 §48）。

从 stock_agent 迁入的统一时间契约：任何调仓点若使用
``available_at > decision_time`` 的数据，立即判定 LOOKAHEAD_VIOLATION。
"""
from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel

from quant_demo.core.error_codes import LOOKAHEAD_VIOLATION


class LookaheadViolation(ValueError):
    """未来函数违规。"""

    code = LOOKAHEAD_VIOLATION


class ScoreMetadata(BaseModel):
    """信号时间契约元数据（feature_time <= available_at < executable_from）。"""

    feature_time: datetime
    available_at: datetime
    executable_from: datetime
    data_snapshot_id: str
    algorithm_version: str
    data_version: str | None = None


class FeatureTimeContract(BaseModel):
    """策略输出必须携带的时间契约（设计文档 §17 / §48）。"""

    signal_time: datetime
    available_at: datetime
    executable_from: datetime
    data_snapshot_id: str = ""
    algorithm_version: str = ""

    def validate_or_raise(self, decision_time: datetime | None = None) -> None:
        if self.available_at < self.signal_time:
            raise LookaheadViolation("available_at 早于 signal_time，时间契约非法")
        if self.executable_from <= self.signal_time:
            raise LookaheadViolation("executable_from 必须晚于信号时间（T+1 可执行）")
        if decision_time is not None and self.available_at > decision_time:
            raise LookaheadViolation(
                f"available_at={self.available_at.isoformat()} 晚于决策时间 "
                f"{decision_time.isoformat()}：LOOKAHEAD_VIOLATION"
            )


def validate_execution_day(metadata: ScoreMetadata, execution_date: date, decision_time: datetime) -> None:
    """在回测调仓点校验元数据（设计文档 §48）。"""
    if metadata.feature_time > metadata.available_at:
        raise LookaheadViolation("feature_time 晚于 available_at")
    if metadata.available_at >= metadata.executable_from:
        raise LookaheadViolation("available_at 必须早于 executable_from")
    if metadata.available_at > decision_time:
        raise LookaheadViolation(
            f"信号 available_at={metadata.available_at.isoformat()} 在决策时间 "
            f"{decision_time.isoformat()} 之后不可用：LOOKAHEAD_VIOLATION"
        )
    if metadata.executable_from.date() != execution_date:
        raise LookaheadViolation(
            f"executable_from={metadata.executable_from.date().isoformat()} "
            f"与执行日 {execution_date.isoformat()} 不一致"
        )


__all__ = ["FeatureTimeContract", "LookaheadViolation", "ScoreMetadata", "validate_execution_day"]
