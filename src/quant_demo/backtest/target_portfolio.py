"""统一 Strategy Contract：Target Portfolio（设计文档 §17）。

执行引擎不关心策略来源（Quant Strategy / Factor Model / Agent Portfolio /
Manual），只消费带时间契约的目标组合。
"""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from quant_demo.backtest.time_contract import LookaheadViolation


class TargetWeight(BaseModel):
    symbol: str
    target_weight: float = Field(ge=0.0, le=1.0)


class TargetPortfolio(BaseModel):
    strategy_id: str = "strategy"
    strategy_version: str = "v1"
    signal_time: datetime
    available_at: datetime
    executable_from: datetime
    targets: list[TargetWeight] = Field(default_factory=list)

    def validate_time_contract(self) -> None:
        if self.available_at < self.signal_time:
            raise LookaheadViolation("available_at 早于 signal_time")
        if self.executable_from <= self.signal_time:
            raise LookaheadViolation("executable_from 必须晚于信号时间（T+1 可执行）")
        total = sum(target.target_weight for target in self.targets)
        if total > 1.0 + 1e-6:
            raise ValueError(f"目标权重总和 {total:.4f} 超过 1")


__all__ = ["TargetPortfolio", "TargetWeight"]
