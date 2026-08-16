"""回测域：严格执行语义。

该模块承接 stock_agent Backtest 的严格语义迁移（设计文档 §15/§80）：
FeatureTimeContract / LookaheadViolation / T+1 / 涨跌停 / 停牌 / 统一成本。
"""
from __future__ import annotations

from quant_demo.backtest.engine import BacktestRunResult, EventDrivenBacktester
from quant_demo.backtest.execution_model import ExecutionModel, ExecutionSchedule
from quant_demo.backtest.target_portfolio import TargetPortfolio, TargetWeight
from quant_demo.backtest.time_contract import FeatureTimeContract, LookaheadViolation, ScoreMetadata
from quant_demo.backtest.transaction_cost import TransactionCostModel

__all__ = [
    "BacktestRunResult",
    "EventDrivenBacktester",
    "ExecutionModel",
    "ExecutionSchedule",
    "FeatureTimeContract",
    "LookaheadViolation",
    "ScoreMetadata",
    "TargetPortfolio",
    "TargetWeight",
    "TransactionCostModel",
]
