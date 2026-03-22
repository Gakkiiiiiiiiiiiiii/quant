from __future__ import annotations

from datetime import datetime

from sqlalchemy.orm import Session

from quant_demo.db.models import PromotionRequestModel, StrategyVersionModel
from quant_demo.experiment.evaluator import EvaluationResult


class PromotionService:
    def create_promotion_request(
        self,
        session: Session,
        strategy_name: str,
        implementation: str,
        params: dict,
        metrics: EvaluationResult,
        requested_by: str = "Codex",
    ) -> tuple[StrategyVersionModel, PromotionRequestModel]:
        version = StrategyVersionModel(
            strategy_name=strategy_name,
            version_label=datetime.now().strftime("v%Y%m%d%H%M%S"),
            implementation=implementation,
            parameters=params,
            metrics={
                "total_return": metrics.total_return,
                "annualized_return": metrics.annualized_return,
                "max_drawdown": metrics.max_drawdown,
                "turnover": metrics.turnover,
            },
        )
        session.add(version)
        session.flush()
        request = PromotionRequestModel(
            strategy_version_id=version.strategy_version_id,
            object_type="promotion_request",
            requested_by=requested_by,
            status="pending",
            reason="回测结果满足人工审阅条件",
        )
        session.add(request)
        return version, request
