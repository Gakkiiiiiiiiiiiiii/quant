from __future__ import annotations

import argparse
import json
import sys

from _bootstrap import ROOT, SRC

sys.path.insert(0, str(SRC))

from quant_demo.core.config import load_app_settings, load_strategy_settings
from quant_demo.db.session import create_session_factory
from quant_demo.experiment.evaluator import EvaluationResult
from quant_demo.experiment.promotion import PromotionService


def main() -> None:
    parser = argparse.ArgumentParser(description="创建策略晋升申请")
    parser.add_argument("--config", default=str(ROOT / "configs" / "app.yaml"))
    parser.add_argument("--strategy", default=str(ROOT / "configs" / "strategy" / "etf_rotation.yaml"))
    args = parser.parse_args()

    app_settings = load_app_settings(args.config)
    strategy_settings = load_strategy_settings(args.strategy)
    session_factory = create_session_factory(app_settings.database_url)
    metrics = EvaluationResult(total_return=0.12, annualized_return=0.16, max_drawdown=-0.05, turnover=1.4)
    with session_factory() as session:
        version, request = PromotionService().create_promotion_request(
            session,
            strategy_name=strategy_settings.name,
            implementation=strategy_settings.implementation,
            params=strategy_settings.model_dump(),
            metrics=metrics,
        )
        session.commit()
    print(json.dumps({"strategy_version_id": version.strategy_version_id, "promotion_request_id": request.promotion_request_id}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
