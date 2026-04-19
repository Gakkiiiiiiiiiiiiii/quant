from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal

from _bootstrap import ROOT, SRC

sys.path.insert(0, str(SRC))

from quant_demo.core.config import load_app_settings, load_strategy_settings
from quant_demo.db.session import create_session_factory
from quant_demo.experiment.manager import ExperimentManager
from quant_demo.experiment.qmt_microcap_trading import QmtMicrocapTradingEngine


MICROCAP_IMPLEMENTATIONS = {
    "joinquant_microcap_alpha",
    "joinquant_microcap_alpha_zf",
    "joinquant_microcap_alpha_zfe",
    "joinquant_microcap_alpha_zr",
    "joinquant_microcap_alpha_zro",
    "monster_prelude_alpha",
    "microcap_100b_layer_rot",
    "microcap_50b_layer_rot",
    "industry_weighted_microcap_alpha",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="运行仿真盘")
    parser.add_argument("--config", default=str(ROOT / "configs" / "paper.yaml"))
    parser.add_argument("--strategy", default=str(ROOT / "configs" / "strategy" / "joinquant_microcap_alpha.yaml"))
    parser.add_argument("--mode", choices=["preview", "execute"], default="preview")
    parser.add_argument("--plan", default="")
    parser.add_argument("--capital", default="100000")
    args = parser.parse_args()

    app_settings = load_app_settings(args.config)
    strategy_settings = load_strategy_settings(args.strategy)
    session_factory = create_session_factory(app_settings.database_url)
    initial_cash = Decimal(str(args.capital))

    if strategy_settings.implementation in MICROCAP_IMPLEMENTATIONS and app_settings.environment.value == "paper":
        engine = QmtMicrocapTradingEngine(session_factory, app_settings, strategy_settings)
        if args.mode == "preview":
            plan_path, payload = engine.preview(initial_cash)
            print(
                json.dumps(
                    {
                        "environment": app_settings.environment.value,
                        "mode": args.mode,
                        "plan_path": str(plan_path),
                        "signal_trade_date": payload.get("signal_trade_date"),
                        "planned_execution_date": payload.get("planned_execution_date"),
                        "strategy_total_asset": payload.get("strategy_total_asset"),
                        "preview_order_count": len(payload.get("preview_orders") or []),
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return
        result = engine.execute_plan(args.plan)
        print(
            json.dumps(
                {
                    "environment": app_settings.environment.value,
                    "mode": args.mode,
                    "report_path": str(result[0]),
                    "total_return": result[1].total_return,
                    "annualized_return": result[1].annualized_return,
                    "max_drawdown": result[1].max_drawdown,
                    "turnover": result[1].turnover,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    result = ExperimentManager(session_factory, app_settings, strategy_settings).run(initial_cash)
    print(json.dumps({"environment": app_settings.environment.value, "report_path": str(result.report_path)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

