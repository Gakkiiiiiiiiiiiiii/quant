from __future__ import annotations

import argparse
import json
import sys

from _bootstrap import ROOT, SRC

sys.path.insert(0, str(SRC))

from quant_demo.core.config import load_app_settings, load_strategy_settings
from quant_demo.db.session import create_session_factory
from quant_demo.experiment.manager import ExperimentManager


def main() -> None:
    parser = argparse.ArgumentParser(description="运行仿真盘")
    parser.add_argument("--config", default=str(ROOT / "configs" / "paper.yaml"))
    parser.add_argument("--strategy", default=str(ROOT / "configs" / "strategy" / "first_alpha_v1.yaml"))
    args = parser.parse_args()

    app_settings = load_app_settings(args.config)
    strategy_settings = load_strategy_settings(args.strategy)
    session_factory = create_session_factory(app_settings.database_url)
    result = ExperimentManager(session_factory, app_settings, strategy_settings).run()
    print(json.dumps({"environment": app_settings.environment.value, "report_path": str(result.report_path)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

