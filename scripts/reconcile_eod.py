from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict

from _bootstrap import ROOT, SRC

sys.path.insert(0, str(SRC))

from quant_demo.audit.alerting import AlertingService
from quant_demo.audit.reconciliation import ReconciliationService
from quant_demo.core.config import load_app_settings
from quant_demo.db.session import create_session_factory


def main() -> None:
    parser = argparse.ArgumentParser(description="执行日终对账")
    parser.add_argument("--config", default=str(ROOT / "configs" / "paper.yaml"))
    args = parser.parse_args()

    app_settings = load_app_settings(args.config)
    session_factory = create_session_factory(app_settings.database_url)
    with session_factory() as session:
        result = ReconciliationService().reconcile(session)
    alerts = [asdict(alert) for alert in AlertingService().build_alerts(result)]
    print(json.dumps({"reconciliation": result, "alerts": alerts}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
