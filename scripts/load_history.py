from __future__ import annotations

import argparse
import json
import sys

from _bootstrap import ROOT, SRC

sys.path.insert(0, str(SRC))

from quant_demo.core.config import load_app_settings
from quant_demo.marketdata.history_manager import history_status, refresh_history


def main() -> None:
    parser = argparse.ArgumentParser(description="加载历史数据")
    parser.add_argument("--config", default=str(ROOT / "configs" / "app.yaml"))
    parser.add_argument("--mode", choices=["auto", "incremental", "full"], default="auto")
    args = parser.parse_args()
    settings = load_app_settings(args.config)
    refresh_result = refresh_history(settings, mode=args.mode)
    status = history_status(settings)
    payload = {"refresh": refresh_result, "status": status}
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()