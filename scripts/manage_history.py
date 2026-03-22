from __future__ import annotations

import argparse
import json
import sys

from _bootstrap import ROOT, SRC

sys.path.insert(0, str(SRC))

from quant_demo.core.config import load_app_settings
from quant_demo.marketdata.history_manager import cleanup_history_cache, history_status, refresh_history


def main() -> None:
    parser = argparse.ArgumentParser(description="维护历史数据与 Qlib 缓存")
    parser.add_argument("--config", default=str(ROOT / "configs" / "app.yaml"))
    parser.add_argument(
        "--mode",
        choices=["status", "incremental", "full", "cleanup-history", "cleanup-qlib", "cleanup-all"],
        default="status",
    )
    args = parser.parse_args()

    settings = load_app_settings(args.config)
    if args.mode == "status":
        result = history_status(settings)
    elif args.mode == "incremental":
        result = refresh_history(settings, mode="incremental")
    elif args.mode == "full":
        result = refresh_history(settings, mode="full")
    elif args.mode == "cleanup-history":
        result = cleanup_history_cache(settings, remove_history=True, remove_qlib=False)
    elif args.mode == "cleanup-qlib":
        result = cleanup_history_cache(settings, remove_history=False, remove_qlib=True)
    else:
        result = cleanup_history_cache(settings, remove_history=True, remove_qlib=True)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()