from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from _bootstrap import ROOT, SRC

sys.path.insert(0, str(SRC))

from quant_demo.core.config import load_app_settings
from quant_demo.marketdata.history_manager import history_status, refresh_history


def _configure_logging(log_path: str) -> Path:
    resolved = Path(log_path)
    if not resolved.is_absolute():
        resolved = (ROOT / resolved).resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(resolved, mode="w", encoding="utf-8"),
        ],
        force=True,
    )
    return resolved


def main() -> None:
    parser = argparse.ArgumentParser(description="加载历史数据")
    parser.add_argument("--config", default=str(ROOT / "configs" / "app.yaml"))
    parser.add_argument("--mode", choices=["auto", "incremental", "full"], default="auto")
    parser.add_argument("--log-file", default="data/reports/history_refresh.log")
    args = parser.parse_args()
    log_path = _configure_logging(args.log_file)
    logger = logging.getLogger(__name__)
    logger.info("开始刷新历史数据: config=%s mode=%s", args.config, args.mode)
    logger.info("日志文件: %s", log_path)
    settings = load_app_settings(args.config)
    refresh_result = refresh_history(settings, mode=args.mode)
    logger.info("历史刷新完成: %s", refresh_result)
    status = history_status(settings)
    logger.info("当前历史状态: %s", status)
    payload = {"refresh": refresh_result, "status": status}
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
