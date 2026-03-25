from __future__ import annotations

import argparse
import sys
from pathlib import Path

from _bootstrap import ROOT, SRC

sys.path.insert(0, str(SRC))

from quant_demo.api.app import serve


DEFAULT_FRONTEND_DIST = ROOT / "frontend" / "joinquant-vue" / "dist"


def run() -> None:
    parser = argparse.ArgumentParser(description="启动 Vue 版聚宽风格量化前端")
    parser.add_argument("--config", default=str(ROOT / "configs" / "app.yaml"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8501)
    parser.add_argument("--frontend-dist", default=str(DEFAULT_FRONTEND_DIST))
    args = parser.parse_args()

    serve(
        host=args.host,
        port=args.port,
        config_path=args.config,
        frontend_dist=str(Path(args.frontend_dist).resolve()),
    )


if __name__ == "__main__":
    run()
