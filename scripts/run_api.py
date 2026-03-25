from __future__ import annotations

import argparse
import sys

from _bootstrap import ROOT, SRC

sys.path.insert(0, str(SRC))

from quant_demo.api.app import serve


def run() -> None:
    parser = argparse.ArgumentParser(description="启动量化系统本地 API")
    parser.add_argument("--config", default=str(ROOT / "configs" / "app.yaml"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8011)
    args = parser.parse_args()

    serve(host=args.host, port=args.port, config_path=args.config, frontend_dist=None)


if __name__ == "__main__":
    run()
