from __future__ import annotations

import argparse
import sys

from _bootstrap import ROOT, SRC

sys.path.insert(0, str(SRC))

from quant_demo.ui.streamlit_app import main


def run() -> None:
    parser = argparse.ArgumentParser(description="启动量化系统终端面板")
    parser.add_argument("--config", default=str(ROOT / "configs" / "app.yaml"))
    args = parser.parse_args()

    main(default_config_path=args.config)


if __name__ == "__main__":
    run()
