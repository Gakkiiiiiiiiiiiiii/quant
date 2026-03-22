from __future__ import annotations

import argparse
import json
import sys

from _bootstrap import ROOT, SRC

sys.path.insert(0, str(SRC))

from quant_demo.adapters.qmt.quote_client import XtQuantQuoteClient
from quant_demo.adapters.qmt.trade_client import XtQuantTradeClient
from quant_demo.core.config import load_app_settings, load_strategy_settings
from quant_demo.core.exceptions import QmtUnavailableError
from quant_demo.db.session import create_session_factory
from quant_demo.experiment.manager import ExperimentManager


def probe_runtime(config_path: str) -> None:
    app_settings = load_app_settings(config_path)
    quote_client = XtQuantQuoteClient(app_settings)
    trade_client = XtQuantTradeClient(app_settings)
    payload = {
        "health": quote_client.healthcheck(),
        "instrument_details": quote_client.get_instrument_details(app_settings.symbols),
        "quotes": {symbol: str(price) for symbol, price in quote_client.get_latest_prices(app_settings.symbols).items()},
        "account": trade_client.get_account_snapshot(),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="运行实盘接入链路")
    parser.add_argument("--config", default=str(ROOT / "configs" / "live.yaml"))
    parser.add_argument("--strategy", default=str(ROOT / "configs" / "strategy" / "first_alpha_v1.yaml"))
    parser.add_argument("--mode", choices=["probe", "strategy"], default="probe")
    args = parser.parse_args()

    if args.mode == "probe":
        try:
            probe_runtime(args.config)
        except QmtUnavailableError as exc:
            print(f"QMT 未就绪: {exc}")
            raise SystemExit(1) from exc
        return

    app_settings = load_app_settings(args.config)
    strategy_settings = load_strategy_settings(args.strategy)
    session_factory = create_session_factory(app_settings.database_url)
    try:
        if not app_settings.qmt_trade_enabled:
            raise QmtUnavailableError("实盘策略模式已禁止自动委托，请先在 live.yaml 中显式开启 qmt_trade_enabled")
        result = ExperimentManager(session_factory, app_settings, strategy_settings).run()
        print(f"实盘链路执行完成，报告: {result.report_path}")
    except QmtUnavailableError as exc:
        print(f"QMT 未就绪: {exc}")
        print("请先执行 scripts/bootstrap_qmt.py 并完成客户端安装、登录与柜台账号配置。")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()

