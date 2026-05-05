from __future__ import annotations

import argparse
import io
import json
import sys
from contextlib import redirect_stderr, redirect_stdout
from decimal import Decimal

from _bootstrap import ROOT, SRC

sys.path.insert(0, str(SRC))

from quant_demo.core.config import load_app_settings, load_strategy_settings, resolve_strategy_config_path
from quant_demo.db.session import create_session_factory
from quant_demo.experiment.manager import ExperimentManager
from quant_demo.experiment.qmt_microcap_trading import QmtMicrocapTradingEngine


MICROCAP_IMPLEMENTATIONS = {
    "joinquant_microcap_alpha",
    "joinquant_microcap_alpha_zf",
    "joinquant_microcap_alpha_zfe",
    "joinquant_microcap_alpha_zr",
    "joinquant_microcap_alpha_zro",
    "joinquant_microcap_alpha_cc",
    "monster_prelude_alpha",
    "microcap_100b_layer_rot",
    "microcap_50b_layer_rot",
    "industry_weighted_microcap_alpha",
}

NOISY_PREVIEW_MARKERS = (
    "服务器连接失败，请稍后再试。",
    "接收数据异常，请稍后再试。",
    "[WinError 10057]",
    "login success!",
    "logout success!",
)


def _build_preview_summary(payload: dict, qmt_client_name: str = "") -> dict:
    preview_orders = payload.get("preview_orders") or []
    target_meta = dict(payload.get("target_meta") or {})
    buy_orders = [order for order in preview_orders if str(order.get("side", "")).lower() == "buy"]
    sell_orders = [order for order in preview_orders if str(order.get("side", "")).lower() == "sell"]

    def _compact_order(order: dict) -> dict:
        return {
            "symbol": order.get("symbol"),
            "qty": order.get("qty"),
            "price": order.get("price"),
            "reason": order.get("reason"),
        }

    return {
        "environment": "paper",
        "qmt_client_name": qmt_client_name,
        "mode": "preview",
        "plan_path": payload.get("plan_path"),
        "signal_trade_date": payload.get("signal_trade_date"),
        "planned_execution_date": payload.get("planned_execution_date"),
        "strategy_total_asset": payload.get("strategy_total_asset"),
        "preview_order_count": len(preview_orders),
        "buy_order_count": len(buy_orders),
        "sell_order_count": len(sell_orders),
        "buy_symbols": [order.get("symbol") for order in buy_orders],
        "sell_symbols": [order.get("symbol") for order in sell_orders],
        "buy_orders": [_compact_order(order) for order in buy_orders],
        "sell_orders": [_compact_order(order) for order in sell_orders],
        "st_risk_blocked_count": int(target_meta.get("st_risk_blocked_count", 0) or 0),
        "st_risk_blocked_symbols": list(target_meta.get("st_risk_blocked_symbols") or []),
        "st_risk_sell_watch": list(target_meta.get("st_risk_sell_watch") or []),
        "forced_exit_untradable_symbols": list(target_meta.get("forced_exit_untradable_symbols") or []),
    }


def _run_preview_quietly(engine: QmtMicrocapTradingEngine, initial_cash: Decimal):
    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()
    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
        result = engine.preview(initial_cash)
    for stream in (stdout_buffer.getvalue(), stderr_buffer.getvalue()):
        for line in stream.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if any(marker in stripped for marker in NOISY_PREVIEW_MARKERS):
                continue
            print(stripped, file=sys.stderr)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="运行仿真盘")
    parser.add_argument("--config", default=str(ROOT / "configs" / "paper.yaml"))
    parser.add_argument("--strategy", default="")
    parser.add_argument("--mode", choices=["preview", "execute"], default="preview")
    parser.add_argument("--plan", default="")
    parser.add_argument("--capital", default="100000")
    args = parser.parse_args()

    app_settings = load_app_settings(args.config)
    strategy_path = resolve_strategy_config_path(
        args.strategy,
        ROOT / "configs" / "strategy",
        default_implementation=app_settings.default_strategy,
    )
    strategy_settings = load_strategy_settings(strategy_path)
    session_factory = create_session_factory(app_settings.database_url)
    initial_cash = Decimal(str(args.capital))

    if strategy_settings.implementation in MICROCAP_IMPLEMENTATIONS and app_settings.environment.value == "paper":
        engine = QmtMicrocapTradingEngine(session_factory, app_settings, strategy_settings)
        if args.mode == "preview":
            plan_path, payload = _run_preview_quietly(engine, initial_cash)
            payload["plan_path"] = str(plan_path)
            print(json.dumps(_build_preview_summary(payload, app_settings.qmt_client_name), ensure_ascii=False, indent=2))
            return
        result = engine.execute_plan(args.plan)
        print(
            json.dumps(
                {
                    "environment": app_settings.environment.value,
                    "qmt_client_name": app_settings.qmt_client_name,
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

