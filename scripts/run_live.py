from __future__ import annotations

import argparse
import io
import json
import sys
from contextlib import redirect_stderr, redirect_stdout
from decimal import Decimal

from _bootstrap import ROOT, SRC

sys.path.insert(0, str(SRC))

from quant_demo.adapters.qmt.gateway import create_gateway
from quant_demo.core.config import load_app_settings, load_strategy_settings, resolve_strategy_config_path
from quant_demo.core.exceptions import QmtUnavailableError
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


def probe_runtime(config_path: str) -> None:
    app_settings = load_app_settings(config_path)
    gateway = create_gateway(app_settings)
    payload = {
        "qmt_client_name": app_settings.qmt_client_name,
        "health": gateway.quote_client.healthcheck(),
        "instrument_details": gateway.quote_client.get_instrument_details(app_settings.symbols),
        "quotes": {symbol: str(price) for symbol, price in gateway.quote_client.get_latest_prices(app_settings.symbols).items()},
        "account": gateway.trade_client.get_account_snapshot(),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


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
        "environment": "live",
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
        "preview_halted_buy_symbols": list(target_meta.get("preview_halted_buy_symbols") or []),
        "forced_exit_untradable_symbols": list(target_meta.get("forced_exit_untradable_symbols") or []),
        "blocked_sell_symbols": list(payload.get("blocked_sell_symbols") or []),
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
    parser = argparse.ArgumentParser(description="运行实盘接入链路")
    parser.add_argument("--config", default=str(ROOT / "configs" / "live.yaml"))
    parser.add_argument("--strategy", default="")
    parser.add_argument("--mode", choices=["probe", "preview", "strategy"], default="probe")
    parser.add_argument("--capital", default="0")
    args = parser.parse_args()

    if args.mode == "probe":
        try:
            probe_runtime(args.config)
        except QmtUnavailableError as exc:
            print(f"QMT 未就绪: {exc}")
            raise SystemExit(1) from exc
        return

    app_settings = load_app_settings(args.config)
    strategy_path = resolve_strategy_config_path(
        args.strategy,
        ROOT / "configs" / "strategy",
        default_implementation=app_settings.default_strategy,
    )
    strategy_settings = load_strategy_settings(strategy_path)
    session_factory = create_session_factory(app_settings.database_url)
    try:
        if args.mode == "preview":
            if strategy_settings.implementation not in MICROCAP_IMPLEMENTATIONS:
                raise RuntimeError(f"实盘 preview 当前仅支持微盘策略，当前实现为: {strategy_settings.implementation}")
            engine = QmtMicrocapTradingEngine(session_factory, app_settings, strategy_settings)
            plan_path, payload = _run_preview_quietly(engine, Decimal(str(args.capital)))
            payload["plan_path"] = str(plan_path)
            print(json.dumps(_build_preview_summary(payload, app_settings.qmt_client_name), ensure_ascii=False, indent=2))
            return
        if not app_settings.qmt_trade_enabled:
            raise QmtUnavailableError("实盘策略模式已禁止自动委托，请先在 live.yaml 中显式开启 qmt_trade_enabled")
        result = ExperimentManager(session_factory, app_settings, strategy_settings).run(Decimal(str(args.capital)))
        print(f"实盘链路执行完成，报告: {result.report_path}")
    except QmtUnavailableError as exc:
        print(f"QMT 未就绪: {exc}")
        print("请先执行 scripts/bootstrap_qmt.py 并完成客户端安装、登录与柜台账号配置。")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()

