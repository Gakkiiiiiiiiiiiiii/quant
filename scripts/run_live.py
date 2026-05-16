from __future__ import annotations

import argparse
import io
import json
import sys
import time
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from decimal import Decimal
from pathlib import Path

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


def _load_plan(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _emit_cli_log(event: str, **payload) -> None:
    print(
        json.dumps(
            {
                "ts": datetime.now().isoformat(timespec="seconds"),
                "event": event,
                **payload,
            },
            ensure_ascii=False,
        )
    )


def _now_local() -> datetime:
    return datetime.now()


def _seconds_until(target_hhmm: str, now: datetime) -> float:
    target_dt = datetime.strptime(f"{now.date()} {target_hhmm}", "%Y-%m-%d %H:%M")
    return (target_dt - now).total_seconds()


def _resolve_receipt_path(report_dir: str, signal_trade_date: str, planned_execution_date: str) -> Path:
    signal_date = signal_trade_date.replace("-", "")
    execution_date = planned_execution_date.replace("-", "")
    return Path(report_dir) / "trade_plans" / f"microcap_t1_execution_{signal_date}_for_{execution_date}.json"


def _resolve_latest_plan_path(report_dir: str) -> Path:
    return Path(report_dir) / "trade_plans" / "microcap_t1_plan_latest.json"


def _wait_until_trading_start(target_hhmm: str) -> None:
    last_bucket: tuple[str, int] | None = None
    while True:
        now = _now_local()
        remaining = _seconds_until(target_hhmm, now)
        if remaining <= 0:
            _emit_cli_log(
                "live_strategy_trading_window_open",
                trading_start=target_hhmm,
                current_time=now.strftime("%H:%M:%S"),
            )
            return
        remaining_seconds = int(round(remaining))
        bucket = "final" if remaining_seconds <= 10 else "minute"
        marker = remaining_seconds if bucket == "final" else remaining_seconds // 60
        if last_bucket != (bucket, marker):
            _emit_cli_log(
                "live_strategy_waiting_for_trading_start",
                trading_start=target_hhmm,
                current_time=now.strftime("%H:%M:%S"),
                remaining_seconds=remaining_seconds,
            )
            last_bucket = (bucket, marker)
        time.sleep(1 if remaining_seconds <= 10 else min(30, max(5, remaining_seconds % 60 or 30)))


def _extract_failed_order_summary(receipt_payload: dict) -> dict:
    execution_summary = dict(receipt_payload.get("execution_summary") or {})
    failed_orders = list(execution_summary.get("failed_orders") or [])
    if not failed_orders:
        failed_orders = [
            {
                "symbol": item.get("symbol"),
                "side": item.get("side"),
                "planned_qty": item.get("planned_qty"),
                "existing_qty": item.get("existing_qty"),
                "remaining_qty": item.get("remaining_qty"),
                "reason": item.get("reason"),
            }
            for item in execution_summary.get("differences") or []
            if int(item.get("remaining_qty") or 0) > 0
        ]
    failed_buy_symbols = sorted({str(item.get("symbol") or "") for item in failed_orders if str(item.get("side") or "") == "buy"})
    failed_sell_symbols = sorted({str(item.get("symbol") or "") for item in failed_orders if str(item.get("side") or "") == "sell"})
    return {
        "can_determine_failures": bool(execution_summary.get("can_determine_failures", True)),
        "failed_buy_symbols": [symbol for symbol in failed_buy_symbols if symbol],
        "failed_sell_symbols": [symbol for symbol in failed_sell_symbols if symbol],
        "failed_orders": failed_orders,
        "execution_summary_error": execution_summary.get("execution_summary_error", ""),
    }


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
    parser.add_argument("--force-refresh-plan", action="store_true")
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
        if strategy_settings.implementation in MICROCAP_IMPLEMENTATIONS:
            engine = QmtMicrocapTradingEngine(session_factory, app_settings, strategy_settings)
            initial_cash = Decimal(str(args.capital))
            today = _now_local().date().isoformat()
            latest_plan_path = _resolve_latest_plan_path(app_settings.report_dir)
            plan_path: Path | None = None
            payload: dict | None = None

            if latest_plan_path.exists() and not args.force_refresh_plan:
                try:
                    cached_payload = _load_plan(latest_plan_path)
                except Exception as exc:
                    _emit_cli_log(
                        "live_strategy_cached_plan_load_failed",
                        plan_path=str(latest_plan_path),
                        error=str(exc),
                    )
                else:
                    cached_execution_date = str(cached_payload.get("planned_execution_date") or "").strip()
                    if cached_execution_date == today:
                        plan_path = latest_plan_path
                        payload = cached_payload
                        _emit_cli_log(
                            "live_strategy_cached_plan_reused",
                            plan_path=str(plan_path),
                            signal_trade_date=str(payload.get("signal_trade_date") or "").strip(),
                            planned_execution_date=cached_execution_date,
                        )

            if plan_path is None or payload is None:
                plan_path, payload = _run_preview_quietly(engine, initial_cash)
                payload["plan_path"] = str(plan_path)
                _emit_cli_log(
                    "live_strategy_plan_generated",
                    plan_path=str(plan_path),
                    signal_trade_date=str(payload.get("signal_trade_date") or "").strip(),
                    planned_execution_date=str(payload.get("planned_execution_date") or "").strip(),
                )
            else:
                payload["plan_path"] = str(plan_path)

            preview_summary = _build_preview_summary(payload, app_settings.qmt_client_name)
            print(json.dumps({**preview_summary, "mode": "strategy-plan"}, ensure_ascii=False, indent=2))

            planned_execution_date = str(payload.get("planned_execution_date") or "").strip()
            signal_trade_date = str(payload.get("signal_trade_date") or "").strip()
            if planned_execution_date != today:
                raise RuntimeError(f"当前计划执行日为 {planned_execution_date}，今天是 {today}，请先刷新最新交易日后再执行。")
            if preview_summary["preview_order_count"] <= 0:
                _emit_cli_log(
                    "live_strategy_noop",
                    plan_path=str(plan_path),
                    signal_trade_date=signal_trade_date,
                    planned_execution_date=planned_execution_date,
                    message="今天没有可执行委托。",
                )
                return

            wait_seconds = _seconds_until(app_settings.risk.trading_start, _now_local())
            if wait_seconds > 0:
                _emit_cli_log(
                    "live_strategy_wait_scheduled",
                    plan_path=str(plan_path),
                    signal_trade_date=signal_trade_date,
                    planned_execution_date=planned_execution_date,
                    trading_start=app_settings.risk.trading_start,
                    current_time=_now_local().strftime("%H:%M:%S"),
                    wait_seconds=round(wait_seconds, 2),
                    buy_symbols=preview_summary["buy_symbols"],
                    sell_symbols=preview_summary["sell_symbols"],
                )
                _wait_until_trading_start(app_settings.risk.trading_start)
            else:
                _emit_cli_log(
                    "live_strategy_execute_now",
                    plan_path=str(plan_path),
                    signal_trade_date=signal_trade_date,
                    planned_execution_date=planned_execution_date,
                    trading_start=app_settings.risk.trading_start,
                    current_time=_now_local().strftime("%H:%M:%S"),
                    buy_symbols=preview_summary["buy_symbols"],
                    sell_symbols=preview_summary["sell_symbols"],
                )

            report_path, metrics, _equity_curve = engine.execute_plan(plan_path)
            receipt_path = _resolve_receipt_path(app_settings.report_dir, signal_trade_date, planned_execution_date)
            failed_summary = {
                "can_determine_failures": False,
                "failed_buy_symbols": [],
                "failed_sell_symbols": [],
                "failed_orders": [],
                "execution_summary_error": f"未找到执行回执: {receipt_path}",
            }
            if receipt_path.exists():
                receipt_payload = json.loads(receipt_path.read_text(encoding="utf-8"))
                failed_summary = _extract_failed_order_summary(receipt_payload)
            print(
                json.dumps(
                    {
                        "environment": app_settings.environment.value,
                        "qmt_client_name": app_settings.qmt_client_name,
                        "mode": "strategy",
                        "plan_path": str(plan_path),
                        "receipt_path": str(receipt_path),
                        "report_path": str(report_path),
                        "signal_trade_date": signal_trade_date,
                        "planned_execution_date": planned_execution_date,
                        "total_return": metrics.total_return,
                        "annualized_return": metrics.annualized_return,
                        "max_drawdown": metrics.max_drawdown,
                        "turnover": metrics.turnover,
                        **failed_summary,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return
        result = ExperimentManager(session_factory, app_settings, strategy_settings).run(Decimal(str(args.capital)))
        print(f"实盘链路执行完成，报告: {result.report_path}")
    except QmtUnavailableError as exc:
        print(f"QMT 未就绪: {exc}")
        print("请先执行 scripts/bootstrap_qmt.py 并完成客户端安装、登录与柜台账号配置。")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()

