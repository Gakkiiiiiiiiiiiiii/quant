from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from _bootstrap import ROOT, SRC

sys.path.insert(0, str(SRC))

from quant_demo.core.config import load_app_settings, load_strategy_settings
from quant_demo.db.session import create_session_factory
from quant_demo.experiment.qmt_microcap_trading import QmtMicrocapTradingEngine


def _load_plan(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_latest_plan_path(report_dir: str) -> Path:
    return Path(report_dir) / "trade_plans" / "microcap_t1_plan_latest.json"


def _resolve_receipt_path(report_dir: str, signal_trade_date: str, planned_execution_date: str) -> Path:
    signal_date = signal_trade_date.replace("-", "")
    execution_date = planned_execution_date.replace("-", "")
    return Path(report_dir) / "trade_plans" / f"microcap_t1_execution_{signal_date}_for_{execution_date}.json"


def _now_local() -> datetime:
    return datetime.now()


def _seconds_until(target_hhmm: str, now: datetime) -> float:
    target_dt = datetime.strptime(f"{now.date()} {target_hhmm}", "%Y-%m-%d %H:%M")
    return (target_dt - now).total_seconds()


def main() -> None:
    parser = argparse.ArgumentParser(description="运行微盘股 T+1 定时仿真执行")
    parser.add_argument("--config", default=str(ROOT / "configs" / "paper.yaml"))
    parser.add_argument("--strategy", default=str(ROOT / "configs" / "strategy" / "joinquant_microcap_alpha.yaml"))
    parser.add_argument("--capital", default="100000")
    parser.add_argument("--execute-at", default="09:35")
    parser.add_argument("--poll-seconds", type=int, default=5)
    parser.add_argument("--force-refresh-plan", action="store_true")
    args = parser.parse_args()

    app_settings = load_app_settings(args.config)
    strategy_settings = load_strategy_settings(args.strategy)
    session_factory = create_session_factory(app_settings.database_url)
    initial_cash = Decimal(str(args.capital))
    engine = QmtMicrocapTradingEngine(session_factory, app_settings, strategy_settings)

    today = _now_local().date().isoformat()
    latest_plan_path = _resolve_latest_plan_path(app_settings.report_dir)
    plan_payload: dict | None = None
    plan_path: Path | None = None

    if latest_plan_path.exists() and not args.force_refresh_plan:
        cached = _load_plan(latest_plan_path)
        if str(cached.get("planned_execution_date") or "").strip() == today:
            plan_payload = cached
            plan_path = latest_plan_path

    if plan_payload is None or plan_path is None:
        plan_path, plan_payload = engine.preview(initial_cash)

    planned_execution_date = str(plan_payload.get("planned_execution_date") or "").strip()
    signal_trade_date = str(plan_payload.get("signal_trade_date") or "").strip()
    if planned_execution_date != today:
        raise RuntimeError(f"当前计划执行日为 {planned_execution_date}，今天是 {today}，请在对应交易日运行或重新生成计划")

    receipt_path = _resolve_receipt_path(app_settings.report_dir, signal_trade_date, planned_execution_date)
    if receipt_path.exists():
        raise RuntimeError(f"今天 {today} 的执行回执已存在，疑似已经执行过，回执文件: {receipt_path}")

    wait_seconds = _seconds_until(args.execute_at, _now_local())
    if wait_seconds > 0:
        print(
            json.dumps(
                {
                    "environment": app_settings.environment.value,
                    "mode": "timed-execute",
                    "plan_path": str(plan_path),
                    "signal_trade_date": signal_trade_date,
                    "planned_execution_date": planned_execution_date,
                    "execute_at": args.execute_at,
                    "wait_seconds": round(wait_seconds, 2),
                    "strategy_total_asset": plan_payload.get("strategy_total_asset"),
                    "preview_order_count": len(plan_payload.get("preview_orders") or []),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        while True:
            remaining = _seconds_until(args.execute_at, _now_local())
            if remaining <= 0:
                break
            time.sleep(min(max(args.poll_seconds, 1), 30))

    report_path, metrics, _equity_curve = engine.execute_plan(plan_path)
    print(
        json.dumps(
            {
                "environment": app_settings.environment.value,
                "mode": "timed-execute",
                "report_path": str(report_path),
                "signal_trade_date": signal_trade_date,
                "planned_execution_date": planned_execution_date,
                "total_return": metrics.total_return,
                "annualized_return": metrics.annualized_return,
                "max_drawdown": metrics.max_drawdown,
                "turnover": metrics.turnover,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
