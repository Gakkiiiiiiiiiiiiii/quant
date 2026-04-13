from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from sqlalchemy import create_engine, delete, select

from quant_demo.adapters.qmt.bridge_client import QmtBridgeClient
from quant_demo.core.config import AppSettings, load_app_settings
from quant_demo.core.enums import Environment
from quant_demo.core.exceptions import QmtUnavailableError
from quant_demo.db.models import CommonStrategyModel, StrategyBacktestResultModel
from quant_demo.db.session import create_session_factory, session_scope
from quant_demo.marketdata.history_manager import history_status

ROOT = Path(__file__).resolve().parents[3]
CONFIGS = {
    "backtest": ROOT / "configs" / "app.yaml",
    "paper": ROOT / "configs" / "paper.yaml",
    "live": ROOT / "configs" / "live.yaml",
}
STRATEGIES = {
    "ETF 轮动": ROOT / "configs" / "strategy" / "etf_rotation.yaml",
    "股票打分": ROOT / "configs" / "strategy" / "stock_ranking.yaml",
    "第一版策略": ROOT / "configs" / "strategy" / "first_alpha_v1.yaml",
    "聚宽风格": ROOT / "configs" / "strategy" / "joinquant_style.yaml",
    "聚宽微盘 Alpha": ROOT / "configs" / "strategy" / "joinquant_microcap_alpha.yaml",
}
USER_PATTERN_OPTIONS = {
    "形态策略 B1": "B1",
    "形态策略 B2": "B2",
    "形态策略 B3": "B3",
    "形态策略 砖型图": "BRICK",
    "四策略对比": "ALL",
}
PATTERN_COMPARE_MODES = ["B1", "B2", "B3", "BRICK"]
PATTERN_STRATEGY_META = {
    "B1": ("b1", "形态策略 B1"),
    "B2": ("b2", "形态策略 B2"),
    "B3": ("b3", "形态策略 B3"),
    "BRICK": ("brick", "形态策略 砖型图"),
}
REPORTS_ROOT = ROOT / "data" / "reports"
USER_PATTERN_REPORT_DIR = ROOT / "data" / "reports" / "user_pattern_backtests"
DEFAULT_PATTERN_REPORT_DIR_NAME = "user_pattern_backtests_b1sample_cover20230101_20260328"
USER_PATTERN_STRATEGY_FILE = ROOT / "strategy" / "strategy.py"
UI_RUNTIME = ROOT / "runtime" / "ui_runtime"
DEMO_LOG_DIR = ROOT / "runtime" / "demo_logs"


@dataclass(slots=True)
class DashboardData:
    assets: pd.DataFrame
    positions: pd.DataFrame
    orders: pd.DataFrame
    trades: pd.DataFrame
    risk: pd.DataFrame
    audit: pd.DataFrame
    rules: pd.DataFrame
    report_text: str
    benchmark_curve: pd.DataFrame


def _empty_dashboard_data() -> DashboardData:
    return DashboardData(
        assets=pd.DataFrame(),
        positions=pd.DataFrame(),
        orders=pd.DataFrame(),
        trades=pd.DataFrame(),
        risk=pd.DataFrame(),
        audit=pd.DataFrame(),
        rules=pd.DataFrame(),
        report_text="暂无日终报告。",
        benchmark_curve=pd.DataFrame(),
    )


def _read_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")


def _decode(payload: bytes | str) -> str:
    if isinstance(payload, str):
        return payload
    for encoding in ("utf-8", "gb18030"):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    return payload.decode("utf-8", errors="ignore")


def _tail(path: Path | None, lines: int = 80) -> str:
    if path is None or not path.exists() or not path.is_file():
        return "日志文件不存在。"
    text = _decode(path.read_bytes())
    return "\n".join(text.splitlines()[-lines:]) or "日志为空。"


def _report_root(report_dir: str) -> Path:
    path = Path(report_dir)
    return ROOT / path if not path.is_absolute() else path


def _report_path(report_dir: str) -> Path:
    return _report_root(report_dir) / "daily_report.md"


def _sql(connection, query: str, dates: list[str] | None = None) -> pd.DataFrame:
    try:
        frame = pd.read_sql(query, connection)
    except Exception:
        return pd.DataFrame()
    for col in dates or []:
        if col in frame.columns:
            frame[col] = pd.to_datetime(frame[col], errors="coerce")
    return frame


def _expand_rules(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if frame.empty:
        return pd.DataFrame()
    for record in frame.to_dict("records"):
        raw = record.get("rule_results")
        payload = json.loads(raw) if isinstance(raw, str) else (raw or [])
        for item in payload:
            rows.append(
                {
                    "decided_at": record.get("decided_at"),
                    "decision_status": record.get("status"),
                    "rule_name": item.get("rule_name"),
                    "passed": item.get("passed"),
                    "message": item.get("message"),
                }
            )
    return pd.DataFrame(rows)


def _frame_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def _json_scalar(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except ValueError:
            return value
    return value


def _json_object(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: _json_scalar(value) for key, value in payload.items()}


def _jsonify(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _jsonify(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonify(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonify(item) for item in value]
    return _json_scalar(value)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_decimal(value: Any) -> Decimal | None:
    number = _safe_float(value)
    if number is None:
        return None
    return Decimal(str(number))


def _format_money_text(value: Any) -> str:
    number = _safe_float(value)
    if number is None:
        return "--"
    return f"{number:,.2f}"


def _format_ratio_text(value: Any) -> str:
    number = _safe_float(value)
    if number is None:
        return "--"
    return f"{number * 100:.2f}%"


def _build_joinquant_microcap_report_text(
    summary: dict[str, Any],
    equity: pd.DataFrame,
    trades: pd.DataFrame,
) -> str:
    latest = equity.iloc[-1] if not equity.empty else {}
    benchmark_return = None
    benchmark_label = str(summary.get("benchmark_name") or summary.get("benchmark_symbol") or "Benchmark")
    if not equity.empty and "benchmark_equity" in equity.columns:
        benchmark_start = _safe_float(equity.iloc[0].get("benchmark_equity"))
        benchmark_end = _safe_float(latest.get("benchmark_equity"))
        if benchmark_start:
            benchmark_return = (benchmark_end - benchmark_start) / benchmark_start
    lines = [
        "# 聚宽微盘 Alpha 回测报告",
        "",
        f"策略标识: {summary.get('strategy', 'joinquant_microcap_alpha')}",
        f"回测区间: {summary.get('history_start', '--')} 至 {summary.get('history_end', '--')}",
        f"期末权益: {_format_money_text(latest.get('equity'))}",
        f"期末现金: {_format_money_text(latest.get('cash'))}",
        f"总收益: {_format_ratio_text(summary.get('total_return'))}",
        f"年化收益: {_format_ratio_text(summary.get('annualized_return'))}",
        f"最大回撤: {_format_ratio_text(summary.get('max_drawdown'))}",
        f"累计换手: {_format_money_text(summary.get('turnover'))}",
        f"回测基准: {benchmark_label}",
        f"基准收益: {_format_ratio_text(benchmark_return)}",
        f"成交笔数: {len(trades)}",
    ]
    hedge_symbol = str(summary.get("hedge_symbol") or "").strip()
    hedge_name = str(summary.get("hedge_name") or "").strip()
    hedge_schedule = summary.get("seasonal_hedge_schedule") or {}
    if hedge_symbol or hedge_name or hedge_schedule:
        lines.extend(
            [
                f"月历对冲: {hedge_name or hedge_symbol or '现金'}",
                f"弱势月对冲表: {hedge_schedule}",
            ]
        )
    execution_assumptions = summary.get("execution_assumptions")
    if isinstance(execution_assumptions, dict) and execution_assumptions:
        lines.extend(
            [
                "",
                "## 成交假设",
                f"- 买入滑点: {execution_assumptions.get('buy_slippage_bps', '--')} bps",
                f"- 卖出滑点: {execution_assumptions.get('sell_slippage_bps', '--')} bps",
                f"- 单日成交上限: 过去20日平均成交量的 {execution_assumptions.get('max_trade_volume_ratio_pct', '--')}%",
            ]
        )
    known_limitations = summary.get("known_limitations")
    if isinstance(known_limitations, list) and known_limitations:
        lines.extend(["", "## 已知限制"])
        lines.extend(f"- {item}" for item in known_limitations)
    return "\n".join(lines)


def _load_joinquant_microcap_report(report_dir: str) -> DashboardData | None:
    report_root = _report_root(report_dir)
    summary_path = report_root / "joinquant_microcap_summary.json"
    equity_path = report_root / "joinquant_microcap_equity.csv"
    trade_path = report_root / "joinquant_microcap_trades.csv"
    if not equity_path.exists():
        return None

    try:
        equity = pd.read_csv(equity_path)
    except Exception:
        return None
    if equity.empty or "equity" not in equity.columns:
        return None

    if "trading_date" in equity.columns:
        equity["trading_date"] = pd.to_datetime(equity["trading_date"], errors="coerce")

    assets = equity.rename(columns={"trading_date": "snapshot_time", "equity": "total_asset"}).copy()
    assets["account_id"] = "joinquant_microcap_alpha"
    if {"total_asset", "cash"}.issubset(assets.columns):
        assets["market_value"] = pd.to_numeric(assets["total_asset"], errors="coerce") - pd.to_numeric(assets["cash"], errors="coerce")
    if "total_pnl" not in assets.columns:
        start_asset = _safe_float(assets.iloc[0].get("total_asset"))
        assets["total_pnl"] = pd.NA
        if start_asset is not None:
            assets["total_pnl"] = pd.to_numeric(assets["total_asset"], errors="coerce") - start_asset

    benchmark_curve = pd.DataFrame()
    if {"trading_date", "benchmark_equity"}.issubset(equity.columns):
        benchmark_curve = equity[["trading_date", "benchmark_equity"]].copy()

    trades = pd.DataFrame()
    if trade_path.exists():
        try:
            trades = pd.read_csv(trade_path)
            if "trading_date" in trades.columns:
                trades["trading_date"] = pd.to_datetime(trades["trading_date"], errors="coerce")
        except Exception:
            trades = pd.DataFrame()

    orders = pd.DataFrame()
    if not trades.empty:
        orders = trades.rename(
            columns={
                "trading_date": "created_at",
                "shares": "qty",
                "price": "avg_price",
            }
        ).copy()
        orders["updated_at"] = orders["created_at"]
        orders["filled_qty"] = orders["qty"]
        orders["status"] = "filled"
        orders["order_id"] = range(1, len(orders) + 1)

    summary: dict[str, Any] = {}
    if summary_path.exists():
        try:
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                summary = payload
        except Exception:
            summary = {}

    report_text = _build_joinquant_microcap_report_text(summary, equity, trades)
    return DashboardData(
        assets=assets,
        positions=pd.DataFrame(),
        orders=orders,
        trades=trades,
        risk=pd.DataFrame(),
        audit=pd.DataFrame(),
        rules=pd.DataFrame(),
        report_text=report_text,
        benchmark_curve=benchmark_curve,
    )


def _is_joinquant_microcap_dashboard(data: DashboardData) -> bool:
    if data.assets.empty or "account_id" not in data.assets.columns:
        return False
    account_id = str(data.assets.iloc[-1].get("account_id") or "")
    return account_id == "joinquant_microcap_alpha"


def resolve_profile_path(profile: str) -> Path:
    key = (profile or "backtest").strip().lower()
    return CONFIGS.get(key, CONFIGS["backtest"])


def resolve_settings(profile: str = "backtest", config_path: str | None = None) -> tuple[str, Path, AppSettings]:
    resolved_path = Path(config_path).resolve() if config_path else resolve_profile_path(profile).resolve()
    settings = load_app_settings(resolved_path)
    profile_key = next(
        (name for name, path in CONFIGS.items() if path.resolve() == resolved_path),
        settings.environment.value,
    )
    return profile_key, resolved_path, settings


def load_dashboard_data(database_url: str, report_dir: str) -> DashboardData:
    microcap_data = _load_joinquant_microcap_report(report_dir)
    if microcap_data is not None:
        return microcap_data
    engine = create_engine(database_url, future=True)
    with engine.connect() as conn:
        assets = _sql(
            conn,
            "select account_id,snapshot_time,total_asset,cash,frozen_cash,total_pnl,turnover,max_drawdown from asset_snapshots order by snapshot_time",
            ["snapshot_time"],
        )
        positions = _sql(
            conn,
            "select symbol,qty,available_qty,cost_price,market_price,qty*market_price as market_value,(market_price-cost_price)*qty as unrealized_pnl,snapshot_time from position_snapshots where snapshot_time=(select max(snapshot_time) from position_snapshots) order by market_value desc",
            ["snapshot_time"],
        )
        orders = _sql(
            conn,
            "select order_id,broker_order_id,symbol,side,qty,filled_qty,status,avg_price,created_at,updated_at from orders order by created_at desc limit 500",
            ["created_at", "updated_at"],
        )
        trades = _sql(
            conn,
            "select trade_id,order_id,symbol,side,fill_qty,fill_price,commission,trade_time from trades order by trade_time desc limit 500",
            ["trade_time"],
        )
        risk = _sql(
            conn,
            "select risk_decision_id,order_intent_id,status,rule_results,decided_at from risk_decisions order by decided_at desc limit 500",
            ["decided_at"],
        )
        audit = _sql(
            conn,
            "select audit_log_id,object_type,object_id,message,payload,created_at from audit_logs order by created_at desc limit 500",
            ["created_at"],
        )
    report_file = _report_path(report_dir)
    report_text = _decode(report_file.read_bytes()) if report_file.exists() else "暂无日终报告。"
    benchmark_curve = pd.DataFrame()
    curve_file = (ROOT / report_dir if not Path(report_dir).is_absolute() else Path(report_dir)) / "qlib_curve.csv"
    if curve_file.exists():
        try:
            benchmark_curve = pd.read_csv(curve_file)
            if "trading_date" in benchmark_curve.columns:
                benchmark_curve["trading_date"] = pd.to_datetime(benchmark_curve["trading_date"], errors="coerce")
        except Exception:
            benchmark_curve = pd.DataFrame()
    return DashboardData(assets, positions, orders, trades, risk, audit, _expand_rules(risk), report_text, benchmark_curve)


def load_live_probe(config_path: Path, settings: AppSettings) -> dict[str, Any]:
    _ = config_path
    if settings.environment == Environment.BACKTEST:
        return {}
    bridge = QmtBridgeClient(settings)
    try:
        return {
            "health": bridge.healthcheck(),
            "quotes": bridge.get_quotes(settings.symbols),
            "account": bridge.get_account_snapshot(),
        }
    except QmtUnavailableError as exc:
        return {"error": str(exc)}


def _parse_json_payload(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str):
        try:
            loaded = json.loads(payload)
        except json.JSONDecodeError:
            return {}
        return loaded if isinstance(loaded, dict) else {}
    return {}


def _normalize_symbol_text(value: Any) -> str:
    if value in (None, ""):
        return ""
    return str(value).strip()


def _pick_text(payload: dict[str, Any], *keys: str, default: str = "") -> str:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return default


def _pick_number(payload: dict[str, Any], *keys: str, default: float = 0.0) -> float:
    for key in keys:
        if key in payload:
            value = _safe_float(payload.get(key))
            if value is not None:
                return value
    return default


def _estimate_next_trading_day(raw_trade_date: Any) -> str | None:
    trade_date = pd.to_datetime(raw_trade_date, errors="coerce")
    if pd.isna(trade_date):
        return None
    return (trade_date.normalize() + pd.offsets.BDay(1)).date().isoformat()


def _format_side_label(side: str) -> str:
    normalized = str(side or "").strip().lower()
    if normalized == "buy":
        return "买入"
    if normalized == "sell":
        return "卖出"
    return normalized or "--"


def _normalize_side(value: Any) -> str:
    if value in (None, ""):
        return ""
    raw = str(value).strip().lower()
    if raw in {"buy", "b", "long", "买入", "证券买入"}:
        return "buy"
    if raw in {"sell", "s", "short", "卖出", "证券卖出"}:
        return "sell"
    if raw.isdigit():
        number = int(raw)
        if number in {1, 23, 48}:
            return "buy"
        if number in {2, 24, 49}:
            return "sell"
    if "买" in raw:
        return "buy"
    if "卖" in raw:
        return "sell"
    return raw


def _build_qmt_plan_rows(plan_payload: dict[str, Any], side: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    target_side = str(side).strip().lower()
    for item in plan_payload.get("planned_orders") or []:
        if str(item.get("side") or "").strip().lower() != target_side:
            continue
        qty = int(_safe_float(item.get("qty")) or 0)
        price = _safe_float(item.get("price"))
        amount = (price * qty) if price is not None and qty > 0 else None
        rows.append(
            {
                "symbol": _normalize_symbol_text(item.get("symbol")),
                "instrument_name": item.get("instrument_name") or "--",
                "qty": qty,
                "price": price,
                "amount": amount,
                "reason": item.get("reason") or "--",
                "side_label": _format_side_label(target_side),
            }
        )
    return rows


def _build_actual_trade_rows_from_qmt_snapshot(
    snapshot: dict[str, Any],
    trade_date: str | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    actual_buys: list[dict[str, Any]] = []
    actual_sells: list[dict[str, Any]] = []
    target_trade_date = pd.to_datetime(trade_date, errors="coerce")
    existing_keys: set[tuple[str, str, str]] = set()
    used_trades = False
    used_orders = False

    trades = snapshot.get("trades", []) or []
    if trades:
        for item in trades:
            side = _normalize_side(
                _pick_text(item, "side", "order_side", "direction", "entrust_bs", "bs_flag", "operation")
                or item.get("order_type")
                or item.get("m_nOrderType")
            )
            if side not in {"buy", "sell"}:
                continue
            occurred_at = _pick_text(item, "traded_time", "trade_time", "business_time", "成交时间", default="")
            occurred_at_value = _json_scalar(occurred_at) if occurred_at else None
            occurred_day = pd.to_datetime(occurred_at, errors="coerce")
            if pd.notna(target_trade_date) and pd.notna(occurred_day) and occurred_day.date() != target_trade_date.date():
                continue
            qty = int(round(_pick_number(item, "traded_volume", "fill_qty", "volume", "business_amount", "qty")))
            if qty <= 0:
                continue
            price = _pick_number(item, "traded_price", "fill_price", "price", "business_price")
            amount = _pick_number(item, "traded_amount", "amount", default=price * qty if price > 0 else 0.0)
            row = {
                "symbol": _pick_text(item, "stock_code", "symbol", "ticker", "instrument_id"),
                "instrument_name": _pick_text(item, "stock_name", "instrument_name", "name", default="--"),
                "qty": qty,
                "price": price or None,
                "amount": amount or None,
                "commission": _pick_number(item, "commission", "fee", default=0.0) or None,
                "status": "filled",
                "broker_order_id": _pick_text(item, "order_id", "order_sysid", "entrust_no", default="--"),
                "executed_at": occurred_at_value,
            }
            key = (row["symbol"], side, str(row["broker_order_id"]))
            existing_keys.add(key)
            if side == "buy":
                actual_buys.append(row)
            else:
                actual_sells.append(row)
        used_trades = bool(actual_buys or actual_sells)

    orders = snapshot.get("orders", []) or []
    if orders:
        appended_order_count = 0
        for item in orders:
            side = _normalize_side(
                _pick_text(item, "side", "order_side", "direction", "entrust_bs", "bs_flag", "operation")
                or item.get("order_type")
                or item.get("m_nOrderType")
            )
            if side not in {"buy", "sell"}:
                continue
            occurred_at = _pick_text(item, "order_time", "created_at", "entrust_time", default="")
            occurred_at_value = _json_scalar(occurred_at) if occurred_at else None
            occurred_day = pd.to_datetime(occurred_at, errors="coerce")
            if pd.notna(target_trade_date) and pd.notna(occurred_day) and occurred_day.date() != target_trade_date.date():
                continue
            qty = int(round(_pick_number(item, "traded_volume", "filled_qty", "volume", "order_volume", "qty")))
            if qty <= 0:
                qty = int(round(_pick_number(item, "order_volume", "qty", "volume")))
            if qty <= 0:
                continue
            price = _pick_number(item, "traded_price", "avg_price", "price", "order_price")
            amount = _pick_number(item, "traded_amount", "amount", default=price * qty if price > 0 else 0.0)
            row = {
                "symbol": _pick_text(item, "stock_code", "symbol", "ticker", "instrument_id"),
                "instrument_name": _pick_text(item, "stock_name", "instrument_name", "name", default="--"),
                "qty": qty,
                "price": price or None,
                "amount": amount or None,
                "status": _pick_text(item, "status_msg", "status", "order_status", default="--"),
                "broker_order_id": _pick_text(item, "order_id", "order_sysid", "entrust_no", default="--"),
                "executed_at": occurred_at_value,
            }
            key = (row["symbol"], side, str(row["broker_order_id"]))
            if key in existing_keys:
                continue
            if side == "buy":
                actual_buys.append(row)
            else:
                actual_sells.append(row)
            existing_keys.add(key)
            appended_order_count += 1
        used_orders = appended_order_count > 0

    if used_trades and used_orders:
        return actual_buys, actual_sells, "QMT 成交/委托回报"
    if used_trades:
        return actual_buys, actual_sells, "QMT 成交回报"
    if used_orders:
        return actual_buys, actual_sells, "QMT 委托回报"

    return actual_buys, actual_sells, "暂无执行记录"


def _build_actual_trade_rows(
    trades: pd.DataFrame,
    orders: pd.DataFrame,
    trade_date: str | None,
    plan_created_at: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    actual_buys: list[dict[str, Any]] = []
    actual_sells: list[dict[str, Any]] = []
    plan_day = pd.to_datetime(plan_created_at, errors="coerce")
    target_trade_date = pd.to_datetime(trade_date, errors="coerce")

    if not trades.empty:
        working = trades.copy()
        if "trade_time" in working.columns:
            working["trade_time"] = pd.to_datetime(working["trade_time"], errors="coerce")
            if pd.notna(plan_day):
                working = working[working["trade_time"].dt.date == plan_day.date()]
            elif pd.notna(target_trade_date):
                working = working[working["trade_time"].dt.date == target_trade_date.date()]
        for record in working.to_dict("records"):
            qty = int(_safe_float(record.get("fill_qty")) or 0)
            price = _safe_float(record.get("fill_price"))
            amount = (price * qty) if price is not None and qty > 0 else None
            row = {
                "symbol": _normalize_symbol_text(record.get("symbol")),
                "instrument_name": record.get("instrument_name") or "--",
                "qty": qty,
                "price": price,
                "amount": amount,
                "commission": _safe_float(record.get("commission")),
                "status": "filled",
                "executed_at": _json_scalar(record.get("trade_time")),
            }
            if str(record.get("side") or "").strip().lower() == "buy":
                actual_buys.append(row)
            else:
                actual_sells.append(row)
        return actual_buys, actual_sells, "成交回报"

    if orders.empty:
        return actual_buys, actual_sells, "暂无执行记录"

    working = orders.copy()
    if "created_at" in working.columns:
        working["created_at"] = pd.to_datetime(working["created_at"], errors="coerce")
        if pd.notna(plan_day):
            working = working[working["created_at"].dt.date == plan_day.date()]
        elif pd.notna(target_trade_date):
            working = working[working["created_at"].dt.date == target_trade_date.date()]
    for record in working.to_dict("records"):
        qty = int(_safe_float(record.get("filled_qty")) or _safe_float(record.get("qty")) or 0)
        price = _safe_float(record.get("avg_price"))
        amount = (price * qty) if price is not None and qty > 0 else None
        row = {
            "symbol": _normalize_symbol_text(record.get("symbol")),
            "instrument_name": record.get("instrument_name") or "--",
            "qty": qty,
            "price": price,
            "amount": amount,
            "status": record.get("status") or "--",
            "broker_order_id": record.get("broker_order_id") or "--",
            "executed_at": _json_scalar(record.get("created_at")),
        }
        if str(record.get("side") or "").strip().lower() == "buy":
            actual_buys.append(row)
        else:
            actual_sells.append(row)
    return actual_buys, actual_sells, "委托记录"


def _build_qmt_realtime_positions(
    settings: AppSettings,
    probe: dict[str, Any],
    fallback_positions: pd.DataFrame,
) -> tuple[list[dict[str, Any]], dict[str, Any], str | None]:
    snapshot = probe.get("account") if isinstance(probe, dict) else None
    error = probe.get("error") if isinstance(probe, dict) else None
    if isinstance(snapshot, dict):
        asset = snapshot.get("asset") or {}
        rows: list[dict[str, Any]] = []
        for item in snapshot.get("positions", []) or []:
            symbol = _pick_text(item, "stock_code", "symbol", "m_strInstrumentID", "instrument_id", "ticker")
            qty = int(round(_pick_number(item, "volume", "qty", "current_amount", "total_qty", "m_nVolume")))
            if not symbol or qty <= 0:
                continue
            available_qty = int(round(_pick_number(item, "can_use_volume", "available_qty", "m_nCanUseVolume", "enable_amount", default=qty)))
            cost_price = _pick_number(item, "open_price", "cost_price", "avg_price", "m_dOpenPrice")
            market_price = _pick_number(item, "market_price", "last_price", "price", "m_dLastPrice")
            market_value = market_price * qty if market_price > 0 and qty > 0 else None
            unrealized_pnl = (market_price - cost_price) * qty if market_price > 0 and cost_price > 0 else None
            rows.append(
                {
                    "symbol": symbol,
                    "instrument_name": _pick_text(item, "stock_name", "instrument_name", "instrument_name_cn", "name", "m_strInstrumentName", default="--"),
                    "qty": qty,
                    "available_qty": max(0, available_qty),
                    "cost_price": cost_price or None,
                    "market_price": market_price or None,
                    "market_value": market_value,
                    "unrealized_pnl": unrealized_pnl,
                }
            )
        rows.sort(key=lambda item: float(item.get("market_value") or 0.0), reverse=True)
        asset_payload = {
            "account_id": str(snapshot.get("account_id") or asset.get("account_id") or settings.qmt_account_id or "--"),
            "cash": _pick_number(asset, "cash", "m_dCash"),
            "frozen_cash": _pick_number(asset, "frozen_cash", "m_dFrozenCash"),
            "total_asset": _pick_number(asset, "total_asset", "m_dTotalAsset"),
        }
        asset_payload["market_value"] = max(float(asset_payload["total_asset"] or 0.0) - float(asset_payload["cash"] or 0.0), 0.0)
        return rows, asset_payload, error

    if fallback_positions.empty:
        return [], {}, error

    working = fallback_positions.copy()
    rows = []
    for record in working.to_dict("records"):
        rows.append(
            {
                "symbol": _normalize_symbol_text(record.get("symbol")),
                "instrument_name": record.get("instrument_name") or "--",
                "qty": int(_safe_float(record.get("qty")) or 0),
                "available_qty": int(_safe_float(record.get("available_qty")) or 0),
                "cost_price": _safe_float(record.get("cost_price")),
                "market_price": _safe_float(record.get("market_price")),
                "market_value": _safe_float(record.get("market_value")),
                "unrealized_pnl": _safe_float(record.get("unrealized_pnl")),
            }
        )
    return rows, {}, error


def build_qmt_trade_board(
    settings: AppSettings,
    data: DashboardData,
    probe: dict[str, Any] | None,
    info: dict[str, Any],
) -> dict[str, Any]:
    environment_value = getattr(settings.environment, "value", str(settings.environment))
    if environment_value == Environment.BACKTEST.value:
        return {
            "available": False,
            "mode": environment_value,
            "message": "当前是回测模式，QMT 交易看板未启用。",
        }

    latest_plan_record: dict[str, Any] | None = None
    latest_plan_payload: dict[str, Any] = {}
    if not data.audit.empty and "object_type" in data.audit.columns:
        plan_rows = data.audit[data.audit["object_type"].astype(str) == "microcap_trade_plan"].copy()
        if not plan_rows.empty:
            if "created_at" in plan_rows.columns:
                plan_rows["created_at"] = pd.to_datetime(plan_rows["created_at"], errors="coerce")
                plan_rows = plan_rows.sort_values("created_at", ascending=False, na_position="last")
            latest_plan_record = plan_rows.iloc[0].to_dict()
            latest_plan_payload = _parse_json_payload(latest_plan_record.get("payload"))

    trade_date = str(latest_plan_payload.get("trade_date") or "") or None
    next_trade_date = _estimate_next_trading_day(trade_date) if trade_date else None
    planned_buys = _build_qmt_plan_rows(latest_plan_payload, "buy")
    planned_sells = _build_qmt_plan_rows(latest_plan_payload, "sell")
    probe_payload = probe or {}
    snapshot = probe_payload.get("account") if isinstance(probe_payload, dict) else {}
    actual_buys, actual_sells, actual_source = _build_actual_trade_rows_from_qmt_snapshot(snapshot or {}, trade_date)
    if not actual_buys and not actual_sells:
        actual_buys, actual_sells, actual_source = _build_actual_trade_rows(
            data.trades,
            data.orders,
            trade_date,
            latest_plan_record.get("created_at") if latest_plan_record else None,
        )
    realtime_positions, realtime_asset, realtime_error = _build_qmt_realtime_positions(settings, probe_payload, data.positions)

    cash_value = _safe_float(realtime_asset.get("cash"))
    total_asset = _safe_float(realtime_asset.get("total_asset")) or _safe_float(info.get("total_asset"))
    market_value = _safe_float(realtime_asset.get("market_value"))
    if market_value is None and total_asset is not None and cash_value is not None:
        market_value = total_asset - cash_value

    return {
        "available": bool(latest_plan_record or realtime_positions or actual_buys or actual_sells or (probe and not probe.get("error"))),
        "mode": environment_value,
        "trade_date": trade_date,
        "next_trade_date": next_trade_date,
        "plan_generated_at": _json_scalar(latest_plan_record.get("created_at")) if latest_plan_record else None,
        "plan_status": "QMT 已启用" if latest_plan_payload.get("qmt_trade_enabled") else "仅生成计划/预演",
        "actual_source": actual_source,
        "planned_buys": planned_buys,
        "planned_sells": planned_sells,
        "actual_buys": actual_buys,
        "actual_sells": actual_sells,
        "positions": realtime_positions,
        "realtime_asset": {
            "total_asset": total_asset,
            "cash": cash_value if cash_value is not None else _safe_float(info.get("cash")),
            "market_value": market_value if market_value is not None else _safe_float(info.get("market_value")),
            "account_id": realtime_asset.get("account_id") or "--",
        },
        "summary": {
            "planned_buy_count": len(planned_buys),
            "planned_sell_count": len(planned_sells),
            "actual_buy_count": len(actual_buys),
            "actual_sell_count": len(actual_sells),
            "position_count": len(realtime_positions),
            "target_count": int(_safe_float(latest_plan_payload.get("target_count")) or 0),
            "ranked_count": int(_safe_float(latest_plan_payload.get("ranked_count")) or 0),
            "hedge_ratio": _safe_float(latest_plan_payload.get("hedge_ratio")),
            "cash_reserve_ratio": _safe_float(latest_plan_payload.get("cash_reserve_ratio")),
        },
        "message": "展示 T 日生成的 T+1 计划、当日实际执行以及当前 QMT 持仓。",
        "realtime_error": realtime_error,
    }


def overview(data: DashboardData) -> dict[str, Any]:
    latest = data.assets.iloc[-1] if not data.assets.empty else None
    total_asset = float(latest["total_asset"]) if latest is not None else None
    cash = float(latest["cash"]) if latest is not None else None
    turnover = float(latest["turnover"]) if latest is not None else None
    drawdown = float(pd.to_numeric(data.assets["max_drawdown"], errors="coerce").min()) if not data.assets.empty and "max_drawdown" in data.assets.columns else (float(latest["max_drawdown"]) if latest is not None and "max_drawdown" in latest.index else None)
    latest_market_value = _safe_float(latest["market_value"]) if latest is not None and "market_value" in latest.index else None
    latest_total_pnl = _safe_float(latest["total_pnl"]) if latest is not None and "total_pnl" in latest.index else None
    market_value = float(data.positions["market_value"].sum()) if not data.positions.empty else float(latest_market_value or 0.0)
    unrealized_pnl = float(data.positions["unrealized_pnl"].sum()) if not data.positions.empty else float(latest_total_pnl or 0.0)
    total_return = None
    if not data.assets.empty:
        start_asset = float(data.assets.iloc[0]["total_asset"])
        if start_asset:
            total_return = (float(data.assets.iloc[-1]["total_asset"]) - start_asset) / start_asset
    return {
        "total_asset": total_asset,
        "cash": cash,
        "turnover": turnover,
        "drawdown": drawdown,
        "market_value": market_value,
        "unrealized_pnl": unrealized_pnl,
        "exposure": market_value / total_asset if total_asset else None,
        "total_return": total_return,
        "approved": int((data.risk["status"] == "approved").sum()) if not data.risk.empty else 0,
        "rejected": int((data.risk["status"] == "rejected").sum()) if not data.risk.empty else 0,
        "order_count": len(data.orders),
        "trade_count": len(data.trades),
        "latest_time": latest["snapshot_time"] if latest is not None else None,
    }


def connection_state(settings: AppSettings, probe: dict[str, Any] | None, data: DashboardData | None = None) -> dict[str, str]:
    if data is not None and _is_joinquant_microcap_dashboard(data):
        return {
            "label": "聚宽微盘回测视图",
            "color": "#0f766e",
            "detail": "当前页面展示 joinquant_microcap_alpha 的本地回测产物。",
        }
    if settings.environment == Environment.BACKTEST:
        return {"label": "离线数据库视图", "color": "#64748b", "detail": "当前未连接 QMT，只展示历史数据。"}
    if not probe or probe.get("error"):
        detail = probe.get("error") if probe else "实盘探测失败，无法读取账户与行情。"
        prefix = "QMT 仿真" if settings.environment == Environment.PAPER else "QMT 实盘"
        return {"label": f"{prefix}未连接", "color": "#b91c1c", "detail": detail}
    statuses = (probe.get("health") or {}).get("account_status") or []
    status_code = statuses[0].get("status") if statuses else None
    if str(status_code) == "0":
        label = "QMT 仿真在线" if settings.environment == Environment.PAPER else "QMT 在线"
        return {"label": label, "color": "#0f766e", "detail": "账户状态正常，行情与账户查询可用。"}
    prefix = "QMT 仿真" if settings.environment == Environment.PAPER else "QMT"
    return {"label": f"{prefix}状态 {status_code}", "color": "#d97706", "detail": "客户端已连接，但账户状态不是正常。"}


def alerts(settings: AppSettings, info: dict[str, Any], data: DashboardData, probe: dict[str, Any] | None) -> list[dict[str, str]]:
    items = []
    conn = connection_state(settings, probe, data)
    items.append({"severity": "info" if conn["color"] == "#0f766e" else "warning", "title": conn["label"], "detail": conn["detail"]})
    if info["rejected"] > 0:
        items.append({"severity": "warning", "title": "存在风控拒绝", "detail": f"累计 {info['rejected']} 条风控拒绝，建议检查风控规则。"})
    if info["drawdown"] is not None and info["drawdown"] <= settings.risk.daily_loss_limit:
        items.append({"severity": "critical", "title": "回撤触及日损阈值", "detail": f"当前最大回撤 {info['drawdown'] * 100:.2f}%。"})
    if settings.environment == Environment.LIVE and not settings.qmt_trade_enabled:
        items.append({"severity": "info", "title": "实盘委托已锁定", "detail": "当前只允许探测账户和行情，不允许自动下单。"})
    return items


def load_runtime_logs() -> dict[str, str]:
    qmt_log = None
    for folder in [
        ROOT / "runtime" / "qmt_client" / "installed" / "userdata_mini" / "log",
        ROOT / "runtime" / "qmt_client" / "installed" / "userdata" / "log",
    ]:
        if folder.exists():
            files = [item for item in folder.glob("*.log") if item.is_file()]
            if files:
                qmt_log = max(files, key=lambda item: item.stat().st_mtime)
    return {
        "QMT 客户端": _tail(qmt_log),
        "系统操作": _tail(ROOT / ".codex" / "operations-log.md", 120),
        "测试记录": _tail(ROOT / ".codex" / "testing.md", 120),
        "UI 日志": _tail(DEMO_LOG_DIR / "ui.stdout.log"),
        "API 日志": _tail(DEMO_LOG_DIR / "api.stdout.log"),
    }


def build_strategy_defaults(default_name: str) -> dict[str, Any]:
    label = next((name for name, path in STRATEGIES.items() if path.stem == default_name), "ETF 轮动")
    defaults = _read_yaml(STRATEGIES[label])
    return {
        "label": label,
        "name": defaults.get("name", STRATEGIES[label].stem),
        "implementation": defaults.get("implementation", STRATEGIES[label].stem),
        "rebalance_frequency": defaults.get("rebalance_frequency", "weekly"),
        "lookback_days": int(defaults.get("lookback_days", 20)),
        "top_n": int(defaults.get("top_n", 2)),
        "lot_size": int(defaults.get("lot_size", 100)),
    }


def build_qlib_runtime_payload(profile: str = "backtest", overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    _, _, settings = resolve_settings(profile)
    payload = settings.model_dump(mode="json")
    payload["backtest_engine"] = "qlib"
    payload["history_source"] = "qmt"
    payload["symbols"] = []
    merged = dict(payload)
    for key, value in (overrides or {}).items():
        if value is not None:
            merged[key] = value
    return merged


def build_pattern_defaults() -> dict[str, Any]:
    return {
        "selection_label": "四策略对比",
        "start_date": "2023-01-01",
        "end_date": date.today().isoformat(),
        "account": 500000,
        "max_holdings": 10,
        "risk_degree": 0.95,
        "max_holding_days": 15,
    }


def _resolve_result_path(raw_path: str | None, base_dir: Path) -> str | None:
    if not raw_path:
        return None
    path = Path(raw_path)
    resolved = path if path.is_absolute() else (base_dir / path).resolve()
    try:
        return str(resolved.relative_to(ROOT.resolve())).replace("\\", "/")
    except ValueError:
        return str(resolved)


def resolve_pattern_report_dir(raw_path: str | Path | None = None, *, require_summary: bool = True) -> Path:
    if raw_path is None or str(raw_path).strip() == "":
        candidate = USER_PATTERN_REPORT_DIR.resolve()
    else:
        path = Path(str(raw_path).strip())
        if path.is_absolute():
            candidate = path.resolve()
        elif len(path.parts) == 1:
            candidate = (REPORTS_ROOT / path).resolve()
        else:
            candidate = (ROOT / path).resolve()
    root_resolved = ROOT.resolve()
    if candidate != root_resolved and root_resolved not in candidate.parents:
        raise ValueError(f"Pattern report dir is outside workspace: {candidate}")
    if require_summary and not (candidate / "summary.json").exists():
        raise FileNotFoundError(f"Pattern report summary.json not found: {candidate}")
    return candidate


def list_pattern_report_dirs() -> list[dict[str, Any]]:
    candidates: list[Path] = []
    if USER_PATTERN_REPORT_DIR.exists():
        candidates.append(USER_PATTERN_REPORT_DIR.resolve())
    if REPORTS_ROOT.exists():
        for item in REPORTS_ROOT.iterdir():
            if item.is_dir():
                candidates.append(item.resolve())
    seen: set[Path] = set()
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        summary_path = candidate / "summary.json"
        if not summary_path.exists():
            continue
        try:
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        frame = pd.DataFrame(payload if isinstance(payload, list) else [payload])
        if frame.empty or "mode" not in frame.columns:
            continue
        rows.append(
            {
                "label": candidate.name,
                "value": workspace_relative(candidate),
                "summary_count": int(len(frame)),
                "updated_at": datetime.fromtimestamp(candidate.stat().st_mtime).isoformat(),
            }
        )
    rows.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
    return rows


def list_common_strategies_with_results(database_url: str) -> list[dict[str, Any]]:
    try:
        factory = create_session_factory(database_url)
        with session_scope(factory) as session:
            strategies = session.scalars(select(CommonStrategyModel).order_by(CommonStrategyModel.created_at.asc())).all()
            result_rows = session.scalars(
                select(StrategyBacktestResultModel).order_by(StrategyBacktestResultModel.created_at.desc())
            ).all()
    except Exception:
        return []
    grouped: dict[str, list[StrategyBacktestResultModel]] = {}
    for row in result_rows:
        grouped.setdefault(row.strategy_id, []).append(row)
    payload: list[dict[str, Any]] = []
    for strategy in strategies:
        rows = grouped.get(strategy.strategy_id, [])
        payload.append(
            {
                "strategy_id": strategy.strategy_id,
                "strategy_key": strategy.strategy_key,
                "display_name": strategy.display_name,
                "is_active": bool(strategy.is_active),
                "results": [
                    {
                        "backtest_result_id": item.backtest_result_id,
                        "run_key": item.run_key,
                        "mode": item.mode,
                        "start_date": item.start_date,
                        "end_date": item.end_date,
                        "account": _safe_float(item.account),
                        "total_return": _safe_float(item.total_return),
                        "annualized_return": _safe_float(item.annualized_return),
                        "max_drawdown": _safe_float(item.max_drawdown),
                        "ending_equity": _safe_float(item.ending_equity),
                        "created_at": item.created_at.isoformat(),
                    }
                    for item in rows
                ],
            }
        )
    return payload


def persist_pattern_backtest_results(database_url: str, summary_rows: list[dict[str, Any]], base_dir: Path) -> None:
    if not summary_rows:
        return
    try:
        factory = create_session_factory(database_url)
        with session_scope(factory) as session:
            run_key = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            strategy_cache: dict[str, CommonStrategyModel] = {}
            for row in summary_rows:
                mode = str(row.get("mode") or "B1")
                strategy_key, display_name = PATTERN_STRATEGY_META.get(mode, (mode.lower(), f"形态策略 {mode}"))
                strategy = strategy_cache.get(strategy_key)
                if strategy is None:
                    strategy = session.scalar(select(CommonStrategyModel).where(CommonStrategyModel.strategy_key == strategy_key))
                    if strategy is None:
                        strategy = CommonStrategyModel(strategy_key=strategy_key, display_name=display_name, is_active=1)
                        session.add(strategy)
                        session.flush()
                    strategy_cache[strategy_key] = strategy
                strategy.updated_at = datetime.utcnow()
                session.add(
                    StrategyBacktestResultModel(
                        strategy_id=strategy.strategy_id,
                        run_key=f"{run_key}-{mode}",
                        mode=mode,
                        start_date=str(row.get("start_date") or ""),
                        end_date=str(row.get("end_date") or ""),
                        account=_to_decimal(row.get("account") or 0) or Decimal("0"),
                        total_return=_to_decimal(row.get("total_return")),
                        annualized_return=_to_decimal(row.get("annualized_return")),
                        max_drawdown=_to_decimal(row.get("max_drawdown")),
                        ending_equity=_to_decimal(row.get("ending_equity")),
                        report_path=_resolve_result_path(row.get("report_path"), base_dir),
                        risk_path=_resolve_result_path(row.get("risk_path"), base_dir),
                        daily_action_path=_resolve_result_path(row.get("daily_action_path"), base_dir),
                        daily_decision_path=_resolve_result_path(row.get("daily_decision_path"), base_dir),
                        raw_payload=row,
                    )
                )
    except Exception:
        return


def delete_backtest_result(database_url: str, backtest_result_id: str) -> int:
    try:
        factory = create_session_factory(database_url)
        with session_scope(factory) as session:
            statement = delete(StrategyBacktestResultModel).where(
                StrategyBacktestResultModel.backtest_result_id == backtest_result_id
            )
            result = session.execute(statement)
            return int(result.rowcount or 0)
    except Exception:
        return 0


def build_b1_score_card(
    symbol: str,
    target_date: str,
    score_file: Path | None = None,
    lookback_days: int = 20,
) -> dict[str, Any]:
    resolved_score_file = score_file or (ROOT / "data" / "reports" / "b1_rank" / "b1_model_scores.parquet")
    if not resolved_score_file.exists():
        csv_fallback = resolved_score_file.with_suffix(".csv")
        if not csv_fallback.exists():
            return {"symbol": symbol, "target_date": target_date, "error": "b1_score_file_not_found"}
        resolved_score_file = csv_fallback
    if resolved_score_file.suffix.lower() == ".csv":
        frame = pd.read_csv(resolved_score_file)
    else:
        frame = pd.read_parquet(resolved_score_file)
    if frame.empty:
        return {"symbol": symbol, "target_date": target_date, "error": "b1_score_frame_empty"}
    if "date" not in frame.columns or "symbol" not in frame.columns:
        return {"symbol": symbol, "target_date": target_date, "error": "b1_score_columns_missing"}
    score_column = "model_score" if "model_score" in frame.columns else ("score" if "score" in frame.columns else "")
    if not score_column:
        return {"symbol": symbol, "target_date": target_date, "error": "b1_score_column_missing"}
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame[frame["symbol"].astype(str) == symbol].dropna(subset=["date"])
    if frame.empty:
        return {"symbol": symbol, "target_date": target_date, "error": "b1_symbol_not_found"}
    target = pd.to_datetime(target_date, errors="coerce")
    if pd.isna(target):
        return {"symbol": symbol, "target_date": target_date, "error": "b1_invalid_date"}
    frame = frame[frame["date"] <= target].sort_values("date")
    if frame.empty:
        return {"symbol": symbol, "target_date": target_date, "error": "b1_date_out_of_range"}
    latest_row = frame.iloc[-1]
    start = latest_row["date"] - pd.Timedelta(days=max(lookback_days, 1) - 1)
    window = frame[frame["date"] >= start]
    score_value = _safe_float(latest_row.get(score_column))
    rank_percentile = None
    daily_slice = pd.read_csv(resolved_score_file) if resolved_score_file.suffix.lower() == ".csv" else pd.read_parquet(resolved_score_file)
    daily_slice["date"] = pd.to_datetime(daily_slice.get("date"), errors="coerce")
    daily_slice = daily_slice[daily_slice["date"] == latest_row["date"]]
    if not daily_slice.empty and score_column in daily_slice.columns and score_value is not None:
        ordered = pd.to_numeric(daily_slice[score_column], errors="coerce").dropna().sort_values()
        if not ordered.empty:
            rank_percentile = float((ordered <= score_value).sum() / len(ordered) * 100.0)
    return {
        "symbol": symbol,
        "target_date": target_date,
        "resolved_date": latest_row["date"].date().isoformat(),
        "score": score_value,
        "percentile": rank_percentile,
        "series": [
            {"date": item["date"].date().isoformat(), "score": _safe_float(item.get(score_column))}
            for item in window.to_dict("records")
        ],
    }




def _resolve_report_file(base: Path, raw_path: str | Path | None) -> Path | None:
    if not raw_path:
        return None
    candidate = Path(raw_path)
    if candidate.is_absolute():
        return candidate
    root_candidate = (ROOT / candidate).resolve()
    if root_candidate.exists():
        return root_candidate
    return (base / candidate.name).resolve()


def _load_pattern_frame(
    summary: pd.DataFrame,
    *,
    base: Path,
    path_column: str,
    date_columns: list[str] | None = None,
    sort_columns: list[tuple[str, bool]] | None = None,
) -> pd.DataFrame:
    if summary.empty or path_column not in summary.columns:
        return pd.DataFrame()
    frames: list[pd.DataFrame] = []
    for record in summary.to_dict("records"):
        report_path = _resolve_report_file(base, record.get(path_column))
        if report_path is None or not report_path.exists() or not report_path.is_file():
            continue
        try:
            frame = pd.read_csv(report_path)
        except Exception:
            continue
        for column in date_columns or []:
            if column in frame.columns:
                frame[column] = pd.to_datetime(frame[column], errors="coerce")
        if "mode" in record and "策略(B1 B2 B3)" not in frame.columns:
            frame["策略(B1 B2 B3)"] = record["mode"]
        frame["数据来源"] = report_path.name
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    merged = pd.concat(frames, ignore_index=True)
    normalized_sort_columns = sort_columns or [
        ("SELL日期", False),
        ("日期", False),
        ("trading_date", False),
        ("策略(B1 B2 B3)", True),
        ("股票代码", True),
        ("标的", True),
    ]
    selected_columns = [column for column, _is_ascending in normalized_sort_columns if column in merged.columns]
    if selected_columns:
        ascending = [is_ascending for column, is_ascending in normalized_sort_columns if column in merged.columns]
        merged = merged.sort_values(selected_columns, ascending=ascending, na_position="last").reset_index(drop=True)
    return merged


def load_user_pattern_results(report_dir: str | Path = USER_PATTERN_REPORT_DIR) -> dict[str, Any]:
    base = resolve_pattern_report_dir(report_dir, require_summary=False)
    summary_path = base / "summary.json"
    comparison_path = base / "equity_comparison.csv"
    summary = pd.DataFrame()
    comparison = pd.DataFrame()
    daily_actions = pd.DataFrame()
    daily_decisions = pd.DataFrame()
    if summary_path.exists():
        summary = pd.DataFrame(json.loads(summary_path.read_text(encoding="utf-8")))
    if comparison_path.exists():
        comparison = pd.read_csv(comparison_path)
        if not comparison.empty and "datetime" in comparison.columns:
            comparison["datetime"] = pd.to_datetime(comparison["datetime"], errors="coerce")
    if not summary.empty:
        daily_actions = _load_pattern_frame(
            summary,
            base=base,
            path_column="daily_action_path",
            date_columns=["日期", "买入信号日期", "SELL日期"],
            sort_columns=[
                ("日期", True),
                ("买入信号日期", True),
                ("策略(B1 B2 B3)", True),
                ("股票代码", True),
                ("SELL日期", True),
            ],
        )
        daily_decisions = _load_pattern_frame(
            summary,
            base=base,
            path_column="daily_decision_path",
            date_columns=["trading_date"],
        )
    return {
        "summary": summary,
        "comparison": comparison,
        "daily_actions": daily_actions,
        "daily_decisions": daily_decisions,
        "png_path": str(base / "equity_comparison.png"),
        "html_path": str(base / "equity_comparison.html"),
        "base_dir": str(base),
    }


def _select_primary_pattern_record(summary: pd.DataFrame) -> dict[str, Any] | None:
    if summary.empty:
        return None
    if "mode" in summary.columns:
        preferred = summary[summary["mode"].astype(str) == "B1"]
        if not preferred.empty:
            return preferred.iloc[0].to_dict()
        candidate = summary[summary["mode"].notna()]
        if not candidate.empty:
            return candidate.iloc[0].to_dict()
    return summary.iloc[0].to_dict()


def _load_pattern_csv(base: Path, raw_path: str | Path | None, *, date_columns: list[str] | None = None) -> pd.DataFrame:
    report_path = _resolve_report_file(base, raw_path)
    if report_path is None or not report_path.exists() or not report_path.is_file():
        return pd.DataFrame()
    try:
        frame = pd.read_csv(report_path)
    except Exception:
        return pd.DataFrame()
    for column in date_columns or []:
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce")
    return frame


def _filter_pattern_frame(frame: pd.DataFrame, column: str, allowed_values: set[str]) -> pd.DataFrame:
    if frame.empty or column not in frame.columns or not allowed_values:
        return frame
    normalized = {str(item) for item in allowed_values if item not in (None, "")}
    if not normalized:
        return frame
    filtered = frame[frame[column].astype(str).isin(normalized)].copy()
    return filtered.reset_index(drop=True)


def _build_pattern_risk_frame(risk_frame: pd.DataFrame, mode: str | None = None) -> pd.DataFrame:
    if risk_frame.empty:
        return pd.DataFrame(columns=["metric", "value", "mode"])
    first_column = str(risk_frame.columns[0])
    value_column = str(risk_frame.columns[-1])
    metrics = risk_frame.loc[:, [first_column, value_column]].copy()
    metrics = metrics.rename(columns={first_column: "metric", value_column: "value"})
    metrics["mode"] = str(mode or "")
    return metrics


def _build_pattern_benchmark_curve(report_frame: pd.DataFrame) -> pd.DataFrame:
    if report_frame.empty or "datetime" not in report_frame.columns or "bench" not in report_frame.columns:
        return pd.DataFrame(columns=["trading_date", "benchmark_equity"])
    working = report_frame.loc[:, ["datetime", "account", "bench"]].copy()
    working["datetime"] = pd.to_datetime(working["datetime"], errors="coerce")
    working["bench"] = pd.to_numeric(working["bench"], errors="coerce").fillna(0.0)
    if working.empty:
        return pd.DataFrame(columns=["trading_date", "benchmark_equity"])
    start_equity = float(pd.to_numeric(working["account"], errors="coerce").dropna().iloc[0]) if working["account"].notna().any() else 1.0
    benchmark_equity = start_equity * (1.0 + working["bench"]).cumprod()
    return pd.DataFrame({"trading_date": working["datetime"], "benchmark_equity": benchmark_equity.astype(float)})


def _build_pattern_overview(record: dict[str, Any], report_frame: pd.DataFrame, daily_actions: pd.DataFrame) -> dict[str, Any]:
    latest = report_frame.iloc[-1] if not report_frame.empty else None
    total_asset = float(latest["account"]) if latest is not None and pd.notna(latest.get("account")) else _safe_float(record.get("ending_equity"))
    cash = float(latest["cash"]) if latest is not None and pd.notna(latest.get("cash")) else None
    market_value = float(latest["value"]) if latest is not None and pd.notna(latest.get("value")) else None
    turnover = float(latest["total_turnover"]) if latest is not None and pd.notna(latest.get("total_turnover")) else None
    latest_time = latest.get("datetime") if latest is not None else record.get("end_date")
    trade_count = int(len(daily_actions))
    return {
        "total_asset": total_asset,
        "cash": cash,
        "turnover": turnover,
        "drawdown": _safe_float(record.get("max_drawdown")),
        "market_value": market_value,
        "unrealized_pnl": None,
        "exposure": (market_value / total_asset) if total_asset and market_value is not None else None,
        "total_return": _safe_float(record.get("total_return")),
        "approved": 0,
        "rejected": 0,
        "order_count": trade_count,
        "trade_count": trade_count,
        "latest_time": latest_time,
    }


def _build_pattern_report_text(record: dict[str, Any], risk_metrics: pd.DataFrame, daily_actions: pd.DataFrame) -> str:
    ratio_keys = {"mean", "std", "annualized_return", "information_ratio", "max_drawdown"}
    lines = [
        "# 形态策略回测报告",
        "",
        f"策略模式：{record.get('mode', '')}",
        f"回测区间：{record.get('start_date', '')} ~ {record.get('end_date', '')}",
        f"总收益率：{_safe_float(record.get('total_return')):.2%}" if pd.notna(_safe_float(record.get('total_return'))) else "总收益率：--",
        f"年化收益：{_safe_float(record.get('annualized_return')):.2%}" if pd.notna(_safe_float(record.get('annualized_return'))) else "年化收益：--",
        f"最大回撤：{_safe_float(record.get('max_drawdown')):.2%}" if pd.notna(_safe_float(record.get('max_drawdown'))) else "最大回撤：--",
        f"期末权益：{_safe_float(record.get('ending_equity')):,.2f}" if pd.notna(_safe_float(record.get('ending_equity'))) else "期末权益：--",
        f"交易笔数：{int(len(daily_actions))}",
    ]
    if not risk_metrics.empty:
        lines.append("")
        lines.append("## 风险指标")
        for item in risk_metrics.to_dict("records"):
            metric = str(item.get("metric", ""))
            value = item.get("value")
            if metric in ratio_keys:
                try:
                    value_text = f"{float(value):.2%}"
                except (TypeError, ValueError):
                    value_text = str(value)
            else:
                value_text = str(value)
            lines.append(f"- {metric}: {value_text}")
    return "\n".join(lines)


def _build_pattern_connection(record: dict[str, Any]) -> dict[str, str]:
    mode = str(record.get("mode", "策略"))
    start_date = str(record.get("start_date", ""))
    end_date = str(record.get("end_date", ""))
    return {
        "label": f"{mode} 回测视图",
        "color": "#0f766e",
        "detail": f"当前页面仅展示 {mode} 策略回测产物，区间 {start_date} 至 {end_date}。",
    }


def _build_pattern_alerts(settings: AppSettings, info: dict[str, Any], record: dict[str, Any]) -> list[dict[str, str]]:
    mode = str(record.get("mode", "策略"))
    items = [
        {
            "severity": "info",
            "title": f"{mode} 策略专属视图",
            "detail": "概览、交易明细、曲线与报告均使用这次形态策略回测产物。",
        }
    ]
    drawdown = _safe_float(info.get("drawdown"))
    if pd.notna(drawdown) and drawdown <= float(settings.risk.daily_loss_limit):
        items.append({
            "severity": "critical",
            "title": "回撤触及日损阈值",
            "detail": f"当前最大回撤 {drawdown * 100:.2f}%。",
        })
    return items


def workspace_relative(path: str | Path) -> str:
    candidate = Path(path)
    resolved = candidate.resolve() if candidate.is_absolute() else (ROOT / candidate).resolve()
    return str(resolved.relative_to(ROOT.resolve())).replace("\\", "/")


def _default_pattern_report_dir_value(pattern_dir_options: list[dict[str, Any]]) -> str:
    preferred_value = f"data/reports/{DEFAULT_PATTERN_REPORT_DIR_NAME}"
    for option in pattern_dir_options:
        if str(option.get("value") or "") == preferred_value:
            return preferred_value
    if not pattern_dir_options:
        return ""
    return str(pattern_dir_options[0].get("value") or "")


def build_dashboard_payload(
    profile: str = "backtest",
    config_path: str | None = None,
    pattern_report_dir: str | None = None,
) -> dict[str, Any]:
    profile_key, resolved_config, settings = resolve_settings(profile, config_path)
    pattern_dir_options = list_pattern_report_dirs()
    default_pattern_dir = pattern_report_dir
    if not default_pattern_dir and pattern_dir_options:
        default_pattern_dir = _default_pattern_report_dir_value(pattern_dir_options)
    try:
        selected_pattern_dir = resolve_pattern_report_dir(default_pattern_dir, require_summary=False)
    except Exception:
        selected_pattern_dir = USER_PATTERN_REPORT_DIR.resolve()
    pattern = load_user_pattern_results(selected_pattern_dir)
    pattern_summary = pattern["summary"].copy()
    pattern_comparison = pattern["comparison"].copy()
    pattern_actions = pattern["daily_actions"].copy()
    pattern_decisions = pattern["daily_decisions"].copy()
    primary_record = _select_primary_pattern_record(pattern_summary)
    data = load_dashboard_data(settings.database_url, settings.report_dir)
    probe = load_live_probe(resolved_config, settings) if settings.environment != Environment.BACKTEST else {}
    strategy_registry = list_common_strategies_with_results(settings.database_url)
    qlib_runtime = build_qlib_runtime_payload(profile_key)
    qlib_status = history_status(AppSettings.model_validate(qlib_runtime))

    info = overview(data)
    connection = connection_state(settings, probe, data)
    alert_items = alerts(settings, info, data, probe)
    display_assets = data.assets
    display_positions = data.positions
    display_orders = data.orders
    display_trades = data.trades
    display_risk = data.risk
    display_audit = data.audit
    display_rules = data.rules
    display_report_text = data.report_text
    display_benchmark_curve = data.benchmark_curve
    qmt_trade_board = build_qmt_trade_board(settings, data, probe, info)

    if primary_record is not None:
        primary_mode = str(primary_record.get("mode") or "B1")
        pattern_summary = _filter_pattern_frame(pattern_summary, "mode", {primary_mode})
        pattern_comparison = _filter_pattern_frame(pattern_comparison, "series", {primary_mode, "Benchmark"})
        pattern_actions = _filter_pattern_frame(pattern_actions, "策略(B1 B2 B3)", {primary_mode})
        pattern_decisions = _filter_pattern_frame(pattern_decisions, "mode", {primary_mode})
        pattern_base = Path(pattern["base_dir"])
        pattern_report = _load_pattern_csv(pattern_base, primary_record.get("report_path"), date_columns=["datetime"])
        pattern_risk_raw = _load_pattern_csv(pattern_base, primary_record.get("risk_path"))
        pattern_risk = _build_pattern_risk_frame(pattern_risk_raw, primary_mode)
        display_risk = pattern_risk
        display_rules = pattern_risk

    return {
        "profile": profile_key,
        "config_path": str(resolved_config),
        "settings": settings.model_dump(mode="json"),
        "strategy_defaults": build_strategy_defaults(settings.default_strategy),
        "overview": _json_object(info),
        "connection": connection,
        "alerts": alert_items,
        "probe": probe,
        "logs": load_runtime_logs(),
        "qlib": {
            "runtime": qlib_runtime,
            "status": _json_object(qlib_status),
        },
        "pattern": {
            "defaults": build_pattern_defaults(),
            "options": USER_PATTERN_OPTIONS,
            "report_dirs": pattern_dir_options,
            "selected_report_dir": workspace_relative(selected_pattern_dir),
            "summary": _frame_records(pattern_summary),
            "comparison": _frame_records(pattern_comparison),
            "daily_actions": _frame_records(pattern_actions),
            "daily_decisions": _frame_records(pattern_decisions),
            "primary_mode": primary_record.get("mode") if primary_record is not None else None,
            "png_url": f"/api/file?path={workspace_relative(pattern['png_path'])}",
            "html_url": f"/api/file?path={workspace_relative(pattern['html_path'])}",
            "base_dir": pattern["base_dir"],
            "strategy_registry": strategy_registry,
        },
        "data": {
            "assets": _frame_records(display_assets),
            "positions": _frame_records(display_positions),
            "orders": _frame_records(display_orders),
            "trades": _frame_records(display_trades),
            "risk": _frame_records(display_risk),
            "audit": _frame_records(display_audit),
            "rules": _frame_records(display_rules),
            "pattern_actions": _frame_records(pattern_actions),
            "pattern_decisions": _frame_records(pattern_decisions),
            "report_text": display_report_text,
            "benchmark_curve": _frame_records(display_benchmark_curve),
            "qmt_trade_board": _jsonify(qmt_trade_board),
        },
        "meta": {
            "profiles": [{"key": key, "label": key, "config_path": str(path)} for key, path in CONFIGS.items()],
            "strategies": [{"label": label, "path": str(path)} for label, path in STRATEGIES.items()],
        },
        "generated_at": datetime.now().isoformat(),
    }


def run_cmd(script_name: str, args: list[str]) -> dict[str, Any]:
    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUNBUFFERED"] = "1"
    cmd = [sys.executable, script_name, *args]
    DEMO_LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = DEMO_LOG_DIR / "api.stdout.log"
    process = subprocess.Popen(
        cmd,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )
    output_lines: list[str] = []
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"\n[{datetime.now().isoformat()}] $ {' '.join(cmd)}\n")
        handle.flush()
        while True:
            raw_line = process.stdout.readline() if process.stdout is not None else ""
            if not raw_line and process.poll() is not None:
                break
            if not raw_line:
                continue
            line = raw_line.rstrip()
            output_lines.append(line)
            handle.write(line + "\n")
            handle.flush()
    returncode = process.wait()
    if process.stdout is not None:
        process.stdout.close()
    return {
        "command": " ".join(cmd),
        "stdout": "\n".join(output_lines).strip(),
        "stderr": "",
        "returncode": returncode,
        "ok": returncode == 0,
    }


def run_strategy_action(payload: dict[str, Any]) -> dict[str, Any]:
    profile = str(payload.get("profile") or "backtest").lower()
    action = str(payload.get("action") or "backtest").lower()
    _, config_path, settings = resolve_settings(profile)
    defaults = build_strategy_defaults(settings.default_strategy)
    strategy_label = str(payload.get("strategy_label") or defaults["label"])
    strategy_file = STRATEGIES.get(strategy_label, STRATEGIES[defaults["label"]])
    strategy_payload = _read_yaml(strategy_file)
    strategy_payload.update(
        {
            "name": payload.get("name", defaults["name"]),
            "implementation": payload.get("implementation", defaults["implementation"]),
            "rebalance_frequency": payload.get("rebalance_frequency", defaults["rebalance_frequency"]),
            "lookback_days": int(payload.get("lookback_days", defaults["lookback_days"])),
            "top_n": int(payload.get("top_n", defaults["top_n"])),
            "lot_size": int(payload.get("lot_size", defaults["lot_size"])),
        }
    )
    UI_RUNTIME.mkdir(parents=True, exist_ok=True)
    strategy_path = UI_RUNTIME / f"strategy_{action}.yaml"
    _write_yaml(strategy_path, strategy_payload)
    if action == "backtest":
        result = run_cmd("scripts/run_backtest.py", ["--config", str(config_path), "--strategy", str(strategy_path)])
    elif action == "paper":
        result = run_cmd("scripts/run_paper.py", ["--config", str(CONFIGS["paper"]), "--strategy", str(strategy_path)])
    elif action == "probe":
        result = run_cmd("scripts/run_live.py", ["--config", str(CONFIGS["live"]), "--strategy", str(strategy_path), "--mode", "probe"])
    else:
        result = run_cmd("scripts/run_live.py", ["--config", str(CONFIGS["live"]), "--strategy", str(strategy_path), "--mode", "strategy"])
    result["action"] = action
    result["strategy_label"] = strategy_label
    return result


def run_qlib_action(payload: dict[str, Any]) -> dict[str, Any]:
    profile = str(payload.get("profile") or "backtest").lower()
    action = str(payload.get("action") or "incremental").lower()
    runtime_overrides = {
        "history_universe_sector": payload.get("history_universe_sector"),
        "history_universe_limit": int(payload["history_universe_limit"]) if payload.get("history_universe_limit") not in (None, "") else None,
        "history_start": payload.get("history_start"),
        "history_adjustment": payload.get("history_adjustment"),
        "history_batch_size": int(payload["history_batch_size"]) if payload.get("history_batch_size") not in (None, "") else None,
        "qlib_n_drop": int(payload["qlib_n_drop"]) if payload.get("qlib_n_drop") not in (None, "") else None,
        "qlib_force_rebuild": bool(payload.get("qlib_force_rebuild")) if payload.get("qlib_force_rebuild") is not None else None,
    }
    runtime_payload = build_qlib_runtime_payload(profile, runtime_overrides)
    UI_RUNTIME.mkdir(parents=True, exist_ok=True)
    config_path = UI_RUNTIME / "qlib_runtime.yaml"
    _write_yaml(config_path, runtime_payload)
    strategy_defaults = build_strategy_defaults(load_app_settings(resolve_profile_path(profile)).default_strategy)
    strategy_payload = {
        "name": payload.get("name", strategy_defaults["name"]),
        "implementation": payload.get("implementation", strategy_defaults["implementation"]),
        "rebalance_frequency": payload.get("rebalance_frequency", strategy_defaults["rebalance_frequency"]),
        "lookback_days": int(payload.get("lookback_days", strategy_defaults["lookback_days"])),
        "top_n": int(payload.get("top_n", strategy_defaults["top_n"])),
        "lot_size": int(payload.get("lot_size", strategy_defaults["lot_size"])),
    }
    strategy_path = UI_RUNTIME / "strategy_qlib.yaml"
    _write_yaml(strategy_path, strategy_payload)
    if action == "backtest":
        result = run_cmd("scripts/run_backtest.py", ["--config", str(config_path), "--strategy", str(strategy_path)])
    else:
        result = run_cmd("scripts/manage_history.py", ["--config", str(config_path), "--mode", action])
    result["action"] = f"qlib-{action}"
    return result


def run_pattern_action(payload: dict[str, Any]) -> dict[str, Any]:
    defaults = build_pattern_defaults()
    selection_label = str(payload.get("selection_label") or defaults["selection_label"])
    selected = USER_PATTERN_OPTIONS.get(selection_label, "ALL")
    run_all = bool(payload.get("run_all")) or selected == "ALL"
    start_date = str(payload.get("start_date") or defaults["start_date"])
    end_date = str(payload.get("end_date") or defaults["end_date"])
    report_dir_value = payload.get("pattern_report_dir") or USER_PATTERN_REPORT_DIR
    report_dir = resolve_pattern_report_dir(report_dir_value, require_summary=False)
    base_payload = _read_yaml(CONFIGS["backtest"])
    base_payload["app_name"] = "user-pattern-ui"
    base_payload["environment"] = "backtest"
    base_payload["backtest_engine"] = "qlib"
    base_payload["history_parquet"] = "data/parquet/user_pattern_history.parquet"
    base_payload["history_source"] = "qmt"
    base_payload["history_adjustment"] = "front"
    base_payload["history_start"] = start_date.replace("-", "")
    base_payload["history_end"] = end_date.replace("-", "")
    base_payload["history_universe_sector"] = "沪深京A股"
    base_payload["history_universe_limit"] = 0
    base_payload["report_dir"] = workspace_relative(report_dir)
    base_payload["qlib_provider_dir"] = "runtime/qlib_data/user_pattern_cn_data"
    base_payload["qlib_dataset_dir"] = "runtime/qlib_data/user_pattern_source"
    base_payload["qlib_force_rebuild"] = False
    base_payload["symbols"] = []
    UI_RUNTIME.mkdir(parents=True, exist_ok=True)
    config_path = UI_RUNTIME / "user_pattern_app.yaml"
    _write_yaml(config_path, base_payload)
    modes = PATTERN_COMPARE_MODES if run_all else [selected]
    result = run_cmd(
        "scripts/run_user_pattern_backtests.py",
        [
            "--config",
            str(config_path),
            "--strategy-file",
            str(USER_PATTERN_STRATEGY_FILE),
            "--start",
            start_date,
            "--end",
            end_date,
            "--account",
            str(int(payload.get("account", defaults["account"]))),
            "--max-holdings",
            str(int(payload.get("max_holdings", defaults["max_holdings"]))),
            "--risk-degree",
            str(float(payload.get("risk_degree", defaults["risk_degree"]))),
            "--max-holding-days",
            str(int(payload.get("max_holding_days", defaults["max_holding_days"]))),
            "--progress",
            "--modes",
            *modes,
            "--output-dir",
            str(report_dir),
        ],
    )
    result["action"] = "user-pattern-all" if run_all else "user-pattern-selected"
    result["selection_label"] = selection_label
    database_url = str(base_payload.get("database_url") or "").strip()
    if result.get("ok") and database_url:
        summary_path = USER_PATTERN_REPORT_DIR / "summary.json"
        if summary_path.exists():
            summary_rows = json.loads(summary_path.read_text(encoding="utf-8"))
            persist_pattern_backtest_results(database_url, summary_rows, USER_PATTERN_REPORT_DIR)
    return result
