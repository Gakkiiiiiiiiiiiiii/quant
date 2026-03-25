from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from sqlalchemy import create_engine

from quant_demo.adapters.qmt.bridge_client import QmtBridgeClient
from quant_demo.core.config import AppSettings, load_app_settings
from quant_demo.core.enums import Environment
from quant_demo.core.exceptions import QmtUnavailableError
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
}
USER_PATTERN_OPTIONS = {
    "形态策略 B1": "B1",
    "形态策略 B2": "B2",
    "形态策略 B3": "B3",
    "三策略对比": "ALL",
}
USER_PATTERN_REPORT_DIR = ROOT / "data" / "reports" / "user_pattern_backtests"
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


def _report_path(report_dir: str) -> Path:
    path = Path(report_dir)
    return (ROOT / path if not path.is_absolute() else path) / "daily_report.md"


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
            "select order_id,broker_order_id,symbol,side,qty,filled_qty,status,avg_price,created_at,updated_at from orders order by created_at desc",
            ["created_at", "updated_at"],
        )
        trades = _sql(
            conn,
            "select trade_id,order_id,symbol,side,fill_qty,fill_price,commission,trade_time from trades order by trade_time desc",
            ["trade_time"],
        )
        risk = _sql(
            conn,
            "select risk_decision_id,order_intent_id,status,rule_results,decided_at from risk_decisions order by decided_at desc",
            ["decided_at"],
        )
        audit = _sql(
            conn,
            "select audit_log_id,object_type,object_id,message,payload,created_at from audit_logs order by created_at desc",
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
    if settings.environment != Environment.LIVE:
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


def overview(data: DashboardData) -> dict[str, Any]:
    latest = data.assets.iloc[-1] if not data.assets.empty else None
    total_asset = float(latest["total_asset"]) if latest is not None else None
    cash = float(latest["cash"]) if latest is not None else None
    turnover = float(latest["turnover"]) if latest is not None else None
    drawdown = float(latest["max_drawdown"]) if latest is not None else None
    market_value = float(data.positions["market_value"].sum()) if not data.positions.empty else 0.0
    unrealized_pnl = float(data.positions["unrealized_pnl"].sum()) if not data.positions.empty else 0.0
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


def connection_state(settings: AppSettings, probe: dict[str, Any] | None) -> dict[str, str]:
    if settings.environment != Environment.LIVE:
        return {"label": "离线数据库视图", "color": "#64748b", "detail": "当前未连接 QMT，只展示历史数据。"}
    if not probe or probe.get("error"):
        detail = probe.get("error") if probe else "实盘探测失败，无法读取账户与行情。"
        return {"label": "QMT 未连接", "color": "#b91c1c", "detail": detail}
    statuses = (probe.get("health") or {}).get("account_status") or []
    status_code = statuses[0].get("status") if statuses else None
    if str(status_code) == "0":
        return {"label": "QMT 在线", "color": "#0f766e", "detail": "账户状态正常，行情与账户查询可用。"}
    return {"label": f"QMT 状态 {status_code}", "color": "#d97706", "detail": "客户端已连接，但账户状态不是正常。"}


def alerts(settings: AppSettings, info: dict[str, Any], data: DashboardData, probe: dict[str, Any] | None) -> list[dict[str, str]]:
    items = []
    conn = connection_state(settings, probe)
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
        "selection_label": "三策略对比",
        "start_date": "2023-01-01",
        "end_date": date.today().isoformat(),
        "account": 500000,
        "max_holdings": 10,
        "risk_degree": 0.95,
        "max_holding_days": 15,
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
    base = Path(report_dir)
    if not base.is_absolute():
        base = ROOT / base
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


def build_dashboard_payload(profile: str = "backtest", config_path: str | None = None) -> dict[str, Any]:
    profile_key, resolved_config, settings = resolve_settings(profile, config_path)
    data = load_dashboard_data(settings.database_url, settings.report_dir)
    probe = load_live_probe(resolved_config, settings) if settings.environment == Environment.LIVE else {}
    pattern = load_user_pattern_results(USER_PATTERN_REPORT_DIR)
    pattern_summary = pattern["summary"].copy()
    pattern_comparison = pattern["comparison"].copy()
    pattern_actions = pattern["daily_actions"].copy()
    pattern_decisions = pattern["daily_decisions"].copy()
    qlib_runtime = build_qlib_runtime_payload(profile_key)
    qlib_status = history_status(AppSettings.model_validate(qlib_runtime))

    info = overview(data)
    connection = connection_state(settings, probe)
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

    primary_record = _select_primary_pattern_record(pattern_summary)
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
        info = _build_pattern_overview(primary_record, pattern_report, pattern_actions)
        connection = _build_pattern_connection(primary_record)
        alert_items = _build_pattern_alerts(settings, info, primary_record)
        display_assets = pattern_report
        display_positions = pd.DataFrame()
        display_orders = pd.DataFrame()
        display_trades = pd.DataFrame()
        display_risk = pattern_risk
        display_audit = pd.DataFrame()
        display_rules = pattern_risk
        display_report_text = _build_pattern_report_text(primary_record, pattern_risk, pattern_actions)
        display_benchmark_curve = _build_pattern_benchmark_curve(pattern_report)

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
            "summary": _frame_records(pattern_summary),
            "comparison": _frame_records(pattern_comparison),
            "daily_actions": _frame_records(pattern_actions),
            "daily_decisions": _frame_records(pattern_decisions),
            "primary_mode": primary_record.get("mode") if primary_record is not None else None,
            "png_url": f"/api/file?path={workspace_relative(pattern['png_path'])}",
            "html_url": f"/api/file?path={workspace_relative(pattern['html_path'])}",
            "base_dir": pattern["base_dir"],
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
    cmd = [sys.executable, script_name, *args]
    done = subprocess.run(cmd, cwd=ROOT, capture_output=True, check=False, env=env)
    return {
        "command": " ".join(cmd),
        "stdout": _decode(done.stdout).strip(),
        "stderr": _decode(done.stderr).strip(),
        "returncode": done.returncode,
        "ok": done.returncode == 0,
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
    base_payload["report_dir"] = "data/reports/user_pattern_backtests"
    base_payload["qlib_provider_dir"] = "runtime/qlib_data/user_pattern_cn_data"
    base_payload["qlib_dataset_dir"] = "runtime/qlib_data/user_pattern_source"
    base_payload["qlib_force_rebuild"] = False
    base_payload["symbols"] = []
    UI_RUNTIME.mkdir(parents=True, exist_ok=True)
    config_path = UI_RUNTIME / "user_pattern_app.yaml"
    _write_yaml(config_path, base_payload)
    modes = ["B1", "B2", "B3"] if run_all else [selected]
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
            "--modes",
            *modes,
            "--output-dir",
            str(USER_PATTERN_REPORT_DIR),
        ],
    )
    result["action"] = "user-pattern-all" if run_all else "user-pattern-selected"
    result["selection_label"] = selection_label
    return result
