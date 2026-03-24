from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st
import yaml
from sqlalchemy import create_engine

from quant_demo.adapters.qmt.bridge_client import QmtBridgeClient
from quant_demo.core.config import AppSettings, load_app_settings
from quant_demo.core.enums import Environment
from quant_demo.core.exceptions import QmtUnavailableError
from quant_demo.marketdata.history_manager import history_status

ROOT = Path(__file__).resolve().parents[3]
CONFIGS = {
    "回测视图": ROOT / "configs" / "app.yaml",
    "仿真视图": ROOT / "configs" / "paper.yaml",
    "实盘监控": ROOT / "configs" / "live.yaml",
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
SEVERITY_COLORS = {"critical": "#b91c1c", "warning": "#d97706", "info": "#0f766e"}
MODE_COLORS = {"backtest": "#1d4ed8", "paper": "#d97706", "live": "#0f766e"}


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


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        .stApp {background:#f2f4f7;color:#0f172a;}
        .block-container {max-width:1680px;padding-top:.4rem;padding-bottom:1.2rem;}
        .jq-topbar {background:#1f3566;color:#fff;border-radius:0;padding:.85rem 1.1rem;margin:-.4rem -1rem .7rem -1rem;display:flex;justify-content:space-between;align-items:center;gap:1rem;flex-wrap:wrap;}
        .jq-brand {font-size:1.8rem;font-weight:800;letter-spacing:.02em;}
        .jq-nav {display:flex;gap:1.1rem;color:#dbeafe;font-size:.95rem;}
        .jq-toolbar {background:#fff;border:1px solid #d9dde5;border-radius:4px;padding:.65rem .8rem;margin-bottom:.8rem;display:flex;justify-content:space-between;align-items:center;gap:1rem;flex-wrap:wrap;}
        .jq-btn {background:#2e62ad;color:#fff;border-radius:4px;padding:.34rem .72rem;font-size:.88rem;}
        .jq-side {background:#f8fafc;border:1px solid #d9dde5;border-radius:4px;padding:.65rem;min-height:680px;}
        .jq-content {background:#fff;border:1px solid #d9dde5;border-radius:4px;padding:.75rem;}
        .jq-card-grid {display:grid;grid-template-columns:repeat(5,minmax(120px,1fr));gap:.6rem;margin-bottom:.7rem;}
        .jq-card {background:#fff;border:1px solid #e5e7eb;border-radius:4px;padding:.52rem .6rem;}
        .jq-card-label {color:#64748b;font-size:.78rem;}
        .jq-card-value {font-size:1.6rem;font-weight:700;margin-top:.2rem;}
        .jq-section-title {font-size:1.45rem;font-weight:800;margin:.2rem 0 .55rem 0;}
        .panel-title {font-size:1rem;font-weight:700;color:#0f172a;margin-bottom:.2rem;}
        .panel-subtitle {color:#64748b;font-size:.84rem;margin-bottom:.55rem;}
        .control,.feed,.alert {background:#fff;border:1px solid #e2e8f0;border-radius:8px;padding:.85rem;}
        .alert {border-left:5px solid var(--accent);margin-bottom:.55rem;}
        .light {display:inline-flex;align-items:center;gap:.45rem;padding:.25rem .55rem;border-radius:999px;background:#fff;border:1px solid #cbd5e1;}
        .dot {width:9px;height:9px;border-radius:50%;}
        </style>
        """,
        unsafe_allow_html=True,
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
            rows.append({"decided_at": record.get("decided_at"), "decision_status": record.get("status"), "rule_name": item.get("rule_name"), "passed": item.get("passed"), "message": item.get("message")})
    return pd.DataFrame(rows)

@st.cache_data(ttl=20, show_spinner=False)
def load_dashboard_data(database_url: str, report_dir: str) -> DashboardData:
    engine = create_engine(database_url, future=True)
    with engine.connect() as conn:
        assets = _sql(conn, "select account_id,snapshot_time,total_asset,cash,frozen_cash,total_pnl,turnover,max_drawdown from asset_snapshots order by snapshot_time", ["snapshot_time"])
        positions = _sql(conn, "select symbol,qty,available_qty,cost_price,market_price,qty*market_price as market_value,(market_price-cost_price)*qty as unrealized_pnl,snapshot_time from position_snapshots where snapshot_time=(select max(snapshot_time) from position_snapshots) order by market_value desc", ["snapshot_time"])
        orders = _sql(conn, "select order_id,broker_order_id,symbol,side,qty,filled_qty,status,avg_price,created_at,updated_at from orders order by created_at desc", ["created_at", "updated_at"])
        trades = _sql(conn, "select trade_id,order_id,symbol,side,fill_qty,fill_price,commission,trade_time from trades order by trade_time desc", ["trade_time"])
        risk = _sql(conn, "select risk_decision_id,order_intent_id,status,rule_results,decided_at from risk_decisions order by decided_at desc", ["decided_at"])
        audit = _sql(conn, "select audit_log_id,object_type,object_id,message,payload,created_at from audit_logs order by created_at desc", ["created_at"])
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


@st.cache_data(ttl=10, show_spinner=False)
def load_live_probe(config_path: str) -> dict[str, Any]:
    settings = load_app_settings(config_path)
    if settings.environment != Environment.LIVE:
        return {}
    bridge = QmtBridgeClient(settings)
    return {"health": bridge.healthcheck(), "quotes": bridge.get_quotes(settings.symbols), "account": bridge.get_account_snapshot()}


@st.cache_data(ttl=8, show_spinner=False)
def load_runtime_logs() -> dict[str, str]:
    qmt_log = None
    for folder in [ROOT / "runtime" / "qmt_client" / "installed" / "userdata_mini" / "log", ROOT / "runtime" / "qmt_client" / "installed" / "userdata" / "log"]:
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


@st.cache_data(ttl=15, show_spinner=False)
def load_history_runtime_status(payload_json: str) -> dict[str, Any]:
    payload = json.loads(payload_json)
    return history_status(AppSettings.model_validate(payload))


def fmt_money(value: float | int | None) -> str:
    return "--" if value is None or pd.isna(value) else f"{float(value):,.2f}"


def fmt_ratio(value: float | int | None) -> str:
    return "--" if value is None or pd.isna(value) else f"{float(value) * 100:.2f}%"


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
    return {"total_asset": total_asset, "cash": cash, "turnover": turnover, "drawdown": drawdown, "market_value": market_value, "unrealized_pnl": unrealized_pnl, "exposure": market_value / total_asset if total_asset else None, "total_return": total_return, "approved": int((data.risk["status"] == "approved").sum()) if not data.risk.empty else 0, "rejected": int((data.risk["status"] == "rejected").sum()) if not data.risk.empty else 0, "order_count": len(data.orders), "trade_count": len(data.trades), "latest_time": latest["snapshot_time"] if latest is not None else None}


def connection_state(settings: AppSettings, probe: dict[str, Any] | None) -> dict[str, str]:
    if settings.environment != Environment.LIVE:
        return {"label": "离线数据库视图", "color": "#64748b", "detail": "当前未连接 QMT，只展示历史数据。"}
    if not probe:
        return {"label": "QMT 未连接", "color": "#b91c1c", "detail": "实盘探测失败，无法读取账户与行情。"}
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
        items.append({"severity": "critical", "title": "回撤触及日损阈值", "detail": f"当前最大回撤 {fmt_ratio(info['drawdown'])}。"})
    if settings.environment == Environment.LIVE and not settings.qmt_trade_enabled:
        items.append({"severity": "info", "title": "实盘委托已锁定", "detail": "当前只允许探测账户和行情，不允许自动下单。"})
    return items

def ensure_strategy_state(default_name: str) -> None:
    label = st.session_state.get("strategy_label")
    if not label:
        label = next((name for name, path in STRATEGIES.items() if path.stem == default_name), "ETF 轮动")
        st.session_state["strategy_label"] = label
    defaults = _read_yaml(STRATEGIES[st.session_state["strategy_label"]])
    if st.session_state.get("strategy_defaults_loaded_for") != st.session_state["strategy_label"]:
        st.session_state["strategy_name"] = defaults.get("name", STRATEGIES[st.session_state["strategy_label"]].stem)
        st.session_state["strategy_impl"] = defaults.get("implementation", STRATEGIES[st.session_state["strategy_label"]].stem)
        st.session_state["rebalance_frequency"] = defaults.get("rebalance_frequency", "weekly")
        st.session_state["lookback_days"] = int(defaults.get("lookback_days", 20))
        st.session_state["top_n"] = int(defaults.get("top_n", 2))
        st.session_state["lot_size"] = int(defaults.get("lot_size", 100))
        st.session_state["strategy_defaults_loaded_for"] = st.session_state["strategy_label"]


def current_strategy_payload() -> dict[str, Any]:
    return {"name": st.session_state["strategy_name"], "implementation": st.session_state["strategy_impl"], "rebalance_frequency": st.session_state["rebalance_frequency"], "lookback_days": int(st.session_state["lookback_days"]), "top_n": int(st.session_state["top_n"]), "lot_size": int(st.session_state["lot_size"])}


def ensure_qlib_state() -> None:
    defaults = _read_yaml(CONFIGS["回测视图"])
    if st.session_state.get("qlib_defaults_loaded"):
        return
    st.session_state["qlib_sector_name"] = defaults.get("history_universe_sector", "沪深京A股")
    st.session_state["qlib_universe_limit"] = int(defaults.get("history_universe_limit", 0))
    st.session_state["qlib_history_start"] = defaults.get("history_start", "20200101")
    st.session_state["qlib_history_adjustment"] = defaults.get("history_adjustment", "front")
    st.session_state["qlib_batch_size"] = int(defaults.get("history_batch_size", 200))
    st.session_state["qlib_n_drop"] = int(defaults.get("qlib_n_drop", 1))
    st.session_state["qlib_force_rebuild"] = bool(defaults.get("qlib_force_rebuild", False))
    st.session_state["qlib_defaults_loaded"] = True


def build_qlib_runtime_payload() -> dict[str, Any]:
    ensure_qlib_state()
    payload = _read_yaml(CONFIGS["回测视图"])
    payload["backtest_engine"] = "qlib"
    payload["history_source"] = "qmt"
    payload["symbols"] = []
    payload["history_universe_sector"] = st.session_state["qlib_sector_name"]
    payload["history_universe_limit"] = int(st.session_state["qlib_universe_limit"])
    payload["history_start"] = st.session_state["qlib_history_start"]
    payload["history_adjustment"] = st.session_state["qlib_history_adjustment"]
    payload["history_batch_size"] = int(st.session_state["qlib_batch_size"])
    payload["qlib_n_drop"] = int(st.session_state["qlib_n_drop"])
    payload["qlib_force_rebuild"] = bool(st.session_state["qlib_force_rebuild"])
    return payload


def build_qlib_runtime_config() -> Path:
    payload = build_qlib_runtime_payload()
    UI_RUNTIME.mkdir(parents=True, exist_ok=True)
    config_path = UI_RUNTIME / "qlib_runtime.yaml"
    _write_yaml(config_path, payload)
    return config_path


def run_cmd(script_name: str, args: list[str]) -> dict[str, Any]:
    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "utf-8"
    cmd = [sys.executable, script_name, *args]
    done = subprocess.run(cmd, cwd=ROOT, capture_output=True, check=False, env=env)
    return {"command": " ".join(cmd), "stdout": _decode(done.stdout).strip(), "stderr": _decode(done.stderr).strip(), "returncode": done.returncode, "ok": done.returncode == 0}


def run_action(action: str, payload: dict[str, Any]) -> None:
    UI_RUNTIME.mkdir(parents=True, exist_ok=True)
    strategy_path = UI_RUNTIME / f"strategy_{action}.yaml"
    _write_yaml(strategy_path, payload)
    if action == "backtest":
        result = run_cmd("scripts/run_backtest.py", ["--config", str(CONFIGS["回测视图"]), "--strategy", str(strategy_path)])
    elif action == "paper":
        result = run_cmd("scripts/run_paper.py", ["--config", str(CONFIGS["仿真视图"]), "--strategy", str(strategy_path)])
    elif action == "probe":
        result = run_cmd("scripts/run_live.py", ["--config", str(CONFIGS["实盘监控"]), "--strategy", str(strategy_path), "--mode", "probe"])
    else:
        result = run_cmd("scripts/run_live.py", ["--config", str(CONFIGS["实盘监控"]), "--strategy", str(strategy_path), "--mode", "strategy"])
    result["action"] = action
    st.session_state["last_command_result"] = result
    st.cache_data.clear()
    st.rerun()


def run_qlib_action(action: str, strategy_payload: dict[str, Any]) -> None:
    config_path = build_qlib_runtime_config()
    strategy_path = UI_RUNTIME / "strategy_qlib.yaml"
    _write_yaml(strategy_path, strategy_payload)
    if action == "backtest":
        result = run_cmd("scripts/run_backtest.py", ["--config", str(config_path), "--strategy", str(strategy_path)])
    else:
        result = run_cmd("scripts/manage_history.py", ["--config", str(config_path), "--mode", action])
    result["action"] = f"qlib-{action}"
    st.session_state["last_qlib_result"] = result
    st.cache_data.clear()
    st.rerun()


def ensure_user_pattern_state() -> None:
    options = list(USER_PATTERN_OPTIONS.keys())
    current_label = st.session_state.get("user_pattern_label")
    if current_label not in options:
        st.session_state["user_pattern_label"] = options[-1] if options else ""
    if st.session_state.get("user_pattern_defaults_loaded"):
        return
    st.session_state["user_pattern_start"] = date(2023, 1, 1)
    st.session_state["user_pattern_end"] = date(2026, 3, 20)
    st.session_state["user_pattern_account"] = 500000
    st.session_state["user_pattern_max_holdings"] = 10
    st.session_state["user_pattern_risk_degree"] = 0.95
    st.session_state["user_pattern_max_holding_days"] = 15
    st.session_state["user_pattern_defaults_loaded"] = True


@st.cache_data(ttl=20, show_spinner=False)
def load_user_pattern_results(report_dir: str) -> dict[str, Any]:
    base = Path(report_dir)
    if not base.is_absolute():
        base = ROOT / base
    summary_path = base / "summary.json"
    comparison_path = base / "equity_comparison.csv"
    summary = pd.DataFrame()
    comparison = pd.DataFrame()
    if summary_path.exists():
        summary = pd.DataFrame(json.loads(summary_path.read_text(encoding="utf-8")))
    if comparison_path.exists():
        comparison = pd.read_csv(comparison_path)
        if not comparison.empty and "datetime" in comparison.columns:
            comparison["datetime"] = pd.to_datetime(comparison["datetime"], errors="coerce")
    return {
        "summary": summary,
        "comparison": comparison,
        "png_path": str(base / "equity_comparison.png"),
        "html_path": str(base / "equity_comparison.html"),
        "base_dir": str(base),
    }


def build_user_pattern_runtime_payload() -> dict[str, Any]:
    ensure_user_pattern_state()
    payload = _read_yaml(CONFIGS["????"])
    payload["environment"] = "backtest"
    payload["backtest_engine"] = "qlib"
    payload["history_parquet"] = "data/parquet/user_pattern_history.parquet"
    payload["history_source"] = "qmt"
    payload["history_adjustment"] = "front"
    payload["history_start"] = st.session_state["user_pattern_start"].strftime("%Y%m%d")
    payload["history_end"] = st.session_state["user_pattern_end"].strftime("%Y%m%d")
    payload["history_universe_sector"] = "???A?"
    payload["history_universe_limit"] = 0
    payload["report_dir"] = "data/reports/user_pattern_backtests"
    payload["qlib_provider_dir"] = "runtime/qlib_data/user_pattern_cn_data"
    payload["qlib_dataset_dir"] = "runtime/qlib_data/user_pattern_source"
    payload["qlib_force_rebuild"] = False
    payload["symbols"] = []
    return payload


def build_user_pattern_runtime_config() -> Path:
    UI_RUNTIME.mkdir(parents=True, exist_ok=True)
    config_path = UI_RUNTIME / "user_pattern_app.yaml"
    _write_yaml(config_path, build_user_pattern_runtime_payload())
    return config_path


def run_user_pattern_action(run_all: bool) -> None:
    ensure_user_pattern_state()
    config_path = build_user_pattern_runtime_config()
    selected = USER_PATTERN_OPTIONS[st.session_state["user_pattern_label"]]
    modes = ["B1", "B2", "B3"] if run_all or selected == "ALL" else [selected]
    result = run_cmd(
        "scripts/run_user_pattern_backtests.py",
        [
            "--config", str(config_path),
            "--strategy-file", str(USER_PATTERN_STRATEGY_FILE),
            "--start", st.session_state["user_pattern_start"].strftime("%Y-%m-%d"),
            "--end", st.session_state["user_pattern_end"].strftime("%Y-%m-%d"),
            "--account", str(int(st.session_state["user_pattern_account"])),
            "--max-holdings", str(int(st.session_state["user_pattern_max_holdings"])),
            "--risk-degree", str(float(st.session_state["user_pattern_risk_degree"])),
            "--max-holding-days", str(int(st.session_state["user_pattern_max_holding_days"])),
            "--modes", *modes,
            "--output-dir", str(USER_PATTERN_REPORT_DIR),
        ],
    )
    result["action"] = "user-pattern-all" if run_all else "user-pattern-selected"
    st.session_state["last_user_pattern_result"] = result
    st.cache_data.clear()
    st.rerun()


def render_user_pattern_panel() -> None:
    ensure_user_pattern_state()
    artifacts = load_user_pattern_results(str(USER_PATTERN_REPORT_DIR))
    summary = artifacts["summary"]
    comparison = artifacts["comparison"]
    st.markdown('<div class="panel-title">Pattern Research</div><div class="panel-subtitle">Run B1, B2, B3 individually or compare all three equity curves.</div>', unsafe_allow_html=True)
    cfg_col, chart_col = st.columns([0.9, 1.1])
    with cfg_col:
        st.markdown('<div class="control">', unsafe_allow_html=True)
        st.selectbox("Pattern Selection", list(USER_PATTERN_OPTIONS.keys()), key="user_pattern_label")
        st.date_input("Start Date", key="user_pattern_start")
        st.date_input("End Date", key="user_pattern_end")
        st.number_input("Initial Capital", min_value=10000, max_value=50000000, step=10000, key="user_pattern_account")
        st.number_input("Risk Degree", min_value=0.1, max_value=1.0, step=0.05, key="user_pattern_risk_degree")
        st.number_input("Max Holdings", min_value=1, max_value=50, key="user_pattern_max_holdings")
        st.number_input("Max Holding Days", min_value=1, max_value=60, key="user_pattern_max_holding_days")
        if st.button("Run Selected", use_container_width=True, type="primary"):
            run_user_pattern_action(False)
        if st.button("Run All", use_container_width=True):
            run_user_pattern_action(True)
        st.caption(f"Report Dir: {artifacts['base_dir']}")
        st.markdown('</div>', unsafe_allow_html=True)
        if not summary.empty:
            selected_mode = USER_PATTERN_OPTIONS[st.session_state["user_pattern_label"]]
            focus = summary if selected_mode == "ALL" else summary[summary["mode"] == selected_mode]
            if not focus.empty:
                item = focus.iloc[0]
                metric_cols = st.columns(3)
                metric_cols[0].metric("Total Return", fmt_ratio(item.get("total_return")))
                metric_cols[1].metric("Annualized", fmt_ratio(item.get("annualized_return")))
                metric_cols[2].metric("Max Drawdown", fmt_ratio(item.get("max_drawdown")))
            st.dataframe(summary[["mode", "total_return", "annualized_return", "max_drawdown", "ending_equity", "trading_days"]], use_container_width=True, hide_index=True)
        result = st.session_state.get("last_user_pattern_result")
        if result:
            with st.expander("Pattern Task Output", expanded=not result.get("ok")):
                st.code(result.get("command", ""), language="bash")
                st.text(result.get("stdout") or result.get("stderr") or "No output")
    with chart_col:
        st.markdown('<div class="control">', unsafe_allow_html=True)
        st.markdown('<div class="panel-title">Equity Comparison</div><div class="panel-subtitle">Compare B1, B2, B3 and benchmark equity curves.</div>', unsafe_allow_html=True)
        if comparison.empty:
            st.info("No pattern equity curve yet. Run a pattern backtest first.")
        else:
            selected_mode = USER_PATTERN_OPTIONS[st.session_state["user_pattern_label"]]
            view = comparison if selected_mode == "ALL" else comparison[comparison["series"].isin([selected_mode, "Benchmark"])]
            chart = px.line(view, x="datetime", y="equity", color="series", template="plotly_white")
            chart.update_layout(height=440, margin=dict(l=16, r=16, t=12, b=16), legend_title_text="Series")
            st.plotly_chart(chart, use_container_width=True)
            png_path = Path(artifacts["png_path"])
            if png_path.exists():
                st.caption(f"Static Image: {png_path}")
        st.markdown('</div>', unsafe_allow_html=True)


def render_pattern_overview() -> None:
    """在首页主看板直接展示三策略收益曲线。"""
    artifacts = load_user_pattern_results(str(USER_PATTERN_REPORT_DIR))
    summary = artifacts["summary"]
    comparison = artifacts["comparison"]
    st.markdown(
        '<div class="panel-title">Pattern Equity Board</div><div class="panel-subtitle">B1, B2, B3 and benchmark equity curves directly on the main dashboard.</div>',
        unsafe_allow_html=True,
    )
    if comparison.empty:
        st.info("No pattern equity curve yet. Run a pattern backtest first.")
        return

    view = comparison[comparison["series"].isin(["B1", "B2", "B3", "Benchmark"])].copy()
    if view.empty:
        st.info("Pattern comparison data is empty.")
        return

    chart = px.line(
        view,
        x="datetime",
        y="equity",
        color="series",
        template="plotly_white",
        color_discrete_map={
            "B1": "#1d4ed8",
            "B2": "#d97706",
            "B3": "#0f766e",
            "Benchmark": "#64748b",
        },
    )
    chart.update_layout(
        height=360,
        margin=dict(l=12, r=12, t=12, b=12),
        legend_title_text="Series",
        hovermode="x unified",
        xaxis_title="Date",
        yaxis_title="Equity",
    )
    chart.update_traces(line=dict(width=2.4))
    st.plotly_chart(chart, use_container_width=True, config={"displayModeBar": False})

    if summary.empty:
        return

    score = summary[summary["mode"].isin(["B1", "B2", "B3"])].copy()
    if score.empty:
        return
    score = score.sort_values("total_return", ascending=False)
    best = score.iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Best Strategy", str(best.get("mode", "-")))
    c2.metric("Best Total Return", fmt_ratio(best.get("total_return")))
    c3.metric("Best Annualized", fmt_ratio(best.get("annualized_return")))
    c4.metric("Best Max Drawdown", fmt_ratio(best.get("max_drawdown")))
    st.dataframe(
        score[["mode", "total_return", "annualized_return", "max_drawdown", "ending_equity", "trading_days"]],
        use_container_width=True,
        hide_index=True,
    )


def render_sidebar(default_profile: str) -> tuple[str, AppSettings, str]:
    names = list(CONFIGS.keys())
    index = names.index(default_profile) if default_profile in names else 0
    st.sidebar.markdown("## 控制台")
    profile = st.sidebar.radio("视图模式", names, index=index)
    config_path = CONFIGS[profile]
    settings = load_app_settings(config_path)
    if st.sidebar.button("刷新面板", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    universe_text = ", ".join(settings.symbols) if settings.symbols else settings.history_universe_sector or "未配置"
    st.sidebar.markdown(f"- 环境: `{settings.environment.value}`")
    st.sidebar.markdown(f"- 默认策略: `{settings.default_strategy}`")
    st.sidebar.markdown(f"- 股票池: `{universe_text}`")
    st.sidebar.code('.\\.venv\\Scripts\\python.exe scripts\\manage_history.py --mode incremental')
    return profile, settings, str(config_path)


def render_hero(settings: AppSettings, profile: str, info: dict[str, Any], probe: dict[str, Any] | None) -> None:
    latest = info["latest_time"].strftime("%Y-%m-%d %H:%M") if info["latest_time"] is not None and not pd.isna(info["latest_time"]) else "暂无快照"
    probe_hint = "QMT 已接通" if probe and probe.get("health") else "使用本地数据库视图"
    st.markdown(f'''<div class="hero"><div style="display:flex;justify-content:space-between;gap:1rem;flex-wrap:wrap;"><div><div style="font-size:.82rem;letter-spacing:.16em;text-transform:uppercase;color:#93c5fd;">Quant Control Room</div><div style="font-size:2rem;font-weight:800;margin-top:.35rem;">事件驱动量化交易终端</div><div style="margin-top:.45rem;color:#cbd5e1;max-width:820px;line-height:1.6;">当前视图为 {profile}，模式 {settings.environment.value}，数据库快照时间 {latest}。</div></div><div style="min-width:260px;text-align:right;"><div style="margin-top:.9rem;color:#cbd5e1;font-size:.9rem;">{probe_hint}</div><div style="margin-top:.2rem;color:#cbd5e1;font-size:.9rem;">数据库: {settings.database_url}</div></div></div></div>''', unsafe_allow_html=True)


def render_kpis(info: dict[str, Any]) -> None:
    cards = [("总资产", fmt_money(info["total_asset"]), f"现金 {fmt_money(info['cash'])}"), ("持仓市值", fmt_money(info["market_value"]), f"暴露度 {fmt_ratio(info['exposure'])}"), ("未实现盈亏", fmt_money(info["unrealized_pnl"]), f"累计换手 {fmt_money(info['turnover'])}"), ("累计收益", fmt_ratio(info["total_return"]), f"最大回撤 {fmt_ratio(info['drawdown'])}"), ("订单 / 成交", f"{info['order_count']} / {info['trade_count']}", f"风控通过 {info['approved']} · 拒绝 {info['rejected']}")]
    for col, (label, value, foot) in zip(st.columns(len(cards)), cards):
        with col:
            st.markdown(f'<div class="kpi"><div class="kpi-label">{label}</div><div class="kpi-value">{value}</div><div class="kpi-foot">{foot}</div></div>', unsafe_allow_html=True)

def render_status(settings: AppSettings, info: dict[str, Any], data: DashboardData, probe: dict[str, Any] | None) -> None:
    st.markdown('<div class="panel-title">运行状态</div><div class="panel-subtitle">模式、快照、风控和连接灯。</div>', unsafe_allow_html=True)
    latest = info["latest_time"].strftime("%Y-%m-%d %H:%M") if info["latest_time"] is not None and not pd.isna(info["latest_time"]) else "暂无数据"
    st.metric("当前模式", "只读联调" if settings.environment == Environment.LIVE and not settings.qmt_trade_enabled else settings.environment.value)
    st.metric("最新快照", latest)
    conn = connection_state(settings, probe)
    st.markdown(f'<div class="light"><span class="dot" style="background:{conn["color"]};"></span><span>{conn["label"]}</span></div><div class="panel-subtitle" style="margin-top:.4rem;">{conn["detail"]}</div>', unsafe_allow_html=True)
    if not data.rules.empty:
        summary = data.rules.groupby(["rule_name", "passed"]).size().reset_index(name="count")
        st.dataframe(summary, use_container_width=True, hide_index=True)


def render_equity(data: DashboardData) -> None:
    st.markdown('<div class="panel-title">账户曲线</div><div class="panel-subtitle">策略净值与 Benchmark 同图对比。</div>', unsafe_allow_html=True)
    if data.assets.empty:
        st.info("当前数据库没有资产快照。")
        return
    assets = data.assets.copy().sort_values("snapshot_time")
    frame = pd.DataFrame({"trading_date": pd.to_datetime(assets["snapshot_time"], errors="coerce"), "strategy_equity": assets["total_asset"].astype(float)})
    benchmark = data.benchmark_curve.copy()
    if not benchmark.empty and {"trading_date", "benchmark_equity"}.issubset(set(benchmark.columns)):
        benchmark = benchmark[["trading_date", "benchmark_equity"]].dropna()
        benchmark["trading_date"] = pd.to_datetime(benchmark["trading_date"], errors="coerce")
        frame = frame.merge(benchmark, on="trading_date", how="left")
    melted = frame.melt(id_vars="trading_date", value_vars=[col for col in ["strategy_equity", "benchmark_equity"] if col in frame.columns], var_name="series", value_name="equity")
    if melted.empty:
        st.area_chart(assets.set_index("snapshot_time")[["total_asset", "cash"]], height=320)
        return
    label_map = {"strategy_equity": "策略曲线", "benchmark_equity": "Benchmark"}
    melted["series"] = melted["series"].map(lambda item: label_map.get(item, item))
    chart = px.line(melted.dropna(), x="trading_date", y="equity", color="series", template="plotly_white")
    chart.update_layout(height=360, margin=dict(l=16, r=16, t=12, b=16), legend_title_text="曲线")
    st.plotly_chart(chart, use_container_width=True, config={"displayModeBar": False})


def render_refresh_controls() -> tuple[bool, int]:
    st.sidebar.markdown("### 刷新设置")
    enabled = st.sidebar.checkbox("开启准实时刷新", value=st.session_state.get("auto_refresh_enabled", False), key="auto_refresh_enabled")
    interval = int(st.sidebar.number_input("刷新间隔(秒)", min_value=3, max_value=120, value=int(st.session_state.get("auto_refresh_interval", 15)), step=1, key="auto_refresh_interval"))
    return enabled, interval


def render_positions(data: DashboardData) -> None:
    st.markdown('<div class="panel-title">持仓总览</div><div class="panel-subtitle">最新持仓快照与浮盈亏。</div>', unsafe_allow_html=True)
    if data.positions.empty:
        st.info("当前没有持仓快照。")
        return
    frame = data.positions.copy()
    frame["snapshot_time"] = frame["snapshot_time"].dt.strftime("%Y-%m-%d %H:%M")
    st.dataframe(frame, use_container_width=True, hide_index=True)


def render_market(settings: AppSettings, probe: dict[str, Any] | None) -> None:
    st.markdown('<div class="panel-title">市场观察</div><div class="panel-subtitle">股票池行情与实时探测结果。</div>', unsafe_allow_html=True)
    if not probe or not probe.get("quotes"):
        rows = settings.symbols or [settings.history_universe_sector or "等待 Qlib 股票池"]
        st.dataframe(pd.DataFrame({"symbol": rows, "status": ["等待实时探测"] * len(rows)}), use_container_width=True, hide_index=True)
        return
    payload_rows = []
    for symbol, payload in probe.get("quotes", {}).items():
        last_price = payload.get("last_price")
        last_close = payload.get("last_close")
        change = None if last_price in (None, "") or last_close in (None, 0, "") else (float(last_price) - float(last_close)) / float(last_close)
        payload_rows.append({"symbol": symbol, "last_price": last_price, "last_close": last_close, "change": change, "volume": payload.get("volume")})
    st.dataframe(pd.DataFrame(payload_rows).sort_values("symbol"), use_container_width=True, hide_index=True)


def render_controls(settings: AppSettings) -> None:
    ensure_strategy_state(settings.default_strategy)
    st.markdown('<div class="panel-title">策略控制台</div><div class="panel-subtitle">调整策略参数并一键运行回测、仿真或实盘探测。</div>', unsafe_allow_html=True)
    st.markdown('<div class="control">', unsafe_allow_html=True)
    st.selectbox("策略模板", list(STRATEGIES.keys()), key="strategy_label")
    ensure_strategy_state(settings.default_strategy)
    left, mid, right, action = st.columns(4)
    with left:
        st.text_input("策略名", key="strategy_name")
        st.number_input("观察窗口", min_value=1, max_value=250, key="lookback_days")
    with mid:
        st.text_input("实现标识", key="strategy_impl")
        st.number_input("Top N", min_value=1, max_value=20, key="top_n")
    with right:
        st.selectbox("调仓频率", ["daily", "weekly"], key="rebalance_frequency")
        st.number_input("最小交易单位", min_value=1, max_value=10000, key="lot_size")
    payload = current_strategy_payload()
    with action:
        if st.button("运行回测", use_container_width=True):
            run_action("backtest", payload)
        if st.button("运行仿真", use_container_width=True):
            run_action("paper", payload)
        if st.button("实盘探测", use_container_width=True):
            run_action("probe", payload)
        if settings.qmt_trade_enabled:
            if st.button("运行实盘", use_container_width=True, type="primary"):
                run_action("strategy", payload)
        else:
            st.button("运行实盘", use_container_width=True, disabled=True)
    st.markdown('</div>', unsafe_allow_html=True)
    result = st.session_state.get("last_command_result")
    if result:
        with st.expander("查看策略任务输出", expanded=not result.get("ok")):
            st.code(result.get("command", ""), language="bash")
            st.text(result.get("stdout") or result.get("stderr") or "无输出")


def render_qlib_panel() -> None:
    ensure_qlib_state()
    payload = build_qlib_runtime_payload()
    status = load_history_runtime_status(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    strategy_payload = current_strategy_payload()
    st.markdown('<div class="panel-title">Qlib 全市场回测</div><div class="panel-subtitle">管理 QMT 全市场前复权历史、Qlib provider 缓存，并直接触发全市场回测。</div>', unsafe_allow_html=True)
    st.markdown('<div class="control">', unsafe_allow_html=True)
    left, mid, right = st.columns(3)
    with left:
        st.text_input("股票池板块", key="qlib_sector_name")
        st.text_input("历史起始日", key="qlib_history_start")
        st.selectbox("复权口径", ["front", "back", "none"], key="qlib_history_adjustment")
    with mid:
        st.number_input("股票数量上限", min_value=0, max_value=10000, key="qlib_universe_limit")
        st.number_input("批量下载大小", min_value=1, max_value=1000, key="qlib_batch_size")
        st.number_input("Qlib 换仓数", min_value=1, max_value=20, key="qlib_n_drop")
    with right:
        st.checkbox("回测前强制重建 Qlib provider", key="qlib_force_rebuild")
        st.metric("历史记录数", status.get("row_count", 0))
        st.metric("股票数", status.get("symbol_count", 0))
        st.metric("最新交易日", status.get("latest_trading_date") or "--")
    cols = st.columns(5)
    if cols[0].button("增量更新历史", use_container_width=True):
        run_qlib_action("incremental", strategy_payload)
    if cols[1].button("全量重建历史", use_container_width=True):
        run_qlib_action("full", strategy_payload)
    if cols[2].button("清理历史缓存", use_container_width=True):
        run_qlib_action("cleanup-history", strategy_payload)
    if cols[3].button("清理 Qlib 缓存", use_container_width=True):
        run_qlib_action("cleanup-qlib", strategy_payload)
    if cols[4].button("运行全市场回测", use_container_width=True, type="primary"):
        run_qlib_action("backtest", strategy_payload)
    st.caption(f"历史文件：{payload.get('history_parquet')} | Provider：{payload.get('qlib_provider_dir')} | Dataset：{payload.get('qlib_dataset_dir')}")
    st.markdown('</div>', unsafe_allow_html=True)
    result = st.session_state.get("last_qlib_result")
    if result:
        with st.expander("查看 Qlib 任务输出", expanded=not result.get("ok")):
            st.code(result.get("command", ""), language="bash")
            st.text(result.get("stdout") or result.get("stderr") or "无输出")


def render_alerts(items: list[dict[str, str]]) -> None:
    st.markdown('<div class="panel-title">告警面板</div><div class="panel-subtitle">风控、连接和运行状态的即时提示。</div>', unsafe_allow_html=True)
    for item in items:
        color = SEVERITY_COLORS[item["severity"]]
        st.markdown(f'<div class="alert" style="--accent:{color};"><div style="font-weight:600;">{item["title"]}</div><div class="panel-subtitle" style="margin:.3rem 0 0 0;">{item["detail"]}</div></div>', unsafe_allow_html=True)

def render_logs() -> None:
    st.markdown('<div class="panel-title">日志中心</div><div class="panel-subtitle">QMT、本地 API、UI 和内部留痕的实时尾部日志。</div>', unsafe_allow_html=True)
    sources = load_runtime_logs()
    selected = st.selectbox("日志源", list(sources.keys()))
    st.code(sources[selected], language="text")


def render_tabs(data: DashboardData) -> None:
    tab_orders, tab_trades, tab_risk, tab_audit, tab_report = st.tabs(["订单簿", "成交流", "风控台", "审计流", "日终报告"])
    with tab_orders:
        st.dataframe(data.orders, use_container_width=True, hide_index=True) if not data.orders.empty else st.info("暂无订单。")
    with tab_trades:
        st.dataframe(data.trades, use_container_width=True, hide_index=True) if not data.trades.empty else st.info("暂无成交。")
    with tab_risk:
        if data.rules.empty:
            st.info("暂无风控规则明细。")
        else:
            st.dataframe(data.rules, use_container_width=True, hide_index=True)
    with tab_audit:
        st.dataframe(data.audit, use_container_width=True, hide_index=True) if not data.audit.empty else st.info("暂无审计日志。")
    with tab_report:
        st.markdown(data.report_text)


def render_joinquant_topbar(settings: AppSettings, info: dict[str, Any]) -> None:
    latest = info["latest_time"].strftime("%Y-%m-%d %H:%M") if info["latest_time"] is not None and not pd.isna(info["latest_time"]) else "暂无快照"
    st.markdown(
        f"""
        <div class="jq-topbar">
          <div style="display:flex;align-items:center;gap:1.1rem;">
            <div class="jq-brand">JoinQuant 风格回测台</div>
            <div class="jq-nav"><span>首页</span><span>量化研究</span><span>回测详情</span><span>实盘模拟</span></div>
          </div>
          <div style="font-size:.9rem;color:#dbeafe;">模式: {settings.environment.value} · 最新快照: {latest}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        f"""
        <div class="jq-toolbar">
          <div>设置：回测资金展示按数据库快照自动读取，当前总资产 <b>{fmt_money(info["total_asset"])}</b>，状态：<b>回测完成</b></div>
          <div style="display:flex;gap:.45rem;align-items:center;">
            <span class="jq-btn">模拟交易</span>
            <span class="jq-btn">归因分析</span>
            <span class="jq-btn">导出</span>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_joinquant_overview(data: DashboardData, info: dict[str, Any]) -> None:
    st.markdown('<div class="jq-content"><div class="jq-section-title">收益概述</div>', unsafe_allow_html=True)
    st.markdown(
        f"""
        <div class="jq-card-grid">
          <div class="jq-card"><div class="jq-card-label">策略收益</div><div class="jq-card-value">{fmt_ratio(info["total_return"])}</div></div>
          <div class="jq-card"><div class="jq-card-label">最大回撤</div><div class="jq-card-value">{fmt_ratio(info["drawdown"])}</div></div>
          <div class="jq-card"><div class="jq-card-label">累计换手</div><div class="jq-card-value">{fmt_money(info["turnover"])}</div></div>
          <div class="jq-card"><div class="jq-card-label">订单数</div><div class="jq-card-value">{info["order_count"]}</div></div>
          <div class="jq-card"><div class="jq-card-label">成交数</div><div class="jq-card-value">{info["trade_count"]}</div></div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    render_equity(data)
    st.markdown("</div>", unsafe_allow_html=True)


def render_joinquant_trade_detail(data: DashboardData) -> None:
    st.markdown('<div class="jq-content"><div class="jq-section-title">交易详情</div>', unsafe_allow_html=True)
    render_tabs(data)
    st.markdown("</div>", unsafe_allow_html=True)


def render_joinquant_daily_pnl(data: DashboardData) -> None:
    st.markdown('<div class="jq-content"><div class="jq-section-title">每日持仓&收益</div>', unsafe_allow_html=True)
    if data.assets.empty:
        st.info("暂无资产快照。")
    else:
        frame = data.assets.copy().sort_values("snapshot_time")
        frame["snapshot_time"] = frame["snapshot_time"].dt.strftime("%Y-%m-%d")
        show_cols = [col for col in ["snapshot_time", "total_asset", "cash", "total_pnl", "turnover", "max_drawdown"] if col in frame.columns]
        st.dataframe(frame[show_cols], use_container_width=True, hide_index=True, height=380)
    st.markdown("</div>", unsafe_allow_html=True)


def render_joinquant_perf(settings: AppSettings, info: dict[str, Any], data: DashboardData, probe: dict[str, Any] | None, alerts_items: list[dict[str, str]]) -> None:
    st.markdown('<div class="jq-content"><div class="jq-section-title">性能分析</div>', unsafe_allow_html=True)
    left, right = st.columns([1.2, 0.8])
    with left:
        render_status(settings, info, data, probe)
    with right:
        render_alerts(alerts_items)
    st.markdown("</div>", unsafe_allow_html=True)


def main(default_config_path: str | None = None) -> None:
    st.set_page_config(page_title="JoinQuant 风格量化平台", layout="wide")
    inject_styles()
    default_profile = next((name for name, path in CONFIGS.items() if default_config_path and Path(default_config_path).resolve() == path.resolve()), "????")
    profile, settings, config_path = render_sidebar(default_profile)
    auto_refresh_enabled, auto_refresh_interval = render_refresh_controls()
    data = load_dashboard_data(settings.database_url, settings.report_dir)
    probe = None
    if settings.environment == Environment.LIVE:
        try:
            probe = load_live_probe(config_path)
        except QmtUnavailableError as exc:
            st.sidebar.warning(f"QMT ????: {exc}")
    info = overview(data)
    alert_items = alerts(settings, info, data, probe)
    if settings.environment == Environment.BACKTEST:
        render_joinquant_topbar(settings, info)
    else:
        render_hero(settings, profile, info, probe)
        render_kpis(info)

    if settings.environment == Environment.BACKTEST:
        layout_left, layout_right = st.columns([0.18, 1])
        with layout_left:
            st.markdown('<div class="jq-side">', unsafe_allow_html=True)
            section = st.radio(
                "回测导航",
                ["收益概述", "交易详情", "每日持仓&收益", "日志输出", "性能分析", "策略代码", "Qlib 全市场", "形态实验室"],
                label_visibility="collapsed",
            )
            st.markdown("---")
            st.caption("可在这里切换与聚宽类似的回测详情视图。")
            st.markdown("</div>", unsafe_allow_html=True)
        with layout_right:
            if section == "收益概述":
                render_joinquant_overview(data, info)
            elif section == "交易详情":
                render_joinquant_trade_detail(data)
            elif section == "每日持仓&收益":
                render_joinquant_daily_pnl(data)
            elif section == "日志输出":
                st.markdown('<div class="jq-content"><div class="jq-section-title">日志输出</div>', unsafe_allow_html=True)
                render_logs()
                st.markdown("</div>", unsafe_allow_html=True)
            elif section == "性能分析":
                render_joinquant_perf(settings, info, data, probe, alert_items)
            elif section == "策略代码":
                st.markdown('<div class="jq-content"><div class="jq-section-title">策略代码与运行</div>', unsafe_allow_html=True)
                render_controls(settings)
                st.markdown("</div>", unsafe_allow_html=True)
            elif section == "Qlib 全市场":
                st.markdown('<div class="jq-content"><div class="jq-section-title">Qlib 全市场回测</div>', unsafe_allow_html=True)
                render_qlib_panel()
                st.markdown("</div>", unsafe_allow_html=True)
            else:
                st.markdown('<div class="jq-content"><div class="jq-section-title">形态实验室</div>', unsafe_allow_html=True)
                render_user_pattern_panel()
                st.markdown("</div>", unsafe_allow_html=True)
    else:
        tab_overview, tab_ops = st.tabs(["Overview", "Ops"])
        with tab_overview:
            left, right = st.columns([1.35, 1])
            with left:
                render_equity(data)
                render_positions(data)
            with right:
                render_status(settings, info, data, probe)
                render_alerts(alert_items)
                render_market(settings, probe)
            render_controls(settings)
        with tab_ops:
            render_logs()
            render_tabs(data)
    if auto_refresh_enabled:
        st.caption(f"准实时刷新已开启：每 {auto_refresh_interval} 秒自动轮询最新快照和报告。")
        time.sleep(auto_refresh_interval)
        st.rerun()


if __name__ == "__main__":
    main()
