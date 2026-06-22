from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


def _safe_slug(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", value).strip("_").lower()
    return slug or "backtest"


def _extract_curve_dates(equity_curve: pd.DataFrame) -> tuple[str, str, int]:
    if equity_curve.empty:
        return "", "", 0

    if "trading_date" in equity_curve.columns:
        series = pd.to_datetime(equity_curve["trading_date"])
    else:
        series = pd.to_datetime(equity_curve.index)

    return str(series.iloc[0].date()), str(series.iloc[-1].date()), int(len(equity_curve))


def build_summary_payload(
    *,
    strategy_name: str,
    config_path: str,
    strategy_path: str,
    report_path: str,
    total_return: float,
    annualized_return: float,
    max_drawdown: float,
    turnover: float,
    equity_curve: pd.DataFrame,
    elapsed_seconds: float | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    start_date, end_date, trading_days = _extract_curve_dates(equity_curve)
    initial_equity = float(equity_curve["equity"].iloc[0]) if not equity_curve.empty and "equity" in equity_curve.columns else 0.0
    final_equity = float(equity_curve["equity"].iloc[-1]) if not equity_curve.empty and "equity" in equity_curve.columns else 0.0

    payload: dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "strategy": strategy_name,
        "config_path": config_path,
        "strategy_path": strategy_path,
        "report_path": report_path,
        "time_range": {
            "start_date": start_date,
            "end_date": end_date,
            "trading_days": trading_days,
        },
        "metrics": {
            "total_return": total_return,
            "annualized_return": annualized_return,
            "max_drawdown": max_drawdown,
            "turnover": turnover,
        },
        "equity": {
            "initial": initial_equity,
            "final": final_equity,
        },
    }
    if elapsed_seconds is not None:
        payload["elapsed_seconds"] = round(float(elapsed_seconds), 2)
    if extra:
        payload["extra"] = extra
    return payload


def _render_markdown(payload: dict[str, Any]) -> str:
    metrics = payload["metrics"]
    time_range = payload["time_range"]
    equity = payload["equity"]
    lines = [
        "# AI Backtest Summary",
        "",
        f"- 生成时间: {payload['generated_at']}",
        f"- 策略: {payload['strategy']}",
        f"- 配置: `{payload['config_path']}`",
        f"- 策略文件: `{payload['strategy_path']}`",
        f"- 审计报告: `{payload['report_path']}`",
        f"- 回测区间: {time_range['start_date']} ~ {time_range['end_date']}",
        f"- 交易日数: {time_range['trading_days']}",
        f"- 初始权益: {equity['initial']:.2f}",
        f"- 期末权益: {equity['final']:.2f}",
        f"- 总收益率: {metrics['total_return']:.2%}",
        f"- 年化收益率: {metrics['annualized_return']:.2%}",
        f"- 最大回撤: {metrics['max_drawdown']:.2%}",
        f"- 换手总额: {metrics['turnover']:,.2f}",
    ]

    elapsed_seconds = payload.get("elapsed_seconds")
    if elapsed_seconds is not None:
        lines.append(f"- 运行耗时: {elapsed_seconds:.2f}s")

    extra = payload.get("extra") or {}
    if extra:
        lines.append("")
        lines.append("## Extra")
        lines.append("")
        for key, value in extra.items():
            lines.append(f"- {key}: {value}")

    lines.extend(
        [
            "",
            "## Model Read Order",
            "",
            "1. 优先读取此文件或同目录的 `latest_*.json`。",
            "2. 只有当摘要不足以回答问题时，才继续读取详细报告或完整日志。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_summary_files(payload: dict[str, Any]) -> dict[str, str]:
    report_path = Path(payload["report_path"])
    summary_dir = report_path.parent / "ai_summaries"
    summary_dir.mkdir(parents=True, exist_ok=True)

    slug = _safe_slug(payload["strategy"])
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    version_json_path = summary_dir / f"{stamp}_{slug}.json"
    version_md_path = summary_dir / f"{stamp}_{slug}.md"
    latest_json_path = summary_dir / f"latest_{slug}.json"
    latest_md_path = summary_dir / f"latest_{slug}.md"

    enriched_payload = dict(payload)
    enriched_payload["summary_json_path"] = str(latest_json_path)
    enriched_payload["summary_markdown_path"] = str(latest_md_path)
    enriched_payload["summary_version_json_path"] = str(version_json_path)
    enriched_payload["summary_version_markdown_path"] = str(version_md_path)

    markdown = _render_markdown(enriched_payload)
    json_text = json.dumps(enriched_payload, ensure_ascii=False, indent=2)

    for path in [version_json_path, latest_json_path]:
        path.write_text(json_text + "\n", encoding="utf-8")
    for path in [version_md_path, latest_md_path]:
        path.write_text(markdown, encoding="utf-8")

    return {
        "summary_dir": str(summary_dir),
        "summary_json_path": str(latest_json_path),
        "summary_markdown_path": str(latest_md_path),
        "summary_version_json_path": str(version_json_path),
        "summary_version_markdown_path": str(version_md_path),
    }
