from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd
from tqdm.auto import tqdm

from _bootstrap import ROOT, SRC

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from quant_demo.core.config import load_app_settings

import run_user_pattern_backtests as backtest_script


LOGGER = logging.getLogger(__name__)


def _resolve_runtime_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return (ROOT / path).resolve()


def _configure_logging(log_path: str) -> Path:
    resolved = Path(log_path)
    if not resolved.is_absolute():
        resolved = (ROOT / resolved).resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(resolved, mode="w", encoding="utf-8"),
        ],
        force=True,
    )
    return resolved


def _load_signal_frame(
    *,
    app_settings,
    user_module,
    start_time: str,
    end_time: str,
    b1_score_file: str,
) -> tuple[pd.DataFrame, str, str]:
    warmup_start = backtest_script._resolve_warmup_start(app_settings, start_time, warmup_bars=114)
    LOGGER.info("开始加载日选股所需信号: warmup_start=%s start=%s end=%s", warmup_start, start_time, end_time)
    ohlcv = backtest_script._load_local_ohlcv(app_settings, "all", warmup_start, end_time)
    LOGGER.info("历史数据加载完成: rows=%s", len(ohlcv))
    features = user_module.build_indicators(ohlcv)
    LOGGER.info("指标计算完成: rows=%s columns=%s", len(features), len(features.columns))
    signal = user_module.build_pattern_signals(features)
    LOGGER.info("信号生成完成: rows=%s columns=%s", len(signal), len(signal.columns))

    score_loader = getattr(user_module, "load_b1_model_scores", None)
    score_applier = getattr(user_module, "apply_b1_model_scores", None)
    if callable(score_applier):
        score_frame = None
        if b1_score_file:
            if callable(score_loader):
                score_frame = score_loader(b1_score_file)
            else:
                score_frame = pd.read_parquet(b1_score_file) if str(b1_score_file).lower().endswith(".parquet") else pd.read_csv(b1_score_file)
        signal = score_applier(signal, score_frame)

    signal_frame = signal.reset_index().copy()
    signal_frame["instrument"] = signal_frame["instrument"].astype(str)
    signal_frame["datetime"] = pd.to_datetime(signal_frame["datetime"]).dt.normalize()
    b1_env_ok, env_symbol = backtest_script._build_b1_env_filter(app_settings, warmup_start, end_time)
    signal_frame["b1_env_ok"] = signal_frame["datetime"].map(b1_env_ok).fillna(False).astype(int)
    signal_frame = signal_frame.sort_values(["datetime", "priority_score", "instrument"], ascending=[True, False, True])
    LOGGER.info("信号准备完成: env_symbol=%s rows=%s", env_symbol, len(signal_frame))
    return signal_frame, env_symbol, warmup_start


def _frame_to_rows(
    frame: pd.DataFrame,
    *,
    trading_date: pd.Timestamp,
    candidate_count: int,
    selected_count: int,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for rank, (instrument, row) in enumerate(frame.iterrows(), start=1):
        rows.append(
            {
                "trading_date": trading_date.date().isoformat(),
                "rank_in_day": rank,
                "candidate_count": int(candidate_count),
                "selected_count": int(selected_count),
                "instrument": str(instrument),
                "stock_code": backtest_script._to_qmt_symbol(str(instrument)),
                "priority_score": float(row.get("priority_score", float("nan"))),
                "rule_priority_score": float(row.get("rule_priority_score", float("nan"))),
                "quality_score": float(row.get("quality_score", float("nan"))),
                "model_score_raw": float(row.get("model_score_raw", float("nan"))),
                "model_score": float(row.get("model_score", float("nan"))),
                "final_score": float(row.get("final_score", row.get("priority_score", float("nan")))),
                "score_source": str(row.get("score_source", "")),
                "b1_confirm": int(row.get("b1_confirm", 0) or 0),
                "b1_env_ok": int(row.get("b1_env_ok", 0) or 0),
            }
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Export daily B1 picks and scores")
    parser.add_argument("--config", required=True)
    parser.add_argument("--strategy-file", default=str(ROOT / "strategy" / "strategy.py"))
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--b1-score-file", default="")
    parser.add_argument("--output-dir", default=str(ROOT / "data" / "reports" / "b1_daily_selection"))
    parser.add_argument("--skip-st-filter", action="store_true")
    parser.add_argument("--log-file", default="data/reports/b1_daily_selection.log")
    parser.add_argument("--progress", dest="progress", action="store_true", default=True)
    parser.add_argument("--no-progress", dest="progress", action="store_false")
    args = parser.parse_args()
    log_path = _configure_logging(args.log_file)
    LOGGER.info(
        "开始导出 B1 日频选股: config=%s start=%s end=%s output_dir=%s",
        args.config,
        args.start,
        args.end,
        args.output_dir,
    )
    LOGGER.info("日志文件: %s", log_path)

    app_settings = load_app_settings(args.config)
    qlib_source_dir = _resolve_runtime_path(app_settings.qlib_source_dir)
    if str(qlib_source_dir) not in sys.path:
        sys.path.insert(0, str(qlib_source_dir))
    user_module = backtest_script.load_user_module(Path(args.strategy_file))
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    signal_frame, env_symbol, warmup_start = _load_signal_frame(
        app_settings=app_settings,
        user_module=user_module,
        start_time=args.start,
        end_time=args.end,
        b1_score_file=args.b1_score_file,
    )
    grouped_frames = {
        trading_date: frame.set_index("instrument", drop=False)
        for trading_date, frame in signal_frame.groupby("datetime", sort=True)
    }
    trade_dates = [
        trading_date
        for trading_date in sorted(grouped_frames.keys())
        if pd.Timestamp(args.start).normalize() <= trading_date <= pd.Timestamp(args.end).normalize()
    ]
    blocked_symbols: set[str] = set()
    if not args.skip_st_filter:
        blocked_symbols = backtest_script.resolve_st_symbols(
            app_settings,
            signal_frame["instrument"].dropna().astype(str).unique().tolist(),
        )
    LOGGER.info("开始按交易日导出: trade_dates=%s blocked_symbols=%s", len(trade_dates), len(blocked_symbols))

    candidate_rows: list[dict[str, object]] = []
    selected_rows: list[dict[str, object]] = []
    summary_rows: list[dict[str, object]] = []

    progress = tqdm(
        trade_dates,
        total=len(trade_dates),
        desc="B1 daily selection",
        dynamic_ncols=True,
        leave=True,
        disable=not args.progress,
    )
    for trading_date in progress:
        day_df = grouped_frames.get(trading_date, backtest_script._empty_signal_frame(signal_frame))
        candidate_df = backtest_script._select_candidates(day_df, "B1", set(), blocked_codes=blocked_symbols)
        selector = getattr(user_module, "select_b1_probe_candidates", None)
        selected_df = selector(candidate_df) if callable(selector) else candidate_df.copy()

        candidate_count = int(len(candidate_df))
        selected_count = int(len(selected_df))
        candidate_rows.extend(
            _frame_to_rows(
                candidate_df,
                trading_date=trading_date,
                candidate_count=candidate_count,
                selected_count=selected_count,
            )
        )
        selected_rows.extend(
            _frame_to_rows(
                selected_df,
                trading_date=trading_date,
                candidate_count=candidate_count,
                selected_count=selected_count,
            )
        )
        summary_rows.append(
            {
                "trading_date": trading_date.date().isoformat(),
                "warmup_start": warmup_start,
                "env_symbol": env_symbol,
                "candidate_count": candidate_count,
                "selected_count": selected_count,
                "candidate_symbols": ",".join(backtest_script._to_qmt_symbol(str(code)) for code in candidate_df.index.tolist()),
                "selected_symbols": ",".join(backtest_script._to_qmt_symbol(str(code)) for code in selected_df.index.tolist()),
            }
        )
        if args.progress:
            progress.set_postfix(
                date=trading_date.date().isoformat(),
                candidate=candidate_count,
                selected=selected_count,
            )
    progress.close()

    candidate_path = output_dir / "b1_daily_candidate_scores.csv"
    selected_path = output_dir / "b1_daily_selected_scores.csv"
    summary_path = output_dir / "b1_daily_selection_summary.csv"
    pd.DataFrame(candidate_rows).to_csv(candidate_path, index=False, encoding="utf-8-sig")
    pd.DataFrame(selected_rows).to_csv(selected_path, index=False, encoding="utf-8-sig")
    pd.DataFrame(summary_rows).to_csv(summary_path, index=False, encoding="utf-8-sig")

    payload = {
        "candidate_path": str(candidate_path),
        "selected_path": str(selected_path),
        "summary_path": str(summary_path),
        "trade_date_count": int(len(trade_dates)),
        "candidate_row_count": int(len(candidate_rows)),
        "selected_row_count": int(len(selected_rows)),
        "env_symbol": env_symbol,
        "warmup_start": warmup_start,
        "used_score_file": str(args.b1_score_file or ""),
    }
    LOGGER.info("B1 日频选股导出完成: %s", payload)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
