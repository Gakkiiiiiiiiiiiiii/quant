from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd
from sklearn.base import clone
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline

from _bootstrap import ROOT, SRC

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from quant_demo.core.config import load_app_settings


def load_user_module(module_path: Path, qlib_source_dir: Path):
    if str(qlib_source_dir) not in sys.path:
        sys.path.insert(0, str(qlib_source_dir))
    spec = importlib.util.spec_from_file_location("user_pattern_strategy_rank", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"无法加载策略模块: {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _resolve_runtime_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return (ROOT / path).resolve()


def _load_local_ohlcv(app_settings, start_time: str, end_time: str) -> pd.DataFrame:
    history_path = _resolve_runtime_path(app_settings.history_parquet)
    frame = pd.read_parquet(
        history_path,
        columns=["trading_date", "symbol", "open", "high", "low", "close", "volume", "amount"],
    )
    frame["trading_date"] = pd.to_datetime(frame["trading_date"]).dt.normalize()
    start_dt = pd.Timestamp(start_time).normalize()
    end_dt = pd.Timestamp(end_time).normalize()
    frame = frame[(frame["trading_date"] >= start_dt) & (frame["trading_date"] <= end_dt)].copy()
    if frame.empty:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume", "amount"])
    frame = frame.rename(columns={"symbol": "instrument", "trading_date": "datetime"})
    frame = frame.sort_values(["instrument", "datetime"]).set_index(["instrument", "datetime"])
    return frame[["open", "high", "low", "close", "volume", "amount"]]


def _build_regressor(backend: str) -> tuple[Pipeline, str]:
    backend_name = str(backend or "auto").lower()
    if backend_name in {"auto", "lightgbm"}:
        try:
            from lightgbm import LGBMRegressor

            model = Pipeline(
                [
                    ("imputer", SimpleImputer(strategy="median")),
                    (
                        "model",
                        LGBMRegressor(
                            objective="regression",
                            n_estimators=300,
                            learning_rate=0.05,
                            num_leaves=31,
                            subsample=0.9,
                            colsample_bytree=0.9,
                            random_state=42,
                        ),
                    ),
                ]
            )
            return model, "lightgbm"
        except ImportError:
            if backend_name == "lightgbm":
                raise
    model = Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            (
                "model",
                HistGradientBoostingRegressor(
                    learning_rate=0.05,
                    max_depth=6,
                    max_iter=250,
                    min_samples_leaf=20,
                    random_state=42,
                ),
            ),
        ]
    )
    return model, "hist_gbdt"


def _score_ic(label: pd.Series, pred: pd.Series) -> float | None:
    if label.empty or pred.empty:
        return None
    value = label.corr(pred, method="spearman")
    if pd.isna(value):
        return None
    return float(value)


def _walk_forward_predict(
    events: pd.DataFrame,
    feature_columns: list[str],
    *,
    backend: str,
    train_months: int,
    valid_months: int,
    test_months: int,
    step_months: int,
    embargo_days: int,
    min_train_rows: int,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    event_frame = events.copy()
    event_frame["datetime"] = pd.to_datetime(event_frame["datetime"]).dt.normalize()
    event_frame = event_frame.sort_values(["datetime", "instrument"]).reset_index(drop=True)
    unique_dates = pd.DatetimeIndex(sorted(event_frame["datetime"].dropna().unique()))
    if len(unique_dates) == 0:
        return pd.DataFrame(), pd.DataFrame(), "none"

    model_template, backend_name = _build_regressor(backend)
    prediction_frames: list[pd.DataFrame] = []
    window_rows: list[dict[str, object]] = []
    test_start = unique_dates.min() + pd.DateOffset(months=int(train_months + valid_months))

    while test_start <= unique_dates.max():
        valid_start = test_start - pd.DateOffset(months=int(valid_months))
        train_start = valid_start - pd.DateOffset(months=int(train_months))
        test_end = test_start + pd.DateOffset(months=int(test_months)) - pd.Timedelta(days=1)
        embargo_cutoff = test_start - pd.Timedelta(days=int(embargo_days))

        train_mask = (event_frame["datetime"] >= train_start) & (event_frame["datetime"] < valid_start)
        valid_mask = (event_frame["datetime"] >= valid_start) & (event_frame["datetime"] < embargo_cutoff)
        fit_mask = (event_frame["datetime"] >= train_start) & (event_frame["datetime"] < embargo_cutoff)
        test_mask = (event_frame["datetime"] >= test_start) & (event_frame["datetime"] <= test_end)

        train_count = int(train_mask.sum())
        valid_count = int(valid_mask.sum())
        test_count = int(test_mask.sum())
        if train_count < int(min_train_rows) or test_count == 0:
            test_start = test_start + pd.DateOffset(months=int(step_months))
            continue

        train_x = event_frame.loc[train_mask, feature_columns]
        train_y = event_frame.loc[train_mask, "label_rank"]
        valid_x = event_frame.loc[valid_mask, feature_columns]
        valid_y = event_frame.loc[valid_mask, "label_rank"]
        fit_x = event_frame.loc[fit_mask, feature_columns]
        fit_y = event_frame.loc[fit_mask, "label_rank"]
        test_x = event_frame.loc[test_mask, feature_columns]
        test_y = event_frame.loc[test_mask, "label_rank"]

        valid_model = clone(model_template)
        valid_model.fit(train_x, train_y)
        valid_pred = pd.Series(dtype=float)
        if valid_count > 0:
            valid_pred = pd.Series(valid_model.predict(valid_x), index=valid_x.index, dtype=float)

        test_model = clone(model_template)
        test_model.fit(fit_x, fit_y)
        test_pred = pd.Series(test_model.predict(test_x), index=test_x.index, dtype=float)

        pred_frame = event_frame.loc[test_mask, ["instrument", "datetime", "label_rank", "label_cls"]].copy()
        pred_frame["model_score_raw"] = test_pred.to_numpy(dtype=float)
        prediction_frames.append(pred_frame)

        window_rows.append(
            {
                "train_start": pd.Timestamp(train_start).date().isoformat(),
                "valid_start": pd.Timestamp(valid_start).date().isoformat(),
                "test_start": pd.Timestamp(test_start).date().isoformat(),
                "test_end": pd.Timestamp(min(test_end, unique_dates.max())).date().isoformat(),
                "train_rows": train_count,
                "valid_rows": valid_count,
                "test_rows": test_count,
                "valid_ic": _score_ic(valid_y, valid_pred),
                "test_ic": _score_ic(test_y, test_pred),
            }
        )
        test_start = test_start + pd.DateOffset(months=int(step_months))

    if not prediction_frames:
        return pd.DataFrame(), pd.DataFrame(window_rows), backend_name

    predictions = pd.concat(prediction_frames, ignore_index=True)
    predictions = predictions.sort_values(["datetime", "instrument"]).drop_duplicates(["instrument", "datetime"], keep="last")
    return predictions, pd.DataFrame(window_rows), backend_name


def main() -> None:
    parser = argparse.ArgumentParser(description="Build B1 rank scores with walk-forward training")
    parser.add_argument("--config", required=True)
    parser.add_argument("--strategy-file", default=str(ROOT / "strategy" / "strategy.py"))
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output-dir", default=str(ROOT / "data" / "reports" / "b1_rank"))
    parser.add_argument("--backend", default="auto", choices=["auto", "lightgbm", "hist_gbdt"])
    parser.add_argument("--train-months", type=int, default=24)
    parser.add_argument("--valid-months", type=int, default=6)
    parser.add_argument("--test-months", type=int, default=3)
    parser.add_argument("--step-months", type=int, default=3)
    parser.add_argument("--embargo-days", type=int, default=10)
    parser.add_argument("--min-train-rows", type=int, default=200)
    args = parser.parse_args()

    app_settings = load_app_settings(args.config)
    qlib_source_dir = _resolve_runtime_path(app_settings.qlib_source_dir)
    user_module = load_user_module(Path(args.strategy_file), qlib_source_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    ohlcv = _load_local_ohlcv(app_settings, args.start, args.end)
    features = user_module.build_indicators(ohlcv)
    signal = user_module.build_pattern_signals(features)
    event_builder = getattr(user_module, "build_b1_rank_event_frame", None)
    if not callable(event_builder):
        raise AttributeError("策略模块缺少 build_b1_rank_event_frame，无法生成 B1 事件样本。")
    events = event_builder(features, signal)
    if events.empty:
        raise RuntimeError("当前区间没有可用的 B1 事件样本。")

    feature_columns = list(getattr(user_module, "B1_RANK_FEATURE_COLUMNS", []))
    if not feature_columns:
        raise AttributeError("策略模块缺少 B1_RANK_FEATURE_COLUMNS。")

    predictions, window_df, backend_name = _walk_forward_predict(
        events,
        feature_columns,
        backend=args.backend,
        train_months=args.train_months,
        valid_months=args.valid_months,
        test_months=args.test_months,
        step_months=args.step_months,
        embargo_days=args.embargo_days,
        min_train_rows=args.min_train_rows,
    )
    if predictions.empty:
        raise RuntimeError("没有生成任何滚动测试分数，请扩大时间区间或降低最小训练样本要求。")

    score_path = output_dir / "b1_model_scores.parquet"
    event_path = output_dir / "b1_events.parquet"
    window_path = output_dir / "b1_windows.csv"
    summary_path = output_dir / "summary.json"
    events.to_parquet(event_path, index=False)
    predictions.loc[:, ["instrument", "datetime", "model_score_raw"]].to_parquet(score_path, index=False)
    window_df.to_csv(window_path, index=False, encoding="utf-8-sig")

    summary = {
        "backend": backend_name,
        "event_count": int(len(events)),
        "score_count": int(len(predictions)),
        "feature_count": int(len(feature_columns)),
        "score_start": pd.Timestamp(predictions["datetime"].min()).date().isoformat(),
        "score_end": pd.Timestamp(predictions["datetime"].max()).date().isoformat(),
        "mean_test_ic": float(window_df["test_ic"].dropna().mean()) if not window_df.empty and window_df["test_ic"].notna().any() else None,
        "event_path": str(event_path),
        "score_path": str(score_path),
        "window_path": str(window_path),
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({**summary, "summary_path": str(summary_path)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
