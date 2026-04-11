from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import os
import re
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.base import clone
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from tqdm.auto import tqdm

from _bootstrap import ROOT, SRC

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from quant_demo.core.config import load_app_settings


LOGGER = logging.getLogger(__name__)


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
    LOGGER.info("加载历史数据: path=%s start=%s end=%s", history_path, start_time, end_time)
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
    LOGGER.info(
        "历史数据加载完成: rows=%s symbols=%s latest=%s",
        len(frame),
        int(frame.index.get_level_values("instrument").nunique()) if not frame.empty else 0,
        str(frame.index.get_level_values("datetime").max()) if not frame.empty else "",
    )
    return frame[["open", "high", "low", "close", "volume", "amount"]]


def _decode_payload(payload: bytes) -> str:
    for encoding in ("utf-8", "gb18030"):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    return payload.decode("utf-8", errors="ignore")


def _run_qmt_bridge_json(app_settings, command: str, extra_args: list[str]) -> dict[str, object]:
    python_path = _resolve_runtime_path(app_settings.qmt_bridge_python)
    script_path = (ROOT / "scripts" / "qmt_bridge.py").resolve()
    install_dir = _resolve_runtime_path(app_settings.qmt_install_dir)
    userdata_dir = _resolve_runtime_path(app_settings.qmt_userdata_dir)
    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "utf-8"
    cmd = [
        str(python_path),
        str(script_path),
        command,
        "--install-dir",
        str(install_dir),
        "--userdata-dir",
        str(userdata_dir),
        *extra_args,
    ]
    if getattr(app_settings, "qmt_account_id", ""):
        cmd.extend(["--account-id", str(app_settings.qmt_account_id)])
    completed = subprocess.run(cmd, cwd=ROOT, capture_output=True, check=False, env=env)
    if completed.returncode != 0:
        stderr = _decode_payload(completed.stderr).strip()
        raise RuntimeError(f"QMT 桥接命令失败: command={command} stderr={stderr}")
    payload = json.loads(_decode_payload(completed.stdout).strip())
    if not payload.get("ok"):
        raise RuntimeError(f"QMT 桥接返回失败: command={command} payload={payload}")
    return dict(payload.get("data") or {})


def _load_sample_specs(sample_path: Path) -> list[dict[str, object]]:
    text = sample_path.read_text(encoding="utf-8")
    rows: list[dict[str, object]] = []
    pattern = re.compile(r"^(?P<name>\S+)\s+(?P<date>\d{4}[.\-/]\d{2}[.\-/]\d{2})\s*$")
    for order, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        match = pattern.match(line)
        if match is None:
            continue
        rows.append(
            {
                "sample_order": len(rows) + 1,
                "sample_name": match.group("name").strip(),
                "sample_date": pd.Timestamp(match.group("date").replace(".", "-").replace("/", "-")).normalize(),
                "source_line": order,
            }
        )
    return rows


def _normalize_instrument_name(value: object) -> str:
    raw = str(value or "").strip().upper()
    return re.sub(r"[\s\-—_]+", "", raw)


def _match_sample_name(sample_name: str, instrument_name: str) -> bool:
    sample_norm = _normalize_instrument_name(sample_name)
    instrument_norm = _normalize_instrument_name(instrument_name)
    if not sample_norm or not instrument_norm:
        return False
    return (
        sample_norm == instrument_norm
        or sample_norm in instrument_norm
        or instrument_norm in sample_norm
    )


def _resolve_sample_symbol_map(app_settings, sample_names: list[str], *, batch_size: int = 200) -> dict[str, str]:
    if not sample_names:
        return {}
    sector_name = str(getattr(app_settings, "history_universe_sector", "") or "沪深京A股")
    sector_payload = _run_qmt_bridge_json(
        app_settings,
        "sector-members",
        ["--sector-name", sector_name],
    )
    symbols = [str(symbol) for symbol in sector_payload.get("symbols", []) if symbol]
    if not symbols:
        raise RuntimeError(f"未能从 QMT 获取股票池成员: sector={sector_name}")
    mapping: dict[str, str] = {}
    pending = list(dict.fromkeys(str(name).strip() for name in sample_names if str(name).strip()))
    for offset in range(0, len(symbols), max(int(batch_size), 1)):
        batch = symbols[offset : offset + max(int(batch_size), 1)]
        detail_payload = _run_qmt_bridge_json(
            app_settings,
            "instrument-detail",
            ["--symbols", ",".join(batch)],
        )
        details = dict(detail_payload.get("details") or {})
        for symbol, item in details.items():
            instrument_name = (
                item.get("InstrumentName")
                or item.get("instrument_name")
                or item.get("name")
                or ""
            )
            for sample_name in pending:
                if sample_name in mapping:
                    continue
                if _match_sample_name(sample_name, str(instrument_name)):
                    mapping[sample_name] = str(symbol)
        if len(mapping) >= len(pending):
            break
    return mapping


def _resolve_nearest_trade_date(
    trading_dates: pd.Index | pd.DatetimeIndex,
    sample_date: pd.Timestamp,
    *,
    max_gap_days: int,
) -> tuple[pd.Timestamp | None, int | None, str]:
    unique_dates = pd.DatetimeIndex(pd.to_datetime(trading_dates).normalize().unique()).sort_values()
    if unique_dates.empty:
        return None, None, "no_history"
    target = pd.Timestamp(sample_date).normalize()
    position = int(unique_dates.searchsorted(target, side="left"))
    candidates: list[pd.Timestamp] = []
    if position < len(unique_dates):
        candidates.append(pd.Timestamp(unique_dates[position]).normalize())
    if position > 0:
        candidates.append(pd.Timestamp(unique_dates[position - 1]).normalize())
    if not candidates:
        return None, None, "no_candidate_date"
    best = min(
        candidates,
        key=lambda value: (
            abs((value - target).days),
            0 if value >= target else 1,
        ),
    )
    delta_days = abs((best - target).days)
    if delta_days > int(max_gap_days):
        return None, delta_days, "date_gap_too_large"
    if delta_days == 0:
        return best, 0, "exact"
    if best >= target:
        return best, delta_days, "next_trade"
    return best, delta_days, "prev_trade"


def _build_b1_sample_training_frame(
    *,
    user_module,
    app_settings,
    features: pd.DataFrame,
    sample_path: Path,
    max_gap_days: int,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, str]]:
    sample_specs = _load_sample_specs(sample_path)
    if not sample_specs:
        raise RuntimeError(f"样本文件中没有解析到任何“名称 + 日期”样本: {sample_path}")
    sample_names = [str(item["sample_name"]) for item in sample_specs]
    sample_symbol_map = _resolve_sample_symbol_map(app_settings, sample_names)
    available_instruments = set(features.index.get_level_values("instrument").astype(str).unique().tolist())
    per_instrument_dates: dict[str, pd.DatetimeIndex] = {}
    for instrument, group in features.groupby(level="instrument", sort=False):
        per_instrument_dates[str(instrument)] = pd.DatetimeIndex(group.index.get_level_values("datetime"))

    match_rows: list[dict[str, object]] = []
    matched_pairs: list[tuple[str, pd.Timestamp]] = []
    for item in sample_specs:
        sample_name = str(item["sample_name"])
        sample_date = pd.Timestamp(item["sample_date"]).normalize()
        symbol = sample_symbol_map.get(sample_name)
        if not symbol:
            match_rows.append(
                {
                    **item,
                    "instrument": "",
                    "matched_date": pd.NaT,
                    "date_shift_days": None,
                    "match_type": "symbol_unresolved",
                    "status": "symbol_unresolved",
                }
            )
            continue
        if symbol not in available_instruments:
            match_rows.append(
                {
                    **item,
                    "instrument": symbol,
                    "matched_date": pd.NaT,
                    "date_shift_days": None,
                    "match_type": "history_missing",
                    "status": "history_missing",
                }
            )
            continue
        matched_date, delta_days, match_type = _resolve_nearest_trade_date(
            per_instrument_dates.get(symbol, pd.DatetimeIndex([])),
            sample_date,
            max_gap_days=max_gap_days,
        )
        if matched_date is None:
            match_rows.append(
                {
                    **item,
                    "instrument": symbol,
                    "matched_date": pd.NaT,
                    "date_shift_days": delta_days,
                    "match_type": match_type,
                    "status": "date_unresolved",
                }
            )
            continue
        matched_pairs.append((symbol, matched_date))
        match_rows.append(
            {
                **item,
                "instrument": symbol,
                "matched_date": matched_date,
                "date_shift_days": int(delta_days or 0),
                "match_type": match_type,
                "status": "matched",
            }
        )

    match_df = pd.DataFrame(match_rows)
    if not matched_pairs:
        raise RuntimeError(f"样本文件没有任何成功匹配到本地历史数据的样本: {sample_path}")

    target_instruments = sorted({instrument for instrument, _ in matched_pairs})
    subset_mask = features.index.get_level_values("instrument").astype(str).isin(target_instruments)
    sample_features = features.loc[subset_mask].copy()
    synthetic_signal = pd.DataFrame({"b1": 0}, index=sample_features.index)
    for instrument, matched_date in matched_pairs:
        key = (str(instrument), pd.Timestamp(matched_date).normalize())
        if key in synthetic_signal.index:
            synthetic_signal.loc[key, "b1"] = 1

    candidate_builder = getattr(user_module, "build_b1_rank_candidate_frame", None)
    if not callable(candidate_builder):
        raise AttributeError("策略模块缺少 build_b1_rank_candidate_frame，无法构建样本特征。")
    sample_frame = candidate_builder(sample_features, synthetic_signal)
    if sample_frame.empty:
        raise RuntimeError("样本匹配成功，但未能从样本日期生成任何 B1 特征行。")

    meta_frame = (
        match_df.loc[match_df["status"].eq("matched")].copy().rename(columns={"matched_date": "datetime"})
    )
    meta_frame["datetime"] = pd.to_datetime(meta_frame["datetime"]).dt.normalize()
    sample_frame = meta_frame.merge(sample_frame, on=["instrument", "datetime"], how="left").sort_values(
        ["sample_order", "datetime", "instrument"]
    ).reset_index(drop=True)

    feature_columns = list(getattr(user_module, "B1_RANK_FEATURE_COLUMNS", []))
    if feature_columns and sample_frame[feature_columns].isna().all(axis=1).any():
        raise RuntimeError("部分样本未能生成有效特征，请检查样本匹配日期是否正确。")
    return sample_frame, match_df, sample_symbol_map


def _robust_center_scale(values: np.ndarray) -> tuple[float, float]:
    numeric = np.asarray(values, dtype=float)
    valid = numeric[np.isfinite(numeric)]
    if valid.size == 0:
        return 0.0, 1.0
    center = float(np.median(valid))
    mad = float(np.median(np.abs(valid - center))) * 1.4826
    std = float(valid.std(ddof=0))
    scale = mad if mad > 1e-6 else std
    if not np.isfinite(scale) or scale <= 1e-6:
        scale = 1.0
    return center, scale


def _score_candidates_from_sample_library(
    sample_frame: pd.DataFrame,
    candidate_frame: pd.DataFrame,
    feature_columns: list[str],
    *,
    top_k: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if sample_frame.empty:
        raise RuntimeError("样本训练集为空，无法进行样本相似度打分。")
    if candidate_frame.empty:
        return pd.DataFrame(columns=["instrument", "datetime", "model_score_raw"]), pd.DataFrame()
    if not feature_columns:
        raise RuntimeError("特征列为空，无法进行样本相似度打分。")

    sample_work = sample_frame.copy()
    candidate_work = candidate_frame.copy()
    sample_feature_blocks: list[np.ndarray] = []
    candidate_feature_blocks: list[np.ndarray] = []
    feature_rows: list[dict[str, object]] = []
    sample_spreads: list[float] = []
    weight_values: list[float] = []

    for column in feature_columns:
        candidate_values = pd.to_numeric(candidate_work[column], errors="coerce").to_numpy(dtype=float)
        sample_values = pd.to_numeric(sample_work[column], errors="coerce").to_numpy(dtype=float)
        bg_center, bg_scale = _robust_center_scale(candidate_values)
        candidate_filled = np.where(np.isfinite(candidate_values), candidate_values, bg_center)
        sample_filled = np.where(np.isfinite(sample_values), sample_values, bg_center)
        candidate_z = (candidate_filled - bg_center) / bg_scale
        sample_z = (sample_filled - bg_center) / bg_scale
        sample_center, sample_scale = _robust_center_scale(sample_z)
        sample_scale = max(sample_scale, 0.35)
        weight = (abs(sample_center) + 0.15) / sample_scale

        sample_feature_blocks.append(sample_z)
        candidate_feature_blocks.append(candidate_z)
        sample_spreads.append(sample_scale)
        weight_values.append(weight)
        feature_rows.append(
            {
                "feature": column,
                "background_center": bg_center,
                "background_scale": bg_scale,
                "sample_center_z": sample_center,
                "sample_scale_z": sample_scale,
                "weight_raw": weight,
                "sample_mean": float(np.nanmean(sample_values)) if np.isfinite(sample_values).any() else np.nan,
                "sample_median": float(np.nanmedian(sample_values)) if np.isfinite(sample_values).any() else np.nan,
            }
        )

    sample_matrix = np.column_stack(sample_feature_blocks)
    candidate_matrix = np.column_stack(candidate_feature_blocks)
    feature_spread = np.asarray(sample_spreads, dtype=float)
    feature_weight = np.asarray(weight_values, dtype=float)
    if (not np.isfinite(feature_weight).all()) or float(feature_weight.sum()) <= 0:
        feature_weight = np.ones(len(feature_columns), dtype=float)
    normalized_weight = feature_weight / feature_weight.sum()

    delta = (candidate_matrix[:, None, :] - sample_matrix[None, :, :]) / feature_spread[None, None, :]
    distance = np.sum((delta**2) * normalized_weight[None, None, :], axis=2)
    nearest_k = max(1, min(int(top_k), sample_matrix.shape[0]))
    nearest_distance = np.partition(distance, kth=nearest_k - 1, axis=1)[:, :nearest_k]
    raw_score = np.exp(-0.5 * nearest_distance.mean(axis=1))

    predictions = candidate_work.loc[:, ["instrument", "datetime"]].copy()
    predictions["model_score_raw"] = raw_score.astype(float)
    feature_profile = pd.DataFrame(feature_rows)
    if not feature_profile.empty:
        feature_profile["weight"] = feature_profile["weight_raw"] / feature_profile["weight_raw"].sum()
        feature_profile = feature_profile.sort_values("weight", ascending=False).reset_index(drop=True)
    return predictions, feature_profile


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
    candidates: pd.DataFrame,
    feature_columns: list[str],
    *,
    backend: str,
    train_months: int,
    valid_months: int,
    test_months: int,
    step_months: int,
    embargo_days: int,
    min_train_rows: int,
    show_progress: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    event_frame = events.copy()
    event_frame["datetime"] = pd.to_datetime(event_frame["datetime"]).dt.normalize()
    event_frame = event_frame.sort_values(["datetime", "instrument"]).reset_index(drop=True)
    candidate_frame = candidates.copy()
    candidate_frame["datetime"] = pd.to_datetime(candidate_frame["datetime"]).dt.normalize()
    candidate_frame = candidate_frame.sort_values(["datetime", "instrument"]).reset_index(drop=True)
    event_dates = pd.DatetimeIndex(sorted(event_frame["datetime"].dropna().unique()))
    candidate_dates = pd.DatetimeIndex(sorted(candidate_frame["datetime"].dropna().unique()))
    if len(event_dates) == 0 or len(candidate_dates) == 0:
        return pd.DataFrame(), pd.DataFrame(), "none"

    model_template, backend_name = _build_regressor(backend)
    prediction_frames: list[pd.DataFrame] = []
    window_rows: list[dict[str, object]] = []
    first_test_start = max(
        candidate_dates.min(),
        event_dates.min() + pd.DateOffset(months=int(train_months + valid_months)),
    )
    window_starts: list[pd.Timestamp] = []
    test_start = first_test_start
    while test_start <= candidate_dates.max():
        window_starts.append(pd.Timestamp(test_start))
        test_start = test_start + pd.DateOffset(months=int(step_months))

    progress = tqdm(
        window_starts,
        total=len(window_starts),
        desc="B1 rank windows",
        dynamic_ncols=True,
        leave=True,
        disable=not show_progress,
    )
    for test_start in progress:
        valid_start = test_start - pd.DateOffset(months=int(valid_months))
        train_start = valid_start - pd.DateOffset(months=int(train_months))
        test_end = test_start + pd.DateOffset(months=int(test_months)) - pd.Timedelta(days=1)
        embargo_cutoff = test_start - pd.Timedelta(days=int(embargo_days))

        train_mask = (event_frame["datetime"] >= train_start) & (event_frame["datetime"] < valid_start)
        valid_mask = (event_frame["datetime"] >= valid_start) & (event_frame["datetime"] < embargo_cutoff)
        fit_mask = (event_frame["datetime"] >= train_start) & (event_frame["datetime"] < embargo_cutoff)
        candidate_test_mask = (candidate_frame["datetime"] >= test_start) & (candidate_frame["datetime"] <= test_end)
        eval_test_mask = (event_frame["datetime"] >= test_start) & (event_frame["datetime"] <= test_end)

        train_count = int(train_mask.sum())
        valid_count = int(valid_mask.sum())
        candidate_test_count = int(candidate_test_mask.sum())
        eval_test_count = int(eval_test_mask.sum())
        if train_count < int(min_train_rows) or candidate_test_count == 0:
            if show_progress:
                progress.set_postfix(
                    test_start=pd.Timestamp(test_start).date().isoformat(),
                    train=train_count,
                    valid=valid_count,
                    candidate=candidate_test_count,
                )
            continue

        train_x = event_frame.loc[train_mask, feature_columns]
        train_y = event_frame.loc[train_mask, "label_rank"]
        valid_x = event_frame.loc[valid_mask, feature_columns]
        valid_y = event_frame.loc[valid_mask, "label_rank"]
        fit_x = event_frame.loc[fit_mask, feature_columns]
        fit_y = event_frame.loc[fit_mask, "label_rank"]
        candidate_test_x = candidate_frame.loc[candidate_test_mask, feature_columns]
        eval_test_x = event_frame.loc[eval_test_mask, feature_columns]
        eval_test_y = event_frame.loc[eval_test_mask, "label_rank"]

        valid_model = clone(model_template)
        valid_model.fit(train_x, train_y)
        valid_pred = pd.Series(dtype=float)
        if valid_count > 0:
            valid_pred = pd.Series(valid_model.predict(valid_x), index=valid_x.index, dtype=float)

        test_model = clone(model_template)
        test_model.fit(fit_x, fit_y)
        candidate_test_pred = pd.Series(test_model.predict(candidate_test_x), index=candidate_test_x.index, dtype=float)
        eval_test_pred = pd.Series(dtype=float)
        if eval_test_count > 0:
            eval_test_pred = pd.Series(test_model.predict(eval_test_x), index=eval_test_x.index, dtype=float)

        pred_frame = candidate_frame.loc[candidate_test_mask, ["instrument", "datetime"]].copy()
        pred_frame["model_score_raw"] = candidate_test_pred.to_numpy(dtype=float)
        prediction_frames.append(pred_frame)

        window_rows.append(
            {
                "train_start": pd.Timestamp(train_start).date().isoformat(),
                "valid_start": pd.Timestamp(valid_start).date().isoformat(),
                "test_start": pd.Timestamp(test_start).date().isoformat(),
                "test_end": pd.Timestamp(min(test_end, candidate_dates.max())).date().isoformat(),
                "train_rows": train_count,
                "valid_rows": valid_count,
                "candidate_rows": candidate_test_count,
                "eval_rows": eval_test_count,
                "valid_ic": _score_ic(valid_y, valid_pred),
                "test_ic": _score_ic(eval_test_y, eval_test_pred),
            }
        )
        if show_progress:
            progress.set_postfix(
                test_start=pd.Timestamp(test_start).date().isoformat(),
                train=train_count,
                valid=valid_count,
                candidate=candidate_test_count,
                eval=eval_test_count,
            )

    progress.close()

    if not prediction_frames:
        return pd.DataFrame(), pd.DataFrame(window_rows), backend_name

    predictions = pd.concat(prediction_frames, ignore_index=True)
    predictions = predictions.sort_values(["datetime", "instrument"]).drop_duplicates(["instrument", "datetime"], keep="last")
    return predictions, pd.DataFrame(window_rows), backend_name


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


def main() -> None:
    parser = argparse.ArgumentParser(description="Build B1 rank scores")
    parser.add_argument("--config", required=True)
    parser.add_argument("--strategy-file", default=str(ROOT / "strategy" / "strategy.py"))
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output-dir", default=str(ROOT / "data" / "reports" / "b1_rank"))
    parser.add_argument("--sample-file", default=str(ROOT / "strategy" / "b1_sample.md"))
    parser.add_argument("--sample-topk", type=int, default=3)
    parser.add_argument("--sample-max-gap-days", type=int, default=240)
    parser.add_argument("--backend", default="auto", choices=["auto", "lightgbm", "hist_gbdt"])
    parser.add_argument("--train-months", type=int, default=24)
    parser.add_argument("--valid-months", type=int, default=6)
    parser.add_argument("--test-months", type=int, default=3)
    parser.add_argument("--step-months", type=int, default=3)
    parser.add_argument("--embargo-days", type=int, default=10)
    parser.add_argument("--min-train-rows", type=int, default=200)
    parser.add_argument("--log-file", default="data/reports/b1_rank_build.log")
    parser.add_argument("--progress", dest="progress", action="store_true", default=True)
    parser.add_argument("--no-progress", dest="progress", action="store_false")
    args = parser.parse_args()
    log_path = _configure_logging(args.log_file)
    LOGGER.info(
        "开始构建 B1 排序分数: config=%s start=%s end=%s output_dir=%s",
        args.config,
        args.start,
        args.end,
        args.output_dir,
    )
    LOGGER.info("日志文件: %s", log_path)

    app_settings = load_app_settings(args.config)
    qlib_source_dir = _resolve_runtime_path(app_settings.qlib_source_dir)
    user_module = load_user_module(Path(args.strategy_file), qlib_source_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    ohlcv = _load_local_ohlcv(app_settings, args.start, args.end)
    LOGGER.info("开始计算指标")
    features = user_module.build_indicators(ohlcv)
    LOGGER.info("指标计算完成: rows=%s columns=%s", len(features), len(features.columns))
    LOGGER.info("开始生成信号")
    signal = user_module.build_pattern_signals(features)
    LOGGER.info("信号生成完成: rows=%s columns=%s", len(signal), len(signal.columns))
    candidate_builder = getattr(user_module, "build_b1_rank_candidate_frame", None)
    if callable(candidate_builder):
        LOGGER.info("开始构建 B1 候选样本")
        candidates = candidate_builder(features, signal)
    else:
        raise AttributeError("策略模块缺少 build_b1_rank_candidate_frame，无法生成 B1 候选样本。")
    if candidates.empty:
        raise RuntimeError("当前区间没有可用的 B1 候选样本，无法生成排序分数。")
    LOGGER.info("B1 候选样本完成: rows=%s", len(candidates))

    feature_columns = list(getattr(user_module, "B1_RANK_FEATURE_COLUMNS", []))
    if not feature_columns:
        raise AttributeError("策略模块缺少 B1_RANK_FEATURE_COLUMNS。")
    LOGGER.info("开始训练/打分: features=%s", len(feature_columns))

    sample_file = Path(args.sample_file).resolve() if str(args.sample_file or "").strip() else None
    feature_profile = pd.DataFrame()
    sample_match_df = pd.DataFrame()
    sample_symbol_map: dict[str, str] = {}
    training_mode = "walk_forward"
    event_output_name = "b1_events.parquet"
    if sample_file is not None and sample_file.exists():
        LOGGER.info("检测到样本文件，切换为样本驱动训练: sample_file=%s", sample_file)
        sample_frame, sample_match_df, sample_symbol_map = _build_b1_sample_training_frame(
            user_module=user_module,
            app_settings=app_settings,
            features=features,
            sample_path=sample_file,
            max_gap_days=int(args.sample_max_gap_days),
        )
        predictions, feature_profile = _score_candidates_from_sample_library(
            sample_frame,
            candidates,
            feature_columns,
            top_k=int(args.sample_topk),
        )
        training_mode = "sample_similarity"
        backend_name = "sample_similarity"
        matched_count = int(sample_match_df["status"].eq("matched").sum()) if not sample_match_df.empty else 0
        unmatched_count = int(len(sample_match_df) - matched_count) if not sample_match_df.empty else 0
        window_df = pd.DataFrame(
            [
                {
                    "training_mode": training_mode,
                    "sample_file": str(sample_file),
                    "matched_samples": matched_count,
                    "unmatched_samples": unmatched_count,
                    "candidate_rows": int(len(candidates)),
                    "score_rows": int(len(predictions)),
                }
            ]
        )
        events = sample_frame
        event_output_name = "b1_sample_training.parquet"
        LOGGER.info(
            "样本驱动训练完成: matched_samples=%s unmatched_samples=%s candidate_rows=%s score_rows=%s",
            matched_count,
            unmatched_count,
            len(candidates),
            len(predictions),
        )
    else:
        event_builder = getattr(user_module, "build_b1_rank_event_frame", None)
        if not callable(event_builder):
            raise AttributeError("策略模块缺少 build_b1_rank_event_frame，无法生成 B1 事件样本。")
        LOGGER.info("开始构建 B1 事件样本")
        events = event_builder(features, signal)
        if events.empty:
            raise RuntimeError("当前区间没有可用的 B1 事件样本。")
        LOGGER.info("B1 事件样本完成: rows=%s", len(events))
        LOGGER.info("未提供有效样本文件，使用原有滚动训练: sample_file=%s", sample_file)
        predictions, window_df, backend_name = _walk_forward_predict(
            events,
            candidates,
            feature_columns,
            backend=args.backend,
            train_months=args.train_months,
            valid_months=args.valid_months,
            test_months=args.test_months,
            step_months=args.step_months,
            embargo_days=args.embargo_days,
            min_train_rows=args.min_train_rows,
            show_progress=bool(args.progress),
        )
        LOGGER.info("滚动训练完成: windows=%s scores=%s backend=%s", len(window_df), len(predictions), backend_name)
    if predictions.empty:
        raise RuntimeError("没有生成任何排序分数，请检查样本匹配结果或扩大时间区间。")

    score_path = output_dir / "b1_model_scores.parquet"
    event_path = output_dir / event_output_name
    candidate_path = output_dir / "b1_candidates.parquet"
    window_path = output_dir / "b1_windows.csv"
    summary_path = output_dir / "summary.json"
    events.to_parquet(event_path, index=False)
    candidates.to_parquet(candidate_path, index=False)
    predictions.loc[:, ["instrument", "datetime", "model_score_raw"]].to_parquet(score_path, index=False)
    window_df.to_csv(window_path, index=False, encoding="utf-8-sig")
    sample_match_path = output_dir / "b1_sample_matches.csv"
    sample_name_map_path = output_dir / "b1_sample_name_map.json"
    feature_profile_path = output_dir / "b1_sample_feature_profile.csv"
    if not sample_match_df.empty:
        export_match_df = sample_match_df.copy()
        for column in ("sample_date", "matched_date"):
            if column in export_match_df.columns:
                export_match_df[column] = pd.to_datetime(export_match_df[column]).dt.date.astype("string")
        export_match_df.to_csv(sample_match_path, index=False, encoding="utf-8-sig")
    if sample_symbol_map:
        sample_name_map_path.write_text(json.dumps(sample_symbol_map, ensure_ascii=False, indent=2), encoding="utf-8")
    if not feature_profile.empty:
        feature_profile.to_csv(feature_profile_path, index=False, encoding="utf-8-sig")

    summary = {
        "training_mode": training_mode,
        "backend": backend_name,
        "event_count": int(len(events)),
        "candidate_count": int(len(candidates)),
        "score_count": int(len(predictions)),
        "feature_count": int(len(feature_columns)),
        "score_start": pd.Timestamp(predictions["datetime"].min()).date().isoformat(),
        "score_end": pd.Timestamp(predictions["datetime"].max()).date().isoformat(),
        "mean_test_ic": float(window_df["test_ic"].dropna().mean()) if ("test_ic" in window_df.columns and window_df["test_ic"].notna().any()) else None,
        "event_path": str(event_path),
        "candidate_path": str(candidate_path),
        "score_path": str(score_path),
        "window_path": str(window_path),
    }
    if sample_file is not None and sample_file.exists():
        summary["sample_file"] = str(sample_file)
    if not sample_match_df.empty:
        summary["sample_count"] = int(len(sample_match_df))
        summary["matched_sample_count"] = int(sample_match_df["status"].eq("matched").sum())
        summary["sample_match_path"] = str(sample_match_path)
    if sample_symbol_map:
        summary["sample_name_map_path"] = str(sample_name_map_path)
    if not feature_profile.empty:
        summary["feature_profile_path"] = str(feature_profile_path)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    LOGGER.info("B1 排序分数输出完成: summary=%s", summary)
    print(json.dumps({**summary, "summary_path": str(summary_path)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
