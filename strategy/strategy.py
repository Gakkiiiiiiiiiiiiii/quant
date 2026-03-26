from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Literal

import numpy as np
import pandas as pd
import qlib
from qlib.constant import REG_CN
from qlib.data import D
from qlib.contrib.evaluate import backtest_daily, risk_analysis
from qlib.contrib.strategy.signal_strategy import WeightStrategyBase


# =========================
# 1) Qlib 初始化与取数
# =========================

def init_qlib(provider_uri: str, region: str = REG_CN) -> None:
    qlib.init(provider_uri=provider_uri, region=region)


def fetch_ohlcv(
    instruments="all",
    start_time: str = "2018-01-01",
    end_time: str = "2024-12-31",
    freq: str = "day",
) -> pd.DataFrame:
    """
    返回 MultiIndex DataFrame:
    index = [instrument, datetime]
    columns = open, high, low, close, volume
    """
    fields = ["$open", "$high", "$low", "$close", "$volume"]
    raw = D.features(
        D.instruments(instruments) if isinstance(instruments, str) else instruments,
        fields,
        start_time=start_time,
        end_time=end_time,
        freq=freq,
        disk_cache=1,
    ).sort_index()

    raw.columns = ["open", "high", "low", "close", "volume"]
    raw.index = raw.index.set_names(["instrument", "datetime"])
    return raw


# =========================
# 2) 指标计算
# =========================

def _ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False, min_periods=span).mean()


def _sma_cn(s: pd.Series, n: int, m: int = 1) -> pd.Series:
    """
    通达信 SMA(X,N,M) 的常用近似:
    Y = (M*X + (N-M)*Y') / N
    用 ewm(alpha=M/N, adjust=False) 表示
    """
    alpha = m / n
    return s.ewm(alpha=alpha, adjust=False).mean()


def _scaled_score(series: pd.Series, low: float, high: float) -> pd.Series:
    width = high - low
    if np.isscalar(width):
        width = max(float(width), 1e-12)
    else:
        width = np.maximum(width, 1e-12)
    return ((series - low) / width).clip(lower=0.0, upper=1.0)


B1_EXPLORATORY_POSITION_RATIO = 0.12
B1_STANDARD_POSITION_RATIO = 0.15
B1_HIGH_CONVICTION_POSITION_RATIO = 0.18
B1_ACTIVE_MAIN_POSITION_LIMIT = 8
B1_PROBE_RATIO_DIVISOR = 3.0
B1_PROBE_TOPK = 3
B1_EVENT_DEDUP_WINDOW = 8
B1_CONFIRM_SCORE_THRESHOLD = 60.0
B1_MODEL_SCORE_Q50 = 50.0
B1_MODEL_SCORE_Q80 = 80.0
B1_QUALITY_WEIGHT = 0.3
B1_MODEL_WEIGHT = 0.7
B1_EXIT_SCORE_ALIASES = {
    "b1_exit_score_tp1": ("b1_exit_score_tp1", "exit_score_tp1", "score_tp1", "tp1_score", "model_tp1", "model_score_tp1"),
    "b1_exit_score_tp2": ("b1_exit_score_tp2", "exit_score_tp2", "score_tp2", "tp2_score", "model_tp2", "model_score_tp2"),
    "b1_exit_score_tp3": ("b1_exit_score_tp3", "exit_score_tp3", "score_tp3", "tp3_score", "model_tp3", "model_score_tp3"),
    "b1_exit_score_tail": ("b1_exit_score_tail", "exit_score_tail", "score_tail", "tail_score", "model_tail", "model_score_tail"),
}
B1_RANK_FEATURE_COLUMNS = [
    "f_lt_slope_3",
    "f_lt_slope_5",
    "f_st_over_lt",
    "f_cnt_above_lt",
    "f_cnt_above_st",
    "f_close_lt_dev",
    "f_close_st_dev",
    "f_range_40",
    "f_bull_bars_20",
    "f_big_up_cnt_30",
    "f_recent_high_days",
    "f_attack_quality",
    "f_j_value",
    "f_pullback_from_hh",
    "f_days_from_recent_high",
    "f_break_lt_cnt_10",
    "f_big_down_cnt_12",
    "f_dist_down_cnt_12",
    "f_low_lt_dev",
    "f_pullback_range_5",
    "f_down_vol_share_8",
    "f_vol_to_ma20",
    "f_vol_rank_10",
    "f_vol_rank_20",
    "f_turnover_to_ma20",
    "f_shrink_days_5",
    "f_atr_5_to_20",
    "f_body_pct",
    "f_range_pct",
    "f_upper_wick",
    "f_lower_wick",
]


def _safe_float(value: object) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return float("nan")
    return number if pd.notna(number) else float("nan")


def _rolling_rank(series: pd.Series, window: int) -> pd.Series:
    def _calc(values: np.ndarray) -> float:
        arr = np.asarray(values, dtype=float)
        if arr.size == 0 or np.isnan(arr[-1]):
            return np.nan
        valid = arr[~np.isnan(arr)]
        if valid.size == 0:
            return np.nan
        return float((valid <= valid[-1]).sum() / valid.size)

    return series.rolling(window, min_periods=window).apply(_calc, raw=True)


def _normalize_cross_sectional_percentile(series: pd.Series) -> pd.Series:
    valid = series.dropna()
    if valid.empty:
        return pd.Series(np.nan, index=series.index, dtype=float)
    ranked = valid.rank(method="average", pct=True) * 100.0
    return ranked.reindex(series.index)


def _normalize_probability_series(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    valid = numeric.dropna()
    if valid.empty:
        return pd.Series(np.nan, index=series.index, dtype=float)
    if valid.between(0.0, 1.0).all():
        return numeric.clip(lower=0.0, upper=1.0)
    if valid.between(0.0, 100.0).all():
        return (numeric / 100.0).clip(lower=0.0, upper=1.0)
    ranked = valid.rank(method="average", pct=True)
    return ranked.reindex(series.index).astype(float)


def load_b1_model_scores(score_path: str | Path | None) -> pd.DataFrame:
    if score_path is None:
        return pd.DataFrame(columns=["instrument", "datetime", "model_score_raw", *B1_EXIT_SCORE_ALIASES.keys()])
    path = Path(score_path)
    if not path.exists():
        raise FileNotFoundError(f"B1 ?????????: {path}")
    if path.suffix.lower() == ".parquet":
        frame = pd.read_parquet(path)
    else:
        frame = pd.read_csv(path)

    renamed = frame.copy()
    if "symbol" in renamed.columns and "instrument" not in renamed.columns:
        renamed = renamed.rename(columns={"symbol": "instrument"})
    if "trading_date" in renamed.columns and "datetime" not in renamed.columns:
        renamed = renamed.rename(columns={"trading_date": "datetime"})
    if "model_score_raw" not in renamed.columns:
        for candidate in ("model_score", "score", "prediction", "pred"):
            if candidate in renamed.columns:
                renamed = renamed.rename(columns={candidate: "model_score_raw"})
                break
    for canonical, aliases in B1_EXIT_SCORE_ALIASES.items():
        if canonical in renamed.columns:
            continue
        for candidate in aliases:
            if candidate in renamed.columns:
                renamed = renamed.rename(columns={candidate: canonical})
                break
    required = {"instrument", "datetime"}
    missing = sorted(required - set(renamed.columns))
    if missing:
        raise ValueError(f"B1 ??????????: {', '.join(missing)}")

    score_columns = [column for column in ["model_score_raw", *B1_EXIT_SCORE_ALIASES.keys()] if column in renamed.columns]
    if not score_columns:
        raise ValueError("B1 ???????????????")
    payload = renamed.loc[:, ["instrument", "datetime", *score_columns]].copy()
    payload["instrument"] = payload["instrument"].astype(str)
    payload["datetime"] = pd.to_datetime(payload["datetime"]).dt.normalize()
    for column in score_columns:
        payload[column] = pd.to_numeric(payload[column], errors="coerce")
    payload = payload.dropna(subset=["instrument", "datetime"]).drop_duplicates(["instrument", "datetime"], keep="last")
    return payload


def apply_b1_model_scores(
    signal: pd.DataFrame,
    score_frame: pd.DataFrame | None = None,
    *,
    quality_weight: float = B1_QUALITY_WEIGHT,
    model_weight: float = B1_MODEL_WEIGHT,
) -> pd.DataFrame:
    enriched = signal.copy()
    if "rule_priority_score" not in enriched.columns:
        enriched["rule_priority_score"] = enriched["priority_score"].astype(float)
    if "model_score_raw" not in enriched.columns:
        enriched["model_score_raw"] = np.nan
    if "model_score" not in enriched.columns:
        enriched["model_score"] = np.nan
    if "final_score" not in enriched.columns:
        enriched["final_score"] = enriched["priority_score"].astype(float)
    if "score_source" not in enriched.columns:
        enriched["score_source"] = "rule_only"
    if "b1_rank_active" not in enriched.columns:
        enriched["b1_rank_active"] = 0
    for canonical in B1_EXIT_SCORE_ALIASES:
        if canonical not in enriched.columns:
            enriched[canonical] = np.nan

    if score_frame is None or score_frame.empty:
        enriched["score"] = enriched["priority_score"].astype(float)
        return enriched

    lookup = score_frame.copy()
    lookup["instrument"] = lookup["instrument"].astype(str)
    lookup["datetime"] = pd.to_datetime(lookup["datetime"]).dt.normalize()
    merge_columns = ["instrument", "datetime", *[column for column in ["model_score_raw", *B1_EXIT_SCORE_ALIASES.keys()] if column in lookup.columns]]
    merged = (
        enriched.reset_index()
        .merge(lookup.loc[:, merge_columns], on=["instrument", "datetime"], how="left", suffixes=("", "_ext"))
        .set_index(["instrument", "datetime"])
        .sort_index()
    )
    if "model_score_raw_ext" in merged.columns:
        merged["model_score_raw"] = pd.to_numeric(merged["model_score_raw_ext"], errors="coerce").combine_first(
            pd.to_numeric(merged["model_score_raw"], errors="coerce")
        )
        merged = merged.drop(columns=["model_score_raw_ext"], errors="ignore")
    for canonical in B1_EXIT_SCORE_ALIASES:
        ext_column = f"{canonical}_ext"
        if ext_column in merged.columns:
            merged[canonical] = pd.to_numeric(merged[ext_column], errors="coerce").combine_first(
                pd.to_numeric(merged[canonical], errors="coerce")
            )
            merged = merged.drop(columns=[ext_column], errors="ignore")

    active_mask = merged["b1"].eq(1) & merged["model_score_raw"].notna()
    if active_mask.any():
        normalized = (
            merged.loc[active_mask, ["model_score_raw"]]
            .groupby(level="datetime")["model_score_raw"]
            .transform(_normalize_cross_sectional_percentile)
        )
        merged.loc[active_mask, "model_score"] = normalized.astype(float)
        blended = quality_weight * merged.loc[active_mask, "quality_score"].astype(float) + model_weight * merged.loc[
            active_mask, "model_score"
        ].astype(float)
        merged.loc[active_mask, "final_score"] = blended.astype(float)
        merged.loc[active_mask, "priority_score"] = merged.loc[active_mask, "final_score"]
        merged.loc[active_mask, "score_source"] = "model_blend"
        merged.loc[active_mask, "b1_rank_active"] = 1

    for canonical in B1_EXIT_SCORE_ALIASES:
        score_mask = merged[canonical].notna()
        if score_mask.any():
            merged.loc[score_mask, canonical] = (
                merged.loc[score_mask, [canonical]]
                .groupby(level="datetime")[canonical]
                .transform(_normalize_probability_series)
                .astype(float)
            )

    merged["score"] = merged["priority_score"].astype(float)
    return merged


def allow_b1_confirm(row: pd.Series | dict[str, object], threshold: float = B1_CONFIRM_SCORE_THRESHOLD) -> bool:
    if not bool(row.get("b1_confirm", 0)):
        return False
    model_score = _safe_float(row.get("model_score", np.nan))
    if pd.notna(model_score):
        return model_score >= float(threshold)
    return True


def select_b1_probe_candidates(
    candidate_df: pd.DataFrame,
    *,
    topk: int = B1_PROBE_TOPK,
) -> pd.DataFrame:
    if candidate_df.empty:
        return candidate_df.copy()
    working = candidate_df.sort_values("priority_score", ascending=False).copy()
    if "model_score" in working.columns and working["model_score"].notna().any():
        score_floor = float(working["model_score"].dropna().median())
        working = working[(working["model_score"].isna()) | (working["model_score"] >= score_floor)]
    if int(topk) > 0:
        working = working.head(int(topk))
    return working


def build_b1_rank_event_frame(
    features: pd.DataFrame,
    signal: pd.DataFrame,
    *,
    dedup_window: int = B1_EVENT_DEDUP_WINDOW,
) -> pd.DataFrame:
    joined = features.join(
        signal[
            [
                "b1",
                "b1_confirm",
                "b1_probe_invalid",
                "b1_lt_hard_stop_flag",
                "b1_st_stop_flag",
                "b1_soft_exit_flag",
                "b1_watch_days",
            ]
        ],
        how="inner",
    ).sort_index()
    rows: list[dict[str, object]] = []

    for instrument, group in joined.groupby(level="instrument", sort=False):
        g = group.droplevel("instrument").sort_index().copy()
        o = g["open"].astype(float)
        h = g["high"].astype(float)
        l = g["low"].astype(float)
        c = g["close"].astype(float)
        v = g["volume"].astype(float)
        st = g["st"].astype(float)
        lt = g["lt"].astype(float)
        prev_close = c.shift(1)
        bullish = (c > o) & (c / (prev_close + 1e-12) > 1.03)
        explosive_bull = bullish & (c / (prev_close + 1e-12) >= 1.05) & (v > g["vol_ma20"] * 1.5)
        big_down = (c < o) & (((o - c) / (prev_close + 1e-12)) >= 0.04)
        dist_down = (c < o) & (v >= g["vol_ma20"] * 1.8)
        recent_high = h >= g["hh20"] * 0.995
        recent_high_days = _barslast(recent_high)
        true_range = pd.concat(
            [
                (h - l).abs(),
                (h - prev_close).abs(),
                (l - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        atr5 = true_range.rolling(5, min_periods=5).mean()
        atr20 = true_range.rolling(20, min_periods=20).mean()
        vol_rank_10 = _rolling_rank(v, 10)
        vol_rank_20 = _rolling_rank(v, 20)
        returns = c.pct_change(fill_method=None)
        attack_quality = returns.clip(lower=0.0).rolling(10, min_periods=10).sum() / (
            returns.abs().rolling(10, min_periods=10).sum() + 1e-6
        )
        pullback_from_hh = (g["hh20"].shift(1) - c) / (g["hh20"].shift(1) + 1e-12)
        down_vol_share_8 = g["down_vol_8"] / (g["down_vol_8"] + g["up_vol_8"] + 1e-6)
        upper_wick = (h - pd.concat([c, o], axis=1).max(axis=1)) / (prev_close + 1e-12)
        lower_wick = (pd.concat([c, o], axis=1).min(axis=1) - l) / (prev_close + 1e-12)

        candidate_positions = np.flatnonzero(g["b1"].fillna(0).astype(int).to_numpy() == 1)
        last_kept_idx: int | None = None
        last_kept_resolved = True
        for pos in candidate_positions:
            if pos + 10 >= len(g):
                continue
            if pos + 1 >= len(g):
                continue
            if last_kept_idx is not None and pos - last_kept_idx <= int(dedup_window) and not last_kept_resolved:
                continue

            entry_px = _safe_float(o.iloc[pos + 1])
            if pd.isna(entry_px) or entry_px <= 0:
                continue

            future10 = g.iloc[pos + 1 : pos + 11]
            future5 = g.iloc[pos + 1 : pos + 6]
            if len(future10) < 10 or len(future5) < 5:
                continue
            confirm_hit = bool(future5["b1_confirm"].fillna(0).astype(int).eq(1).any())
            probe_fail = bool(
                future5[
                    [
                        "b1_probe_invalid",
                        "b1_lt_hard_stop_flag",
                        "b1_st_stop_flag",
                    ]
                ]
                .fillna(0)
                .astype(int)
                .any(axis=1)
                .any()
            )
            probe_timeout = bool(g.iloc[pos + 1 : pos + 7]["b1_watch_days"].fillna(-1).astype(float).gt(5).any())
            if not probe_fail:
                probe_fail = probe_timeout
            mfe_10 = float(future10["high"].max() / entry_px - 1.0)
            mae_5 = float(future5["low"].min() / entry_px - 1.0)
            label_rank = (
                1.2 * float(np.clip(mfe_10, 0.0, 0.15))
                - 1.0 * float(np.clip(-mae_5, 0.0, 0.08))
                + 0.4 * float(int(confirm_hit))
                - 0.8 * float(int(probe_fail))
            )
            label_cls = int(confirm_hit and mfe_10 >= 0.10 and not probe_fail)

            row = {
                "instrument": str(instrument),
                "datetime": g.index[pos],
                "entry_date": g.index[pos + 1],
                "entry_open": entry_px,
                "mfe_10": mfe_10,
                "mae_5": mae_5,
                "confirm_hit": int(confirm_hit),
                "probe_fail": int(probe_fail),
                "label_rank": float(label_rank),
                "label_cls": label_cls,
                "f_lt_slope_3": float(lt.iloc[pos] / (lt.shift(3).iloc[pos] + 1e-12) - 1.0),
                "f_lt_slope_5": float(lt.iloc[pos] / (lt.shift(5).iloc[pos] + 1e-12) - 1.0),
                "f_st_over_lt": float(st.iloc[pos] / (lt.iloc[pos] + 1e-12) - 1.0),
                "f_cnt_above_lt": float(g["count_above_lt_30"].iloc[pos] / 30.0),
                "f_cnt_above_st": float(g["count_above_st_20"].iloc[pos] / 20.0),
                "f_close_lt_dev": float(c.iloc[pos] / (lt.iloc[pos] + 1e-12) - 1.0),
                "f_close_st_dev": float(c.iloc[pos] / (st.iloc[pos] + 1e-12) - 1.0),
                "f_range_40": float(g["hh40"].iloc[pos] / (g["ll40"].iloc[pos] + 1e-12) - 1.0),
                "f_bull_bars_20": float(bullish.rolling(20, min_periods=20).sum().iloc[pos]),
                "f_big_up_cnt_30": float(explosive_bull.rolling(30, min_periods=30).sum().iloc[pos]),
                "f_recent_high_days": float(recent_high_days.iloc[pos]),
                "f_attack_quality": float(attack_quality.iloc[pos]),
                "f_j_value": float(g["j"].iloc[pos]),
                "f_pullback_from_hh": float(pullback_from_hh.iloc[pos]),
                "f_days_from_recent_high": float(recent_high_days.iloc[pos]),
                "f_break_lt_cnt_10": float((c < lt).rolling(10, min_periods=10).sum().iloc[pos]),
                "f_big_down_cnt_12": float(big_down.rolling(12, min_periods=12).sum().iloc[pos]),
                "f_dist_down_cnt_12": float(dist_down.rolling(12, min_periods=12).sum().iloc[pos]),
                "f_low_lt_dev": float(l.iloc[pos] / (lt.iloc[pos] + 1e-12) - 1.0),
                "f_pullback_range_5": float(h.rolling(5, min_periods=5).max().iloc[pos] / (l.rolling(5, min_periods=5).min().iloc[pos] + 1e-12) - 1.0),
                "f_down_vol_share_8": float(down_vol_share_8.iloc[pos]),
                "f_vol_to_ma20": float(v.iloc[pos] / (g["vol_ma20"].iloc[pos] + 1e-12)),
                "f_vol_rank_10": float(vol_rank_10.iloc[pos]),
                "f_vol_rank_20": float(vol_rank_20.iloc[pos]),
                "f_turnover_to_ma20": float(g["amount_proxy"].iloc[pos] / (g["amount_ma20"].iloc[pos] + 1e-12)),
                "f_shrink_days_5": float((v < g["vol_ma20"] * 0.8).rolling(5, min_periods=5).sum().iloc[pos]),
                "f_atr_5_to_20": float(atr5.iloc[pos] / (atr20.iloc[pos] + 1e-12)),
                "f_body_pct": float(g["body_pct"].iloc[pos]),
                "f_range_pct": float(g["range_pct_prev_close"].iloc[pos]),
                "f_upper_wick": float(upper_wick.iloc[pos]),
                "f_lower_wick": float(lower_wick.iloc[pos]),
            }
            rows.append(row)

            last_kept_idx = pos
            resolve_window = g.iloc[pos + 1 : pos + 6]
            last_kept_resolved = bool(
                resolve_window["b1_confirm"].fillna(0).astype(int).eq(1).any()
                or resolve_window["b1_probe_invalid"].fillna(0).astype(int).eq(1).any()
                or resolve_window["b1_lt_hard_stop_flag"].fillna(0).astype(int).eq(1).any()
                or resolve_window["b1_st_stop_flag"].fillna(0).astype(int).eq(1).any()
            )

    if not rows:
        columns = ["instrument", "datetime", "entry_date", "entry_open", "mfe_10", "mae_5", "confirm_hit", "probe_fail", "label_rank", "label_cls", *B1_RANK_FEATURE_COLUMNS]
        return pd.DataFrame(columns=columns)
    dataset = pd.DataFrame(rows)
    dataset["datetime"] = pd.to_datetime(dataset["datetime"]).dt.normalize()
    dataset["entry_date"] = pd.to_datetime(dataset["entry_date"]).dt.normalize()
    dataset = dataset.sort_values(["datetime", "instrument"]).reset_index(drop=True)
    return dataset


def resolve_b1_position_ratio(row: pd.Series | dict[str, object]) -> float:
    """
    按 v4 文档把 B1 初始主仓分成三档：
    - 试探仓：10%
    - 标准仓：15%
    - 高确信仓：18%
    """
    model_score = _safe_float(row.get("model_score", np.nan))
    if pd.notna(model_score):
        if model_score >= B1_MODEL_SCORE_Q80:
            return B1_HIGH_CONVICTION_POSITION_RATIO
        if model_score >= B1_MODEL_SCORE_Q50:
            return B1_STANDARD_POSITION_RATIO
        return B1_EXPLORATORY_POSITION_RATIO
    priority_score = float(row.get("priority_score", 0.0) or 0.0)
    quality_score = float(row.get("quality_score", max(priority_score - 60.0, 0.0)) or 0.0)
    b1_confirm = bool(row.get("b1_confirm", 0))
    if priority_score >= 145.0 or (b1_confirm and quality_score >= 72.0):
        return B1_HIGH_CONVICTION_POSITION_RATIO
    if priority_score >= 125.0 or quality_score >= 60.0 or b1_confirm:
        return B1_STANDARD_POSITION_RATIO
    return B1_EXPLORATORY_POSITION_RATIO


def _barslast(mask: pd.Series) -> pd.Series:
    values = mask.fillna(False).astype(bool).tolist()
    result: list[float] = []
    last_true = -1
    for idx, flag in enumerate(values):
        if flag:
            last_true = idx
            result.append(0.0)
        elif last_true < 0:
            result.append(np.nan)
        else:
            result.append(float(idx - last_true))
    return pd.Series(result, index=mask.index, dtype=float)


def _last_event_value(values: pd.Series, mask: pd.Series) -> pd.Series:
    event_mask = mask.fillna(False).astype(bool).tolist()
    raw_values = values.tolist()
    result: list[float] = []
    last_value = np.nan
    for value, flag in zip(raw_values, event_mask):
        if flag:
            last_value = float(value)
        result.append(last_value)
    return pd.Series(result, index=values.index, dtype=float)


def build_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    基于 OHLCV 计算你三个战法要用的全部基础指标
    """
    def _per_inst(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_index(level="datetime").copy()
        o = g["open"]
        c = g["close"]
        h = g["high"]
        l = g["low"]
        v = g["volume"]

        g["ret1"] = c.pct_change(fill_method=None)
        g["ret3"] = c.pct_change(3, fill_method=None)
        g["ret5"] = c.pct_change(5, fill_method=None)
        g["listed_days"] = np.arange(len(g), dtype=float) + 1.0

        # 你前面统一的两根线
        g["ma14"] = c.rolling(14, min_periods=14).mean()
        g["ma28"] = c.rolling(28, min_periods=28).mean()
        g["ma57"] = c.rolling(57, min_periods=57).mean()
        g["ma114"] = c.rolling(114, min_periods=114).mean()
        g["st"] = _ema(_ema(c, 10), 10)  # 知行短期趋势线
        g["lt"] = (g["ma14"] + g["ma28"] + g["ma57"] + g["ma114"]) / 4.0  # 知行多空线

        g["vol_ma5"] = v.rolling(5, min_periods=5).mean()
        g["vol_ma20"] = v.rolling(20, min_periods=20).mean()

        # KDJ(9,3,3)
        ll9 = l.rolling(9, min_periods=9).min()
        hh9 = h.rolling(9, min_periods=9).max()
        rsv = (c - ll9) / (hh9 - ll9 + 1e-12) * 100.0
        g["k"] = _sma_cn(rsv, 3, 1)
        g["d"] = _sma_cn(g["k"], 3, 1)
        g["j"] = 3 * g["k"] - 2 * g["d"]

        # 常用滚动区间
        g["ll6"] = l.rolling(6, min_periods=6).min()
        g["hh8"] = h.rolling(8, min_periods=8).max()
        g["ll8"] = l.rolling(8, min_periods=8).min()
        g["hh6"] = h.rolling(6, min_periods=6).max()
        g["ll10"] = l.rolling(10, min_periods=10).min()
        g["hh6_prev"] = g["hh6"].shift(1)
        g["hh10_prev"] = h.rolling(10, min_periods=10).max().shift(1)
        g["hh20"] = h.rolling(20, min_periods=20).max()
        g["hh25"] = h.rolling(25, min_periods=25).max()
        g["hh30"] = h.rolling(30, min_periods=30).max()
        g["hh40"] = h.rolling(40, min_periods=40).max()
        g["hh50"] = h.rolling(50, min_periods=50).max()
        g["ll20"] = l.rolling(20, min_periods=20).min()
        g["ll40"] = l.rolling(40, min_periods=40).min()
        g["ll50"] = l.rolling(50, min_periods=50).min()
        g["vol_hh60"] = v.rolling(60, min_periods=60).max()

        # 补票战法的区间位置值
        g["range_pos_3"] = 100.0 * (c - l.rolling(3, min_periods=3).min()) / (
            c.rolling(3, min_periods=3).max() - l.rolling(3, min_periods=3).min() + 1e-12
        )
        g["range_pos_21"] = 100.0 * (c - l.rolling(21, min_periods=21).min()) / (
            c.rolling(21, min_periods=21).max() - l.rolling(21, min_periods=21).min() + 1e-12
        )

        g["count_above_lt_20"] = (c > g["lt"]).rolling(20, min_periods=20).sum()
        g["count_above_lt_30"] = (c > g["lt"]).rolling(30, min_periods=30).sum()
        g["count_above_lt_10"] = (c > g["lt"]).rolling(10, min_periods=10).sum()
        g["count_above_st_20"] = (c > g["st"]).rolling(20, min_periods=20).sum()
        g["small_move_5"] = (c.pct_change(fill_method=None).abs() < 0.03).rolling(5, min_periods=5).sum()

        g["st_slope_up"] = g["st"] >= g["st"].shift(1)
        g["lt_slope_up"] = g["lt"] >= g["lt"].shift(1)

        g["close_pos"] = (c - l) / (h - l + 1e-12)
        g["body_ratio"] = (c - o).abs() / (o.abs() + 1e-12)
        g["body_pct"] = (c / (o + 1e-12) - 1.0).abs()
        g["vol_ratio"] = v / (g["vol_ma5"] + 1e-12)
        g["vol_rank_60"] = _rolling_rank(v.astype(float), 60)
        g["structure_range_6"] = g["hh6"] / (g["ll6"] + 1e-12) - 1.0
        g["range_pct_prev_close"] = (h - l) / (c.shift(1) + 1e-12)
        g["amount_proxy"] = g["amount"] if "amount" in g.columns else c * v
        g["amount_ma20"] = g["amount_proxy"].rolling(20, min_periods=20).mean()
        g["upper_wick"] = (h - pd.concat([c, o], axis=1).max(axis=1)) / (c.shift(1) + 1e-12)
        g["lower_wick"] = (pd.concat([c, o], axis=1).min(axis=1) - l) / (c.shift(1) + 1e-12)
        g["upper_shadow_ratio"] = (h - c) / (h - l + 1e-12)
        g["count_above_st_10"] = (c > g["st"]).rolling(10, min_periods=10).sum()
        down_day = c < o
        up_day = c >= o
        g["down_vol_8"] = v.where(down_day, 0.0).rolling(8, min_periods=8).sum()
        g["up_vol_8"] = v.where(up_day, 0.0).rolling(8, min_periods=8).sum()
        g["down_body_8"] = (((o - c) / (c.shift(1) + 1e-12)).clip(lower=0.0).where(down_day, 0.0)).rolling(8, min_periods=8).sum()
        g["up_body_8"] = (((c - o) / (c.shift(1) + 1e-12)).clip(lower=0.0).where(up_day, 0.0)).rolling(8, min_periods=8).sum()
        return g

    return df.groupby(level="instrument", group_keys=False).apply(_per_inst)


# =========================
# 3) 三个战法的信号定义
# =========================

def build_pattern_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    输出一个给 Qlib strategy 直接使用的 signal DataFrame
    columns:
        score / priority_score / b1 / b2 / b3 / entry_flag / exit_flag / stop_price ...
    """

    def _per_inst(g: pd.DataFrame) -> pd.DataFrame:
        g = g.copy()
        c = g["close"]
        h = g["high"]
        l = g["low"]
        v = g["volume"]
        eps = 1e-12
        eligible_entry = (
            (v > 0)
            & (g["listed_days"] >= 60)
            & (g["amount_ma20"] >= 30_000_000)
        )

        # -----------------
        # B1: 按用户给定的通达信公式实现
        # -----------------
        hh40 = g["hh40"] if "hh40" in g.columns else h.rolling(40, min_periods=1).max()
        ll40 = g["ll40"] if "ll40" in g.columns else l.rolling(40, min_periods=1).min()
        count_above_lt_30 = g["count_above_lt_30"] if "count_above_lt_30" in g.columns else (c > g["lt"]).rolling(30, min_periods=1).sum()
        count_above_st_20 = g["count_above_st_20"] if "count_above_st_20" in g.columns else (c > g["st"]).rolling(20, min_periods=1).sum()
        prev_high = g["hh20"].shift(1)
        dist_from_prev_high = (prev_high - c) / (prev_high + eps)
        recent_high = h >= g["hh20"] * 0.995
        days_since_recent_high = _barslast(recent_high)
        explosive_bull = (c > g["open"]) & (c / (c.shift(1) + eps) >= 1.05) & (v >= g["vol_ma20"] * 1.5)
        pullback_big_bear = (c < g["open"]) & (((g["open"] - c) / (c.shift(1) + eps)) >= 0.04)
        pullback_distribution_bear = (c < g["open"]) & (v >= g["vol_ma20"] * 1.8)
        b1_structure = (
            (g["lt"] > g["lt"].shift(3))
            & (g["st"] > g["lt"])
            & (count_above_lt_30 >= 20)
            & (count_above_st_20 >= 12)
        )
        b1_startup_base = (
            (hh40 / (ll40 + eps) >= 1.25)
            & (explosive_bull.rolling(30, min_periods=1).sum() >= 1)
            & (recent_high.rolling(15, min_periods=1).sum() >= 1)
            & b1_structure
        )
        b1_retrace_ready = (
            (g["j"] < 20)
            & (c <= g["st"] * 1.02)
            & (c >= g["lt"] * 0.99)
            & (l >= g["lt"] * 0.97)
            & (dist_from_prev_high >= 0.04)
            & (dist_from_prev_high <= 0.15)
            & (days_since_recent_high >= 2)
            & (days_since_recent_high <= 12)
            & ((c < g["lt"]).rolling(10, min_periods=1).sum() <= 1)
            & (pullback_big_bear.rolling(12, min_periods=1).sum() == 0)
            & (pullback_distribution_bear.rolling(12, min_periods=1).sum() == 0)
        )
        b1_extreme_shrink = (
            (v <= v.rolling(10, min_periods=1).min())
            & (v < g["vol_ma20"] * 0.65)
        )
        b1_kline = (
            (g["body_pct"] <= 0.022)
            & (g["range_pct_prev_close"] <= 0.055)
        )

        o = g["open"].astype(float)
        prev_close = c.shift(1)
        ret3 = g["ret3"] if "ret3" in g.columns else c.pct_change(3, fill_method=None)
        ret5 = g["ret5"] if "ret5" in g.columns else c.pct_change(5, fill_method=None)
        close_pos = g["close_pos"] if "close_pos" in g.columns else (c - l) / (h - l + eps)
        upper_wick = g["upper_wick"] if "upper_wick" in g.columns else (h - pd.concat([o, c], axis=1).max(axis=1)) / (prev_close + eps)
        vol_rank_60 = g["vol_rank_60"] if "vol_rank_60" in g.columns else _rolling_rank(v.astype(float), 60)
        vol_to_ma20 = v / (g["vol_ma20"] + eps)
        turnover_ratio = g["amount_proxy"] / (g["amount_ma20"] + eps)
        body_pct = (c / (o + eps) - 1.0).abs()

        accel_bear = (c < o) & (body_pct >= 0.05) & (close_pos <= 0.35)
        accel_false_bear = (body_pct >= 0.04) & (close_pos <= 0.40) & (upper_wick >= 0.02)
        accel_turnover_bear = (turnover_ratio >= 2.5) & (c < h * 0.96) & (upper_wick >= 0.025)
        b1_accel_exhaust_day = (
            ((ret3 >= 0.10) | (ret5 >= 0.16))
            & (vol_to_ma20 >= 2.2)
            & (vol_rank_60 >= 0.95)
            & (accel_bear | accel_false_bear | accel_turnover_bear)
        ).fillna(False)
        b1_accel_exhaust_hard = (
            b1_accel_exhaust_day
            & ((c < g["st"]) | (l < g["lt"] * 0.995))
        ).fillna(False)
        b1_accel_exhaust_hard = (
            b1_accel_exhaust_hard
            | (
                b1_accel_exhaust_day.shift(1, fill_value=False)
                & (h <= h.shift(1) * 1.01)
                & (c < c.shift(1))
            )
            | (
                b1_accel_exhaust_day.shift(2, fill_value=False)
                & (pd.concat([h.shift(1), h], axis=1).max(axis=1) <= h.shift(2) * 1.01)
                & (pd.concat([c.shift(1), c], axis=1).min(axis=1) < c.shift(2))
            )
        ).fillna(False)

        prior_peak = g["hh20"].shift(1)
        pre_accel = (c.shift(1) / (c.shift(6) + eps) - 1.0) >= 0.15
        long_bear = (c < o) & (body_pct >= 0.05)
        close_bad = close_pos <= 0.30
        b1_secondary_peak_distribution = (
            pre_accel
            & (h >= prior_peak * 0.97)
            & (c <= prior_peak * 1.01)
            & (vol_to_ma20 >= 2.0)
            & long_bear
            & close_bad
        ).fillna(False)

        b1_stair_dist_3d = (
            (h.shift(1) >= g["hh20"].shift(2) * 0.995)
            & (h < h.shift(1))
            & (h.shift(1) < h.shift(2))
            & (c < c.shift(1))
            & (c.shift(1) < c.shift(2))
            & (v >= v.shift(1))
            & (v.shift(1) >= v.shift(2))
            & (close_pos <= 0.40)
        ).fillna(False)
        b1_stair_dist_4d = (b1_stair_dist_3d & (c < g["st"])).fillna(False)

        bear_blow = ((vol_to_ma20 >= 2.0) & (close_pos <= 0.35) & (body_pct >= 0.04)).fillna(False)
        prior_bear_blow_high = _last_event_value(h.shift(1), bear_blow.shift(1, fill_value=False))
        prior_bear_blow_bars = (_barslast(bear_blow.shift(1, fill_value=False)) + 1.0).fillna(np.nan)
        b1_double_top_distribution = (
            bear_blow
            & prior_bear_blow_high.notna()
            & prior_bear_blow_bars.between(3.0, 20.0, inclusive="both")
            & (((h / (prior_bear_blow_high + eps)) - 1.0).abs() <= 0.04)
            & (h <= prior_bear_blow_high * 1.02)
        ).fillna(False)

        neg_mask = c < o
        pos_mask = c > o
        neg_count_6 = neg_mask.rolling(6, min_periods=6).sum().replace(0.0, np.nan)
        pos_count_6 = pos_mask.rolling(6, min_periods=6).sum().replace(0.0, np.nan)
        neg_body_mean = body_pct.where(neg_mask, 0.0).rolling(6, min_periods=6).sum() / neg_count_6
        pos_body_mean = body_pct.where(pos_mask, 0.0).rolling(6, min_periods=6).sum() / pos_count_6
        neg_vol_mean = v.where(neg_mask, 0.0).rolling(6, min_periods=6).sum() / neg_count_6
        pos_vol_mean = v.where(pos_mask, 0.0).rolling(6, min_periods=6).sum() / pos_count_6
        b1_weak_rebound_top = (
            ((neg_body_mean / (pos_body_mean + eps)) >= 1.4)
            & ((neg_vol_mean / (pos_vol_mean + eps)) >= 1.2)
            & (c < c.rolling(5, min_periods=5).max().shift(1) * 1.01)
        ).fillna(False)
        b1_weak_rebound_top_count = b1_weak_rebound_top.astype(int).rolling(8, min_periods=1).sum()

        b1_distribution_score = (
            2.0 * b1_double_top_distribution.astype(float)
            + 1.6 * b1_secondary_peak_distribution.astype(float)
            + 1.3 * b1_accel_exhaust_day.astype(float)
            + 1.0 * b1_stair_dist_3d.astype(float)
            + 0.8 * b1_weak_rebound_top.astype(float)
        )

        b1_soft_exit_flag = (b1_stair_dist_3d | b1_weak_rebound_top).fillna(False)
        b1_st_stop_flag = (b1_secondary_peak_distribution & (c < g["st"])).fillna(False)
        b1_hard_distribution_flag = (
            b1_double_top_distribution
            | b1_accel_exhaust_hard
            | b1_st_stop_flag
        ).fillna(False)
        b1_exit_signal = (
            b1_accel_exhaust_day
            | b1_secondary_peak_distribution
            | b1_stair_dist_3d
            | b1_double_top_distribution
            | b1_weak_rebound_top
        ).fillna(False)
        recent_distribution = b1_exit_signal.rolling(30, min_periods=1).sum() >= 1
        repair = (c > g["hh30"].shift(1) * 1.03) | (g["count_above_st_10"] >= 8)
        b1_pullback_distribution_forbidden = pullback_distribution_bear.rolling(10, min_periods=1).sum() >= 1
        b1_forbidden = (recent_distribution & ~repair) | b1_pullback_distribution_forbidden
        b1_candidate_v5 = (
            eligible_entry
            & b1_startup_base
            & b1_retrace_ready
            & b1_extreme_shrink
            & b1_kline
            & ~b1_forbidden
        )
        b1_probe_bars = _barslast(b1_candidate_v5)
        b1_signal_high = _last_event_value(h, b1_candidate_v5)
        b1_signal_low = _last_event_value(l, b1_candidate_v5)
        b1_confirm_window = (b1_probe_bars >= 1) & (b1_probe_bars <= 5)
        b1_confirm = (
            b1_confirm_window
            & (c > b1_signal_high * 1.01)
            & (c > g["st"])
            & (c > h.shift(1))
            & (v > np.maximum(v.shift(1) * 1.2, g["vol_ma20"] * 1.2))
        )
        b1_probe_invalid = (
            (b1_probe_bars <= 5)
            & (
                (c < b1_signal_low * 0.99)
                | (c < g["lt"] * 0.98)
                | ((b1_probe_bars >= 5) & (c < g["st"]))
            )
        ).fillna(False)
        b1_core = b1_candidate_v5
        b1_formula = b1_candidate_v5
        b1_lt_hard_stop_flag = (c < g["lt"]).fillna(False)
        b1_platform_established = (g["hh8"] / (g["ll8"] + eps) < 1.12).fillna(False)
        g["b1_platform_low"] = g["ll8"].astype(float)

        # -----------------
        # B2: 确认启动 / 4%阳线主升
        # -----------------
        b2_trend = (
            g["lt_slope_up"]
            & (c > g["lt"])
            & (g["count_above_lt_10"] >= 7)
            & (g["hh20"] / (g["ll20"] + 1e-12) < 1.45)
        )
        b2_consolidation = (
            (g["ll6"] <= g["st"] * 1.02)
            & (g["ll6"] >= g["lt"] * 0.97)
            & (g["hh6"] / (g["ll6"] + 1e-12) < 1.18)
            & (g["small_move_5"] >= 3)
        )
        b2_trigger = (
            b2_trend
            & b2_consolidation
            & eligible_entry
            & (g["ret1"] >= 0.04)
            & (c > g["open"])
            & (g["upper_shadow_ratio"] < 0.25)
            & ((c > g["hh10_prev"]) | (h > g["hh10_prev"]))
            & (v > g["vol_ma5"] * 1.3)
            & (v < g["vol_ma5"] * 3.0)
            & (c < g["st"] * 1.08)
            & (c < g["lt"] * 1.18)
            & (g["k"] > g["d"])
            & (g["j"] > g["j"].shift(1))
            & (g["j"] < 95)
        )

        # -----------------
        # B3: 补票战法
        # B2 启动后的强势回踩再起
        # -----------------
        b2_memory = b2_trigger.rolling(10, min_periods=1).max().astype(bool)
        b3_zone = (
            ((g["range_pos_3"] <= 30) & (g["range_pos_21"] >= 85))
            | ((g["range_pos_3"] <= 20) & (g["range_pos_21"] > 80))
        )
        b3_prewarn = (
            g["lt_slope_up"]
            & ((c > g["lt"]).rolling(10, min_periods=10).sum() >= 7)
            & (g["st"] >= g["lt"] * 0.99)
            & b2_memory
            & b3_zone
            & (v <= g["vol_ma5"] * 1.1)
            & (l >= g["lt"] * 0.98)
        )
        b3_trigger = (
            b3_prewarn.shift(1, fill_value=False)
            & eligible_entry
            & (c > h.shift(1))
            & (c >= g["st"])
            & (c > g["open"])
        )

        # -----------------
        # 通用退出条件
        # -----------------
        generic_exit_flag = (
            (c < g["lt"])
            | ((c < g["st"]) & (v > g["vol_ma5"] * 1.2))
            | ((g["ret1"] < -0.07) & (c < g["open"]))
            | (
                (g["upper_shadow_ratio"] > 0.55)
                & (g["ret1"] < 0.01)
                & (g["k"] < g["d"])
                & (c < g["st"])
            )
        )
        g["b1_exit_flag"] = b1_exit_signal.astype(int)
        g["exit_flag"] = (generic_exit_flag | b1_exit_signal).astype(int)

        g["b1_trigger_raw"] = b1_core.astype(int)
        g["b1_watch_days"] = b1_probe_bars.astype(float)
        g["b1_anchor_low"] = l.where(b1_formula)
        g["b1_trigger_entry"] = b1_formula.astype(int)
        g["b1_pullback_entry"] = 0
        g["b1_core"] = b1_core.astype(int)
        g["b1_confirm"] = b1_confirm.astype(int)
        g["b1_probe_invalid"] = b1_probe_invalid.astype(int)
        g["b1_signal_high"] = b1_signal_high.astype(float)
        g["b1_signal_low"] = b1_signal_low.astype(float)
        g["b1_forbidden"] = b1_forbidden.astype(int)
        g["b1"] = b1_formula.astype(int)
        g["b1_accel_exhaust_day"] = b1_accel_exhaust_day.astype(int)
        g["b1_accel_exhaust_hard"] = b1_accel_exhaust_hard.astype(int)
        g["b1_secondary_peak_distribution"] = b1_secondary_peak_distribution.astype(int)
        g["b1_stair_dist_3d"] = b1_stair_dist_3d.astype(int)
        g["b1_stair_dist_4d"] = b1_stair_dist_4d.astype(int)
        g["b1_double_top_distribution"] = b1_double_top_distribution.astype(int)
        g["b1_weak_rebound_top"] = b1_weak_rebound_top.astype(int)
        g["b1_weak_rebound_top_count"] = b1_weak_rebound_top_count.astype(float)
        g["b1_distribution_score"] = b1_distribution_score.astype(float)
        g["b1_soft_exit_flag"] = b1_soft_exit_flag.astype(int)
        g["b1_hard_distribution_flag"] = b1_hard_distribution_flag.astype(int)
        g["b1_lt_hard_stop_flag"] = b1_lt_hard_stop_flag.astype(int)
        g["b1_st_stop_flag"] = b1_st_stop_flag.astype(int)
        g["b1_platform_established"] = b1_platform_established.astype(int)
        g["b2"] = b2_trigger.astype(int)
        g["b3"] = b3_trigger.astype(int)

        g["pattern"] = np.select(
            [g["b2"].eq(1), g["b3"].eq(1), g["b1"].eq(1)],
            ["B2", "B3", "B1"],
            default="",
        )
        g["entry_flag"] = ((g["b1"] + g["b2"] + g["b3"]) > 0).astype(int)

        # 保护位:
        # B2 -> 当天低点
        # B3 -> 预警日低点(即前一日低点)
        # B1 -> 当天低点
        g["stop_price"] = np.where(
            g["b2"].eq(1),
            l,
            np.where(
                g["b3"].eq(1),
                l.shift(1),
                np.where(g["b1"].eq(1), np.nan, l),
            ),
        )

        g["quality_score"] = (
            18.0 * _scaled_score(g["lt"] / (g["lt"].shift(3) + eps) - 1.0, 0.0, 0.03)
            + 16.0 * _scaled_score(count_above_lt_30, 20.0, 30.0)
            + 14.0 * _scaled_score(count_above_st_20, 12.0, 20.0)
            + 14.0 * _scaled_score(20.0 - g["j"], 0.0, 20.0)
            + 14.0 * _scaled_score((g["vol_ma20"] * 0.65 - v) / (g["vol_ma20"] + eps), 0.0, 0.35)
            + 12.0 * _scaled_score(dist_from_prev_high, 0.04, 0.15)
            + 12.0 * _scaled_score(0.055 - g["range_pct_prev_close"], 0.0, 0.055)
        )
        g["priority_score"] = (
            g["quality_score"]
            + 100.0 * g["b2"]
            + 80.0 * g["b3"]
            + 60.0 * g["b1"]
            + 20.0 * g["b1_confirm"]
        ).astype(float)
        g["rule_priority_score"] = g["priority_score"].astype(float)
        g["model_score_raw"] = np.nan
        g["model_score"] = np.nan
        g["b1_exit_score_tp1"] = np.nan
        g["b1_exit_score_tp2"] = np.nan
        g["b1_exit_score_tp3"] = np.nan
        g["b1_exit_score_tail"] = np.nan
        g["final_score"] = g["priority_score"].astype(float)
        g["score_source"] = "rule_only"
        g["b1_rank_active"] = 0

        return g

    out = df.groupby(level="instrument", group_keys=False).apply(_per_inst)

    cols = [
        "open", "high", "low", "close", "volume",
        "st", "lt", "vol_ma5",
        "b1_trigger_raw", "b1_watch_days",
        "b1_trigger_entry", "b1_pullback_entry",
        "b1_core", "b1_confirm", "b1_probe_invalid", "b1_signal_high", "b1_signal_low", "b1_forbidden", "b1_exit_flag",
        "b1_accel_exhaust_day", "b1_accel_exhaust_hard", "b1_secondary_peak_distribution",
        "b1_stair_dist_3d", "b1_stair_dist_4d", "b1_double_top_distribution", "b1_weak_rebound_top", "b1_weak_rebound_top_count",
        "b1_distribution_score", "b1_soft_exit_flag", "b1_hard_distribution_flag",
        "b1_lt_hard_stop_flag", "b1_st_stop_flag", "b1_platform_established", "b1_platform_low",
        "b1", "b2", "b3",
        "entry_flag", "exit_flag", "stop_price",
        "priority_score", "rule_priority_score", "quality_score", "model_score_raw", "model_score",
        "b1_exit_score_tp1", "b1_exit_score_tp2", "b1_exit_score_tp3", "b1_exit_score_tail",
        "final_score", "score_source", "b1_rank_active", "pattern",
    ]
    signal = out[cols].copy()
    signal["score"] = signal["priority_score"]  # 兼容 qlib signal 的 score 列
    return signal


# =========================
# 4) Qlib 自定义策略
# =========================

@dataclass
class StrategyConfig:
    mode: Literal["B1", "B2", "B3", "COMBINED"] = "COMBINED"
    max_holdings: int = 8
    risk_degree: float = 0.95
    max_holding_days: int = 15
    min_swap_score_gap: float = 15.0


class PatternSignalStrategy(WeightStrategyBase):
    """
    基于预计算 signal DataFrame 的持仓策略:
    - 支持单独回测 B1 / B2 / B3
    - 支持 COMBINED 合并回测
    - 等权持仓
    """

    def __init__(self, *, config: StrategyConfig, signal: pd.DataFrame, **kwargs):
        super().__init__(signal=signal, risk_degree=config.risk_degree, **kwargs)
        self.cfg = config
        self.entry_meta: Dict[str, Dict[str, object]] = {}

    def generate_target_weight_position(self, score, current, trade_start_time, trade_end_time):
        if score is None or len(score) == 0:
            return {}

        if isinstance(score, pd.Series):
            score = score.to_frame().T

        day_df = score.copy()
        if "instrument" in day_df.columns:
            day_df = day_df.set_index("instrument")
        day_df.index = day_df.index.astype(str)

        current_codes = [str(x) for x in current.get_stock_list()]
        keep = []

        # 1) 先判断当前持仓是否继续保留
        for code in current_codes:
            row = day_df.loc[code] if code in day_df.index else None
            meta = self.entry_meta.get(code, {})
            pattern = str(meta.get("pattern", self.cfg.mode or ""))
            if row is None:
                exit_now = True
            elif pattern == "B1":
                exit_now = bool(row.get("b1_exit_flag", row.get("exit_flag", 0)))
            else:
                exit_now = bool(row.get("exit_flag", 0))

            if row is not None and not exit_now and pattern != "B1":
                stop_price = meta.get("stop_price")
                if stop_price is not None and float(row.get("close", float("inf"))) < float(stop_price):
                    exit_now = True

                get_stock_count = getattr(current, "get_stock_count", None)
                if callable(get_stock_count):
                    hold_days = int(get_stock_count(code))
                    if hold_days >= self.cfg.max_holding_days and float(row.get("close", 0.0)) < float(row.get("st", 0.0)):
                        exit_now = True

            if not exit_now:
                keep.append(code)
            else:
                self.entry_meta.pop(code, None)

        # 2) 再筛选新开仓候选
        candidate_df = day_df.loc[day_df.index.difference(keep)].copy()

        if self.cfg.mode == "B1":
            candidate_df = candidate_df[candidate_df["b1"] == 1]
        elif self.cfg.mode == "B2":
            candidate_df = candidate_df[candidate_df["b2"] == 1]
        elif self.cfg.mode == "B3":
            candidate_df = candidate_df[candidate_df["b3"] == 1]
        else:
            candidate_df = candidate_df[candidate_df["entry_flag"] == 1]

        candidate_df = candidate_df.sort_values("priority_score", ascending=False)

        if self.cfg.mode == "B1":
            main_limit = min(int(self.cfg.max_holdings), B1_ACTIVE_MAIN_POSITION_LIMIT)
            slots = max(main_limit - len(keep), 0)
            target_codes = keep + [str(code) for code in candidate_df.head(slots).index.tolist()]
            new_codes = [code for code in target_codes if code not in keep]
            target_ratios: dict[str, float] = {}
            for code in keep:
                target_ratios[code] = float(self.entry_meta.get(code, {}).get("position_ratio", B1_STANDARD_POSITION_RATIO))
            for code in new_codes:
                row = candidate_df.loc[code]
                position_ratio = resolve_b1_position_ratio(row)
                self.entry_meta[code] = {
                    "stop_price": None,
                    "pattern": row.get("pattern", ""),
                    "entry_dt": trade_start_time,
                    "position_ratio": position_ratio,
                }
                target_ratios[code] = position_ratio
            if not target_codes:
                return {}
            total_ratio = sum(target_ratios.get(code, B1_STANDARD_POSITION_RATIO) for code in target_codes)
            if total_ratio <= 0:
                return {}
            scale = min(1.0 / total_ratio, 1.0)
            return {code: target_ratios.get(code, B1_STANDARD_POSITION_RATIO) * scale for code in target_codes}

        keep_scores = []
        for code in keep:
            row = day_df.loc[code] if code in day_df.index else None
            score_value = float(row.get("priority_score", -np.inf)) if row is not None else -np.inf
            keep_scores.append((code, score_value))

        desired = {code for code, _ in sorted(keep_scores, key=lambda item: item[1], reverse=True)}
        for code in candidate_df.index.tolist():
            desired.add(str(code))
            ranked = sorted(
                desired,
                key=lambda symbol: float(day_df.loc[symbol].get("priority_score", -np.inf)) if symbol in day_df.index else -np.inf,
                reverse=True,
            )
            desired = set(ranked[: self.cfg.max_holdings])

        if candidate_df.empty:
            target_codes = sorted(desired, key=lambda symbol: keep.index(symbol) if symbol in keep else len(keep))
        else:
            strongest_candidate_score = float(candidate_df.iloc[0].get("priority_score", -np.inf))
            weakest_keep_score = min((score_value for _, score_value in keep_scores), default=np.inf)
            if (
                keep_scores
                and strongest_candidate_score - weakest_keep_score < float(self.cfg.min_swap_score_gap)
                and len(keep) >= self.cfg.max_holdings
            ):
                target_codes = keep
            else:
                target_codes = [
                    code for code, _ in sorted(
                        [(symbol, float(day_df.loc[symbol].get("priority_score", -np.inf))) for symbol in desired],
                        key=lambda item: item[1],
                        reverse=True,
                    )
                ]

        new_codes = [code for code in target_codes if code not in keep]

        for code in new_codes:
            row = candidate_df.loc[code]
            self.entry_meta[code] = {
                "stop_price": float(row.get("stop_price", np.nan)),
                "pattern": row.get("pattern", ""),
                "entry_dt": trade_start_time,
            }
        if not target_codes:
            return {}

        weight = 1.0 / len(target_codes)
        return {code: weight for code in target_codes}


# =========================
# 5) 回测入口
# =========================

def run_pattern_backtest(
    provider_uri: str,
    region: str = REG_CN,
    instruments: str = "all",
    start_time: str = "2018-01-01",
    end_time: str = "2024-12-31",
    benchmark: str = "SH000300",
    deal_price: str = "open",  # 默认按 T-1 信号、T 日开盘执行
    mode: Literal["B1", "B2", "B3", "COMBINED"] = "COMBINED",
    max_holdings: int = 8,
    risk_degree: float = 0.95,
    max_holding_days: int = 15,
    account: float = 10_000_000,
):
    init_qlib(provider_uri=provider_uri, region=region)

    ohlcv = fetch_ohlcv(
        instruments=instruments,
        start_time=start_time,
        end_time=end_time,
        freq="day",
    )
    feats = build_indicators(ohlcv)
    signal = build_pattern_signals(feats)

    strategy = PatternSignalStrategy(
        config=StrategyConfig(
            mode=mode,
            max_holdings=max_holdings,
            risk_degree=risk_degree,
            max_holding_days=max_holding_days,
        ),
        signal=signal,
    )

    report_df, positions = backtest_daily(
        start_time=start_time,
        end_time=end_time,
        strategy=strategy,
        account=account,
        benchmark=benchmark,
        exchange_kwargs={
            "freq": "day",
            "limit_threshold": 0.095,  # A股普通涨跌停 9.5% 容错写法
            "deal_price": deal_price,
            "open_cost": 0.0005,
            "close_cost": 0.0015,
            "min_cost": 5,
        },
    )

    # 超额收益分析
    excess = report_df["return"] - report_df["bench"]
    risk = risk_analysis(excess, freq="day")

    return {
        "ohlcv": ohlcv,
        "features": feats,
        "signal": signal,
        "report": report_df,
        "positions": positions,
        "risk": risk,
    }


# =========================
# 6) 示例
# =========================

if __name__ == "__main__":
    result = run_pattern_backtest(
        provider_uri="~/.qlib/qlib_data/cn_data",
        region=REG_CN,
        instruments="all",          # 或 "csi300" / "csi500"
        start_time="2019-01-01",
        end_time="2024-12-31",
        benchmark="SH000300",
        deal_price="open",          # 默认按 T-1 信号、T 日开盘执行
        mode="COMBINED",            # "B1" / "B2" / "B3" / "COMBINED"
        max_holdings=8,
        risk_degree=0.95,
        max_holding_days=15,
        account=10_000_000,
    )

    print("===== Risk Analysis =====")
    print(result["risk"])
    print("\n===== Report Tail =====")
    print(result["report"].tail())
