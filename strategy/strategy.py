from __future__ import annotations

from dataclasses import dataclass
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


def build_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    基于 OHLCV 计算你三个战法要用的全部基础指标
    """
    def _per_inst(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_index(level="datetime").copy()
        c = g["close"]
        h = g["high"]
        l = g["low"]
        v = g["volume"]

        g["ret1"] = c.pct_change()

        # 你前面统一的两根线
        g["ma14"] = c.rolling(14, min_periods=14).mean()
        g["ma28"] = c.rolling(28, min_periods=28).mean()
        g["ma57"] = c.rolling(57, min_periods=57).mean()
        g["ma114"] = c.rolling(114, min_periods=114).mean()
        g["st"] = _ema(_ema(c, 10), 10)  # 知行短期趋势线
        g["lt"] = (g["ma14"] + g["ma28"] + g["ma57"] + g["ma114"]) / 4.0  # 知行多空线

        g["vol_ma5"] = v.rolling(5, min_periods=5).mean()

        # KDJ(9,3,3)
        ll9 = l.rolling(9, min_periods=9).min()
        hh9 = h.rolling(9, min_periods=9).max()
        rsv = (c - ll9) / (hh9 - ll9 + 1e-12) * 100.0
        g["k"] = _sma_cn(rsv, 3, 1)
        g["d"] = _sma_cn(g["k"], 3, 1)
        g["j"] = 3 * g["k"] - 2 * g["d"]

        # 常用滚动区间
        g["ll6"] = l.rolling(6, min_periods=6).min()
        g["hh6"] = h.rolling(6, min_periods=6).max()
        g["hh10_prev"] = h.rolling(10, min_periods=10).max().shift(1)
        g["hh20"] = h.rolling(20, min_periods=20).max()
        g["ll20"] = l.rolling(20, min_periods=20).min()

        # 补票战法的区间位置值
        g["range_pos_3"] = 100.0 * (c - l.rolling(3, min_periods=3).min()) / (
            c.rolling(3, min_periods=3).max() - l.rolling(3, min_periods=3).min() + 1e-12
        )
        g["range_pos_21"] = 100.0 * (c - l.rolling(21, min_periods=21).min()) / (
            c.rolling(21, min_periods=21).max() - l.rolling(21, min_periods=21).min() + 1e-12
        )

        g["count_above_lt_10"] = (c > g["lt"]).rolling(10, min_periods=10).sum()
        g["small_move_5"] = (c.pct_change().abs() < 0.03).rolling(5, min_periods=5).sum()

        g["st_slope_up"] = g["st"] >= g["st"].shift(1)
        g["lt_slope_up"] = g["lt"] >= g["lt"].shift(1)

        g["upper_shadow_ratio"] = (h - c) / (h - l + 1e-12)
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

        # -----------------
        # B1: 结构成形 / 右侧启动
        # -----------------
        b1_trend = (
            (c > g["lt"])
            & g["lt_slope_up"]
            & (g["st"] > g["lt"] * 0.99)
            & g["st_slope_up"]
        )
        b1_setup = (
            (g["ll6"] <= g["st"] * 1.02)
            & (g["ll6"] >= g["lt"] * 0.97)
            & (g["hh6"] / (g["ll6"] + 1e-12) < 1.18)
            & (g["small_move_5"] >= 3)
        )
        b1_trigger = (
            b1_trend
            & b1_setup
            & (c > g["st"])
            & (v > g["vol_ma5"] * 1.5)
            & (g["k"] > g["d"])
            & (g["j"] > g["j"].shift(1))
            & (g["j"] < 95)
        )
        b1_trigger_index = pd.Series(
            np.where(b1_trigger, np.arange(len(g), dtype=float), np.nan),
            index=g.index,
        ).ffill()
        b1_trigger_low = pd.Series(
            np.where(b1_trigger, l.astype(float), np.nan),
            index=g.index,
        ).ffill()
        b1_days_since_trigger = pd.Series(np.arange(len(g), dtype=float), index=g.index) - b1_trigger_index
        b1_watch_window = b1_trigger_index.notna() & b1_days_since_trigger.between(1, 3)
        b1_pullback_entry = (
            b1_watch_window
            & (l <= g["st"] * 1.02)
            & (l >= g["lt"] * 0.99)
            & (c >= g["st"] * 0.995)
            & (c <= h.shift(1) * 1.01)
            & (v <= v.shift(1).fillna(v))
            & (v <= g["vol_ma5"] * 1.1)
        )

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
            b3_prewarn.shift(1).fillna(False)
            & (c > h.shift(1))
            & (c >= g["st"])
            & (c > g["open"])
        )

        # -----------------
        # 通用退出条件
        # -----------------
        g["exit_flag"] = (
            (c < g["lt"])
            | ((c < g["st"]) & (v > g["vol_ma5"] * 1.2))
            | ((g["ret1"] < -0.07) & (c < g["open"]))
        ).astype(int)

        g["b1_trigger_raw"] = b1_trigger.astype(int)
        g["b1_watch_days"] = b1_days_since_trigger.where(b1_watch_window)
        g["b1_anchor_low"] = b1_trigger_low.where(b1_watch_window)
        g["b1"] = b1_pullback_entry.astype(int)
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
            np.where(g["b3"].eq(1), l.shift(1), np.where(g["b1"].eq(1), g["b1_anchor_low"], l)),
        )

        # 排序分数: B2 > B3 > B1
        g["priority_score"] = (
            100 * g["b2"]
            + 80 * g["b3"]
            + 60 * g["b1"]
            + 10 * np.clip(g["ret1"].fillna(0), 0, 0.12)
            + np.clip((v / (g["vol_ma5"] + 1e-12)).fillna(0), 0, 3)
        )

        return g

    out = df.groupby(level="instrument", group_keys=False).apply(_per_inst)

    cols = [
        "open", "high", "low", "close", "volume",
        "st", "lt", "vol_ma5",
        "b1_trigger_raw", "b1_watch_days",
        "b1", "b2", "b3",
        "entry_flag", "exit_flag", "stop_price",
        "priority_score", "pattern",
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
    max_holdings: int = 10
    risk_degree: float = 0.95
    max_holding_days: int = 15


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
            exit_now = row is None or bool(row.get("exit_flag", 0))
            meta = self.entry_meta.get(code, {})

            if row is not None and not exit_now:
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

        slots = max(self.cfg.max_holdings - len(keep), 0)
        new_codes = candidate_df.head(slots).index.tolist()

        for code in new_codes:
            row = candidate_df.loc[code]
            self.entry_meta[code] = {
                "stop_price": float(row.get("stop_price", np.nan)),
                "pattern": row.get("pattern", ""),
                "entry_dt": trade_start_time,
            }

        target_codes = keep + new_codes
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
    deal_price: str = "close",  # 要做 next-open 执行就改成 "open"
    mode: Literal["B1", "B2", "B3", "COMBINED"] = "COMBINED",
    max_holdings: int = 10,
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
        deal_price="close",         # 若要 next-open，改成 "open"
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
