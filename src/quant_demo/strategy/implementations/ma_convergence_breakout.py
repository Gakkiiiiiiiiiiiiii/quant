"""均线粘合放倍量突破策略 (MA Convergence Volume Breakout)

在趋势向上的股票中，捕捉MA5/MA10/MA20均线粘合后的放倍量突破买点。

卖出规则：
  - 固定止损 -5%
  - 收盘跌破 MA20 卖出
  - 收盘跌破信号日最低价卖出
  - 持有3日仍未盈利卖出

过滤规则：
  - 剔除 ST/*ST/退市整理
  - 剔除信号出现日涨停的股票
  - 剔除上市不足 min_list_days 天的股票
  - 剔除20日均成交额不足的股票
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from quant_demo.strategy.base import StrategyContext

PROJECT_ROOT = Path(__file__).resolve().parents[4]


@dataclass(slots=True)
class HoldInfo:
    buy_date: date
    buy_price: float
    signal_low: float
    signal_date: date


@dataclass(slots=True)
class MAConvergenceConfig:
    convergence_limit: float = 2.5
    volume_multiple: float = 2.0
    max_hold_num: int = 10
    min_avg_amount_20: float = 30_000_000.0
    hold_days: int = 3
    stop_loss: float = -0.05
    min_list_days: int = 80
    buy_slippage_bps: float = 10.0
    sell_slippage_bps: float = 10.0
    filter_st: bool = True
    filter_limit_up: bool = True


class MAConvergenceBreakoutStrategy:
    name = "ma_convergence_breakout"

    def __init__(self, lookback_days: int = 260, top_n: int = 30, extra: dict[str, Any] | None = None) -> None:
        payload = extra or {}
        self.lookback_days = lookback_days
        self.top_n = top_n
        self.cfg = MAConvergenceConfig(
            convergence_limit=float(payload.get("convergence_limit", 2.5)),
            volume_multiple=float(payload.get("volume_multiple", 2.0)),
            max_hold_num=int(payload.get("max_hold_num", 10)),
            min_avg_amount_20=float(payload.get("min_avg_amount_20", 30_000_000)),
            hold_days=int(payload.get("hold_days", 3)),
            stop_loss=float(payload.get("stop_loss", -0.05)),
            min_list_days=int(payload.get("min_list_days", 80)),
            buy_slippage_bps=float(payload.get("buy_slippage_bps", 10.0)),
            sell_slippage_bps=float(payload.get("sell_slippage_bps", 10.0)),
            filter_st=bool(payload.get("filter_st", True)),
            filter_limit_up=bool(payload.get("filter_limit_up", True)),
        )
        self._hold_info: dict[str, HoldInfo] = {}
        self._initialized = False
        self._all_signals: dict[date, dict[str, dict[str, Any]]] = {}
        self._st_symbols: set[str] = set()
        self._list_days_map: dict[str, date] = {}
        self._sorted_dates: list[date] = []
        self._history_df: pd.DataFrame | None = None
        self._ma20_cache: dict[str, float] = {}

    def _precompute_signals(self, history: pd.DataFrame) -> None:
        """一次性预计算所有日期的均线粘合放倍量信号"""
        cfg = self.cfg
        df = history.copy()

        # 加载ST信息
        inst_path = PROJECT_ROOT / "data" / "parquet" / "joinquant_microcap_instruments.parquet"
        if inst_path.exists():
            inst = pd.read_parquet(inst_path)
            for _, row in inst.iterrows():
                name = str(row.get("instrument_name", ""))
                sym = str(row.get("symbol", ""))
                if "ST" in name.upper() or "退" in name:
                    self._st_symbols.add(sym)
                open_date = row.get("open_date")
                if open_date is not None:
                    try:
                        if isinstance(open_date, str):
                            open_date = pd.Timestamp(open_date)
                        if hasattr(open_date, "date"):
                            open_date = open_date.date()
                        if isinstance(open_date, pd.Timestamp):
                            open_date = open_date.date()
                        self._list_days_map[sym] = open_date
                    except (ValueError, TypeError):
                        pass

        # 按 symbol 分组，向量化计算均线指标
        all_signals: dict[date, dict[str, dict[str, Any]]] = {}

        grouped = df.groupby("symbol")
        for symbol, group in grouped:
            ordered = group.sort_values("trading_date").reset_index(drop=True)
            n = len(ordered)
            if n < 21:
                continue

            close = ordered["close"].values
            volume = ordered["volume"].values
            highs = ordered["high"].values
            lows = ordered["low"].values
            opens = ordered["open"].values
            amounts = ordered["amount"].values if "amount" in ordered.columns else None
            dates = ordered["trading_date"].values

            # ST 过滤
            if cfg.filter_st and symbol in self._st_symbols:
                continue

            # 向量化计算均线
            ma5 = pd.Series(close).rolling(5, min_periods=5).mean().values
            ma10 = pd.Series(close).rolling(10, min_periods=10).mean().values
            ma20_s = pd.Series(close).rolling(20, min_periods=20).mean()
            ma20 = ma20_s.values
            ma60 = pd.Series(close).rolling(60, min_periods=60).mean().values
            ma120 = pd.Series(close).rolling(120, min_periods=120).mean().values
            ma240 = pd.Series(close).rolling(240, min_periods=240).mean().values

            prev_close = np.roll(close, 1)
            prev_close[0] = np.nan

            # 放倍量: volume >= volume_prev * 2
            vol_prev = np.roll(volume, 1)
            vol_prev[0] = 0
            volume_signal = (volume >= cfg.volume_multiple * vol_prev) & (vol_prev > 0)

            # avg amount 20
            if amounts is not None:
                avg_amt20 = pd.Series(amounts).rolling(20, min_periods=20).mean().values
                amount_filter = avg_amt20 >= cfg.min_avg_amount_20
            else:
                amount_filter = np.ones(n, dtype=bool)

            # 涨停过滤预计算
            limit_up_threshold = 0.098
            if symbol.startswith("688") or symbol.startswith("8") or symbol.startswith("4"):
                limit_up_threshold = 0.198
            pct_change = np.where(prev_close > 0, (close - prev_close) / prev_close, 0)
            is_limit_up = (close >= highs) & (pct_change >= limit_up_threshold)

            # 逐日计算信号
            for i in range(n):
                if not volume_signal[i]:
                    continue
                if not amount_filter[i]:
                    continue
                if cfg.filter_limit_up and is_limit_up[i]:
                    continue
                if np.isnan(ma20[i]) or np.isnan(ma5[i]) or np.isnan(ma10[i]):
                    continue
                if close[i] <= 0 or volume[i] <= 0:
                    continue

                # 趋势过滤
                trend_ok = False
                if i >= 259 and not np.isnan(ma240[i]):
                    # MA240 向上
                    ma240_now = ma240[i]
                    ma240_5 = ma240[i - 5] if not np.isnan(ma240[i - 5]) else None
                    ma240_20 = ma240[i - 20] if not np.isnan(ma240[i - 20]) else None
                    if ma240_5 is not None and ma240_20 is not None:
                        trend_ok = ma240_now > ma240_5 and ma240_now > ma240_20
                elif i >= 139:
                    ma120_up = False
                    ma60_up = False
                    if not np.isnan(ma120[i]) and i >= 124 and not np.isnan(ma120[i - 5]) and not np.isnan(ma120[i - 20]):
                        ma120_up = ma120[i] > ma120[i - 5] and ma120[i] > ma120[i - 20]
                    if i >= 79 and not np.isnan(ma60[i]) and not np.isnan(ma60[i - 5]) and not np.isnan(ma60[i - 20]):
                        ma60_up = ma60[i] > ma60[i - 5] and ma60[i] > ma60[i - 20]
                    trend_ok = ma120_up or ma60_up
                elif i >= 79 and not np.isnan(ma60[i]) and not np.isnan(ma60[i - 5]) and not np.isnan(ma60[i - 20]):
                    trend_ok = ma60[i] > ma60[i - 5] and ma60[i] > ma60[i - 20]

                if not trend_ok:
                    continue

                # 当前均线粘合
                m5, m10, m20_val = ma5[i], ma10[i], ma20[i]
                cur_max = max(m5, m10, m20_val)
                cur_min = min(m5, m10, m20_val)
                if cur_min <= 0:
                    continue
                cur_nhd = (cur_max - cur_min) / cur_min * 100
                current_converge = cur_nhd <= cfg.convergence_limit and close[i] > cur_max

                # 扣抵均线粘合
                deduct_converge = False
                kd_nhd_val = float("inf")
                if i >= 19:
                    # KDM5 = MA5 + (close[i] - close[i-4]) / 5
                    kdm5 = m5 + (close[i] - close[i - 4]) / 5
                    # KDM10 = MA10 + (close[i] - close[i-9]) / 10
                    kdm10 = m10 + (close[i] - close[i - 9]) / 10 if i >= 9 else None
                    # KDM20 = MA20 + (close[i] - close[i-19]) / 20
                    kdm20 = m20_val + (close[i] - close[i - 19]) / 20

                    if kdm10 is not None:
                        kd_max = max(kdm5, kdm10, kdm20)
                        kd_min = min(kdm5, kdm10, kdm20)
                        if kd_min > 0:
                            kd_nhd_val = (kd_max - kd_min) / kd_min * 100
                            deduct_converge = kd_nhd_val <= cfg.convergence_limit and close[i] > kd_max

                ma_signal = current_converge or deduct_converge
                if not ma_signal:
                    continue

                nhd = min(
                    cur_nhd if current_converge else float("inf"),
                    kd_nhd_val if deduct_converge else float("inf"),
                )

                trading_date = dates[i]
                if trading_date not in all_signals:
                    all_signals[trading_date] = {}

                all_signals[trading_date][symbol] = {
                    "nhd": float(nhd),
                    "signal_low": float(lows[i]),
                    "signal_high": float(highs[i]),
                    "close": float(close[i]),
                    "open": float(opens[i]),
                    "prev_close": float(prev_close[i]) if not np.isnan(prev_close[i]) else float(close[i]),
                    "volume": float(volume[i]),
                    "ma20": float(m20_val),
                    "avg_amount_20": float(avg_amt20[i]) if amounts is not None else 0,
                }

        self._all_signals = all_signals
        self._sorted_dates = sorted(all_signals.keys())
        self._history_df = history
        self._initialized = True

    def target_weights(self, context: StrategyContext) -> dict[str, float]:
        trading_date = context.trading_date
        cfg = self.cfg
        history = context.history

        # 首次调用时预计算信号
        if not self._initialized:
            self._precompute_signals(history)

        prices = context.prices
        account_state = context.account_state

        # T+1延迟：使用前一交易日的信号
        # 找到当前日期在 sorted_dates 中的位置
        if not self._sorted_dates:
            return {}
        # binary search for prev_date
        import bisect
        idx = bisect.bisect_right(self._sorted_dates, trading_date)
        if idx < 2:
            return {}
        prev_date = self._sorted_dates[idx - 2]  # T-1 日（信号日）
        # 实际上找到有信号的最近前一日
        # 从 idx-1 开始往前找有信号的日期
        prev_date = None
        for j in range(idx - 1, -1, -1):
            if self._sorted_dates[j] in self._all_signals:
                prev_date = self._sorted_dates[j]
                break
        if prev_date is None:
            return {}

        # 获取前一日的信号
        prev_signals = self._all_signals.get(prev_date, {})

        # 卖出判断
        sell_set: set[str] = set()

        for symbol in list(account_state.positions.keys()):
            if symbol not in self._hold_info:
                continue

            hi = self._hold_info[symbol]
            price_dec = prices.get(symbol)
            current_price = float(price_dec) if price_dec is not None else float(account_state.positions[symbol].last_price)
            buy_price = hi.buy_price
            hold_days_count = (trading_date - hi.buy_date).days

            # 止损 -5%
            if current_price <= buy_price * (1 + cfg.stop_loss):
                sell_set.add(symbol)
                continue

            # 跌破信号日最低价
            if current_price < hi.signal_low:
                sell_set.add(symbol)
                continue

            # 持有N日仍未盈利
            if hold_days_count >= cfg.hold_days and current_price <= buy_price:
                sell_set.add(symbol)
                continue

            # 跌破MA20 — 从当前日信号取ma20，没有则从历史计算
            curr_date_signals = self._all_signals.get(trading_date, {})
            if symbol in curr_date_signals:
                sig_ma20 = curr_date_signals[symbol].get("ma20")
                if sig_ma20 and current_price < sig_ma20:
                    sell_set.add(symbol)
                    continue
            else:
                # 该symbol当天没有信号，从缓存历史中计算MA20
                if self._history_df is not None:
                    sym_h = self._history_df[self._history_df["symbol"] == symbol]
                    sym_h = sym_h[sym_h["trading_date"] <= trading_date].sort_values("trading_date")
                    if len(sym_h) >= 20:
                        ma20_val = sym_h["close"].iloc[-20:].mean()
                        if current_price < ma20_val:
                            sell_set.add(symbol)
                            continue

        for s in sell_set:
            self._hold_info.pop(s, None)

        # 买入候选（来自前一日信号，T+1延迟）
        buy_candidates: list[tuple[str, float]] = []
        for symbol, sig in prev_signals.items():
            if symbol in sell_set or symbol in account_state.positions:
                continue

            # 上市天数不足
            if cfg.min_list_days > 0 and symbol in self._list_days_map:
                days = (prev_date - self._list_days_map[symbol]).days
                if days < cfg.min_list_days:
                    continue

            buy_candidates.append((symbol, sig["nhd"]))

        # 按粘合度排序，取 max_hold_num - 当前持仓数
        buy_candidates.sort(key=lambda x: x[1])
        existing_positions = len(account_state.positions) - len(sell_set)
        max_new = max(0, cfg.max_hold_num - existing_positions)
        buy_candidates = buy_candidates[:max_new]

        # 记录 hold_info
        for symbol, nhd in buy_candidates:
            sig = prev_signals[symbol]
            price_dec = prices.get(symbol)
            price = float(price_dec) if price_dec is not None else sig["close"]
            if price > 0:
                self._hold_info[symbol] = HoldInfo(
                    buy_date=trading_date,
                    buy_price=price,
                    signal_low=sig["signal_low"],
                    signal_date=prev_date,
                )

        # 构建权重
        weights: dict[str, float] = {}
        for s in sell_set:
            weights[s] = 0.0

        hold_symbols = [s for s in account_state.positions if s not in sell_set]
        new_symbols = [s for s, _ in buy_candidates]
        all_symbols = hold_symbols + [s for s in new_symbols if s not in hold_symbols]

        if not all_symbols and not sell_set:
            return {}

        if all_symbols:
            w = 1.0 / len(all_symbols)
            for s in all_symbols:
                weights[s] = round(w, 4)

        return weights