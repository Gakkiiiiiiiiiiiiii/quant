from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
from sqlalchemy import delete
from sqlalchemy.orm import sessionmaker

from quant_demo.adapters.qmt.bridge_client import QmtBridgeClient
from quant_demo.audit.report_service import AuditReportService
from quant_demo.core.config import AppSettings, StrategySettings
from quant_demo.db.models import (
    AssetSnapshotModel,
    AuditLogModel,
    OrderEventModel,
    OrderIntentModel,
    OrderModel,
    PositionSnapshotModel,
    RiskDecisionModel,
    TradeModel,
)
from quant_demo.experiment.evaluator import EvaluationResult, Evaluator

PROJECT_ROOT = Path(__file__).resolve().parents[3]


@dataclass(slots=True)
class MicrocapStrategyConfig:
    target_hold_num: int = 35
    buy_rank: int = 35
    keep_rank: int = 60
    query_limit: int = 1500
    min_list_days: int = 60
    min_avg_money_20: float = 8_000_000.0
    cash_buffer: float = 0.01
    min_price_floor: float = 1.0
    max_price_normal_hard: float = 50.0
    max_price_star_hard: float = 25.0
    max_overweight_ratio: float = 1.15
    benchmark_symbol: str = ""
    benchmark_name: str = ""
    hedge_symbol: str = ""
    hedge_name: str = ""
    hedge_history_adjustment: str = "none"
    seasonal_hedge_schedule: dict[int, float] = field(default_factory=dict)
    buy_slippage_bps: float = 35.0
    sell_slippage_bps: float = 35.0
    max_trade_volume_ratio: float = 0.03
    instrument_cache_path: str = "data/parquet/joinquant_microcap_instruments.parquet"
    capital_cache_path: str = "data/parquet/joinquant_microcap_capital.parquet"

    @classmethod
    def from_strategy_settings(cls, settings: StrategySettings) -> "MicrocapStrategyConfig":
        payload = settings.extra or {}
        return cls(
            target_hold_num=max(1, int(payload.get("target_hold_num", 35))),
            buy_rank=max(1, int(payload.get("buy_rank", 35))),
            keep_rank=max(1, int(payload.get("keep_rank", 60))),
            query_limit=max(50, int(payload.get("query_limit", 1500))),
            min_list_days=max(0, int(payload.get("min_list_days", 60))),
            min_avg_money_20=float(payload.get("min_avg_money_20", 8_000_000.0)),
            cash_buffer=max(0.0, min(float(payload.get("cash_buffer", 0.01)), 0.2)),
            min_price_floor=max(0.01, float(payload.get("min_price_floor", 1.0))),
            max_price_normal_hard=max(0.01, float(payload.get("max_price_normal_hard", 50.0))),
            max_price_star_hard=max(0.01, float(payload.get("max_price_star_hard", 25.0))),
            max_overweight_ratio=max(1.0, float(payload.get("max_overweight_ratio", 1.15))),
            benchmark_symbol=str(payload.get("benchmark_symbol", "") or "").strip(),
            benchmark_name=str(payload.get("benchmark_name", "") or "").strip(),
            hedge_symbol=str(payload.get("hedge_symbol", "") or "").strip(),
            hedge_name=str(payload.get("hedge_name", "") or "").strip(),
            hedge_history_adjustment=str(payload.get("hedge_history_adjustment", "none") or "none").strip(),
            seasonal_hedge_schedule=_parse_seasonal_hedge_schedule(payload),
            buy_slippage_bps=max(0.0, min(float(payload.get("buy_slippage_bps", 35.0)), 500.0)),
            sell_slippage_bps=max(0.0, min(float(payload.get("sell_slippage_bps", 35.0)), 500.0)),
            max_trade_volume_ratio=max(0.0, min(float(payload.get("max_trade_volume_ratio", 0.03)), 1.0)),
            instrument_cache_path=str(payload.get("instrument_cache_path", "data/parquet/joinquant_microcap_instruments.parquet")),
            capital_cache_path=str(payload.get("capital_cache_path", "data/parquet/joinquant_microcap_capital.parquet")),
        )


def _batched(items: Iterable[str], size: int) -> Iterable[list[str]]:
    batch: list[str] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _resolve_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return (PROJECT_ROOT / path).resolve()


def _clip_ratio(value: Any) -> float:
    return max(0.0, min(float(value), 1.0))


def _parse_seasonal_hedge_schedule(payload: dict[str, Any]) -> dict[int, float]:
    schedule: dict[int, float] = {}
    raw_schedule = payload.get("seasonal_hedge_schedule")
    if isinstance(raw_schedule, dict):
        for raw_month, raw_ratio in raw_schedule.items():
            try:
                month = int(raw_month)
            except (TypeError, ValueError):
                continue
            if 1 <= month <= 12:
                ratio = _clip_ratio(raw_ratio)
                if ratio > 0:
                    schedule[month] = ratio
        return dict(sorted(schedule.items()))

    raw_months = payload.get("seasonal_hedge_months") or []
    if not isinstance(raw_months, (list, tuple, set)):
        return {}
    ratio = _clip_ratio(payload.get("seasonal_hedge_ratio", 0.0))
    if ratio <= 0:
        return {}
    for raw_month in raw_months:
        try:
            month = int(raw_month)
        except (TypeError, ValueError):
            continue
        if 1 <= month <= 12:
            schedule[month] = ratio
    return dict(sorted(schedule.items()))


def is_star_stock(symbol: str) -> bool:
    return str(symbol or "").upper().startswith("688")


def is_beijing_stock(symbol: str) -> bool:
    raw = str(symbol or "").upper()
    if raw.startswith(("4", "8")):
        return True
    return raw.endswith(".BJ") or raw.endswith(".XBJ")


def is_b_share(symbol: str) -> bool:
    raw = str(symbol or "").upper()
    return raw.startswith("200") or raw.startswith("900")


def get_min_buy_amount(symbol: str) -> int:
    return 200 if is_star_stock(symbol) else 100


def estimate_min_order_cost(symbol: str, price: float) -> float:
    return float(price) * get_min_buy_amount(symbol) * 1.002


def normalize_buy_amount(symbol: str, raw_amount: int) -> int:
    raw_amount = int(max(0, raw_amount))
    if raw_amount <= 0:
        return 0
    if is_star_stock(symbol):
        return raw_amount if raw_amount >= 200 else 0
    return (raw_amount // 100) * 100


def normalize_sell_amount(symbol: str, raw_amount: int, current_amount: int) -> int:
    raw_amount = int(max(0, raw_amount))
    current_amount = int(max(0, current_amount))
    if raw_amount <= 0 or current_amount <= 0:
        return 0
    if is_star_stock(symbol):
        if raw_amount >= 200:
            return min(raw_amount, current_amount)
        if current_amount < 200 and raw_amount >= current_amount:
            return current_amount
        return 0
    lot_amount = (raw_amount // 100) * 100
    if lot_amount >= 100:
        return min(lot_amount, current_amount)
    if current_amount < 100 and raw_amount >= current_amount:
        return current_amount
    return 0


def adjust_target_amount_for_rules(symbol: str, current_amount: int, raw_target_amount: int) -> int:
    current_amount = int(max(0, current_amount))
    raw_target_amount = int(max(0, raw_target_amount))
    if raw_target_amount == current_amount:
        return current_amount
    if raw_target_amount > current_amount:
        buy_delta = raw_target_amount - current_amount
        valid_buy_delta = normalize_buy_amount(symbol, buy_delta)
        if valid_buy_delta <= 0:
            return current_amount
        return current_amount + valid_buy_delta
    sell_delta = current_amount - raw_target_amount
    valid_sell_delta = normalize_sell_amount(symbol, sell_delta, current_amount)
    if valid_sell_delta <= 0:
        return current_amount
    return max(0, current_amount - valid_sell_delta)


def calc_target_amount_by_value(symbol: str, target_value: float, price: float) -> int:
    if price <= 0:
        return 0
    raw_amount = int(float(target_value) / float(price))
    if is_star_stock(symbol):
        return raw_amount if raw_amount >= 200 else 0
    return (raw_amount // 100) * 100


def calc_amount_by_cash(symbol: str, cash: float, price: float) -> int:
    if price <= 0 or cash <= 0:
        return 0
    effective_cash = max(float(cash) - 5.0, 0.0)
    raw_amount = int(effective_cash / (float(price) * 1.0003))
    return normalize_buy_amount(symbol, raw_amount)


def buy_fee(amount: float) -> float:
    if amount <= 0:
        return 0.0
    return max(amount * 0.0003, 5.0)


def sell_fee(amount: float) -> float:
    if amount <= 0:
        return 0.0
    return max(amount * 0.0003, 5.0) + amount * 0.001


def board_limit_ratio(symbol: str, trade_date: pd.Timestamp) -> float:
    raw = str(symbol or "").upper()
    if is_star_stock(raw):
        return 0.20
    if raw.startswith(("300", "301")):
        return 0.20 if trade_date >= pd.Timestamp("2020-08-24") else 0.10
    return 0.10


def resolve_price_limits(symbol: str, trade_date: pd.Timestamp, prev_close: float) -> tuple[float | None, float | None]:
    if pd.isna(prev_close) or float(prev_close) <= 0:
        return None, None
    ratio = board_limit_ratio(symbol, trade_date)
    high_limit = round(float(prev_close) * (1.0 + ratio), 2)
    low_limit = round(float(prev_close) * (1.0 - ratio), 2)
    return high_limit, low_limit


def can_trade(symbol: str, trade_date: pd.Timestamp, open_price: float, volume: float, prev_close: float, *, is_buy: bool) -> bool:
    if pd.isna(open_price) or float(open_price) <= 0:
        return False
    if pd.isna(volume) or float(volume) <= 0:
        return False
    if pd.isna(prev_close) or float(prev_close) <= 0:
        return True
    high_limit, low_limit = resolve_price_limits(symbol, trade_date, prev_close)
    if is_buy and float(open_price) >= high_limit * 0.995:
        return False
    if (not is_buy) and float(open_price) <= low_limit * 1.005:
        return False
    return True


def execution_price(symbol: str, trade_date: pd.Timestamp, open_price: float, prev_close: float, *, is_buy: bool, slippage_bps: float) -> float:
    if pd.isna(open_price) or float(open_price) <= 0:
        return 0.0
    adjusted = float(open_price) * (1.0 + float(slippage_bps) / 10000.0) if is_buy else float(open_price) * (1.0 - float(slippage_bps) / 10000.0)
    high_limit, low_limit = resolve_price_limits(symbol, trade_date, prev_close)
    if high_limit is not None:
        adjusted = min(adjusted, high_limit)
    if low_limit is not None:
        adjusted = max(adjusted, low_limit)
    return round(max(adjusted, 0.01), 4)


def volume_units_to_shares(volume: float) -> int:
    if pd.isna(volume):
        return 0
    return int(max(float(volume or 0.0), 0.0) * 100.0)


def available_trade_shares(symbol: str, desired_shares: int, remaining_shares: int, current_amount: int, *, is_buy: bool) -> int:
    capped = min(int(max(desired_shares, 0)), int(max(remaining_shares, 0)))
    if capped <= 0:
        return 0
    if is_buy:
        return normalize_buy_amount(symbol, capped)
    return normalize_sell_amount(symbol, capped, current_amount)


def calendar_hedge_ratio(trade_date: pd.Timestamp, cfg: MicrocapStrategyConfig) -> float:
    return float(cfg.seasonal_hedge_schedule.get(int(pd.Timestamp(trade_date).month), 0.0))


def get_dynamic_price_cap(symbol: str, slot_value: float, cfg: MicrocapStrategyConfig) -> float:
    min_amount = get_min_buy_amount(symbol)
    dynamic_cap = float(slot_value) / float(min_amount) * 0.98 if min_amount > 0 else 0.0
    if is_star_stock(symbol):
        return min(cfg.max_price_star_hard, dynamic_cap)
    return min(cfg.max_price_normal_hard, dynamic_cap)


def fit_target_count_by_cash(
    candidates: list[str],
    price_lookup: dict[str, float],
    invest_value: float,
    cfg: MicrocapStrategyConfig,
) -> list[str]:
    if not candidates:
        return []
    max_n = min(cfg.target_hold_num, len(candidates))
    for count in range(max_n, 0, -1):
        each_value = float(invest_value) / float(count)
        ok = True
        for symbol in candidates[:count]:
            price = float(price_lookup.get(symbol, 0.0) or 0.0)
            if price <= 0 or each_value < estimate_min_order_cost(symbol, price):
                ok = False
                break
        if ok:
            return candidates[:count]
    return []


def build_target_portfolio(
    day_frame: pd.DataFrame,
    holdings: list[str],
    total_value_open: float,
    cfg: MicrocapStrategyConfig,
) -> tuple[list[str], int]:
    if day_frame.empty:
        return [], 0
    invest_value = float(total_value_open) * (1.0 - cfg.cash_buffer)
    slot_value = invest_value / float(cfg.target_hold_num)
    working = day_frame.copy()
    if "is_overlay_asset" in working.columns:
        overlay_mask = pd.Series(working["is_overlay_asset"], index=working.index, dtype="boolean").fillna(False)
    else:
        overlay_mask = pd.Series(False, index=working.index, dtype=bool)
    working = working[
        (~overlay_mask)
        & (~working["is_b_share"])
        & (~working["is_beijing_stock"])
        & (~working["is_st_name"])
        & working["open"].fillna(0.0).ge(cfg.min_price_floor)
        & working["volume"].fillna(0.0).gt(0.0)
        & working["prev_close"].fillna(0.0).ge(cfg.min_price_floor)
        & working["listed_days"].fillna(-1).ge(cfg.min_list_days)
        & working["avg_amount_20_prev"].fillna(0.0).ge(cfg.min_avg_money_20)
        & working["market_cap_prev"].fillna(float("inf")).gt(0.0)
    ].copy()
    if working.empty:
        return [], 0
    working["price_cap"] = working["symbol"].map(lambda symbol: get_dynamic_price_cap(symbol, slot_value, cfg))
    working = working[working["open"].le(working["price_cap"])].copy()
    if working.empty:
        return [], 0
    working = working.sort_values(["market_cap_prev", "symbol"], ascending=[True, True]).head(cfg.query_limit)
    ranked = working["symbol"].astype(str).tolist()
    ranked_cap = max(cfg.keep_rank + 30, cfg.target_hold_num * 4)
    ranked = ranked[:ranked_cap]
    keep_pool = ranked[: min(cfg.keep_rank, len(ranked))]
    buy_pool = ranked[: min(cfg.buy_rank, len(ranked))]
    target: list[str] = []
    for symbol in keep_pool:
        if symbol in holdings and symbol not in target:
            target.append(symbol)
        if len(target) >= cfg.target_hold_num:
            return target[: cfg.target_hold_num], len(ranked)
    for symbol in buy_pool:
        if symbol not in target:
            target.append(symbol)
        if len(target) >= cfg.target_hold_num:
            return target[: cfg.target_hold_num], len(ranked)
    for symbol in ranked:
        if symbol not in target:
            target.append(symbol)
        if len(target) >= cfg.target_hold_num:
            break
    return target[: cfg.target_hold_num], len(ranked)


class JoinQuantMicrocapBacktestEngine:
    def __init__(self, session_factory: sessionmaker, app_settings: AppSettings, strategy_settings: StrategySettings) -> None:
        self.session_factory = session_factory
        self.app_settings = app_settings
        self.strategy_settings = strategy_settings
        self.cfg = MicrocapStrategyConfig.from_strategy_settings(strategy_settings)
        self.account_id = "joinquant-microcap-backtest"
        self.bridge = QmtBridgeClient(app_settings)

    def run(self, initial_cash: Decimal) -> tuple[Path, EvaluationResult, pd.DataFrame]:
        self._emit_progress(1, 5, "加载日线历史")
        history = self._load_history()
        self._emit_progress(2, 5, "加载证券元数据")
        symbols = history["symbol"].dropna().astype(str).unique().tolist()
        instrument_frame = self._load_instrument_frame(symbols)
        capital_frame = self._load_capital_frame(symbols)
        self._emit_progress(3, 5, "构建回测截面")
        prepared = self._prepare_history(history, instrument_frame, capital_frame)
        overlay_history = self._load_overlay_history()
        overlay_frame = self._prepare_overlay_history(overlay_history)
        if not overlay_frame.empty:
            prepared = pd.concat([prepared, overlay_frame], ignore_index=True).sort_values(["trading_date", "symbol"]).reset_index(drop=True)
        benchmark = self._load_benchmark()
        self._emit_progress(4, 5, "执行微盘策略回测")
        summary = self._simulate(prepared, benchmark, float(initial_cash), instrument_frame)
        self._emit_progress(5, 5, "写入回测结果")
        return self._persist_results(summary, initial_cash)

    def _emit_progress(self, current: int, total: int, phase: str) -> None:
        print(f"[JoinQuantMicrocap] backtest {current}/{total} phase={phase}", flush=True)

    def _load_history(self) -> pd.DataFrame:
        history_path = _resolve_path(self.app_settings.history_parquet)
        frame = pd.read_parquet(history_path, columns=["trading_date", "symbol", "open", "close", "volume", "amount"])
        frame["trading_date"] = pd.to_datetime(frame["trading_date"]).dt.normalize()
        start_dt = pd.Timestamp(self.app_settings.history_start or "2020-01-01").normalize()
        if self.app_settings.history_end:
            end_dt = pd.Timestamp(self.app_settings.history_end).normalize()
            frame = frame[(frame["trading_date"] >= start_dt) & (frame["trading_date"] <= end_dt)].copy()
        else:
            frame = frame[frame["trading_date"] >= start_dt].copy()
        frame["symbol"] = frame["symbol"].astype(str)
        return frame.sort_values(["symbol", "trading_date"]).reset_index(drop=True)

    def _load_benchmark(self) -> pd.DataFrame:
        symbol = self.cfg.benchmark_symbol or str(self.app_settings.qlib_benchmark_symbol or "").strip()
        if not symbol:
            return pd.DataFrame(columns=["trading_date", "close"])
        try:
            frame = self.bridge.get_history(
                [symbol],
                period=self.app_settings.history_period,
                start_time=self.app_settings.history_start,
                end_time=self.app_settings.history_end,
                dividend_type=self.app_settings.history_adjustment,
                fill_data=self.app_settings.history_fill_data,
            )
        except Exception:
            return pd.DataFrame(columns=["trading_date", "close"])
        if frame.empty:
            return pd.DataFrame(columns=["trading_date", "close"])
        frame["trading_date"] = pd.to_datetime(frame["trading_date"]).dt.normalize()
        return frame.loc[:, ["trading_date", "close"]].sort_values("trading_date").reset_index(drop=True)

    def _load_overlay_history(self) -> pd.DataFrame:
        symbol = str(self.cfg.hedge_symbol or "").strip()
        if not symbol or not self.cfg.seasonal_hedge_schedule:
            return pd.DataFrame(columns=["trading_date", "symbol", "open", "close", "volume", "amount"])
        try:
            frame = self.bridge.get_history(
                [symbol],
                period=self.app_settings.history_period,
                start_time=self.app_settings.history_start,
                end_time=self.app_settings.history_end,
                dividend_type=self.cfg.hedge_history_adjustment,
                fill_data=self.app_settings.history_fill_data,
            )
        except Exception:
            return pd.DataFrame(columns=["trading_date", "symbol", "open", "close", "volume", "amount"])
        if frame.empty:
            return pd.DataFrame(columns=["trading_date", "symbol", "open", "close", "volume", "amount"])
        frame["trading_date"] = pd.to_datetime(frame["trading_date"]).dt.normalize()
        frame["symbol"] = frame["symbol"].astype(str)
        return frame.loc[:, ["trading_date", "symbol", "open", "close", "volume", "amount"]].sort_values(["symbol", "trading_date"]).reset_index(drop=True)

    def _load_instrument_frame(self, symbols: list[str]) -> pd.DataFrame:
        cache_path = _resolve_path(self.cfg.instrument_cache_path)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cached = pd.DataFrame()
        if cache_path.exists():
            cached = pd.read_parquet(cache_path)
            cached["symbol"] = cached["symbol"].astype(str)
        known = set(cached["symbol"].astype(str)) if not cached.empty else set()
        missing = sorted(set(symbols) - known)
        rows: list[dict[str, Any]] = []
        for batch in _batched(missing, 200):
            details = self.bridge.get_instrument_details(batch)
            for symbol in batch:
                payload = details.get(symbol) or {}
                rows.append(
                    {
                        "symbol": symbol,
                        "instrument_name": str(payload.get("InstrumentName") or payload.get("instrument_name") or symbol),
                        "open_date": str(payload.get("OpenDate") or ""),
                        "total_capital_current": float(payload.get("TotalVolume") or payload.get("total_capital") or 0.0),
                        "float_capital_current": float(payload.get("FloatVolume") or payload.get("float_capital") or 0.0),
                    }
                )
        if rows:
            fresh = pd.DataFrame(rows)
            combined = pd.concat([cached, fresh], ignore_index=True) if not cached.empty else fresh
            combined = combined.drop_duplicates("symbol", keep="last").sort_values("symbol").reset_index(drop=True)
            combined.to_parquet(cache_path, index=False)
            cached = combined
        if cached.empty:
            return pd.DataFrame(columns=["symbol", "instrument_name", "open_date", "total_capital_current", "float_capital_current"])
        cached["open_date"] = pd.to_datetime(cached["open_date"], format="%Y%m%d", errors="coerce").dt.normalize()
        cached["instrument_name"] = cached["instrument_name"].fillna("")
        return cached.drop_duplicates("symbol", keep="last").reset_index(drop=True)

    def _load_capital_frame(self, symbols: list[str]) -> pd.DataFrame:
        cache_path = _resolve_path(self.cfg.capital_cache_path)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cached = pd.DataFrame()
        if cache_path.exists():
            cached = pd.read_parquet(cache_path)
            cached["symbol"] = cached["symbol"].astype(str)
        known = set(cached["symbol"].astype(str)) if not cached.empty else set()
        missing = sorted(set(symbols) - known)
        rows: list[dict[str, Any]] = []
        for batch in _batched(missing, 100):
            payload = self.bridge.get_financial_data(
                batch,
                ["Capital"],
                start_time=self.app_settings.history_start,
                end_time=self.app_settings.history_end,
                report_type="announce_time",
            )
            for symbol, table_map in payload.items():
                for item in table_map.get("Capital", []) or []:
                    rows.append(
                        {
                            "symbol": symbol,
                            "announce_date": str(item.get("m_anntime") or ""),
                            "report_date": str(item.get("m_timetag") or ""),
                            "total_capital": float(item.get("total_capital") or 0.0),
                            "circulating_capital": float(item.get("circulating_capital") or 0.0),
                            "free_float_capital": float(item.get("freeFloatCapital") or 0.0),
                        }
                    )
        if rows:
            fresh = pd.DataFrame(rows)
            combined = pd.concat([cached, fresh], ignore_index=True) if not cached.empty else fresh
            combined = combined.drop_duplicates(["symbol", "announce_date", "report_date"], keep="last")
            combined = combined.sort_values(["symbol", "announce_date", "report_date"]).reset_index(drop=True)
            combined.to_parquet(cache_path, index=False)
            cached = combined
        if cached.empty:
            return pd.DataFrame(columns=["symbol", "effective_date", "total_capital"])
        cached["announce_date"] = pd.to_datetime(cached["announce_date"], format="%Y%m%d", errors="coerce").dt.normalize()
        cached["report_date"] = pd.to_datetime(cached["report_date"], format="%Y%m%d", errors="coerce").dt.normalize()
        cached["effective_date"] = cached["announce_date"].fillna(cached["report_date"])
        cached = cached.dropna(subset=["effective_date"]).copy()
        cached = cached[cached["total_capital"].fillna(0.0) > 0.0].copy()
        return cached.loc[:, ["symbol", "effective_date", "total_capital", "circulating_capital", "free_float_capital"]]

    def _prepare_history(
        self,
        history: pd.DataFrame,
        instrument_frame: pd.DataFrame,
        capital_frame: pd.DataFrame,
    ) -> pd.DataFrame:
        frame = history.sort_values(["symbol", "trading_date"]).reset_index(drop=True).copy()
        grouped = frame.groupby("symbol", sort=False)
        frame["prev_close"] = grouped["close"].shift(1)
        frame["avg_amount_20_prev"] = grouped["amount"].transform(lambda series: series.rolling(20, min_periods=20).mean().shift(1))
        frame["avg_volume_20_prev"] = grouped["volume"].transform(lambda series: series.rolling(20, min_periods=20).mean().shift(1))

        instrument_base = instrument_frame.drop_duplicates("symbol", keep="last").set_index("symbol")
        frame["open_date"] = frame["symbol"].map(instrument_base["open_date"])
        first_seen = frame.groupby("symbol", sort=False)["trading_date"].transform("min")
        frame["open_date"] = frame["open_date"].fillna(first_seen)
        frame["listed_days"] = (frame["trading_date"] - frame["open_date"]).dt.days
        names = instrument_base["instrument_name"].fillna("")
        frame["is_st_name"] = frame["symbol"].map(
            lambda symbol: any(token in str(names.get(symbol, "")).upper() for token in ["ST", "*", "退"])
        )
        frame["is_beijing_stock"] = frame["symbol"].map(is_beijing_stock)
        frame["is_b_share"] = frame["symbol"].map(is_b_share)
        current_capital = instrument_base["total_capital_current"].fillna(0.0)

        if not capital_frame.empty:
            capital_groups = {
                symbol: group.sort_values("effective_date").reset_index(drop=True)
                for symbol, group in capital_frame.groupby("symbol", sort=False)
            }
            capital_values = np.full(len(frame), np.nan, dtype=float)
            for symbol, positions in frame.groupby("symbol", sort=False).indices.items():
                capital_rows = capital_groups.get(symbol)
                if capital_rows is None or capital_rows.empty:
                    continue
                trade_dates = frame.loc[positions, "trading_date"].to_numpy(dtype="datetime64[ns]")
                effective_dates = capital_rows["effective_date"].to_numpy(dtype="datetime64[ns]")
                total_capital_values = capital_rows["total_capital"].to_numpy(dtype=float)
                indexer = effective_dates.searchsorted(trade_dates, side="right") - 1
                valid = indexer >= 0
                if valid.any():
                    capital_values[np.asarray(positions)[valid]] = total_capital_values[indexer[valid]]
            frame["total_capital"] = capital_values
        else:
            frame["total_capital"] = pd.NA

        frame["total_capital"] = frame["total_capital"].fillna(frame["symbol"].map(current_capital))
        frame["total_capital"] = frame.groupby("symbol", sort=False)["total_capital"].ffill().bfill()
        frame["total_capital_prev"] = frame.groupby("symbol", sort=False)["total_capital"].shift(1).fillna(frame["total_capital"])
        frame["raw_vwap"] = np.where(
            frame["volume"].fillna(0.0) > 0.0,
            frame["amount"].fillna(0.0) / (frame["volume"].fillna(0.0) * 100.0),
            np.nan,
        )
        frame["raw_vwap_prev"] = frame.groupby("symbol", sort=False)["raw_vwap"].shift(1)
        frame["ranking_price_prev"] = frame["raw_vwap_prev"].fillna(frame["prev_close"])
        frame["market_cap_prev"] = frame["ranking_price_prev"].fillna(0.0) * frame["total_capital_prev"].fillna(0.0)
        columns = [
            "trading_date",
            "symbol",
            "open",
            "close",
            "volume",
            "amount",
            "prev_close",
            "avg_volume_20_prev",
            "avg_amount_20_prev",
            "listed_days",
            "is_st_name",
            "is_beijing_stock",
            "is_b_share",
            "is_overlay_asset",
            "market_cap_prev",
        ]
        frame["is_overlay_asset"] = False
        return frame.loc[:, columns].sort_values(["trading_date", "symbol"]).reset_index(drop=True)

    def _prepare_overlay_history(self, history: pd.DataFrame) -> pd.DataFrame:
        if history.empty:
            return pd.DataFrame(columns=[
                "trading_date",
                "symbol",
                "open",
                "close",
                "volume",
                "amount",
                "prev_close",
                "avg_volume_20_prev",
                "avg_amount_20_prev",
                "listed_days",
                "is_st_name",
                "is_beijing_stock",
                "is_b_share",
                "is_overlay_asset",
                "market_cap_prev",
            ])
        frame = history.sort_values(["symbol", "trading_date"]).reset_index(drop=True).copy()
        grouped = frame.groupby("symbol", sort=False)
        frame["prev_close"] = grouped["close"].shift(1)
        frame["avg_volume_20_prev"] = grouped["volume"].transform(lambda series: series.rolling(20, min_periods=20).mean().shift(1))
        frame["avg_amount_20_prev"] = grouped["amount"].transform(lambda series: series.rolling(20, min_periods=20).mean().shift(1))
        frame["listed_days"] = 9999
        frame["is_st_name"] = False
        frame["is_beijing_stock"] = False
        frame["is_b_share"] = False
        frame["is_overlay_asset"] = True
        frame["market_cap_prev"] = float("inf")
        return frame.loc[:, [
            "trading_date",
            "symbol",
            "open",
            "close",
            "volume",
            "amount",
            "prev_close",
            "avg_volume_20_prev",
            "avg_amount_20_prev",
            "listed_days",
            "is_st_name",
            "is_beijing_stock",
            "is_b_share",
            "is_overlay_asset",
            "market_cap_prev",
        ]].sort_values(["trading_date", "symbol"]).reset_index(drop=True)

    def _simulate(
        self,
        prepared: pd.DataFrame,
        benchmark: pd.DataFrame,
        initial_cash: float,
        instrument_frame: pd.DataFrame,
    ) -> dict[str, Any]:
        cash = float(initial_cash)
        holdings: dict[str, dict[str, float]] = {}
        trade_rows: list[dict[str, Any]] = []
        daily_target_rows: list[dict[str, Any]] = []
        position_rows: list[dict[str, Any]] = []
        equity_rows: list[dict[str, Any]] = []
        benchmark_close = benchmark.set_index("trading_date")["close"].astype(float) if not benchmark.empty else pd.Series(dtype=float)
        benchmark_returns = benchmark_close.pct_change().fillna(0.0) if not benchmark_close.empty else pd.Series(dtype=float)
        benchmark_equity = float(initial_cash)
        cumulative_turnover = 0.0
        peak_equity = float(initial_cash)
        name_map = (
            instrument_frame.drop_duplicates("symbol", keep="last").set_index("symbol")["instrument_name"].fillna("").to_dict()
            if not instrument_frame.empty
            else {}
        )
        overlay_symbol = str(self.cfg.hedge_symbol or "").strip()
        if overlay_symbol and overlay_symbol not in name_map:
            name_map[overlay_symbol] = self.cfg.hedge_name or overlay_symbol

        for trade_date, day_frame in prepared.groupby("trading_date", sort=True):
            day_frame = day_frame.sort_values(["market_cap_prev", "symbol"], ascending=[True, True]).copy()
            day_frame = day_frame.set_index("symbol", drop=False)
            remaining_trade_capacity = {
                str(symbol): int(volume_units_to_shares(float(row.get("avg_volume_20_prev", 0.0) or 0.0)) * self.cfg.max_trade_volume_ratio)
                for symbol, row in day_frame.iterrows()
            }
            open_value = cash
            for symbol, meta in holdings.items():
                open_price = float(day_frame.loc[symbol, "open"]) if symbol in day_frame.index else float(meta.get("last_close", meta["avg_cost"]))
                open_value += int(meta["shares"]) * open_price

            stock_holdings = [symbol for symbol in holdings.keys() if symbol != overlay_symbol]
            hedge_ratio = calendar_hedge_ratio(trade_date, self.cfg)
            invest_value = open_value * (1.0 - self.cfg.cash_buffer)
            stock_invest_value = invest_value * (1.0 - hedge_ratio)
            hedge_target_value = invest_value * hedge_ratio if overlay_symbol else 0.0
            target_stocks, ranked_count = build_target_portfolio(
                day_frame.reset_index(drop=True),
                stock_holdings,
                open_value * (1.0 - hedge_ratio),
                self.cfg,
            )
            price_lookup = {str(symbol): float(row["open"]) for symbol, row in day_frame.iterrows()}
            fitted_targets = fit_target_count_by_cash(
                target_stocks,
                price_lookup=price_lookup,
                invest_value=stock_invest_value,
                cfg=self.cfg,
            )
            target_set = set(fitted_targets)
            target_num = len(fitted_targets)
            each_target_value = (stock_invest_value / float(target_num)) if target_num > 0 else 0.0
            daily_turnover = 0.0
            daily_fees = 0.0

            for symbol in list(holdings.keys()):
                if symbol == overlay_symbol or symbol in target_set or symbol not in day_frame.index:
                    continue
                row = day_frame.loc[symbol]
                if not can_trade(symbol, trade_date, float(row["open"]), float(row["volume"]), float(row["prev_close"] or 0.0), is_buy=False):
                    continue
                shares = int(holdings[symbol]["shares"])
                if shares <= 0:
                    holdings.pop(symbol, None)
                    continue
                sell_shares = available_trade_shares(
                    symbol,
                    shares,
                    remaining_trade_capacity.get(symbol, 0),
                    shares,
                    is_buy=False,
                )
                if sell_shares <= 0:
                    continue
                price = execution_price(
                    symbol,
                    trade_date,
                    float(row["open"]),
                    float(row["prev_close"] or 0.0),
                    is_buy=False,
                    slippage_bps=self.cfg.sell_slippage_bps,
                )
                amount = sell_shares * price
                fee = sell_fee(amount)
                cash += amount - fee
                daily_turnover += amount
                daily_fees += fee
                cumulative_turnover += amount
                remaining_trade_capacity[symbol] = max(0, remaining_trade_capacity.get(symbol, 0) - sell_shares)
                trade_rows.append(
                    {
                        "trading_date": trade_date.date().isoformat(),
                        "symbol": symbol,
                        "instrument_name": name_map.get(symbol, symbol),
                        "side": "SELL",
                        "shares": sell_shares,
                        "price": round(price, 4),
                        "amount": round(amount, 2),
                        "fee": round(fee, 2),
                        "reason": "not_in_target",
                    }
                )
                remaining_shares = shares - sell_shares
                if remaining_shares > 0:
                    holdings[symbol]["shares"] = remaining_shares
                else:
                    holdings.pop(symbol, None)

            for symbol in fitted_targets:
                if symbol not in holdings or symbol not in day_frame.index:
                    continue
                row = day_frame.loc[symbol]
                if not can_trade(symbol, trade_date, float(row["open"]), float(row["volume"]), float(row["prev_close"] or 0.0), is_buy=False):
                    continue
                open_price = float(row["open"])
                current_shares = int(holdings[symbol]["shares"])
                current_value = current_shares * open_price
                if current_value <= each_target_value * self.cfg.max_overweight_ratio:
                    continue
                target_amount = calc_target_amount_by_value(symbol, each_target_value, open_price)
                adjusted_target = adjust_target_amount_for_rules(symbol, current_shares, target_amount)
                desired_sell = current_shares - adjusted_target
                sell_shares = available_trade_shares(
                    symbol,
                    desired_sell,
                    remaining_trade_capacity.get(symbol, 0),
                    current_shares,
                    is_buy=False,
                )
                if sell_shares <= 0:
                    continue
                sell_price = execution_price(
                    symbol,
                    trade_date,
                    float(row["open"]),
                    float(row["prev_close"] or 0.0),
                    is_buy=False,
                    slippage_bps=self.cfg.sell_slippage_bps,
                )
                amount = sell_shares * sell_price
                fee = sell_fee(amount)
                cash += amount - fee
                daily_turnover += amount
                daily_fees += fee
                cumulative_turnover += amount
                remaining_trade_capacity[symbol] = max(0, remaining_trade_capacity.get(symbol, 0) - sell_shares)
                holdings[symbol]["shares"] = current_shares - sell_shares
                trade_rows.append(
                    {
                        "trading_date": trade_date.date().isoformat(),
                        "symbol": symbol,
                        "instrument_name": name_map.get(symbol, symbol),
                        "side": "SELL",
                        "shares": sell_shares,
                        "price": round(sell_price, 4),
                        "amount": round(amount, 2),
                        "fee": round(fee, 2),
                        "reason": "overweight_trim",
                    }
                )
                if holdings[symbol]["shares"] <= 0:
                    holdings.pop(symbol, None)

            if overlay_symbol and overlay_symbol in day_frame.index:
                row = day_frame.loc[overlay_symbol]
                current_shares = int(holdings.get(overlay_symbol, {}).get("shares", 0))
                open_price = float(row["open"])
                current_value = current_shares * open_price
                target_amount = calc_target_amount_by_value(overlay_symbol, hedge_target_value, open_price)
                adjusted_target = adjust_target_amount_for_rules(overlay_symbol, current_shares, target_amount)
                if adjusted_target < current_shares and can_trade(
                    overlay_symbol,
                    trade_date,
                    float(row["open"]),
                    float(row["volume"]),
                    float(row["prev_close"] or 0.0),
                    is_buy=False,
                ):
                    desired_sell = current_shares - adjusted_target
                    sell_shares = available_trade_shares(
                        overlay_symbol,
                        desired_sell,
                        remaining_trade_capacity.get(overlay_symbol, 0),
                        current_shares,
                        is_buy=False,
                    )
                    if sell_shares > 0:
                        sell_price = execution_price(
                            overlay_symbol,
                            trade_date,
                            float(row["open"]),
                            float(row["prev_close"] or 0.0),
                            is_buy=False,
                            slippage_bps=self.cfg.sell_slippage_bps,
                        )
                        amount = sell_shares * sell_price
                        fee = sell_fee(amount)
                        cash += amount - fee
                        daily_turnover += amount
                        daily_fees += fee
                        cumulative_turnover += amount
                        remaining_trade_capacity[overlay_symbol] = max(0, remaining_trade_capacity.get(overlay_symbol, 0) - sell_shares)
                        trade_rows.append(
                            {
                                "trading_date": trade_date.date().isoformat(),
                                "symbol": overlay_symbol,
                                "instrument_name": name_map.get(overlay_symbol, overlay_symbol),
                                "side": "SELL",
                                "shares": sell_shares,
                                "price": round(sell_price, 4),
                                "amount": round(amount, 2),
                                "fee": round(fee, 2),
                                "reason": "seasonal_hedge_reduce",
                            }
                        )
                        remaining_shares = current_shares - sell_shares
                        if remaining_shares > 0:
                            holdings[overlay_symbol]["shares"] = remaining_shares
                        else:
                            holdings.pop(overlay_symbol, None)
                elif adjusted_target > current_shares and can_trade(
                    overlay_symbol,
                    trade_date,
                    float(row["open"]),
                    float(row["volume"]),
                    float(row["prev_close"] or 0.0),
                    is_buy=True,
                ):
                    buy_price = execution_price(
                        overlay_symbol,
                        trade_date,
                        float(row["open"]),
                        float(row["prev_close"] or 0.0),
                        is_buy=True,
                        slippage_bps=self.cfg.buy_slippage_bps,
                    )
                    desired_buy = adjusted_target - current_shares
                    buy_shares = available_trade_shares(
                        overlay_symbol,
                        desired_buy,
                        remaining_trade_capacity.get(overlay_symbol, 0),
                        current_shares,
                        is_buy=True,
                    )
                    if buy_shares > 0:
                        amount = buy_shares * buy_price
                        fee = buy_fee(amount)
                        total_cash = amount + fee
                        if total_cash <= cash:
                            position = holdings.setdefault(overlay_symbol, {"shares": 0, "avg_cost": 0.0, "last_close": float(row["open"])})
                            prev_shares = int(position["shares"])
                            new_shares = prev_shares + buy_shares
                            avg_cost_numerator = float(position["avg_cost"]) * prev_shares + amount + fee
                            position["shares"] = new_shares
                            position["avg_cost"] = avg_cost_numerator / float(new_shares)
                            cash -= total_cash
                            daily_turnover += amount
                            daily_fees += fee
                            cumulative_turnover += amount
                            remaining_trade_capacity[overlay_symbol] = max(0, remaining_trade_capacity.get(overlay_symbol, 0) - buy_shares)
                            trade_rows.append(
                                {
                                    "trading_date": trade_date.date().isoformat(),
                                    "symbol": overlay_symbol,
                                    "instrument_name": name_map.get(overlay_symbol, overlay_symbol),
                                    "side": "BUY",
                                    "shares": buy_shares,
                                    "price": round(buy_price, 4),
                                    "amount": round(amount, 2),
                                    "fee": round(fee, 2),
                                    "reason": "seasonal_hedge_buy",
                                }
                            )

            buy_plan: list[tuple[str, float]] = []
            for symbol in fitted_targets:
                if symbol not in day_frame.index:
                    continue
                row = day_frame.loc[symbol]
                if not can_trade(symbol, trade_date, float(row["open"]), float(row["volume"]), float(row["prev_close"] or 0.0), is_buy=True):
                    continue
                open_price = float(row["open"])
                current_value = int(holdings.get(symbol, {}).get("shares", 0)) * open_price
                gap_value = each_target_value - current_value
                if gap_value <= 0:
                    continue
                buy_plan.append((symbol, gap_value))
            buy_plan.sort(key=lambda item: item[1], reverse=True)

            for index, (symbol, gap_value) in enumerate(buy_plan):
                remaining = len(buy_plan) - index
                if remaining <= 0 or symbol not in day_frame.index:
                    continue
                row = day_frame.loc[symbol]
                buy_price = execution_price(
                    symbol,
                    trade_date,
                    float(row["open"]),
                    float(row["prev_close"] or 0.0),
                    is_buy=True,
                    slippage_bps=self.cfg.buy_slippage_bps,
                )
                budget = min(float(gap_value), cash / float(remaining) * 0.98)
                if budget < estimate_min_order_cost(symbol, buy_price):
                    continue
                desired_buy = calc_amount_by_cash(symbol, budget, buy_price)
                buy_shares = available_trade_shares(
                    symbol,
                    desired_buy,
                    remaining_trade_capacity.get(symbol, 0),
                    int(holdings.get(symbol, {}).get("shares", 0)),
                    is_buy=True,
                )
                if buy_shares <= 0:
                    continue
                amount = buy_shares * buy_price
                fee = buy_fee(amount)
                total_cash = amount + fee
                if total_cash > cash:
                    continue
                position = holdings.setdefault(symbol, {"shares": 0, "avg_cost": 0.0, "last_close": float(row["open"])})
                prev_shares = int(position["shares"])
                new_shares = prev_shares + buy_shares
                avg_cost_numerator = float(position["avg_cost"]) * prev_shares + amount + fee
                position["shares"] = new_shares
                position["avg_cost"] = avg_cost_numerator / float(new_shares)
                cash -= total_cash
                daily_turnover += amount
                daily_fees += fee
                cumulative_turnover += amount
                remaining_trade_capacity[symbol] = max(0, remaining_trade_capacity.get(symbol, 0) - buy_shares)
                trade_rows.append(
                    {
                        "trading_date": trade_date.date().isoformat(),
                        "symbol": symbol,
                        "instrument_name": name_map.get(symbol, symbol),
                        "side": "BUY",
                        "shares": buy_shares,
                        "price": round(buy_price, 4),
                        "amount": round(amount, 2),
                        "fee": round(fee, 2),
                        "reason": "rebalance_buy",
                    }
                )

            end_value = cash
            for symbol, meta in list(holdings.items()):
                close_price = float(day_frame.loc[symbol, "close"]) if symbol in day_frame.index else float(meta.get("last_close", meta["avg_cost"]))
                meta["last_close"] = close_price
                if int(meta["shares"]) <= 0:
                    holdings.pop(symbol, None)
                    continue
                end_value += int(meta["shares"]) * close_price
                position_rows.append(
                    {
                        "trading_date": trade_date,
                        "symbol": symbol,
                        "qty": int(meta["shares"]),
                        "cost_price": float(meta["avg_cost"]),
                        "market_price": close_price,
                    }
                )

            benchmark_equity *= 1.0 + float(benchmark_returns.get(trade_date, 0.0))
            peak_equity = max(peak_equity, end_value)
            drawdown = end_value / peak_equity - 1.0 if peak_equity > 0 else 0.0
            equity_rows.append(
                {
                    "trading_date": trade_date.date(),
                    "equity": float(end_value),
                    "cash": float(cash),
                    "turnover": float(cumulative_turnover),
                    "benchmark_equity": float(benchmark_equity),
                    "fees": float(daily_fees),
                    "max_drawdown": float(drawdown),
                }
            )
            daily_target_rows.append(
                {
                    "trading_date": trade_date.date().isoformat(),
                    "ranked_count": int(ranked_count),
                    "target_count": int(target_num),
                    "target_symbols": ",".join(fitted_targets),
                    "holding_count": int(len(holdings)),
                    "hedge_ratio": round(hedge_ratio, 4),
                    "hedge_symbol": overlay_symbol,
                    "hedge_target_value": round(hedge_target_value, 2),
                    "hedge_holding_shares": int(holdings.get(overlay_symbol, {}).get("shares", 0)) if overlay_symbol else 0,
                    "daily_turnover": round(daily_turnover, 2),
                    "daily_fees": round(daily_fees, 2),
                }
            )

        return {
            "equity_curve": pd.DataFrame(equity_rows),
            "positions": pd.DataFrame(position_rows),
            "trades": pd.DataFrame(trade_rows),
            "daily_targets": pd.DataFrame(daily_target_rows),
        }

    def _persist_results(self, summary: dict[str, Any], initial_cash: Decimal) -> tuple[Path, EvaluationResult, pd.DataFrame]:
        equity_curve = summary["equity_curve"].copy()
        positions = summary["positions"].copy()
        trades = summary["trades"].copy()
        daily_targets = summary["daily_targets"].copy()
        report_dir = _resolve_path(self.app_settings.report_dir)
        report_dir.mkdir(parents=True, exist_ok=True)
        equity_path = report_dir / "joinquant_microcap_equity.csv"
        trade_path = report_dir / "joinquant_microcap_trades.csv"
        target_path = report_dir / "joinquant_microcap_daily_targets.csv"
        summary_path = report_dir / "joinquant_microcap_summary.json"
        equity_curve.to_csv(equity_path, index=False, encoding="utf-8-sig")
        trades.to_csv(trade_path, index=False, encoding="utf-8-sig")
        daily_targets.to_csv(target_path, index=False, encoding="utf-8-sig")
        metrics = Evaluator().evaluate(equity_curve)

        with self.session_factory() as session:
            self._reset_non_live_state(session)
            for row in equity_curve.to_dict(orient="records"):
                snapshot_time = datetime.combine(pd.Timestamp(row["trading_date"]).date(), datetime.min.time())
                total_asset = Decimal(str(row["equity"]))
                session.add(
                    AssetSnapshotModel(
                        account_id=self.account_id,
                        cash=Decimal(str(row.get("cash", 0.0))),
                        frozen_cash=Decimal("0"),
                        total_asset=total_asset,
                        total_pnl=total_asset - initial_cash,
                        turnover=Decimal(str(row.get("turnover", 0.0))),
                        max_drawdown=Decimal(str(row.get("max_drawdown", 0.0))),
                        snapshot_time=snapshot_time,
                    )
                )
            if not positions.empty:
                for row in positions.to_dict(orient="records"):
                    snapshot_time = datetime.combine(pd.Timestamp(row["trading_date"]).date(), datetime.min.time())
                    session.add(
                        PositionSnapshotModel(
                            account_id=self.account_id,
                            symbol=str(row["symbol"]),
                            qty=int(row["qty"]),
                            available_qty=int(row["qty"]),
                            cost_price=Decimal(str(row["cost_price"])),
                            market_price=Decimal(str(row["market_price"])),
                            snapshot_time=snapshot_time,
                        )
                    )
            summary_payload = {
                "strategy": self.strategy_settings.implementation,
                "history_start": self.app_settings.history_start,
                "history_end": self.app_settings.history_end or (
                    str(equity_curve["trading_date"].iloc[-1]) if not equity_curve.empty else ""
                ),
                "report_dir": str(report_dir),
                "equity_path": str(equity_path),
                "trade_path": str(trade_path),
                "target_path": str(target_path),
                "benchmark_symbol": self.cfg.benchmark_symbol or self.app_settings.qlib_benchmark_symbol,
                "benchmark_name": self.cfg.benchmark_name or self.cfg.benchmark_symbol or self.app_settings.qlib_benchmark_symbol,
                "hedge_symbol": self.cfg.hedge_symbol,
                "hedge_name": self.cfg.hedge_name or self.cfg.hedge_symbol,
                "seasonal_hedge_schedule": self.cfg.seasonal_hedge_schedule,
                "total_return": metrics.total_return,
                "annualized_return": metrics.annualized_return,
                "max_drawdown": metrics.max_drawdown,
                "turnover": metrics.turnover,
                "execution_assumptions": {
                    "buy_slippage_bps": self.cfg.buy_slippage_bps,
                    "sell_slippage_bps": self.cfg.sell_slippage_bps,
                    "max_trade_volume_ratio_pct": round(self.cfg.max_trade_volume_ratio * 100.0, 2),
                    "volume_window": "20d_avg_prev",
                    "ranking_price_source": "raw_vwap_prev",
                    "hedge_history_adjustment": self.cfg.hedge_history_adjustment,
                },
                "known_limitations": [
                    "历史股票池仍由当前板块成分回推，仍存在生存者偏差。",
                    "停牌与当日无成交的识别仍依赖日线结果，无法完全复原 09:35 盘中可成交状态。",
                ],
            }
            session.add(
                AuditLogModel(
                    object_type="backtest_engine",
                    object_id="joinquant_microcap",
                    message="JoinQuant microcap backtest finished",
                    payload=summary_payload,
                )
            )
            session.commit()
            report_path = AuditReportService().write_daily_report(session, report_dir)

        summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return report_path, metrics, equity_curve

    def _reset_non_live_state(self, session) -> None:
        for model in [
            AuditLogModel,
            TradeModel,
            OrderEventModel,
            OrderModel,
            RiskDecisionModel,
            OrderIntentModel,
            PositionSnapshotModel,
            AssetSnapshotModel,
        ]:
            session.execute(delete(model))
        session.flush()
