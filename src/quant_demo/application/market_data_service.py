"""Quant 市场数据应用服务：market-data.v1 唯一事实源。

设计文档对应：
- §4.1 quant 是市场数据唯一事实源
- §7   market-data.v1 Bars Batch Contract
- §8   PIT 市场元数据（security_master / security_status_daily /
       price_limit_daily / trading_calendar / adjustment_factor 等）
- §9   Market Snapshot 不可变登记
- §31  Common Metadata
- §91  数据新鲜度
"""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from quant_demo.core.exceptions import DataNotReadyError
from quant_demo.db.models_market import (
    AdjustmentFactorRow,
    MarketSnapshotRow,
    PriceLimitDailyRow,
    SecurityMasterRow,
    SecurityStatusDailyRow,
    TradingCalendarRow,
)
from quant_demo.marketdata.ingestion import load_history_dataframe

MARKET_DATA_CONTRACT = "market-data.v1"
BAR_FIELDS = ("open", "high", "low", "close", "volume", "amount", "turnover")


def _parse_date(value: str | date | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


class MarketDataService:
    """以 Parquet 历史行情 + PostgreSQL PIT 元数据为事实源。"""

    def __init__(self, history_path: str | Path, session_factory, source: str = "qmt") -> None:
        self._history_path = Path(history_path)
        self._session_factory = session_factory
        self._source = source
        self._frame_cache: pd.DataFrame | None = None

    # ------------------------------------------------------------------ bars
    def bars_batch(
        self,
        symbols: list[str],
        start: str | date,
        end: str | date,
        frequency: str = "1d",
        adjust: str = "qfq",
        session: Session | None = None,
    ) -> dict[str, Any]:
        start_d, end_d = _parse_date(start), _parse_date(end)
        if start_d is None or end_d is None:
            raise ValueError("start and end are required")
        if end_d < start_d:
            raise ValueError("end must be on or after start")

        frame = self._load_frame()
        window = frame[(frame["trading_date"] >= start_d) & (frame["trading_date"] <= end_d)]
        if adjust != "none":
            window = self._apply_adjustment(window, adjust, session)

        by_symbol: dict[str, dict[str, dict[str, Any]]] = {}
        all_dates: set[str] = set()
        for symbol in symbols:
            rows = window[window["symbol"] == symbol]
            records: dict[str, dict[str, Any]] = {}
            for row in rows.itertuples(index=False):
                day = row.trading_date.isoformat()
                records[day] = {field: getattr(row, field, None) for field in BAR_FIELDS}
            by_symbol[symbol] = records
            all_dates.update(records)

        dates = sorted(all_dates)
        bars: dict[str, list] = {field: [] for field in BAR_FIELDS}
        for symbol in symbols:
            records = by_symbol[symbol]
            for field in BAR_FIELDS:
                bars[field].append([
                    _jsonable(records[day].get(field)) if day in records else None for day in dates
                ])

        version_material = {
            "symbols": symbols,
            "dates": dates,
            "bars": bars,
            "adjust": adjust,
            "source": self._source,
        }
        data_version = hashlib.sha256(
            json.dumps(version_material, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
        ).hexdigest()
        # 与 stock_agent market-data.v1 语义一致：内容寻址的可重放快照 ID。
        data_snapshot_id = f"mds-{data_version}"
        self._register_snapshot(
            data_snapshot_id,
            data_version,
            frequency=frequency,
            adjustment=adjust,
            as_of=dates[-1] if dates else None,
            summary={
                "symbol_count": len(symbols),
                "date_count": len(dates),
                "start": str(start_d),
                "end": str(end_d),
                "symbols": symbols,
            },
        )
        return {
            "symbols": symbols,
            "dates": dates,
            "bars": bars,
            "data_version": data_version,
            "data_snapshot_id": data_snapshot_id,
            "source": self._source,
        }

    # ------------------------------------------------------------ PIT status
    def upsert_security_status(self, rows: list[dict]) -> int:
        with self._session_factory() as session:
            for item in rows:
                trading_date = _parse_date(item["trading_date"])
                existing = session.scalar(
                    select(SecurityStatusDailyRow).where(
                        SecurityStatusDailyRow.trading_date == trading_date,
                        SecurityStatusDailyRow.symbol == item["symbol"],
                    )
                )
                row = existing or SecurityStatusDailyRow(trading_date=trading_date, symbol=item["symbol"])
                for field in ("is_st", "is_star_st", "is_suspended", "is_delisting", "listing_days", "risk_warning", "source"):
                    if field in item:
                        setattr(row, field, item[field])
                row.available_at = _parse_datetime(item.get("available_at")) or datetime.utcnow()
                if existing is None:
                    session.add(row)
            session.commit()
        return len(rows)

    def get_security_status(self, symbol: str, start: str | date, end: str | date) -> list[dict]:
        start_d, end_d = _parse_date(start), _parse_date(end)
        with self._session_factory() as session:
            rows = session.scalars(
                select(SecurityStatusDailyRow)
                .where(
                    SecurityStatusDailyRow.symbol == symbol,
                    SecurityStatusDailyRow.trading_date >= start_d,
                    SecurityStatusDailyRow.trading_date <= end_d,
                )
                .order_by(SecurityStatusDailyRow.trading_date)
            ).all()
            return [
                {
                    "trading_date": row.trading_date.isoformat(),
                    "symbol": row.symbol,
                    "is_st": row.is_st,
                    "is_star_st": row.is_star_st,
                    "is_suspended": row.is_suspended,
                    "is_delisting": row.is_delisting,
                    "listing_days": row.listing_days,
                    "risk_warning": row.risk_warning,
                    "available_at": row.available_at.isoformat() if row.available_at else None,
                    "source": row.source,
                }
                for row in rows
            ]

    def status_map(self, symbols: list[str], start: date, end: date) -> dict[tuple[date, str], SecurityStatusDailyRow]:
        with self._session_factory() as session:
            rows = session.scalars(
                select(SecurityStatusDailyRow).where(
                    SecurityStatusDailyRow.symbol.in_(symbols),
                    SecurityStatusDailyRow.trading_date >= start,
                    SecurityStatusDailyRow.trading_date <= end,
                )
            ).all()
            return {(row.trading_date, row.symbol): row for row in rows}

    # ---------------------------------------------------------- price limits
    def upsert_price_limits(self, rows: list[dict]) -> int:
        with self._session_factory() as session:
            for item in rows:
                trading_date = _parse_date(item["trading_date"])
                existing = session.scalar(
                    select(PriceLimitDailyRow).where(
                        PriceLimitDailyRow.trading_date == trading_date,
                        PriceLimitDailyRow.symbol == item["symbol"],
                    )
                )
                row = existing or PriceLimitDailyRow(trading_date=trading_date, symbol=item["symbol"])
                for field in ("limit_rate", "upper_limit_price", "lower_limit_price", "rule_version"):
                    if field in item:
                        setattr(row, field, item[field])
                row.available_at = _parse_datetime(item.get("available_at")) or datetime.utcnow()
                if existing is None:
                    session.add(row)
            session.commit()
        return len(rows)

    def limit_map(self, symbols: list[str], start: date, end: date) -> dict[tuple[date, str], PriceLimitDailyRow]:
        with self._session_factory() as session:
            rows = session.scalars(
                select(PriceLimitDailyRow).where(
                    PriceLimitDailyRow.symbol.in_(symbols),
                    PriceLimitDailyRow.trading_date >= start,
                    PriceLimitDailyRow.trading_date <= end,
                )
            ).all()
            return {(row.trading_date, row.symbol): row for row in rows}

    # --------------------------------------------------------------- masters
    def upsert_security_master(self, rows: list[dict]) -> int:
        with self._session_factory() as session:
            for item in rows:
                existing = session.get(SecurityMasterRow, item["symbol"])
                row = existing or SecurityMasterRow(symbol=item["symbol"])
                for field in ("exchange", "security_type", "name", "board", "currency", "lot_size", "tick_size"):
                    if field in item:
                        setattr(row, field, item[field])
                if "list_date" in item:
                    row.list_date = _parse_date(item["list_date"])
                if "delist_date" in item:
                    row.delist_date = _parse_date(item["delist_date"])
                if existing is None:
                    session.add(row)
            session.commit()
        return len(rows)

    def list_securities(self, limit: int = 100) -> list[dict]:
        with self._session_factory() as session:
            rows = session.scalars(select(SecurityMasterRow).limit(min(max(limit, 1), 1000))).all()
            return [
                {
                    "symbol": row.symbol,
                    "exchange": row.exchange,
                    "security_type": row.security_type,
                    "name": row.name,
                    "list_date": row.list_date.isoformat() if row.list_date else None,
                    "delist_date": row.delist_date.isoformat() if row.delist_date else None,
                    "board": row.board,
                    "currency": row.currency,
                    "lot_size": row.lot_size,
                    "tick_size": row.tick_size,
                }
                for row in rows
            ]

    def upsert_calendar(self, trading_days: list[str]) -> int:
        with self._session_factory() as session:
            for value in trading_days:
                day = _parse_date(value)
                if session.get(TradingCalendarRow, day) is None:
                    session.add(TradingCalendarRow(trading_date=day, is_trading_day=True))
            session.commit()
        return len(trading_days)

    def trading_days(self, start: date, end: date) -> list[date]:
        with self._session_factory() as session:
            rows = session.scalars(
                select(TradingCalendarRow).where(
                    TradingCalendarRow.trading_date >= start,
                    TradingCalendarRow.trading_date <= end,
                    TradingCalendarRow.is_trading_day.is_(True),
                )
            ).all()
        if rows:
            return sorted(row.trading_date for row in rows)
        # 日历缺省时回退为工作日（质量标志会标记 PIT 元数据缺失）。
        return [day.date() for day in pd.bdate_range(start, end)]

    # ------------------------------------------------------------ snapshots
    def _register_snapshot(
        self,
        snapshot_id: str,
        data_version: str,
        frequency: str,
        adjustment: str,
        as_of: str | None,
        summary: dict,
    ) -> None:
        with self._session_factory() as session:
            row = session.get(MarketSnapshotRow, snapshot_id)
            if row is None:
                session.add(
                    MarketSnapshotRow(
                        snapshot_id=snapshot_id,
                        data_version=data_version,
                        source=self._source,
                        frequency=frequency,
                        adjustment=adjustment,
                        as_of=as_of,
                        payload_summary=summary,
                    )
                )
                session.commit()

    def get_snapshot(self, snapshot_id: str) -> dict | None:
        with self._session_factory() as session:
            row = session.get(MarketSnapshotRow, snapshot_id)
            if row is None:
                return None
            return {
                "snapshot_id": row.snapshot_id,
                "data_version": row.data_version,
                "source": row.source,
                "frequency": row.frequency,
                "adjustment": row.adjustment,
                "universe": row.universe,
                "as_of": row.as_of,
                "created_at": row.created_at.isoformat(timespec="seconds"),
                "payload_summary": row.payload_summary,
            }

    # ------------------------------------------------------------- internal
    def _load_frame(self) -> pd.DataFrame:
        if self._frame_cache is None:
            if not self._history_path.exists():
                raise DataNotReadyError(f"历史行情不存在: {self._history_path}")
            frame = load_history_dataframe(self._history_path)
            for field in BAR_FIELDS:
                if field not in frame.columns:
                    frame[field] = None
            self._frame_cache = frame
        return self._frame_cache

    def _apply_adjustment(self, frame: pd.DataFrame, adjust: str, session: Session | None) -> pd.DataFrame:
        with self._session_factory() as own_session:
            factors = (session or own_session).scalars(
                select(AdjustmentFactorRow).where(AdjustmentFactorRow.adjustment_type == adjust)
            ).all()
        if not factors:
            return frame
        factor_map = {(row.trading_date, row.symbol): row.factor for row in factors}
        adjusted = frame.copy()
        multipliers = [factor_map.get((day, symbol), 1.0) for day, symbol in zip(adjusted["trading_date"], adjusted["symbol"])]
        for field in ("open", "high", "low", "close"):
            adjusted[field] = [
                None if price is None else round(float(price) * multiplier, 6)
                for price, multiplier in zip(adjusted[field], multipliers)
            ]
        return adjusted


def _jsonable(value: Any) -> Any:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def _parse_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))
