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
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from quant_demo.core.exceptions import DataNotReadyError
from quant_demo.db.models_market import (
    AdjustmentFactorRow,
    ConceptMembershipRow,
    CorporateActionRow,
    IndexConstituentRow,
    IndustryMembershipRow,
    MarketSnapshotRow,
    PriceLimitDailyRow,
    SecurityMasterRow,
    SecurityStatusDailyRow,
    TradingCalendarRow,
)
from quant_demo.marketdata.ingestion import load_history_dataframe
from quant_demo.marketdata.quality import evaluate_frame_quality
from quant_demo.snapshot.manifest import build_manifest
from quant_demo.snapshot.models import SNAPSHOT_SCHEMA_VERSION
from quant_demo.snapshot.store import (
    LocalParquetSnapshotStore,
    MarketSnapshotStore,
    SnapshotCorruptedError,
    SnapshotNotFoundError,
)

MARKET_DATA_CONTRACT = "market-data.v1"
BAR_FIELDS = ("open", "high", "low", "close", "volume", "amount", "turnover")


def _parse_date(value: str | date | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


class MarketDataService:
    """以 Parquet 历史行情 + PostgreSQL PIT 元数据为事实源。

    收尾文档 §10：bars_batch 物化不可变快照（SnapshotStore.exists? 复用 :
    写 immutable parquet + manifest + 登记 metadata）。
    """

    def __init__(
        self,
        history_path: str | Path,
        session_factory,
        source: str = "qmt",
        snapshot_store: MarketSnapshotStore | None = None,
    ) -> None:
        self._history_path = Path(history_path)
        self._session_factory = session_factory
        self._source = source
        self._frame_cache: pd.DataFrame | None = None
        # 默认快照目录与历史行情同级（data/market_snapshots，§7）。
        self._snapshot_store: MarketSnapshotStore = snapshot_store or LocalParquetSnapshotStore(
            self._history_path.parent / "market_snapshots"
        )

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
        # 详细修改方案 §18：Market API 必须返回 quality_flags。
        quality_flags = evaluate_frame_quality(window, symbols, end_d)
        self._materialize_snapshot(
            data_snapshot_id,
            data_version,
            frequency=frequency,
            adjustment=adjust,
            start_d=start_d,
            end_d=end_d,
            symbols=symbols,
            dates=dates,
            bars=bars,
            quality_flags=quality_flags,
        )
        return {
            "symbols": symbols,
            "dates": dates,
            "bars": bars,
            "data_version": data_version,
            "data_snapshot_id": data_snapshot_id,
            "source": self._source,
            "quality_flags": quality_flags,
        }

    # ------------------------------------------------------------ snapshots
    def create_snapshot(
        self,
        symbols: list[str],
        start: str | date,
        end: str | date,
        adjust: str = "qfq",
        frequency: str = "1d",
    ) -> dict[str, Any]:
        """收尾文档 §11：显式创建不可变快照（Backtest 未指定 snapshot 时调用）。"""
        data = self.bars_batch(symbols=symbols, start=start, end=end, frequency=frequency, adjust=adjust)
        return {"snapshot_id": data["data_snapshot_id"], "data_version": data["data_version"]}

    def load_snapshot(self, snapshot_id: str) -> dict[str, Any]:
        """收尾文档 §11：从不可变 Dataset 读取快照，绝不重新访问 current source。

        返回与 bars_batch 相同结构的 market-data.v1 payload。
        """
        try:
            frame = self._snapshot_store.load(snapshot_id)
            manifest = self._snapshot_store.read_manifest(snapshot_id)
        except SnapshotNotFoundError:
            raise
        except SnapshotCorruptedError:
            raise
        symbols = list(manifest.get("symbols") or sorted(frame["symbol"].unique()))
        dates = sorted({day.isoformat() for day in frame["trading_date"]})
        bars: dict[str, list] = {field: [] for field in BAR_FIELDS}
        by_symbol = {symbol: group.set_index("trading_date") for symbol, group in frame.groupby("symbol")}
        for symbol in symbols:
            group = by_symbol.get(symbol)
            for field in BAR_FIELDS:
                bars[field].append(
                    [
                        _jsonable(group.at[day, field]) if group is not None and day in group.index else None
                        for day in [_parse_date(item) for item in dates]
                    ]
                )
        return {
            "symbols": symbols,
            "dates": dates,
            "bars": bars,
            "data_version": manifest.get("data_version", ""),
            "data_snapshot_id": snapshot_id,
            "source": manifest.get("source", self._source),
            "snapshot_manifest": manifest,
        }

    def verify_snapshot(self, snapshot_id: str) -> dict[str, Any]:
        """详细修改方案 §20：POST /api/v1/market/snapshots/{id}/verify。

        重新计算每个落盘文件的 sha256 并与 manifest 比对，确认可复算、未被篡改。
        """
        import hashlib
        from pathlib import Path

        manifest = self._snapshot_store.read_manifest(snapshot_id)  # 不存在时抛 SnapshotNotFoundError
        directory = Path(self._snapshot_store._directory(snapshot_id))  # noqa: SLF001
        manifest_hash = hashlib.sha256((directory / "manifest.json").read_bytes()).hexdigest()
        checks: list[dict[str, Any]] = []
        for item in manifest.get("files") or []:
            path = directory / item["path"]
            if not path.exists():
                raise SnapshotCorruptedError(f"{snapshot_id}: 文件缺失 {item['path']}")
            digest = hashlib.sha256()
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1 << 20), b""):
                    digest.update(chunk)
            actual = digest.hexdigest()
            if item.get("sha256") and actual != item["sha256"]:
                raise SnapshotCorruptedError(f"{snapshot_id}: {item['path']} sha256 不一致")
            checks.append({"path": item["path"], "sha256": actual})
        return {
            "snapshot_id": snapshot_id,
            "verified": True,
            "schema_version": manifest.get("schema_version"),
            "manifest_hash": manifest_hash,
            "files": checks,
        }

    def _materialize_snapshot(
        self,
        snapshot_id: str,
        data_version: str,
        *,
        frequency: str,
        adjustment: str,
        start_d: date,
        end_d: date,
        symbols: list[str],
        dates: list[str],
        bars: dict[str, list],
        quality_flags: list[str] | None = None,
    ) -> None:
        """收尾文档 §6/§10：写不可变 parquet + manifest，再登记 metadata。"""
        location = None
        row_count = 0
        if self._snapshot_store.exists(snapshot_id):
            location = self._snapshot_store.location(snapshot_id)  # 幂等复用，不覆盖
            manifest = self._snapshot_store.read_manifest(snapshot_id)
            row_count = sum(len(column) for column in bars.values()) // max(len(BAR_FIELDS), 1)
        else:
            records: list[dict] = []
            for symbol_index, symbol in enumerate(symbols):
                for date_index, day in enumerate(dates):
                    record = {"symbol": symbol, "trading_date": _parse_date(day)}
                    for field in BAR_FIELDS:
                        record[field] = bars[field][symbol_index][date_index]
                    records.append(record)
            frame = pd.DataFrame.from_records(records)
            manifest = build_manifest(
                snapshot_id=snapshot_id,
                data_version=data_version,
                source=self._source,
                frequency=frequency,
                adjustment=adjustment,
                start=str(start_d),
                end=str(end_d),
                as_of=dates[-1] if dates else None,
                symbols=symbols,
                fields=list(BAR_FIELDS),
                dataset_path="bars.parquet",
                dataset_sha256="",
                dates=dates,
                row_count=len(records),
                quality_flags=quality_flags,
            )
            location = self._snapshot_store.save(snapshot_id, frame, manifest)
            manifest = self._snapshot_store.read_manifest(snapshot_id)
            row_count = len(records)
        self._register_snapshot(
            snapshot_id,
            data_version,
            frequency=frequency,
            adjustment=adjustment,
            start_d=start_d,
            end_d=end_d,
            as_of=dates[-1] if dates else None,
            location=location,
            row_count=row_count,
            summary={
                "symbol_count": len(symbols),
                "date_count": len(dates),
                "start": str(start_d),
                "end": str(end_d),
                "symbols": symbols,
                "manifest_hash": location.manifest_hash if location else None,
            },
        )

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

    # ---------------------------------------------------- corporate actions
    def upsert_corporate_actions(self, rows: list[dict]) -> int:
        """收尾文档 §13：PIT 公司行动（available_at 约束可见性）。幂等：重复注入不得产生 double adjustment。"""
        with self._session_factory() as session:
            for item in rows:
                ex_date = _parse_date(item["ex_date"])
                action_type = item.get("action_type", "DIVIDEND")
                existing = session.scalars(
                    select(CorporateActionRow).where(
                        CorporateActionRow.symbol == item["symbol"],
                        CorporateActionRow.ex_date == ex_date,
                        CorporateActionRow.action_type == action_type,
                    )
                ).first()
                payload = dict(
                    announcement_date=_parse_date(item.get("announcement_date")),
                    cash_dividend=item.get("cash_dividend"),
                    split_ratio=item.get("split_ratio"),
                    rights_ratio=item.get("rights_ratio"),
                    adjustment_factor=item.get("adjustment_factor"),
                    available_at=_parse_datetime(item.get("available_at")) or datetime.utcnow(),
                )
                if existing is None:
                    session.add(
                        CorporateActionRow(symbol=item["symbol"], ex_date=ex_date, action_type=action_type, **payload)
                    )
                else:
                    for key, value in payload.items():
                        setattr(existing, key, value)
            session.commit()
        return len(rows)

    def get_corporate_actions(self, symbol: str, start: str | date, end: str | date) -> list[dict]:
        start_d, end_d = _parse_date(start), _parse_date(end)
        with self._session_factory() as session:
            rows = session.scalars(
                select(CorporateActionRow)
                .where(
                    CorporateActionRow.symbol == symbol,
                    CorporateActionRow.ex_date >= start_d,
                    CorporateActionRow.ex_date <= end_d,
                )
                .order_by(CorporateActionRow.ex_date)
            ).all()
            return [
                {
                    "symbol": row.symbol,
                    "announcement_date": row.announcement_date.isoformat() if row.announcement_date else None,
                    "ex_date": row.ex_date.isoformat(),
                    "action_type": row.action_type,
                    "cash_dividend": row.cash_dividend,
                    "split_ratio": row.split_ratio,
                    "rights_ratio": row.rights_ratio,
                    "adjustment_factor": row.adjustment_factor,
                    "available_at": row.available_at.isoformat() if row.available_at else None,
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

    # ------------------------------------------- PIT memberships（收尾文档 §13）
    def upsert_industry_memberships(self, rows: list[dict]) -> int:
        """收尾文档 §13：PIT 行业归属（industry_membership_daily）。"""
        with self._session_factory() as session:
            for item in rows:
                session.add(
                    IndustryMembershipRow(
                        symbol=item["symbol"],
                        industry_standard=item.get("industry_standard", "SW"),
                        industry_level=item.get("industry_level", "L1"),
                        industry_code=item["industry_code"],
                        valid_from=_parse_date(item["valid_from"]),
                        valid_to=_parse_date(item.get("valid_to")),
                        available_at=_parse_datetime(item.get("available_at")) or datetime.utcnow(),
                    )
                )
            session.commit()
        return len(rows)

    def get_industry_memberships(self, symbol: str, as_of: str | date) -> list[dict]:
        as_of_d = _parse_date(as_of)
        with self._session_factory() as session:
            rows = session.scalars(
                select(IndustryMembershipRow)
                .where(
                    IndustryMembershipRow.symbol == symbol,
                    IndustryMembershipRow.valid_from <= as_of_d,
                    (IndustryMembershipRow.valid_to.is_(None)) | (IndustryMembershipRow.valid_to >= as_of_d),
                )
                .order_by(IndustryMembershipRow.industry_standard, IndustryMembershipRow.industry_level)
            ).all()
            return [
                {
                    "symbol": row.symbol,
                    "industry_standard": row.industry_standard,
                    "industry_level": row.industry_level,
                    "industry_code": row.industry_code,
                    "valid_from": row.valid_from.isoformat(),
                    "valid_to": row.valid_to.isoformat() if row.valid_to else None,
                    "available_at": row.available_at.isoformat() if row.available_at else None,
                }
                for row in rows
            ]

    def upsert_index_constituents(self, rows: list[dict]) -> int:
        """收尾文档 §13/§49：历史指数成分（防幸存者偏差）。幂等：同窗口重复注入不产生重复成分。"""
        with self._session_factory() as session:
            for item in rows:
                valid_from = _parse_date(item["valid_from"])
                existing = session.scalars(
                    select(IndexConstituentRow).where(
                        IndexConstituentRow.index_code == item["index_code"],
                        IndexConstituentRow.symbol == item["symbol"],
                        IndexConstituentRow.valid_from == valid_from,
                    )
                ).first()
                payload = dict(
                    valid_to=_parse_date(item.get("valid_to")),
                    available_at=_parse_datetime(item.get("available_at")) or datetime.utcnow(),
                )
                if existing is None:
                    session.add(
                        IndexConstituentRow(
                            index_code=item["index_code"], symbol=item["symbol"], valid_from=valid_from, **payload
                        )
                    )
                else:
                    for key, value in payload.items():
                        setattr(existing, key, value)
            session.commit()
        return len(rows)

    def get_index_constituents(self, index_code: str, as_of: str | date) -> list[dict]:
        as_of_d = _parse_date(as_of)
        with self._session_factory() as session:
            rows = session.scalars(
                select(IndexConstituentRow)
                .where(
                    IndexConstituentRow.index_code == index_code,
                    IndexConstituentRow.valid_from <= as_of_d,
                    (IndexConstituentRow.valid_to.is_(None)) | (IndexConstituentRow.valid_to >= as_of_d),
                )
                .order_by(IndexConstituentRow.symbol)
            ).all()
            return [
                {
                    "index_code": row.index_code,
                    "symbol": row.symbol,
                    "valid_from": row.valid_from.isoformat(),
                    "valid_to": row.valid_to.isoformat() if row.valid_to else None,
                    "available_at": row.available_at.isoformat() if row.available_at else None,
                }
                for row in rows
            ]

    def upsert_concept_memberships(self, rows: list[dict]) -> int:
        """收尾文档 §13：PIT 概念板块归属（concept_membership_daily）。"""
        with self._session_factory() as session:
            for item in rows:
                session.add(
                    ConceptMembershipRow(
                        symbol=item["symbol"],
                        concept_code=item["concept_code"],
                        valid_from=_parse_date(item["valid_from"]),
                        valid_to=_parse_date(item.get("valid_to")),
                        available_at=_parse_datetime(item.get("available_at")) or datetime.utcnow(),
                    )
                )
            session.commit()
        return len(rows)

    def get_concept_memberships(self, symbol: str, as_of: str | date) -> list[dict]:
        as_of_d = _parse_date(as_of)
        with self._session_factory() as session:
            rows = session.scalars(
                select(ConceptMembershipRow)
                .where(
                    ConceptMembershipRow.symbol == symbol,
                    ConceptMembershipRow.valid_from <= as_of_d,
                    (ConceptMembershipRow.valid_to.is_(None)) | (ConceptMembershipRow.valid_to >= as_of_d),
                )
                .order_by(ConceptMembershipRow.concept_code)
            ).all()
            return [
                {
                    "symbol": row.symbol,
                    "concept_code": row.concept_code,
                    "valid_from": row.valid_from.isoformat(),
                    "valid_to": row.valid_to.isoformat() if row.valid_to else None,
                    "available_at": row.available_at.isoformat() if row.available_at else None,
                }
                for row in rows
            ]

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
        *,
        start_d: date | None = None,
        end_d: date | None = None,
        as_of: str | None,
        location=None,
        row_count: int = 0,
        summary: dict,
    ) -> None:
        with self._session_factory() as session:
            row = session.get(MarketSnapshotRow, snapshot_id)
            if row is None:
                session.add(
                    MarketSnapshotRow(
                        snapshot_id=snapshot_id,
                        data_version=data_version,
                        dataset_uri=location.dataset_uri if location else None,
                        manifest_uri=location.manifest_uri if location else None,
                        manifest_hash=location.manifest_hash if location else None,
                        source=self._source,
                        frequency=frequency,
                        adjustment=adjustment,
                        start_date=start_d,
                        end_date=end_d,
                        as_of=as_of,
                        symbol_count=int(summary.get("symbol_count", 0)),
                        date_count=int(summary.get("date_count", 0)),
                        row_count=row_count,
                        schema_version=SNAPSHOT_SCHEMA_VERSION,
                        immutable=True,
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
                "dataset_uri": row.dataset_uri,
                "manifest_uri": row.manifest_uri,
                "manifest_hash": row.manifest_hash,
                "source": row.source,
                "frequency": row.frequency,
                "adjustment": row.adjustment,
                "universe": row.universe,
                "start_date": row.start_date.isoformat() if row.start_date else None,
                "end_date": row.end_date.isoformat() if row.end_date else None,
                "as_of": row.as_of,
                "symbol_count": row.symbol_count,
                "date_count": row.date_count,
                "row_count": row.row_count,
                "schema_version": row.schema_version,
                "immutable": row.immutable,
                "quality_status": row.quality_status,
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
