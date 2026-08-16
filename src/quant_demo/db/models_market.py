"""市场数据域 / PIT 元数据 / 回测任务 / Paper 账户的 ORM 模型。

覆盖设计文档：
- §8  Market Data 数据模型（security_master / security_status_daily /
      price_limit_daily / adjustment_factor / trading_calendar /
      index_constituent / industry_membership / market_snapshot）
- §42 Quant 关键数据库对象
- §43 Backtest Job Schema
- §21 Paper API 支撑表
"""
from __future__ import annotations

from datetime import date, datetime
from uuid import uuid4

from sqlalchemy import JSON, Boolean, Date, DateTime, Float, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from quant_demo.db.base import Base


class SecurityMasterRow(Base):
    __tablename__ = "security_master"

    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    exchange: Mapped[str] = mapped_column(String(16), nullable=False, default="")
    security_type: Mapped[str] = mapped_column(String(16), nullable=False, default="STOCK")
    name: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    list_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    delist_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    board: Mapped[str | None] = mapped_column(String(32), nullable=True)
    currency: Mapped[str] = mapped_column(String(8), nullable=False, default="CNY")
    lot_size: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    tick_size: Mapped[float] = mapped_column(Float, nullable=False, default=0.01)


class SecurityStatusDailyRow(Base):
    """Point-In-Time 证券状态（§8.2）。"""

    __tablename__ = "security_status_daily"
    __table_args__ = (UniqueConstraint("trading_date", "symbol", name="uq_status_date_symbol"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trading_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    is_st: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_star_st: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_suspended: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_delisting: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    listing_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    risk_warning: Mapped[str | None] = mapped_column(String(128), nullable=True)
    available_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default="manual")


class PriceLimitDailyRow(Base):
    """Point-In-Time 涨跌停（§8.3）：不允许固定 10%/20%/30% 假设。"""

    __tablename__ = "price_limit_daily"
    __table_args__ = (UniqueConstraint("trading_date", "symbol", name="uq_limit_date_symbol"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trading_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    limit_rate: Mapped[float] = mapped_column(Float, nullable=False)
    upper_limit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    lower_limit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    rule_version: Mapped[str] = mapped_column(String(32), nullable=False, default="limit-rule-v1")
    available_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class AdjustmentFactorRow(Base):
    __tablename__ = "adjustment_factor"
    __table_args__ = (UniqueConstraint("trading_date", "symbol", name="uq_adjust_date_symbol"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trading_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    factor: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    adjustment_type: Mapped[str] = mapped_column(String(16), nullable=False, default="qfq")
    available_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class TradingCalendarRow(Base):
    __tablename__ = "trading_calendar"

    trading_date: Mapped[date] = mapped_column(Date, primary_key=True)
    exchange: Mapped[str] = mapped_column(String(16), nullable=False, default="CN_A")
    is_trading_day: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class IndexConstituentRow(Base):
    """历史指数成分（§49 防止幸存者偏差必须保留历史成分）。"""

    __tablename__ = "index_constituent"
    __table_args__ = (UniqueConstraint("index_code", "symbol", "valid_from", name="uq_index_constituent"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    index_code: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    valid_from: Mapped[date] = mapped_column(Date, nullable=False)
    valid_to: Mapped[date | None] = mapped_column(Date, nullable=True)
    available_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class IndustryMembershipRow(Base):
    """PIT 行业归属（§8.5；收尾文档 §13）。"""

    __tablename__ = "industry_membership"
    __table_args__ = (
        UniqueConstraint("symbol", "industry_standard", "industry_level", "valid_from", name="uq_industry_membership"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    industry_standard: Mapped[str] = mapped_column(String(32), nullable=False, default="SW")
    industry_level: Mapped[str] = mapped_column(String(16), nullable=False, default="L1")
    industry_code: Mapped[str] = mapped_column(String(64), nullable=False)
    valid_from: Mapped[date] = mapped_column(Date, nullable=False)
    valid_to: Mapped[date | None] = mapped_column(Date, nullable=True)
    available_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class CorporateActionRow(Base):
    """PIT 公司行动（收尾文档 §13）：分红/拆股/配股，available_at 约束 PIT 可见性。"""

    __tablename__ = "corporate_action"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    announcement_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    ex_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    action_type: Mapped[str] = mapped_column(String(32), nullable=False, default="DIVIDEND")
    cash_dividend: Mapped[float | None] = mapped_column(Float, nullable=True)
    split_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)
    rights_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)
    adjustment_factor: Mapped[float | None] = mapped_column(Float, nullable=True)
    available_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class ConceptMembershipRow(Base):
    """PIT 概念板块归属（收尾文档 §13）。"""

    __tablename__ = "concept_membership"
    __table_args__ = (UniqueConstraint("symbol", "concept_code", "valid_from", name="uq_concept_membership"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    concept_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    valid_from: Mapped[date] = mapped_column(Date, nullable=False)
    valid_to: Mapped[date | None] = mapped_column(Date, nullable=True)
    available_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class MarketSnapshotRow(Base):
    """不可变市场快照登记（§9 / §101；收尾文档 §9 字段扩展）。"""

    __tablename__ = "market_snapshots"

    snapshot_id: Mapped[str] = mapped_column(String(96), primary_key=True)
    data_version: Mapped[str] = mapped_column(String(96), nullable=False)
    dataset_uri: Mapped[str | None] = mapped_column(String(512), nullable=True)
    manifest_uri: Mapped[str | None] = mapped_column(String(512), nullable=True)
    manifest_hash: Mapped[str | None] = mapped_column(String(96), nullable=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default="qmt")
    frequency: Mapped[str] = mapped_column(String(8), nullable=False, default="1d")
    adjustment: Mapped[str] = mapped_column(String(8), nullable=False, default="qfq")
    universe: Mapped[str] = mapped_column(String(32), nullable=False, default="A_SHARE")
    start_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    end_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    as_of: Mapped[str | None] = mapped_column(String(64), nullable=True)
    symbol_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    date_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    row_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    schema_version: Mapped[str] = mapped_column(String(32), nullable=False, default="market-snapshot.v1")
    immutable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    quality_status: Mapped[str] = mapped_column(String(32), nullable=False, default="OK")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    payload_summary: Mapped[dict] = mapped_column(JSON, default=dict)


class BacktestJobRow(Base):
    """回测任务（§43 Schema）。"""

    __tablename__ = "backtest_jobs"

    backtest_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: f"bt-{uuid4().hex[:12]}")
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="QUEUED", index=True)
    strategy_id: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    strategy_version: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    market_snapshot_id: Mapped[str | None] = mapped_column(String(96), nullable=True)
    config_hash: Mapped[str] = mapped_column(String(64), nullable=False, default="", index=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(128), nullable=True, unique=True)
    start_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    end_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    initial_cash: Mapped[float] = mapped_column(Float, nullable=False, default=1_000_000.0)
    benchmark: Mapped[str | None] = mapped_column(String(32), nullable=True)
    progress: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    phase: Mapped[str] = mapped_column(String(32), nullable=False, default="queued")
    request_payload: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)


class BacktestResultRow(Base):
    __tablename__ = "backtest_results"

    backtest_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    metrics: Mapped[dict] = mapped_column(JSON, default=dict)
    quality_flags: Mapped[list] = mapped_column(JSON, default=list)
    equity: Mapped[list] = mapped_column(JSON, default=list)
    trades: Mapped[list] = mapped_column(JSON, default=list)
    positions: Mapped[list] = mapped_column(JSON, default=list)
    daily_actions: Mapped[list] = mapped_column(JSON, default=list)
    diagnostics: Mapped[dict] = mapped_column(JSON, default=dict)


class PaperAccountRow(Base):
    __tablename__ = "paper_accounts"

    account_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: f"pa-{uuid4().hex[:12]}")
    name: Mapped[str] = mapped_column(String(128), nullable=False, default="paper")
    initial_cash: Mapped[float] = mapped_column(Float, nullable=False)
    cash: Mapped[float] = mapped_column(Float, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="ACTIVE")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class PaperPlanRow(Base):
    __tablename__ = "paper_plans"

    plan_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: f"pp-{uuid4().hex[:12]}")
    account_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    signal_time: Mapped[str | None] = mapped_column(String(64), nullable=True)
    available_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    executable_from: Mapped[str | None] = mapped_column(String(64), nullable=True)
    targets: Mapped[list] = mapped_column(JSON, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class PaperOrderRow(Base):
    __tablename__ = "paper_orders"

    order_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: f"po-{uuid4().hex[:12]}")
    account_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    plan_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    trading_date: Mapped[date] = mapped_column(Date, nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    qty: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="PENDING")
    reject_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class PaperTradeRow(Base):
    __tablename__ = "paper_trades"

    trade_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: f"pt-{uuid4().hex[:12]}")
    account_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    order_id: Mapped[str] = mapped_column(String(36), nullable=False)
    trading_date: Mapped[date] = mapped_column(Date, nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    qty: Mapped[int] = mapped_column(Integer, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    commission: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    stamp_duty: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)


class PaperPositionRow(Base):
    __tablename__ = "paper_positions"
    __table_args__ = (UniqueConstraint("account_id", "trading_date", "symbol", name="uq_paper_position"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    trading_date: Mapped[date] = mapped_column(Date, nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    qty: Mapped[int] = mapped_column(Integer, nullable=False)
    sellable_qty: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    avg_cost: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)


class PaperEquityRow(Base):
    __tablename__ = "paper_equity"
    __table_args__ = (UniqueConstraint("account_id", "trading_date", name="uq_paper_equity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    trading_date: Mapped[date] = mapped_column(Date, nullable=False)
    total_asset: Mapped[float] = mapped_column(Float, nullable=False)
    cash: Mapped[float] = mapped_column(Float, nullable=False)
    market_value: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
