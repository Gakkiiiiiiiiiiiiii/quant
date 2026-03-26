from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import JSON, Date, DateTime, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from quant_demo.db.base import Base


class StrategyVersionModel(Base):
    __tablename__ = "strategy_versions"

    strategy_version_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    strategy_name: Mapped[str] = mapped_column(String(64), nullable=False)
    version_label: Mapped[str] = mapped_column(String(64), nullable=False)
    implementation: Mapped[str] = mapped_column(String(64), nullable=False)
    parameters: Mapped[dict] = mapped_column(JSON, default=dict)
    metrics: Mapped[dict] = mapped_column(JSON, default=dict)
    artifact_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class PromotionRequestModel(Base):
    __tablename__ = "promotion_requests"

    promotion_request_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    strategy_version_id: Mapped[str] = mapped_column(ForeignKey("strategy_versions.strategy_version_id"), nullable=False)
    object_type: Mapped[str] = mapped_column(String(32), nullable=False)
    requested_by: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class OrderIntentModel(Base):
    __tablename__ = "order_intents"

    order_intent_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    strategy_version_id: Mapped[str | None] = mapped_column(ForeignKey("strategy_versions.strategy_version_id"), nullable=True)
    account_id: Mapped[str] = mapped_column(String(64), nullable=False)
    trading_date: Mapped[date] = mapped_column(Date, nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    qty: Mapped[int] = mapped_column(Integer, nullable=False)
    limit_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    reference_price: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    source: Mapped[str] = mapped_column(String(16), nullable=False)
    metadata_json: Mapped[dict] = mapped_column("metadata", JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    risk_decision: Mapped["RiskDecisionModel | None"] = relationship(back_populates="order_intent", uselist=False)
    orders: Mapped[list["OrderModel"]] = relationship(back_populates="order_intent")


class RiskDecisionModel(Base):
    __tablename__ = "risk_decisions"

    risk_decision_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    order_intent_id: Mapped[str] = mapped_column(ForeignKey("order_intents.order_intent_id"), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    rule_results: Mapped[list] = mapped_column(JSON, default=list)
    decided_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    order_intent: Mapped[OrderIntentModel] = relationship(back_populates="risk_decision")


class OrderModel(Base):
    __tablename__ = "orders"

    order_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    order_intent_id: Mapped[str] = mapped_column(ForeignKey("order_intents.order_intent_id"), nullable=False)
    risk_decision_id: Mapped[str | None] = mapped_column(ForeignKey("risk_decisions.risk_decision_id"), nullable=True)
    broker_order_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    account_id: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    qty: Mapped[int] = mapped_column(Integer, nullable=False)
    filled_qty: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    avg_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    order_intent: Mapped[OrderIntentModel] = relationship(back_populates="orders")
    order_events: Mapped[list["OrderEventModel"]] = relationship(back_populates="order")
    trades: Mapped[list["TradeModel"]] = relationship(back_populates="order")


class OrderEventModel(Base):
    __tablename__ = "order_events"

    order_event_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[str] = mapped_column(ForeignKey("orders.order_id"), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    source: Mapped[str] = mapped_column(String(16), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    event_time: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    order: Mapped[OrderModel] = relationship(back_populates="order_events")


class TradeModel(Base):
    __tablename__ = "trades"

    trade_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    order_id: Mapped[str] = mapped_column(ForeignKey("orders.order_id"), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    fill_qty: Mapped[int] = mapped_column(Integer, nullable=False)
    fill_price: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    commission: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False, default=0)
    trade_time: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    order: Mapped[OrderModel] = relationship(back_populates="trades")


class PositionSnapshotModel(Base):
    __tablename__ = "position_snapshots"
    __table_args__ = (UniqueConstraint("account_id", "snapshot_time", "symbol"),)

    snapshot_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    qty: Mapped[int] = mapped_column(Integer, nullable=False)
    available_qty: Mapped[int] = mapped_column(Integer, nullable=False)
    cost_price: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    market_price: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    snapshot_time: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class AssetSnapshotModel(Base):
    __tablename__ = "asset_snapshots"
    __table_args__ = (UniqueConstraint("account_id", "snapshot_time"),)

    snapshot_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[str] = mapped_column(String(64), nullable=False)
    cash: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    frozen_cash: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False, default=0)
    total_asset: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    total_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False, default=0)
    turnover: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False, default=0)
    max_drawdown: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False, default=0)
    snapshot_time: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class AuditLogModel(Base):
    __tablename__ = "audit_logs"

    audit_log_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    object_type: Mapped[str] = mapped_column(String(32), nullable=False)
    object_id: Mapped[str] = mapped_column(String(64), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class CommonStrategyModel(Base):
    __tablename__ = "common_strategies"
    __table_args__ = (UniqueConstraint("strategy_key"),)

    strategy_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    strategy_key: Mapped[str] = mapped_column(String(32), nullable=False)
    display_name: Mapped[str] = mapped_column(String(64), nullable=False)
    is_active: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    backtest_results: Mapped[list["StrategyBacktestResultModel"]] = relationship(back_populates="strategy")


class StrategyBacktestResultModel(Base):
    __tablename__ = "strategy_backtest_results"

    backtest_result_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    strategy_id: Mapped[str] = mapped_column(ForeignKey("common_strategies.strategy_id"), nullable=False)
    run_key: Mapped[str] = mapped_column(String(64), nullable=False)
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    start_date: Mapped[str] = mapped_column(String(16), nullable=False)
    end_date: Mapped[str] = mapped_column(String(16), nullable=False)
    account: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False, default=0)
    total_return: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    annualized_return: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    max_drawdown: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    ending_equity: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    report_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    risk_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    daily_action_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    daily_decision_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_payload: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    strategy: Mapped[CommonStrategyModel] = relationship(back_populates="backtest_results")
