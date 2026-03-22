from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

from quant_demo.core.enums import EventType, IntentSource, OrderSide, OrderStatus, RiskStatus


def _decimal(value: float | int | str | Decimal) -> Decimal:
    return Decimal(str(value))


@dataclass(slots=True)
class Event:
    event_type: EventType
    timestamp: datetime = field(default_factory=datetime.utcnow)
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class MarketBar:
    trading_date: date
    symbol: str
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: int

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "MarketBar":
        return cls(
            trading_date=row["trading_date"],
            symbol=row["symbol"],
            open_price=_decimal(row["open"]),
            high_price=_decimal(row["high"]),
            low_price=_decimal(row["low"]),
            close_price=_decimal(row["close"]),
            volume=int(row["volume"]),
        )


@dataclass(slots=True)
class Position:
    symbol: str
    qty: int
    available_qty: int
    cost_price: Decimal
    last_price: Decimal

    @property
    def market_value(self) -> Decimal:
        return self.last_price * self.qty


@dataclass(slots=True)
class AccountState:
    account_id: str
    cash: Decimal
    frozen_cash: Decimal = field(default_factory=lambda: Decimal("0"))
    positions: dict[str, Position] = field(default_factory=dict)
    turnover: Decimal = field(default_factory=lambda: Decimal("0"))
    realized_pnl: Decimal = field(default_factory=lambda: Decimal("0"))
    peak_total_asset: Decimal = field(default_factory=lambda: Decimal("0"))

    def total_asset(self, prices: dict[str, Decimal]) -> Decimal:
        market_value = Decimal("0")
        for symbol, position in self.positions.items():
            current_price = prices.get(symbol, position.last_price)
            position.last_price = current_price
            market_value += current_price * position.qty
        total = self.cash + market_value
        if total > self.peak_total_asset:
            self.peak_total_asset = total
        return total

    def to_position_dicts(self) -> list[dict[str, Any]]:
        return [
            {
                "symbol": position.symbol,
                "qty": position.qty,
                "available_qty": position.available_qty,
                "cost_price": float(position.cost_price),
                "market_price": float(position.last_price),
            }
            for position in self.positions.values()
        ]


@dataclass(slots=True)
class OrderIntent:
    account_id: str
    trading_date: date
    symbol: str
    side: OrderSide
    qty: int
    reference_price: Decimal
    limit_price: Decimal | None = None
    source: IntentSource = IntentSource.STRATEGY
    metadata: dict[str, Any] = field(default_factory=dict)
    order_intent_id: str = field(default_factory=lambda: str(uuid4()))

    def notional(self) -> Decimal:
        return self.reference_price * self.qty


@dataclass(slots=True)
class RuleResult:
    rule_name: str
    passed: bool
    message: str
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RiskDecision:
    order_intent_id: str
    status: RiskStatus
    rule_results: list[RuleResult]
    risk_decision_id: str = field(default_factory=lambda: str(uuid4()))

    def is_approved(self) -> bool:
        return self.status == RiskStatus.APPROVED


@dataclass(slots=True)
class OrderRecord:
    account_id: str
    symbol: str
    side: OrderSide
    qty: int
    order_intent_id: str
    risk_decision_id: str | None
    broker_order_id: str | None = None
    status: OrderStatus = OrderStatus.CREATED
    filled_qty: int = 0
    avg_price: Decimal | None = None
    order_id: str = field(default_factory=lambda: str(uuid4()))


@dataclass(slots=True)
class TradeFill:
    order_id: str
    symbol: str
    side: OrderSide
    fill_qty: int
    fill_price: Decimal
    commission: Decimal
    trade_time: datetime = field(default_factory=datetime.utcnow)
    trade_id: str = field(default_factory=lambda: str(uuid4()))


@dataclass(slots=True)
class AuditRecord:
    object_type: str
    object_id: str
    message: str
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class EventBus:
    subscribers: dict[EventType, list] = field(default_factory=dict)

    def subscribe(self, event_type: EventType, handler) -> None:
        self.subscribers.setdefault(event_type, []).append(handler)

    def publish(self, event_type: EventType, payload: dict[str, Any]) -> None:
        for handler in self.subscribers.get(event_type, []):
            handler(Event(event_type=event_type, payload=payload))
