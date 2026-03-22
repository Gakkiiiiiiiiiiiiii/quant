from enum import Enum


class Environment(str, Enum):
    BACKTEST = "backtest"
    PAPER = "paper"
    LIVE = "live"


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderStatus(str, Enum):
    CREATED = "created"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    REJECTED = "rejected"
    CANCELLED = "cancelled"


class IntentSource(str, Enum):
    STRATEGY = "strategy"
    MANUAL = "manual"
    SYSTEM = "system"


class RiskStatus(str, Enum):
    APPROVED = "approved"
    REJECTED = "rejected"
    MANUAL_REVIEW = "manual_review"


class EventType(str, Enum):
    MARKET = "market"
    SIGNAL = "signal"
    ORDER_INTENT = "order_intent"
    RISK_DECISION = "risk_decision"
    ORDER = "order"
    TRADE = "trade"
    SNAPSHOT = "snapshot"
    AUDIT = "audit"
