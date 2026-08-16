from enum import Enum


class Environment(str, Enum):
    BACKTEST = "backtest"
    PAPER = "paper"
    LIVE = "live"


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderStatus(str, Enum):
    """详细修改方案 §15：OMS 全量订单状态。"""

    CREATED = "created"
    RISK_REJECTED = "risk_rejected"
    APPROVED = "approved"
    SUBMITTED = "submitted"
    ACKNOWLEDGED = "acknowledged"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCEL_PENDING = "cancel_pending"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


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
