"""OMS 防护测试（详细修改方案 §15 / §16 / §17 / §20）。"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from quant_demo.api.v1_app import dispatch
from quant_demo.core.enums import OrderSide, OrderStatus, RiskStatus
from quant_demo.core.events import OrderIntent, RiskDecision
from quant_demo.db.models import Base as LegacyBase
from quant_demo.oms.kill_switch import GLOBAL_SCOPE, KillSwitchManager, account_scope, strategy_scope
from quant_demo.oms.reconciliation import CRITICAL, INFO, WARNING, reconcile
from quant_demo.oms.service import OmsService, OrderBlockedError
from quant_demo.oms.state_machine import validate_transition

from v1_fixtures import build_state


def _oms(tmp_path):
    engine = create_engine(f"sqlite:///{(tmp_path / 'oms.db').as_posix()}")
    LegacyBase.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    return OmsService(session)


def _intent() -> OrderIntent:
    return OrderIntent(
        account_id="acc-1",
        trading_date=date(2026, 8, 14),
        symbol="600519.SH",
        side=OrderSide.BUY,
        qty=100,
        reference_price=Decimal("1500.0"),
    )


def _decision(intent: OrderIntent) -> RiskDecision:
    return RiskDecision(order_intent_id=intent.order_intent_id, status=RiskStatus.APPROVED, rule_results=[])


# ------------------------------------------------------------------ §15 状态机
def test_state_machine_full_lifecycle():
    order = OrderStatus.CREATED
    for target in (
        OrderStatus.APPROVED,
        OrderStatus.SUBMITTED,
        OrderStatus.ACKNOWLEDGED,
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
    ):
        validate_transition(order, target)
        order = target


def test_state_machine_rejects_illegal_transitions():
    with pytest.raises(ValueError):
        validate_transition(OrderStatus.FILLED, OrderStatus.CANCELLED)  # 终态不可迁移
    with pytest.raises(ValueError):
        validate_transition(OrderStatus.CREATED, OrderStatus.FILLED)  # 不允许跳级
    with pytest.raises(ValueError):
        validate_transition(OrderStatus.RISK_REJECTED, OrderStatus.SUBMITTED)


def test_state_machine_unknown_recovery_and_cancel_pending():
    validate_transition(OrderStatus.SUBMITTED, OrderStatus.UNKNOWN)
    validate_transition(OrderStatus.UNKNOWN, OrderStatus.ACKNOWLEDGED)
    validate_transition(OrderStatus.ACKNOWLEDGED, OrderStatus.CANCEL_PENDING)
    validate_transition(OrderStatus.CANCEL_PENDING, OrderStatus.CANCELLED)


# ------------------------------------------------------- §15 下单必备身份字段
def test_place_order_requires_identity_fields(tmp_path):
    oms = _oms(tmp_path)
    intent = _intent()
    decision = _decision(intent)
    with pytest.raises(OrderBlockedError):
        oms.place_order(
            intent, decision,
            client_order_id="co-1", idempotency_key="", decision_id="d-1", portfolio_id="p-1",
        )


def test_place_order_idempotent_by_key(tmp_path):
    oms = _oms(tmp_path)
    intent = _intent()
    decision = _decision(intent)
    kwargs = {
        "client_order_id": "co-1", "idempotency_key": "idem-1",
        "decision_id": "d-1", "portfolio_id": "p-1",
    }
    first, reused_first = oms.place_order(intent, decision, **kwargs)
    oms.session.commit()
    second, reused_second = oms.place_order(intent, decision, **kwargs)
    assert reused_first is False and reused_second is True
    assert first.order_id == second.order_id
    assert second.decision_id == "d-1" and second.portfolio_id == "p-1"


# ------------------------------------------------------------------ §17 Kill Switch
def test_kill_switch_scopes_block_orders(tmp_path):
    oms = _oms(tmp_path)
    kill_switch = KillSwitchManager()
    intent = _intent()
    decision = _decision(intent)
    kwargs = {"client_order_id": "co-2", "idempotency_key": "idem-2", "decision_id": "d-2", "portfolio_id": "p-2"}

    kill_switch.engage(account_scope("acc-1"), "DAILY_LOSS")
    with pytest.raises(OrderBlockedError):
        oms.place_order(intent, decision, kill_switch=kill_switch, **kwargs)

    # account 熔断不影响其他账户
    other = _intent()
    other.account_id = "acc-2"
    order, reused = oms.place_order(other, _decision(other), kill_switch=kill_switch, **dict(kwargs, idempotency_key="idem-3"))
    assert reused is False and order.order_id

    # strategy 级熔断
    kill_switch.engage(strategy_scope("s-1"), "ORDER_REJECTION_STORM")
    blocked_intent = _intent()
    blocked_intent.account_id = "acc-3"
    with pytest.raises(OrderBlockedError):
        oms.place_order(
            blocked_intent, _decision(blocked_intent), kill_switch=kill_switch,
            strategy_id="s-1", **dict(kwargs, idempotency_key="idem-4"),
        )

    # global 熔断解除后可下单
    kill_switch.engage(GLOBAL_SCOPE, "BROKER_DISCONNECTED")
    blocked, scope = kill_switch.is_blocked("acc-9", None)
    assert blocked and scope == GLOBAL_SCOPE
    kill_switch.release(GLOBAL_SCOPE)
    assert kill_switch.is_blocked("acc-9", None)[0] is False


def test_kill_switch_rejects_unknown_reason():
    with pytest.raises(ValueError):
        KillSwitchManager().engage(GLOBAL_SCOPE, "NOT_A_REASON")


# ------------------------------------------------------------------ §16 Reconciliation
def test_reconcile_clean_books_no_issues():
    books = {
        "orders": [{"client_order_id": "co-1", "status": "filled", "filled_qty": 100}],
        "trades": [{"symbol": "600519.SH", "side": "buy", "qty": 100, "price": 1500.0}],
        "positions": [{"symbol": "600519.SH", "qty": 100}],
    }
    report = reconcile(books, books)
    assert report["issue_count"] == 0 and report["max_severity"] == INFO


def test_reconcile_position_mismatch_is_critical():
    internal = {"orders": [], "trades": [], "positions": [{"symbol": "600519.SH", "qty": 100}]}
    broker = {"orders": [], "trades": [], "positions": [{"symbol": "600519.SH", "qty": 90}]}
    report = reconcile(internal, broker)
    assert report["max_severity"] == CRITICAL
    assert any(issue["category"] == "POSITION" for issue in report["issues"])


def test_reconcile_order_and_trade_diffs():
    internal = {
        "orders": [{"client_order_id": "co-1", "status": "filled", "filled_qty": 100}],
        "trades": [{"symbol": "A", "side": "buy", "qty": 100, "price": 10.0}],
        "positions": [],
    }
    broker = {
        "orders": [
            {"client_order_id": "co-1", "status": "filled", "filled_qty": 90},
            {"client_order_id": "co-ghost", "status": "filled", "filled_qty": 10},
        ],
        "trades": [],
        "positions": [],
    }
    report = reconcile(internal, broker)
    severities = {issue["severity"] for issue in report["issues"]}
    assert CRITICAL in severities  # 成交数量不一致 + 券商幽灵订单 + 内部成交缺失
    assert WARNING in severities or CRITICAL in severities


# ------------------------------------------------------------------ §20 API
def test_trading_reconciliation_api_engages_kill_switch_on_critical(tmp_path):
    build_state(tmp_path, days=30)
    internal = {"orders": [], "trades": [], "positions": [{"symbol": "600519.SH", "qty": 100}]}
    broker = {"orders": [], "trades": [], "positions": [{"symbol": "600519.SH", "qty": 1}]}
    status, payload = dispatch("POST", "/api/v1/trading/reconciliation", {}, {"internal": internal, "broker": broker}, {})
    assert status == 200
    report = payload["data"]
    assert report["max_severity"] == CRITICAL
    assert report["kill_switch_engaged"] == GLOBAL_SCOPE

    status, payload = dispatch("GET", "/api/v1/trading/reconciliation", {}, {}, {})
    assert status == 200 and payload["data"]["report"]["critical_count"] >= 1

    # kill switch state 可见且可手动解除
    status, payload = dispatch("POST", "/api/v1/trading/kill-switch", {}, {"action": "state"}, {})
    assert status == 200
    assert any(entry["scope"] == GLOBAL_SCOPE and entry["engaged"] for entry in payload["data"]["switches"])
    status, payload = dispatch("POST", "/api/v1/trading/kill-switch", {}, {"action": "release", "scope": GLOBAL_SCOPE}, {})
    assert status == 200 and payload["data"]["engaged"] is False


def test_trading_kill_switch_api_manual_engage(tmp_path):
    build_state(tmp_path, days=30)
    status, payload = dispatch(
        "POST", "/api/v1/trading/kill-switch", {},
        {"action": "engage", "scope": "account:acc-1", "reason": "MARKET_DATA_STALE"}, {},
    )
    assert status == 200 and payload["data"]["reason"] == "MARKET_DATA_STALE"

    status, payload = dispatch(
        "POST", "/api/v1/trading/kill-switch", {},
        {"action": "engage", "scope": "global", "reason": "NOT_A_REASON"}, {},
    )
    assert status == 422

    status, payload = dispatch("POST", "/api/v1/trading/kill-switch", {}, {"action": "release", "scope": "strategy:none"}, {})
    assert status == 404
