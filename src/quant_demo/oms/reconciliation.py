"""Reconciliation（详细修改方案 §16）。

每日对账：Internal Orders/Trades/Positions vs Broker Orders/Trades/Positions。
差异产生 ReconciliationIssue，严重级别 INFO / WARNING / CRITICAL；
出现 CRITICAL 时必须自动阻止新订单（由调用方联动 Kill Switch）。
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

INFO = "INFO"
WARNING = "WARNING"
CRITICAL = "CRITICAL"

_SEVERITY_RANK = {INFO: 0, WARNING: 1, CRITICAL: 2}


def _issue(severity: str, category: str, key: str, internal: Any, broker: Any, message: str) -> dict:
    return {
        "severity": severity,
        "category": category,
        "key": key,
        "internal": internal,
        "broker": broker,
        "message": message,
    }


def reconcile(internal: dict, broker: dict) -> dict:
    """对账入口。

    internal/broker 结构：
        {"orders": [{client_order_id, status, filled_qty}],
         "trades": [{trade_id, symbol, side, qty, price}],
         "positions": [{symbol, qty}]}
    """
    issues: list[dict] = []
    issues.extend(_reconcile_orders(internal.get("orders") or [], broker.get("orders") or []))
    issues.extend(_reconcile_trades(internal.get("trades") or [], broker.get("trades") or []))
    issues.extend(_reconcile_positions(internal.get("positions") or [], broker.get("positions") or []))
    max_severity = INFO
    for item in issues:
        if _SEVERITY_RANK[item["severity"]] > _SEVERITY_RANK[max_severity]:
            max_severity = item["severity"]
    return {
        "reconciled_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "max_severity": max_severity,
        "issue_count": len(issues),
        "critical_count": sum(1 for item in issues if item["severity"] == CRITICAL),
        "issues": issues,
    }


def _reconcile_orders(internal_orders: list[dict], broker_orders: list[dict]) -> list[dict]:
    issues: list[dict] = []
    broker_map = {order.get("client_order_id"): order for order in broker_orders if order.get("client_order_id")}
    seen = set()
    for order in internal_orders:
        key = order.get("client_order_id")
        if not key:
            issues.append(_issue(WARNING, "ORDER", "missing-client-order-id", order, None, "内部订单缺少 client_order_id"))
            continue
        seen.add(key)
        broker_order = broker_map.get(key)
        if broker_order is None:
            issues.append(_issue(WARNING, "ORDER", key, order, None, "内部订单在券商侧不存在"))
            continue
        if str(order.get("status", "")).lower() != str(broker_order.get("status", "")).lower():
            issues.append(
                _issue(WARNING, "ORDER", key, order.get("status"), broker_order.get("status"), "订单状态不一致")
            )
        if int(order.get("filled_qty", 0)) != int(broker_order.get("filled_qty", 0)):
            issues.append(
                _issue(CRITICAL, "ORDER", key, order.get("filled_qty"), broker_order.get("filled_qty"), "成交数量不一致")
            )
    for key, broker_order in broker_map.items():
        if key not in seen:
            issues.append(_issue(CRITICAL, "ORDER", key, None, broker_order, "券商侧订单在内部不存在（可能漏记）"))
    return issues


def _reconcile_trades(internal_trades: list[dict], broker_trades: list[dict]) -> list[dict]:
    issues: list[dict] = []

    def _signature(trade: dict) -> tuple:
        return (trade.get("symbol"), str(trade.get("side", "")).lower(), int(trade.get("qty", 0)), float(trade.get("price", 0.0)))

    broker_counter: dict[tuple, int] = {}
    for trade in broker_trades:
        broker_counter[_signature(trade)] = broker_counter.get(_signature(trade), 0) + 1
    internal_counter: dict[tuple, int] = {}
    for trade in internal_trades:
        internal_counter[_signature(trade)] = internal_counter.get(_signature(trade), 0) + 1
    for signature, count in internal_counter.items():
        broker_count = broker_counter.get(signature, 0)
        if count != broker_count:
            severity = CRITICAL if broker_count == 0 else WARNING
            issues.append(_issue(severity, "TRADE", "|".join(map(str, signature)), count, broker_count, "成交记录数量不一致"))
    for signature, count in broker_counter.items():
        if signature not in internal_counter:
            issues.append(_issue(CRITICAL, "TRADE", "|".join(map(str, signature)), 0, count, "券商成交在内部不存在"))
    return issues


def _reconcile_positions(internal_positions: list[dict], broker_positions: list[dict]) -> list[dict]:
    issues: list[dict] = []
    broker_map = {position.get("symbol"): int(position.get("qty", 0)) for position in broker_positions}
    internal_map = {position.get("symbol"): int(position.get("qty", 0)) for position in internal_positions}
    for symbol in sorted(set(internal_map) | set(broker_map)):
        internal_qty = internal_map.get(symbol, 0)
        broker_qty = broker_map.get(symbol, 0)
        if internal_qty != broker_qty:
            # 持仓不一致是最高级别风险：必须阻止新订单（§16/§17）。
            issues.append(_issue(CRITICAL, "POSITION", symbol, internal_qty, broker_qty, "持仓数量不一致"))
    return issues


__all__ = ["INFO", "WARNING", "CRITICAL", "reconcile"]
