"""Kill Switch（详细修改方案 §17）。

三级熔断：global / account / strategy。任一命中即阻断对应范围的新订单。

触发原因（§17）：
POSITION_MISMATCH / DAILY_LOSS / BROKER_DISCONNECTED / MARKET_DATA_STALE /
ORDER_REJECTION_STORM / RECONCILIATION_CRITICAL / MANUAL。
"""
from __future__ import annotations

import threading
from datetime import UTC, datetime

GLOBAL_SCOPE = "global"

TRIGGER_REASONS = frozenset(
    {
        "POSITION_MISMATCH",
        "DAILY_LOSS",
        "BROKER_DISCONNECTED",
        "MARKET_DATA_STALE",
        "ORDER_REJECTION_STORM",
        "RECONCILIATION_CRITICAL",
        "MANUAL",
    }
)


def account_scope(account_id: str) -> str:
    return f"account:{account_id}"


def strategy_scope(strategy_id: str) -> str:
    return f"strategy:{strategy_id}"


class KillSwitchManager:
    """进程内熔断状态管理（生产应落库/共享存储）。"""

    def __init__(self) -> None:
        self._switches: dict[str, dict] = {}
        self._lock = threading.Lock()

    def engage(self, scope: str, reason: str, detail: str = "") -> dict:
        if reason not in TRIGGER_REASONS:
            raise ValueError(f"未知熔断原因: {reason}")
        with self._lock:
            entry = {
                "scope": scope,
                "engaged": True,
                "reason": reason,
                "detail": detail,
                "engaged_at": datetime.now(UTC).isoformat(timespec="seconds"),
            }
            self._switches[scope] = entry
            return dict(entry)

    def release(self, scope: str) -> bool:
        with self._lock:
            entry = self._switches.get(scope)
            if entry is None:
                return False
            entry["engaged"] = False
            entry["released_at"] = datetime.now(UTC).isoformat(timespec="seconds")
            return True

    def state(self) -> list[dict]:
        with self._lock:
            return [dict(entry) for entry in self._switches.values()]

    def is_blocked(self, account_id: str | None = None, strategy_id: str | None = None) -> tuple[bool, str | None]:
        """返回 (是否阻断, 命中的 scope)。global 命中一切；account/strategy 精确匹配。"""
        with self._lock:
            engaged = [entry for entry in self._switches.values() if entry["engaged"]]
        for entry in engaged:
            if entry["scope"] == GLOBAL_SCOPE:
                return True, GLOBAL_SCOPE
            if account_id and entry["scope"] == account_scope(account_id):
                return True, entry["scope"]
            if strategy_id and entry["scope"] == strategy_scope(strategy_id):
                return True, entry["scope"]
        return False, None


__all__ = [
    "GLOBAL_SCOPE",
    "TRIGGER_REASONS",
    "KillSwitchManager",
    "account_scope",
    "strategy_scope",
]
