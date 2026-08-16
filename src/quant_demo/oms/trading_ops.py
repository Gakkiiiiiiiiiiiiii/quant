"""Trading Ops（详细修改方案 §16 / §17 / §20）。

为 trading.v1 API 提供 Reconciliation + Kill Switch 的运行态：

- POST /api/v1/trading/reconciliation：提交 internal/broker 数据并执行对账；
  CRITICAL 自动拉闸 global kill switch（§16：CRITICAL 自动阻止新订单）。
- GET  /api/v1/trading/reconciliation：查询最近一次对账报告。
- POST /api/v1/trading/kill-switch：engage / release / state。
"""
from __future__ import annotations

from quant_demo.oms.kill_switch import GLOBAL_SCOPE, TRIGGER_REASONS, KillSwitchManager
from quant_demo.oms.reconciliation import CRITICAL, reconcile


class TradingOpsService:
    def __init__(self, kill_switch: KillSwitchManager | None = None) -> None:
        self.kill_switch = kill_switch or KillSwitchManager()
        self._last_report: dict | None = None
        self._history: list[dict] = []

    def run_reconciliation(self, internal: dict, broker: dict) -> dict:
        report = reconcile(internal, broker)
        self._last_report = report
        self._history.append(report)
        if len(self._history) > 50:
            self._history.pop(0)
        if report["max_severity"] == CRITICAL:
            # §16：出现 CRITICAL 差异 -> 自动阻止新订单。
            self.kill_switch.engage(
                GLOBAL_SCOPE,
                "RECONCILIATION_CRITICAL",
                detail=f"{report['critical_count']} critical issue(s)",
            )
            report["kill_switch_engaged"] = GLOBAL_SCOPE
        return report

    def last_report(self) -> dict | None:
        return self._last_report

    def engage_kill_switch(self, scope: str, reason: str, detail: str = "") -> dict:
        return self.kill_switch.engage(scope, reason, detail)

    def release_kill_switch(self, scope: str) -> bool:
        return self.kill_switch.release(scope)

    def kill_switch_state(self) -> list[dict]:
        return self.kill_switch.state()

    def validate_reason(self, reason: str) -> bool:
        return reason in TRIGGER_REASONS


__all__ = ["TradingOpsService"]
