from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class Alert:
    level: str
    message: str
    payload: dict = field(default_factory=dict)


class AlertingService:
    def build_alerts(self, reconciliation_result: dict) -> list[Alert]:
        alerts: list[Alert] = []
        for issue in reconciliation_result.get("issues", []):
            alerts.append(Alert(level="warning", message=issue, payload=reconciliation_result))
        return alerts
