from __future__ import annotations

from quant_demo.core.events import EventBus
from quant_demo.core.enums import EventType


class CallbacksConsumer:
    def __init__(self, event_bus: EventBus) -> None:
        self.event_bus = event_bus

    def emit_fill(self, payload: dict) -> None:
        self.event_bus.publish(EventType.TRADE, payload)
