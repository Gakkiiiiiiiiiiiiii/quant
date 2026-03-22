from __future__ import annotations


class IntentDeduplicator:
    def __init__(self) -> None:
        self._seen: set[tuple] = set()

    def should_process(self, key: tuple) -> bool:
        if key in self._seen:
            return False
        self._seen.add(key)
        return True
