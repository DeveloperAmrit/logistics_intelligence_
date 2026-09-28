"""Deduplication on event_id.

The same event can reach us twice: a producer retry, a replay, or a
deliberate duplicate in a test. The business event must be counted once.

This is the streaming layer. It is not the only layer: persisted events also
carry a uniqueness constraint on event_id, because the same event travels
through several independent consumers.
"""
from __future__ import annotations

from collections import OrderedDict
from datetime import datetime, timedelta


class Deduplicator:
    def __init__(self, window_seconds: int):
        self.window = timedelta(seconds=window_seconds)
        self._seen: OrderedDict[str, datetime] = OrderedDict()
        self.duplicates = 0

    def is_duplicate(self, event_id: str, event_time: datetime) -> bool:
        self._evict(event_time)
        if event_id in self._seen:
            self.duplicates += 1
            return True
        self._seen[event_id] = event_time
        return False

    def _evict(self, now: datetime) -> None:
        cutoff = now - self.window
        while self._seen:
            oldest_id, oldest_time = next(iter(self._seen.items()))
            if oldest_time >= cutoff:
                break
            self._seen.popitem(last=False)

    def __len__(self) -> int:
        return len(self._seen)
