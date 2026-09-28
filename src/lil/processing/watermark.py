"""Event-time bookkeeping.

The watermark is the system's statement that it no longer expects events
older than (max event time seen - bounded out-of-orderness). Allowed
lateness keeps a window updatable for a further period. Anything older than
that is not dropped: it goes to the late side output, where it is counted
and can be inspected.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

ON_TIME = "ON_TIME"
LATE_WITHIN_TOLERANCE = "LATE_WITHIN_TOLERANCE"
LATE_SIDE_OUTPUT = "LATE_SIDE_OUTPUT"


@dataclass
class LatenessReport:
    classification: str
    lateness_seconds: float
    watermark: datetime


class WatermarkTracker:
    def __init__(self, watermark_seconds: int, allowed_lateness_seconds: int):
        self.out_of_orderness = timedelta(seconds=watermark_seconds)
        self.allowed_lateness = timedelta(seconds=allowed_lateness_seconds)
        self._max_event_time: datetime | None = None
        self.counts = {ON_TIME: 0, LATE_WITHIN_TOLERANCE: 0, LATE_SIDE_OUTPUT: 0}

    @property
    def watermark(self) -> datetime | None:
        if self._max_event_time is None:
            return None
        return self._max_event_time - self.out_of_orderness

    def observe(self, event_time: datetime) -> LatenessReport:
        watermark = self.watermark
        if watermark is None:
            classification = ON_TIME
            lateness = 0.0
        else:
            lateness = (watermark - event_time).total_seconds()
            if lateness <= 0:
                classification = ON_TIME
            elif lateness <= self.allowed_lateness.total_seconds():
                classification = LATE_WITHIN_TOLERANCE
            else:
                classification = LATE_SIDE_OUTPUT
        if self._max_event_time is None or event_time > self._max_event_time:
            self._max_event_time = event_time
        self.counts[classification] += 1
        return LatenessReport(classification, max(lateness, 0.0), watermark or event_time)
