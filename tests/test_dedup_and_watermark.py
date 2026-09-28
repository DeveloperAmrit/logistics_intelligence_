from datetime import datetime, timedelta, timezone

from lil.processing.dedup import Deduplicator
from lil.processing.watermark import (LATE_SIDE_OUTPUT, LATE_WITHIN_TOLERANCE, ON_TIME,
                                      WatermarkTracker)

WHEN = datetime(2026, 9, 22, 11, 0, tzinfo=timezone.utc)


def test_the_same_event_id_is_counted_once():
    dedup = Deduplicator(window_seconds=3600)
    assert dedup.is_duplicate("evt-1", WHEN) is False
    assert dedup.is_duplicate("evt-1", WHEN) is True
    assert dedup.duplicates == 1


def test_different_events_are_not_duplicates():
    dedup = Deduplicator(window_seconds=3600)
    assert dedup.is_duplicate("evt-1", WHEN) is False
    assert dedup.is_duplicate("evt-2", WHEN) is False


def test_the_dedup_window_eventually_forgets():
    dedup = Deduplicator(window_seconds=60)
    dedup.is_duplicate("evt-1", WHEN)
    assert dedup.is_duplicate("evt-1", WHEN + timedelta(seconds=600)) is False


def test_in_order_events_are_on_time():
    tracker = WatermarkTracker(60, 300)
    for offset in (0, 10, 20):
        assert tracker.observe(WHEN + timedelta(seconds=offset)).classification == ON_TIME


def test_a_twenty_second_late_event_is_still_on_time():
    tracker = WatermarkTracker(60, 300)
    tracker.observe(WHEN + timedelta(seconds=120))
    assert tracker.observe(WHEN + timedelta(seconds=100)).classification == ON_TIME


def test_a_two_minute_late_event_is_tolerated():
    tracker = WatermarkTracker(60, 300)
    tracker.observe(WHEN + timedelta(seconds=600))
    report = tracker.observe(WHEN + timedelta(seconds=420))
    assert report.classification == LATE_WITHIN_TOLERANCE


def test_a_ten_minute_late_event_goes_to_the_side_output():
    tracker = WatermarkTracker(60, 300)
    tracker.observe(WHEN + timedelta(seconds=900))
    report = tracker.observe(WHEN + timedelta(seconds=180))
    assert report.classification == LATE_SIDE_OUTPUT
    assert report.lateness_seconds > 300
    assert tracker.counts[LATE_SIDE_OUTPUT] == 1
