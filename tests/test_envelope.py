from datetime import datetime, timezone

import pytest

from lil.envelope import Event, parse_iso, to_utc


def _event(**kwargs):
    base = dict(event_type="gps.telemetry",
                event_time=datetime(2026, 9, 22, 11, 0, tzinfo=timezone.utc),
                source="simulator", payload={"latitude": 26.2, "longitude": 73.0})
    base.update(kwargs)
    return Event(**base)


def test_naive_timestamps_are_rejected():
    with pytest.raises(ValueError):
        to_utc(datetime(2026, 9, 22, 11, 0))


def test_every_event_carries_the_five_envelope_fields():
    event = _event().stamp_ingest()
    data = event.to_dict()
    for field in ("event_id", "event_time", "ingest_time", "source", "schema_version"):
        assert data[field] is not None


def test_round_trip_preserves_identity_and_time():
    event = _event(vehicle_id="veh-001").stamp_ingest()
    restored = Event.from_json(event.to_json())
    assert restored.event_id == event.event_id
    assert restored.event_time == event.event_time


def test_partition_key_is_the_entity_not_the_event():
    assert _event(vehicle_id="veh-007").key() == "veh-007"
    assert _event(event_type="sensor.reading", sensor_id="sen-003").key() == "sen-003"
    assert _event(event_type="warehouse.pick", shipment_id="ship-0001").key() == "ship-0001"


def test_times_are_normalised_to_utc():
    assert parse_iso("2026-09-22T16:30:00+05:30").hour == 11
