from datetime import datetime, timezone

from lil.envelope import Event
from lil.validation.rules import Validator, quarantine_record

WHEN = datetime(2026, 9, 22, 11, 0, tzinfo=timezone.utc)


def gps(lat=26.2, lon=73.0, speed=40.0, vehicle="veh-001"):
    return Event(event_type="gps.telemetry", event_time=WHEN, source="simulator",
                 vehicle_id=vehicle,
                 payload={"latitude": lat, "longitude": lon, "speed_kph": speed})


def sensor(temp=5.0, humidity=50.0, sensor_id="sen-001"):
    return Event(event_type="sensor.reading", event_time=WHEN, source="iot-simulator",
                 sensor_id=sensor_id,
                 payload={"temperature_c": temp, "humidity_pct": humidity})


def make(cfg):
    return Validator(cfg, known_vehicles=["veh-001"], known_sensors=["sen-001"])


def test_a_good_reading_passes(cfg):
    assert make(cfg).validate(gps()).valid


def test_impossible_latitude_is_rejected_with_a_reason(cfg):
    result = make(cfg).validate(gps(lat=200.0))
    assert not result.valid
    assert "GPS_LATITUDE_RANGE" in result.reason_codes


def test_impossible_humidity_is_rejected(cfg):
    assert "SENSOR_HUMIDITY_RANGE" in make(cfg).validate(sensor(humidity=150.0)).reason_codes


def test_negative_speed_is_rejected(cfg):
    assert "GPS_SPEED_NEGATIVE" in make(cfg).validate(gps(speed=-3.0)).reason_codes


def test_unknown_vehicle_is_rejected(cfg):
    assert "GPS_VEHICLE_UNKNOWN" in make(cfg).validate(gps(vehicle="veh-999")).reason_codes


def test_unsupported_schema_version_is_rejected(cfg):
    event = gps()
    event.schema_version = "9.9"
    assert "SCHEMA_UNSUPPORTED" in make(cfg).validate(event).reason_codes


def test_quarantine_keeps_reason_raw_payload_and_arrival(cfg):
    event = gps(lat=200.0)
    result = make(cfg).validate(event)
    record = quarantine_record(event, result, "2026-09-22T11:00:03Z")
    assert record["reason_codes"] == ["GPS_LATITUDE_RANGE"]
    assert record["raw_payload"]["payload"]["latitude"] == 200.0
    assert record["arrival_time"] == "2026-09-22T11:00:03Z"
    assert record["source"] == "simulator"
