"""Validation.

Two layers, in this order:

  Structural - is this a record we can read at all? Missing identifier,
               missing timestamp, unknown event type, unsupported schema
               version, required field absent.
  Domain     - is the value possible, and does it reference something we
               know about? Latitude in range, speed not negative, sensor
               assigned, shipment known.

A failing record is never dropped. It is returned with a reason code so the
caller can route it to the quarantine topic with its raw payload intact.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Optional

from ..envelope import EVENT_TYPES, SCHEMA_VERSION, Event


@dataclass
class ValidationError:
    rule_id: str
    field: str
    observed: object
    expected: str
    severity: str = "ERROR"

    def as_dict(self) -> dict:
        return {"rule_id": self.rule_id, "field": self.field,
                "observed": self.observed, "expected": self.expected,
                "severity": self.severity}


@dataclass
class ValidationResult:
    valid: bool
    event: Optional[Event] = None
    errors: list[ValidationError] = field(default_factory=list)

    @property
    def reason_codes(self) -> list[str]:
        return [error.rule_id for error in self.errors]


class Validator:
    """Holds the reference data that domain checks are made against."""

    SUPPORTED_SCHEMAS = {"1.0", SCHEMA_VERSION}

    def __init__(self, cfg, known_vehicles: Iterable[str] = (),
                 known_sensors: Iterable[str] = (), known_shipments: Iterable[str] = ()):
        self.cfg = cfg
        self.lat_min, self.lat_max = cfg.get_path("validation.latitude_range")
        self.lon_min, self.lon_max = cfg.get_path("validation.longitude_range")
        self.temp_min, self.temp_max = cfg.get_path("validation.temperature_range")
        self.hum_min, self.hum_max = cfg.get_path("validation.humidity_range")
        self.vehicles = set(known_vehicles)
        self.sensors = set(known_sensors)
        self.shipments = set(known_shipments)

    # ------------------------------------------------------------ structural
    def _structural(self, event: Event) -> list[ValidationError]:
        errors: list[ValidationError] = []
        if not event.event_id:
            errors.append(ValidationError("EVT_ID_MISSING", "event_id", None, "non-null"))
        if event.event_time is None:
            errors.append(ValidationError("EVT_TIME_MISSING", "event_time", None, "non-null"))
        if event.event_type not in EVENT_TYPES:
            errors.append(ValidationError("EVT_TYPE_UNKNOWN", "event_type",
                                          event.event_type, "a registered event type"))
        if event.schema_version not in self.SUPPORTED_SCHEMAS:
            errors.append(ValidationError("SCHEMA_UNSUPPORTED", "schema_version",
                                          event.schema_version,
                                          f"one of {sorted(self.SUPPORTED_SCHEMAS)}"))
        return errors

    # ---------------------------------------------------------------- domain
    def _domain(self, event: Event) -> list[ValidationError]:
        errors: list[ValidationError] = []
        payload = event.payload or {}

        if event.event_type == "gps.telemetry":
            lat, lon = payload.get("latitude"), payload.get("longitude")
            speed = payload.get("speed_kph")
            if lat is None or not (self.lat_min <= lat <= self.lat_max):
                errors.append(ValidationError("GPS_LATITUDE_RANGE", "latitude", lat,
                                              f"[{self.lat_min}, {self.lat_max}]"))
            if lon is None or not (self.lon_min <= lon <= self.lon_max):
                errors.append(ValidationError("GPS_LONGITUDE_RANGE", "longitude", lon,
                                              f"[{self.lon_min}, {self.lon_max}]"))
            if speed is None or speed < 0:
                errors.append(ValidationError("GPS_SPEED_NEGATIVE", "speed_kph", speed, ">= 0"))
            if self.vehicles and event.vehicle_id not in self.vehicles:
                errors.append(ValidationError("GPS_VEHICLE_UNKNOWN", "vehicle_id",
                                              event.vehicle_id, "a known vehicle"))

        elif event.event_type == "sensor.reading":
            temperature, humidity = payload.get("temperature_c"), payload.get("humidity_pct")
            if temperature is None or not (self.temp_min <= temperature <= self.temp_max):
                errors.append(ValidationError("SENSOR_TEMP_RANGE", "temperature_c", temperature,
                                              f"[{self.temp_min}, {self.temp_max}]"))
            if humidity is None or not (self.hum_min <= humidity <= self.hum_max):
                errors.append(ValidationError("SENSOR_HUMIDITY_RANGE", "humidity_pct", humidity,
                                              f"[{self.hum_min}, {self.hum_max}]"))
            if self.sensors and event.sensor_id not in self.sensors:
                errors.append(ValidationError("SENSOR_UNKNOWN", "sensor_id",
                                              event.sensor_id, "a known sensor"))

        if event.shipment_id and self.shipments and event.shipment_id not in self.shipments:
            errors.append(ValidationError("SHIPMENT_UNKNOWN", "shipment_id",
                                          event.shipment_id, "a known shipment"))
        return errors

    def validate(self, event: Event) -> ValidationResult:
        errors = self._structural(event)
        if not errors:
            errors = self._domain(event)
        if errors:
            return ValidationResult(False, event, errors)
        return ValidationResult(True, event, [])


def quarantine_record(event: Event, result: ValidationResult, arrival_iso: str) -> dict:
    """What goes on quarantine.dlq: reason, raw payload, arrival time, source.

    The raw payload is kept so the record can be replayed once the cause is
    fixed, and the reasons are kept so failures can be reported by rule.
    """
    return {
        "event_id": event.event_id,
        "event_type": event.event_type,
        "reasons": [error.as_dict() for error in result.errors],
        "reason_codes": result.reason_codes,
        "raw_payload": event.to_dict(),
        "arrival_time": arrival_iso,
        "source": event.source,
    }
