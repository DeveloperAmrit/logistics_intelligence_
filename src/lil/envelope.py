"""The event envelope.

Every record on every topic carries the same five fields:
event_id, event_time, ingest_time, source, schema_version.

event_time is when the measurement happened in the simulated world.
ingest_time is when our system first saw it. Keeping them apart is what
makes late and out-of-order reasoning possible at all.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Optional

SCHEMA_VERSION = "1.1"

EVENT_TYPES = {
    "gps.telemetry",
    "sensor.reading",
    "warehouse.inbound",
    "warehouse.pick",
    "warehouse.pack",
    "warehouse.dispatch",
    "warehouse.receive",
    "shipment.created",
    "shipment.status_changed",
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def to_utc(value: datetime) -> datetime:
    """Force an aware UTC datetime. Naive input is rejected, not guessed."""
    if value.tzinfo is None:
        raise ValueError("naive datetime rejected; event times must carry a timezone")
    return value.astimezone(timezone.utc)


def iso(value: datetime) -> str:
    return to_utc(value).isoformat().replace("+00:00", "Z")


def parse_iso(value: str) -> datetime:
    return to_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


@dataclass
class Event:
    event_type: str
    event_time: datetime
    source: str
    payload: dict = field(default_factory=dict)
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    ingest_time: Optional[datetime] = None
    schema_version: str = SCHEMA_VERSION
    vehicle_id: Optional[str] = None
    sensor_id: Optional[str] = None
    shipment_id: Optional[str] = None
    warehouse_id: Optional[str] = None
    route_id: Optional[str] = None

    def __post_init__(self) -> None:
        self.event_time = to_utc(self.event_time)
        if self.ingest_time is not None:
            self.ingest_time = to_utc(self.ingest_time)

    def stamp_ingest(self, when: Optional[datetime] = None) -> "Event":
        self.ingest_time = to_utc(when or utcnow())
        return self

    def key(self) -> str:
        """Partition key: keeps one entity's events ordered inside a partition."""
        if self.event_type == "gps.telemetry":
            return self.vehicle_id or self.event_id
        if self.event_type == "sensor.reading":
            return self.sensor_id or self.event_id
        return self.shipment_id or self.event_id

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["event_time"] = iso(self.event_time)
        data["ingest_time"] = iso(self.ingest_time) if self.ingest_time else None
        return data

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Event":
        data = dict(data)
        data["event_time"] = parse_iso(data["event_time"])
        if data.get("ingest_time"):
            data["ingest_time"] = parse_iso(data["ingest_time"])
        known = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in data.items() if k in known})

    @classmethod
    def from_json(cls, raw: str) -> "Event":
        return cls.from_dict(json.loads(raw))
