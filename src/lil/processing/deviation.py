"""Route-deviation detection.

A deviation is not one GPS fix outside the corridor. Telemetry is noisy, so
the vehicle must stay outside the accepted corridor for a configured period
before an alert is raised. One alert per departure, keyed by vehicle.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Optional

from ..envelope import Event
from ..sim.network import Route


@dataclass
class DeviationAlert:
    alert_id: str
    vehicle_id: str
    shipment_id: str
    route_id: str
    rule_id: str
    severity: str
    start_event_time: datetime
    trigger_event_id: str
    observed_latitude: float
    observed_longitude: float
    deviation_metres: float
    explanation: str

    def as_dict(self) -> dict:
        data = self.__dict__.copy()
        data["start_event_time"] = data["start_event_time"].isoformat().replace("+00:00", "Z")
        return data


class RouteDeviationDetector:
    RULE_ID = "RD-01"

    def __init__(self, cfg, route_lookup: Callable[[str], Optional[Route]]):
        self.corridor_m = cfg.get_path("route_deviation.corridor_metres")
        self.persistence = timedelta(seconds=cfg.get_path("route_deviation.persistence_seconds"))
        self.route_lookup = route_lookup
        self._outside_since: dict[str, datetime] = {}
        self._alerted: set[str] = set()
        self._worst: dict[str, float] = {}

    def observe(self, event: Event) -> Optional[DeviationAlert]:
        if event.event_type != "gps.telemetry" or not event.route_id:
            return None
        route = self.route_lookup(event.route_id)
        if route is None:
            return None
        payload = event.payload or {}
        lat, lon = payload.get("latitude"), payload.get("longitude")
        if lat is None or lon is None:
            return None

        distance = route.distance_from_corridor_m((lat, lon))
        key = event.vehicle_id or event.event_id

        if distance <= self.corridor_m:
            self._outside_since.pop(key, None)
            self._worst.pop(key, None)
            self._alerted.discard(key)
            return None

        self._worst[key] = max(self._worst.get(key, 0.0), distance)
        since = self._outside_since.setdefault(key, event.event_time)
        if key in self._alerted:
            return None
        if event.event_time - since >= self.persistence:
            self._alerted.add(key)
            return DeviationAlert(
                alert_id=f"alert-rd-{key}-{int(since.timestamp())}",
                vehicle_id=key, shipment_id=event.shipment_id or "",
                route_id=event.route_id, rule_id=self.RULE_ID, severity="WARNING",
                start_event_time=since, trigger_event_id=event.event_id,
                observed_latitude=lat, observed_longitude=lon,
                deviation_metres=round(self._worst[key], 1),
                explanation=(
                    f"outside the {self.corridor_m} m corridor for "
                    f"{(event.event_time - since).total_seconds():.0f} s, "
                    f"peak {self._worst[key]:.0f} m"),
            )
        return None
