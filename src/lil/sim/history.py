"""Weeks of operating history, for analytics and the models.

The scenario world (world.py) is one busy hour with one fault switched on.
Models need weeks: demand that rises and falls with the hour and the weekday,
traffic that slows peak-hour dispatches, carriers that are not equally
reliable, warehouses that back up when they are busy, and a few faults whose
ground truth is known. This module generates that history as ordinary events
with the same envelope and the same topics, so it goes through the same
validation, deduplication, detection and lakehouse path as everything else.
The models then learn from Gold, not from the simulator's internals.

What the simulator knows and the pipeline must not (which reefer was
drifting, which shipment was held at a dock, the true demand rate) is kept in
``labels``. Labels are written next to the run and are used only to score the
detectors and the forecast. Nothing in processing/ or ml/ training reads them.
"""
from __future__ import annotations

import math
import random
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta, timezone
from typing import Iterator, Optional

from ..envelope import Event, iso
from .network import NODES, WAREHOUSES, RoadNetwork, Route, haversine_m
from .world import CARRIER_NAMES, STATUS_FLOW, Carrier, MasterData, Sensor, Shipment, Vehicle

IST = timezone(timedelta(hours=5, minutes=30))

# Relative order intake by IST hour (06:00-21:59): a late-morning and an
# early-evening peak, quiet at the edges of the operating day.
HOURLY_PROFILE = {6: 0.4, 7: 0.7, 8: 1.0, 9: 1.4, 10: 1.7, 11: 1.6, 12: 1.2, 13: 1.0,
                  14: 1.0, 15: 1.1, 16: 1.3, 17: 1.6, 18: 1.7, 19: 1.3, 20: 0.8, 21: 0.5}
_PROFILE_SUM = sum(HOURLY_PROFILE.values())

# Carrier behaviour the pipeline is never told about: a speed factor and how
# much a trip's duration varies. The models have to find this in Gold.
CARRIER_SPEED = {"car-001": 1.00, "car-002": 0.95, "car-003": 0.82,
                 "car-004": 1.06, "car-005": 0.97, "car-006": 0.93}
CARRIER_SPREAD = {"car-001": 0.10, "car-002": 0.10, "car-003": 0.22,
                  "car-004": 0.08, "car-005": 0.12, "car-006": 0.10}
# Pick/pack pace by warehouse; wh-003 has the slow pack line.
WAREHOUSE_PACE = {"wh-001": 1.00, "wh-002": 0.90, "wh-003": 1.35, "wh-004": 1.00, "wh-005": 0.85}

AMBIENT_CARRIERS = {"car-001": 0.22, "car-002": 0.22, "car-003": 0.18,
                    "car-004": 0.20, "car-005": 0.18}
COLD_CARRIERS = {"car-006": 0.60, "car-001": 0.20, "car-004": 0.20}


def congestion_factor(hour_ist: int, weekday: int) -> float:
    """Share of free-flow speed a truck gets when it leaves at this hour."""
    if 8 <= hour_ist <= 10 or 17 <= hour_ist <= 19:
        factor = 0.68
    elif 11 <= hour_ist <= 16:
        factor = 0.86
    else:
        factor = 1.0
    if weekday == 6:                       # Sunday traffic is lighter
        factor = min(1.0, factor + 0.12)
    return factor


def promised_time(created: datetime, route_length_m: float) -> datetime:
    """The promise a dispatcher would quote: a fixed processing allowance plus
    transit at a planning speed of 26 km/h, plus ten minutes of buffer. It
    knows nothing about peaks, carriers or backlog, which is why it misses."""
    transit_min = (route_length_m / 1000.0) / 26.0 * 60.0
    return created + timedelta(minutes=40.0 + transit_min + 10.0)


def _poisson(rng: random.Random, lam: float) -> int:
    limit, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1


def _weighted(rng: random.Random, weights: dict[str, float]) -> str:
    keys = sorted(weights)
    return rng.choices(keys, weights=[weights[k] for k in keys], k=1)[0]


@dataclass
class HistorySettings:
    days: int = 35
    start_date: date = date(2026, 9, 7)        # a Monday
    gps_interval_seconds: int = 60
    sensor_interval_seconds: int = 120
    cold_share: float = 0.30
    weekly_growth: float = 0.01
    incident_rate: float = 0.05
    detour_rate: float = 0.01
    drift_rate: float = 0.04                   # share of cold trips with a slow reefer drift
    excursion_rate: float = 0.02               # share of cold trips with a real breach
    spike_rate: float = 0.01                   # per reading, single noisy value
    stuck_rate: float = 0.015                  # shipments held at the dock
    invalid_rate: float = 0.01
    duplicate_rate: float = 0.005
    late_rate: float = 0.003
    late_delay_seconds: int = 420
    base_daily_orders: Optional[dict] = None
    weekday_factor: tuple = (1.10, 1.00, 1.00, 1.00, 1.05, 0.85, 0.60)   # Mon..Sun

    @classmethod
    def from_config(cls, cfg, **overrides) -> "HistorySettings":
        section = dict(cfg.get_path("history", {}) or {})
        section.update({k: v for k, v in overrides.items() if v is not None})
        if isinstance(section.get("start_date"), str):
            section["start_date"] = date.fromisoformat(section["start_date"])
        if "weekday_factor" in section:
            section["weekday_factor"] = tuple(section["weekday_factor"])
        known = cls.__dataclass_fields__
        return cls(**{k: v for k, v in section.items() if k in known})


@dataclass
class Trip:
    """Everything decided about one shipment before any event is emitted."""
    shipment: Shipment
    origin: str
    destination: str
    created: datetime
    inbound: datetime
    picked: datetime
    packed: datetime
    dispatched: datetime
    delivered: datetime
    speed_kph: float
    incident: Optional[tuple[datetime, datetime]]
    detour: bool
    sensor_profile: str
    base_temperature: float
    drift_peak: float = 0.0
    excursion: Optional[tuple[datetime, datetime]] = None


class HistoryWorld:
    """Same interface as World: ``master`` for reference data, ``run()`` for
    (release_time, Event) pairs."""

    def __init__(self, cfg, settings: Optional[HistorySettings] = None, seed: Optional[int] = None):
        self.cfg = cfg
        self.settings = settings or HistorySettings.from_config(cfg)
        self.seed = seed if seed is not None else cfg.get_path("simulation.seed")
        self.rng = random.Random(self.seed)
        self._ids = random.Random(f"{self.seed}-history-event-ids")
        self.network = RoadNetwork()
        self.base_daily = self.settings.base_daily_orders or {
            "wh-001": 34, "wh-002": 22, "wh-003": 28, "wh-004": 18, "wh-005": 14}
        self.labels: dict = {"sensor_drift": [], "sensor_excursion": [], "spike_only": [],
                             "dwell_stuck": [], "transit_incident": [], "route_detour": [],
                             "expected_hourly_demand": {}}
        self.stats = {"gps": 0, "sensor": 0, "warehouse": 0, "shipment": 0,
                      "duplicates_emitted": 0, "late_emitted": 0, "invalid_emitted": 0,
                      "noise_spikes_emitted": 0}
        self.master = self._build_master()
        self.trips = self._plan()
        self.master.shipments = [t.shipment for t in self.trips]

    # ------------------------------------------------------------------ setup
    def _build_master(self) -> MasterData:
        carriers = [Carrier(f"car-{i:03d}", f"C{i:02d}", name)
                    for i, name in enumerate(CARRIER_NAMES, start=1)]
        n_vehicles = self.cfg.get_path("simulation.vehicles")
        vehicles = []
        for i in range(1, n_vehicles + 1):
            carrier = carriers[(i - 1) % len(carriers)].carrier_id
            fleet_index = (i - 1) // len(carriers)
            reefer = carrier == "car-006" or (carrier in {"car-001", "car-004"} and fleet_index < 3)
            vehicles.append(Vehicle(f"veh-{i:03d}", carrier,
                                    f"RJ19G{chr(65 + i % 26)}{2000 + i}", reefer))
        sensors = [Sensor(f"sen-{i:03d}", "reefer_probe", f"SN-{self.rng.randint(10**6, 10**7 - 1)}")
                   for i in range(1, self.cfg.get_path("simulation.sensors") + 1)]
        routes: list[Route] = []
        codes = sorted(WAREHOUSES)
        for origin in codes:
            for destination in codes:
                if origin == destination:
                    continue
                nodes = self.network.shortest_path(WAREHOUSES[origin], WAREHOUSES[destination])
                routes.append(Route(f"route-{len(routes) + 1:03d}", nodes, origin, destination))
        self.vehicle_speed = {v.vehicle_id: self.rng.uniform(28.0, 40.0) for v in vehicles}
        return MasterData(carriers, vehicles, sensors, routes, shipments=[])

    def _event_id(self) -> str:
        return str(uuid.UUID(int=self._ids.getrandbits(128), version=4))

    def _route_for(self, origin: str, destination: str) -> Route:
        return next(r for r in self.master.routes
                    if r.origin_warehouse_id == origin and r.destination_warehouse_id == destination)

    # ------------------------------------------------------------------- plan
    def _orders(self) -> list[tuple[datetime, str]]:
        s = self.settings
        orders = []
        for day_index in range(s.days):
            day = s.start_date + timedelta(days=day_index)
            growth = (1.0 + s.weekly_growth) ** (day_index / 7.0)
            weekday_factor = s.weekday_factor[day.weekday()]
            for hour, weight in HOURLY_PROFILE.items():
                hour_start = datetime.combine(day, dtime(hour), IST).astimezone(timezone.utc)
                for warehouse in sorted(self.base_daily):
                    lam = self.base_daily[warehouse] * weekday_factor * growth * weight / _PROFILE_SUM
                    self.labels["expected_hourly_demand"][f"{warehouse}|{iso(hour_start)}"] = round(lam, 4)
                    for _ in range(_poisson(self.rng, lam)):
                        created = hour_start + timedelta(seconds=self.rng.randint(0, 3599))
                        orders.append((created, warehouse))
        orders.sort()
        return orders

    def _plan(self) -> list[Trip]:
        s, rng = self.settings, self.rng
        vehicle_free: dict[str, datetime] = {}
        sensor_free: dict[str, datetime] = {}
        recent: dict[str, deque] = {w: deque() for w in WAREHOUSES}
        trips: list[Trip] = []

        for index, (created, origin) in enumerate(self._orders(), start=1):
            destination = rng.choice([w for w in sorted(WAREHOUSES) if w != origin])
            route = self._route_for(origin, destination)
            cold = rng.random() < s.cold_share
            carrier = _weighted(rng, COLD_CARRIERS if cold else AMBIENT_CARRIERS)

            window = recent[origin]
            while window and window[0] <= created - timedelta(hours=1):
                window.popleft()
            backlog = len(window)
            window.append(created)

            pace = WAREHOUSE_PACE[origin]
            inbound = created + timedelta(minutes=8.0 * rng.lognormvariate(0, 0.25))
            picked = inbound + timedelta(minutes=10.0 * pace * (1 + 0.06 * backlog)
                                         * rng.lognormvariate(0, 0.25))
            packed = picked + timedelta(minutes=7.0 * pace * rng.lognormvariate(0, 0.20))

            fleet = [v for v in self.master.vehicles
                     if v.carrier_id == carrier and (v.refrigeration_capable or not cold)]
            vehicle = min(fleet, key=lambda v: (vehicle_free.get(v.vehicle_id, created), v.vehicle_id))
            ready = max(packed, vehicle_free.get(vehicle.vehicle_id, packed))
            dispatched = ready + timedelta(minutes=4.0)
            stuck = rng.random() < s.stuck_rate
            if stuck:
                dispatched += timedelta(minutes=rng.uniform(60.0, 150.0))

            hour_ist = dispatched.astimezone(IST).hour
            speed = (self.vehicle_speed[vehicle.vehicle_id] * CARRIER_SPEED[carrier]
                     * congestion_factor(hour_ist, dispatched.astimezone(IST).weekday()))
            travel = route.length_m() / (speed / 3.6) * rng.lognormvariate(0, CARRIER_SPREAD[carrier])
            incident = None
            if rng.random() < s.incident_rate:
                stop_at = dispatched + timedelta(seconds=travel * rng.uniform(0.2, 0.8))
                incident = (stop_at, stop_at + timedelta(minutes=rng.uniform(10.0, 40.0)))
                travel += (incident[1] - incident[0]).total_seconds()
            delivered = dispatched + timedelta(seconds=travel)
            vehicle_free[vehicle.vehicle_id] = delivered + timedelta(minutes=15)

            sensor_id = None
            profile, base_temp, drift_peak, excursion = "none", 0.0, 0.0, None
            if cold:
                sensor = min(self.master.sensors,
                             key=lambda x: (max(sensor_free.get(x.sensor_id, dispatched), dispatched),
                                            x.sensor_id))
                sensor_id = sensor.sensor_id
                sensor_free[sensor_id] = delivered + timedelta(minutes=5)
                base_temp = rng.uniform(3.2, 5.2)
                draw = rng.random()
                if draw < s.excursion_rate:
                    profile = "excursion"
                    start = dispatched + timedelta(seconds=travel * rng.uniform(0.15, 0.5))
                    excursion = (start, start + timedelta(minutes=rng.uniform(8.0, 25.0)))
                elif draw < s.excursion_rate + s.drift_rate:
                    profile = "drift"
                    drift_peak = rng.uniform(6.9, 7.7)
                else:
                    profile = "normal"

            shipment_id = f"ship-h{index:05d}"
            shipment = Shipment(
                shipment_id=shipment_id, order_id=f"ord-h{index:05d}", carrier_id=carrier,
                vehicle_id=vehicle.vehicle_id, route_id=route.route_id, sensor_id=sensor_id,
                temperature_class="COLD_2_8" if cold else "AMBIENT",
                promised_delivery_time=promised_time(created, route.length_m()),
                dispatched_at=dispatched, delivered_at=delivered)
            detour = rng.random() < s.detour_rate
            trips.append(Trip(shipment, origin, destination, created, inbound, picked, packed,
                              dispatched, delivered, speed, incident, detour, profile, base_temp,
                              drift_peak, excursion))
            if stuck:
                self.labels["dwell_stuck"].append(shipment_id)
            if incident:
                self.labels["transit_incident"].append(shipment_id)
            if detour:
                self.labels["route_detour"].append(shipment_id)
            if profile == "drift":
                self.labels["sensor_drift"].append(shipment_id)
            elif profile == "excursion":
                self.labels["sensor_excursion"].append(shipment_id)
        return trips

    # ----------------------------------------------------------------- events
    def run(self) -> Iterator[tuple[datetime, Event]]:
        for trip in self.trips:
            yield from self._trip_events(trip)

    def _trip_events(self, trip: Trip):
        milestones = [("CREATED", trip.created), ("INBOUND", trip.inbound), ("PICKED", trip.picked),
                      ("PACKED", trip.packed), ("DISPATCHED", trip.dispatched)]
        for status, when in milestones:
            for event in self._lifecycle(trip, status, when):
                yield from self._emit(event, when)
        yield from self._telemetry(trip)
        for event in self._lifecycle(trip, "DELIVERED", trip.delivered):
            yield from self._emit(event, trip.delivered)

    def _lifecycle(self, trip: Trip, status: str, when: datetime) -> list[Event]:
        scans = {"INBOUND": "warehouse.inbound", "PICKED": "warehouse.pick",
                 "PACKED": "warehouse.pack", "DISPATCHED": "warehouse.dispatch",
                 "DELIVERED": "warehouse.receive"}
        shipment = trip.shipment
        events = []
        if status in scans:
            self.stats["warehouse"] += 1
            events.append(Event(
                event_id=self._event_id(),
                event_type=scans[status], event_time=when, source="warehouse-simulator",
                shipment_id=shipment.shipment_id,
                warehouse_id=trip.destination if status == "DELIVERED" else trip.origin,
                payload={"scan_type": status.lower()}))
        previous = STATUS_FLOW[max(STATUS_FLOW.index(status) - 1, 0)]
        self.stats["shipment"] += 1
        events.append(Event(
            event_id=self._event_id(),
            event_type="shipment.created" if status == "CREATED" else "shipment.status_changed",
            event_time=when, source="postgres", shipment_id=shipment.shipment_id,
            vehicle_id=shipment.vehicle_id, route_id=shipment.route_id,
            payload={"old_status": None if status == "CREATED" else previous,
                     "new_status": status, "carrier_id": shipment.carrier_id,
                     "temperature_class": shipment.temperature_class,
                     "promised_delivery_time": iso(shipment.promised_delivery_time)}))
        return events

    def _moving_fraction(self, trip: Trip, when: datetime) -> float:
        total = (trip.delivered - trip.dispatched).total_seconds()
        elapsed = (when - trip.dispatched).total_seconds()
        if trip.incident:
            stop = (trip.incident[1] - trip.incident[0]).total_seconds()
            total -= stop
            if when >= trip.incident[1]:
                elapsed -= stop
            elif when > trip.incident[0]:
                elapsed = (trip.incident[0] - trip.dispatched).total_seconds()
        return max(0.0, min(1.0, elapsed / max(total, 1.0)))

    @staticmethod
    def _along(route: Route, fraction: float) -> tuple[float, float]:
        points = route.points
        lengths = [haversine_m(points[i], points[i + 1]) for i in range(len(points) - 1)]
        target = fraction * sum(lengths)
        for i, length in enumerate(lengths):
            if target <= length or i == len(lengths) - 1:
                local = 0.0 if length == 0 else min(1.0, target / length)
                a, b = points[i], points[i + 1]
                return (a[0] + (b[0] - a[0]) * local, a[1] + (b[1] - a[1]) * local)
            target -= length
        return points[-1]

    def _telemetry(self, trip: Trip):
        s, rng = self.settings, self.rng
        shipment = trip.shipment
        route = next(r for r in self.master.routes if r.route_id == shipment.route_id)
        when = trip.dispatched
        while when <= trip.delivered:
            fraction = self._moving_fraction(trip, when)
            lat, lon = self._along(route, fraction)
            if trip.detour and 0.35 < fraction < 0.7:
                offset = 700.0 / 111_320.0
                lat, lon = lat + offset, lon + offset * 0.6
            stopped = trip.incident is not None and trip.incident[0] <= when < trip.incident[1]
            speed = 0.0 if stopped else max(0.0, trip.speed_kph * (1 + rng.gauss(0, 0.08)))
            if rng.random() < s.invalid_rate:
                lat = 200.0
                self.stats["invalid_emitted"] += 1
            self.stats["gps"] += 1
            yield from self._emit(Event(
                event_id=self._event_id(),
                event_type="gps.telemetry", event_time=when, source="simulator",
                vehicle_id=shipment.vehicle_id, shipment_id=shipment.shipment_id,
                route_id=shipment.route_id,
                payload={"latitude": round(lat, 6), "longitude": round(lon, 6),
                         "speed_kph": round(speed, 2)}), when)
            when += timedelta(seconds=s.gps_interval_seconds)

        if shipment.temperature_class != "COLD_2_8":
            return
        temperature, spiked, battery = trip.base_temperature, False, rng.uniform(70.0, 99.0)
        when = trip.dispatched
        while when <= trip.delivered:
            fraction = (when - trip.dispatched).total_seconds() / max(
                (trip.delivered - trip.dispatched).total_seconds(), 1.0)
            temperature = trip.base_temperature + 0.7 * (temperature - trip.base_temperature) \
                + rng.gauss(0, 0.18)
            reading = temperature
            if trip.sensor_profile == "drift":
                reading = min(7.85, reading + (trip.drift_peak - trip.base_temperature) * fraction)
            if trip.excursion and trip.excursion[0] <= when <= trip.excursion[1]:
                reading = rng.uniform(9.0, 11.5)
            elif trip.sensor_profile == "normal" and rng.random() < s.spike_rate:
                reading = rng.uniform(8.4, 10.2)
                spiked = True
                self.stats["noise_spikes_emitted"] += 1
            humidity = max(0.0, min(100.0, 52.0 + rng.gauss(0, 3.0)))
            if rng.random() < s.invalid_rate:
                humidity = 150.0
                self.stats["invalid_emitted"] += 1
            battery = max(1.0, battery - 0.02)
            self.stats["sensor"] += 1
            yield from self._emit(Event(
                event_id=self._event_id(),
                event_type="sensor.reading", event_time=when, source="iot-simulator",
                sensor_id=shipment.sensor_id, shipment_id=shipment.shipment_id,
                vehicle_id=shipment.vehicle_id,
                payload={"temperature_c": round(reading, 2), "humidity_pct": round(humidity, 2),
                         "shock_g": round(abs(rng.gauss(0.04, 0.03)), 3),
                         "battery_pct": round(battery, 2),
                         "temperature_class": shipment.temperature_class}), when)
            when += timedelta(seconds=s.sensor_interval_seconds)
        if spiked:
            self.labels["spike_only"].append(shipment.shipment_id)

    def _emit(self, event: Event, when: datetime):
        s, rng = self.settings, self.rng
        release = when
        if s.late_rate and rng.random() < s.late_rate:
            release = when + timedelta(seconds=s.late_delay_seconds)
            self.stats["late_emitted"] += 1
        yield release, event
        if s.duplicate_rate and rng.random() < s.duplicate_rate:
            self.stats["duplicates_emitted"] += 1
            yield release + timedelta(seconds=2), Event.from_dict(event.to_dict())
