"""The simulated logistics world.

Master data is generated once from the seed, then the world is stepped in
simulated seconds. Each step returns events with the simulated time at which
they should be published, which is how dropouts and late arrivals are
modelled: the event time stays honest while the release time moves.
"""
from __future__ import annotations

import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from ..envelope import Event
from .network import NODES, WAREHOUSES, RoadNetwork, Route, interpolate
from .scenarios import ScenarioSettings

STATUS_FLOW = [
    "CREATED", "INBOUND", "PICKED", "PACKED", "DISPATCHED", "IN_TRANSIT", "DELIVERED",
]

EPOCH = datetime(2026, 9, 22, 4, 0, 0, tzinfo=timezone.utc)  # 09:30 IST

CARRIER_NAMES = ["Marwar Freight", "Thar Logistics", "Mehrangarh Carriers",
                 "Suncity Transport", "Aravalli Lines", "Rajputana Cold Chain"]


@dataclass
class Carrier:
    carrier_id: str
    carrier_code: str
    carrier_name: str


@dataclass
class Vehicle:
    vehicle_id: str
    carrier_id: str
    registration_no: str
    refrigeration_capable: bool


@dataclass
class Sensor:
    sensor_id: str
    sensor_type: str
    serial_number: str


@dataclass
class Shipment:
    shipment_id: str
    order_id: str
    carrier_id: str
    vehicle_id: str
    route_id: str
    sensor_id: str
    temperature_class: str
    promised_delivery_time: datetime
    status: str = "CREATED"
    progress: float = 0.0
    dispatched_at: datetime | None = None
    delivered_at: datetime | None = None
    dwell_until: datetime | None = None


@dataclass
class MasterData:
    carriers: list[Carrier]
    vehicles: list[Vehicle]
    sensors: list[Sensor]
    routes: list[Route]
    shipments: list[Shipment]
    warehouses: dict[str, str] = field(default_factory=lambda: dict(WAREHOUSES))


class World:
    def __init__(self, cfg, settings: ScenarioSettings, seed: int | None = None):
        self.cfg = cfg
        self.settings = settings
        self.seed = seed if seed is not None else cfg.get_path("simulation.seed")
        self.rng = random.Random(self.seed)
        # Event ids come from their own seeded stream: the same seed gives the
        # same ids, so re-running a scenario is a replay, not new data, and the
        # main random stream (and every result) is unchanged.
        self._ids = random.Random(f"{self.seed}-event-ids")
        self.network = RoadNetwork()
        self.master = self._build_master()
        self.gps_interval = cfg.get_path("simulation.gps_interval_seconds")
        self.sensor_interval = cfg.get_path("simulation.sensor_interval_seconds")
        low, high = cfg.get_path("simulation.speed_kph_range")
        self.speeds = {v.vehicle_id: self.rng.uniform(low, high) for v in self.master.vehicles}
        self.temperatures = {s.sensor_id: self.rng.uniform(3.0, 6.5) for s in self.master.sensors}
        self.batteries = {s.sensor_id: self.rng.uniform(82.0, 99.0) for s in self.master.sensors}
        self._deviating = {v.vehicle_id for v in self.master.vehicles[: settings.deviation_vehicles]}
        self._excursion = {s.sensor_id for s in self.master.sensors[: settings.excursion_sensors]}
        self._dropout_vehicle = self.master.vehicles[0].vehicle_id if settings.dropout_seconds else None
        self.max_noise_celsius = cfg.get_path("cold_chain.max_celsius")
        self.stats = {"gps": 0, "sensor": 0, "warehouse": 0, "shipment": 0,
                      "duplicates_emitted": 0, "late_emitted": 0, "invalid_emitted": 0,
                      "noise_spikes_emitted": 0}

    # ------------------------------------------------------------------ setup
    def _build_master(self) -> MasterData:
        cfg = self.cfg
        n_vehicles = cfg.get_path("simulation.vehicles")
        n_sensors = cfg.get_path("simulation.sensors")
        n_shipments = cfg.get_path("simulation.shipments")

        carriers = [
            Carrier(f"car-{i:03d}", f"C{i:02d}", name)
            for i, name in enumerate(CARRIER_NAMES, start=1)
        ]
        vehicles = [
            Vehicle(f"veh-{i:03d}", carriers[i % len(carriers)].carrier_id,
                    f"RJ19{chr(65 + i % 26)}{chr(65 + (i // 26) % 26)}{1000 + i}",
                    refrigeration_capable=(i % 3 == 0))
            for i in range(1, n_vehicles + 1)
        ]
        sensors = [Sensor(f"sen-{i:03d}", "reefer_probe", f"SN-{self.rng.randint(10**6, 10**7 - 1)}")
                   for i in range(1, n_sensors + 1)]
        routes = self.network.build_routes(self.rng, count=8)

        shipments = []
        for i in range(1, n_shipments + 1):
            vehicle = vehicles[(i - 1) % len(vehicles)]
            route = routes[(i - 1) % len(routes)]
            sensor = sensors[(i - 1) % len(sensors)]
            cold = vehicle.refrigeration_capable
            promised = EPOCH + timedelta(seconds=self.rng.randint(1500, 3300))
            shipments.append(Shipment(
                shipment_id=f"ship-{i:04d}",
                order_id=f"ord-{i:04d}",
                carrier_id=vehicle.carrier_id,
                vehicle_id=vehicle.vehicle_id,
                route_id=route.route_id,
                sensor_id=sensor.sensor_id,
                temperature_class="COLD_2_8" if cold else "AMBIENT",
                promised_delivery_time=promised,
            ))
        return MasterData(carriers, vehicles, sensors, routes, shipments)

    def _event_id(self) -> str:
        return str(uuid.UUID(int=self._ids.getrandbits(128), version=4))

    def route_by_id(self, route_id: str) -> Route:
        return next(r for r in self.master.routes if r.route_id == route_id)

    # ------------------------------------------------------------------- step
    def run(self):
        """Yield (release_time, Event) pairs for the whole simulated run."""
        duration = self.cfg.get_path("simulation.duration_seconds")
        step = self.cfg.get_path("simulation.step_seconds")
        active = self.master.shipments

        for shipment in active:
            for event in self._lifecycle_events(shipment, EPOCH, "CREATED"):
                yield EPOCH, event

        for elapsed in range(0, duration + 1, step):
            now = EPOCH + timedelta(seconds=elapsed)
            for shipment in active:
                yield from self._advance_shipment(shipment, now, elapsed)
            if elapsed % self.sensor_interval == 0:
                for shipment in active:
                    if shipment.status in {"DISPATCHED", "IN_TRANSIT"}:
                        yield from self._emit(self._sensor_event(shipment, now, elapsed), now)

    def _advance_shipment(self, shipment: Shipment, now: datetime, elapsed: int):
        stage_seconds = {"CREATED": 120, "INBOUND": 240, "PICKED": 180, "PACKED": 150}
        if shipment.status in stage_seconds:
            index = STATUS_FLOW.index(shipment.status)
            due = stage_seconds[shipment.status] * (index + 1)
            if shipment.status == "PACKED" and self.settings.dwell_extra_seconds:
                due += self.settings.dwell_extra_seconds
            if elapsed >= due:
                nxt = STATUS_FLOW[index + 1]
                shipment.status = nxt
                if nxt == "DISPATCHED":
                    shipment.dispatched_at = now
                for event in self._lifecycle_events(shipment, now, nxt):
                    yield from self._emit(event, now)
            return

        if shipment.status == "DISPATCHED":
            shipment.status = "IN_TRANSIT"

        if shipment.status == "IN_TRANSIT":
            if elapsed % self.gps_interval == 0:
                yield from self._emit(self._gps_event(shipment, now, elapsed), now)
            route = self.route_by_id(shipment.route_id)
            speed_mps = self.speeds[shipment.vehicle_id] / 3.6
            shipment.progress += (speed_mps * self.cfg.get_path("simulation.step_seconds")) / max(route.length_m(), 1.0)
            if shipment.progress >= 1.0:
                shipment.progress = 1.0
                shipment.status = "DELIVERED"
                shipment.delivered_at = now
                for event in self._lifecycle_events(shipment, now, "DELIVERED"):
                    yield from self._emit(event, now)

    # ----------------------------------------------------------------- events
    def _position(self, shipment: Shipment) -> tuple[float, float]:
        route = self.route_by_id(shipment.route_id)
        points = route.points
        span = 1.0 / max(len(points) - 1, 1)
        index = min(int(shipment.progress / span), len(points) - 2)
        local = (shipment.progress - index * span) / span
        lat, lon = interpolate(points[index], points[index + 1], local)
        if shipment.vehicle_id in self._deviating and 0.35 < shipment.progress < 0.75:
            offset = self.settings.deviation_metres / 111_320.0
            lat += offset
            lon += offset * 0.6
        return lat, lon

    def _gps_event(self, shipment: Shipment, now: datetime, elapsed: int) -> Event:
        lat, lon = self._position(shipment)
        speed = max(0.0, self.speeds[shipment.vehicle_id] + self.rng.uniform(-4.0, 4.0))
        if self.rng.random() < self.settings.invalid_rate:
            lat = 200.0  # impossible: must be quarantined, never stored as valid
            self.stats["invalid_emitted"] += 1
        self.stats["gps"] += 1
        return Event(
            event_id=self._event_id(),
            event_type="gps.telemetry", event_time=now, source="simulator",
            vehicle_id=shipment.vehicle_id, shipment_id=shipment.shipment_id,
            route_id=shipment.route_id,
            payload={"latitude": round(lat, 6), "longitude": round(lon, 6),
                     "speed_kph": round(speed, 2)},
        )

    def _sensor_event(self, shipment: Shipment, now: datetime, elapsed: int) -> Event:
        sensor_id = shipment.sensor_id
        temperature = self.temperatures[sensor_id] + self.rng.uniform(-0.35, 0.35)
        spike = (shipment.temperature_class == "COLD_2_8"
                 and sensor_id not in self._excursion
                 and self.rng.random() < 0.006)
        if spike:
            # One bad reading. It must be recorded and ignored, never alerted on.
            temperature = self.max_noise_celsius + self.rng.uniform(0.2, 2.4)
            self.stats["noise_spikes_emitted"] += 1
        if sensor_id in self._excursion and shipment.status == "IN_TRANSIT":
            start = 900
            if start <= elapsed <= start + self.settings.excursion_seconds:
                temperature = self.settings.excursion_celsius + self.rng.uniform(-0.4, 0.9)
        temperature = max(-5.0, min(20.0, temperature))
        if not spike:
            self.temperatures[sensor_id] = 0.85 * self.temperatures[sensor_id] + 0.15 * temperature
        humidity = 48.0 + self.rng.uniform(-6.0, 6.0)
        if self.rng.random() < self.settings.invalid_rate:
            humidity = 150.0  # impossible: must be quarantined
            self.stats["invalid_emitted"] += 1
        self.batteries[sensor_id] = max(1.0, self.batteries[sensor_id] - 0.003)
        self.stats["sensor"] += 1
        return Event(
            event_id=self._event_id(),
            event_type="sensor.reading", event_time=now, source="iot-simulator",
            sensor_id=sensor_id, shipment_id=shipment.shipment_id,
            vehicle_id=shipment.vehicle_id,
            payload={"temperature_c": round(temperature, 2),
                     "humidity_pct": round(humidity, 2),
                     "shock_g": round(abs(self.rng.gauss(0.04, 0.03)), 3),
                     "battery_pct": round(self.batteries[sensor_id], 2),
                     "temperature_class": shipment.temperature_class},
        )

    def _lifecycle_events(self, shipment: Shipment, now: datetime, status: str) -> list[Event]:
        warehouse_map = {"INBOUND": "warehouse.inbound", "PICKED": "warehouse.pick",
                         "PACKED": "warehouse.pack", "DISPATCHED": "warehouse.dispatch",
                         "DELIVERED": "warehouse.receive"}
        events: list[Event] = []
        route = self.route_by_id(shipment.route_id)
        if status in warehouse_map:
            warehouse_id = (route.destination_warehouse_id if status == "DELIVERED"
                            else route.origin_warehouse_id)
            self.stats["warehouse"] += 1
            events.append(Event(
                event_id=self._event_id(),
                event_type=warehouse_map[status], event_time=now, source="warehouse-simulator",
                shipment_id=shipment.shipment_id, warehouse_id=warehouse_id,
                payload={"scan_type": status.lower()},
            ))
        previous = STATUS_FLOW[max(STATUS_FLOW.index(status) - 1, 0)]
        self.stats["shipment"] += 1
        events.append(Event(
            event_id=self._event_id(),
            event_type="shipment.created" if status == "CREATED" else "shipment.status_changed",
            event_time=now, source="postgres", shipment_id=shipment.shipment_id,
            vehicle_id=shipment.vehicle_id, route_id=shipment.route_id,
            payload={"old_status": None if status == "CREATED" else previous,
                     "new_status": status,
                     "carrier_id": shipment.carrier_id,
                     "temperature_class": shipment.temperature_class,
                     "promised_delivery_time": shipment.promised_delivery_time.isoformat()},
        ))
        return events

    # ------------------------------------------------------------ fault layer
    def _emit(self, event: Event, now: datetime):
        """Apply duplication, lateness and dropout to one event."""
        settings = self.settings
        release = now

        if (self._dropout_vehicle and event.vehicle_id == self._dropout_vehicle
                and settings.dropout_seconds):
            window_start = EPOCH + timedelta(seconds=1200)
            window_end = window_start + timedelta(seconds=settings.dropout_seconds)
            if window_start <= event.event_time <= window_end:
                # The truck is in a dead zone. The reading is kept and sent on
                # reconnect, so its event time is older than what arrives before it.
                release = window_end + timedelta(seconds=5)
                self.stats["late_emitted"] += 1

        if settings.late_rate and self.rng.random() < settings.late_rate:
            release = release + timedelta(seconds=settings.late_delay_seconds)
            self.stats["late_emitted"] += 1

        yield release, event

        if settings.duplicate_rate and self.rng.random() < settings.duplicate_rate:
            self.stats["duplicates_emitted"] += 1
            yield release + timedelta(seconds=2), Event.from_dict(event.to_dict())
