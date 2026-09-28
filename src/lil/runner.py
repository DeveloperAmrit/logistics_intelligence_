"""One end-to-end run: simulate, publish, process, land, build, report.

This is what 'make demo' executes. The same components run against Kafka and
PostgreSQL when the Docker stack is up; only the bus backend changes.
"""
from __future__ import annotations

import heapq
import json
import time
from dataclasses import dataclass
from pathlib import Path

from .bus import Bus, MemoryBus, build_bus
from .config import Config, load_config
from .envelope import Event
from .processing.pipeline import Pipeline
from .sim.scenarios import ScenarioSettings
from .sim.world import World
from .storage.lakehouse import Lakehouse
from .validation.rules import Validator


@dataclass
class RunResult:
    scenario: str
    seed: int
    published: int
    stats: dict
    bronze_rows: int
    silver: dict
    gold: dict
    kpis: dict
    alerts: list
    quarantine_sample: list
    replay_match: bool
    sizes: dict
    seconds: float

    def as_dict(self) -> dict:
        return dict(self.__dict__)


def publish_world(world: World, bus: Bus, topics: dict) -> int:
    """Publish in release-time order, so late events really do arrive late."""
    topic_for = {
        "gps.telemetry": topics["gps"],
        "sensor.reading": topics["sensor"],
        "shipment.created": topics["shipment"],
        "shipment.status_changed": topics["shipment"],
    }
    queue: list[tuple[float, int, Event, str]] = []
    for order, (release, event) in enumerate(world.run()):
        topic = topic_for.get(event.event_type, topics["warehouse"])
        heapq.heappush(queue, (release.timestamp(), order, event, topic))

    published = 0
    while queue:
        release_ts, _, event, topic = heapq.heappop(queue)
        # Ingest time is when the pipeline first saw it, not when it happened.
        event.ingest_time = event.event_time.__class__.fromtimestamp(
            release_ts, tz=event.event_time.tzinfo)
        bus.publish(topic, event)
        published += 1
    bus.flush()
    return published


def run_once(scenario: str = "NORMAL_DELIVERY", seed: int | None = None,
             cfg: Config | None = None, backend: str = "memory",
             invalid_rate: float = 0.01, run_dir: str | Path | None = None) -> RunResult:
    started = time.time()
    cfg = cfg or load_config()
    if seed is not None:
        cfg["simulation"]["seed"] = seed
    seed = cfg.get_path("simulation.seed")

    run_dir = Path(run_dir) if run_dir else cfg.lake_root.parent / "runs" / scenario.lower()
    bus_dir = run_dir / "bus"
    lake_dir = run_dir / "lake"
    for path in (bus_dir, lake_dir):
        path.mkdir(parents=True, exist_ok=True)
    for stale in bus_dir.glob("*.jsonl"):
        stale.unlink()

    settings = ScenarioSettings.for_scenario(scenario, invalid_rate=invalid_rate)
    world = World(cfg, settings, seed=seed)
    bus = build_bus(cfg, backend=backend, root=bus_dir)
    topics = cfg.get_path("kafka.topics")

    published = publish_world(world, bus, topics)

    validator = Validator(
        cfg,
        known_vehicles=[v.vehicle_id for v in world.master.vehicles],
        known_sensors=[s.sensor_id for s in world.master.sensors],
        known_shipments=[s.shipment_id for s in world.master.shipments],
    )
    routes = {r.route_id: r for r in world.master.routes}
    pipeline = Pipeline(cfg, bus, validator, route_lookup=routes.get)
    stats = pipeline.run_topics(
        [topics["gps"], topics["sensor"], topics["warehouse"], topics["shipment"]])

    lake = Lakehouse(lake_dir)
    bronze_rows = lake.write_bronze(pipeline.bronze)
    silver = lake.build_silver()
    gold = lake.build_gold()

    from .analytics import kpis as kpi_module
    kpis = {
        "delivery": kpi_module.delivery_kpis(lake),
        "cold_chain": kpi_module.cold_chain_summary(lake),
        "dwell": kpi_module.dwell_summary(lake),
        "carriers": kpi_module.carrier_performance(lake)[:5],
    }

    replay_match = True
    if isinstance(bus, MemoryBus):
        original = bus.count(topics["gps"])
        replayed = sum(1 for _ in bus.replay(topics["gps"]))
        replay_match = original == replayed

    (run_dir / "alerts.json").write_text(json.dumps(pipeline.alerts, indent=2))
    (run_dir / "quarantine.json").write_text(json.dumps(pipeline.quarantined[:50], indent=2))
    (run_dir / "late_events.json").write_text(json.dumps(pipeline.late_events[:50], indent=2))

    result = RunResult(
        scenario=settings.name, seed=seed, published=published, stats=stats.as_dict(),
        bronze_rows=bronze_rows, silver=silver, gold=gold, kpis=kpis,
        alerts=pipeline.alerts[:10], quarantine_sample=pipeline.quarantined[:3],
        replay_match=replay_match, sizes=lake.size_report(),
        seconds=round(time.time() - started, 2),
    )
    (run_dir / "run_report.json").write_text(json.dumps(result.as_dict(), indent=2, default=str))
    return result
