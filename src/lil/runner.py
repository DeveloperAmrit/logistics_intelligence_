"""One end-to-end run: simulate, publish, process, land, build, report.

This is what 'make demo' executes. The same components run against Kafka and
PostgreSQL when the Docker stack is up; only the bus backend changes.
'make history' runs the same path over weeks of simulated operations, which
is what the models in ml/ are trained on.
"""
from __future__ import annotations

import heapq
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

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
    operational: Optional[dict] = None

    def as_dict(self) -> dict:
        return dict(self.__dict__)


def release_order(world, topics: dict) -> list[tuple[str, Event]]:
    """Every event the world emits, with its topic, in release-time order, so
    late events really do arrive late. Ingest time is set to the release time:
    when the pipeline first saw the event, not when it happened."""
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

    ordered = []
    while queue:
        release_ts, _, event, topic = heapq.heappop(queue)
        event.ingest_time = event.event_time.__class__.fromtimestamp(
            release_ts, tz=event.event_time.tzinfo)
        ordered.append((topic, event))
    return ordered


def publish_world(world, bus: Bus, topics: dict) -> int:
    published = 0
    for topic, event in release_order(world, topics):
        bus.publish(topic, event)
        published += 1
    bus.flush()
    return published


def validator_for(cfg: Config, master) -> Validator:
    return Validator(
        cfg,
        known_vehicles=[v.vehicle_id for v in master.vehicles],
        known_sensors=[s.sensor_id for s in master.sensors],
        known_shipments=[s.shipment_id for s in master.shipments],
    )


def _execute(world, name: str, seed: int, cfg: Config, backend: str, run_dir: Path,
             postgres: bool = False, scorers=()) -> tuple[RunResult, Pipeline, Lakehouse]:
    started = time.time()
    bus_dir = run_dir / "bus"
    lake_dir = run_dir / "lake"
    for path in (bus_dir, lake_dir):
        path.mkdir(parents=True, exist_ok=True)
    for stale in bus_dir.glob("*.jsonl"):
        stale.unlink()

    bus = build_bus(cfg, backend=backend, root=bus_dir)
    topics = cfg.get_path("kafka.topics")
    published = publish_world(world, bus, topics)

    routes = {r.route_id: r for r in world.master.routes}
    pipeline = Pipeline(cfg, bus, validator_for(cfg, world.master), route_lookup=routes.get,
                        scorers=scorers)
    stats = pipeline.run_topics(
        [topics["gps"], topics["sensor"], topics["warehouse"], topics["shipment"]])

    lake = Lakehouse(lake_dir)
    lake.write_reference(world.master)
    bronze_rows = lake.write_bronze(pipeline.bronze)
    lake.write_alerts(pipeline.alerts)
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

    operational = None
    if postgres:
        from .storage.operational import OperationalStore
        with OperationalStore.from_config(cfg) as store:
            operational = store.write_run(world, pipeline, scenario=name, seed=seed)

    (run_dir / "alerts.json").write_text(json.dumps(pipeline.alerts, indent=2))
    (run_dir / "quarantine.json").write_text(json.dumps(pipeline.quarantined[:50], indent=2))
    (run_dir / "late_events.json").write_text(json.dumps(pipeline.late_events[:50], indent=2))

    result = RunResult(
        scenario=name, seed=seed, published=published, stats=stats.as_dict(),
        bronze_rows=bronze_rows, silver=silver, gold=gold, kpis=kpis,
        alerts=pipeline.alerts[:10], quarantine_sample=pipeline.quarantined[:3],
        replay_match=replay_match, sizes=lake.size_report(),
        seconds=round(time.time() - started, 2), operational=operational,
    )
    (run_dir / "run_report.json").write_text(json.dumps(result.as_dict(), indent=2, default=str))
    return result, pipeline, lake


def run_once(scenario: str = "NORMAL_DELIVERY", seed: int | None = None,
             cfg: Config | None = None, backend: str = "memory",
             invalid_rate: float = 0.01, run_dir: str | Path | None = None,
             postgres: bool = False) -> RunResult:
    cfg = cfg or load_config()
    if seed is not None:
        cfg["simulation"]["seed"] = seed
    seed = cfg.get_path("simulation.seed")
    run_dir = Path(run_dir) if run_dir else cfg.lake_root.parent / "runs" / scenario.lower()

    settings = ScenarioSettings.for_scenario(scenario, invalid_rate=invalid_rate)
    world = World(cfg, settings, seed=seed)
    result, _, _ = _execute(world, settings.name, seed, cfg, backend, run_dir, postgres=postgres)
    return result


def run_history(days: int | None = None, seed: int | None = None, cfg: Config | None = None,
                run_dir: str | Path | None = None, postgres: bool = False) -> RunResult:
    """Weeks of operations through the unchanged pipeline, for the models."""
    from .sim.history import HistorySettings, HistoryWorld

    cfg = cfg or load_config()
    if seed is not None:
        cfg["simulation"]["seed"] = seed
    seed = cfg.get_path("simulation.seed")
    run_dir = Path(run_dir) if run_dir else cfg.lake_root.parent / "history"
    run_dir.mkdir(parents=True, exist_ok=True)

    settings = HistorySettings.from_config(cfg, days=days)
    world = HistoryWorld(cfg, settings, seed=seed)
    result, _, _ = _execute(world, f"HISTORY_{settings.days}D", seed, cfg, "memory", run_dir,
                            postgres=postgres)
    labels = dict(world.labels)
    labels["generator_stats"] = world.stats
    labels["start_date"] = settings.start_date.isoformat()
    labels["days"] = settings.days
    (run_dir / "labels.json").write_text(json.dumps(labels, indent=1))
    return result
