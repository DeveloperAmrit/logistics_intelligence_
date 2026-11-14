"""Event-by-event trace of a finished run, for the live view of the dashboard.

The run's own bus files are read back in the order the consumer saw them
(ingest time), and every event goes through a fresh copy of the real
pipeline: the same validator, deduplicator, watermark and detectors. For a
window of the stream we record what each stage decided and why, so the
dashboard can replay it as an animation without inventing anything.

Nothing is written: the pipeline publishes to a bus that discards.
"""
from __future__ import annotations

import json
import zlib
from functools import lru_cache
from pathlib import Path

from ..envelope import Event, iso
from ..processing.pipeline import Pipeline
from ..processing.watermark import LATE_SIDE_OUTPUT, LATE_WITHIN_TOLERANCE

RAW_TOPICS = ("gps", "sensor", "warehouse", "shipment")


class _DiscardBus:
    """Publishing target for the replayed pipeline: alerts and the DLQ go nowhere."""

    def publish(self, topic, event):  # noqa: D401 - interface method
        return None


def _load_events(bus_dir: Path, topics: dict) -> list[tuple[str, int, Event]]:
    events = []
    for name in RAW_TOPICS:
        topic = topics[name]
        path = bus_dir / f"{topic}.jsonl"
        if not path.exists():
            continue
        with path.open() as handle:
            for offset, line in enumerate(handle):
                record = json.loads(line)
                events.append((topic, offset, Event.from_dict(record["value"])))
    events.sort(key=lambda item: (item[2].ingest_time or item[2].event_time))
    return events


def _checks(event: Event, cfg) -> list[str]:
    """The checks the validator applies to this event, phrased for a reader."""
    p = event.payload or {}
    out = ["event_id present", "event_time in UTC",
           f"event_type '{event.event_type}' registered",
           f"schema_version {event.schema_version} supported"]
    if event.event_type == "gps.telemetry":
        lat_r = cfg.get_path("validation.latitude_range")
        lon_r = cfg.get_path("validation.longitude_range")
        out += [f"latitude {p.get('latitude')} in {lat_r}",
                f"longitude {p.get('longitude')} in {lon_r}",
                f"speed {p.get('speed_kph')} km/h ≥ 0",
                f"vehicle {event.vehicle_id} known"]
    elif event.event_type == "sensor.reading":
        out += [f"temperature {p.get('temperature_c')} °C in {cfg.get_path('validation.temperature_range')}",
                f"humidity {p.get('humidity_pct')} % in {cfg.get_path('validation.humidity_range')}",
                f"sensor {event.sensor_id} known"]
    if event.shipment_id:
        out.append(f"shipment {event.shipment_id} known")
    return out


def _payload_view(event: Event) -> dict:
    keep = ("latitude", "longitude", "speed_kph", "temperature_c", "humidity_pct",
            "battery_pct", "temperature_class", "old_status", "new_status", "carrier_id",
            "warehouse_id", "dock", "event")
    p = event.payload or {}
    view = {k: p[k] for k in keep if k in p}
    if not view:
        view = {k: v for k, v in list(p.items())[:6] if not isinstance(v, (dict, list))}
    return view


def trace_run(run_dir: Path, cfg, limit: int = 900, focus: str = "auto") -> dict:
    """Replay the run and return a window of per-event decisions."""
    from ..runner import validator_for
    from ..sim.scenarios import ScenarioSettings
    from ..sim.world import World

    report = json.loads((run_dir / "run_report.json").read_text())
    topics = cfg.get_path("kafka.topics")
    partitions = int(cfg.get_path("kafka.partitions", 6))
    settings = ScenarioSettings.for_scenario(report["scenario"], invalid_rate=0.01)
    world = World(cfg, settings, seed=report["seed"])
    routes = {r.route_id: r for r in world.master.routes}
    pipeline = Pipeline(cfg, _DiscardBus(), validator_for(cfg, world.master),
                        route_lookup=routes.get)
    corridor = cfg.get_path("route_deviation.corridor_metres")
    max_c = cfg.get_path("cold_chain.max_celsius")

    events = _load_events(run_dir / "bus", topics)
    records = []
    for index, (topic, offset, event) in enumerate(events):
        stats = pipeline.stats
        before = (stats.quarantined, stats.duplicates, stats.late_within_tolerance,
                  stats.late_side_output, len(pipeline.alerts), pipeline.cold_chain.suppressed_spikes)
        watermark = pipeline.watermarks.watermark
        pipeline.handle(event)
        after = (stats.quarantined, stats.duplicates, stats.late_within_tolerance,
                 stats.late_side_output, len(pipeline.alerts), pipeline.cold_chain.suppressed_spikes)

        if after[0] > before[0]:
            outcome = "quarantined"
        elif after[1] > before[1]:
            outcome = "duplicate"
        elif after[3] > before[3]:
            outcome = "late_side_output"
        elif after[2] > before[2]:
            outcome = "late_tolerated"
        else:
            outcome = "on_time"

        rec = {"i": index, "topic": topic, "offset": offset,
               "key": event.key(), "partition": zlib.crc32(event.key().encode()) % partitions,
               "outcome": outcome, "event_type": event.event_type,
               "event_id": event.event_id, "event_time": iso(event.event_time),
               "ingest_time": iso(event.ingest_time) if event.ingest_time else None,
               "watermark": iso(watermark) if watermark else None,
               "lateness_s": round((watermark - event.event_time).total_seconds(), 1)
               if watermark else 0.0,
               "source": event.source, "schema_version": event.schema_version,
               "vehicle_id": event.vehicle_id, "sensor_id": event.sensor_id,
               "shipment_id": event.shipment_id, "warehouse_id": event.warehouse_id,
               "payload": _payload_view(event)}
        if outcome == "quarantined":
            rec["reasons"] = pipeline.quarantined[-1].get("reasons", [])
        if after[4] > before[4]:
            rec["alert"] = pipeline.alerts[-1]
        if after[5] > before[5]:
            rec["note"] = "previous breach did not persist: spike suppressed, no alert"
        p = event.payload or {}
        if (event.event_type == "sensor.reading" and p.get("temperature_class") == "COLD_2_8"
                and p.get("temperature_c") is not None and outcome != "quarantined"):
            rec["breach"] = p["temperature_c"] > max_c
            episode = pipeline.cold_chain.open_episodes.get(event.shipment_id or event.sensor_id or "")
            rec["breach_readings"] = episode.readings if episode else 0
        if event.event_type == "gps.telemetry" and event.route_id in routes and outcome != "quarantined":
            lat, lon = p.get("latitude"), p.get("longitude")
            if lat is not None and lon is not None:
                rec["corridor_m"] = round(routes[event.route_id].distance_from_corridor_m((lat, lon)), 1)
                rec["outside"] = rec["corridor_m"] > corridor
        rec["checks"] = _checks(event, cfg)
        records.append(rec)

    # Pick the window: the first alert if there is one, else the first event
    # that shows the scenario's fault, else ten minutes in.
    anchor = None
    order = {"alert": lambda r: "alert" in r, "late": lambda r: r["outcome"] == "late_side_output",
             "duplicate": lambda r: r["outcome"] == "duplicate",
             "tolerated": lambda r: r["outcome"] == "late_tolerated"}
    wanted = list(order) if focus == "auto" else [focus] if focus in order else []
    for name in wanted:
        anchor = next((r["i"] for r in records if order[name](r)), None)
        if anchor is not None:
            break
    if anchor is None:
        anchor = min(len(records) - 1, len(records) // 6) if records else 0
    start = max(0, anchor - int(limit * 0.7))
    window = records[start:start + limit]

    return {"scenario": report["scenario"], "seed": report["seed"], "total_events": len(records),
            "window_start": start, "window": window,
            "rules": {"watermark_s": cfg.get_path("streaming.watermark_seconds"),
                      "allowed_lateness_s": cfg.get_path("streaming.allowed_lateness_seconds"),
                      "dedup_window_s": cfg.get_path("streaming.dedup_window_seconds"),
                      "max_celsius": max_c,
                      "consecutive": cfg.get_path("cold_chain.consecutive_readings"),
                      "corridor_m": corridor,
                      "persistence_s": cfg.get_path("route_deviation.persistence_seconds"),
                      "partitions": partitions}}


@lru_cache(maxsize=8)
def cached_trace(run_dir: str, mtime: float, limit: int, focus: str) -> dict:
    from ..config import load_config
    return trace_run(Path(run_dir), load_config(), limit=limit, focus=focus)
