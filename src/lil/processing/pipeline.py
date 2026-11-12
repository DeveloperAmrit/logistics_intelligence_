"""The consumer that ties the stream together.

poll -> deserialize -> validate -> deduplicate -> classify lateness ->
detect -> persist. A record that fails validation goes to quarantine with
its reason and its raw payload. Nothing is dropped silently.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

from ..bus import Bus
from ..envelope import Event, iso, utcnow
from ..validation.rules import Validator, quarantine_record
from .coldchain import ColdChainDetector
from .dedup import Deduplicator
from .deviation import RouteDeviationDetector
from .watermark import LATE_SIDE_OUTPUT, LATE_WITHIN_TOLERANCE, ON_TIME, WatermarkTracker


@dataclass
class PipelineStats:
    consumed: int = 0
    valid: int = 0
    quarantined: int = 0
    duplicates: int = 0
    on_time: int = 0
    late_within_tolerance: int = 0
    late_side_output: int = 0
    cold_chain_alerts: int = 0
    route_deviation_alerts: int = 0
    model_alerts: int = 0
    suppressed_spikes: int = 0
    quarantine_by_reason: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return dict(self.__dict__)


class Pipeline:
    def __init__(self, cfg, bus: Bus, validator: Validator, route_lookup, scorers=()):
        self.cfg = cfg
        self.bus = bus
        self.validator = validator
        self.dedup = Deduplicator(cfg.get_path("streaming.dedup_window_seconds"))
        self.watermarks = WatermarkTracker(
            cfg.get_path("streaming.watermark_seconds"),
            cfg.get_path("streaming.allowed_lateness_seconds"))
        self.cold_chain = ColdChainDetector(cfg)
        self.deviation = RouteDeviationDetector(cfg, route_lookup)
        # Optional model-based detectors (ml/online.py). Same contract as the
        # rule detectors: observe one valid event, maybe return an alert.
        self.scorers = list(scorers)
        self.stats = PipelineStats()
        self.alerts: list[dict] = []
        self.quarantined: list[dict] = []
        self.late_events: list[dict] = []
        self.bronze: list[dict] = []
        self.topics = cfg.get_path("kafka.topics")

    # -------------------------------------------------------------- checkpoint
    def snapshot(self) -> dict:
        """Everything the detectors remember, so a restarted consumer resumes
        with the same state it had at the checkpoint's offsets."""
        return {
            "dedup": (list(self.dedup._seen.items()), self.dedup.duplicates),
            "watermark": (self.watermarks._max_event_time, dict(self.watermarks.counts)),
            "cold_chain": (dict(self.cold_chain.open_episodes), self.cold_chain.suppressed_spikes),
            "deviation": (dict(self.deviation._outside_since), set(self.deviation._alerted),
                          dict(self.deviation._worst)),
            "scorers": [s.snapshot() for s in self.scorers if hasattr(s, "snapshot")],
            "stats": self.stats.as_dict(),
        }

    def restore(self, state: dict) -> None:
        from collections import OrderedDict
        seen, duplicates = state["dedup"]
        self.dedup._seen = OrderedDict(seen)
        self.dedup.duplicates = duplicates
        self.watermarks._max_event_time, self.watermarks.counts = state["watermark"]
        self.cold_chain.open_episodes, self.cold_chain.suppressed_spikes = state["cold_chain"]
        (self.deviation._outside_since, self.deviation._alerted,
         self.deviation._worst) = state["deviation"]
        for scorer, saved in zip([s for s in self.scorers if hasattr(s, "restore")],
                                 state.get("scorers", [])):
            scorer.restore(saved)
        stats = dict(state["stats"])
        self.stats = PipelineStats(**stats)

    def drain(self) -> dict:
        """Hand over what was produced since the last drain (streaming use)."""
        out = {"alerts": self.alerts, "quarantined": self.quarantined,
               "late_events": self.late_events, "bronze": self.bronze}
        self.alerts, self.quarantined, self.late_events, self.bronze = [], [], [], []
        return out

    def _record_quarantine(self, event: Event, result) -> None:
        record = quarantine_record(event, result, iso(event.ingest_time or utcnow()))
        self.quarantined.append(record)
        self.stats.quarantined += 1
        for code in result.reason_codes:
            self.stats.quarantine_by_reason[code] = self.stats.quarantine_by_reason.get(code, 0) + 1
        self.bus.publish(self.topics["dlq"], Event(
            event_type=event.event_type,
            event_time=event.event_time, source="validation",
            shipment_id=event.shipment_id, payload={"quarantine": record}))

    def handle(self, event: Event) -> None:
        self.stats.consumed += 1

        result = self.validator.validate(event)
        if not result.valid:
            self._record_quarantine(event, result)
            return

        if self.dedup.is_duplicate(event.event_id, event.event_time):
            self.stats.duplicates += 1
            return

        report = self.watermarks.observe(event.event_time)
        if report.classification == ON_TIME:
            self.stats.on_time += 1
        elif report.classification == LATE_WITHIN_TOLERANCE:
            self.stats.late_within_tolerance += 1
        else:
            self.stats.late_side_output += 1
            self.late_events.append({"event_id": event.event_id,
                                     "event_time": iso(event.event_time),
                                     "lateness_seconds": report.lateness_seconds})

        self.stats.valid += 1
        self.bronze.append(self._bronze_row(event, report.classification))

        # The trigger's ingest time travels with the alert, so the latency from
        # the moment the reading entered the system to the alert is measurable.
        trigger_ingest = iso(event.ingest_time) if event.ingest_time else None

        alert = self.cold_chain.observe(event)
        if alert is not None:
            self.stats.cold_chain_alerts += 1
            body = {**alert.as_dict(), "trigger_ingest_time": trigger_ingest}
            self.alerts.append({"alert_type": "cold_chain", **body})
            self.bus.publish(self.topics["cold_chain_alert"], Event(
                event_type="shipment.status_changed", event_time=event.event_time,
                source="cold-chain-detector", shipment_id=alert.shipment_id,
                payload=body))

        deviation = self.deviation.observe(event)
        if deviation is not None:
            self.stats.route_deviation_alerts += 1
            body = {**deviation.as_dict(), "trigger_ingest_time": trigger_ingest}
            self.alerts.append({"alert_type": "route_deviation", **body})
            self.bus.publish(self.topics["route_deviation_alert"], Event(
                event_type="shipment.status_changed", event_time=event.event_time,
                source="route-deviation-detector", shipment_id=deviation.shipment_id,
                payload=body))

        for scorer in self.scorers:
            scored = scorer.observe(event)
            if scored is not None:
                self.stats.model_alerts += 1
                body = {**scored, "trigger_ingest_time": trigger_ingest}
                self.alerts.append(body)
                self.bus.publish(self.topics.get(scorer.topic_key, "alerts.model"), Event(
                    event_type="shipment.status_changed", event_time=event.event_time,
                    source=scorer.source, shipment_id=body.get("shipment_id"), payload=body))

    def _bronze_row(self, event: Event, lateness: str) -> dict:
        payload = event.payload or {}
        return {
            "event_id": event.event_id,
            "event_type": event.event_type,
            "event_time": iso(event.event_time),
            "ingest_time": iso(event.ingest_time) if event.ingest_time else None,
            "event_date": iso(event.event_time)[:10],
            "source": event.source,
            "schema_version": event.schema_version,
            "vehicle_id": event.vehicle_id,
            "sensor_id": event.sensor_id,
            "shipment_id": event.shipment_id,
            "warehouse_id": event.warehouse_id,
            "route_id": event.route_id,
            "lateness_class": lateness,
            "latitude": payload.get("latitude"),
            "longitude": payload.get("longitude"),
            "speed_kph": payload.get("speed_kph"),
            "temperature_c": payload.get("temperature_c"),
            "humidity_pct": payload.get("humidity_pct"),
            "battery_pct": payload.get("battery_pct"),
            "temperature_class": payload.get("temperature_class"),
            "old_status": payload.get("old_status"),
            "new_status": payload.get("new_status"),
            "carrier_id": payload.get("carrier_id"),
            "promised_delivery_time": payload.get("promised_delivery_time"),
            "raw_payload": json.dumps(payload, separators=(",", ":")),
        }

    def run_topics(self, topics: Iterable[str], group: str = "processing") -> PipelineStats:
        merged: list[Event] = []
        for topic in topics:
            merged.extend(self.bus.consume(topic, group=group))
        merged.sort(key=lambda e: (e.ingest_time or e.event_time))
        for event in merged:
            self.handle(event)
        self.stats.suppressed_spikes = self.cold_chain.suppressed_spikes
        return self.stats
