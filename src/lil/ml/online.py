"""The delay model inside the stream.

DelayRiskScorer is a detector like the cold-chain rule: the pipeline hands it
every valid event and it may return an alert. It keeps just enough state to
rebuild, at the moment of dispatch, the raw fields the batch path reads from
Gold: when each shipment was created, recent creations per warehouse, and
recent deliveries per carrier. It then calls the same ``features.vector`` the
model was trained with.

``replay_check`` runs the history back through the pipeline with the scorer
switched on and compares, shipment by shipment, the features and predictions
made online with the ones computed in batch. Any difference is training and
serving skew, and the check says which field caused it.
"""
from __future__ import annotations

import bisect
import math
from datetime import timedelta
from pathlib import Path
from typing import Optional

import numpy as np

from ..envelope import iso, parse_iso
from . import features as F


class DelayRiskScorer:
    topic_key = "delay_risk_alert"
    source = "delay-risk-model"
    RULE_ID = "ML-DELAY-01"

    def __init__(self, model: dict, routes: dict, threshold: float = 0.5, version: str = "",
                 carrier_window_days: int = 7, backlog_window_hours: int = 2, record: bool = False):
        self.model = model
        self.routes = routes
        self.threshold = threshold
        self.version = version
        self.window = timedelta(days=carrier_window_days)
        self.backlog = timedelta(hours=backlog_window_hours)
        self.record = record
        self.created: dict[str, object] = {}
        self.origin_created: dict[str, list] = {}
        self.carrier_history: dict[str, list] = {}
        self.recorded: dict[str, dict] = {}
        self.unscored = 0

    # ------------------------------------------------------------ checkpoint
    def snapshot(self) -> dict:
        return {"created": dict(self.created),
                "origin_created": {k: list(v) for k, v in self.origin_created.items()},
                "carrier_history": {k: list(v) for k, v in self.carrier_history.items()},
                "unscored": self.unscored}

    def restore(self, state: dict) -> None:
        self.created = state["created"]
        self.origin_created = state["origin_created"]
        self.carrier_history = state["carrier_history"]
        self.unscored = state["unscored"]

    # -------------------------------------------------------------- observe
    def observe(self, event) -> Optional[dict]:
        if event.event_type not in ("shipment.created", "shipment.status_changed"):
            return None
        payload = event.payload or {}
        route = self.routes.get(event.route_id)
        status = payload.get("new_status")

        if status == "CREATED":
            self.created[event.shipment_id] = event.event_time
            if route is not None:
                bisect.insort(self.origin_created.setdefault(route.origin_warehouse_id, []),
                              event.event_time)
            return None

        if status == "DELIVERED" and payload.get("promised_delivery_time"):
            promised = parse_iso(payload["promised_delivery_time"])
            delay_min = F.delay_minutes(promised, event.event_time)
            bisect.insort(self.carrier_history.setdefault(payload.get("carrier_id"), []),
                          (event.event_time, int(event.event_time <= promised), delay_min))
            self.created.pop(event.shipment_id, None)
            return None

        if status != "DISPATCHED" or route is None:
            return None
        created = self.created.get(event.shipment_id)
        if created is None or not payload.get("promised_delivery_time"):
            self.unscored += 1            # its creation has not arrived: nothing honest to score
            return None

        raw = F.raw_fields(
            route_length_km=round(route.length_m() / 1000.0, 3),
            origin_warehouse_id=route.origin_warehouse_id,
            carrier_id=payload.get("carrier_id"), temperature_class=payload.get("temperature_class"),
            created_at=created, dispatched_at=event.event_time,
            promised_at=parse_iso(payload["promised_delivery_time"]),
            carrier_history=self.carrier_history.get(payload.get("carrier_id"), []),
            origin_created=self.origin_created.get(route.origin_warehouse_id, []),
            carrier_window_days=self.window.days,
            backlog_window_hours=int(self.backlog.total_seconds() // 3600))
        x = np.array([F.vector(raw)])
        probability = float(self.model["classifier"].predict_proba(x)[0, 1])
        predicted = float(self.model["regressor"].predict(x)[0])
        if self.record:
            self.recorded[event.shipment_id] = {"raw": raw, "probability": probability,
                                                "predicted_delay_min": predicted}
        self._prune(event.event_time)
        if probability < self.threshold:
            return None
        return {
            "alert_type": "delay_risk", "alert_id": f"alert-dr-{event.shipment_id}",
            "rule_id": self.RULE_ID, "severity": "WARNING",
            "shipment_id": event.shipment_id, "vehicle_id": event.vehicle_id,
            "route_id": event.route_id, "start_event_time": iso(event.event_time),
            "trigger_event_id": event.event_id, "trigger_event_time": iso(event.event_time),
            "probability": round(probability, 3), "predicted_delay_min": round(predicted, 1),
            "model_version": self.version,
            "explanation": (f"P(late) {probability:.2f} at dispatch, predicted "
                            f"{predicted:+.0f} min against the promise"),
        }

    def _prune(self, now) -> None:
        horizon = now - self.window - timedelta(days=1)
        for history in self.carrier_history.values():
            while history and history[0][0] < horizon:
                history.pop(0)
        cutoff = now - self.backlog - timedelta(hours=1)
        for created in self.origin_created.values():
            while created and created[0] < cutoff:
                created.pop(0)


def replay_check(cfg, history_dir: Path, model: dict, version: str, test_days: list[str]) -> dict:
    """Replay the retained history through the pipeline with the scorer on."""
    import tempfile

    from ..bus import MemoryBus
    from ..processing.pipeline import Pipeline
    from ..runner import validator_for
    from ..sim.history import HistorySettings, HistoryWorld
    from ..storage.lakehouse import Lakehouse
    from . import delay

    # Reference data comes from the generator with the same seed, exactly as
    # the operational store would hold it; no events are taken from it.
    world = HistoryWorld(cfg, HistorySettings.from_config(cfg, days=_days(history_dir)),
                         seed=cfg.get_path("simulation.seed"))
    routes = {r.route_id: r for r in world.master.routes}
    scorer = DelayRiskScorer(model, routes, cfg.get_path("ml.delay_risk_threshold", 0.5), version,
                             cfg.get_path("ml.carrier_window_days", 7),
                             cfg.get_path("ml.backlog_window_hours", 2), record=True)
    retained = MemoryBus(history_dir / "bus")
    with tempfile.TemporaryDirectory() as scratch:
        pipeline = Pipeline(cfg, MemoryBus(scratch), validator_for(cfg, world.master),
                            route_lookup=routes.get, scorers=[scorer])
        topics = cfg.get_path("kafka.topics")
        events = []
        for key in ("gps", "sensor", "warehouse", "shipment"):
            events.extend(retained.replay(topics[key]))
        events.sort(key=lambda e: e.ingest_time or e.event_time)
        for event in events:
            pipeline.handle(event)
        alerts = [a for a in pipeline.alerts if a.get("alert_type") == "delay_risk"]

    lake = Lakehouse(history_dir / "lake")
    data = delay.prepare(lake, cfg)
    batch = {sid: raw for sid, raw in zip(data["ids"], data["raws"])}
    probability_batch = model["classifier"].predict_proba(data["X"])[:, 1]
    batch_p = dict(zip(data["ids"], probability_batch))

    matched, mismatched, field_counts, max_p_gap = 0, 0, {}, 0.0
    for sid, online in scorer.recorded.items():
        if sid not in batch:
            continue
        diffs = [name for name in F.FEATURE_NAMES if not _same(
            F.vector(online["raw"])[F.FEATURE_NAMES.index(name)],
            F.vector(batch[sid])[F.FEATURE_NAMES.index(name)])]
        if diffs:
            mismatched += 1
            for name in diffs:
                field_counts[name] = field_counts.get(name, 0) + 1
        else:
            matched += 1
            max_p_gap = max(max_p_gap, abs(online["probability"] - batch_p[sid]))

    in_test = np.isin(np.array([str(d) for d in data["day"]]), test_days)
    late = dict(zip(data["ids"], data["late"]))
    test_ids = set(data["ids"][in_test])
    flagged = {a["shipment_id"] for a in alerts if a["shipment_id"] in test_ids}
    actual = {sid for sid in test_ids if late[sid] == 1}
    hits = len(flagged & actual)
    return {
        "dispatches_scored_online": len(scorer.recorded), "unscored_creation_missing": scorer.unscored,
        "compared_with_batch": matched + mismatched, "identical_features": matched,
        "different_features": mismatched, "differing_fields": field_counts,
        "max_probability_gap_when_identical": round(max_p_gap, 9),
        "test_window_alerts": len(flagged), "test_window_late": len(actual),
        "alert_precision": round(hits / max(len(flagged), 1), 3),
        "alert_recall": round(hits / max(len(actual), 1), 3),
    }


def _same(a: float, b: float) -> bool:
    if math.isnan(a) and math.isnan(b):
        return True
    return abs(a - b) <= 1e-6


def _days(history_dir: Path) -> int:
    import json
    labels = json.loads((history_dir / "labels.json").read_text())
    return int(labels["days"])
