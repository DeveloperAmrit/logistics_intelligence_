"""The pipeline as a long-running Kafka consumer.

runner.py reads a retained topic and processes it in one pass. This module
puts the same Pipeline behind a real consumer loop: poll, handle each event
as it arrives, publish alerts as they fire, and once a second save a
checkpoint holding the detector state together with the offsets it belongs
to. A restarted consumer restores that state and seeks to those offsets, so
it neither skips events nor counts any event twice in its state. Alerts
emitted between the last checkpoint and a crash are emitted again with the
same alert_id, and the alert table and downstream consumers keep one.

Static group membership (group.instance.id) lets a restarted consumer take
its partitions back at once instead of waiting out the session timeout.

Bronze landing and PostgreSQL writes happen at checkpoint time, off the
per-event path; Bronze is at-least-once and Silver deduplicates on event_id.
"""
from __future__ import annotations

import json
import os
import pickle
import queue
import threading
import time
from array import array
from pathlib import Path
from typing import Optional

from .bus import KafkaBus
from .envelope import Event
from .processing.pipeline import Pipeline
from .runner import validator_for


class Checkpoint:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> Optional[dict]:
        if not self.path.exists():
            return None
        with open(self.path, "rb") as handle:
            return pickle.load(handle)

    def save(self, state: dict) -> None:
        tmp = self.path.with_suffix(".tmp")
        with open(tmp, "wb") as handle:
            pickle.dump(state, handle, protocol=pickle.HIGHEST_PROTOCOL)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, self.path)              # atomic: a crash leaves the old one intact


class BronzeWriter(threading.Thread):
    """Lands micro-batches in Bronze without blocking the consumer loop."""

    def __init__(self, lake_dir: Path):
        super().__init__(daemon=True)
        from .storage.lakehouse import Lakehouse
        self.lake = Lakehouse(lake_dir)
        self.batches: "queue.Queue[Optional[list]]" = queue.Queue()
        self.written = 0
        self._sequence = 0

    def run(self) -> None:
        while True:
            rows = self.batches.get()
            if rows is None:
                return
            self._sequence += 1
            self.written += self.lake.write_bronze(rows, part=f"{os.getpid()}-{self._sequence:06d}")

    def close(self) -> None:
        self.batches.put(None)
        self.join(timeout=30)


def percentiles(values, points=(50, 95, 99)) -> dict:
    import numpy as np
    if len(values) == 0:
        return {f"p{p}": None for p in points} | {"max": None, "count": 0}
    data = np.frombuffer(values, dtype="d") if isinstance(values, array) else np.asarray(values)
    out = {f"p{p}": round(float(np.percentile(data, p)) * 1000, 2) for p in points}
    out["max"] = round(float(data.max()) * 1000, 2)
    out["count"] = int(len(data))
    return out


class StreamService:
    def __init__(self, cfg, master, *, checkpoint_path: str | Path, lake_dir: Optional[Path] = None,
                 postgres: bool = False, scorers=(), instance_id: Optional[str] = None,
                 group_id: Optional[str] = None, status_path: Optional[Path] = None):
        self.cfg = cfg
        self.master = master
        self.topics = cfg.get_path("kafka.topics")
        settings = cfg.get_path("stream", {}) or {}
        self.group_id = group_id or settings.get("group_id", "lil.stream")
        self.instance_id = instance_id or settings.get("instance_id", "lil-stream-1")
        self.session_timeout_ms = int(settings.get("session_timeout_ms", 10000))
        self.checkpoint_every = float(settings.get("checkpoint_seconds", 1.0))
        self.poll_batch = int(settings.get("poll_batch", 500))
        self.poll_timeout = float(settings.get("poll_timeout_ms", 10)) / 1000.0
        self.bus = KafkaBus(cfg.get_path("kafka.bootstrap_servers"))
        routes = {r.route_id: r for r in master.routes}
        self.pipeline = Pipeline(cfg, self.bus, validator_for(cfg, master),
                                 route_lookup=routes.get, scorers=scorers)
        self.checkpoint = Checkpoint(checkpoint_path)
        self.writer = BronzeWriter(lake_dir) if lake_dir else None
        self.store = None
        if postgres:
            from .storage.operational import OperationalStore
            self.store = OperationalStore.from_config(cfg)
            self.store.upsert_reference(master)
            self.store.conn.commit()
        self.status_path = Path(status_path) if status_path else None
        self.positions: dict[str, int] = {}
        self.latency = array("d")
        self.finished = array("d")          # when each event was done, for catch-up timing
        self.processed = 0
        self.alert_ids: set[str] = set()
        self.checkpoint_seconds = array("d")
        self._status_written = 0.0
        self.restored_from: Optional[dict] = None
        self.first_message_at: Optional[float] = None
        self.last_message_at: Optional[float] = None
        self.started_at = time.time()
        self.checkpoints = 0

    # ---------------------------------------------------------- checkpoints
    def _restore(self) -> dict:
        state = self.checkpoint.load()
        if not state:
            return {}
        self.pipeline.restore(state["pipeline"])
        self.restored_from = {"offsets": dict(state["offsets"]),
                              "events_in_state": state["pipeline"]["stats"]["consumed"]}
        return dict(state["offsets"])

    def _save(self, consumer) -> None:
        from confluent_kafka import TopicPartition  # type: ignore
        began = time.perf_counter()
        self.bus.flush()                           # alerts and DLQ records acknowledged first
        produced = self.pipeline.drain()
        self.alert_ids.update(a["alert_id"] for a in produced["alerts"])
        if self.writer and produced["bronze"]:
            self.writer.batches.put(produced["bronze"])
        if self.store is not None:
            status_rows = [r for r in produced["bronze"]
                           if r["event_type"] in ("shipment.created", "shipment.status_changed")]
            self.store.apply_status_events(status_rows, self.master)
            self.store.write_alerts(produced["alerts"])
            self.store.write_quarantine(produced["quarantined"])
            self.store.write_late_events(produced["late_events"])
            self.store.conn.commit()
        self.checkpoint.save({"offsets": dict(self.positions), "pipeline": self.pipeline.snapshot(),
                              "saved_at": time.time()})
        self.checkpoints += 1
        self.checkpoint_seconds.append(time.perf_counter() - began)
        if self.positions:
            consumer.commit(offsets=[TopicPartition(t.rsplit(":", 1)[0], int(t.rsplit(":", 1)[1]), o)
                                     for t, o in self.positions.items()], asynchronous=True)
        self._write_status()

    def _write_status(self, done: bool = False) -> None:
        if not self.status_path:
            return
        tmp = self.status_path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"processed": self.processed, "done": done,
                                   "checkpoints": self.checkpoints,
            "checkpoint_ms": percentiles(self.checkpoint_seconds, (50, 95)),
                                   "pid": os.getpid(), "time": time.time()}))
        os.replace(tmp, self.status_path)
        self._status_written = time.time()

    # ----------------------------------------------------------------- loop
    def run(self, idle_exit_seconds: Optional[float] = None, max_events: Optional[int] = None,
            ready_path: Optional[Path] = None) -> dict:
        from confluent_kafka import Consumer  # type: ignore

        offsets = self._restore()
        consumer = Consumer({
            "bootstrap.servers": self.cfg.get_path("kafka.bootstrap_servers"),
            "group.id": self.group_id,
            "group.instance.id": self.instance_id,
            "session.timeout.ms": self.session_timeout_ms,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "fetch.wait.max.ms": 10,
        })
        assigned = threading.Event()

        def on_assign(client, partitions):
            for partition in partitions:
                key = f"{partition.topic}:{partition.partition}"
                if key in offsets:
                    partition.offset = offsets[key]
            client.assign(partitions)
            assigned.set()

        raw = [self.topics[k] for k in ("gps", "sensor", "warehouse", "shipment")]
        consumer.subscribe(raw, on_assign=on_assign)
        if self.writer:
            self.writer.start()
        last_save = time.time()
        idle_since = time.time()
        ready_written = False
        try:
            while True:
                messages = consumer.consume(num_messages=self.poll_batch, timeout=self.poll_timeout)
                now = time.time()
                if assigned.is_set() and ready_path and not ready_written:
                    Path(ready_path).write_text(str(os.getpid()))
                    ready_written = True
                if not messages:
                    if now - last_save >= self.checkpoint_every and self.processed:
                        self._save(consumer)
                        last_save = now
                    if idle_exit_seconds and self.processed and now - idle_since >= idle_exit_seconds:
                        break
                    continue
                idle_since = now
                for message in messages:
                    if message.error():
                        continue
                    event = Event.from_json(message.value())
                    self.pipeline.handle(event)
                    finished = time.time()
                    if self.first_message_at is None:
                        self.first_message_at = finished
                    self.last_message_at = finished
                    self.positions[f"{message.topic()}:{message.partition()}"] = message.offset() + 1
                    if event.ingest_time is not None:
                        self.latency.append(finished - event.ingest_time.timestamp())
                        self.finished.append(finished)
                    self.processed += 1
                if time.time() - last_save >= self.checkpoint_every:
                    self._save(consumer)
                    last_save = time.time()
                elif self.status_path and time.time() - self._status_written >= 0.1:
                    self._write_status()
                if max_events and self.processed >= max_events:
                    break
            self._save(consumer)
        finally:
            consumer.close()
            if self.writer:
                self.writer.close()
            if self.store is not None:
                self.store.conn.close()
            self._write_status(done=True)
        busy = (self.last_message_at or 0) - (self.first_message_at or 0)
        return {
            "processed": self.processed,
            "stats": self.pipeline.stats.as_dict(),
            "latency_ms": percentiles(self.latency),
            "busy_seconds": round(busy, 3),
            "throughput_eps": round(self.processed / busy, 1) if busy > 0 else None,
            "restored_from": self.restored_from,
            "first_message_at": self.first_message_at,
            "started_at": self.started_at,
            "checkpoints": self.checkpoints,
            "checkpoint_ms": percentiles(self.checkpoint_seconds, (50, 95)),
            "bronze_rows_landed": self.writer.written if self.writer else 0,
            "alert_ids": sorted(self.alert_ids),
        }
