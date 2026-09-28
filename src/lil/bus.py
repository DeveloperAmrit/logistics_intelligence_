"""The event bus.

Two backends behind one interface:

  KafkaBus   - real Apache Kafka, used with the Docker stack.
  MemoryBus  - append-only JSONL files on disk, used by the offline demo
               and by the tests. It keeps the same semantics that matter:
               per-key ordering, retained records, replay from offset zero,
               and independent consumer groups.

Producers publish to this, never to the database or the dashboard.
"""
from __future__ import annotations

import json
import threading
from collections import defaultdict
from pathlib import Path
from typing import Callable, Iterable, Iterator, Optional

from .envelope import Event


class Bus:
    def publish(self, topic: str, event: Event) -> None:
        raise NotImplementedError

    def consume(self, topic: str, group: str = "default", from_offset: int = 0) -> Iterator[Event]:
        raise NotImplementedError

    def count(self, topic: str) -> int:
        raise NotImplementedError

    def topics(self) -> list[str]:
        raise NotImplementedError

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


class MemoryBus(Bus):
    """File-backed log. One JSONL file per topic, offsets are line numbers."""

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._offsets: dict[tuple[str, str], int] = defaultdict(int)
        self._lock = threading.Lock()

    def _path(self, topic: str) -> Path:
        return self.root / f"{topic}.jsonl"

    def publish(self, topic: str, event: Event) -> None:
        if event.ingest_time is None:
            event.stamp_ingest()
        line = json.dumps({"key": event.key(), "value": event.to_dict()}, separators=(",", ":"))
        with self._lock, open(self._path(topic), "a", encoding="utf-8") as handle:
            handle.write(line + "\n")

    def consume(self, topic: str, group: str = "default", from_offset: int = 0) -> Iterator[Event]:
        path = self._path(topic)
        if not path.exists():
            return iter(())
        start = max(from_offset, self._offsets[(topic, group)])

        def _iter() -> Iterator[Event]:
            with open(path, "r", encoding="utf-8") as handle:
                for offset, line in enumerate(handle):
                    if offset < start:
                        continue
                    record = json.loads(line)
                    self._offsets[(topic, group)] = offset + 1
                    yield Event.from_dict(record["value"])

        return _iter()

    def replay(self, topic: str) -> Iterator[Event]:
        """Read the whole retained topic again, ignoring committed offsets."""
        path = self._path(topic)
        if not path.exists():
            return iter(())

        def _iter() -> Iterator[Event]:
            with open(path, "r", encoding="utf-8") as handle:
                for line in handle:
                    yield Event.from_dict(json.loads(line)["value"])

        return _iter()

    def count(self, topic: str) -> int:
        path = self._path(topic)
        if not path.exists():
            return 0
        with open(path, "r", encoding="utf-8") as handle:
            return sum(1 for _ in handle)

    def topics(self) -> list[str]:
        return sorted(p.stem for p in self.root.glob("*.jsonl"))


class KafkaBus(Bus):
    """Apache Kafka backend. Requires confluent-kafka and a running broker."""

    def __init__(self, bootstrap_servers: str, group_id: str = "lil"):
        try:
            from confluent_kafka import Consumer, Producer  # type: ignore
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "confluent-kafka is not installed. Run 'pip install confluent-kafka' "
                "or use the offline demo (make demo), which needs no broker."
            ) from exc
        self._Consumer = Consumer
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
        })
        self._consumers: dict[tuple[str, str], object] = {}

    def publish(self, topic: str, event: Event) -> None:
        if event.ingest_time is None:
            event.stamp_ingest()
        self._producer.produce(topic, key=event.key().encode(), value=event.to_json().encode())
        self._producer.poll(0)

    def consume(self, topic: str, group: str = "default", from_offset: int = 0) -> Iterator[Event]:
        consumer = self._Consumer({
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": f"{self.group_id}.{group}",
            "auto.offset.reset": "earliest" if from_offset == 0 else "latest",
            "enable.auto.commit": True,
        })
        consumer.subscribe([topic])
        try:
            while True:
                message = consumer.poll(1.0)
                if message is None:
                    break
                if message.error():
                    continue
                yield Event.from_json(message.value().decode())
        finally:
            consumer.close()

    def count(self, topic: str) -> int:  # pragma: no cover - needs a broker
        return sum(1 for _ in self.consume(topic, group="count"))

    def topics(self) -> list[str]:  # pragma: no cover - needs a broker
        return []

    def flush(self) -> None:
        self._producer.flush()

    def close(self) -> None:
        self.flush()


def build_bus(cfg, backend: str = "memory", root: Optional[str | Path] = None) -> Bus:
    if backend == "kafka":
        return KafkaBus(cfg.get_path("kafka.bootstrap_servers"))
    return MemoryBus(root or (cfg.lake_root.parent / "bus"))
