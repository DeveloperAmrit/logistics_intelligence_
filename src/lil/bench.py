"""Measure the streaming path on a real Kafka broker.

    python -m lil bench --bootstrap localhost:9092        (make bench)

Every measurement gets fresh topics (prefix bench<id>.) and a fresh consumer
group, so nothing from an earlier run leaks in. The consumer runs in its own
process, as it would when deployed, so producer and consumer do not share an
interpreter lock. The workload is the BENCHMARK_MIX hour (every fault at
once, 40k+ events, seed 42).

  capacity  the topics are filled first, then one consumer drains them; the
            drain rate is what one consumer process sustains.
  latency   the producer replays the hour at a fixed rate and stamps each
            event as it sends it. The consumer records, for every event, the
            time from that stamp to the end of its processing; a separate
            consumer on the alert topics records stamp-to-alert for every
            alert.
  recovery  the consumer is killed with SIGKILL mid-stream and restarted on
            the same checkpoint. Reported: the gap, the time to catch up,
            alerts emitted twice, and whether the final counts and the alert
            set equal those of the offline run of the same hour.
"""
from __future__ import annotations

import copy
import json
import multiprocessing as mp
import os
import signal
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np

from .config import Config, load_config
from .runner import release_order
from .sim.scenarios import ScenarioSettings
from .sim.world import World

ORDER_FREE = ("consumed", "valid", "quarantined", "duplicates", "cold_chain_alerts",
              "route_deviation_alerts")


def _cfg_for(cfg: Config, prefix: str, bootstrap: str) -> Config:
    copy_ = Config(copy.deepcopy(dict(cfg)))
    copy_["kafka"]["bootstrap_servers"] = bootstrap
    copy_["kafka"]["topics"] = {k: f"{prefix}{v}" for k, v in cfg["kafka"]["topics"].items()}
    return copy_


def _create_topics(bootstrap: str, topics: list[str], partitions: int) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.create_topics([NewTopic(t, num_partitions=partitions, replication_factor=1,
                                            config={"retention.ms": str(7 * 24 * 3600 * 1000)})
                                   for t in topics])
    for future in futures.values():
        future.result(timeout=30)


def _world_events(cfg: Config, seed: int):
    world = World(cfg, ScenarioSettings.benchmark_mix(), seed=seed)
    return world, release_order(world, cfg.get_path("kafka.topics"))


# ------------------------------------------------------------------ consumer
def _consumer_main(cfg_dict: dict, seed: int, workdir: str, tag: str,
                   idle_exit: float, lake: bool) -> None:
    from .stream import StreamService
    cfg = Config(cfg_dict)
    world = World(cfg, ScenarioSettings.benchmark_mix(), seed=seed)   # master data only
    work = Path(workdir)
    service = StreamService(cfg, world.master, checkpoint_path=work / "checkpoint.pkl",
                            lake_dir=(work / "lake") if lake else None,
                            group_id=cfg_dict["_bench_group"], instance_id="bench-consumer-1",
                            status_path=work / f"status-{tag}.json")
    result = service.run(idle_exit_seconds=idle_exit, ready_path=work / f"ready-{tag}")
    np.save(work / f"latency-{tag}.npy", np.frombuffer(service.latency, dtype="d"))
    np.save(work / f"finished-{tag}.npy", np.frombuffer(service.finished, dtype="d"))
    (work / f"result-{tag}.json").write_text(json.dumps(result, default=str))


def _start_consumer(cfg: Config, seed: int, work: Path, tag: str, idle_exit: float = 3.0,
                    lake: bool = False, wait_ready: bool = True):
    context = mp.get_context("spawn")
    process = context.Process(target=_consumer_main,
                              args=(dict(cfg), seed, str(work), tag, idle_exit, lake), daemon=False)
    process.start()
    if wait_ready:
        deadline = time.time() + 60
        while not (work / f"ready-{tag}").exists():
            if time.time() > deadline or not process.is_alive():
                raise RuntimeError(f"consumer {tag} did not become ready")
            time.sleep(0.05)
    return process


# --------------------------------------------------------------- alert tap
class AlertTap(threading.Thread):
    """A downstream consumer of the alert topics: what an operator would see."""

    def __init__(self, cfg: Config):
        super().__init__(daemon=True)
        self.cfg = cfg
        self.records: list[tuple[str, str, float]] = []
        self.ready = threading.Event()
        self.stop = threading.Event()

    def run(self) -> None:
        from confluent_kafka import Consumer  # type: ignore
        topics = self.cfg.get_path("kafka.topics")
        consumer = Consumer({"bootstrap.servers": self.cfg.get_path("kafka.bootstrap_servers"),
                             "group.id": f"tap-{uuid.uuid4().hex[:8]}",
                             "auto.offset.reset": "earliest", "fetch.wait.max.ms": 10})
        consumer.subscribe([topics["cold_chain_alert"], topics["route_deviation_alert"]],
                           on_assign=lambda c, p: (c.assign(p), self.ready.set()))
        try:
            while not self.stop.is_set():
                message = consumer.poll(0.05)
                if message is None or message.error():
                    continue
                seen = time.time()
                payload = json.loads(message.value())["payload"]
                stamp = payload.get("trigger_ingest_time")
                if stamp:
                    lag = seen - datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp()
                    self.records.append((payload["alert_id"], payload.get("rule_id", ""), lag))
        finally:
            consumer.close()


# ---------------------------------------------------------------- producer
def _produce(cfg: Config, events, rate: Optional[float]) -> dict:
    from confluent_kafka import Producer  # type: ignore
    producer = Producer({"bootstrap.servers": cfg.get_path("kafka.bootstrap_servers"),
                         "enable.idempotence": True, "acks": "all", "linger.ms": 5})
    started = time.perf_counter()
    wall_start = time.time()
    for index, (topic, event) in enumerate(events):
        if rate:
            delay = started + index / rate - time.perf_counter()
            if delay > 0.0002:
                time.sleep(delay)
        event.ingest_time = datetime.fromtimestamp(time.time(), tz=timezone.utc)
        producer.produce(topic, key=event.key().encode(), value=event.to_json().encode())
        if index % 200 == 0:
            producer.poll(0)
    producer.flush(30)
    seconds = time.perf_counter() - started
    return {"sent": len(events), "seconds": round(seconds, 3),
            "achieved_rate": round(len(events) / seconds, 1), "wall_start": wall_start}


def _summary(values) -> dict:
    data = np.asarray(values, dtype=float)
    if data.size == 0:
        return {"count": 0}
    return {"count": int(data.size), "p50": round(float(np.percentile(data, 50)) * 1000, 1),
            "p95": round(float(np.percentile(data, 95)) * 1000, 1),
            "p99": round(float(np.percentile(data, 99)) * 1000, 1),
            "max": round(float(data.max()) * 1000, 1)}


def _fresh(cfg: Config, bootstrap: str, root: Path, name: str, partitions: int):
    run_id = f"{name}{uuid.uuid4().hex[:6]}"
    bench_cfg = _cfg_for(cfg, f"bench{run_id}.", bootstrap)
    bench_cfg["_bench_group"] = f"bench-{run_id}"
    _create_topics(bootstrap, list(bench_cfg["kafka"]["topics"].values()), partitions)
    work = root / run_id
    work.mkdir(parents=True, exist_ok=True)
    return bench_cfg, work


# -------------------------------------------------------------- measurements
def measure_capacity(cfg, bootstrap, root, seed, partitions, repeats: int = 3) -> dict:
    """Fill fresh topics, then time one consumer draining them; median of repeats."""
    producer_rates, consumer_rates = [], []
    for _ in range(repeats):
        bench_cfg, work = _fresh(cfg, bootstrap, root, "cap", partitions)
        _, events = _world_events(bench_cfg, seed)
        produced = _produce(bench_cfg, events, rate=None)
        process = _start_consumer(bench_cfg, seed, work, "c", wait_ready=False)
        process.join(300)
        result = json.loads((work / "result-c.json").read_text())
        producer_rates.append(produced["achieved_rate"])
        consumer_rates.append(result["throughput_eps"])
    return {"events": len(events), "repeats": repeats,
            "producer_rate": round(float(np.median(producer_rates)), 1),
            "consumer_rate": round(float(np.median(consumer_rates)), 1),
            "consumer_rates": consumer_rates, "processed": result["processed"]}


def measure_latency(cfg, bootstrap, root, seed, partitions, rate) -> dict:
    bench_cfg, work = _fresh(cfg, bootstrap, root, f"lat{int(rate)}", partitions)
    _, events = _world_events(bench_cfg, seed)
    process = _start_consumer(bench_cfg, seed, work, "c")
    tap = AlertTap(bench_cfg)
    tap.start()
    tap.ready.wait(30)
    produced = _produce(bench_cfg, events, rate=rate)
    process.join(300)
    time.sleep(1.0)
    tap.stop.set()
    tap.join(10)
    result = json.loads((work / "result-c.json").read_text())
    latency = np.load(work / "latency-c.npy")
    return {"rate": rate, "achieved_rate": produced["achieved_rate"], "events": len(events),
            "event_latency_ms": _summary(latency),
            "alert_latency_ms": _summary([r[2] for r in tap.records]),
            "alerts_seen": len(tap.records),
            "checkpoint_ms": result.get("checkpoint_ms"),
            "stats": {k: result["stats"][k] for k in ORDER_FREE + ("late_within_tolerance",
                                                                   "late_side_output")},
            "consumer_rate": result["throughput_eps"]}


def measure_recovery(cfg, bootstrap, root, seed, partitions, rate, kill_after) -> dict:
    bench_cfg, work = _fresh(cfg, bootstrap, root, "rec", partitions)
    world, events = _world_events(bench_cfg, seed)
    first = _start_consumer(bench_cfg, seed, work, "a")
    tap = AlertTap(bench_cfg)
    tap.start()
    tap.ready.wait(30)
    producer_result: dict = {}
    producing = threading.Thread(target=lambda: producer_result.update(
        _produce(bench_cfg, events, rate=rate)))
    producing.start()
    time.sleep(kill_after)
    status_before = json.loads((work / "status-a.json").read_text())
    os.kill(first.pid, signal.SIGKILL)
    killed_at = time.time()
    first.join()
    second = _start_consumer(bench_cfg, seed, work, "b", wait_ready=False)
    producing.join()
    second.join(300)
    time.sleep(1.0)
    tap.stop.set()
    tap.join(10)

    result = json.loads((work / "result-b.json").read_text())
    finished = np.load(work / "finished-b.npy")
    latency = np.load(work / "latency-b.npy")
    caught_up = None
    if finished.size:
        behind = np.nonzero(latency > 0.25)[0]
        last_behind = int(behind.max()) if behind.size else -1
        if last_behind + 1 < finished.size:
            caught_up = round(float(finished[last_behind + 1] - killed_at), 3)
    ids = [r[0] for r in tap.records]
    restored_events = result["restored_from"]["events_in_state"] if result["restored_from"] else 0
    return {
        "rate": rate, "killed_after_seconds": kill_after,
        "processed_before_kill_at_least": status_before["processed"],
        "events_in_restored_state": restored_events,
        "processed_after_restart": result["processed"],
        "reprocessed_events": status_before["processed"] - restored_events
        if status_before["processed"] >= restored_events else None,
        "gap_seconds": round(float(result["first_message_at"] - killed_at), 3)
        if result["first_message_at"] else None,
        "caught_up_seconds_after_kill": caught_up,
        "alerts_received": len(ids), "distinct_alerts": len(set(ids)),
        "duplicate_alert_messages": len(ids) - len(set(ids)),
        "final_counts": {k: result["stats"][k] for k in ORDER_FREE},
        "alert_ids": sorted(set(ids)),
        "total_events": len(events),
    }


def offline_reference(cfg: Config, seed: int) -> dict:
    """The same hour through the batch runner, as the correctness reference."""
    import tempfile
    from .runner import _execute
    with tempfile.TemporaryDirectory() as scratch:
        world = World(cfg, ScenarioSettings.benchmark_mix(), seed=seed)
        result, pipeline, _ = _execute(world, "BENCHMARK_MIX", seed, cfg, "memory", Path(scratch))
    return {"counts": {k: result.stats[k] for k in ORDER_FREE},
            "alert_ids": sorted({a["alert_id"] for a in pipeline.alerts}),
            "published": result.published}


def run_benchmark(bootstrap: str, rates=(1000, 2000, 4000, 8000), seed: int = 42,
                  partitions: int = 6, out: Optional[Path] = None, recovery_rate: float = 2000,
                  kill_after: float = 6.0, skip: tuple = ()) -> dict:
    cfg = load_config()
    root = Path(out) if out else cfg.lake_root.parent / "bench"
    root.mkdir(parents=True, exist_ok=True)
    # Measurements can be taken in several sittings (make bench RATES=... SKIP=...);
    # each one updates the same report, latency rows are kept per rate.
    existing = root / "bench_report.json"
    report: dict = json.loads(existing.read_text()) if existing.exists() else {}
    report.update({"bootstrap": bootstrap, "seed": seed, "partitions": partitions,
                   "workload": "BENCHMARK_MIX", "cpu_count": os.cpu_count(),
                   "updated": datetime.now(timezone.utc).isoformat(timespec="seconds")})
    reference = offline_reference(load_config(), seed)
    report["offline_reference"] = {"counts": reference["counts"], "alerts": len(reference["alert_ids"]),
                                   "published": reference["published"]}
    if "capacity" not in skip:
        report["capacity"] = measure_capacity(cfg, bootstrap, root, seed, partitions)
    if "latency" not in skip:
        rows = {row["rate"]: row for row in report.get("latency", [])}
        for rate in rates:
            row = measure_latency(cfg, bootstrap, root, seed, partitions, rate)
            row["counts_match_offline"] = all(row["stats"][k] == reference["counts"][k]
                                              for k in ORDER_FREE)
            rows[rate] = row
        report["latency"] = [rows[r] for r in sorted(rows)]
    if "recovery" not in skip:
        recovery = measure_recovery(cfg, bootstrap, root, seed, partitions, recovery_rate, kill_after)
        recovery["counts_match_offline"] = recovery["final_counts"] == reference["counts"]
        recovery["alert_set_matches_offline"] = recovery.pop("alert_ids") == reference["alert_ids"]
        report["recovery"] = recovery
    (root / "bench_report.json").write_text(json.dumps(report, indent=2, default=str))
    return report
