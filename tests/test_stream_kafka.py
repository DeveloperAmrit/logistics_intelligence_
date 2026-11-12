"""The long-running consumer against a real broker.

Opt-in, because it needs Kafka: start the stack (make up) and run
    LIL_KAFKA_TESTS=1 make test
"""
import os

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("LIL_KAFKA_TESTS") != "1",
                                reason="set LIL_KAFKA_TESTS=1 with a broker running")


def test_the_consumer_on_kafka_matches_the_offline_run(cfg, tmp_path):
    pytest.importorskip("confluent_kafka")
    from lil.bench import _cfg_for, _create_topics, _produce
    from lil.runner import _execute, release_order
    from lil.sim.scenarios import ScenarioSettings
    from lil.sim.world import World
    from lil.stream import StreamService

    bootstrap = cfg.get_path("kafka.bootstrap_servers")
    kafka_cfg = _cfg_for(cfg, f"test{os.getpid()}.", bootstrap)
    _create_topics(bootstrap, list(kafka_cfg["kafka"]["topics"].values()), partitions=3)

    world = World(kafka_cfg, ScenarioSettings.benchmark_mix(), seed=5)
    _produce(kafka_cfg, release_order(world, kafka_cfg.get_path("kafka.topics")), rate=None)
    service = StreamService(kafka_cfg, world.master, checkpoint_path=tmp_path / "cp.pkl",
                            group_id=f"test-{os.getpid()}", instance_id=f"test-{os.getpid()}")
    streamed = service.run(idle_exit_seconds=3)

    offline, _, _ = _execute(World(cfg, ScenarioSettings.benchmark_mix(), seed=5), "MIX", 5, cfg,
                             "memory", tmp_path / "offline")
    for key in ("consumed", "valid", "quarantined", "duplicates", "cold_chain_alerts",
                "route_deviation_alerts"):
        assert streamed["stats"][key] == offline.stats[key], key
