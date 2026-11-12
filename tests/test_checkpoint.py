"""A consumer restored from a checkpoint ends in the same state as one that
never stopped, and raises the same alerts with the same ids."""
import pickle

from lil.bus import MemoryBus
from lil.processing.pipeline import Pipeline
from lil.runner import release_order, validator_for
from lil.sim.scenarios import ScenarioSettings
from lil.sim.world import World


def _pipeline(cfg, world, tmp_path, name):
    routes = {r.route_id: r for r in world.master.routes}
    return Pipeline(cfg, MemoryBus(tmp_path / name), validator_for(cfg, world.master),
                    route_lookup=routes.get)


def test_restore_then_continue_equals_an_uninterrupted_run(cfg, tmp_path):
    world = World(cfg, ScenarioSettings.benchmark_mix(), seed=11)
    events = [event for _, event in release_order(world, cfg.get_path("kafka.topics"))]
    cut = len(events) // 2

    whole = _pipeline(cfg, world, tmp_path, "whole")
    for event in events:
        whole.handle(event)

    first = _pipeline(cfg, world, tmp_path, "first")
    for event in events[:cut]:
        first.handle(event)
    saved = pickle.loads(pickle.dumps(first.snapshot()))
    for event in events[cut:cut + 500]:          # work done after the checkpoint, then a crash
        first.handle(event)

    second = _pipeline(cfg, world, tmp_path, "second")
    second.restore(saved)
    for event in events[cut:]:
        second.handle(event)

    assert second.stats.as_dict() == whole.stats.as_dict()
    before_cut = {a["alert_id"] for a in first.alerts}
    after_restore = {a["alert_id"] for a in second.alerts}
    assert before_cut | after_restore == {a["alert_id"] for a in whole.alerts}
    assert whole.stats.cold_chain_alerts > 0 and whole.stats.route_deviation_alerts > 0
