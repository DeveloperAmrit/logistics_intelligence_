"""The PostgreSQL writer. Skipped when no database is reachable (make up)."""
import uuid

import pytest

from lil.runner import _execute
from lil.sim.scenarios import ScenarioSettings
from lil.sim.world import World


@pytest.fixture
def store(cfg):
    from lil.storage.operational import OperationalStore
    schema = f"lil_test_{uuid.uuid4().hex[:8]}"
    try:
        store = OperationalStore.from_config(cfg, schema=schema)
    except Exception as exc:  # no database: an optional integration test
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    store.migrate()
    yield store
    store.conn.rollback()
    with store.conn.cursor() as cur:
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
    store.conn.commit()
    store.conn.close()


def _run(cfg, tmp_path, scenario):
    world = World(cfg, ScenarioSettings.for_scenario(scenario, invalid_rate=0.01), seed=42)
    _, pipeline, _ = _execute(world, scenario, 42, cfg, "memory", tmp_path / scenario.lower())
    return world, pipeline


def test_writing_the_same_run_twice_adds_nothing(cfg, tmp_path, store):
    world, pipeline = _run(cfg, tmp_path, "COLD_CHAIN_EXCURSION")
    first = store.write_run(world, pipeline)
    second = store.write_run(world, pipeline)
    assert first["inserted"]["status_history"] > 0 and first["inserted"]["alerts"] > 0
    assert set(second["inserted"].values()) == {0}
    assert second["totals"] == first["totals"]


def test_late_events_are_recorded_and_status_only_moves_forward(cfg, tmp_path, store):
    world, pipeline = _run(cfg, tmp_path, "LATE_EVENT")
    result = store.write_run(world, pipeline)
    assert result["inserted"]["late_events"] > 0
    with store.conn.cursor() as cur:
        cur.execute("""
            SELECT count(*) FROM shipment s
            JOIN LATERAL (SELECT max(event_time) AS latest FROM shipment_status_history h
                          WHERE h.shipment_id = s.shipment_id) h ON true
            WHERE s.status_event_time <> h.latest""")
        assert cur.fetchone()[0] == 0
