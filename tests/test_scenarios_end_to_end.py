"""Each named scenario is an integration test with an expected outcome."""
import pytest

from lil.runner import run_once
from lil.sim.scenarios import (COLD_CHAIN_EXCURSION, DUPLICATE_EVENT, GPS_DROPOUT, LATE_EVENT,
                               NORMAL_DELIVERY, ROUTE_DEVIATION, WAREHOUSE_DWELL_DELAY)


@pytest.fixture(scope="module")
def runs(tmp_path_factory):
    from lil.config import load_config
    cache = {}

    def _run(scenario):
        if scenario not in cache:
            cfg = load_config()
            cfg["simulation"]["duration_seconds"] = 1800
            cfg["simulation"]["shipments"] = 24
            cfg["simulation"]["vehicles"] = 12
            cfg["simulation"]["sensors"] = 8
            cache[scenario] = run_once(
                scenario=scenario, cfg=cfg, invalid_rate=0.01,
                run_dir=tmp_path_factory.mktemp(scenario.lower()))
        return cache[scenario]

    return _run


def test_normal_delivery_raises_no_false_alerts(runs):
    stats = runs(NORMAL_DELIVERY).stats
    assert stats["cold_chain_alerts"] == 0
    assert stats["route_deviation_alerts"] == 0
    assert stats["valid"] > 0


def test_nothing_is_lost_every_event_is_accounted_for(runs):
    result = runs(NORMAL_DELIVERY)
    stats = result.stats
    assert stats["consumed"] == result.published
    assert stats["consumed"] == stats["valid"] + stats["quarantined"] + stats["duplicates"]


def test_invalid_records_are_quarantined_with_reasons(runs):
    result = runs(NORMAL_DELIVERY)
    assert result.stats["quarantined"] > 0
    assert set(result.stats["quarantine_by_reason"]) <= {
        "GPS_LATITUDE_RANGE", "GPS_LONGITUDE_RANGE", "GPS_SPEED_NEGATIVE",
        "SENSOR_TEMP_RANGE", "SENSOR_HUMIDITY_RANGE"}
    assert result.quarantine_sample[0]["raw_payload"]["payload"]


def test_duplicates_are_suppressed(runs):
    assert runs(DUPLICATE_EVENT).stats["duplicates"] > 0
    assert runs(NORMAL_DELIVERY).stats["duplicates"] == 0


def test_a_dropout_produces_late_events_that_are_still_tolerated(runs):
    stats = runs(GPS_DROPOUT).stats
    assert stats["late_within_tolerance"] > 0


def test_events_past_the_lateness_window_go_to_the_side_output(runs):
    stats = runs(LATE_EVENT).stats
    assert stats["late_side_output"] > 0


def test_a_sustained_excursion_alerts_and_a_spike_does_not(runs):
    stats = runs(COLD_CHAIN_EXCURSION).stats
    assert stats["cold_chain_alerts"] > 0
    assert stats["suppressed_spikes"] > 0


def test_leaving_the_corridor_raises_a_deviation_alert(runs):
    assert runs(ROUTE_DEVIATION).stats["route_deviation_alerts"] > 0


def test_dwell_delay_shows_up_in_gold(runs):
    result = runs(WAREHOUSE_DWELL_DELAY)
    assert result.gold["fact_warehouse_dwell"] > 0


def test_replaying_the_topic_returns_the_same_record_count(runs):
    assert runs(NORMAL_DELIVERY).replay_match is True


def test_the_lakehouse_layers_are_built(runs):
    result = runs(NORMAL_DELIVERY)
    assert result.bronze_rows > 0
    assert result.silver["silver_gps"] > 0
    assert result.gold["fact_delivery_performance"] > 0


def test_the_same_seed_produces_the_same_run(tmp_path):
    from lil.config import load_config
    outcomes = []
    for index in range(2):
        cfg = load_config()
        cfg["simulation"]["duration_seconds"] = 600
        cfg["simulation"]["shipments"] = 8
        cfg["simulation"]["vehicles"] = 4
        cfg["simulation"]["sensors"] = 4
        outcomes.append(run_once(scenario=NORMAL_DELIVERY, seed=42, cfg=cfg,
                                 run_dir=tmp_path / f"run{index}").stats)
    assert outcomes[0] == outcomes[1]
