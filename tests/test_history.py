"""Weeks of operations go through the same pipeline as the live scenarios."""
import json

from lil.config import load_config
from lil.runner import run_history
from lil.storage.lakehouse import Lakehouse


def test_every_history_event_is_accounted_for(small_history):
    result = small_history["result"]
    stats = result.stats
    assert stats["consumed"] == result.published
    assert stats["consumed"] == stats["valid"] + stats["quarantined"] + stats["duplicates"]
    assert stats["late_side_output"] > 0          # the injected late events were seen as late


def test_hourly_demand_keeps_the_empty_hours(small_history):
    lake = Lakehouse(small_history["dir"] / "lake")
    total, zeros, warehouses = lake.query("""
        SELECT count(*), sum(CASE WHEN orders = 0 THEN 1 ELSE 0 END),
               count(DISTINCT warehouse_id) FROM fact_hourly_demand""")[0]
    assert warehouses == 5 and zeros > 0 and total % 5 == 0
    created = lake.query("SELECT count(*) FROM silver_shipment_status WHERE new_status = 'CREATED'")
    assert lake.query("SELECT sum(orders) FROM fact_hourly_demand")[0][0] == created[0][0]


def test_gold_excursions_and_stream_alerts_agree_and_drift_is_invisible_to_the_rule(small_history):
    run_dir = small_history["dir"]
    labels = json.loads((run_dir / "labels.json").read_text())
    alerts = json.loads((run_dir / "alerts.json").read_text())
    alerted = {a["shipment_id"] for a in alerts if a["alert_type"] == "cold_chain"}
    gold = {r[0] for r in Lakehouse(run_dir / "lake").query(
        "SELECT DISTINCT shipment_id FROM fact_cold_chain_excursion")}
    assert gold == alerted
    assert labels["sensor_drift"], "the history should contain drifting reefers"
    assert not set(labels["sensor_drift"]) & alerted


def test_history_is_reproducible(tmp_path):
    runs = []
    for name in ("a", "b"):
        config = load_config()
        config["storage"]["root"] = str(tmp_path / name / "lake")
        runs.append(run_history(days=2, seed=3, cfg=config, run_dir=tmp_path / name).stats)
    assert runs[0] == runs[1]
