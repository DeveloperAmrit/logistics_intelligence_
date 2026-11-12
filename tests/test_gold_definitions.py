"""Gold must define things the same way the stream does."""
from datetime import datetime, timedelta, timezone

from lil.envelope import Event
from lil.processing.pipeline import Pipeline
from lil.storage.lakehouse import Lakehouse

WHEN = datetime(2026, 9, 22, 4, 0, tzinfo=timezone.utc)


def _reading(shipment, offset, temperature):
    event = Event(event_type="sensor.reading", event_time=WHEN + timedelta(seconds=offset),
                  source="test", sensor_id="sen-001", shipment_id=shipment,
                  ingest_time=WHEN + timedelta(seconds=offset),
                  payload={"temperature_c": temperature, "humidity_pct": 50.0,
                           "temperature_class": "COLD_2_8"})
    return Pipeline._bronze_row(None, event, "ON_TIME")


def test_two_separate_spikes_are_not_an_excursion_but_two_in_a_row_are(tmp_path):
    rows = [_reading("ship-a", i * 30, t) for i, t in enumerate([5.0, 9.0, 5.1, 9.2, 5.0])]
    rows += [_reading("ship-b", i * 30, t) for i, t in enumerate([5.0, 9.0, 9.4, 5.2])]
    lake = Lakehouse(tmp_path)
    lake.write_bronze(rows)
    lake.build_silver()
    lake.build_gold()
    found = lake.query("SELECT shipment_id, breach_readings FROM fact_cold_chain_excursion")
    assert found == [("ship-b", 2)]
