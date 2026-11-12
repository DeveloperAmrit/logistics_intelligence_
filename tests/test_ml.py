"""The models: point-in-time features, honest baselines, reproducible versions."""
import math
from datetime import datetime, timedelta, timezone

import numpy as np

from lil.ml import features as F
from lil.ml.registry import ModelRegistry

T = datetime(2026, 9, 20, 6, 0, tzinfo=timezone.utc)


def _raw(history, created=None):
    return F.raw_fields(route_length_km=10.0, origin_warehouse_id="wh-001", carrier_id="car-001",
                        temperature_class="AMBIENT", created_at=T - timedelta(minutes=40),
                        dispatched_at=T, promised_at=T + timedelta(minutes=45),
                        carrier_history=history, origin_created=created or [])


def test_carrier_reliability_uses_only_deliveries_finished_before_dispatch():
    history = [(T - timedelta(days=8), 0, 30.0),       # outside the 7-day window
               (T - timedelta(hours=1), 1, -10.0),     # inside
               (T + timedelta(minutes=1), 0, 50.0)]    # after dispatch: the future
    raw = _raw(history)
    assert raw["carrier_trips_7d"] == 1
    assert raw["carrier_ontime_7d"] == 1.0
    assert raw["carrier_delay_7d_min"] == -10.0


def test_backlog_counts_orders_in_the_two_hours_before_dispatch():
    created = [T - timedelta(hours=3), T - timedelta(minutes=90), T - timedelta(minutes=5),
               T + timedelta(minutes=5)]
    assert _raw([], created)["origin_orders_2h"] == 2


def test_unknown_categories_become_missing_not_a_wrong_code():
    raw = _raw([])
    raw["carrier_id"] = "car-999"
    vector = F.vector(raw)
    assert math.isnan(vector[F.FEATURE_NAMES.index("carrier")])
    assert math.isnan(vector[F.FEATURE_NAMES.index("carrier_ontime_7d")])   # no history yet


def test_delay_model_beats_the_promise_and_the_static_eta(small_ml):
    models = {m["model"]: m for m in small_ml["delay"]["models"]}
    assert models["gbt"]["mae_min"] < models["promise"]["mae_min"]
    assert models["gbt"]["mae_min"] < models["static_eta"]["mae_min"]
    assert 0.5 < models["gbt"]["auc"] <= 1.0


def test_forecast_is_scored_against_the_noise_floor(small_ml):
    names = [m["model"] for m in small_ml["demand_forecast"]["models"]]
    assert "true_rate (floor)" in names and "gbt_poisson" in names


def test_online_features_match_batch_features(small_ml):
    parity = small_ml["online_parity"]
    assert parity["compared_with_batch"] > 500
    assert parity["identical_features"] / parity["compared_with_batch"] > 0.98
    assert parity["max_probability_gap_when_identical"] < 1e-9


def test_registry_versions_follow_the_inputs(tmp_path):
    registry = ModelRegistry(tmp_path)
    meta = {"feature_spec": "x", "training_window": "w", "data_fingerprint": "d",
            "params": {"a": 1}, "model_class": "M"}
    first = registry.save("m", {"weights": np.arange(3)}, meta)
    again = registry.save("m", {"weights": np.arange(3)}, meta)
    other = registry.save("m", {"weights": np.arange(3)}, {**meta, "params": {"a": 2}})
    assert first == again and first != other
    model, saved = registry.load("m")
    assert saved["version"] == other and list(model["weights"]) == [0, 1, 2]
