"""Anomalies the fixed rules cannot see.

Telemetry: the cold-chain rule fires only above 8 C. A reefer whose
temperature creeps from 4 C to 7.8 C over a trip never trips it, yet that is
the failure that becomes an excursion on the next trip. Each refrigerated
trip is summarised in Gold (fact_sensor_trip) and scored with an isolation
forest fitted on earlier trips. Features are deliberately robust (median,
p90, rise, slope, share above 7 C), so a single noisy spike, which the rule
already ignores, does not make a trip look unusual. The maximum is left out
for the same reason.

Dwell: time from order creation to dispatch, compared with that warehouse's
own history on a log scale (median and MAD), because a slow warehouse at its
peak is not an anomaly and a fast one holding a shipment for an hour is.

Labels from the simulator are used only to score the result.
"""
from __future__ import annotations

import numpy as np
from sklearn.ensemble import IsolationForest

TRIP_FEATURES = ["mean_c", "median_c", "p90_c", "std_c", "slope_c_per_h", "rise_c", "frac_above_7"]
FOREST_PARAMS = {"n_estimators": 300, "random_state": 0}


def trip_frame(lake):
    frame = lake.frame("""
        SELECT t.*, f.dispatched_at,
               (t.end_c - t.start_c) AS rise_c,
               t.readings_above_7 * 1.0 / t.readings AS frac_above_7,
               CAST(f.dispatched_at + INTERVAL 330 MINUTE AS DATE) AS day,
               EXISTS (SELECT 1 FROM fact_alert a
                       WHERE a.shipment_id = t.shipment_id AND a.alert_type = 'cold_chain') AS rule_flag
        FROM fact_sensor_trip t JOIN fact_delivery_performance f USING (shipment_id)
        WHERE t.readings >= 5
    """)
    return frame


def _split(days, test_days):
    ordered = sorted(set(days))
    return set(ordered[:-test_days]), set(ordered[-test_days:])


def evaluate_trips(lake, labels: dict, test_days: int, quantile: float) -> tuple[dict, dict]:
    frame = trip_frame(lake)
    train_days, test_window = _split(frame["day"], test_days)
    train = frame[frame["day"].isin(train_days)]
    test = frame[frame["day"].isin(test_window)].copy()

    forest = IsolationForest(**FOREST_PARAMS).fit(train[TRIP_FEATURES].to_numpy())
    train_scores = -forest.score_samples(train[TRIP_FEATURES].to_numpy())
    threshold = float(np.quantile(train_scores, quantile))
    test["score"] = -forest.score_samples(test[TRIP_FEATURES].to_numpy())
    test["model_flag"] = test["score"] > threshold

    drift, excursion = set(labels["sensor_drift"]), set(labels["sensor_excursion"])
    spike = set(labels["spike_only"])

    def category(shipment):
        if shipment in drift:
            return "drift (under 8 C)"
        if shipment in excursion:
            return "excursion (over 8 C)"
        if shipment in spike:
            return "single spikes only"
        return "normal"

    test["category"] = test["shipment_id"].map(category)
    rows = []
    for name in ("drift (under 8 C)", "excursion (over 8 C)", "single spikes only", "normal"):
        part = test[test["category"] == name]
        rows.append({"category": name, "trips": int(len(part)),
                     "rule_flagged": int(part["rule_flag"].sum()),
                     "model_flagged": int(part["model_flag"].sum())})
    anomalous = test["category"].isin(["drift (under 8 C)", "excursion (over 8 C)"])
    either = test["model_flag"] | test["rule_flag"]
    summary = {
        "train_trips": int(len(train)), "test_trips": int(len(test)),
        "threshold_quantile": quantile, "by_category": rows,
        "model_precision": round(float((test["model_flag"] & anomalous).sum()
                                       / max(test["model_flag"].sum(), 1)), 3),
        "model_recall": round(float((test["model_flag"] & anomalous).sum() / max(anomalous.sum(), 1)), 3),
        "rule_recall": round(float((test["rule_flag"] & anomalous).sum() / max(anomalous.sum(), 1)), 3),
        "combined_recall": round(float((either & anomalous).sum() / max(anomalous.sum(), 1)), 3),
    }
    return summary, {"forest": forest, "threshold": threshold, "features": TRIP_FEATURES}


def dwell_frame(lake):
    return lake.frame("""
        SELECT shipment_id, origin_warehouse_id,
               processing_seconds / 60.0 AS processing_min,
               CAST(dispatched_at + INTERVAL 330 MINUTE AS DATE) AS day
        FROM fact_delivery_performance WHERE processing_seconds IS NOT NULL
    """)


def evaluate_dwell(lake, labels: dict, test_days: int, z_threshold: float,
                   fixed_minutes: float = 60.0) -> tuple[dict, dict]:
    frame = dwell_frame(lake)
    train_days, test_window = _split(frame["day"], test_days)
    train = frame[frame["day"].isin(train_days)]
    test = frame[frame["day"].isin(test_window)].copy()

    baseline = {}
    for warehouse, part in train.groupby("origin_warehouse_id"):
        logs = np.log(part["processing_min"].clip(lower=1.0))
        median = float(np.median(logs))
        mad = float(np.median(np.abs(logs - median))) * 1.4826
        baseline[warehouse] = (median, max(mad, 1e-6))

    def zscore(row):
        median, mad = baseline.get(row.origin_warehouse_id, (0.0, 1.0))
        return (np.log(max(row.processing_min, 1.0)) - median) / mad

    test["z"] = [zscore(r) for r in test.itertuples(index=False)]
    stuck = test["shipment_id"].isin(set(labels["dwell_stuck"]))

    def score(flags):
        hits = int((flags & stuck).sum())
        return {"flagged": int(flags.sum()), "true_positives": hits,
                "precision": round(hits / max(int(flags.sum()), 1), 3),
                "recall": round(hits / max(int(stuck.sum()), 1), 3)}

    summary = {"test_shipments": int(len(test)), "held_at_dock": int(stuck.sum()),
               "robust_z": {"threshold": z_threshold, **score(test["z"] > z_threshold)},
               f"fixed_{int(fixed_minutes)}_min": {"threshold": fixed_minutes,
                                                   **score(test["processing_min"] > fixed_minutes)}}
    return summary, {"baseline": baseline, "z_threshold": z_threshold}
