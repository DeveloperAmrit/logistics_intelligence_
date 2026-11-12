"""Train, evaluate and register every model from one history run.

    python -m lil ml        (make ml)

Reads data/history/lake (Gold), writes models to the registry, predictions
to data/history/lake/ml/ for the API, and the evaluation to
data/history/ml_report.json. labels.json is opened only to score.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ..config import REPO_ROOT, Config
from ..storage.lakehouse import GOLD, Lakehouse
from . import anomaly, delay, forecast
from .features import FEATURE_SPEC
from .online import replay_check
from .registry import ModelRegistry, fingerprint


def _registry(cfg: Config) -> ModelRegistry:
    root = Path(cfg.get_path("ml.registry", "data/models"))
    return ModelRegistry(root if root.is_absolute() else REPO_ROOT / root)


def train_all(cfg: Config, history_dir: Path | None = None, online_check: bool = True) -> dict:
    started = time.time()
    history_dir = Path(history_dir) if history_dir else cfg.lake_root.parent / "history"
    lake = Lakehouse(history_dir / "lake")
    labels = json.loads((history_dir / "labels.json").read_text())
    registry = _registry(cfg)
    gold_files = sorted((history_dir / "lake" / GOLD).glob("*.parquet"))
    data_print = fingerprint(gold_files)
    test_days = cfg.get_path("ml.test_days", 14)
    out_dir = history_dir / "lake" / "ml"
    out_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ delay
    data = delay.prepare(lake, cfg)
    evaluation = delay.evaluate(data, test_days, cfg.get_path("ml.delay_risk_threshold", 0.5))
    first_test = np.datetime64(f"{evaluation['test_days'][0]}T00:00:00") - np.timedelta64(330, "m")
    before = data["delivered"] < first_test
    holdout = delay.fit(data, before)
    test_mask = np.isin(np.array([str(d) for d in data["day"]]), evaluation["test_days"])
    evaluation["importance"] = delay.importance(holdout, data, test_mask)[:6]
    common = {"feature_spec": FEATURE_SPEC, "data_fingerprint": data_print,
              "params": delay.GBT_PARAMS, "model_class": "HistGradientBoosting"}
    holdout_version = registry.save("delay_gbt", holdout, {
        **common, "training_window": f"delivered before {evaluation['test_days'][0]} IST",
        "rows": int(before.sum()), "evaluation": evaluation}, tag="holdout")
    final = delay.fit(data)
    final_version = registry.save("delay_gbt", final, {
        **common, "training_window": f"{data['day'].min()} to {data['day'].max()} IST",
        "rows": int(len(data["late"])), "evaluation": evaluation})
    scores = final["classifier"].predict_proba(data["X"])[:, 1]
    pq.write_table(pa.table({"shipment_id": data["ids"].astype(str),
                             "p_late": np.round(scores, 4),
                             "predicted_delay_min": np.round(final["regressor"].predict(data["X"]), 2),
                             "actual_delay_min": np.round(data["delay_min"], 2)}),
                   out_dir / "delay_scores.parquet")

    # ---------------------------------------------------------------- anomaly
    trips, trip_model = anomaly.evaluate_trips(lake, labels, test_days,
                                               cfg.get_path("ml.anomaly_quantile", 0.95))
    trip_version = registry.save("trip_anomaly_iforest", trip_model, {
        "feature_spec": "trip-features-v1", "data_fingerprint": data_print,
        "params": {**anomaly.FOREST_PARAMS, "quantile": cfg.get_path("ml.anomaly_quantile", 0.95)},
        "model_class": "IsolationForest", "training_window": f"before the last {test_days} days",
        "evaluation": trips})
    dwell, dwell_model = anomaly.evaluate_dwell(lake, labels, test_days,
                                                cfg.get_path("ml.dwell_z_threshold", 3.5))
    dwell_version = registry.save("dwell_robust_z", dwell_model, {
        "feature_spec": "dwell-log-processing-v1", "data_fingerprint": data_print,
        "params": {"z": cfg.get_path("ml.dwell_z_threshold", 3.5)}, "model_class": "median/MAD",
        "training_window": f"before the last {test_days} days", "evaluation": dwell})

    # --------------------------------------------------------------- forecast
    demand, _ = forecast.evaluate(lake, labels, cfg.get_path("ml.forecast_test_days", 7))
    forecaster, next_day = forecast.fit_and_forecast(lake)
    forecast_version = registry.save("demand_gbt_poisson", forecaster, {
        "feature_spec": "hourly-demand-v1", "data_fingerprint": data_print,
        "params": forecast.GBT_PARAMS, "model_class": "HistGradientBoostingRegressor",
        "training_window": "all history", "evaluation": demand})
    next_day["hour_ist"] = next_day["hour_ist"].astype(str)
    pq.write_table(pa.Table.from_pandas(next_day, preserve_index=False),
                   out_dir / "demand_forecast_next_day.parquet")

    report = {
        "history_dir": str(history_dir), "data_fingerprint": data_print,
        "delay": {"holdout_version": holdout_version, "final_version": final_version, **evaluation},
        "trip_anomaly": {"version": trip_version, **trips},
        "dwell_anomaly": {"version": dwell_version, **dwell},
        "demand_forecast": {"version": forecast_version, **demand},
    }
    if online_check:
        report["online_parity"] = replay_check(cfg, history_dir, holdout, holdout_version,
                                               evaluation["test_days"])
    report["seconds"] = round(time.time() - started, 1)
    (history_dir / "ml_report.json").write_text(json.dumps(report, indent=2, default=str))
    return report
