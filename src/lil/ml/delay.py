"""Delivery delay, predicted when the truck leaves the warehouse.

Two targets from the same features: minutes late (regression) and late or
not (classification). The comparison that matters is against what a
dispatcher already has, so the baselines are the promise itself, a static
ETA at the planning speed, and the carrier's average for that time of day.

Evaluation is a rolling origin over the last ``test_days`` days. Before each
test day every model is refit on deliveries completed before that day began,
then asked about the shipments dispatched during it. Nothing from the test
day, including its own outcomes, reaches training.
"""
from __future__ import annotations

from datetime import timedelta, timezone

import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import f1_score, mean_absolute_error, precision_score, recall_score, roc_auc_score

from . import features as F

PLANNING_SPEED_KPH = 26.0
GBT_PARAMS = {"max_iter": 300, "learning_rate": 0.05, "max_leaf_nodes": 31,
              "min_samples_leaf": 20, "l2_regularization": 1.0, "random_state": 0}


def prepare(lake, cfg) -> dict:
    frame = F.delivery_frame(lake)
    raws = F.batch_raw_fields(frame, cfg.get_path("ml.carrier_window_days", 7),
                              cfg.get_path("ml.backlog_window_hours", 2))
    keep = frame["delivered_at"].notna().to_numpy()
    X = F.matrix(raws)
    dispatched = frame["dispatched_at"].to_numpy(dtype="datetime64[s]")
    delivered = frame["delivered_at"].to_numpy(dtype="datetime64[s]")
    local_day = (frame["dispatched_at"] + timedelta(hours=5, minutes=30)).dt.date.to_numpy()
    return {
        "ids": frame["shipment_id"].to_numpy()[keep],
        "raws": [r for r, k in zip(raws, keep) if k],
        "X": X[keep],
        "delay_min": (frame["delay_seconds"].to_numpy(dtype=float) / 60.0)[keep],
        "late": (frame["on_time_flag"].to_numpy() == 0)[keep].astype(int),
        "dispatched": dispatched[keep],
        "delivered": delivered[keep],
        "day": local_day[keep],
        "carrier": frame["carrier_id"].to_numpy()[keep],
    }


def gbt_regressor() -> HistGradientBoostingRegressor:
    return HistGradientBoostingRegressor(categorical_features=F.CATEGORICAL, **GBT_PARAMS)


def gbt_classifier() -> HistGradientBoostingClassifier:
    return HistGradientBoostingClassifier(categorical_features=F.CATEGORICAL, **GBT_PARAMS)


class LinearDesign:
    """One-hot categoricals, mean-impute and scale numerics, fitted on training rows only."""

    def fit(self, X):
        numeric = [i for i in range(X.shape[1]) if i not in F.CATEGORICAL]
        self.numeric = numeric
        self.means = np.nanmean(X[:, numeric], axis=0)
        filled = np.where(np.isnan(X[:, numeric]), self.means, X[:, numeric])
        self.scale = filled.std(axis=0) + 1e-9
        return self

    def transform(self, X):
        filled = np.where(np.isnan(X[:, self.numeric]), self.means, X[:, self.numeric])
        parts = [(filled - self.means) / self.scale]
        for index, size in zip(F.CATEGORICAL, (len(F.CARRIERS), len(F.WAREHOUSES))):
            codes = np.nan_to_num(X[:, index], nan=-1).astype(int)
            parts.append(np.eye(size)[np.clip(codes, 0, size - 1)] * (codes >= 0)[:, None])
        return np.hstack(parts)


def _peak_band(hour: float) -> int:
    hour = int(hour)
    return 2 if (8 <= hour <= 10 or 17 <= hour <= 19) else (1 if 11 <= hour <= 16 else 0)


def _static_eta_delay(X) -> np.ndarray:
    length = X[:, F.FEATURE_NAMES.index("route_length_km")]
    slack = X[:, F.FEATURE_NAMES.index("slack_min")]
    return length / PLANNING_SPEED_KPH * 60.0 - slack


def _carrier_hour_mean(X_train, y_train, X_test) -> np.ndarray:
    c, h = F.FEATURE_NAMES.index("carrier"), F.FEATURE_NAMES.index("dispatch_hour_ist")
    table: dict[tuple, list[float]] = {}
    for row, y in zip(X_train, y_train):
        table.setdefault((row[c], _peak_band(row[h])), []).append(y)
    overall = float(np.mean(y_train))
    return np.array([np.mean(table.get((row[c], _peak_band(row[h])), [overall])) for row in X_test])


def _scores(name, y_delay, y_late, delay_hat, late_score, late_pred) -> dict:
    out = {"model": name,
           "mae_min": round(float(mean_absolute_error(y_delay, delay_hat)), 2),
           "rmse_min": round(float(np.sqrt(np.mean((y_delay - delay_hat) ** 2))), 2)}
    if late_score is None:
        out.update({"auc": 0.5, "precision": None, "recall": 0.0, "f1": 0.0})
    else:
        out.update({
            "auc": round(float(roc_auc_score(y_late, late_score)), 3),
            "precision": round(float(precision_score(y_late, late_pred, zero_division=0)), 3),
            "recall": round(float(recall_score(y_late, late_pred, zero_division=0)), 3),
            "f1": round(float(f1_score(y_late, late_pred, zero_division=0)), 3),
        })
    return out


def evaluate(data: dict, test_days: int, threshold: float = 0.5) -> dict:
    days = sorted(set(data["day"]))
    test = days[-test_days:]
    preds = {k: {"delay": [], "score": [], "pred": []}
             for k in ("promise", "static_eta", "carrier_hour_mean", "linear", "gbt")}
    y_delay, y_late, per_day = [], [], []

    for day in test:
        start = np.datetime64(f"{day}T00:00:00") - np.timedelta64(330, "m")   # IST midnight in UTC
        train = data["delivered"] < start
        rows = data["day"] == day
        if not rows.any() or train.sum() < 100:
            continue
        Xtr, Xte = data["X"][train], data["X"][rows]
        dtr, ltr = data["delay_min"][train], data["late"][train]
        y_delay.append(data["delay_min"][rows])
        y_late.append(data["late"][rows])

        n = rows.sum()
        preds["promise"]["delay"].append(np.zeros(n))
        static = _static_eta_delay(Xte)
        preds["static_eta"]["delay"].append(static)
        preds["static_eta"]["score"].append(static)
        preds["static_eta"]["pred"].append((static > 0).astype(int))
        mean = _carrier_hour_mean(Xtr, dtr, Xte)
        preds["carrier_hour_mean"]["delay"].append(mean)
        preds["carrier_hour_mean"]["score"].append(mean)
        preds["carrier_hour_mean"]["pred"].append((mean > 0).astype(int))

        design = LinearDesign().fit(Xtr)
        ridge = Ridge(alpha=1.0).fit(design.transform(Xtr), dtr)
        logit = LogisticRegression(max_iter=2000).fit(design.transform(Xtr), ltr)
        p_lin = logit.predict_proba(design.transform(Xte))[:, 1]
        preds["linear"]["delay"].append(ridge.predict(design.transform(Xte)))
        preds["linear"]["score"].append(p_lin)
        preds["linear"]["pred"].append((p_lin >= threshold).astype(int))

        reg = gbt_regressor().fit(Xtr, dtr)
        clf = gbt_classifier().fit(Xtr, ltr)
        p_gbt = clf.predict_proba(Xte)[:, 1]
        preds["gbt"]["delay"].append(reg.predict(Xte))
        preds["gbt"]["score"].append(p_gbt)
        preds["gbt"]["pred"].append((p_gbt >= threshold).astype(int))
        per_day.append({"day": str(day), "train_rows": int(train.sum()), "test_rows": int(n),
                        "late": int(data["late"][rows].sum()),
                        "gbt_mae_min": round(float(mean_absolute_error(
                            data["delay_min"][rows], preds["gbt"]["delay"][-1])), 2)})

    yd, yl = np.concatenate(y_delay), np.concatenate(y_late)
    results = []
    for name, p in preds.items():
        delay_hat = np.concatenate(p["delay"])
        score = np.concatenate(p["score"]) if p["score"] else None
        pred = np.concatenate(p["pred"]) if p["pred"] else None
        results.append(_scores(name, yd, yl, delay_hat, score, pred))
    return {"test_days": [str(d) for d in test], "test_rows": int(len(yd)),
            "late_share": round(float(yl.mean()), 3), "models": results, "per_day": per_day}


def fit(data: dict, mask=None):
    mask = np.ones(len(data["late"]), dtype=bool) if mask is None else mask
    reg = gbt_regressor().fit(data["X"][mask], data["delay_min"][mask])
    clf = gbt_classifier().fit(data["X"][mask], data["late"][mask])
    return {"regressor": reg, "classifier": clf, "feature_names": F.FEATURE_NAMES,
            "feature_spec": F.FEATURE_SPEC}


def importance(model: dict, data: dict, mask) -> list[dict]:
    result = permutation_importance(model["classifier"], data["X"][mask], data["late"][mask],
                                    scoring="roc_auc", n_repeats=5, random_state=0)
    order = np.argsort(-result.importances_mean)
    return [{"feature": F.FEATURE_NAMES[i], "auc_drop": round(float(result.importances_mean[i]), 4)}
            for i in order]
