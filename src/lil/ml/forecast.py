"""Day-ahead order forecast per warehouse and hour.

The series is Gold's fact_hourly_demand: orders created per origin warehouse
per local hour, zeros included. Every morning at 00:00 IST the forecaster
predicts the 16 operating hours of the coming day using only what is known
then: the same hour yesterday, the same hour on the last three same
weekdays, the hour, the weekday and the warehouse.

Baselines are the seasonal naive forecasts a planner would use without a
model. Counts this small are mostly Poisson noise, so the report also gives
the error of the true demand rate (known only to the simulator): no model can
be expected to beat that floor.
"""
from __future__ import annotations

from datetime import timedelta

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor

from .features import WAREHOUSES

OPEN_HOURS = range(6, 22)
FEATURES = ["warehouse", "hour", "weekday", "lag_24", "lag_168", "lag_336", "mean_3w", "day_index"]
GBT_PARAMS = {"loss": "poisson", "max_iter": 250, "learning_rate": 0.05, "max_leaf_nodes": 15,
              "min_samples_leaf": 20, "random_state": 0}


def demand_frame(lake) -> pd.DataFrame:
    frame = lake.frame("""
        SELECT warehouse_id, hour_ist, hour_start_utc, orders
        FROM fact_hourly_demand ORDER BY warehouse_id, hour_ist
    """)
    frame["hour"] = frame["hour_ist"].dt.hour
    frame["weekday"] = frame["hour_ist"].dt.weekday
    frame["date"] = frame["hour_ist"].dt.date
    frame["warehouse"] = frame["warehouse_id"].map(lambda w: float(WAREHOUSES.index(w))
                                                   if w in WAREHOUSES else np.nan)
    first = frame["hour_ist"].min().normalize()
    frame["day_index"] = (frame["hour_ist"].dt.normalize() - first).dt.days
    grouped = frame.groupby("warehouse_id")["orders"]
    for lag in (24, 168, 336, 504):
        frame[f"lag_{lag}"] = grouped.shift(lag)
    frame["mean_3w"] = frame[["lag_168", "lag_336", "lag_504"]].mean(axis=1, skipna=True)
    return frame


def _wape(y, yhat) -> float:
    return float(np.abs(y - yhat).sum() / max(y.sum(), 1e-9))


def model() -> HistGradientBoostingRegressor:
    return HistGradientBoostingRegressor(categorical_features=[0], **GBT_PARAMS)


def usable(train: pd.DataFrame) -> list[str]:
    """Lags longer than the history are entirely missing early on; leave them
    out rather than ask the model to split on nothing."""
    return [c for c in FEATURES if c == "warehouse" or train[c].notna().any()]


def evaluate(lake, labels: dict, test_days: int) -> tuple[dict, dict]:
    frame = demand_frame(lake)
    frame = frame[frame["hour"].isin(OPEN_HOURS)].copy()
    dates = sorted(frame["date"].unique())
    test_dates = dates[-test_days:]
    columns = {"seasonal_naive_day": [], "seasonal_naive_week": [], "mean_last_3_weeks": [], "gbt_poisson": []}
    tested = []
    for day in test_dates:
        train = frame[frame["date"] < day]
        rows = frame[frame["date"] == day].copy()
        columns_used = usable(train)
        gbt = model().fit(train[columns_used].to_numpy(), train["orders"].to_numpy())
        rows["seasonal_naive_day"] = rows["lag_24"].fillna(0)
        rows["seasonal_naive_week"] = rows["lag_168"].fillna(0)
        rows["mean_last_3_weeks"] = rows["mean_3w"].fillna(0)
        rows["gbt_poisson"] = gbt.predict(rows[columns_used].to_numpy())
        tested.append(rows)
    test = pd.concat(tested)

    expected = labels.get("expected_hourly_demand", {})
    keys = test["warehouse_id"] + "|" + test["hour_start_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    test["true_rate"] = keys.map(expected).astype(float)

    y = test["orders"].to_numpy(dtype=float)
    daily = test.groupby(["warehouse_id", "date"]).sum(numeric_only=True)
    results = []
    for name in list(columns) + (["true_rate (floor)"] if test["true_rate"].notna().all() else []):
        col = "true_rate" if name.startswith("true_rate") else name
        results.append({
            "model": name,
            "hourly_wape": round(_wape(y, test[col].to_numpy()), 3),
            "hourly_mae": round(float(np.abs(y - test[col].to_numpy()).mean()), 3),
            "daily_wape": round(_wape(daily["orders"].to_numpy(), daily[col].to_numpy()), 3),
        })
    summary = {"test_days": [str(d) for d in test_dates], "test_hours": int(len(test)),
               "mean_orders_per_hour": round(float(y.mean()), 2), "models": results}
    return summary, {"frame": frame}


def fit_and_forecast(lake) -> tuple[HistGradientBoostingRegressor, pd.DataFrame]:
    """Fit on everything and forecast the day after the data ends."""
    full = demand_frame(lake)
    train = full[full["hour"].isin(OPEN_HOURS)]
    columns_used = usable(train)
    gbt = model().fit(train[columns_used].to_numpy(), train["orders"].to_numpy())
    gbt.feature_names_used_ = columns_used

    next_day = full["hour_ist"].max().normalize() + timedelta(days=1)
    series = full.set_index(["warehouse_id", "hour_ist"])["orders"]
    rows = []
    for warehouse in sorted(full["warehouse_id"].unique()):
        for hour in OPEN_HOURS:
            when = next_day + timedelta(hours=hour)
            lags = {lag: series.get((warehouse, when - timedelta(hours=lag)), np.nan)
                    for lag in (24, 168, 336, 504)}
            rows.append({"warehouse_id": warehouse, "hour_ist": when,
                         "warehouse": float(WAREHOUSES.index(warehouse)), "hour": hour,
                         "weekday": when.weekday(),
                         "lag_24": lags[24], "lag_168": lags[168], "lag_336": lags[336],
                         "mean_3w": np.nanmean([lags[168], lags[336], lags[504]]),
                         "day_index": int(full["day_index"].max()) + 1})
    out = pd.DataFrame(rows)
    out["forecast_orders"] = gbt.predict(out[columns_used].to_numpy()).round(2)
    return gbt, out[["warehouse_id", "hour_ist", "forecast_orders"]]
