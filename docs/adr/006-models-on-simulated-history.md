# ADR 006 — Models train on five weeks of simulated history, scored by rolling origin

**Status:** accepted (Sprint 9)

## Context
The scenario hour has 120 shipments, all created at once. A delay model, a
demand forecast or an anomaly score needs weeks of operations with daily and
weekly rhythm, and an honest test needs data the model has not seen. There
is still no live carrier feed (ADR 002).

## Decision
`make history` generates 35 days of operations (about 3,900 shipments and
200,000 events) with demand that follows the hour and the weekday, peak-hour
congestion, carriers of different reliability, warehouse backlog, transit
incidents, and a few injected faults: reefers that drift upward but stay
under 8 °C, real excursions, shipments held at the dock, detours. The events
go through the unchanged pipeline, and the models read only Gold.

What the simulator knows and the pipeline must not (which trip drifted, the
true demand rate) is written to `labels.json` and is opened only to score.

Every model is evaluated with a rolling origin: before each test day it is
refit on what was known before that day began. Baselines are what a
dispatcher already has: the promise, a static ETA at the planning speed, the
carrier's average, seasonal naive forecasts and the fixed 8 °C rule.

## Consequences
* Results are reproducible from a seed and traceable to Gold through the
  registry's data fingerprint.
* The numbers describe the simulator's world, not a real carrier. The report
  says so, and gives the noise floor where one exists (Poisson demand).
* Where a rule is already enough (a fixed 60-minute dwell threshold finds
  every held shipment), the report says that too rather than preferring the
  model.

## Rejected
Prophet for demand: a large dependency for a day-ahead hourly series that a
Poisson gradient-boosted model on calendar features and lags handles.
XGBoost: scikit-learn's histogram gradient boosting gives the same family of
model without another native dependency.
