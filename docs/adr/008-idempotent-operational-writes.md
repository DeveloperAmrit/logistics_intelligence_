# ADR 008 — The pipeline writes PostgreSQL, idempotently

**Status:** accepted (Sprint 8, closed in Sprint 9)

## Context
The migrations existed from Sprint 3, but the demo path wrote only the
lakehouse. Operators need the current status of a shipment and the open
alerts, which are transactional questions, not analytical ones.

## Decision
`storage/operational.py` writes master data, the append-only status history,
the current status, alerts, quarantine records and late events. Every write
is safe to repeat: history is unique on `event_id`, alerts on `alert_id`,
quarantine on `(event_id, arrival_time)`, late events on `event_id`, and
master data is first-writer-wins. The current status only moves forward in
event time, so a late older event is kept in history but never overwrites a
newer status. Each run records its scenario, seed and counts in
`pipeline_run`.

Event ids are generated from their own seeded stream, so re-running a
scenario with the same seed is a replay of the same events rather than new
data, and the database does not grow.

## Consequences
* Writing the same run twice inserts nothing the second time (tested).
* The stream consumer uses the same writer at each checkpoint.
* Any status event can create the shipment row, because in a stream the
  CREATED event may arrive after a later one.
