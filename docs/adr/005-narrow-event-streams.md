# ADR 005 — Narrow event streams, not one wide table

**Status:** accepted (Sprint 3)

## Context
One table holding every measurement is simpler to query at first.

## Decision
GPS, sensor, warehouse and lifecycle events keep separate schemas and
separate topics, joined by stable identifiers and event time. Master data
stays relational in PostgreSQL.

## Consequences
* No repetition of latitude and longitude in every temperature row, and no
  null-heavy records.
* Each measurement has one clear owner and one schema that can evolve on its
  own.
* Joins are explicit, which is the trade we accept.
