# ADR 001 — Kafka is the durable event backbone

**Status:** accepted (Sprint 3)

## Context
The simulator could write straight into PostgreSQL, or straight into the
dashboard. That is fewer moving parts and less to explain.

## Decision
Producers publish to Kafka. Nothing downstream is written directly by a
producer.

## Consequences
* Replay is possible. A consumer that crashes reprocesses retained events
  instead of forcing the whole simulation to be regenerated.
* Consumers are independent. The alert path and the storage path read the
  same stream without knowing about each other.
* A slow consumer does not stop the producers.
* The cost is a broker to run and offsets to reason about, which is the part
  of the course this project exists to learn.

## Rejected
Simulator to dashboard directly: removes the durable layer, prevents replay,
and makes a second consumer a rewrite rather than an addition.
