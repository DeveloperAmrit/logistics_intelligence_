# ADR 007 — The stream consumer checkpoints state and offsets together

**Status:** accepted (Sprint 9), keeps the Flink semantics of ADR 003 without Flink

## Context
The batch runner reads a retained topic and processes it in one pass. A
deployed consumer runs forever, and when it crashes its detectors lose their
memory (open excursion episodes, the deduplication window, the watermark).
Committing Kafka offsets alone is not enough: restarting from committed
offsets with empty detector state misses alerts, and restarting with state
that is ahead of the offsets counts events twice.

## Decision
`lil stream` runs the same Pipeline behind a confluent-kafka consumer. Once a
second it flushes the producer (alerts and DLQ records acknowledged), then
writes one checkpoint file holding the detector state and the offsets it
corresponds to, atomically. On restart it restores the state and seeks to
those offsets in the assignment callback. Kafka offsets are still committed,
but only so lag can be monitored.

Alerts have deterministic ids, so an alert re-emitted after a crash is the
same alert: the alert table's primary key and downstream consumers keep one.
Static group membership (`group.instance.id`) lets the restarted consumer
take its partitions back without waiting for the session timeout.

## Consequences
* State is exactly-once: a run killed with SIGKILL and restarted ends with the
  same counts and the same alert set as an uninterrupted one (measured,
  `make bench`).
* Output is at-least-once: events after the last checkpoint are processed
  twice, so up to one second of alerts can be emitted twice, with the same id.
* One consumer process. Scaling out means partitioning the detector state by
  key, which Flink's keyed state does for you; that remains the production
  path.
