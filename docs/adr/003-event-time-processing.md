# ADR 003 — Event time, not processing time

**Status:** accepted (Sprint 6)

## Context
A truck passing through a dead zone reconnects and sends old positions after
newer ones have already arrived. Windowing on arrival time would put those
readings in the wrong window and quietly corrupt the history.

## Decision
Every record carries both `event_time` and `ingest_time`. All time-based
logic uses event time. A watermark of 60 seconds of bounded out-of-orderness
closes windows, 5 minutes of allowed lateness keeps them updatable, and
anything later goes to a late side output.

## Consequences
* Late data is a measured number rather than an accident.
* Nothing is dropped for being late; it is counted and inspectable.
* The two values are starting points from the design and are expected to be
  tuned against measured behaviour, not treated as constants.
