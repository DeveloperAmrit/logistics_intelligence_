# ADR 004 — DuckDB and Parquet for the lakehouse

**Status:** accepted (Sprint 7), supersedes the MinIO + Iceberg plan for the
prototype

## Context
The original architecture named MinIO with Apache Iceberg for analytical
history. Both are capable, but both add a service to run, and MinIO's licence
and console restrictions changed after that plan was written.

## Decision
Bronze, Silver and Gold are Parquet files on the local filesystem, queried
with DuckDB. Both DuckDB (MIT) and Parquet (Apache-2.0) are open source and
need no server.

## Consequences
* The whole prototype runs on a laptop with no object store to operate.
* The directory layout is the one an object store would hold
  (`bronze/<event_type>/event_date=YYYY-MM-DD/`), so moving Bronze to
  S3-compatible storage later is a path change, not a rewrite.
* Compression is ZSTD, as originally specified.
* What is given up for now: Iceberg snapshots, time travel and partition
  evolution. Those are a Sprint 9 extension if the core stays stable.
