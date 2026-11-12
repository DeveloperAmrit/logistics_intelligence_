"""Models over Gold: delivery delay, telemetry and dwell anomalies, demand.

Everything here trains on the lakehouse built by the unchanged pipeline, is
evaluated with rolling origins (train only on what was known before the day
being predicted), and is saved with the data window and feature definition
that produced it.
"""
