# ADR 002 — A Jodhpur-centred simulator, not a public dataset

**Status:** accepted (Sprint 3)

## Context
Continuous truck GPS, reefer sensor and warehouse scan feeds are not
publicly available at the volume this project needs, and no carrier gives a
student project live device access.

## Decision
Generate the live stream with a controlled simulator over a small Jodhpur
road network. Use public datasets (Olist, DataCo, NYC TLC) only to calibrate
plausible trip durations and delay distributions.

## Consequences
* Faults can be injected on demand and repeated exactly with a fixed seed,
  which a replayed public CSV cannot do.
* Movement is road-valid, so route deviation and geofencing mean something.
* The project cannot claim interoperability with a carrier's real devices or
  ERP. That is documented as a limitation rather than implied away.

## Note on the backend
The default simulator is built in and needs nothing installed. A SUMO/TraCI
backend can replace it without touching anything downstream, because the
producers only need position, speed, route and time.
