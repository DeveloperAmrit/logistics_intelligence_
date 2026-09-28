"""Command line entry point.

    python -m lil demo                      one scenario, end to end, offline
    python -m lil simulate --scenario X     publish a scenario to the bus
    python -m lil pipeline                  consume, validate, detect, land
    python -m lil lakehouse                 rebuild Silver and Gold
    python -m lil kpis                      print the business KPIs
    python -m lil scenarios                 run every scenario and compare
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .config import load_config
from .runner import run_once
from .sim.scenarios import ALL_SCENARIOS
from .storage.lakehouse import Lakehouse


def _print_report(result) -> None:
    stats = result.stats
    print(f"\nscenario            {result.scenario}   seed {result.seed}   "
          f"{result.seconds}s")
    print("-" * 64)
    print(f"published           {result.published}")
    print(f"consumed            {stats['consumed']}")
    print(f"valid               {stats['valid']}")
    print(f"quarantined         {stats['quarantined']}  {stats['quarantine_by_reason']}")
    print(f"duplicates dropped  {stats['duplicates']}")
    print(f"on time             {stats['on_time']}")
    print(f"late, tolerated     {stats['late_within_tolerance']}")
    print(f"late, side output   {stats['late_side_output']}")
    print(f"cold-chain alerts   {stats['cold_chain_alerts']}  "
          f"(spikes suppressed: {stats['suppressed_spikes']})")
    print(f"deviation alerts    {stats['route_deviation_alerts']}")
    print(f"replay count match  {result.replay_match}")
    print(f"bronze rows         {result.bronze_rows}")
    print(f"silver              {result.silver}")
    print(f"gold                {result.gold}")
    delivery = result.kpis["delivery"]
    print(f"OTIF                {delivery['otif_pct']}%  "
          f"median delay {delivery['median_delay_min']} min  "
          f"p95 {delivery['p95_delay_min']} min")
    cold = result.kpis["cold_chain"]
    print(f"excursions in gold  {cold['excursions']}  peak {cold['peak_temperature']} C")
    print(f"parquet on disk     {result.sizes}")
    print()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lil", description="Logistics Intelligence Platform")
    sub = parser.add_subparsers(dest="command", required=True)

    demo = sub.add_parser("demo", help="run one scenario end to end with no broker")
    demo.add_argument("--scenario", default="COLD_CHAIN_EXCURSION", choices=ALL_SCENARIOS)
    demo.add_argument("--seed", type=int, default=None)
    demo.add_argument("--backend", default="memory", choices=["memory", "kafka"])
    demo.add_argument("--invalid-rate", type=float, default=0.01)
    demo.add_argument("--json", action="store_true", help="print the full report as JSON")

    every = sub.add_parser("scenarios", help="run every scenario and compare the outcomes")
    every.add_argument("--seed", type=int, default=None)

    kpis = sub.add_parser("kpis", help="print KPIs from an existing run")
    kpis.add_argument("--run-dir", default=None)

    args = parser.parse_args(argv)
    cfg = load_config()

    if args.command == "demo":
        result = run_once(scenario=args.scenario, seed=args.seed, cfg=cfg,
                          backend=args.backend, invalid_rate=args.invalid_rate)
        if args.json:
            print(json.dumps(result.as_dict(), indent=2, default=str))
        else:
            _print_report(result)
        return 0

    if args.command == "scenarios":
        print(f"{'scenario':<24}{'valid':>8}{'quar':>7}{'dupes':>7}"
              f"{'late.tol':>9}{'late.side':>10}{'cc':>5}{'rd':>5}")
        print("-" * 76)
        for scenario in ALL_SCENARIOS:
            result = run_once(scenario=scenario, seed=args.seed, cfg=cfg)
            stats = result.stats
            print(f"{scenario:<24}{stats['valid']:>8}{stats['quarantined']:>7}"
                  f"{stats['duplicates']:>7}{stats['late_within_tolerance']:>9}"
                  f"{stats['late_side_output']:>10}{stats['cold_chain_alerts']:>5}"
                  f"{stats['route_deviation_alerts']:>5}")
        return 0

    if args.command == "kpis":
        from .analytics import kpis as kpi_module
        run_dir = Path(args.run_dir) if args.run_dir else cfg.lake_root.parent / "runs"
        lake_dir = run_dir if (run_dir / "gold").exists() else run_dir / "lake"
        lake = Lakehouse(lake_dir)
        print(json.dumps({
            "delivery": kpi_module.delivery_kpis(lake),
            "cold_chain": kpi_module.cold_chain_summary(lake),
            "dwell": kpi_module.dwell_summary(lake),
            "carriers": kpi_module.carrier_performance(lake),
        }, indent=2))
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
