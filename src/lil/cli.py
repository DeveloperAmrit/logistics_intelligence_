"""Command line entry point.

    python -m lil demo                      one scenario, end to end, offline
    python -m lil scenarios                 run every scenario and compare
    python -m lil kpis                      print the business KPIs
    python -m lil history --days 35         weeks of operations through the same pipeline
    python -m lil ml                        train, evaluate and register the models
    python -m lil db migrate                apply db/migrations to PostgreSQL
    python -m lil produce --rate 2000       publish a scenario to Kafka at a fixed rate
    python -m lil stream                    the pipeline as a long-running Kafka consumer
    python -m lil bench                     latency, capacity and recovery on the broker
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .config import load_config
from .runner import run_history, run_once
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
    if result.operational:
        print(f"postgres            inserted {result.operational['inserted']}")
    print()


def _print_ml(report: dict) -> None:
    delay = report["delay"]
    print(f"\ndelay at dispatch   rolling origin over {len(delay['test_days'])} days, "
          f"{delay['test_rows']} shipments, {delay['late_share'] * 100:.1f}% late")
    print(f"  {'model':<20}{'MAE min':>9}{'RMSE':>8}{'AUC':>7}{'prec':>7}{'recall':>8}")
    for row in delay["models"]:
        precision = "-" if row["precision"] is None else f"{row['precision']:.2f}"
        print(f"  {row['model']:<20}{row['mae_min']:>9}{row['rmse_min']:>8}{row['auc']:>7}"
              f"{precision:>7}{row['recall']:>8}")
    trips = report["trip_anomaly"]
    print(f"\nreefer trips        {trips['test_trips']} test trips, model flags vs the 8 C rule")
    for row in trips["by_category"]:
        print(f"  {row['category']:<24}{row['trips']:>5} trips   rule {row['rule_flagged']:>3}"
              f"   model {row['model_flagged']:>3}")
    dwell = report["dwell_anomaly"]
    print(f"\ndwell               {dwell['held_at_dock']} held shipments in {dwell['test_shipments']}")
    for key in ("robust_z", "fixed_60_min"):
        print(f"  {key:<14} flagged {dwell[key]['flagged']:>3}  precision {dwell[key]['precision']}"
              f"  recall {dwell[key]['recall']}")
    demand = report["demand_forecast"]
    print(f"\ndemand, day ahead   {len(demand['test_days'])} days, "
          f"{demand['mean_orders_per_hour']} orders per warehouse-hour")
    for row in demand["models"]:
        print(f"  {row['model']:<22} hourly WAPE {row['hourly_wape']:<6} daily WAPE {row['daily_wape']}")
    if "online_parity" in report:
        parity = report["online_parity"]
        print(f"\nonline vs batch     {parity['identical_features']} of {parity['compared_with_batch']}"
              f" dispatches identical; differing fields {parity['differing_fields']}")
        print(f"  delay-risk alerts in test window {parity['test_window_alerts']}, precision "
              f"{parity['alert_precision']}, recall {parity['alert_recall']}")
    print()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lil", description="Logistics Intelligence Platform")
    sub = parser.add_subparsers(dest="command", required=True)

    demo = sub.add_parser("demo", help="run one scenario end to end with no broker")
    demo.add_argument("--scenario", default="COLD_CHAIN_EXCURSION", choices=ALL_SCENARIOS)
    demo.add_argument("--seed", type=int, default=None)
    demo.add_argument("--backend", default="memory", choices=["memory", "kafka"])
    demo.add_argument("--invalid-rate", type=float, default=0.01)
    demo.add_argument("--postgres", action="store_true", help="also write the operational store")
    demo.add_argument("--json", action="store_true", help="print the full report as JSON")

    every = sub.add_parser("scenarios", help="run every scenario and compare the outcomes")
    every.add_argument("--seed", type=int, default=None)

    kpis = sub.add_parser("kpis", help="print KPIs from an existing run")
    kpis.add_argument("--run-dir", default=None)

    history = sub.add_parser("history", help="simulate weeks of operations through the pipeline")
    history.add_argument("--days", type=int, default=None)
    history.add_argument("--seed", type=int, default=None)
    history.add_argument("--postgres", action="store_true")

    ml = sub.add_parser("ml", help="train, evaluate and register the models")
    ml.add_argument("--history-dir", default=None)
    ml.add_argument("--no-online-check", action="store_true")

    db = sub.add_parser("db", help="operational store")
    db.add_argument("action", choices=["migrate", "counts"])

    produce = sub.add_parser("produce", help="publish a scenario to Kafka")
    produce.add_argument("--scenario", default="COLD_CHAIN_EXCURSION",
                         choices=ALL_SCENARIOS + ["BENCHMARK_MIX"])
    produce.add_argument("--seed", type=int, default=None)
    produce.add_argument("--rate", type=float, default=None, help="events per second (default: as fast as possible)")

    stream = sub.add_parser("stream", help="run the pipeline as a Kafka consumer")
    stream.add_argument("--scenario", default="COLD_CHAIN_EXCURSION",
                        choices=ALL_SCENARIOS + ["BENCHMARK_MIX"])
    stream.add_argument("--seed", type=int, default=None)
    stream.add_argument("--postgres", action="store_true")
    stream.add_argument("--lake", action="store_true", help="land Bronze micro-batches")
    stream.add_argument("--idle-exit", type=float, default=None, help="stop after N idle seconds")

    bench = sub.add_parser("bench", help="measure latency, capacity and recovery on the broker")
    bench.add_argument("--bootstrap", default=None)
    bench.add_argument("--rates", default="1000,2000,4000,8000")
    bench.add_argument("--recovery-rate", type=float, default=2000)
    bench.add_argument("--kill-after", type=float, default=6.0)
    bench.add_argument("--skip", default="", help="comma list of capacity,latency,recovery")
    bench.add_argument("--out", default=None)

    args = parser.parse_args(argv)
    cfg = load_config()

    if args.command == "demo":
        result = run_once(scenario=args.scenario, seed=args.seed, cfg=cfg,
                          backend=args.backend, invalid_rate=args.invalid_rate,
                          postgres=args.postgres)
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
            result = run_once(scenario=scenario, seed=args.seed, cfg=load_config())
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

    if args.command == "history":
        result = run_history(days=args.days, seed=args.seed, cfg=cfg, postgres=args.postgres)
        _print_report(result)
        return 0

    if args.command == "ml":
        from .ml.train import train_all
        report = train_all(cfg, Path(args.history_dir) if args.history_dir else None,
                           online_check=not args.no_online_check)
        _print_ml(report)
        return 0

    if args.command == "db":
        from .storage.operational import OperationalStore
        with OperationalStore.from_config(cfg) as store:
            if args.action == "migrate":
                print("applied", ", ".join(store.migrate()))
            else:
                print(json.dumps({"counts": store.counts(), "status": store.current_status()}, indent=2))
        return 0

    if args.command in ("produce", "stream"):
        from .sim.scenarios import ScenarioSettings
        from .sim.world import World
        if args.seed is not None:
            cfg["simulation"]["seed"] = args.seed
        settings = (ScenarioSettings.benchmark_mix() if args.scenario == "BENCHMARK_MIX"
                    else ScenarioSettings.for_scenario(args.scenario, invalid_rate=0.01))
        world = World(cfg, settings, seed=cfg.get_path("simulation.seed"))
        if args.command == "produce":
            from .bench import _produce
            from .runner import release_order
            print(json.dumps(_produce(cfg, release_order(world, cfg.get_path("kafka.topics")),
                                      args.rate), indent=2))
            return 0
        from .stream import StreamService
        checkpoints = cfg.lake_root.parent / "checkpoints"
        service = StreamService(cfg, world.master, checkpoint_path=checkpoints / "stream.pkl",
                                lake_dir=(cfg.lake_root.parent / "stream_lake") if args.lake else None,
                                postgres=args.postgres)
        result = service.run(idle_exit_seconds=args.idle_exit)
        result.pop("alert_ids", None)
        print(json.dumps(result, indent=2, default=str))
        return 0

    if args.command == "bench":
        from .bench import run_benchmark
        rates = tuple(float(r) for r in args.rates.split(",") if r)
        report = run_benchmark(args.bootstrap or cfg.get_path("kafka.bootstrap_servers"),
                               rates=rates, recovery_rate=args.recovery_rate,
                               kill_after=args.kill_after,
                               skip=tuple(s for s in args.skip.split(",") if s),
                               out=Path(args.out) if args.out else None)
        print(json.dumps(report, indent=2, default=str))
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
