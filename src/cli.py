"""Settlement Lakehouse CLI.

Cara pakai:
  python -m src.cli --help
  python -m src.cli generate --rows 300000 --cdc-rows 20000 --seed 42
  python -m src.cli demo --rows 50000 --cdc-rows 5000   # pipeline lengkap
  (jalankan di container: docker compose run --rm pipeline python -m src.cli <perintah>)
"""
from __future__ import annotations

import argparse
import json
import sys
import time

from scripts import generate_data


def cmd_generate(args: argparse.Namespace) -> int:
    t0 = time.time()
    print(f"[generate] rows={args.rows} cdc={args.cdc_rows} dup={args.duplicate_rows} seed={args.seed}")
    stats = generate_data.run(args)
    print(json.dumps(stats, indent=2))
    print(f"[generate] done in {time.time() - t0:.1f}s -> {args.output_dir}/{stats['batch_id']}")
    return 0


def cmd_ingest(args: argparse.Namespace) -> int:
    from pathlib import Path

    from src.ingestion.bronze import ingest_bronze, latest_batch
    from src.utilities.spark import get_spark

    batch = Path(args.raw_dir) / args.batch if args.batch else latest_batch(args.raw_dir)
    ingest_bronze(get_spark("bronze"), batch)
    return 0


def cmd_inspect(args: argparse.Namespace) -> int:
    from src.utilities.inspect_tables import inspect
    from src.utilities.spark import get_spark

    inspect(get_spark("inspect"), args.namespace)
    return 0


def _spark(name: str):
    from src.utilities.spark import get_spark

    return get_spark(name)


def cmd_transform(args: argparse.Namespace) -> int:
    from src.transformations.silver import run_silver

    run_silver(_spark("silver"))
    return 0


def cmd_cdc(args: argparse.Namespace) -> int:
    from src.cdc.apply import apply_cdc

    apply_cdc(_spark("cdc"))
    return 0


def cmd_reconcile(args: argparse.Namespace) -> int:
    from src.reconciliation.engine import run_reconciliation

    run_reconciliation(_spark("reconcile"))
    return 0


def cmd_quality(args: argparse.Namespace) -> int:
    from src.quality.checks import run_checks

    return 0 if all(r["passed"] for r in run_checks(_spark("quality"))) else 1


def cmd_iceberg_demo(args: argparse.Namespace) -> int:
    from src.iceberg_demo import run_demo

    run_demo(_spark("iceberg-demo"), args.step)
    return 0


SPARK_QUERIES = ["daily_summary", "provider_success", "reconciliation_breakdown", "cdc_changes",
                 "snapshot_history", "rejected_records"]


def cmd_analytics(args: argparse.Namespace) -> int:
    if args.engine == "duckdb":
        from src.analytics.duckdb_reader import run_duckdb_analytics

        try:
            run_duckdb_analytics()
        except Exception as exc:
            print(f"[analytics] DuckDB unavailable: {exc}", file=sys.stderr)
            return 1
        return 0
    from src.utilities.spark import run_sql_file

    spark = _spark("analytics")
    for q in SPARK_QUERIES:
        print(f"\n--- {q} (Spark SQL) ---")
        run_sql_file(spark, f"analytics/{q}.sql").show(truncate=False)
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    steps = [
        ("generate", cmd_generate), ("ingest", cmd_ingest), ("transform", cmd_transform), ("cdc", cmd_cdc),
        ("reconcile", cmd_reconcile), ("quality", cmd_quality), ("iceberg-demo", cmd_iceberg_demo),
        ("analytics (spark)", lambda a: cmd_analytics(argparse.Namespace(engine="spark"))),
        ("analytics (duckdb)", lambda a: cmd_analytics(argparse.Namespace(engine="duckdb"))),
        ("inspect", cmd_inspect)]
    results = []
    for name, fn in steps:
        print(f"\n########## {name} ##########")
        t0 = time.time()
        rc = fn(args)
        results.append((name, rc, time.time() - t0))
    print("\nSummary")
    for name, rc, sec in results:
        print(f"  {name:<20} {'ok' if rc == 0 else 'rc=' + str(rc):<8} {sec:6.1f}s")
    # DuckDB is a documented optional reader; other failures fail the demo.
    return 1 if any(rc for name, rc, _ in results if "duckdb" not in name) else 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="src.cli", description="Settlement Lakehouse pipeline")
    sub = ap.add_subparsers(dest="command", required=True)
    g = sub.add_parser("generate", help="generate synthetic raw data")
    generate_data.add_arguments(g)
    g.set_defaults(func=cmd_generate)
    i = sub.add_parser("ingest", help="raw CSV -> bronze Iceberg tables")
    i.add_argument("--raw-dir", default="data/raw")
    i.add_argument("--batch", default=None)
    i.set_defaults(func=cmd_ingest)
    n = sub.add_parser("inspect", help="list tables, snapshots, files")
    n.add_argument("--namespace", default=None)
    n.set_defaults(func=cmd_inspect)
    sub.add_parser("transform", help="bronze -> silver").set_defaults(func=cmd_transform)
    sub.add_parser("cdc", help="apply CDC events with MERGE INTO").set_defaults(func=cmd_cdc)
    sub.add_parser("reconcile", help="build gold tables").set_defaults(func=cmd_reconcile)
    sub.add_parser("quality", help="run data quality checks").set_defaults(func=cmd_quality)
    d = sub.add_parser("iceberg-demo", help="snapshots, time travel, rollback, evolution, compaction")
    d.add_argument("--step", default="all", choices=["all", *__import__("src.iceberg_demo", fromlist=["STEPS"]).STEPS])
    d.set_defaults(func=cmd_iceberg_demo)
    an = sub.add_parser("analytics", help="sample analytical queries")
    an.add_argument("--engine", choices=["spark", "duckdb"], default="spark")
    an.set_defaults(func=cmd_analytics)
    dm = sub.add_parser("demo", help="run the whole pipeline end to end")
    generate_data.add_arguments(dm)
    dm.add_argument("--raw-dir", default="data/raw")
    dm.add_argument("--batch", default=None)
    dm.add_argument("--namespace", default=None)
    dm.add_argument("--step", default="all")
    dm.set_defaults(func=cmd_demo)
    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
