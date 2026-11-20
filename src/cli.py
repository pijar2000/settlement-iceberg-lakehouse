"""Settlement Lakehouse CLI.

Cara pakai:
  python -m src.cli --help
  python -m src.cli generate --rows 300000 --cdc-rows 20000 --seed 42
  (jalankan di container: docker compose run --rm pipeline python -m src.cli <perintah>)
"""
from __future__ import annotations

import argparse
import json
import sys
import time

from scripts import generate_data

PLANNED = ["transform", "cdc", "reconcile", "quality", "iceberg-demo", "analytics", "demo"]


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


def cmd_planned(args: argparse.Namespace) -> int:
    print(f"[{args.command}] belum diimplementasi (Phase 3+).", file=sys.stderr)
    return 2


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
    for name in PLANNED:
        sub.add_parser(name, help="(planned)").set_defaults(func=cmd_planned)
    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
