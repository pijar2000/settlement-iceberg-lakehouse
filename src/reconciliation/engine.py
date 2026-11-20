"""Reconciliation + Gold tables.

Cara pakai:
  python -m src.cli reconcile
Settings: config/reconciliation.yaml (amount_tolerance, ignore_pending_without_partner).
Output: gold.reconciliation_results, gold.daily_settlement_summary, gold.provider_performance
"""
from __future__ import annotations

from pathlib import Path

import yaml

from src.utilities.spark import run_sql_file, timed

CONFIG = Path(__file__).resolve().parents[2] / "config" / "reconciliation.yaml"


def load_settings(path: Path = CONFIG) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def sql_params(settings: dict) -> dict:
    return {"amount_tolerance": settings.get("amount_tolerance", 0),
            "exclude_pending": str(bool(settings.get("ignore_pending_without_partner", True))).lower()}


def run_reconciliation(spark) -> dict:
    params = sql_params(load_settings())
    with timed("gold.reconciliation_results", **params) as m:
        run_sql_file(spark, "gold/reconciliation.sql", params)
        rows = spark.sql("SELECT result, count(*) n FROM gold.reconciliation_results GROUP BY result").collect()
        m["result_counts"] = {r["result"]: r["n"] for r in rows}
    for name in ("daily_settlement_summary", "provider_performance"):
        with timed(f"gold.{name}") as m:
            run_sql_file(spark, f"gold/{name}.sql")
            m["output_rows"] = spark.table(f"gold.{name}").count()
    return m
