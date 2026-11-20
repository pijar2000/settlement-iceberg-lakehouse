"""Bronze ingestion: raw CSV -> Iceberg, all columns kept as strings + ingestion metadata.

Cara pakai:
  python -m src.cli ingest --raw-dir data/raw [--batch batch-seed42]
  --raw-dir : folder hasil generate (default data/raw)
  --batch   : nama batch; default = batch terbaru
Partner schemas differ, so bronze.partner_settlements keeps the raw columns in a `payload` map.
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utilities.providers import load_providers
from src.utilities.spark import timed


def latest_batch(raw_dir: str | Path) -> Path:
    batches = sorted((p for p in Path(raw_dir).iterdir() if p.is_dir()), key=lambda p: p.stat().st_mtime)
    if not batches:
        raise SystemExit(f"no batches in {raw_dir}; run `generate` first")
    return batches[-1]


def _read(spark: SparkSession, path: Path, delimiter: str = ",") -> DataFrame:
    return (spark.read.option("header", True).option("delimiter", delimiter)
            .csv(f"file://{path.resolve().as_posix()}/*.csv"))


def _meta(df: DataFrame) -> DataFrame:
    return df.withColumn("ingested_at", F.current_timestamp())


def _write(df: DataFrame, table: str) -> int:
    df.writeTo(table).using("iceberg").tableProperty("format-version", "2").createOrReplace()
    return df.sparkSession.table(table).count()


def ingest_bronze(spark: SparkSession, batch_dir: Path) -> dict:
    for ns in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")
    counts = {}

    with timed("bronze.internal_transactions", batch=batch_dir.name) as m:
        df = _read(spark, batch_dir / "internal_transactions")
        m["input_rows"] = df.count()
        m["output_rows"] = counts["internal_transactions"] = _write(_meta(df), "bronze.internal_transactions")

    with timed("bronze.cdc_events", batch=batch_dir.name) as m:
        df = _read(spark, batch_dir / "cdc_events")
        m["input_rows"] = df.count()
        m["output_rows"] = counts["cdc_events"] = _write(_meta(df), "bronze.cdc_events")

    with timed("bronze.partner_settlements", batch=batch_dir.name) as m:
        parts = []
        for name, cfg in load_providers().items():
            df = _read(spark, batch_dir / "partner_settlements" / name, cfg["delimiter"])
            raw_cols = [c for c in df.columns if c not in ("source_file", "batch_id")]
            payload = F.create_map(*[x for c in raw_cols for x in (F.lit(c), F.col(c))])
            parts.append(df.select(F.lit(name).alias("provider"), payload.alias("payload"), "source_file", "batch_id"))
        union = parts[0]
        for p in parts[1:]:
            union = union.unionByName(p)
        m["input_rows"] = union.count()
        m["output_rows"] = counts["partner_settlements"] = _write(_meta(union), "bronze.partner_settlements")
    return counts
