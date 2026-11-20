"""CDC: bronze.cdc_events -> silver.transaction_cdc_history -> MERGE INTO silver.transactions.

Cara pakai:
  python -m src.cli cdc
Per transaction only the latest event (highest lsn) is merged: I=insert, U=update, D=delete.
Re-running is safe: updates are idempotent, deletes of missing keys are no-ops, re-delivered inserts are ignored.
"""
from __future__ import annotations

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from src.utilities.spark import run_sql_file, timed


def apply_cdc(spark: SparkSession) -> dict:
    b = spark.table("bronze.cdc_events")
    typed = b.select(
        "event_id", F.col("lsn").cast("bigint").alias("lsn"), "op", F.col("event_ts").cast("timestamp").alias("event_ts"),
        "transaction_id", "merchant_id", "provider", "reference_no", F.col("amount").cast("decimal(18,2)").alias("amount"),
        "currency", "status", "source_file", "batch_id", "ingested_at")
    w = Window.partitionBy("event_id").orderBy(F.col("ingested_at").desc())
    history = typed.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn", "ingested_at")
    history.writeTo("silver.transaction_cdc_history").using("iceberg").tableProperty("format-version", "2").createOrReplace()

    w2 = Window.partitionBy("transaction_id").orderBy(F.col("lsn").desc())
    latest = history.withColumn("_rn", F.row_number().over(w2)).filter("_rn = 1").drop("_rn")
    latest.createOrReplaceTempView("cdc_latest")

    ops = {r["op"]: r["n"] for r in latest.groupBy("op").agg(F.count("*").alias("n")).collect()}
    before = spark.table("silver.transactions").count()
    with timed("cdc.merge", events=history.count(), latest_per_key=latest.count(), ops=ops) as m:
        run_sql_file(spark, "silver/merge_cdc.sql")
        after = spark.table("silver.transactions").count()
        m.update(rows_before=before, rows_after=after)
    return {"ops": ops, "rows_before": before, "rows_after": after}
