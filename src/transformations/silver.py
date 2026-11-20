"""Bronze -> Silver: typing, provider mapping (providers.yaml), validation, quarantine, dedup.

Cara pakai:
  python -m src.cli transform
Output: silver.transactions, silver.partner_settlements, silver.rejected_records
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.utilities.providers import load_providers, strptime_to_spark
from src.utilities.spark import timed

STATUSES = ["SUCCESS", "FAILED", "PENDING"]
REJECT_COLS = ["source_table", "reason", "record_key", "raw", "batch_id", "source_file", "rejected_at"]


def _dedup(df: DataFrame, keys: list[str]) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(F.col("ingested_at").desc(), F.col("source_file").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def _rejects(df: DataFrame, table: str, key: str) -> DataFrame:
    return df.filter(F.col("reason").isNotNull()).select(
        F.lit(table).alias("source_table"), "reason", F.col(key).cast("string").alias("record_key"),
        F.col("_raw").alias("raw"), "batch_id", "source_file", F.current_timestamp().alias("rejected_at"))


def _write(df: DataFrame, table: str, partition=None) -> None:
    w = df.writeTo(table).using("iceberg").tableProperty("format-version", "2")
    (w.partitionedBy(partition) if partition is not None else w).createOrReplace()


def transform_transactions(spark: SparkSession) -> tuple[DataFrame, dict]:
    b = spark.table("bronze.internal_transactions")
    typed = b.select(
        "transaction_id", "merchant_id", "provider", "reference_no",
        F.col("amount").cast("decimal(18,2)").alias("amount"), "currency", "status",
        F.col("created_at").cast("timestamp").alias("created_at"),
        F.col("updated_at").cast("timestamp").alias("updated_at"),
        F.col("expected_settlement_date").cast("date").alias("expected_settlement_date"),
        "source_file", "batch_id", "ingested_at", F.to_json(F.struct(*b.columns)).alias("_raw"))
    reason = (F.when(F.col("transaction_id").isNull(), "missing_transaction_id")
              .when(F.col("reference_no").isNull() | (F.trim("reference_no") == ""), "missing_reference_no")
              .when(F.col("amount").isNull() | (F.col("amount") <= 0), "invalid_amount")
              .when(~F.col("status").isin(STATUSES) | F.col("status").isNull(), "invalid_status")
              .when(F.col("created_at").isNull(), "invalid_created_at"))
    flagged = typed.withColumn("reason", reason).cache()
    valid = flagged.filter(F.col("reason").isNull())
    deduped = _dedup(valid, ["transaction_id"])
    stats = {"input_rows": flagged.count(), "valid_rows": valid.count(), "duplicates_removed": 0}
    out = deduped.drop("reason", "_raw", "source_file", "ingested_at")
    stats["output_rows"] = out.count()
    stats["duplicates_removed"] = stats["valid_rows"] - stats["output_rows"]
    stats["rejected_rows"] = stats["input_rows"] - stats["valid_rows"]
    _write(out, "silver.transactions", F.days("created_at"))
    return _rejects(flagged, "bronze.internal_transactions", "transaction_id"), stats


def partner_columns(cfg: dict) -> dict:
    """Spark column expressions for one provider, built purely from its YAML metadata."""
    c, p = cfg["columns"], F.col("payload")
    status_map = F.create_map(*[x for k, v in cfg["status_map"].items() for x in (F.lit(str(k)), F.lit(v))])
    return {
        "reference_no": F.trim(p[c["reference_no"]]),
        "amount": (p[c["amount"]].cast("decimal(18,2)") / F.lit(cfg["amount_scale"])).cast("decimal(18,2)"),
        "status": status_map[p[c["status"]]],
        "settlement_date": F.to_date(p[c["settlement_date"]], strptime_to_spark(cfg["date_format"])),
    }


def transform_partner(spark: SparkSession) -> tuple[DataFrame, dict]:
    bronze = spark.table("bronze.partner_settlements")
    parts = []
    for name, cfg in load_providers().items():
        cols = partner_columns(cfg)
        parts.append(bronze.filter(F.col("provider") == name).select(
            "provider", *[e.alias(k) for k, e in cols.items()], "source_file", "batch_id", "ingested_at",
            F.to_json(F.struct("provider", "payload")).alias("_raw")))
    typed = parts[0]
    for p in parts[1:]:
        typed = typed.unionByName(p)
    reason = (F.when(F.col("reference_no").isNull() | (F.col("reference_no") == ""), "missing_reference_no")
              .when(F.col("amount").isNull() | (F.col("amount") <= 0), "invalid_amount")
              .when(F.col("status").isNull(), "invalid_status")
              .when(F.col("settlement_date").isNull(), "invalid_settlement_date"))
    flagged = typed.withColumn("reason", reason).cache()
    valid = flagged.filter(F.col("reason").isNull())
    out = _dedup(valid, ["provider", "reference_no"]).drop("reason", "_raw", "ingested_at")
    stats = {"input_rows": flagged.count(), "valid_rows": valid.count(), "output_rows": out.count()}
    stats["duplicates_removed"] = stats["valid_rows"] - stats["output_rows"]
    stats["rejected_rows"] = stats["input_rows"] - stats["valid_rows"]
    _write(out, "silver.partner_settlements")
    return _rejects(flagged, "bronze.partner_settlements", "reference_no"), stats


def run_silver(spark: SparkSession) -> None:
    with timed("silver.transactions") as m:
        rej_t, stats = transform_transactions(spark)
        m.update(stats)
    with timed("silver.partner_settlements") as m:
        rej_p, stats = transform_partner(spark)
        m.update(stats)
    with timed("silver.rejected_records") as m:
        rejected = rej_t.unionByName(rej_p).select(*REJECT_COLS)
        _write(rejected, "silver.rejected_records")
        m["output_rows"] = spark.table("silver.rejected_records").count()
