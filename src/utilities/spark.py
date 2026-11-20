"""Spark helpers (catalog settings live in config/spark-defaults.conf).

Cara pakai (di dalam container):
  from src.utilities.spark import get_spark, run_sql_file
  --SPARK_DRIVER_MEMORY (env) : memori driver, default 2g
"""
from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from pathlib import Path

SQL_DIR = Path(__file__).resolve().parents[2] / "sql"
METRICS_FILE = Path(os.environ.get("METRICS_FILE", "data/metrics/runs.jsonl"))


def get_spark(app: str = "settlement-lakehouse"):
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.appName(app)
             .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g")).getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def run_sql_file(spark, rel_path: str, params: dict | None = None):
    """Run a ;-separated SQL file under sql/. {name} placeholders are filled from params. Returns last DataFrame."""
    text = (SQL_DIR / rel_path).read_text(encoding="utf-8")
    for k, v in (params or {}).items():
        text = text.replace("{" + k + "}", str(v))
    df = None
    for stmt in (s.strip() for s in text.split(";")):
        if stmt and not all(line.strip().startswith("--") for line in stmt.splitlines() if line.strip()):
            df = spark.sql(stmt)
    return df


@contextmanager
def timed(step: str, **info):
    """Log step duration + info to stdout and data/metrics/runs.jsonl."""
    rec = {"step": step, **info}
    t0 = time.time()
    try:
        yield rec
    finally:
        rec["seconds"] = round(time.time() - t0, 2)
        print(f"[{step}] {json.dumps({k: v for k, v in rec.items() if k != 'step'}, default=str)}")
        try:
            METRICS_FILE.parent.mkdir(parents=True, exist_ok=True)
            with METRICS_FILE.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(rec, default=str) + "\n")
        except OSError:
            pass
