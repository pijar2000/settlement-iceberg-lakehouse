"""Integration test: needs the Docker stack. Run: docker compose run --rm pipeline pytest -m integration -p no:cacheprovider

Executes the pipeline on a small dataset and checks table contents. NOT run in CI.
"""
import pytest

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from scripts.generate_data import generate
    from src.cdc.apply import apply_cdc
    from src.ingestion.bronze import ingest_bronze
    from src.reconciliation.engine import run_reconciliation
    from src.transformations.silver import run_silver
    from src.utilities.spark import get_spark

    raw = tmp_path_factory.mktemp("raw")
    stats = generate(rows=3000, cdc_rows=300, duplicate_rows=60, chunk_size=1000, seed=1, output_dir=raw)
    s = get_spark("it")
    ingest_bronze(s, raw / stats["batch_id"])
    run_silver(s)
    apply_cdc(s)
    run_reconciliation(s)
    return s


def test_silver_has_no_duplicates(spark):
    assert spark.sql("SELECT count(*) - count(DISTINCT transaction_id) FROM silver.transactions").first()[0] == 0


def test_cdc_deletes_removed(spark):
    deleted = spark.sql("SELECT count(*) FROM silver.transaction_cdc_history WHERE op='D'").first()[0]
    assert deleted > 0
    assert spark.sql("""SELECT count(*) FROM silver.transactions t JOIN
        (SELECT transaction_id, max_by(op, lsn) op FROM silver.transaction_cdc_history GROUP BY transaction_id) h
        ON t.transaction_id = h.transaction_id WHERE h.op = 'D'""").first()[0] == 0


def test_reconciliation_classes(spark):
    got = {r[0] for r in spark.sql("SELECT DISTINCT result FROM gold.reconciliation_results").collect()}
    assert {"MATCHED", "AMOUNT_MISMATCH", "STATUS_MISMATCH", "MISSING_IN_INTERNAL"} <= got


def test_iceberg_snapshots_exist(spark):
    assert spark.sql("SELECT count(*) FROM silver.transactions.snapshots").first()[0] >= 2
