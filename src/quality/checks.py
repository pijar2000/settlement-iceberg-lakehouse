"""Data quality checks on Silver/Gold + quarantine summary.

Cara pakai:
  python -m src.cli quality     # exit code 1 kalau ada check yang gagal
Rejected rows are stored in silver.rejected_records (written by `transform`).
"""
from __future__ import annotations

CHECKS = [
    ("silver.transactions", "unique_transaction_id",
     "SELECT count(*) - count(DISTINCT transaction_id) FROM silver.transactions"),
    ("silver.transactions", "non_positive_amount", "SELECT count(*) FROM silver.transactions WHERE amount <= 0"),
    ("silver.transactions", "null_reference", "SELECT count(*) FROM silver.transactions WHERE reference_no IS NULL"),
    ("silver.transactions", "invalid_status",
     "SELECT count(*) FROM silver.transactions WHERE status NOT IN ('SUCCESS','FAILED','PENDING')"),
    ("silver.partner_settlements", "unique_provider_reference",
     "SELECT count(*) - count(DISTINCT provider, reference_no) FROM silver.partner_settlements"),
    ("silver.partner_settlements", "non_positive_amount",
     "SELECT count(*) FROM silver.partner_settlements WHERE amount <= 0"),
    ("gold.reconciliation_results", "unclassified_rows",
     "SELECT count(*) FROM gold.reconciliation_results WHERE result IS NULL"),
]


def run_checks(spark) -> list[dict]:
    results = []
    for table, name, sql in CHECKS:
        try:
            failed = spark.sql(sql).first()[0]
            results.append({"table": table, "check": name, "failed_rows": failed, "passed": failed == 0})
        except Exception as exc:  # table may not exist yet
            results.append({"table": table, "check": name, "failed_rows": None, "passed": False,
                            "error": str(exc).splitlines()[0][:80]})
    print(f"{'table':<28} {'check':<28} {'failed':>8}  result")
    for r in results:
        print(f"{r['table']:<28} {r['check']:<28} {str(r['failed_rows']):>8}  {'PASS' if r['passed'] else 'FAIL'}")
    try:
        print("\nQuarantine (silver.rejected_records):")
        spark.sql("SELECT source_table, reason, count(*) AS rows FROM silver.rejected_records "
                  "GROUP BY source_table, reason ORDER BY source_table, rows DESC").show(truncate=False)
    except Exception:
        print("  (not available - run `transform` first)")
    return results
