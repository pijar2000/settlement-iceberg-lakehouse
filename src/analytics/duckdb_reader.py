"""DuckDB as a secondary, read-only analytical engine over the Gold Iceberg tables.

Cara pakai:
  python -m src.cli analytics --engine duckdb
Compatibility is verified. Two access paths are tried, in order:
  1. ATTACH to the REST catalog via DuckDB's iceberg extension.
  2. Fetch the table's metadata location from the REST API and use iceberg_scan().
Needs network access to install the `iceberg`/`httpfs` extensions on first use.
"""
from __future__ import annotations

import json
import os
from urllib.request import urlopen

QUERIES = {
    "provider_performance": "SELECT provider, total_transactions, success_rate_pct, match_rate_pct "
                            "FROM {provider_performance} ORDER BY provider",
    "reconciliation_breakdown": "SELECT result, count(*) AS records FROM {reconciliation_results} "
                                "GROUP BY result ORDER BY records DESC",
    "daily_summary_top": "SELECT settlement_date, provider, total_records, match_rate_pct "
                         "FROM {daily_settlement_summary} ORDER BY settlement_date, provider LIMIT 10",
}
TABLES = {"provider_performance": "gold.provider_performance", "reconciliation_results": "gold.reconciliation_results",
          "daily_settlement_summary": "gold.daily_settlement_summary"}


def _q(v: str) -> str:
    return v.replace("'", "''")


def connect():
    """Return (connection, mode, resolver) where resolver(table) is a SQL relation expression."""
    import duckdb

    catalog = os.environ.get("CATALOG_URI", "http://catalog:8181")
    endpoint = os.environ.get("S3_ENDPOINT", "http://minio:9000").replace("http://", "").replace("https://", "")
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;")
    con.execute(f"CREATE OR REPLACE SECRET minio (TYPE S3, KEY_ID '{_q(os.environ['AWS_ACCESS_KEY_ID'])}', "
                f"SECRET '{_q(os.environ['AWS_SECRET_ACCESS_KEY'])}', ENDPOINT '{endpoint}', URL_STYLE 'path', "
                f"USE_SSL false, REGION '{os.environ.get('AWS_REGION', 'us-east-1')}')")
    errors = []
    try:
        con.execute(f"ATTACH 'warehouse' AS lake (TYPE iceberg, ENDPOINT '{catalog}', AUTHORIZATION_TYPE 'none')")
        con.execute("SELECT 1 FROM lake.gold.provider_performance LIMIT 1").fetchall()
        return con, "ATTACH (REST catalog)", lambda t: f"lake.{t}"
    except Exception as exc:
        errors.append(f"attach: {str(exc).splitlines()[0]}")

    def metadata_location(table: str) -> str:
        ns, name = table.split(".")
        with urlopen(f"{catalog}/v1/namespaces/{ns}/tables/{name}") as r:
            return json.load(r)["metadata-location"]

    try:
        resolver = lambda t: f"iceberg_scan('{metadata_location(t)}')"  # noqa: E731
        con.execute(f"SELECT 1 FROM {resolver('gold.provider_performance')} LIMIT 1").fetchall()
        return con, "iceberg_scan (metadata location from REST API)", resolver
    except Exception as exc:
        errors.append(f"iceberg_scan: {str(exc).splitlines()[0]}")
    raise RuntimeError("DuckDB could not read the Iceberg tables. " + " | ".join(errors))


def run_duckdb_analytics() -> None:
    con, mode, resolver = connect()
    print(f"[duckdb] access mode: {mode}")
    names = {k: resolver(v) for k, v in TABLES.items()}
    for title, q in QUERIES.items():
        print(f"\n--- {title} (DuckDB) ---")
        con.sql(q.format(**names)).show()
