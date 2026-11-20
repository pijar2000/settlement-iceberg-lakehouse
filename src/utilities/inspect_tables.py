"""Inspect Iceberg tables: row count, snapshots, data files, size.

Cara pakai:
  python -m src.cli inspect [--namespace bronze]   # namespace opsional; default semua
"""
from __future__ import annotations


def inspect(spark, namespace: str | None = None) -> list[dict]:
    spaces = [namespace] if namespace else [r[0] for r in spark.sql("SHOW NAMESPACES").collect()]
    out = []
    for ns in spaces:
        for t in spark.sql(f"SHOW TABLES IN {ns}").collect():
            name = f"{ns}.{t['tableName']}"
            files = spark.sql(f"SELECT count(*) n, coalesce(sum(file_size_in_bytes),0) b FROM {name}.files").first()
            out.append({"table": name, "rows": spark.table(name).count(),
                        "snapshots": spark.sql(f"SELECT count(*) FROM {name}.snapshots").first()[0],
                        "data_files": files["n"], "size_mb": round(files["b"] / 1048576, 2)})
    width = max((len(r["table"]) for r in out), default=10)
    print(f"{'table':<{width}} {'rows':>10} {'snaps':>6} {'files':>6} {'MB':>8}")
    for r in out:
        print(f"{r['table']:<{width}} {r['rows']:>10} {r['snapshots']:>6} {r['data_files']:>6} {r['size_mb']:>8}")
    return out
