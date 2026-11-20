"""Iceberg feature demo on a throwaway table demo.ledger (does not touch silver/gold).

Cara pakai:
  python -m src.cli iceberg-demo [--step all|snapshots|timetravel|merge|rollback|schema|partition|compaction|files|expire]
  --step : default all. Satu langkah saja butuh `all` dijalankan dulu (membuat demo.ledger).
Steps: ACID snapshots, time travel, MERGE, rollback, schema evolution, partition evolution,
       compaction, data-file inspection, snapshot expiration.
"""
from __future__ import annotations

from datetime import datetime, timedelta

STEPS = ["snapshots", "timetravel", "merge", "rollback", "schema", "partition", "compaction", "files", "expire"]
T = "demo.ledger"
CALL = "CALL lake.system"
STATE: dict = {}


def _h(title: str) -> None:
    print(f"\n=== {title} ===")


def _snapshots(spark):
    return spark.sql(f"SELECT snapshot_id, committed_at, operation, summary['added-records'] AS added "
                     f"FROM {T}.snapshots ORDER BY committed_at").collect()


def _n_files(spark) -> int:
    return spark.sql(f"SELECT count(*) FROM {T}.files").first()[0]


def setup(spark):
    _h("setup: create table + 3 separate commits (ACID, one snapshot each)")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo")
    spark.sql(f"DROP TABLE IF EXISTS {T} PURGE")
    spark.sql(f"CREATE TABLE {T} (id bigint, provider string, amount decimal(18,2), status string, created_at timestamp) "
              f"USING iceberg PARTITIONED BY (days(created_at)) TBLPROPERTIES ('format-version'='2')")
    for i, rows in enumerate([
        "(1,'OVO',100000,'PENDING',TIMESTAMP '2025-01-01 10:00:00'),(2,'DANA',250000,'SUCCESS',TIMESTAMP '2025-01-01 11:00:00')",
        "(3,'GOPAY',75000,'PENDING',TIMESTAMP '2025-01-02 09:00:00')",
        "(4,'LINKAJA',500000,'SUCCESS',TIMESTAMP '2025-01-02 12:00:00')"], 1):
        spark.sql(f"INSERT INTO {T} VALUES {rows}")
        print(f"commit {i} done")


def step_snapshots(spark):
    _h("1. snapshot inspection")
    spark.sql(f"SELECT snapshot_id, committed_at, operation, summary['added-records'] AS added_records "
              f"FROM {T}.snapshots ORDER BY committed_at").show(truncate=False)
    spark.sql(f"SELECT * FROM {T}.history").show(truncate=False)


def step_timetravel(spark):
    _h("2. time travel (VERSION AS OF / TIMESTAMP AS OF)")
    snaps = _snapshots(spark)
    first = snaps[0]
    print(f"current rows: {spark.table(T).count()}")
    n_ver = spark.sql(f"SELECT * FROM {T} VERSION AS OF {first['snapshot_id']}").count()
    print(f"rows at first snapshot ({first['snapshot_id']}): {n_ver}")
    ts = first["committed_at"].strftime("%Y-%m-%d %H:%M:%S.%f")
    print(f"rows TIMESTAMP AS OF '{ts}': {spark.sql(f'SELECT * FROM {T} TIMESTAMP AS OF {chr(39)}{ts}{chr(39)}').count()}")


def step_merge(spark):
    _h("3. MERGE INTO (late correction: PENDING -> SUCCESS, new row, delete)")
    STATE["pre_merge"] = _snapshots(spark)[-1]["snapshot_id"]
    spark.createDataFrame([(1, "OVO", 100000, "SUCCESS", "U"), (5, "FINNET", 90000, "SUCCESS", "I"),
                           (3, None, None, None, "D")],
                          "id long, provider string, amount long, status string, op string").createOrReplaceTempView("chg")
    spark.sql(f"""MERGE INTO {T} t USING chg s ON t.id = s.id
      WHEN MATCHED AND s.op = 'D' THEN DELETE
      WHEN MATCHED AND s.op = 'U' THEN UPDATE SET t.status = s.status
      WHEN NOT MATCHED AND s.op = 'I' THEN INSERT (id, provider, amount, status, created_at)
        VALUES (s.id, s.provider, s.amount, s.status, TIMESTAMP '2025-01-03 08:00:00')""")
    spark.sql(f"SELECT * FROM {T} ORDER BY id").show()


def step_rollback(spark):
    _h("4. rollback to the snapshot before the MERGE")
    target = STATE.get("pre_merge") or _snapshots(spark)[-2]["snapshot_id"]
    spark.sql(f"{CALL}.rollback_to_snapshot('{T}', {target})")
    print(f"rolled back to {target} (later snapshots stay in history until expired)")
    spark.sql(f"SELECT * FROM {T} ORDER BY id").show()


def step_schema(spark):
    _h("5. schema evolution (metadata only, old files untouched)")
    before = _n_files(spark)
    spark.sql(f"ALTER TABLE {T} ADD COLUMNS (settlement_reference string, merchant_category string, settlement_channel string)")
    print(f"data files before/after ALTER: {before}/{_n_files(spark)}")
    spark.sql(f"INSERT INTO {T} VALUES (6,'OVO',120000,'SUCCESS',TIMESTAMP '2025-01-03 10:00:00','SR-001','FOOD','APP')")
    spark.sql(f"SELECT * FROM {T} ORDER BY id").show(truncate=False)


def step_partition(spark):
    _h("6. partition evolution: days(created_at) -> + provider")
    spark.sql(f"ALTER TABLE {T} ADD PARTITION FIELD provider")
    spark.sql(f"INSERT INTO {T} VALUES (7,'DANA',60000,'SUCCESS',TIMESTAMP '2025-01-04 10:00:00',NULL,NULL,NULL),"
              f"(8,'GOPAY',45000,'FAILED',TIMESTAMP '2025-01-04 11:00:00',NULL,NULL,NULL)")
    spark.sql(f"SELECT spec_id, count(*) AS files, sum(record_count) AS records FROM {T}.files GROUP BY spec_id ORDER BY spec_id").show()


def step_compaction(spark):
    _h("7. compaction (rewrite_data_files)")
    for i in range(10, 18):
        spark.sql(f"INSERT INTO {T} VALUES ({i},'OVO',1000,'SUCCESS',TIMESTAMP '2025-01-05 10:00:00',NULL,NULL,NULL)")
    before = _n_files(spark)
    spark.sql(f"{CALL}.rewrite_data_files(table => '{T}', options => map('min-input-files','2'))").show(truncate=False)
    print(f"data files before/after compaction: {before}/{_n_files(spark)}  (old files remain until snapshots expire)")


def step_files(spark):
    _h("8. data file inspection")
    spark.sql(f"SELECT file_path, file_format, record_count, file_size_in_bytes, spec_id FROM {T}.files "
              f"ORDER BY file_path").show(10, truncate=60)


def step_expire(spark):
    _h("9. snapshot expiration (keeps last 1) - time travel to old snapshots is lost")
    snaps = _snapshots(spark)
    oldest = snaps[0]["snapshot_id"]
    cutoff = (datetime.now() + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    print(f"snapshots before: {len(snaps)}")
    spark.sql(f"{CALL}.expire_snapshots(table => '{T}', older_than => TIMESTAMP '{cutoff}', retain_last => 1)").show(truncate=False)
    print(f"snapshots after: {len(_snapshots(spark))}")
    try:
        spark.sql(f"SELECT * FROM {T} VERSION AS OF {oldest}").count()
        print("old snapshot still readable (unexpected)")
    except Exception as exc:
        print(f"time travel to expired snapshot {oldest} fails as expected: {str(exc).splitlines()[0][:100]}")


FUNCS = {"snapshots": step_snapshots, "timetravel": step_timetravel, "merge": step_merge, "rollback": step_rollback,
         "schema": step_schema, "partition": step_partition, "compaction": step_compaction, "files": step_files,
         "expire": step_expire}


def run_demo(spark, step: str = "all") -> None:
    if step == "all":
        setup(spark)
        for s in STEPS:
            FUNCS[s](spark)
    else:
        FUNCS[step](spark)
