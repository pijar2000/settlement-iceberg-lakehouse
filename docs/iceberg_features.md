# Iceberg features

> Implemented in `src/iceberg_demo.py` (`python -m src.cli iceberg-demo`) on a throwaway table `demo.ledger`. Run successfully as part of the full pipeline.

| Step | What it shows | Spark SQL / procedure |
|---|---|---|
| snapshots | ACID commits, one snapshot each | `SELECT ... FROM demo.ledger.snapshots` |
| timetravel | read old state | `VERSION AS OF`, `TIMESTAMP AS OF` |
| merge | late correction + insert + delete | `MERGE INTO` |
| rollback | return to pre-merge snapshot | `CALL lake.system.rollback_to_snapshot` |
| schema | add 3 provider columns, files untouched | `ALTER TABLE ADD COLUMNS` |
| partition | `days(created_at)` then add `provider` | `ALTER TABLE ADD PARTITION FIELD` |
| compaction | merge small files | `CALL lake.system.rewrite_data_files` |
| files | data file inspection | `demo.ledger.files` |
| expire | drop old snapshots, time travel then fails | `CALL lake.system.expire_snapshots` |

Notes: rollback leaves later snapshots in history until expired; compaction keeps old files until expiry; time travel only works while snapshots are retained. `silver.transactions` is partitioned by `days(created_at)`.
