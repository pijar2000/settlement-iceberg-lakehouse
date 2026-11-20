# Architecture Decisions and Trade-offs

1. **Iceberg vs plain Parquet management.** Iceberg standardizes snapshots, atomic commits, schema/partition evolution and file tracking so fewer custom mechanisms are needed. Cost: a catalog, metadata growth, maintenance jobs, engine-compatibility checks.
2. **Spark vs Python-only.** Spark scales out and has mature Iceberg integration (`MERGE INTO`, maintenance procedures). Python-only is simpler for small data. Here Spark runs in local mode on one laptop, so scale-out is not demonstrated.
3. **MinIO vs public cloud storage.** Data must stay in the private data center. Trade-off: you operate storage durability, upgrades and licensing yourself (Ceph/HDFS are alternatives).
4. **REST catalog.** A common protocol decouples engines from catalog internals. The fixture used here is for testing; production needs a maintained implementation, with its own database, HA and backups.
5. **Trino in addition to Spark.** Spark for batch ETL; Trino for interactive SQL and BI. Not included in this repo.
6. **DuckDB.** Handy for local investigations on a single machine. Iceberg/REST support depends on the DuckDB version and extension; **not yet verified here**.
7. **When this is unnecessary.** Small append-only data, one engine, no audit or correction needs.
8. **Operational cost.** Catalog operations, compaction, snapshot expiration, orphan-file cleanup, monitoring, upgrades.
9. **Limitations of this implementation.** Single host, local-mode Spark, synthetic data, demo credentials, REST fixture catalog, no Airflow/Trino, and the whole Spark pipeline (Bronze to Gold, CDC, Iceberg demo) is written but has never been executed.
10. **Alternatives.** Delta Lake or Apache Hudi (table formats); Hive Metastore, Nessie or Polaris-style catalogs; HDFS or Ceph for storage; a warehouse database for small volumes.

## Decision guide
| Situation | Guidance |
|---|---|
| Small append-only workloads | Parquet may be sufficient |
| Single-engine analytics | A simpler architecture may be preferable |
| Multi-engine analytical platform | Iceberg becomes more valuable |
| Frequent corrections and CDC | Strong candidate |
| Snapshots and audit requirements | Useful |
| Multi-team enterprise platform | May provide significant operational value |

Iceberg is not the right answer for every organization, and it does not make queries faster by itself.
