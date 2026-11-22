# Architecture

```mermaid
flowchart LR
    GEN[Python generator] --> RAW[(Raw CSV)]
    RAW -->|Spark| BR[Bronze]
    BR --> SI[Silver<br/>dedup · CDC · quality]
    SI --> GO[Gold<br/>summaries · reconciliation]
    BR & SI & GO --- ICE[(Iceberg tables)]
    ICE --- MINIO[(MinIO s3://warehouse)]
    CAT[REST Catalog] -. metadata .- ICE
    SPARK[Spark SQL] --> ICE
    DUCK[DuckDB reader] --> ICE
```

| Component | Image / library | Role | RAM limit |
|---|---|---|---|
| MinIO | built from source (`docker/minio`) | S3-compatible object store | 512 MB |
| REST Catalog | `apache/iceberg-rest-fixture:1.9.2` | Table registration (SQLite-backed, persisted in a volume) | 512 MB |
| Pipeline | custom image: Python 3.11, Java 17, PySpark 3.5.5, Iceberg 1.9.2 jars, DuckDB | Spark local-mode writer, CLI | 3 GB |

Spark runs in local mode inside the `pipeline` container, so there is no separate Spark service. `docker/spark/` is reserved for a future standalone cluster profile.

**Status:** MinIO and the REST catalog were verified healthy on Docker; the Spark/Iceberg pipeline stages have been run end to end successfully. MinIO is built from source (`docker/minio`).

DuckDB reading Iceberg tables via the REST catalog has been verified.
