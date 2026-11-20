# Enterprise Use Cases: AFFH IYH E-WALLET

> **AFFH IYH E-WALLET is fictional. All data is synthetic.** The scenarios describe logical processes. Unless stated, they have **not** been executed in this repository yet.

## Company profile
A medium-to-large payment gateway running its own Linux servers in a private data center. It has an internal PostgreSQL transaction database, receives daily settlement files from external providers, has a Finance team needing daily reconciliation and a Data Engineering team that owns history. Regulation and policy prevent moving transaction data to public cloud.

## Business challenges
1. Corrections arrive after initial settlement. 2. Provider schemas are inconsistent. 3. Duplicate files are occasionally delivered. 4. Records go missing on either side. 5. Finance needs auditability. 6. Several tools read the same data. 7. Historical reports must be reproducible. 8. Small Parquet files accumulate. 9. Volume grows. 10. The company wants full infrastructure control.

A file-based ETL can handle each of these with custom engineering. As they combine, the custom file/version logic grows in every job. Iceberg standardizes much of the table management, at the price of operating a catalog and maintenance jobs.

## 1. Late-arriving settlement corrections
A provider reports a transaction `PENDING`; two days later a correction says `SETTLED`.
1. Ingest the correction into Bronze (raw, with batch and source file).
2. Validate identity (provider + reference exists; else quarantine).
3. Treat it as a CDC update.
4. `MERGE INTO silver.transactions` on the key.
5. Each commit creates a snapshot, so the earlier state stays queryable.
6. Recompute Gold reconciliation.

With plain Parquet the team must rewrite the affected files, publish atomically and record the prior version itself.

```sql
MERGE INTO silver.transactions t USING staged_updates s ON t.transaction_id = s.transaction_id
WHEN MATCHED AND s.op = 'U' THEN UPDATE SET *
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN NOT MATCHED AND s.op = 'I' THEN INSERT *;
```
(Illustrative; the repository's actual CDC job is not implemented yet.)

## 2. Historical financial audit
Yesterday's reconciliation report differs from today's.
```sql
SELECT snapshot_id, committed_at, operation FROM gold.reconciliation_results.snapshots;
SELECT count(*) FROM gold.reconciliation_results TIMESTAMP AS OF '2025-02-01 00:00:00';
```
Compare the two states, find the commit that changed it, and roll back only if appropriate (`CALL system.rollback_to_snapshot(...)`). Time travel works **only while the snapshots and their data files are retained**; snapshot expiration removes them.

## 3. Provider schema evolution
A provider adds `settlement_reference`, `merchant_category`, `settlement_channel`.
```sql
ALTER TABLE silver.partner_settlements ADD COLUMNS (
  settlement_reference string, merchant_category string, settlement_channel string);
```
Old data files are not rewritten; old rows read as NULL for the new columns. In this repo the provider-side change would also need an update to `config/providers.yaml`.

## 4. Multi-engine analytics
Spark does ETL, Trino serves SQL, DuckDB supports local investigation, Superset or Power BI read results. One governed table avoids unmanaged copies drifting apart. Compatibility depends on engine and catalog versions, and the REST catalog must be supported by each engine. **In this repo only Spark is configured; Trino is not included and DuckDB is not yet verified.**

## 5. Small-file problem
Daily ingestion creates many small files; each adds open/metadata overhead for readers. Iceberg's manifests track files and `rewrite_data_files` compacts them, but compaction must be scheduled and monitored (and expired files cleaned up).

## 6. Failed pipeline execution
A job crashes mid-write. Written data files are not part of the table until the commit swaps the catalog's metadata pointer, so readers keep seeing the last committed snapshot, and orphan files can be cleaned later. This is not a promise that every failure self-heals: catalog outages, partial maintenance jobs and storage loss need operational planning and backups.
