-- Provider scorecard: transaction outcomes (silver) + reconciliation quality (gold)
CREATE OR REPLACE TABLE gold.provider_performance USING iceberg AS
WITH t AS (
  SELECT provider,
         count(*)                                             AS total_transactions,
         sum(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END)  AS success_count,
         sum(CASE WHEN status = 'FAILED'  THEN 1 ELSE 0 END)  AS failed_count,
         sum(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END)  AS pending_count,
         sum(amount)                                          AS gross_amount,
         round(avg(amount), 2)                                AS avg_amount
  FROM silver.transactions GROUP BY provider
), r AS (
  SELECT provider,
         count(*)                                                        AS recon_records,
         round(sum(CASE WHEN result = 'MATCHED' THEN 1 ELSE 0 END) * 100.0 / count(*), 2) AS match_rate_pct,
         sum(CASE WHEN result = 'MISSING_IN_PARTNER' THEN 1 ELSE 0 END)  AS missing_in_partner,
         sum(CASE WHEN result = 'MISSING_IN_INTERNAL' THEN 1 ELSE 0 END) AS missing_in_internal
  FROM gold.reconciliation_results GROUP BY provider
)
SELECT t.*, round(t.success_count * 100.0 / t.total_transactions, 2) AS success_rate_pct,
       r.recon_records, r.match_rate_pct, r.missing_in_partner, r.missing_in_internal
FROM t LEFT JOIN r ON t.provider = r.provider
