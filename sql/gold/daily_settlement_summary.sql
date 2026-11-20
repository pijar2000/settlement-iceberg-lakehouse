-- Daily reconciliation summary per provider
CREATE OR REPLACE TABLE gold.daily_settlement_summary USING iceberg AS
SELECT
  coalesce(expected_settlement_date, partner_settlement_date) AS settlement_date,
  provider,
  count(*)                                                        AS total_records,
  sum(CASE WHEN result = 'MATCHED' THEN 1 ELSE 0 END)             AS matched,
  sum(CASE WHEN result = 'AMOUNT_MISMATCH' THEN 1 ELSE 0 END)     AS amount_mismatch,
  sum(CASE WHEN result = 'STATUS_MISMATCH' THEN 1 ELSE 0 END)     AS status_mismatch,
  sum(CASE WHEN result = 'MISSING_IN_PARTNER' THEN 1 ELSE 0 END)  AS missing_in_partner,
  sum(CASE WHEN result = 'MISSING_IN_INTERNAL' THEN 1 ELSE 0 END) AS missing_in_internal,
  round(sum(CASE WHEN result = 'MATCHED' THEN 1 ELSE 0 END) * 100.0 / count(*), 2) AS match_rate_pct,
  sum(internal_amount)                                            AS internal_amount,
  sum(partner_amount)                                             AS partner_amount,
  coalesce(sum(internal_amount), 0) - coalesce(sum(partner_amount), 0) AS amount_difference
FROM gold.reconciliation_results
GROUP BY coalesce(expected_settlement_date, partner_settlement_date), provider
