-- Internal vs partner reconciliation. Params: {amount_tolerance}, {exclude_pending} (true/false)
CREATE OR REPLACE TABLE gold.reconciliation_results USING iceberg AS
SELECT
  coalesce(i.provider, p.provider)         AS provider,
  coalesce(i.reference_no, p.reference_no) AS reference_no,
  i.transaction_id,
  i.amount                                 AS internal_amount,
  p.amount                                 AS partner_amount,
  i.status                                 AS internal_status,
  p.status                                 AS partner_status,
  i.expected_settlement_date,
  p.settlement_date                        AS partner_settlement_date,
  datediff(p.settlement_date, i.expected_settlement_date) AS settlement_date_diff_days,
  CASE
    WHEN i.transaction_id IS NULL THEN 'MISSING_IN_INTERNAL'
    WHEN p.reference_no IS NULL THEN 'MISSING_IN_PARTNER'
    WHEN abs(i.amount - p.amount) > {amount_tolerance} THEN 'AMOUNT_MISMATCH'
    WHEN i.status <> p.status THEN 'STATUS_MISMATCH'
    ELSE 'MATCHED'
  END                                      AS result,
  current_timestamp()                      AS reconciled_at
FROM silver.transactions i
FULL OUTER JOIN silver.partner_settlements p
  ON i.provider = p.provider AND i.reference_no = p.reference_no
WHERE NOT ({exclude_pending} AND p.reference_no IS NULL AND i.status = 'PENDING')
