-- Apply the latest CDC event per transaction to silver.transactions (temp view: cdc_latest)
MERGE INTO silver.transactions t
USING cdc_latest s
ON t.transaction_id = s.transaction_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED AND s.op = 'U' THEN UPDATE SET
  t.amount = s.amount, t.status = s.status, t.updated_at = s.event_ts, t.batch_id = s.batch_id
WHEN NOT MATCHED AND s.op = 'I' THEN INSERT
  (transaction_id, merchant_id, provider, reference_no, amount, currency, status,
   created_at, updated_at, expected_settlement_date, batch_id)
  VALUES (s.transaction_id, s.merchant_id, s.provider, s.reference_no, s.amount, s.currency, s.status,
   s.event_ts, s.event_ts, date_add(to_date(s.event_ts), 1), s.batch_id)
