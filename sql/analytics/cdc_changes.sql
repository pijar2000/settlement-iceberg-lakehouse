SELECT op, count(*) AS events, min(event_ts) AS first_event, max(event_ts) AS last_event
FROM silver.transaction_cdc_history GROUP BY op ORDER BY op
