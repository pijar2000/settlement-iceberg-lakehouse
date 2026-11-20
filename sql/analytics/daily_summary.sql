SELECT settlement_date, provider, total_records, matched, amount_mismatch, status_mismatch, match_rate_pct
FROM gold.daily_settlement_summary ORDER BY settlement_date, provider LIMIT 10
