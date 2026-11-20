SELECT provider, total_transactions, success_rate_pct, match_rate_pct, missing_in_partner, missing_in_internal
FROM gold.provider_performance ORDER BY success_rate_pct DESC
