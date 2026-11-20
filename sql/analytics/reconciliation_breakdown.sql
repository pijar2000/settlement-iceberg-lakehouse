SELECT result, count(*) AS records, round(count(*) * 100.0 / sum(count(*)) OVER (), 2) AS pct
FROM gold.reconciliation_results GROUP BY result ORDER BY records DESC
