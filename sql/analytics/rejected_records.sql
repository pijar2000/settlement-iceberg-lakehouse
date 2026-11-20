SELECT source_table, reason, count(*) AS rows FROM silver.rejected_records GROUP BY source_table, reason ORDER BY rows DESC
