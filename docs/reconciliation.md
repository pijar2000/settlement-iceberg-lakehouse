# Reconciliation

> Implemented in `sql/gold/reconciliation.sql` + `src/reconciliation/engine.py`. **Not yet executed.**

Full outer join of `silver.transactions` and `silver.partner_settlements` on `provider` + `reference_no`. First matching rule wins:

| Result | Rule |
|---|---|
| MISSING_IN_INTERNAL | no internal row |
| MISSING_IN_PARTNER | no partner row (internal `PENDING` excluded by default) |
| AMOUNT_MISMATCH | `abs(internal - partner) > amount_tolerance` |
| STATUS_MISMATCH | statuses differ |
| MATCHED | otherwise |

Settings: `config/reconciliation.yaml`. The settlement date difference (`settlement_date_diff_days`) is reported but does not change the class. Provider columns never appear in the SQL: Silver maps them from `config/providers.yaml`.

The generator injects ~2% amount mismatches, ~2% status mismatches, ~3% missing-in-partner and ~0.5% orphan partner rows. CDC deletes can additionally surface as MISSING_IN_INTERNAL, because partner files still list the transaction.

Gold tables: `reconciliation_results`, `daily_settlement_summary`, `provider_performance`.
