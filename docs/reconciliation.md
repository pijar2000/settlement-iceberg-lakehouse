# Reconciliation

Planned. Match internal vs partner records on `reference_no` + provider; compare amount, status and settlement date. Classes: MATCHED, AMOUNT_MISMATCH, STATUS_MISMATCH, MISSING_IN_PARTNER, MISSING_IN_INTERNAL. Provider columns come from `config/providers.yaml`.

The generator already injects ~2% amount mismatches, ~2% status mismatches, ~3% missing-in-partner and ~0.5% orphan partner rows to exercise these classes.
