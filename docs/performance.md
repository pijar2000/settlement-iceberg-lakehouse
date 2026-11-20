# Performance and memory

Measured (host Python 3.12, Windows): default dataset (300k rows, 20k CDC events, 5 providers) generated in ~20 s, ~71 MB of CSV, with chunked writing (constant memory).

Reduce memory: set `SPARK_DRIVER_MEMORY=1g` in `.env`, lower `--rows` (e.g. 50000), and Docker Desktop → Settings → Resources (WSL2: `%UserProfile%\.wslconfig` with `memory=5GB`).

Spark/Iceberg timings, file counts and Parquet-vs-Iceberg comparison: not yet measured.
