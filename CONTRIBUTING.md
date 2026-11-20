# Contributing

1. Fork and create a feature branch.
2. Run checks locally: `ruff check .` and `pytest` (unit tests need only `pyyaml`, `pytest`).
3. Integration tests (`pytest -m integration`) require the Docker stack: `docker compose up -d --build`.
4. Keep provider-specific logic in `config/providers.yaml`, not in code.
5. Open a PR describing what changed and how you tested it.
