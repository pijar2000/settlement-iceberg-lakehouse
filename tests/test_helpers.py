"""Unit tests for Spark-free helpers (formats, reconciliation settings, demo/CLI wiring).

Cara pakai: pytest tests/test_helpers.py
"""
import pytest

from src.cli import main
from src.iceberg_demo import STEPS
from src.reconciliation.engine import load_settings, sql_params
from src.utilities.providers import load_providers, strptime_to_spark


@pytest.mark.parametrize("fmt,expected", [
    ("%Y-%m-%d", "yyyy-MM-dd"), ("%d/%m/%Y", "dd/MM/yyyy"), ("%Y%m%d", "yyyyMMdd"),
    ("%Y-%m-%dT%H:%M:%SZ", "yyyy-MM-dd'T'HH:mm:ss'Z'")])
def test_strptime_to_spark(fmt, expected):
    assert strptime_to_spark(fmt) == expected


def test_all_provider_formats_convert():
    for cfg in load_providers().values():
        assert strptime_to_spark(cfg["date_format"])


def test_reconciliation_params():
    p = sql_params(load_settings())
    assert p["exclude_pending"] in ("true", "false") and p["amount_tolerance"] >= 0


def test_cli_commands_registered(capsys):
    with pytest.raises(SystemExit):
        main(["--help"])
    out = capsys.readouterr().out
    for cmd in ["transform", "cdc", "reconcile", "quality", "iceberg-demo", "analytics", "demo"]:
        assert cmd in out
    assert len(STEPS) == 9
