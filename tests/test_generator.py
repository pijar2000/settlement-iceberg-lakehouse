"""Unit tests: generator determinism, volumes, provider mapping.

Cara pakai: pytest tests/test_generator.py
"""
import csv
import filecmp
from pathlib import Path

import pytest

from scripts.generate_data import generate
from src.utilities.providers import load_providers, normalize_row

PROVIDERS = load_providers()
ARGS = dict(rows=2000, cdc_rows=300, duplicate_rows=40, chunk_size=800, seed=7, invalid_rate=0.01)


def read(path, delimiter=","):
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh, delimiter=delimiter))


@pytest.fixture(scope="module")
def out(tmp_path_factory):
    d = tmp_path_factory.mktemp("raw")
    stats = generate(output_dir=d, **ARGS)
    return d / stats["batch_id"], stats


def test_counts(out):
    root, stats = out
    internal = [r for f in sorted((root / "internal_transactions").glob("*.csv")) for r in read(f)]
    assert stats["internal_rows"] == 2000
    assert len(internal) == 2000 + stats["internal_duplicates"]
    assert 35 <= stats["internal_duplicates"] <= 40
    assert sum(stats["cdc_events"].values()) == 300
    assert all(stats["cdc_events"][k] > 0 for k in "IUD")


def test_deterministic(out, tmp_path):
    root, _ = out
    other = tmp_path / generate(output_dir=tmp_path, **ARGS)["batch_id"]
    for f in root.rglob("*.csv"):
        assert filecmp.cmp(f, other / f.relative_to(root), shallow=False)


def test_five_providers_with_distinct_schemas(out):
    root, _ = out
    headers = set()
    for name, cfg in PROVIDERS.items():
        files = sorted((root / "partner_settlements" / name).glob("*.csv"))
        assert files
        rows = read(files[0], cfg["delimiter"])
        assert set(cfg["columns"].values()) <= set(rows[0])
        headers.add(tuple(cfg["columns"].values()))
    assert len(PROVIDERS) == 5 and len(headers) == 5


def test_partner_rows_map_to_canonical(out):
    root, stats = out
    ok = bad = 0
    for name, cfg in PROVIDERS.items():
        for f in (root / "partner_settlements" / name).glob("*.csv"):
            for raw in read(f, cfg["delimiter"]):
                try:
                    n = normalize_row(cfg, raw)
                    assert n["status"] in {"SUCCESS", "FAILED", "PENDING"} and n["amount"] > 0
                    ok += 1
                except ValueError:
                    bad += 1
    assert ok > 0 and bad == stats["partner_invalid"]


def test_amount_scale_linkaja():
    cfg = PROVIDERS["LINKAJA"]
    row = {"trxRef": "R", "nominal": "15000000", "state": "S", "settled_on": "20250102"}
    assert int(normalize_row(cfg, row)["amount"]) == 150000


@pytest.mark.parametrize("bad", [{"amt": "-1"}, {"ovo_ref": ""}, {"trx_status": "???"}, {"settle_date": "x"}])
def test_invalid_rows_rejected(bad):
    row = {"ovo_ref": "R", "amt": "100", "trx_status": "SUCCESS", "settle_date": "2025-01-02", **bad}
    with pytest.raises(ValueError):
        normalize_row(PROVIDERS["OVO"], row)


def test_cdc_delete_has_key_only(out):
    root, _ = out
    ev = read(next((root / "cdc_events").glob("*.csv")))
    d = next(e for e in ev if e["op"] == "D")
    assert d["transaction_id"] and d["amount"] == "" and d["status"] == ""
    assert [int(e["lsn"]) for e in ev] == sorted(int(e["lsn"]) for e in ev)


def test_cli_help():
    from src.cli import main
    with pytest.raises(SystemExit) as e:
        main(["--help"])
    assert e.value.code == 0
    assert Path("src/cli.py").exists()
