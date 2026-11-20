"""Provider metadata loader and row normalizer (pure Python, no Spark needed).

Usage:
    from src.utilities.providers import load_providers, normalize_row
    cfg = load_providers()["OVO"]
    normalize_row(cfg, {"ovo_ref": "R1", "amt": "100", "trx_status": "SUCCESS", "settle_date": "2025-01-02"})
"""
from __future__ import annotations

import os
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import yaml

_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PATH = Path(os.environ.get("PROVIDERS_CONFIG", _ROOT / "config" / "providers.yaml"))


def load_providers(path: str | Path = DEFAULT_PATH) -> dict[str, dict]:
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)["providers"]


def normalize_row(cfg: dict, raw: dict) -> dict:
    """Map one raw provider row to canonical fields. Raises ValueError if invalid."""
    cols = cfg["columns"]
    ref = (raw.get(cols["reference_no"]) or "").strip()
    if not ref:
        raise ValueError("missing reference_no")
    try:
        amount = Decimal(str(raw.get(cols["amount"]))) / Decimal(cfg["amount_scale"])
    except (InvalidOperation, TypeError) as exc:
        raise ValueError("bad amount") from exc
    if amount <= 0:
        raise ValueError("non-positive amount")
    status = cfg["status_map"].get(str(raw.get(cols["status"])))
    if status is None:
        raise ValueError("unknown status")
    try:
        settled = datetime.strptime(str(raw.get(cols["settlement_date"])), cfg["date_format"]).date()
    except ValueError as exc:
        raise ValueError("bad settlement date") from exc
    return {"reference_no": ref, "amount": amount, "status": status, "settlement_date": settled}
