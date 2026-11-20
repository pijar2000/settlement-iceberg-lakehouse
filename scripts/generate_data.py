"""Synthetic payment data generator (chunked, deterministic).

Cara pakai:
  python -m src.cli generate --rows 300000 --cdc-rows 20000 --seed 42
  --rows / --cdc-rows : jumlah transaksi internal / event CDC (default 300000 / 20000)
  --duplicate-rows    : baris duplikat per stream, internal & partner (default 5000)
  --invalid-rate      : porsi baris invalid, 0-1 (default 0.002)
  --chunk-size        : baris per chunk/file (default 50000)
  --seed              : seed agar hasil identik (default 42)
  --output-dir        : folder output (default data/raw)
"""
from __future__ import annotations

import argparse
import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

from src.utilities.providers import load_providers

BASE = datetime(2025, 1, 1)
CDC_BASE = BASE + timedelta(days=31)
CURRENCY = "IDR"
INTERNAL_COLS = ["transaction_id", "merchant_id", "provider", "reference_no", "amount", "currency",
                 "status", "created_at", "updated_at", "expected_settlement_date", "source_file", "batch_id"]
CDC_COLS = ["event_id", "lsn", "op", "event_ts", "transaction_id", "merchant_id", "provider", "reference_no",
            "amount", "currency", "status", "source_file", "batch_id"]
STATUSES = ["SUCCESS"] * 90 + ["FAILED"] * 7 + ["PENDING"] * 3


def txn(idx: int, seed: int, providers: list[str]) -> dict:
    """Deterministic internal transaction for index idx (independent of chunking)."""
    r = random.Random(f"{seed}:t:{idx}")
    created = BASE + timedelta(seconds=r.randrange(30 * 86400))
    amount = min(max(int(r.lognormvariate(11, 1)) // 100 * 100, 1000), 50_000_000)
    return {
        "transaction_id": f"TXN{idx:010d}", "merchant_id": f"M{r.randrange(1, 501):04d}",
        "provider": providers[r.randrange(len(providers))], "reference_no": f"REF{seed:04d}{idx:010d}",
        "amount": amount, "currency": CURRENCY, "status": r.choice(STATUSES),
        "created_at": created.isoformat(), "updated_at": created.isoformat(),
        "expected_settlement_date": (created + timedelta(days=1)).date().isoformat(),
    }


def partner_row(t: dict, cfg: dict, seed: int, idx: int, mismatch: float, status_mismatch: float) -> dict | None:
    """Provider-specific raw row for transaction t (None = missing in partner)."""
    r = random.Random(f"{seed}:p:{idx}")
    if t["status"] == "PENDING" or r.random() < 0.03:
        return None
    amount, status = t["amount"], t["status"]
    if r.random() < mismatch:
        amount += r.choice([-1000, 500, 100, 2500])
    if r.random() < status_mismatch:
        status = "FAILED" if status == "SUCCESS" else "SUCCESS"
    settled = datetime.fromisoformat(t["expected_settlement_date"]) + timedelta(hours=2)
    src_status = next(k for k, v in cfg["status_map"].items() if v == status)
    c = cfg["columns"]
    return {
        c["reference_no"]: t["reference_no"],
        c["amount"]: f"{amount * cfg['amount_scale']:.{cfg['amount_decimals']}f}",
        c["status"]: src_status,
        c["settlement_date"]: settled.strftime(cfg["date_format"]),
    }


def _invalidate(row: dict, key_cols: dict, r: random.Random) -> None:
    kind = r.randrange(3)
    if kind == 0:
        row[key_cols["reference_no"]] = ""
    elif kind == 1:
        row[key_cols["amount"]] = "-1"
    else:
        row[key_cols["status"]] = "???"


def generate(rows=300_000, cdc_rows=20_000, duplicate_rows=5_000, chunk_size=50_000, seed=42,
             output_dir="data/raw", invalid_rate=0.002, mismatch_rate=0.02, status_mismatch_rate=0.02,
             providers_cfg: dict | None = None) -> dict:
    cfgs = providers_cfg or load_providers()
    names = list(cfgs)
    batch_id = f"batch-seed{seed}"
    out = Path(output_dir) / batch_id
    stats = {"batch_id": batch_id, "seed": seed, "internal_rows": 0, "internal_duplicates": 0,
             "internal_invalid": 0, "partner_rows": 0, "partner_duplicates": 0, "partner_invalid": 0,
             "partner_orphans": 0, "cdc_events": {"I": 0, "U": 0, "D": 0}}
    (out / "internal_transactions").mkdir(parents=True, exist_ok=True)
    for n in names:
        (out / "partner_settlements" / n).mkdir(parents=True, exist_ok=True)
    (out / "cdc_events").mkdir(parents=True, exist_ok=True)

    for chunk_no, start in enumerate(range(0, rows, chunk_size)):
        end = min(start + chunk_size, rows)
        r = random.Random(f"{seed}:chunk:{chunk_no}")
        n_dup = round(duplicate_rows * (end - start) / rows)
        dup_idx = {r.randrange(start, end) for _ in range(n_dup)}
        fname = f"internal_transactions_{chunk_no:04d}.csv"
        pfiles, pwriters = {}, {}
        for n in names:
            pname = f"{cfgs[n]['file_prefix']}_{chunk_no:04d}.csv"
            fh = open(out / "partner_settlements" / n / pname, "w", newline="", encoding="utf-8")
            c = cfgs[n]["columns"]
            w = csv.DictWriter(fh, [*c.values(), "source_file", "batch_id"], delimiter=cfgs[n]["delimiter"])
            w.writeheader()
            pfiles[n], pwriters[n] = fh, (w, pname)
        with open(out / "internal_transactions" / fname, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, INTERNAL_COLS)
            w.writeheader()
            for idx in range(start, end):
                t = txn(idx, seed, names)
                prow = partner_row(t, cfgs[t["provider"]], seed, idx, mismatch_rate, status_mismatch_rate)
                irow = {**t, "source_file": fname, "batch_id": batch_id}
                bad_i = r.random() < invalid_rate
                if bad_i:
                    _invalidate(irow, {"reference_no": "reference_no", "amount": "amount", "status": "status"}, r)
                    stats["internal_invalid"] += 1
                w.writerow(irow)
                stats["internal_rows"] += 1
                if idx in dup_idx:
                    w.writerow(irow)
                    stats["internal_duplicates"] += 1
                    stats["internal_invalid"] += int(bad_i)
                if prow is not None:
                    cfg = cfgs[t["provider"]]
                    pw, pname = pwriters[t["provider"]]
                    prow.update(source_file=pname, batch_id=batch_id)
                    bad_p = r.random() < invalid_rate
                    if bad_p:
                        _invalidate(prow, cfg["columns"], r)
                        stats["partner_invalid"] += 1
                    pw.writerow(prow)
                    stats["partner_rows"] += 1
                    if idx in dup_idx:
                        pw.writerow(prow)
                        stats["partner_duplicates"] += 1
                        stats["partner_invalid"] += int(bad_p)
                if r.random() < 0.005:  # partner record unknown to internal system
                    on = names[r.randrange(len(names))]
                    oc, (ow, oname) = cfgs[on], pwriters[on]
                    o = partner_row({**t, "reference_no": f"ORPHAN{seed:04d}{idx:010d}", "status": "SUCCESS"},
                                    oc, seed, idx, 0, 0) or {}
                    if o:
                        ow.writerow({**o, "source_file": oname, "batch_id": batch_id})
                        stats["partner_orphans"] += 1
        for fh in pfiles.values():
            fh.close()

    _write_cdc(out, rows, cdc_rows, chunk_size, seed, names, batch_id, stats)
    (out / "manifest.json").write_text(json.dumps(stats, indent=2), encoding="utf-8")
    return stats


def _write_cdc(out, rows, cdc_rows, chunk_size, seed, names, batch_id, stats) -> None:
    next_insert = rows
    for chunk_no, start in enumerate(range(0, cdc_rows, chunk_size)):
        fname = f"cdc_events_{chunk_no:04d}.csv"
        with open(out / "cdc_events" / fname, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, CDC_COLS)
            w.writeheader()
            for k in range(start, min(start + chunk_size, cdc_rows)):
                r = random.Random(f"{seed}:c:{k}")
                op = r.choices("IUD", weights=[40, 45, 15])[0]
                idx = next_insert if op == "I" else r.randrange(rows)
                if op == "I":
                    next_insert += 1
                t = txn(idx, seed, names)
                if op == "U":
                    t["status"] = r.choice(["SUCCESS", "FAILED"])
                    t["amount"] = max(1000, t["amount"] + r.choice([-500, 0, 1000]))
                ts = (CDC_BASE + timedelta(seconds=5 * k)).isoformat()
                row = {"event_id": f"EVT{k:09d}", "lsn": k, "op": op, "event_ts": ts,
                       "source_file": fname, "batch_id": batch_id, **{c: t[c] for c in
                       ["transaction_id", "merchant_id", "provider", "reference_no", "amount", "currency", "status"]}}
                if op == "D":  # delete carries the key only
                    row.update(merchant_id="", provider="", reference_no="", amount="", currency="", status="")
                w.writerow(row)
                stats["cdc_events"][op] += 1


def add_arguments(p: argparse.ArgumentParser) -> None:
    p.add_argument("--rows", type=int, default=300_000)
    p.add_argument("--cdc-rows", type=int, default=20_000)
    p.add_argument("--duplicate-rows", type=int, default=5_000)
    p.add_argument("--invalid-rate", type=float, default=0.002)
    p.add_argument("--chunk-size", type=int, default=50_000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--output-dir", default="data/raw")


def run(args: argparse.Namespace) -> dict:
    return generate(args.rows, args.cdc_rows, args.duplicate_rows, args.chunk_size, args.seed,
                    args.output_dir, args.invalid_rate)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    add_arguments(ap)
    print(json.dumps(run(ap.parse_args()), indent=2))
