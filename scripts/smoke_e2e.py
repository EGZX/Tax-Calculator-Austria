"""End-to-end smoke test against the user's actual broker exports.

Imports every CSV under exports/, fetches ECB FX as needed, and prints
the 2024 E1kv summary plus pool snapshots. Run from repo root with:

    .venv\\Scripts\\python.exe scripts\\smoke_e2e.py
"""

from __future__ import annotations

import sys
import traceback
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from tax_calc_at.engine.rules import load_brokers
from tax_calc_at.service import build_year_report, import_file

DB = ROOT / "data" / "smoke.db"
RAW = ROOT / "data" / "raw"
DB.parent.mkdir(exist_ok=True)
RAW.mkdir(exist_ok=True)
if DB.exists():
    DB.unlink()  # fresh run every time


def main() -> int:
    brokers = load_brokers()
    plan = [
        ("scalable_capital", "Scalable Capital/2023 Scalable Transaktionen.csv"),
        ("scalable_capital", "Scalable Capital/2024 Scalable Transaktionen.csv"),
        ("scalable_capital", "Scalable Capital/2025 Scalable Transaktionen.csv"),
        ("trade_republic", "Trade Republic/TR Transaction export 2023-2025.csv"),
        ("trading212", "Trading 212/from_2024-07-12_to_2024-12-08_MTc2NTIwNjMzMzQyMA.csv"),
        ("trading212", "Trading 212/from_2024-12-08_to_2025-12-08_MTc2NTIwNjI4MTkyNA.csv"),
        ("ibkr", "IBKR/FlexQ_last365days.csv"),
    ]
    failures = 0
    for broker_key, rel in plan:
        path = ROOT / "exports" / rel
        if not path.exists():
            print(f"SKIP missing: {path}")
            continue
        print(f"\n=== Importing {broker_key}: {path.name} ===")
        try:
            result = import_file(
                broker_key=broker_key,
                source_path=path,
                db_path=DB,
                raw_dir=RAW,
                brokers=brokers,
            )
            report = result.parse_report
            print(
                f"  rows_total={result.rows_total} emitted={result.rows_emitted} "
                f"inserted={result.rows_inserted} existed={result.rows_existed} "
                f"ignored={result.rows_ignored} rejected={result.rows_rejected}"
            )
            for fl in report.flags:
                print(f"  [{fl.severity.value}] {fl.code}: {fl.message}")
        except Exception as e:  # noqa: BLE001 — we want to see every failure
            failures += 1
            print(f"  FAIL {type(e).__name__}: {e}")
            traceback.print_exc()

    for year in (2023, 2024, 2025):
        print(f"\n=== E1kv report for {year} ===")
        try:
            # Smoke against real, occasionally-incomplete exports: tolerant
            # mode so pool errors become ReportHealth blockers instead of
            # aborting. by_kennzahl() is called with allow_partial=True to
            # see figures anyway; production filing code MUST NOT do this.
            report, _year_txns, pm = build_year_report(
                year, db_path=DB, tolerant=True
            )
        except Exception as e:  # noqa: BLE001
            failures += 1
            print(f"  FAIL build_year_report({year}): {type(e).__name__}: {e}")
            traceback.print_exc()
            continue
        if report.health.blockers:
            print(f"  report.health.blockers ({len(report.health.blockers)}):")
            for b in report.health.blockers:
                print(f"    - {b.splitlines()[0]}")
        if report.health.warnings:
            print(f"  report.health.warnings ({len(report.health.warnings)}):")
            for w in report.health.warnings:
                print(f"    - {w.splitlines()[0]}")
        for kz, val in sorted(report.by_kennzahl(allow_partial=True).items()):
            print(f"  KZ {kz}: {val:>14}")
        if report.creditable_withholding:
            print("  creditable_withholding:")
            for k, v in report.creditable_withholding.items():
                print(f"    {k}: {v}")
        if report.loss_offset_note:
            print(f"  loss_offset: {report.loss_offset_note}")
        if pm.errors:
            print(f"  pool errors ({len(pm.errors)}):")
            for broker, isin, msg in pm.errors:
                print(f"    [{broker}] {isin}: {msg.splitlines()[0]}")
    print(f"\nDone. failures={failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
