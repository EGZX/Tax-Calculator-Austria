"""Store (dedup, schema version) and service end-to-end."""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from conftest import mk

from tax_calc_at.engine.rules import load_tax_rules
from tax_calc_at.model import (
    AssetClass,
    DatabaseVersionError,
    DuplicateMismatchError,
    TxType,
)
from tax_calc_at.service import build_year_report, import_file, reimport_archive
from tax_calc_at.store import connect, fetch_transactions, record_batch, upsert_transactions

D = Decimal


def _register(conn, batch_id: str = "b1") -> None:
    record_batch(conn, batch_id=batch_id, broker="test", source_file="x.csv",
                 file_sha256="0" * 64, imported_at="2024-01-01T00:00:00", rows_total=0,
                 rows_emitted=0, rows_ignored=0, rows_rejected=0, flags=[])


def test_dedup_idempotent_and_mismatch(tmp_path: Path):
    conn = connect(tmp_path / "t.db")
    for b in ("b1", "b2", "b3"):
        _register(conn, b)
    assert upsert_transactions(conn, [mk(TxType.BUY, qty="1", amount_eur="-100",
                                         raw_ref="A")], batch_id="b1") == (1, 0)
    assert upsert_transactions(conn, [mk(TxType.BUY, qty="1", amount_eur="-100",
                                         raw_ref="A")], batch_id="b2") == (0, 1)
    with pytest.raises(DuplicateMismatchError):
        upsert_transactions(conn, [mk(TxType.BUY, qty="1", amount_eur="-999", raw_ref="A")],
                            batch_id="b3")


def test_roundtrip_keeps_new_fields(tmp_path: Path):
    conn = connect(tmp_path / "t.db")
    _register(conn)
    tx = mk(TxType.FX_CONVERSION, currency="USD", gross_native="10", amount_eur="9", rate="0.9",
            counter_currency="EUR", counter_amount=D("-9.1"), link_id="L1", isin=None,
            asset_class=AssetClass.CASH, raw_ref="R")
    upsert_transactions(conn, [tx], batch_id="b1")
    (back,) = fetch_transactions(conn)
    assert (back.link_id, back.counter_currency, back.counter_amount) == ("L1", "EUR", D("-9.1"))


def test_schema_version_guard(tmp_path: Path):
    db = tmp_path / "old.db"
    raw = sqlite3.connect(db)
    raw.executescript(
        "CREATE TABLE transactions (dedup_key TEXT PRIMARY KEY);"
        "INSERT INTO transactions VALUES ('x');"
    )
    raw.commit()
    raw.close()
    with pytest.raises(DatabaseVersionError, match="re-import"):
        connect(db)


def test_classification_rules_2024():
    rules = load_tax_rules(2024)
    assert rules.classify(TxType.SELL, AssetClass.STOCK) == "realisiert_ueberschuesse_27_5"
    assert rules.classify(TxType.SELL, AssetClass.DERIVATIVE) == "derivate_ueberschuesse_27_5"
    assert rules.classify(TxType.DIVIDEND_CASH, AssetClass.ETF) == "fonds_ausschuettungen_27_5"
    assert rules.classify(TxType.INTEREST, AssetClass.CASH,
                          credit_institution=True) == "zinsen_kreditinstitute_25"
    assert rules.classify(TxType.INTEREST, AssetClass.CASH,
                          credit_institution=False) == "ueberlassung_27_5"
    assert rules.classify(TxType.BUY, AssetClass.STOCK) is None


def _store(db: Path, txns) -> None:
    conn = connect(db)
    _register(conn)
    for i, t in enumerate(txns):
        t.raw_ref = f"R{i}"
    upsert_transactions(conn, txns, batch_id="b1")
    conn.close()


def _etf(day, tx_type, qty, eur, line):
    return mk(tx_type, broker="scalable_capital", day=date(2024, 1, day), qty=qty,
              amount_eur=eur, isin="IE00B4ND3602", asset_class=AssetClass.ETF, line=line)


def test_etf_blocker_downgraded_when_no_year_end_holding(tmp_path: Path):
    db = tmp_path / "t.db"
    _store(db, [_etf(2, TxType.BUY, "1", "-100", 2), _etf(3, TxType.SELL, "1", "110", 3)])
    report, _, _ = build_year_report(2024, db_path=db, tolerant=True)
    assert not any("ETF row(s) present" in b for b in report.health.blockers)
    assert any("no ETF position is open at year-end" in w for w in report.health.warnings)


def test_etf_blocker_kept_when_year_end_holding_exists(tmp_path: Path):
    db = tmp_path / "t.db"
    _store(db, [_etf(2, TxType.BUY, "1", "-100", 2)])
    report, _, _ = build_year_report(2024, db_path=db, tolerant=True)
    assert any("ETF row(s) present" in b for b in report.health.blockers)


def test_import_and_reimport_archive_end_to_end(tmp_path: Path):
    csv = tmp_path / "sc.csv"
    csv.write_text(
        "date;time;status;reference;description;assetType;type;isin;shares;price;amount;fee;tax;currency\n"
        "2024-01-02;10:00:00;Executed;S1;Apple;Security;Buy;US0378331005;10;100,00;-1000,00;0,99;0,00;EUR\n"
        "2024-03-02;10:00:00;Executed;S2;Apple;Security;Sell;US0378331005;4;120,00;480,00;0,99;0,00;EUR\n"
        '2024-05-02;01:00:00;Executed;"WWEK 1";Apple;Cash;Distribution;US0378331005;;;8,50;0,00;1,50;EUR\n',
        encoding="utf-8",
    )
    db, raw = tmp_path / "a.db", tmp_path / "raw"
    res = import_file(broker_key="scalable_capital", source_path=csv, db_path=db, raw_dir=raw)
    assert res.rows_inserted == 3
    again = import_file(broker_key="scalable_capital", source_path=csv, db_path=db, raw_dir=raw)
    assert again.rows_existed == 3
    report, _, _ = build_year_report(2024, db_path=db)
    assert report.by_kennzahl() == {994: D("80.00"), 863: D("10.00"), 998: D("1.50")}
    rebuilt = reimport_archive(db_path=tmp_path / "b.db", raw_dir=raw)
    assert [r.rows_inserted for r in rebuilt] == [3]


def test_asset_class_harmonized_per_isin(tmp_path: Path):
    """Regression: a sell row described without 'UCITS ETF' fell back to
    STOCK while the buys of the same ISIN were ETF."""
    db = tmp_path / "t.db"
    _store(db, [
        _etf(2, TxType.BUY, "2", "-200", 2),
        mk(TxType.SELL, day=date(2024, 1, 3), qty="2", amount_eur="210", isin="IE00B4ND3602",
           asset_class=AssetClass.STOCK, line=3),
    ])
    _, txns, pm = build_year_report(2024, db_path=db, tolerant=True)
    assert {t.asset_class for t in txns} == {AssetClass.ETF}
    assert pm.realized_events()[0].asset_class is AssetClass.ETF
