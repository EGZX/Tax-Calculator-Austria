"""End-to-end: store + dedup + classify."""
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from tax_calc_at.engine.rules import load_tax_rules
from tax_calc_at.model import (
    AssetClass,
    DuplicateMismatchError,
    FxSource,
    Transaction,
    TxType,
)
from tax_calc_at.service import build_year_report
from tax_calc_at.store import (
    connect,
    fetch_transactions,
    record_batch,
    upsert_transactions,
)


def _register(conn, batch_id: str = "b1") -> None:
    record_batch(
        conn,
        batch_id=batch_id,
        broker="test",
        source_file="x.csv",
        file_sha256="0" * 64,
        imported_at="2024-01-01T00:00:00",
        rows_total=0,
        rows_emitted=0,
        rows_ignored=0,
        rows_rejected=0,
        flags=[],
    )


def _tx(qty: str = "1", gross: str = "-100", line: int = 1, ref: str | None = "ABC") -> Transaction:
    tx = Transaction(
        broker="test",
        trade_date=date(2024, 1, line),
        tx_type=TxType.BUY,
        asset_class=AssetClass.STOCK,
        quantity=Decimal(qty),
        currency_native="EUR",
        gross_native=Decimal(gross),
        source_file="x.csv",
        source_line=line,
        isin="US88160R1014",
        raw_ref=ref,
    )
    tx.amount_eur = Decimal(gross)
    tx.fee_eur = Decimal("0")
    tx.tax_withheld_eur = Decimal("0")
    tx.fx_rate_used = Decimal("1")
    tx.fx_rate_source = FxSource.NATIVE_EUR
    return tx


def test_dedup_idempotent_and_mismatch(tmp_path: Path):
    db = tmp_path / "t.db"
    conn = connect(db)
    _register(conn, "b1")
    _register(conn, "b2")
    _register(conn, "b3")
    a = _tx()
    inserted, existed = upsert_transactions(conn, [a], batch_id="b1")
    assert (inserted, existed) == (1, 0)
    # Same row again → no insert, no error
    inserted, existed = upsert_transactions(conn, [_tx()], batch_id="b2")
    assert (inserted, existed) == (0, 1)
    # Same ref but a different gross → DuplicateMismatchError
    bad = _tx(gross="-999")
    with pytest.raises(DuplicateMismatchError):
        upsert_transactions(conn, [bad], batch_id="b3")
    conn.close()


def test_fetch_filter_by_year(tmp_path: Path):
    db = tmp_path / "t.db"
    conn = connect(db)
    _register(conn, "b1")
    upsert_transactions(conn, [_tx(line=i, ref=f"R{i}") for i in range(1, 5)], batch_id="b1")
    rows = fetch_transactions(conn, year=2024)
    assert len(rows) == 4
    rows = fetch_transactions(conn, year=2099)
    assert len(rows) == 0
    conn.close()


def test_classification_rules_2024():
    rules = load_tax_rules(2024)
    assert rules.classify(TxType.SELL, AssetClass.STOCK) == "einkuenfte_realisierte_wertsteigerungen_27_5"
    assert rules.classify(TxType.DIVIDEND_CASH, AssetClass.STOCK) == "einkuenfte_ueberlassung_27_5"
    assert rules.classify(TxType.INTEREST, AssetClass.CASH) == "zinsen_geldeinlagen_25"
    # Non-income types map to None
    assert rules.classify(TxType.BUY, AssetClass.STOCK) is None
    assert rules.classify(TxType.DEPOSIT_CASH, AssetClass.CASH) is None
    # DIVIDEND_STOCK is declared in every YAML (review fix #6) \u2014 maps to None.
    assert rules.classify(TxType.DIVIDEND_STOCK, AssetClass.STOCK) is None


def test_kennzahl_credit_bucket_from_yaml():
    rules = load_tax_rules(2024)
    # YAML declares credit_bucket per income kennzahl (review fix #4).
    assert rules.kennzahlen["einkuenfte_ueberlassung_27_5"].credit_bucket == "anrechenbare_quellensteuer_27_5"
    assert rules.kennzahlen["zinsen_geldeinlagen_25"].credit_bucket == "anrechenbare_quellensteuer_25"
    # Loss-offset bucket list is also YAML-driven now.
    assert rules.loss_offset.cross_bucket_within_275_buckets[0] == "einkuenfte_realisierte_wertsteigerungen_27_5_verluste"


def _etf_tx(*, day: int, tx_type: TxType, qty: str, gross: str, line: int) -> Transaction:
    tx = Transaction(
        broker="test",
        trade_date=date(2024, 1, day),
        tx_type=tx_type,
        asset_class=AssetClass.ETF,
        quantity=Decimal(qty),
        currency_native="EUR",
        gross_native=Decimal(gross),
        source_file="etf.csv",
        source_line=line,
        isin="IE00B4ND3602",
        raw_ref=f"ETF{line}",
    )
    tx.amount_eur = Decimal(gross)
    tx.fee_eur = Decimal("0")
    tx.tax_withheld_eur = Decimal("0")
    tx.fx_rate_used = Decimal("1")
    tx.fx_rate_source = FxSource.NATIVE_EUR
    return tx


def test_etf_blocker_downgraded_when_no_year_end_holding(tmp_path: Path):
    db = tmp_path / "t.db"
    conn = connect(db)
    _register(conn, "b1")
    txns = [
        _etf_tx(day=2, tx_type=TxType.BUY, qty="1", gross="-100", line=2),
        _etf_tx(day=3, tx_type=TxType.SELL, qty="1", gross="110", line=3),
    ]
    upsert_transactions(conn, txns, batch_id="b1")
    conn.close()

    report, _year_txns, _pm = build_year_report(2024, db_path=db, tolerant=True)
    assert not any("ETF row(s) present for ISINs" in b for b in report.health.blockers)
    assert any("ETF rows detected, but no ETF position is open at year-end" in w for w in report.health.warnings)


def test_etf_blocker_kept_when_year_end_holding_exists(tmp_path: Path):
    db = tmp_path / "t.db"
    conn = connect(db)
    _register(conn, "b1")
    txns = [
        _etf_tx(day=2, tx_type=TxType.BUY, qty="1", gross="-100", line=2),
    ]
    upsert_transactions(conn, txns, batch_id="b1")
    conn.close()

    report, _year_txns, _pm = build_year_report(2024, db_path=db, tolerant=True)
    assert any("ETF row(s) present for ISINs" in b for b in report.health.blockers)
