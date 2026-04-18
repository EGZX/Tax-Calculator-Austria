"""Worksheet export smoke tests.

Verifies the Berechnungsblatt ZIP contains every expected sheet, that the
HTML index and summary CSV include the computed Kennzahl totals, and that
a not-fileable report renders its blocker loudly (so a Steuerberater
reviewing the bundle cannot miss it).
"""
from __future__ import annotations

import io
import zipfile
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from tax_calc_at.engine.e1kv import build_report
from tax_calc_at.engine.rules import load_tax_rules
from tax_calc_at.export import build_worksheet
from tax_calc_at.model import AssetClass, FxSource, Transaction, TxType
from tax_calc_at.pool import PoolManager


def _make_tx(
    *,
    broker: str = "scalable_capital",
    trade_date: date,
    tx_type: TxType,
    asset_class: AssetClass = AssetClass.STOCK,
    quantity: str = "0",
    gross_native: str = "0",
    amount_eur: str = "0",
    isin: str | None = "US0378331005",
    name: str = "APPLE",
    currency: str = "EUR",
    tax_withheld_eur: str = "0",
    fx_source: FxSource = FxSource.NATIVE_EUR,
    dividend_is_net: bool | None = None,
    source_line: int = 1,
) -> Transaction:
    return Transaction(
        broker=broker,
        trade_date=trade_date,
        tx_type=tx_type,
        asset_class=asset_class,
        quantity=Decimal(quantity),
        currency_native=currency,
        gross_native=Decimal(gross_native),
        source_file="test.csv",
        source_line=source_line,
        isin=isin,
        name=name,
        amount_eur=Decimal(amount_eur),
        fee_eur=Decimal("0"),
        tax_withheld_eur=Decimal(tax_withheld_eur),
        fx_rate_used=Decimal("1"),
        fx_rate_source=fx_source,
        dividend_is_net=dividend_is_net,
    )


def _tiny_year_2024() -> tuple[list[Transaction], PoolManager]:
    """Buy + partial sell + dividend for a single ISIN in 2024 (EUR)."""
    buy = _make_tx(
        trade_date=date(2024, 1, 15),
        tx_type=TxType.BUY,
        quantity="10",
        gross_native="-1000",
        amount_eur="-1000",
        source_line=1,
    )
    sell = _make_tx(
        trade_date=date(2024, 6, 1),
        tx_type=TxType.SELL,
        quantity="4",
        gross_native="520",
        amount_eur="520",
        source_line=2,
    )
    div = _make_tx(
        trade_date=date(2024, 9, 1),
        tx_type=TxType.DIVIDEND_CASH,
        gross_native="8.50",
        amount_eur="8.50",
        tax_withheld_eur="1.50",
        dividend_is_net=True,
        source_line=3,
    )
    pm = PoolManager()
    pm.replay([buy, sell, div], on_error="raise")
    return [buy, sell, div], pm


def test_worksheet_contains_all_expected_files(tmp_path: Path) -> None:
    rules = load_tax_rules(2024)
    txns, pm = _tiny_year_2024()
    report = build_report(
        year=2024,
        rules=rules,
        transactions=txns,
        realized=pm.realized_events(),
        pool_manager=pm,
    )
    bundle = build_worksheet(
        year=2024, rules=rules, report=report, year_txns=txns, pool_manager=pm
    )
    assert bundle.filename.endswith(".zip")
    assert bundle.filename.startswith("e1kv_2024_")
    assert len(bundle.content) > 1000  # non-trivial

    with zipfile.ZipFile(io.BytesIO(bundle.content)) as zf:
        names = {n.rsplit("/", 1)[-1] for n in zf.namelist()}
    required = {
        "00_summary.csv",
        "01_transactions.csv",
        "02_realized_events.csv",
        "03_pool_snapshots.csv",
        "04_pool_events.csv",
        "05_kennzahl_contributions.csv",
        "06_health.csv",
        "07_fx_trail.csv",
        "index.html",
        "README.txt",
    }
    missing = required - names
    assert not missing, f"missing from worksheet: {missing}"


def test_worksheet_summary_includes_realized_gain_and_dividend() -> None:
    rules = load_tax_rules(2024)
    txns, pm = _tiny_year_2024()
    report = build_report(
        year=2024,
        rules=rules,
        transactions=txns,
        realized=pm.realized_events(),
        pool_manager=pm,
    )
    bundle = build_worksheet(
        year=2024, rules=rules, report=report, year_txns=txns, pool_manager=pm
    )
    with zipfile.ZipFile(io.BytesIO(bundle.content)) as zf:
        entries = {
            n.rsplit("/", 1)[-1]: zf.read(n).decode("utf-8-sig")
            for n in zf.namelist()
        }

    # Summary lists KZ 994 (realized gain: 520 - 4*100 = 120) and KZ 863
    # (grossed-up dividend: 8.50 + 1.50 = 10.00).
    summary = entries["00_summary.csv"]
    assert "994" in summary
    assert "120,00" in summary  # realized PnL
    assert "863" in summary
    assert "10,00" in summary   # dividend grossed up
    assert "# Steuerjahr: 2024" in summary

    # Realized events CSV carries the sell-line derivation.
    realized = entries["02_realized_events.csv"]
    assert "US0378331005" in realized
    assert "520,00" in realized    # proceeds
    assert "400,00" in realized    # cost basis (4 * 100)
    assert "120,00" in realized    # pnl

    # HTML is self-describing and escaped.
    html = entries["index.html"]
    assert "<title>E1kv Berechnungsblatt 2024</title>" in html
    assert "120,00" in html        # PnL visible
    assert "Automatisierte Prüfungen" in html or "NICHT zur Abgabe" in html


def test_worksheet_flags_not_fileable_when_blockers_present() -> None:
    rules = load_tax_rules(2024)
    txns, pm = _tiny_year_2024()
    report = build_report(
        year=2024,
        rules=rules,
        transactions=txns,
        realized=pm.realized_events(),
        pool_manager=pm,
    )
    # Inject a synthetic blocker to simulate an incomplete run.
    report.health.blockers.append("Unit test: pretend Meldefonds data missing")
    bundle = build_worksheet(
        year=2024, rules=rules, report=report, year_txns=txns, pool_manager=pm
    )
    with zipfile.ZipFile(io.BytesIO(bundle.content)) as zf:
        html = zf.read(
            next(n for n in zf.namelist() if n.endswith("index.html"))
        ).decode("utf-8-sig")
        summary = zf.read(
            next(n for n in zf.namelist() if n.endswith("00_summary.csv"))
        ).decode("utf-8-sig")
        health = zf.read(
            next(n for n in zf.namelist() if n.endswith("06_health.csv"))
        ).decode("utf-8-sig")

    assert "NICHT zur Abgabe" in html
    assert "Fileable: NO" in summary
    assert "BLOCKER" in health
    assert "Meldefonds" in health


def test_worksheet_csv_uses_austrian_locale_format() -> None:
    """Excel at the AT locale expects ';' separator and ',' decimal."""
    rules = load_tax_rules(2024)
    txns, pm = _tiny_year_2024()
    report = build_report(
        year=2024,
        rules=rules,
        transactions=txns,
        realized=pm.realized_events(),
        pool_manager=pm,
    )
    bundle = build_worksheet(
        year=2024, rules=rules, report=report, year_txns=txns, pool_manager=pm
    )
    with zipfile.ZipFile(io.BytesIO(bundle.content)) as zf:
        tx_csv = zf.read(
            next(n for n in zf.namelist() if n.endswith("01_transactions.csv"))
        )
    # BOM prefix — Excel detects UTF-8 automatically without the user
    # selecting the encoding in Text Import Wizard.
    assert tx_csv.startswith(b"\xef\xbb\xbf")
    text = tx_csv.decode("utf-8-sig")
    header = text.splitlines()[0]
    assert ";" in header
    # No dot-decimals leaking through for our seeded rows
    body = "\n".join(text.splitlines()[1:])
    assert "1000.00" not in body
    assert "1000,00" in body or "-1000,00" in body or "-1000" in body
