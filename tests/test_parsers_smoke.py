"""Smoke tests for parsers using the user's actual exports."""
from pathlib import Path

import pytest

from tax_calc_at.model import CutoffViolationError, ParserError, TxType
from tax_calc_at.parsers import scalable, trade_republic, trading212
from tax_calc_at.parsers import ibkr_flex

EXPORTS = Path(__file__).resolve().parents[1] / "exports"


def test_scalable_2024_parses():
    p = EXPORTS / "Scalable Capital" / "2024 Scalable Transaktionen.csv"
    if not p.exists():
        pytest.skip("fixture missing")
    txns, report = scalable.parse(p)
    assert report.rows_total > 0
    assert report.rows_emitted > 0
    types = {t.tx_type for t in txns}
    assert TxType.BUY in types
    assert TxType.SELL in types
    assert TxType.DIVIDEND_CASH in types


def test_trading212_v2_parses_with_usd_trades():
    p = EXPORTS / "Trading 212" / "from_2024-12-08_to_2025-12-08_MTc2NTIwNjI4MTkyNA.csv"
    if not p.exists():
        pytest.skip("fixture missing")
    txns, report = trading212.parse(p)
    assert report.rows_total > 0
    # USD trades present
    assert any(t.currency_native == "USD" and t.tx_type == TxType.BUY for t in txns)
    # Conversion fees emitted as separate FEE rows
    assert any(t.tx_type == TxType.FEE and (t.raw_ref or "").endswith("::convfee") for t in txns)


def test_trade_republic_cutoff_enforced(tmp_path: Path):
    # Build a tiny TR-shaped CSV with a row past the cutoff.
    p = tmp_path / "tr.csv"
    p.write_text(
        '"datetime","date","account_type","category","type","asset_class","name","symbol","shares","price","amount","fee","tax","currency","original_amount","original_currency","fx_rate","description","transaction_id","counterparty_name","counterparty_iban","payment_reference","mcc_code"\n'
        '"2025-05-01T10:00:00.000Z","2025-05-01","DEFAULT","TRADING","BUY","STOCK","Foo","US88160R1014","1","100.0","-100.00","","","EUR","","","","","past-cutoff","","","",""\n',
        encoding="utf-8",
    )
    from datetime import date

    with pytest.raises(CutoffViolationError):
        trade_republic.parse(p, steuereinfach_from=date(2025, 4, 29), strict_cutoff=True)


def test_trade_republic_full_2023_2025_parses():
    p = EXPORTS / "Trade Republic" / "TR Transaction export 2023-2025.csv"
    if not p.exists():
        pytest.skip("fixture missing")
    from datetime import date

    txns, report = trade_republic.parse(p, steuereinfach_from=date(2025, 4, 29))
    assert report.rows_total > 0
    # Must contain BUY, SELL, BONUS_SHARE (Stockperk) and INTEREST
    types = {t.tx_type for t in txns}
    assert TxType.BUY in types
    assert TxType.SELL in types
    assert TxType.BONUS_SHARE in types
    assert TxType.INTEREST in types


def test_ibkr_flex_full_fixture_parses():
    p = EXPORTS / "IBKR" / "FlexQ_last365days.csv"
    if not p.exists():
        pytest.skip("fixture missing")
    txns, report = ibkr_flex.parse(p)
    assert report.rows_total > 0
    types = {t.tx_type for t in txns}
    assert TxType.BUY in types
    assert TxType.SELL in types
    assert TxType.DIVIDEND_CASH in types
    # IBKR is not an AT Kreditinstitut — broker/bond interest routes to
    # INTEREST_OTHER (27.5% KZ 863), not INTEREST (25% KZ 857).
    assert TxType.INTEREST_OTHER in types
    assert TxType.DEPOSIT_CASH in types
    # FX conversions in the trades section become IGNORED audit rows.
    assert any(
        t.tx_type is TxType.IGNORED and any(f.code == "ibkr.fx_conversion" for f in t.flags)
        for t in txns
    )
    # Dividend rows must fold in the paired withholding via ActionID.
    dividends_with_wh = [
        t for t in txns
        if t.tx_type is TxType.DIVIDEND_CASH and t.tax_withheld_native > 0
    ]
    assert dividends_with_wh, "expected at least one IBKR dividend with paired withholding"
    # withholding_country populated (from IssuerCountryCode or ISIN fallback).
    assert all(t.withholding_country for t in dividends_with_wh)


def test_scalable_paired_security_transfer_drops_basis_missing_warning(tmp_path: Path):
    p = tmp_path / "scalable_transfer.csv"
    p.write_text(
        "date;time;status;reference;description;assetType;type;isin;shares;price;amount;fee;tax;currency\n"
        "2025-12-05;01:00:00;Executed;WWUM-1;Microsoft;Security;Security transfer;US5949181045;-7;410,30;-2872,10;;;EUR\n"
        "2025-12-06;01:00:00;Executed;SWITCH-1;Microsoft;Security;Security transfer;US5949181045;7;414,55;2901,85;;;EUR\n",
        encoding="utf-8",
    )
    txns, _report = scalable.parse(p)
    txs = [t for t in txns if t.isin == "US5949181045"]
    assert len(txs) == 2
    assert all(t.tx_type is TxType.SPLIT for t in txs)
    assert all(
        not any(f.code == "scalable.security_transfer" for f in t.flags)
        for t in txs
    )
    assert all(
        any(f.code == "scalable.security_transfer_paired" for f in t.flags)
        for t in txs
    )


def test_scalable_security_transfer_in_before_out_is_not_paired(tmp_path: Path):
    """IN dated BEFORE OUT must NOT be silently folded into a SPLIT pair.

    Only OUT-then-IN on the same/subsequent day is a cost-basis-preserving
    internal custodian rebooking. An IN-then-OUT sequence is a different
    event (e.g. inbound external transfer followed by outbound external
    transfer) and must remain a MIGRATION pair so basis is requested.
    """
    p = tmp_path / "scalable_xfer_wrong_order.csv"
    p.write_text(
        "date;time;status;reference;description;assetType;type;isin;shares;price;amount;fee;tax;currency\n"
        # IN first, OUT second — must stay MIGRATION_*.
        "2025-12-05;01:00:00;Executed;IN-1;Microsoft;Security;Security transfer;US5949181045;7;414,55;2901,85;;;EUR\n"
        "2025-12-06;01:00:00;Executed;OUT-1;Microsoft;Security;Security transfer;US5949181045;-7;410,30;-2872,10;;;EUR\n",
        encoding="utf-8",
    )
    txns, _report = scalable.parse(p)
    txs = [t for t in txns if t.isin == "US5949181045"]
    assert len(txs) == 2
    kinds = sorted(t.tx_type.value for t in txs)
    assert kinds == [TxType.MIGRATION_IN.value, TxType.MIGRATION_OUT.value]
    assert all(
        any(f.code == "scalable.security_transfer" for f in t.flags)
        for t in txs
    )


def test_scalable_security_transfer_zero_shares_raises(tmp_path: Path):
    """A Security transfer row with shares=0 is ambiguous and must raise."""
    p = tmp_path / "scalable_xfer_zero.csv"
    p.write_text(
        "date;time;status;reference;description;assetType;type;isin;shares;price;amount;fee;tax;currency\n"
        "2025-12-05;01:00:00;Executed;BAD-1;Microsoft;Security;Security transfer;US5949181045;0;0;0;;;EUR\n",
        encoding="utf-8",
    )
    with pytest.raises(ParserError, match="zero shares"):
        scalable.parse(p)


def test_t212_return_of_capital_carries_net_gross_ambiguity_flag(tmp_path: Path):
    """T212 RoC row must surface the net/gross ambiguity as a WARNING flag."""
    p = tmp_path / "t212_roc.csv"
    p.write_text(
        "Action,Time,ISIN,Ticker,Name,No. of shares,Price / share,"
        "Currency (Price / share),Total,Currency (Total),Notes,ID\n"
        'Dividend (Return of capital),2024-06-01 12:00:00,US0378331005,AAPL,'
        "Apple,0,0,USD,5.00,EUR,,roc-1\n",
        encoding="utf-8",
    )
    txns, _ = trading212.parse(p)
    assert len(txns) == 1
    assert any(f.code == "t212.roc_net_gross_ambiguous" for f in txns[0].flags)
