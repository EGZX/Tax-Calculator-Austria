"""Engine-level tax-behavior tests.

These verify the *math* the engine performs once parsing + FX conversion are
done, not just YAML metadata: gross-up of net dividends with withholding,
DBA credit cap, per-country override, exclusion of return-of-capital from
income buckets, and inclusion of credit Kennzahlen in by_kennzahl().
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from tax_calc_at.engine.e1kv import build_report
from tax_calc_at.engine.rules import load_tax_rules
from tax_calc_at.model import AssetClass, FxSource, Transaction, TxType


def _div(
    *,
    net_eur: str,
    wh_eur: str,
    country: str | None = "US",
    isin: str = "US0378331005",
    line: int = 1,
) -> Transaction:
    tx = Transaction(
        broker="t212",
        trade_date=date(2024, 6, 1),
        tx_type=TxType.DIVIDEND_CASH,
        asset_class=AssetClass.STOCK,
        quantity=Decimal("0"),
        currency_native="EUR",
        gross_native=Decimal(net_eur),
        source_file="x.csv",
        source_line=line,
        isin=isin,
        withholding_country=country,
    )
    tx.amount_eur = Decimal(net_eur)
    tx.tax_withheld_eur = Decimal(wh_eur)
    tx.fx_rate_used = Decimal("1")
    tx.fx_rate_source = FxSource.NATIVE_EUR
    # Helper takes net_eur explicitly — declare NET convention so the engine
    # grosses up (dividend_is_net now defaults to None and is a hard blocker).
    tx.dividend_is_net = True
    return tx


def _roc(*, amount_eur: str, isin: str = "US0378331005", line: int = 2) -> Transaction:
    tx = Transaction(
        broker="t212",
        trade_date=date(2024, 6, 2),
        tx_type=TxType.RETURN_OF_CAPITAL,
        asset_class=AssetClass.STOCK,
        quantity=Decimal("0"),
        currency_native="EUR",
        gross_native=Decimal(amount_eur),
        source_file="x.csv",
        source_line=line,
    )
    tx.amount_eur = Decimal(amount_eur)
    tx.tax_withheld_eur = Decimal("0")
    tx.fx_rate_used = Decimal("1")
    tx.fx_rate_source = FxSource.NATIVE_EUR
    return tx


def test_dividend_grossup_net_plus_withholding_is_income():
    """Net dividend of 85 EUR with 15 EUR withholding → 100 EUR income."""
    rules = load_tax_rules(2024)
    tx = _div(net_eur="85", wh_eur="15", country="US")
    rep = build_report(year=2024, rules=rules, transactions=[tx], realized=[])
    income = rep.buckets["einkuenfte_ueberlassung_27_5"].total_eur
    assert income == Decimal("100")


def test_withholding_credit_capped_at_15_percent_default():
    """Withholding of 30 EUR on 100 EUR gross is capped at 15 % = 15 EUR."""
    rules = load_tax_rules(2024)
    # Net 70, WH 30 → gross 100; cap = 15 % * 100 = 15.
    tx = _div(net_eur="70", wh_eur="30", country="ZZ")  # ZZ → no override → default cap
    rep = build_report(year=2024, rules=rules, transactions=[tx], realized=[])
    credit = rep.creditable_withholding["anrechenbare_quellensteuer_27_5"]
    assert credit == Decimal("15.00")


def test_credit_cap_is_global_15_percent_even_with_country_override_present():
    """Country-specific caps above 15% are clamped; Austrian ceiling stays 15%."""
    rules = load_tax_rules(2024)
    # Even if a country-specific cap is misconfigured above 15%, the hard
    # Austrian ceiling wins. See also test_country_cap_never_raises_above_...
    rules.foreign_withholding.country_caps["CH"] = Decimal("0.25")
    tx = _div(
        net_eur="65", wh_eur="35", country="CH", isin="CH0038863350"
    )  # gross 100, WH 35
    rep = build_report(year=2024, rules=rules, transactions=[tx], realized=[])
    credit = rep.creditable_withholding["anrechenbare_quellensteuer_27_5"]
    assert credit == Decimal("15.00")


def test_credit_cap_never_exceeds_15_even_if_default_is_misconfigured_higher():
    """Configured defaults above 15% must not increase creditable withholding."""
    rules = load_tax_rules(2024)
    # Simulate a bad config edit; engine must still enforce hard 15% ceiling.
    rules.foreign_withholding.default_creditable_cap = Decimal("0.30")
    # Country without a per-country cap → falls through to default → clamped.
    tx = _div(net_eur="70", wh_eur="30", country="ZZ")  # gross 100, WH 30
    rep = build_report(year=2024, rules=rules, transactions=[tx], realized=[])
    credit = rep.creditable_withholding["anrechenbare_quellensteuer_27_5"]
    assert credit == Decimal("15.00")


def test_country_specific_cap_lowers_creditable_withholding():
    """Japan DBA cap is 10%: withholding above 10% of gross is not creditable.

    Gross 100, WH 15 → creditable = min(15, 100 * 10%) = 10.
    The 5 EUR excess surfaces as uncreditable_withholding.
    """
    rules = load_tax_rules(2024)
    assert rules.foreign_withholding.country_caps["JP"] == Decimal("0.10")
    tx = _div(
        net_eur="85", wh_eur="15", country="JP", isin="JP3633400001"
    )  # gross 100
    rep = build_report(year=2024, rules=rules, transactions=[tx], realized=[])
    assert rep.creditable_withholding["anrechenbare_quellensteuer_27_5"] == Decimal(
        "10.00"
    )
    assert rep.uncreditable_withholding["anrechenbare_quellensteuer_27_5"] == Decimal(
        "5.00"
    )


def test_country_cap_never_raises_above_global_15_percent():
    """Even if a country is (mis-)configured above 15%, the 15% ceiling wins."""
    rules = load_tax_rules(2024)
    rules.foreign_withholding.country_caps["CH"] = Decimal("0.25")
    tx = _div(
        net_eur="65", wh_eur="35", country="CH", isin="CH0038863350"
    )  # gross 100
    rep = build_report(year=2024, rules=rules, transactions=[tx], realized=[])
    credit = rep.creditable_withholding["anrechenbare_quellensteuer_27_5"]
    assert credit == Decimal("15.00")


def test_return_of_capital_excluded_from_dividend_bucket():
    rules = load_tax_rules(2024)
    div = _div(net_eur="50", wh_eur="0", country="US", line=1)
    roc = _roc(amount_eur="40", line=2)
    rep = build_report(
        year=2024, rules=rules, transactions=[div, roc], realized=[]
    )
    income = rep.buckets["einkuenfte_ueberlassung_27_5"].total_eur
    # Only the dividend, NOT the return of capital, may flow into the bucket.
    assert income == Decimal("50")


def test_by_kennzahl_includes_creditable_withholding():
    rules = load_tax_rules(2024)
    tx = _div(net_eur="70", wh_eur="30", country="ZZ")
    rep = build_report(year=2024, rules=rules, transactions=[tx], realized=[])
    flat = rep.by_kennzahl()
    # Income KZ 863 = 100, credit KZ 998 = 15 — both present in the flat dict.
    assert flat[863] == Decimal("100.00")
    assert flat[998] == Decimal("15.00")


def test_t212_parser_emits_missing_withholding_flag(tmp_path):
    """A real T212-shaped Dividend row gets the missing-wh flag."""
    from tax_calc_at.parsers.trading212 import parse as t212_parse

    csv = tmp_path / "t212.csv"
    csv.write_text(
        "Action,Time,ISIN,Ticker,Name,No. of shares,Price / share,"
        "Currency (Price / share),Total,Currency (Total),Notes,ID\n"
        'Dividend (Ordinary),2024-06-01 12:00:00,US0378331005,AAPL,Apple,'
        "0,0,USD,12.34,EUR,,div-1\n",
        encoding="utf-8",
    )
    txns, _ = t212_parse(csv)
    assert len(txns) == 1
    tx = txns[0]
    assert tx.tx_type is TxType.DIVIDEND_CASH
    assert tx.withholding_country == "US"
    assert any(f.code == "t212.missing_withholding_detail" for f in tx.flags)


def test_t212_parser_splits_return_of_capital(tmp_path):
    from tax_calc_at.parsers.trading212 import parse as t212_parse

    csv = tmp_path / "t212.csv"
    csv.write_text(
        "Action,Time,ISIN,Ticker,Name,No. of shares,Price / share,"
        "Currency (Price / share),Total,Currency (Total),Notes,ID\n"
        'Dividend (Return of capital),2024-06-01 12:00:00,US0378331005,AAPL,'
        "Apple,0,0,USD,5.00,EUR,,roc-1\n",
        encoding="utf-8",
    )
    txns, _ = t212_parse(csv)
    assert len(txns) == 1
    tx = txns[0]
    assert tx.tx_type is TxType.RETURN_OF_CAPITAL
    assert any(f.code == "t212.return_of_capital" for f in tx.flags)


def test_scalable_distribution_defaults_to_dividend_without_ambiguous_flag(tmp_path):
    from tax_calc_at.parsers.scalable import parse as sc_parse

    csv = tmp_path / "sc.csv"
    csv.write_text(
        "date;time;status;reference;description;assetType;type;isin;shares;"
        "price;amount;fee;tax;currency\n"
        '2024-06-01;12:00:00;Executed;"REF1";"Apple Dividend";Stock;'
        "Distribution;US0378331005;0;0;12,34;0;0;EUR\n",
        encoding="utf-8",
    )
    txns, _ = sc_parse(csv)
    assert len(txns) == 1
    tx = txns[0]
    assert tx.tx_type is TxType.DIVIDEND_CASH
    assert tx.withholding_country == "US"
    assert not any(f.code == "scalable.distribution_ambiguous" for f in tx.flags)


def test_scalable_distribution_can_be_overridden_to_roc(tmp_path, monkeypatch):
    from tax_calc_at.parsers import scalable as sc

    overrides = tmp_path / "ov.yaml"
    overrides.write_text(
        """
overrides:
  - isin: US0378331005
    date_from: 2024-06-01
    date_to: 2024-06-01
    reference_regex: '^REF1$'
    tx_type: RETURN_OF_CAPITAL
    note: test override
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(sc, "_DIST_OVERRIDES_FILE", Path(overrides))
    sc.reset_distribution_overrides_cache()

    csv = tmp_path / "sc.csv"
    csv.write_text(
        "date;time;status;reference;description;assetType;type;isin;shares;"
        "price;amount;fee;tax;currency\n"
        '2024-06-01;12:00:00;Executed;"REF1";"Apple Distribution";Stock;'
        "Distribution;US0378331005;0;0;12,34;0;0;EUR\n",
        encoding="utf-8",
    )
    txns, _ = sc.parse(csv)
    assert len(txns) == 1
    tx = txns[0]
    assert tx.tx_type is TxType.RETURN_OF_CAPITAL
    assert any(f.code == "scalable.distribution_override_roc" for f in tx.flags)


# ---------------------------------------------------------- post-audit regressions


def _interest_tx(
    *,
    tx_type: TxType,
    amount_eur: str,
    asset_class: AssetClass = AssetClass.CASH,
    line: int = 1,
) -> Transaction:
    tx = Transaction(
        broker="ibkr",
        trade_date=date(2024, 6, 1),
        tx_type=tx_type,
        asset_class=asset_class,
        quantity=Decimal("0"),
        currency_native="EUR",
        gross_native=Decimal(amount_eur),
        source_file="x.csv",
        source_line=line,
    )
    tx.amount_eur = Decimal(amount_eur)
    tx.fx_rate_used = Decimal("1")
    tx.fx_rate_source = FxSource.NATIVE_EUR
    return tx


def test_interest_other_routes_to_27_5_bucket():
    """Broker interest / bond coupons / share-lending → KZ 863 (27,5 %)."""
    rules = load_tax_rules(2024)
    tx = _interest_tx(tx_type=TxType.INTEREST_OTHER, amount_eur="50")
    rep = build_report(year=2024, rules=rules, transactions=[tx], realized=[])
    assert rep.buckets["einkuenfte_ueberlassung_27_5"].total_eur == Decimal("50")
    assert "zinsen_geldeinlagen_25" not in rep.buckets


def test_interest_bank_routes_to_25_bucket():
    """Deposit interest at a Kreditinstitut → KZ 857 (25 %)."""
    rules = load_tax_rules(2024)
    tx = _interest_tx(tx_type=TxType.INTEREST, amount_eur="50")
    rep = build_report(year=2024, rules=rules, transactions=[tx], realized=[])
    assert rep.buckets["zinsen_geldeinlagen_25"].total_eur == Decimal("50")
    assert "einkuenfte_ueberlassung_27_5" not in rep.buckets


def test_uncreditable_withholding_surfaces_excess():
    """30 % foreign withholding — only 15 % creditable; 15 % surfaces as excess."""
    rules = load_tax_rules(2024)
    tx = _div(net_eur="70", wh_eur="30", country="ZZ")  # default 15 % cap
    rep = build_report(year=2024, rules=rules, transactions=[tx], realized=[])
    assert rep.creditable_withholding["anrechenbare_quellensteuer_27_5"] == Decimal(
        "15.00"
    )
    assert rep.uncreditable_withholding["anrechenbare_quellensteuer_27_5"] == Decimal(
        "15.00"
    )


def test_cross_broker_same_isin_emits_warning():
    """Same ISIN at two brokers → warning (cost basis is per-broker)."""
    from tax_calc_at.pool import PoolManager

    def _buy(broker: str, line: int) -> Transaction:
        tx = Transaction(
            broker=broker,
            trade_date=date(2024, 1, line),
            tx_type=TxType.BUY,
            asset_class=AssetClass.STOCK,
            quantity=Decimal("10"),
            currency_native="EUR",
            gross_native=Decimal("-1000"),
            source_file=f"{broker}.csv",
            source_line=line,
            isin="US0378331005",
        )
        tx.amount_eur = Decimal("-1000")
        tx.fx_rate_source = FxSource.NATIVE_EUR
        return tx

    pm = PoolManager()
    pm.replay([_buy("A", 1), _buy("B", 2)])
    rules = load_tax_rules(2024)
    rep = build_report(
        year=2024, rules=rules, transactions=[], realized=[], pool_manager=pm
    )
    assert any(
        "held at multiple brokers" in w and "US0378331005" in w
        for w in rep.health.warnings
    )


def test_dividend_is_net_unset_blocks_filing():
    """Withholding without declared NET/GROSS convention → blocker, not silent."""
    rules = load_tax_rules(2024)
    tx = _div(net_eur="70", wh_eur="30", country="US")
    tx.dividend_is_net = None  # undo helper default
    rep = build_report(year=2024, rules=rules, transactions=[tx], realized=[])
    assert not rep.health.fileable
    assert any("dividend_is_net is unset" in b for b in rep.health.blockers)


def test_2025_realized_sell_split_into_gain_and_loss_kennzahlen():
    rules = load_tax_rules(2025)

    def _ev(pnl: str, line: str):
        from tax_calc_at.pool import RealizedEvent

        return RealizedEvent(
            trade_date=date(2025, 6, 1),
            broker="scalable_capital",
            isin="US0378331005",
            symbol="AAPL",
            name="Apple",
            asset_class=AssetClass.STOCK,
            quantity=Decimal("1"),
            proceeds_eur=Decimal("100"),
            cost_basis_eur=Decimal("90"),
            pnl_eur=Decimal(pnl),
            source_ref=f"x.csv:{line}",
        )

    rep = build_report(
        year=2025,
        rules=rules,
        transactions=[],
        realized=[_ev("10", "1"), _ev("-3", "2")],
    )

    flat = rep.by_kennzahl(allow_partial=True)
    assert flat[994] == Decimal("10.00")
    assert flat[892] == Decimal("-3.00")


def test_trade_republic_bonus_share_broker_gift_not_flagged(tmp_path):
    from datetime import date as _date
    from tax_calc_at.parsers.trade_republic import parse as tr_parse

    csv = tmp_path / "tr.csv"
    csv.write_text(
        '"datetime","date","account_type","category","type","asset_class","name","symbol","shares","price","amount","fee","tax","currency","original_amount","original_currency","fx_rate","description","transaction_id","counterparty_name","counterparty_iban","payment_reference","mcc_code"\n'
        '"2025-01-02T10:00:00.000Z","2025-01-02","DEFAULT","TRADING","STOCKPERK","ETF","Physical Gold USD (Acc)","IE00B4ND3602","0.123","70.73","8.70","0","0","EUR","","","","","ref-1","","","",""\n',
        encoding="utf-8",
    )

    txns, _ = tr_parse(csv, steuereinfach_from=_date(2025, 4, 29))
    assert len(txns) == 1
    tx = txns[0]
    assert tx.tx_type is TxType.BONUS_SHARE
    assert not any(f.code == "bonus_share_review" for f in tx.flags)
    assert not any(f.code == "bonus_share_non_broker_source" for f in tx.flags)


def test_yaml_schema_validation_rejects_typo_bucket(tmp_path):
    """load_tax_rules() rejects a YAML whose classification references an
    unknown bucket — prevents silent income routing to None."""
    from tax_calc_at.engine.rules import load_tax_rules as load

    p = tmp_path / "tax_9999.yaml"
    p.write_text(
        """
year: 9999
rates: {kapitalvermoegen_general: 0.275, zinsen_geldeinlagen: 0.25}
foreign_withholding: {default_creditable_cap: 0.15}
kennzahlen:
  bkt: {nr: 863, label: "x"}
loss_offset:
  cross_broker: true
  cross_bucket_within_275_buckets: [bkt]
classification:
  - when: {tx_type: SELL}
    bucket: TYPO_BUCKET
""",
        encoding="utf-8",
    )
    import pytest as _pytest

    with _pytest.raises(ValueError, match="unknown bucket 'TYPO_BUCKET'"):
        load(9999, path=p)


def test_t212_dividend_without_withholding_detail_blocks_filing():
    """Trading 212 dividend row with the missing-WH flag → blocker, not silent."""
    rules = load_tax_rules(2024)
    tx = _div(net_eur="85", wh_eur="0", country="US")
    # _div defaults dividend_is_net=True; the engine sees wh==0 so the old
    # gross-up branch would silently treat net as gross. The parser flag
    # declares that the WHT is UNKNOWN — must become a blocker.
    from tax_calc_at.model import Flag, Severity
    tx.flags.append(Flag(
        code="t212.missing_withholding_detail",
        severity=Severity.WARNING,
        message="test",
    ))
    rep = build_report(year=2024, rules=rules, transactions=[tx], realized=[])
    assert not rep.health.fileable
    assert any(
        "does not disclose the withholding amount" in b
        for b in rep.health.blockers
    )


def test_t212_dividend_flag_does_not_block_zero_cap_country():
    """IE / GB have 0% cap — nothing to lose to under-reporting, no blocker."""
    rules = load_tax_rules(2024)
    # Ireland: country_cap=0.0 in rules.
    tx = _div(net_eur="85", wh_eur="0", country="IE", isin="IE00B4L5Y983")
    from tax_calc_at.model import Flag, Severity
    tx.flags.append(Flag(
        code="t212.missing_withholding_detail",
        severity=Severity.WARNING,
        message="test",
    ))
    rep = build_report(year=2024, rules=rules, transactions=[tx], realized=[])
    assert not any(
        "does not disclose the withholding amount" in b
        for b in rep.health.blockers
    )


def test_t212_roc_ambiguous_blocks_filing():
    """RoC row with the ambiguous-net/gross flag → blocker."""
    rules = load_tax_rules(2024)
    tx = _roc(amount_eur="40")
    from tax_calc_at.model import Flag, Severity
    tx.flags.append(Flag(
        code="t212.roc_net_gross_ambiguous",
        severity=Severity.WARNING,
        message="test",
    ))
    rep = build_report(year=2024, rules=rules, transactions=[tx], realized=[])
    assert not rep.health.fileable
    assert any(
        "may be net of foreign withholding" in b
        for b in rep.health.blockers
    )


def test_trade_republic_stockperk_paired_buy_zero_cost(tmp_path):
    """STOCKPERK (FMV in EUR) + paired BUY must produce a zero-cost lot."""
    from datetime import date as _date
    from tax_calc_at.parsers.trade_republic import parse as tr_parse

    csv = tmp_path / "tr.csv"
    csv.write_text(
        '"datetime","date","account_type","category","type","asset_class","name","symbol","shares","price","amount","fee","tax","currency","original_amount","original_currency","fx_rate","description","transaction_id","counterparty_name","counterparty_iban","payment_reference","mcc_code"\n'
        '"2023-12-28T16:12:09.559467Z","2023-12-28","DEFAULT","CASH","STOCKPERK","STOCK","Tesla","US88160R1014","","","10.170000","","","EUR","","","","Stockperk","ref-sp","","","",""\n'
        '"2023-12-28T16:21:32.873Z","2023-12-28","DEFAULT","TRADING","BUY","STOCK","Tesla","US88160R1014","0.0435000000","233.750000","-10.17","","","EUR","","","","","ref-buy","","","",""\n',
        encoding="utf-8",
    )
    txns, _ = tr_parse(csv, steuereinfach_from=_date(2025, 4, 29))
    assert len(txns) == 2
    buy = next(t for t in txns if t.raw_ref == "ref-buy")
    # Paired BUY must now be a zero-cost BONUS_SHARE so later SELLs do not
    # inherit the 10.17 EUR of spurious basis.
    assert buy.tx_type is TxType.BONUS_SHARE
    assert buy.gross_native == Decimal("0")
    assert buy.quantity == Decimal("0.0435")
    assert any(f.code == "tr.stockperk_paired_buy" for f in buy.flags)


def test_trade_republic_stockperk_unpaired_is_unchanged(tmp_path):
    """A Stockperk without a matching BUY (shares in STOCKPERK row itself)
    must not be silently repaired — it is already a zero-cost lot."""
    from datetime import date as _date
    from tax_calc_at.parsers.trade_republic import parse as tr_parse

    csv = tmp_path / "tr.csv"
    csv.write_text(
        '"datetime","date","account_type","category","type","asset_class","name","symbol","shares","price","amount","fee","tax","currency","original_amount","original_currency","fx_rate","description","transaction_id","counterparty_name","counterparty_iban","payment_reference","mcc_code"\n'
        '"2025-01-02T10:00:00.000Z","2025-01-02","DEFAULT","TRADING","STOCKPERK","ETF","Physical Gold USD (Acc)","IE00B4ND3602","0.123","70.73","8.70","0","0","EUR","","","","","ref-1","","","",""\n',
        encoding="utf-8",
    )
    txns, _ = tr_parse(csv, steuereinfach_from=_date(2025, 4, 29))
    assert len(txns) == 1
    assert txns[0].tx_type is TxType.BONUS_SHARE
    assert txns[0].quantity == Decimal("0.123")
    assert not any(f.code == "tr.stockperk_paired_buy" for f in txns[0].flags)


def test_yaml_schema_validation_requires_full_txtype_coverage(tmp_path):
    """Missing a TxType in classification → hard error at load time."""
    from tax_calc_at.engine.rules import load_tax_rules as load

    p = tmp_path / "tax_9998.yaml"
    p.write_text(
        """
year: 9998
rates: {kapitalvermoegen_general: 0.275, zinsen_geldeinlagen: 0.25}
foreign_withholding: {default_creditable_cap: 0.15}
kennzahlen:
  bkt: {nr: 863, label: "x"}
loss_offset:
  cross_broker: true
  cross_bucket_within_275_buckets: [bkt]
classification:
  - when: {tx_type: SELL}
    bucket: bkt
""",
        encoding="utf-8",
    )
    import pytest as _pytest

    with _pytest.raises(ValueError, match="missing rules for TxType values"):
        load(9998, path=p)
