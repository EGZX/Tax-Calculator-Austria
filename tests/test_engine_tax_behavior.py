"""E1kv engine: Kennzahlen, gross reporting, foreign-tax credit, loss offset."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from conftest import mk

from tax_calc_at.engine.e1kv import build_report
from tax_calc_at.engine.rules import load_tax_rules
from tax_calc_at.model import (
    FLAG_CA_UNCLASSIFIED,
    FLAG_PAYMENT_IN_LIEU,
    FLAG_WHT_COUNTRY_UNCERTAIN,
    FLAG_WHT_NOT_DISCLOSED,
    AssetClass,
    Flag,
    ReportNotFileableError,
    Severity,
    TxType,
)
from tax_calc_at.pool import RealizedEvent

D = Decimal


def _report(txns, realized=(), year=2024, brokers=None):
    from tax_calc_at.engine.rules import load_brokers

    return build_report(
        year=year,
        rules=load_tax_rules(year),
        transactions=list(txns),
        realized=list(realized),
        brokers=brokers or load_brokers(),
    )


def _div(gross, wh="0", country="US", *, net=False, isin="US0378331005", line=1, **kw):
    """Dividend with GROSS (net=False) or NET (net=True) amount_eur."""
    amount = D(gross) - D(wh) if net else D(gross)
    return mk(TxType.DIVIDEND_CASH, amount_eur=str(amount), withheld_eur=wh, country=country,
              is_net=net, isin=isin, line=line, **kw)


def _ev(pnl, asset_class=AssetClass.STOCK, broker="scalable_capital", isin="US0378331005"):
    return RealizedEvent(
        trade_date=date(2024, 7, 1), broker=broker, isin=isin, symbol=None, name=None,
        asset_class=asset_class, quantity=D("1"), proceeds_eur=D("100"),
        cost_basis_eur=D("100") - D(pnl), pnl_eur=D(pnl), source_ref="t.csv:9",
    )


# ------------------------------------------------------------- Kennzahlen
def test_kennzahl_numbers_match_bmf_form():
    """KZ verified against the BMF E 1kv 2023–2025 forms (foreign column)."""
    for year in (2023, 2024, 2025):
        nrs = {k: v.nr for k, v in load_tax_rules(year).kennzahlen.items()}
        assert nrs == {
            "zinsen_kreditinstitute_25": 861,  # was wrongly 857 (tariff income!)
            "ueberlassung_27_5": 863,
            "realisiert_ueberschuesse_27_5": 994,
            "realisiert_ueberschuesse_25": 865,
            "realisiert_verluste": 892,
            "derivate_ueberschuesse_27_5": 995,
            "derivate_verluste": 896,
            "fonds_ausschuettungen_27_5": 898,
            "fonds_age_27_5": 937,
            "anrechenbare_qst_27_5": 998,
            "anrechenbare_qst_25": 901,  # was wrongly 799 (not on the form)
        }


def test_2026_form_unpublished_blocks_filing():
    rep = _report([mk(TxType.DIVIDEND_CASH, day=date(2026, 3, 1), amount_eur="100")], year=2026)
    assert any("Unverified Kennzahl" in b for b in rep.health.blockers)


def test_interest_bank_vs_non_bank(brokers):
    """25 % only for deposits at a credit institution (§ 27a Abs 1 Z 1)."""
    rep = _report([
        mk(TxType.INTEREST, broker="trade_republic", amount_eur="10", isin=None,
           asset_class=AssetClass.CASH, line=1),
        mk(TxType.INTEREST, broker="ibkr", amount_eur="7", isin=None,
           asset_class=AssetClass.CASH, line=2),
        mk(TxType.INTEREST_OTHER, broker="trade_republic", amount_eur="3", line=3),
    ])
    flat = rep.by_kennzahl()
    assert flat[861] == D("10.00")
    assert flat[863] == D("10.00")  # IBKR interest 7 + coupon 3


def test_cashback_and_promo_bonus_are_income():
    rep = _report([
        mk(TxType.CASHBACK, broker="trade_republic", amount_eur="3.00", isin=None,
           asset_class=AssetClass.CASH, line=1),  # Saveback → 861 (Rz 6134)
        mk(TxType.CASHBACK, broker="trading212", amount_eur="1.00", isin=None,
           asset_class=AssetClass.CASH, line=2),  # non-bank → 863
        mk(TxType.PROMO_BONUS, broker="trading212", amount_eur="10.00", isin=None,
           asset_class=AssetClass.CASH, line=3),  # free-share promo → 863 (Rz 6132)
    ])
    flat = rep.by_kennzahl()
    assert flat[861] == D("3.00")
    assert flat[863] == D("11.00")


def test_fund_distribution_goes_to_898_not_863():
    rep = _report([_div("20", asset_class=AssetClass.ETF, isin="IE00B4L5Y983", country="IE")])
    assert rep.by_kennzahl(allow_partial=True) == {898: D("20.00")}


def test_realized_routing_gain_loss_derivative_fx():
    rep = _report([], realized=[
        _ev("50"), _ev("-20"),
        _ev("30", AssetClass.DERIVATIVE), _ev("-5", AssetClass.DERIVATIVE),
        _ev("-1", AssetClass.CASH, broker="ibkr"),
        _ev("2", AssetClass.CASH, broker="trade_republic"),  # bank deposit → 25 %
    ])
    assert rep.by_kennzahl() == {
        994: D("50.00"), 892: D("-21.00"), 995: D("30.00"), 896: D("-5.00"), 865: D("2.00"),
    }


# ------------------------------------------------------- gross reporting
def test_losses_are_not_netted_into_dividends():
    """Regression: the engine used to subtract losses from KZ 863 and shrink
    KZ 892, which also destroyed the KZ 998 credit. The form wants gross
    figures; the Finanzamt nets (Anm 19)."""
    rep = _report([_div("300")], realized=[_ev("1000"), _ev("-500")])
    flat = rep.by_kennzahl()
    assert flat[863] == D("300.00")
    assert flat[994] == D("1000.00")
    assert flat[892] == D("-500.00")
    assert rep.loss_offset.saldo_1_3_eur == D("800.00")


# ------------------------------------------------------------ credit math
def test_net_dividend_is_grossed_up():
    rep = _report([_div("100", "15", net=True)])
    assert rep.by_kennzahl() == {863: D("100.00"), 998: D("15.00")}


def test_gross_dividend_is_not_grossed_up_twice():
    """Regression (IBKR/TR): gross rows flagged NET added the tax twice."""
    rep = _report([_div("100", "15", net=False)])
    assert rep.by_kennzahl() == {863: D("100.00"), 998: D("15.00")}


@pytest.mark.parametrize(
    "country,wh,credit,excess",
    [
        ("US", "30", "15.00", "15.00"),
        ("FI", "35", "10.00", "25.00"),  # was 15 % in the old table
        ("SE", "30", "10.00", "20.00"),  # was 15 % in the old table
        ("JP", "15.315", "10.00", "5.32"),
        ("CH", "35", "15.00", "20.00"),
        ("DK", "27", "15.00", "12.00"),
        ("IE", "25", "0.00", "25.00"),  # DBA: exempt at source → refundable only
    ],
)
def test_dba_caps(country, wh, credit, excess):
    rep = _report([_div("100", wh, country)])
    assert rep.creditable_withholding.get("anrechenbare_qst_27_5", D("0")) == D(credit)
    assert rep.uncreditable_withholding["anrechenbare_qst_27_5"] == D(excess)


def test_unknown_country_blocks_instead_of_default_cap():
    rep = _report([_div("100", "20", "KZ", isin="KZ1C00000744")])
    assert any("no DBA dividend rate for KZ" in b for b in rep.health.blockers)
    assert "anrechenbare_qst_27_5" not in rep.creditable_withholding


def test_suspended_treaty_country_blocks():
    rep = _report([_div("100", "15", "RU", isin="RU0009029540")])
    assert any("suspended" in b for b in rep.health.blockers)


def test_adr_without_declared_country_blocks():
    tx = _div("100", "21", "US", isin="US8740391003")
    tx.flags.append(Flag(FLAG_WHT_COUNTRY_UNCERTAIN, Severity.WARNING, "ADR"))
    rep = _report([tx])
    assert any("withholding source country is unknown" in b for b in rep.health.blockers)


def test_payment_in_lieu_not_credited():
    tx = _div("100", "15", "US")
    tx.flags.append(Flag(FLAG_PAYMENT_IN_LIEU, Severity.WARNING, "PIL"))
    rep = _report([tx])
    assert rep.by_kennzahl(allow_partial=True) == {863: D("100.00")}
    assert rep.uncreditable_withholding["anrechenbare_qst_27_5"] == D("15.00")


def test_interest_withholding_not_credited():
    tx = mk(TxType.INTEREST, broker="ibkr", amount_eur="0.11", withheld_eur="0.02",
            is_net=False, isin=None, asset_class=AssetClass.CASH, country="IE")
    rep = _report([tx])
    assert rep.by_kennzahl() == {863: D("0.11")}
    assert any("not creditable" in w for w in rep.health.warnings)


def test_withholding_convention_must_be_declared():
    rep = _report([_div("85", "15", net=None)])
    assert any("NET/GROSS convention" in b for b in rep.health.blockers)


def test_undisclosed_withholding_blocks_unless_cap_zero():
    us = _div("85", "0", "US")
    us.flags.append(Flag(FLAG_WHT_NOT_DISCLOSED, Severity.WARNING, "x"))
    ie = _div("85", "0", "IE", isin="IE00B4L5Y983", line=2)
    ie.flags.append(Flag(FLAG_WHT_NOT_DISCLOSED, Severity.WARNING, "x"))
    assert any("does not disclose" in b for b in _report([us]).health.blockers)
    assert not any("does not disclose" in b for b in _report([ie]).health.blockers)


def test_austrian_kest_dividend_is_excluded():
    """AT dividend at a foreign depot: KESt withheld by the issuer → final."""
    tx = _div("16.20", "4.46", "AT", net=True, isin="AT0000A0E9W5")
    rep = _report([tx])
    assert rep.by_kennzahl() == {}
    assert any("final-taxed" in w for w in rep.health.warnings)


# ------------------------------------------------- loss offset vs. credit
def test_rz_7752h_example_credit_reduced_by_loss_offset():
    """EStR Rz 7752h Beispiel: dividends 100 (WHT 15), gains 50, losses 100.
    Losses first against the gain (50), then 50 of the dividends → only half
    of the dividend keeps its credit: 7.50."""
    rep = _report([_div("100", "15")], realized=[_ev("50"), _ev("-100")])
    flat = rep.by_kennzahl()
    assert flat[863] == D("100.00") and flat[994] == D("50.00") and flat[892] == D("-100.00")
    assert flat[998] == D("7.50")
    assert rep.credit_lost_to_loss_offset["anrechenbare_qst_27_5"] == D("7.50")


def test_loss_offset_uses_income_without_credit_first():
    """WHT-free income (broker interest) absorbs losses before dividends."""
    rep = _report(
        [_div("100", "15"),
         mk(TxType.INTEREST, broker="ibkr", amount_eur="80", isin=None,
            asset_class=AssetClass.CASH, line=2)],
        realized=[_ev("-80")],
    )
    assert rep.creditable_withholding["anrechenbare_qst_27_5"] == D("15.00")


def test_loss_offset_prefers_lowest_credit_ratio():
    """Losses consume the dividend with the lower credit ratio first."""
    rep = _report(
        [_div("100", "15", "US", line=1),
         _div("100", "10", "JP", isin="JP3633400001", line=2)],
        realized=[_ev("-100")],
    )
    # JP (10 %) fully consumed, US (15 %) credit intact.
    assert rep.creditable_withholding["anrechenbare_qst_27_5"] == D("15.00")


def test_unabsorbed_losses_warned():
    rep = _report([], realized=[_ev("-100")])
    assert rep.loss_offset.unabsorbed_eur == D("100.00")
    assert any("lost" in w for w in rep.health.warnings)


def test_25_percent_interest_not_offsettable():
    rep = _report(
        [mk(TxType.INTEREST, broker="trade_republic", amount_eur="50", isin=None,
            asset_class=AssetClass.CASH)],
        realized=[_ev("-50")],
    )
    assert rep.loss_offset.unabsorbed_eur == D("50.00")


# -------------------------------------------------------------- gating
def test_by_kennzahl_refuses_with_blockers():
    rep = _report([_div("100", "20", "KZ", isin="KZ1C00000744")])
    with pytest.raises(ReportNotFileableError):
        rep.by_kennzahl()


def test_row_warnings_surface_in_report():
    tx = _div("10")
    tx.flags.append(Flag("x.custom", Severity.WARNING, "needs review"))
    rep = _report([tx])
    assert any("[x.custom]" in w for w in rep.health.warnings)


# ------------------------------------------------------------ YAML schema
def test_yaml_rejects_unknown_bucket(tmp_path: Path):
    from tax_calc_at.engine.rules import load_tax_rules as load

    p = tmp_path / "tax_9999.yaml"
    p.write_text(
        """
year: 9999
form_source: test
foreign_withholding: {dividend_caps: {"US": 0.15}}
kennzahlen:
  inc: {nr: 863, label: x, kind: income, rate: 0.275}
classification:
  - when: {tx_type: SELL}
    bucket: TYPO
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="unknown bucket 'TYPO'"):
        load(9999, path=p)


def test_yaml_requires_full_txtype_coverage(tmp_path: Path):
    from tax_calc_at.engine.rules import load_tax_rules as load

    p = tmp_path / "tax_9998.yaml"
    p.write_text(
        """
year: 9998
form_source: test
foreign_withholding: {dividend_caps: {}}
kennzahlen:
  inc: {nr: 863, label: x, kind: income, rate: 0.275}
classification:
  - when: {tx_type: DIVIDEND_CASH}
    bucket: inc
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="missing rules for TxType values"):
        load(9998, path=p)


def test_yaml_country_codes_survive_norway_problem():
    """`NO:` unquoted would parse as boolean False."""
    assert load_tax_rules(2024).foreign_withholding.dividend_caps["NO"] == D("0.15")


def test_undeclared_share_increase_blocks_its_year_and_later_years():
    tx = mk(TxType.SPLIT, day=date(2024, 6, 10), qty="10")
    tx.flags.append(Flag(FLAG_CA_UNCLASSIFIED, Severity.ERROR, "declare me"))
    assert any(FLAG_CA_UNCLASSIFIED in b for b in _report([tx], year=2024).health.blockers)
    later = _report([tx], year=2025).health.blockers
    assert any("earlier corporate action" in b for b in later)
