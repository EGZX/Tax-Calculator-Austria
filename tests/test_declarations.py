"""User declarations (rules/*.yaml) applied at report time."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from conftest import mk

from tax_calc_at.declarations import (
    DeclarationError,
    apply_declarations,
    load_declarations,
)
from tax_calc_at.model import (
    FLAG_CA_UNCLASSIFIED,
    FLAG_ROC_LABEL,
    FLAG_WHT_COUNTRY_UNCERTAIN,
    CorporateActionError,
    DepotTransferError,
    Flag,
    Severity,
    TxType,
)
from tax_calc_at.pool import PoolManager

D = Decimal


def _decl(tmp_path: Path, name: str, text: str):
    (tmp_path / name).write_text(text, encoding="utf-8")
    return load_declarations(tmp_path)


def test_repository_templates_are_empty_and_valid():
    d = load_declarations()
    assert (d.withholding_countries, d.return_of_capital, d.corporate_actions, d.transfers) == (
        {}, [], [], [])


def test_withholding_country_declaration(tmp_path):
    d = _decl(tmp_path, "withholding_countries.yaml",
              "withholding_countries:\n  US8740391003: TW\n")
    tx = mk(TxType.DIVIDEND_CASH, isin="US8740391003", country="US")
    tx.flags.append(Flag(FLAG_WHT_COUNTRY_UNCERTAIN, Severity.WARNING, "x"))
    apply_declarations([tx], d)
    assert tx.withholding_country == "TW"
    assert FLAG_WHT_COUNTRY_UNCERTAIN not in {f.code for f in tx.flags}


def test_unmatched_declaration_fails(tmp_path):
    d = _decl(tmp_path, "withholding_countries.yaml",
              "withholding_countries:\n  US0000000000: TW\n")
    with pytest.raises(DeclarationError, match="no transactions"):
        apply_declarations([mk(TxType.DIVIDEND_CASH)], d)


def test_return_of_capital_requires_evidence_and_converts(tmp_path):
    with pytest.raises(DeclarationError):
        _decl(tmp_path, "return_of_capital.yaml",
              "return_of_capital:\n  - {broker: trading212, isin: US0378331005}\n")
    d = _decl(tmp_path, "return_of_capital.yaml",
              "return_of_capital:\n  - {broker: trading212, isin: US0378331005, "
              "evidence: 'issuer resolution'}\n")
    tx = mk(TxType.DIVIDEND_CASH, broker="trading212")
    tx.flags.append(Flag(FLAG_ROC_LABEL, Severity.WARNING, "x"))
    apply_declarations([tx], d)
    assert tx.tx_type is TxType.RETURN_OF_CAPITAL
    assert FLAG_ROC_LABEL not in {f.code for f in tx.flags}


def test_spinoff_declaration_moves_basis(tmp_path):
    parent, child = "US3696043013", "US36266G1076"
    d = _decl(tmp_path, "corporate_actions.yaml", f"""
corporate_actions:
  - {{broker: scalable_capital, date: 2023-01-04, isin: {child}, treatment: spinoff,
     parent_isin: {parent}, basis_fraction: "0.25"}}
""")
    txns = [
        mk(TxType.BUY, day=date(2023, 1, 2), qty="4", amount_eur="-400", isin=parent),
        mk(TxType.SPLIT, day=date(2023, 1, 4), qty="1", isin=child, line=2),
    ]
    apply_declarations(txns, d)
    pm = PoolManager()
    pm.replay(txns)
    pools = pm.by_broker["scalable_capital"].by_isin
    assert pools[parent].total_cost_eur == 300 and pools[child].total_cost_eur == 100


def test_taxable_distribution_declaration(tmp_path):
    d = _decl(tmp_path, "corporate_actions.yaml", """
corporate_actions:
  - {broker: scalable_capital, date: 2024-03-01, isin: US0378331005,
     treatment: taxable_distribution, market_value_eur: "42.50"}
""")
    tx = mk(TxType.SPLIT, day=date(2024, 3, 1), qty="2")
    apply_declarations([tx], d)
    assert tx.tx_type is TxType.DIVIDEND_STOCK and tx.amount_eur == D("42.50")


def _unclassified(tx):
    tx.flags.append(Flag(FLAG_CA_UNCLASSIFIED, Severity.ERROR, "declare me"))
    return tx


def test_split_declaration_keeps_basis_and_clears_blocker(tmp_path):
    d = _decl(tmp_path, "corporate_actions.yaml", """
corporate_actions:
  - {broker: scalable_capital, date: 2025-06-18, isin: US0378331005, treatment: split}
""")
    txns = [mk(TxType.BUY, day=date(2025, 1, 2), qty="5", amount_eur="-600.00"),
            _unclassified(mk(TxType.SPLIT, day=date(2025, 6, 18), qty="15", line=2))]
    apply_declarations(txns, d)
    assert txns[1].tx_type is TxType.SPLIT
    assert FLAG_CA_UNCLASSIFIED not in {f.code for f in txns[1].flags}
    pm = PoolManager()
    pm.replay(txns)
    st = pm.by_broker["scalable_capital"].by_isin["US0378331005"]
    assert (st.quantity, st.total_cost_eur) == (D("20"), D("600.00"))


def test_partly_taxable_bonus_issue(tmp_path):
    """BYD 2025 shape: one +20:10 booking = 12:10 from the capital reserve
    (neutral) + 8:10 bonus shares out of profits (income at market value,
    which is also their cost). Synthetic amounts."""
    d = _decl(tmp_path, "corporate_actions.yaml", """
corporate_actions:
  - {broker: scalable_capital, date: 2025-06-10, isin: CNE100000296,
     treatment: taxable_distribution, quantity: "8", market_value_eur: "116.00"}
""")
    isin = "CNE100000296"
    txns = [mk(TxType.BUY, day=date(2025, 1, 2), qty="10", amount_eur="-400.00", isin=isin),
            _unclassified(mk(TxType.SPLIT, day=date(2025, 6, 10), qty="20", isin=isin,
                             line=2, raw_ref="WWUM 3")),
            mk(TxType.SELL, day=date(2025, 12, 8), qty="30", amount_eur="450.00", isin=isin,
               line=3)]
    apply_declarations(txns, d)
    income = [t for t in txns if t.tx_type is TxType.DIVIDEND_STOCK]
    assert len(income) == 1 and income[0].quantity == D("8")
    assert income[0].amount_eur == D("116.00") and income[0].raw_ref == "WWUM 3#taxable"
    split = next(t for t in txns if t.tx_type is TxType.SPLIT)
    assert split.quantity == D("12")
    assert not any(f.code == FLAG_CA_UNCLASSIFIED for t in txns for f in t.flags)
    pm = PoolManager()
    pm.replay(txns)
    (ev,) = pm.realized_events()
    assert ev.cost_basis_eur == D("516.00") and ev.pnl_eur == D("-66.00")


def test_taxable_quantity_validation(tmp_path):
    with pytest.raises(DeclarationError, match="only valid for taxable_distribution"):
        _decl(tmp_path, "corporate_actions.yaml", """
corporate_actions:
  - {broker: scalable_capital, date: 2025-06-10, isin: US0378331005, treatment: split,
     quantity: "1"}
""")
    d = _decl(tmp_path, "corporate_actions.yaml", """
corporate_actions:
  - {broker: scalable_capital, date: 2025-06-10, isin: US0378331005,
     treatment: taxable_distribution, quantity: "3", market_value_eur: "10"}
""")
    with pytest.raises(CorporateActionError, match="exceeds"):
        apply_declarations([mk(TxType.SPLIT, day=date(2025, 6, 10), qty="2")], d)


def test_corporate_action_declaration_must_match_one_leg(tmp_path):
    d = _decl(tmp_path, "corporate_actions.yaml", """
corporate_actions:
  - {broker: scalable_capital, date: 2024-03-01, isin: US0378331005,
     treatment: taxable_distribution, market_value_eur: "1"}
""")
    with pytest.raises(CorporateActionError, match="exactly"):
        apply_declarations([], d)


TRANSFER = """
transfers:
  - id: t1
    notified: {notified}
    out: {{broker: scalable_capital, date_from: 2025-12-05, date_to: 2025-12-05}}
    in: {{broker: {dest}, date_from: 2025-12-06, date_to: 2025-12-06}}
    isins: ["*"]
"""


def _transfer_txns(dest="scalable_capital", in_qty="5"):
    return [
        mk(TxType.BUY, day=date(2025, 1, 2), qty="5", amount_eur="-500"),
        mk(TxType.MIGRATION_OUT, day=date(2025, 12, 5), qty="5", line=2, price_native=D("130")),
        mk(TxType.MIGRATION_IN, broker=dest, day=date(2025, 12, 6), qty=in_qty, line=3,
           price_native=D("131")),
    ]


def test_notified_transfer_is_neutral(tmp_path):
    d = _decl(tmp_path, "depot_transfers.yaml", TRANSFER.format(notified="true", dest="ibkr"))
    txns = _transfer_txns("ibkr")
    apply_declarations(txns, d)
    pm = PoolManager()
    pm.replay(txns)
    assert pm.realized_events() == []
    assert pm.by_broker["ibkr"].by_isin["US0378331005"].total_cost_eur == 500


def test_not_notified_transfer_is_deemed_sale_at_row_valuation(tmp_path):
    d = _decl(tmp_path, "depot_transfers.yaml", TRANSFER.format(notified="false", dest="ibkr"))
    txns = _transfer_txns("ibkr")
    apply_declarations(txns, d)
    pm = PoolManager()
    pm.replay(txns)
    (ev,) = pm.realized_events()
    assert ev.kind == "deemed_sale" and ev.proceeds_eur == D("650") and ev.pnl_eur == D("150")
    # The receiving depot takes over the deemed-sale value (Rz 6160a), not
    # its own next-day valuation (131 × 5).
    assert pm.by_broker["ibkr"].by_isin["US0378331005"].total_cost_eur == D("650")


def test_transfer_quantity_mismatch_fails(tmp_path):
    d = _decl(tmp_path, "depot_transfers.yaml", TRANSFER.format(notified="true", dest="ibkr"))
    with pytest.raises(DepotTransferError, match="differ"):
        apply_declarations(_transfer_txns("ibkr", in_qty="4"), d)


def test_transfer_in_only_needs_cost_basis(tmp_path):
    d = _decl(tmp_path, "depot_transfers.yaml", """
transfers:
  - id: from-austrian-bank
    notified: true
    in: {broker: ibkr, date_from: 2025-12-06, date_to: 2025-12-06}
    isins: [US0378331005]
    cost_basis_eur: {US0378331005: "480.00"}
""")
    txns = [mk(TxType.MIGRATION_IN, broker="ibkr", day=date(2025, 12, 6), qty="5")]
    apply_declarations(txns, d)
    assert txns[0].declared_cost_eur == D("480.00")


def test_local_overlay_extends_public_templates(tmp_path):
    (tmp_path / "withholding_countries.yaml").write_text(
        "withholding_countries:\n  US8740391003: TW\n", encoding="utf-8")
    (tmp_path / "local").mkdir()
    (tmp_path / "local" / "withholding_countries.yaml").write_text(
        "withholding_countries:\n  US0595781040: BR\n", encoding="utf-8")
    d = load_declarations(tmp_path)
    assert d.withholding_countries == {"US8740391003": "TW", "US0595781040": "BR"}
    (tmp_path / "local" / "withholding_countries.yaml").write_text(
        "withholding_countries:\n  US8740391003: US\n", encoding="utf-8")
    with pytest.raises(DeclarationError, match="conflicting"):
        load_declarations(tmp_path)


def test_local_broker_and_asset_class_overrides(tmp_path, monkeypatch):
    import tax_calc_at.engine.rules as rules_mod
    from tax_calc_at.engine.rules import load_brokers
    from tax_calc_at.model import AssetClass
    from tax_calc_at.parsers.base import asset_class_override_for, reset_overrides_cache

    monkeypatch.setattr(rules_mod, "LOCAL_RULES_DIR", tmp_path)
    (tmp_path / "brokers.yaml").write_text(
        "brokers:\n  trade_republic: {steuereinfach_from: 2025-05-02}\n", encoding="utf-8")
    (tmp_path / "asset_class_overrides.yaml").write_text(
        "overrides:\n  IE00B4ND3602: DERIVATIVE\n", encoding="utf-8")
    reset_overrides_cache()
    cfg = load_brokers().get("trade_republic")
    assert cfg.steuereinfach_from == date(2025, 5, 2) and cfg.credit_institution is True
    assert asset_class_override_for("IE00B4ND3602") is AssetClass.DERIVATIVE
    assert asset_class_override_for("LU2290522684") is AssetClass.STOCK  # public file


def test_notified_basis_differing_from_computed_basis_is_reported(tmp_path):
    """The computed (Austrian) basis is carried; the notified figure only
    triggers a reminder to correct the records."""
    d = _decl(tmp_path, "depot_transfers.yaml", TRANSFER.format(notified="true", dest="ibkr")
              + '    notified_cost_basis_eur: {US0378331005: "900.00"}\n')
    txns = _transfer_txns("ibkr")
    apply_declarations(txns, d)
    pm = PoolManager()
    pm.replay(txns)
    assert pm.by_broker["ibkr"].by_isin["US0378331005"].total_cost_eur == 500
    ((_, msg),) = pm.basis_mismatches
    assert "500.0000 EUR" in msg and "900.0000 EUR" in msg


def test_transfer_split_into_notified_and_wrong_data_isins(tmp_path):
    """Rz 6166a: wrong data for one security → only that one is a deemed sale."""
    other = "US5949181045"
    d = _decl(tmp_path, "depot_transfers.yaml", """
transfers:
  - id: all-but-one
    notified: true
    out: {broker: scalable_capital, date_from: 2025-12-05, date_to: 2025-12-05}
    in: {broker: ibkr, date_from: 2025-12-06, date_to: 2025-12-06}
    isins: ["*"]
    exclude_isins: [US0378331005]
  - id: wrong-cost-basis
    notified: false
    out: {broker: scalable_capital, date_from: 2025-12-05, date_to: 2025-12-05}
    in: {broker: ibkr, date_from: 2025-12-06, date_to: 2025-12-06}
    isins: [US0378331005]
""")
    txns = _transfer_txns("ibkr") + [
        mk(TxType.BUY, day=date(2025, 1, 2), qty="1", amount_eur="-300", isin=other, line=4),
        mk(TxType.MIGRATION_OUT, day=date(2025, 12, 5), qty="1", isin=other, line=5),
        mk(TxType.MIGRATION_IN, broker="ibkr", day=date(2025, 12, 6), qty="1", isin=other, line=6),
    ]
    apply_declarations(txns, d)
    pm = PoolManager()
    pm.replay(txns)
    (ev,) = pm.realized_events()
    assert ev.isin == "US0378331005" and ev.kind == "deemed_sale"
    assert pm.by_broker["ibkr"].by_isin[other].total_cost_eur == 300
