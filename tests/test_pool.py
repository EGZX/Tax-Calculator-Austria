"""Pool engine: moving average, corporate actions, transfers, FX ledger."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest
from conftest import mk

from tax_calc_at.model import (
    AssetClass,
    CorporateActionError,
    CostBasisMissingError,
    DepotTransferError,
    OversellError,
    TxType,
)
from tax_calc_at.pool import PoolManager

D = Decimal
ISIN = "US88160R1014"


def _t(tx_type, day, qty="0", eur="0", **kw):
    return mk(tx_type, day=date(2024, 1, day), qty=qty, amount_eur=eur,
              isin=kw.pop("isin", ISIN), line=kw.pop("line", day), **kw)


def _pool(pm, broker="scalable_capital", isin=ISIN):
    return pm.by_broker[broker].by_isin[isin]


# ------------------------------------------------------------ averages
def test_rolling_average_buy_sell():
    pm = PoolManager()
    pm.replay([
        _t(TxType.BUY, 1, "10", "-1000"),
        _t(TxType.BUY, 2, "10", "-1500"),
        _t(TxType.SELL, 3, "5", "750"),
    ])
    (ev,) = pm.realized_events()
    assert (ev.cost_basis_eur, ev.proceeds_eur, ev.pnl_eur) == (D("625"), D("750"), D("125"))
    assert _pool(pm).quantity == 15 and _pool(pm).avg_cost_eur == D("125")


def test_oversell_raises():
    with pytest.raises(OversellError):
        PoolManager().replay([_t(TxType.BUY, 1, "1", "-100"), _t(TxType.SELL, 2, "2", "250")])


def test_per_broker_pools_never_merged():
    pm = PoolManager()
    pm.replay([
        _t(TxType.BUY, 1, "10", "-1000", broker="A"),
        _t(TxType.BUY, 2, "10", "-2000", broker="B"),
    ])
    assert _pool(pm, "A").avg_cost_eur == 100 and _pool(pm, "B").avg_cost_eur == 200


def test_fees_never_touch_basis_or_proceeds():
    """§ 27a Abs 4 Z 2 / § 20 Abs 2 EStG."""
    pm = PoolManager()
    pm.replay([
        _t(TxType.BUY, 1, "10", "-1000", fee_native=D("5")),
        _t(TxType.SELL, 2, "5", "600", fee_native=D("3")),
    ])
    (ev,) = pm.realized_events()
    assert (ev.proceeds_eur, ev.cost_basis_eur, ev.pnl_eur) == (D("600"), D("500"), D("100"))


def test_no_drift_after_partial_sells():
    pm = PoolManager()
    pm.replay([_t(TxType.BUY, 1, "3", "-100")] + [
        _t(TxType.SELL, d, "1", "50") for d in (2, 3, 4)
    ])
    assert _pool(pm).quantity == 0 and _pool(pm).total_cost_eur == 0
    # Per-event figures are rounded to 4 dp; the pool itself is exact.
    assert abs(sum(e.cost_basis_eur for e in pm.realized_events()) - 100) <= D("0.0003")


def test_same_day_order_uses_local_time():
    """tz-aware times are compared in Vienna time."""
    from zoneinfo import ZoneInfo

    utc = ZoneInfo("UTC")
    buy = _t(TxType.BUY, 5, "1", "-100", at=datetime(2024, 1, 5, 9, 0, tzinfo=utc), line=9)
    sell = _t(TxType.SELL, 5, "1", "110", at=datetime(2024, 1, 5, 10, 0, tzinfo=utc), line=1)
    pm = PoolManager()
    pm.replay([sell, buy])
    assert pm.realized_events()[0].pnl_eur == D("10")


# ----------------------------------------------------- corporate actions
def test_split_preserves_basis():
    pm = PoolManager()
    pm.replay([_t(TxType.BUY, 1, "1", "-771.50"), _t(TxType.SPLIT, 2, "9")])
    assert _pool(pm).quantity == 10 and _pool(pm).total_cost_eur == D("771.50")


def test_split_on_empty_position_is_rejected():
    """Regression: a spin-off/new-ISIN leg silently became a zero-cost lot."""
    with pytest.raises(CorporateActionError, match="holds no position"):
        PoolManager().replay([_t(TxType.SPLIT, 2, "10")])


def test_split_cannot_eliminate_position():
    """Regression: an unpaired out-leg kept its cost on a dead ISIN, and the
    next BUY inherited it."""
    with pytest.raises(CorporateActionError, match="cannot eliminate"):
        PoolManager().replay([_t(TxType.BUY, 1, "4", "-400"), _t(TxType.SPLIT, 2, "-4")])


def test_exchange_carries_basis_to_new_isin():
    """ISIN change (KapMaßVO § 8): 3 old → 12 new shares (synthetic)."""
    old, new = "US0404131064", "US0404132054"
    pm = PoolManager()
    pm.replay([
        _t(TxType.BUY, 1, "3", "-750.00", isin=old),
        _t(TxType.EXCHANGE_OUT, 2, "3", isin=old, link_id="L"),
        _t(TxType.EXCHANGE_IN, 2, "12", isin=new, link_id="L", line=1),
        _t(TxType.SELL, 3, "12", "800", isin=new),
    ])
    assert _pool(pm, isin=old).quantity == 0
    (ev,) = pm.realized_events()
    assert ev.cost_basis_eur == D("750.00") and ev.pnl_eur == D("50.00")


def test_unmatched_exchange_out_fails_loudly():
    with pytest.raises(CorporateActionError, match="never matched"):
        PoolManager().replay([
            _t(TxType.BUY, 1, "4", "-400"),
            _t(TxType.EXCHANGE_OUT, 2, "4", link_id="L"),
        ])


def test_spinoff_moves_basis_fraction():
    child = "US36266G1076"
    pm = PoolManager()
    pm.replay([
        _t(TxType.BUY, 1, "10", "-1000"),
        _t(TxType.SPINOFF_IN, 2, "3", isin=child, link_id=ISIN, basis_fraction=D("0.2")),
    ])
    assert _pool(pm).total_cost_eur == D("800") and _pool(pm).quantity == 10
    assert _pool(pm, isin=child).total_cost_eur == D("200")


@pytest.mark.parametrize("kind", [TxType.BONUS_SHARE, TxType.DIVIDEND_STOCK])
def test_shares_received_as_income_have_market_value_basis(kind):
    pm = PoolManager()
    pm.replay([_t(kind, 1, "2", "50")])
    assert _pool(pm).total_cost_eur == D("50")


def test_return_of_capital_reduces_basis_and_realizes_excess():
    pm = PoolManager()
    pm.replay([
        _t(TxType.BUY, 1, "10", "-100"),
        _t(TxType.RETURN_OF_CAPITAL, 2, "0", "60"),
        _t(TxType.RETURN_OF_CAPITAL, 3, "0", "60"),
    ])
    assert _pool(pm).total_cost_eur == 0
    (ev,) = pm.realized_events()
    assert ev.kind == "roc_excess" and ev.pnl_eur == D("20")


# ------------------------------------------------------------ transfers
def test_undeclared_transfer_out_is_rejected():
    with pytest.raises(DepotTransferError, match="deemed sale"):
        PoolManager().replay([
            _t(TxType.BUY, 1, "5", "-500"),
            _t(TxType.MIGRATION_OUT, 2, "5"),
        ])


def test_notified_transfer_carries_basis_across_brokers():
    pm = PoolManager()
    out = _t(TxType.MIGRATION_OUT, 2, "5", link_id="T", declared=True)
    pm.replay([
        _t(TxType.BUY, 1, "10", "-1000"),
        out,
        _t(TxType.MIGRATION_IN, 3, "5", broker="ibkr", link_id="T"),
        _t(TxType.SELL, 4, "5", "600", broker="ibkr"),
    ])
    assert _pool(pm).total_cost_eur == D("500")
    (ev,) = pm.realized_events()
    assert ev.broker == "ibkr" and ev.cost_basis_eur == D("500")


def test_not_notified_transfer_is_deemed_sale():
    pm = PoolManager()
    pm.replay([
        _t(TxType.BUY, 1, "5", "-500"),
        _t(TxType.MIGRATION_OUT, 2, "5", declared=True, deemed_sale_eur=D("650")),
        _t(TxType.MIGRATION_IN, 2, "5", broker="ibkr", declared=True,
           declared_cost_eur=D("650"), line=1),
    ])
    (ev,) = pm.realized_events()
    assert ev.kind == "deemed_sale" and ev.pnl_eur == D("150")
    assert _pool(pm, "ibkr").total_cost_eur == D("650")


def test_undeclared_transfer_in_blocks_sale():
    with pytest.raises(CostBasisMissingError):
        PoolManager().replay([_t(TxType.MIGRATION_IN, 1, "5"), _t(TxType.SELL, 2, "2", "400")])


def test_tolerant_mode_keeps_results_before_error():
    """Regression: an error in a later year wiped earlier realized gains."""
    pm = PoolManager()
    pm.replay([
        _t(TxType.BUY, 1, "10", "-1000"),
        _t(TxType.SELL, 2, "5", "600"),
        _t(TxType.MIGRATION_OUT, 3, "5"),  # undeclared → error
        _t(TxType.SELL, 4, "5", "700"),
    ], on_error="collect")
    assert [e.pnl_eur for e in pm.realized_events()] == [D("100")]
    assert pm.errors[0][3] == date(2024, 1, 3)


# ------------------------------------------------------------ FX ledger
def _fx(day, ccy_amt, eur_amt, line, ccy="USD", at=None):
    """Conversion: +ccy_amt USD bought for -eur_amt EUR (actual rate)."""
    tx = mk(TxType.FX_CONVERSION, day=date(2024, 1, day), amount_eur=str(abs(D(eur_amt))),
            currency=ccy, gross_native=ccy_amt, rate="0.9", isin=None,
            asset_class=AssetClass.CASH, line=line, at=at, counter_currency="EUR",
            counter_amount=D(eur_amt))
    return tx


def test_fx_balance_disposal_realizes_gain():
    """EStR Rz 6201b Beispiel 3: USD bought for 900 EUR, spent on a security
    worth 1000 EUR → FX gain 100; the security's basis is 1000."""
    pm = PoolManager()
    pm.replay([
        _fx(1, "1100", "-900", 1),
        mk(TxType.BUY, day=date(2024, 1, 5), qty="1", amount_eur="-1000", currency="USD",
           gross_native="-1100", rate=str(D(1000) / D(1100)), line=2, isin=ISIN),
    ])
    (ev,) = pm.realized_events()
    assert ev.kind == "fx" and ev.pnl_eur == D("100.0000")
    assert _pool(pm).total_cost_eur == D("1000")


def test_fx_conversion_back_to_eur_uses_actual_rate():
    """Rz 6201c: actual conversion into EUR is valued at the actual rate."""
    pm = PoolManager()
    pm.replay([
        _fx(1, "1100", "-900", 1),
        _fx(9, "-1100", "1000", 2),  # USD → EUR, 1000 EUR received
    ])
    (ev,) = pm.realized_events()
    assert ev.pnl_eur == D("100.0000") and ev.name.endswith("tatsächlicher Kurs)")


def test_fx_debt_effects_are_not_taxable():
    """Spending USD not held = borrowing; repaying later realizes nothing."""
    pm = PoolManager()
    pm.replay([
        mk(TxType.BUY, day=date(2024, 1, 2), qty="1", amount_eur="-900", currency="USD",
           gross_native="-1000", rate="0.9", line=1),
        _fx(3, "1000", "-950", 2),  # repay the USD debt at a worse rate
    ])
    assert pm.realized_events() == []
    st = pm.by_broker["scalable_capital"].fx["USD"]
    assert st.quantity == 0 and st.liability == 0


def test_fx_to_floating_fx_carries_basis_but_stable_realizes():
    def conv(ccy_in, amt_in, eur_value, line):
        return mk(TxType.FX_CONVERSION, day=date(2024, 1, 5), amount_eur=eur_value,
                  currency=ccy_in, gross_native=amt_in, rate="0.1", isin=None,
                  asset_class=AssetClass.CASH, line=line, counter_currency="USD",
                  counter_amount=D("-1100"))

    floating = PoolManager(eur_stable_currencies=frozenset({"DKK"}))
    floating.replay([_fx(1, "1100", "-900", 1), conv("JPY", "150000", "1000", 2)])
    assert floating.realized_events() == []
    assert floating.by_broker["scalable_capital"].fx["JPY"].total_cost_eur == D("900")

    stable = PoolManager(eur_stable_currencies=frozenset({"DKK"}))
    stable.replay([_fx(1, "1100", "-900", 1), conv("DKK", "7460", "1000", 2)])
    (ev,) = stable.realized_events()
    assert ev.pnl_eur == D("100.0000")


def test_conversion_before_purchase_and_after_sale_same_timestamp():
    at = datetime(2024, 1, 5, 15, 0)
    rate = str(D(1000) / D(1100))
    pm = PoolManager()
    pm.replay([
        mk(TxType.BUY, day=date(2024, 1, 5), qty="1", amount_eur="-1000", currency="USD",
           gross_native="-1100", rate=rate, line=1, at=at),
        _fx(5, "1100", "-990", 2, at=at),  # must run first
    ])
    (ev,) = pm.realized_events()
    assert ev.kind == "fx" and ev.pnl_eur == D("10.0000")
