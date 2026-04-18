"""Pool engine: rolling-average cost basis, oversell, migration, splits."""
from datetime import date
from decimal import Decimal

import pytest

from tax_calc_at.model import (
    AssetClass,
    CostBasisMissingError,
    FxSource,
    OversellError,
    Transaction,
    TxType,
)
from tax_calc_at.pool import PoolManager


def _eur_tx(
    *,
    broker: str,
    day: int,
    tx_type: TxType,
    qty: Decimal,
    gross: Decimal,
    fee: Decimal = Decimal("0"),
    isin: str = "US88160R1014",
) -> Transaction:
    tx = Transaction(
        broker=broker,
        trade_date=date(2024, 1, day),
        tx_type=tx_type,
        asset_class=AssetClass.STOCK,
        quantity=qty,
        currency_native="EUR",
        gross_native=gross,
        source_file="t.csv",
        source_line=day,
        isin=isin,
        fee_native=fee,
    )
    tx.amount_eur = gross
    tx.fee_eur = fee
    tx.tax_withheld_eur = Decimal("0")
    tx.fx_rate_used = Decimal("1")
    tx.fx_rate_source = FxSource.NATIVE_EUR
    return tx


def test_rolling_average_buy_sell():
    pm = PoolManager()
    pm.replay(
        [
            _eur_tx(broker="b", day=1, tx_type=TxType.BUY, qty=Decimal("10"), gross=Decimal("-1000")),
            _eur_tx(broker="b", day=2, tx_type=TxType.BUY, qty=Decimal("10"), gross=Decimal("-1500")),
            # avg should now be 125 EUR/share
            _eur_tx(broker="b", day=3, tx_type=TxType.SELL, qty=Decimal("5"), gross=Decimal("750")),
        ]
    )
    realized = pm.realized_events()
    assert len(realized) == 1
    ev = realized[0]
    # cost basis = 5 * 125 = 625; proceeds = 750; PnL = 125
    assert ev.cost_basis_eur == Decimal("625.0000")
    assert ev.proceeds_eur == Decimal("750.0000")
    assert ev.pnl_eur == Decimal("125.0000")
    # Pool should retain avg = 125 on the remaining 15 shares
    state = pm.by_broker["b"].by_isin["US88160R1014"]
    assert state.quantity == Decimal("15")
    assert state.avg_cost_eur == Decimal("125.0000")


def test_oversell_raises():
    pm = PoolManager()
    with pytest.raises(OversellError):
        pm.replay(
            [
                _eur_tx(broker="b", day=1, tx_type=TxType.BUY, qty=Decimal("1"), gross=Decimal("-100")),
                _eur_tx(broker="b", day=2, tx_type=TxType.SELL, qty=Decimal("2"), gross=Decimal("250")),
            ]
        )


def test_per_broker_pools_never_merged():
    pm = PoolManager()
    pm.replay(
        [
            _eur_tx(broker="A", day=1, tx_type=TxType.BUY, qty=Decimal("10"), gross=Decimal("-1000")),
            _eur_tx(broker="B", day=2, tx_type=TxType.BUY, qty=Decimal("10"), gross=Decimal("-2000")),
        ]
    )
    a = pm.by_broker["A"].by_isin["US88160R1014"]
    b = pm.by_broker["B"].by_isin["US88160R1014"]
    assert a.avg_cost_eur == Decimal("100.0000")
    assert b.avg_cost_eur == Decimal("200.0000")


def test_migration_in_blocks_sell_until_basis_provided():
    pm = PoolManager()
    with pytest.raises(CostBasisMissingError):
        pm.replay(
            [
                _eur_tx(broker="b", day=1, tx_type=TxType.MIGRATION_IN, qty=Decimal("5"), gross=Decimal("0")),
                _eur_tx(broker="b", day=2, tx_type=TxType.SELL, qty=Decimal("2"), gross=Decimal("400")),
            ]
        )


def test_bonus_share_drops_average():
    pm = PoolManager()
    pm.replay(
        [
            _eur_tx(broker="b", day=1, tx_type=TxType.BUY, qty=Decimal("10"), gross=Decimal("-1000")),
            _eur_tx(broker="b", day=2, tx_type=TxType.BONUS_SHARE, qty=Decimal("2"), gross=Decimal("0")),
        ]
    )
    state = pm.by_broker["b"].by_isin["US88160R1014"]
    assert state.quantity == Decimal("12")
    # 1000 EUR / 12 ≈ 83.3333
    assert state.avg_cost_eur.quantize(Decimal("0.0001")) == Decimal("83.3333")


def test_fees_excluded_from_basis_on_buy():
    """§ 20 Abs. 2 + § 27a Abs. 4 Z 2 EStG: Werbungskosten nicht abzugsfähig.
    A BUY with 5 EUR fee must NOT grow the pool's cost basis."""
    pm = PoolManager()
    pm.replay(
        [
            _eur_tx(
                broker="b",
                day=1,
                tx_type=TxType.BUY,
                qty=Decimal("10"),
                gross=Decimal("-1000"),
                fee=Decimal("5"),
            ),
        ]
    )
    state = pm.by_broker["b"].by_isin["US88160R1014"]
    # Basis is 1000, not 1005 (fee excluded).
    assert state.total_cost_eur == Decimal("1000")
    assert state.avg_cost_eur == Decimal("100.0000")


def test_fees_excluded_from_proceeds_on_sell():
    """A SELL with 3 EUR fee must NOT shrink proceeds in the realized event."""
    pm = PoolManager()
    pm.replay(
        [
            _eur_tx(broker="b", day=1, tx_type=TxType.BUY, qty=Decimal("10"), gross=Decimal("-1000")),
            _eur_tx(
                broker="b",
                day=2,
                tx_type=TxType.SELL,
                qty=Decimal("5"),
                gross=Decimal("600"),
                fee=Decimal("3"),
            ),
        ]
    )
    (ev,) = pm.realized_events()
    # Proceeds are 600 (not 597); cost basis 500; PnL 100.
    assert ev.proceeds_eur == Decimal("600.0000")
    assert ev.cost_basis_eur == Decimal("500.0000")
    assert ev.pnl_eur == Decimal("100.0000")


def test_rolling_average_no_drift_after_many_partial_sells():
    """Partial sells must not leave residual total_cost drift once qty → 0.
    Covers C-8: cost subtraction uses unquantized value so many small sells
    leave state internally consistent."""
    pm = PoolManager()
    txns = [
        _eur_tx(broker="b", day=1, tx_type=TxType.BUY, qty=Decimal("3"), gross=Decimal("-100"))
    ]
    # Sell 3 shares one-at-a-time; avg_cost = 100/3 = 33.333…
    for d in range(2, 5):
        txns.append(
            _eur_tx(broker="b", day=d, tx_type=TxType.SELL, qty=Decimal("1"), gross=Decimal("50"))
        )
    pm.replay(txns)
    state = pm.by_broker["b"].by_isin["US88160R1014"]
    assert state.quantity == Decimal("0")
    assert state.total_cost_eur == Decimal("0")


def test_sort_key_handles_mixed_datetime_and_date():
    """Replaying a batch with some rows that carry trade_datetime and some
    that don't must not raise — covers C-6."""
    from datetime import datetime

    pm = PoolManager()
    tx_with_dt = _eur_tx(
        broker="b", day=1, tx_type=TxType.BUY, qty=Decimal("1"), gross=Decimal("-100")
    )
    tx_with_dt.trade_datetime = datetime(2024, 1, 1, 14, 30, 0)
    tx_date_only = _eur_tx(
        broker="b", day=2, tx_type=TxType.SELL, qty=Decimal("1"), gross=Decimal("110")
    )
    pm.replay([tx_with_dt, tx_date_only])  # should not raise
    (ev,) = pm.realized_events()
    assert ev.pnl_eur == Decimal("10.0000")
