"""ECB rate cache: offline history loader, weekend back-off, conversion."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from conftest import mk

from tax_calc_at.fx.convert import convert_transaction
from tax_calc_at.fx.ecb import load_ecb_history_csv, lookup_rate
from tax_calc_at.model import FxRateMissingError, FxSource, TxType
from tax_calc_at.store import connect

HIST = "Date,USD,JPY,DKK,\n2024-01-05,1.0921,158.1,7.4568,\n2024-01-04,1.0953,N/A,7.4571,\n"


def _conn(tmp_path: Path):
    conn = connect(tmp_path / "fx.db")
    p = tmp_path / "eurofxref-hist.csv"
    p.write_text(HIST, encoding="utf-8")
    assert load_ecb_history_csv(conn, str(p)) == 5
    return conn


def test_weekend_uses_last_published_rate(tmp_path):
    conn = _conn(tmp_path)
    assert lookup_rate(conn, "USD", date(2024, 1, 7)) == (Decimal(1) / Decimal("1.0921")).quantize(
        Decimal("0.0000000001"))


def test_convert_uses_ecb_and_never_broker_rates(tmp_path):
    conn = _conn(tmp_path)
    tx = mk(TxType.BUY, day=date(2024, 1, 5), qty="1", currency="USD", gross_native="-1092.1")
    tx.fx_rate_source = FxSource.NONE
    convert_transaction(conn, tx)
    assert tx.fx_rate_source is FxSource.ECB and tx.amount_eur == Decimal("-1000.0000")


def test_missing_rate_raises_typed_error(tmp_path, monkeypatch):
    import tax_calc_at.fx.ecb as ecb

    conn = _conn(tmp_path)

    def offline(_ccy):
        raise ecb.requests.ConnectionError("offline")

    monkeypatch.setattr(ecb, "fetch_ecb_series", offline)
    tx = mk(TxType.BUY, day=date(2024, 3, 5), qty="1", currency="USD", gross_native="-1")
    tx.fx_rate_source = FxSource.NONE
    with pytest.raises(FxRateMissingError, match="unreachable"):
        convert_transaction(conn, tx)
