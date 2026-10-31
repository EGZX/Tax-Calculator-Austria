"""Shared test helpers. All fixtures are synthetic (no personal data)."""

from __future__ import annotations

import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from tax_calc_at.engine.rules import BrokersConfig, load_brokers  # noqa: E402
from tax_calc_at.model import AssetClass, FxSource, Transaction, TxType  # noqa: E402

EXPORTS = ROOT / "exports"


@pytest.fixture(autouse=True)
def _no_personal_rules(monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory):
    """Tests must not see the user's git-ignored rules/local/ overlay."""
    import tax_calc_at.engine.rules as rules_mod
    from tax_calc_at.parsers.base import reset_overrides_cache

    monkeypatch.setattr(rules_mod, "LOCAL_RULES_DIR", tmp_path_factory.mktemp("no-local"))
    reset_overrides_cache()
    yield
    reset_overrides_cache()


@pytest.fixture
def brokers() -> BrokersConfig:
    return load_brokers()


def mk(
    tx_type: TxType,
    *,
    broker: str = "scalable_capital",
    day: date = date(2024, 6, 1),
    qty: str = "0",
    amount_eur: str = "0",
    isin: str | None = "US0378331005",
    asset_class: AssetClass = AssetClass.STOCK,
    currency: str = "EUR",
    gross_native: str | None = None,
    rate: str = "1",
    withheld_eur: str = "0",
    country: str | None = None,
    is_net: bool | None = None,
    line: int = 1,
    at: datetime | None = None,
    **kw: object,
) -> Transaction:
    """A transaction that already went through FX conversion."""
    rate_d = Decimal(rate)
    gross = Decimal(gross_native) if gross_native is not None else Decimal(amount_eur) / rate_d
    tx = Transaction(
        broker=broker,
        trade_date=day,
        trade_datetime=at,
        tx_type=tx_type,
        asset_class=asset_class,
        quantity=Decimal(qty),
        currency_native=currency,
        gross_native=gross,
        source_file="t.csv",
        source_line=line,
        isin=isin,
        withholding_country=country,
        dividend_is_net=is_net,
        **kw,  # type: ignore[arg-type]
    )
    tx.amount_eur = Decimal(amount_eur)
    tx.tax_withheld_eur = Decimal(withheld_eur)
    tx.tax_withheld_native = Decimal(withheld_eur) / rate_d
    tx.fee_eur = tx.fee_native * rate_d
    tx.fx_rate_used = rate_d
    tx.fx_rate_source = FxSource.NATIVE_EUR if currency == "EUR" else FxSource.ECB
    return tx
