"""Convert native-currency amounts on transactions to EUR using ECB rates.

Strict policy: every non-EUR amount **must** have an ECB rate for its
FX reference date (see :func:`_fx_date`) or the most recent prior business
day (see ``ecb.lookup_rate``). If the rate is missing, raises
:class:`FxRateMissingError` — never silently falls back to a broker-supplied
rate.

FX reference date:
    * Acquisitions / disposals (BUY, SELL, SPLIT, BONUS_SHARE, MIGRATION_*):
      :attr:`Transaction.trade_date` (Anschaffungs-/Veräußerungszeitpunkt,
      § 27a Abs. 3 EStG).
    * Income events (DIVIDEND_CASH, INTEREST, RETURN_OF_CAPITAL, FEE):
      :attr:`Transaction.settle_date` if provided (Zuflusstag), else
      :attr:`Transaction.trade_date`.
"""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Iterable

from ..model import FxRateMissingError, FxSource, Severity, Transaction, TxType
from .ecb import lookup_rate

EUR_QUANT = Decimal("0.0001")  # 4 dp internally; final report rounds further

_INCOME_TYPES = {
    TxType.DIVIDEND_CASH,
    TxType.INTEREST,
    TxType.INTEREST_OTHER,
    TxType.RETURN_OF_CAPITAL,
    TxType.FEE,
}


def _fx_date(tx: Transaction) -> date:
    """Reference date for ECB lookup: Zuflusstag for income, trade for others."""
    if tx.tx_type in _INCOME_TYPES and tx.settle_date is not None:
        return tx.settle_date
    return tx.trade_date


def _to_eur(amount: Decimal | None, rate: Decimal) -> Decimal | None:
    if amount is None:
        return None
    return (amount * rate).quantize(EUR_QUANT, rounding=ROUND_HALF_EVEN)


def convert_transaction(conn: sqlite3.Connection, tx: Transaction) -> Transaction:
    """Populate ``amount_eur``, ``fee_eur``, ``tax_withheld_eur`` and FX metadata.

    Idempotent: if already converted, returns ``tx`` unchanged.
    """
    if tx.fx_rate_source is not FxSource.NONE:
        return tx
    ccy = tx.currency_native.upper()
    if ccy == "EUR":
        tx.amount_eur = tx.gross_native
        tx.fee_eur = tx.fee_native
        tx.tax_withheld_eur = tx.tax_withheld_native
        tx.fx_rate_used = Decimal("1")
        tx.fx_rate_source = FxSource.NATIVE_EUR
        return tx
    on = _fx_date(tx)
    rate = lookup_rate(conn, ccy, on)
    if rate is None:
        raise FxRateMissingError(
            f"No ECB rate for {ccy} on or before {on} "
            f"(transaction {tx.broker} {tx.source_file}:{tx.source_line})"
        )
    tx.amount_eur = _to_eur(tx.gross_native, rate)
    tx.fee_eur = _to_eur(tx.fee_native, rate)
    tx.tax_withheld_eur = _to_eur(tx.tax_withheld_native, rate)
    tx.fx_rate_used = rate
    tx.fx_rate_source = FxSource.ECB
    # Audit transparency: document which date drove the FX lookup so a
    # Steuerberater can verify the rate choice against § 27a / § 19 EStG.
    if on != tx.trade_date:
        tx.add_flag(
            "fx.settle_date_used",
            Severity.INFO,
            f"FX rate from {on} (settle_date) used instead of trade_date "
            f"{tx.trade_date}; Zuflussprinzip per § 19 EStG for income events.",
        )
    return tx


def convert_all(conn: sqlite3.Connection, txns: Iterable[Transaction]) -> None:
    for tx in txns:
        convert_transaction(conn, tx)
