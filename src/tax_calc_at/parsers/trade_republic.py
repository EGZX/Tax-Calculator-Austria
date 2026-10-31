"""Trade Republic transaction CSV parser (in-app "Transaktionsexport").

Columns (all quoted): datetime, date, account_type, category, type,
asset_class, name, symbol (= ISIN), shares, price, amount, fee, tax, currency,
original_amount, original_currency, fx_rate, description, transaction_id, …

Semantics (net cash = amount + fee + tax; fee/tax are signed):

* BUY/SELL: ``amount`` = −shares × price (excl. fee; bonds incl. accrued
  interest, which belongs to cost/proceeds); SELL ``shares`` negative.
* DIVIDEND/DISTRIBUTION: ``amount`` is the GROSS dividend in EUR, ``tax``
  (negative) the tax withheld. For foreign-currency dividends
  ``original_amount``/``original_currency`` hold the gross in the payment
  currency, which is valued at the ECB rate (EStR Rz 6201c); TR's conversion
  into EUR is booked as an ``FX_CONVERSION`` at TR's rate.
* STOCKPERK (card reward in shares of the merchant) and BENEFITS_SAVEBACK
  (1 % of card spending invested) are card cashback — capital income at
  receipt (EStR Rz 6134; 25 % as TR is a credit institution). The cash row
  carries the value; the shares are bought by a separate BUY at full cost.
* MIGRATION: direction is the sign of ``shares`` (``amount`` is empty).

The account's steuereinfach cutoff is applied by :func:`parsers.parse_file`.
"""

from __future__ import annotations

import csv
from decimal import Decimal
from pathlib import Path

from ..model import (
    AssetClass,
    ParseReport,
    ParserError,
    Severity,
    Transaction,
    TxType,
)
from ..normalize import (
    country_from_isin,
    is_valid_isin,
    normalize_currency,
    parse_date,
    parse_datetime,
    parse_decimal,
)
from .base import asset_class_from_isin, count_rows, file_sha256, make_tx

BROKER = "trade_republic"
ZERO = Decimal("0")

_CASH_TYPES: dict[str, TxType] = {
    # Trade Republic Bank GmbH is a credit institution → 25 % via broker config.
    "INTEREST_PAYMENT": TxType.INTEREST,
    "BENEFITS_SAVEBACK": TxType.CASHBACK,
    "CUSTOMER_INBOUND": TxType.DEPOSIT_CASH,
    "CUSTOMER_INPAYMENT": TxType.DEPOSIT_CASH,
    "TRANSFER_INBOUND": TxType.DEPOSIT_CASH,
    "TRANSFER_INSTANT_INBOUND": TxType.DEPOSIT_CASH,
    "CUSTOMER_OUTBOUND_REQUEST": TxType.WITHDRAWAL_CASH,
    "TRANSFER_OUTBOUND": TxType.WITHDRAWAL_CASH,
    "TRANSFER_INSTANT_OUTBOUND": TxType.WITHDRAWAL_CASH,
}
_IGNORED_TYPES = {
    "CARD_TRANSACTION",
    "CARD_TRANSACTION_INTERNATIONAL",
    "CARD_ORDERING_FEE",
    "CARD_REFUND",
    "CARD_REPAYMENT",
    "CARD_TRANSACTION_REFUND",
}


def parse(path: Path) -> tuple[list[Transaction], ParseReport]:
    report = ParseReport(broker=BROKER, source_file=str(path), file_sha256=file_sha256(path))
    txns: list[Transaction] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for lineno, row in enumerate(csv.DictReader(f), start=2):
            report.rows_total += 1
            try:
                txns.extend(_parse_row(path, lineno, row, report))
            except ParserError:
                raise
            except (ValueError, ArithmeticError) as e:
                raise ParserError(
                    str(e), broker=BROKER, source_file=str(path), source_line=lineno,
                    raw=",".join(f"{k}={v}" for k, v in row.items()),
                ) from e
    count_rows(txns, report)
    return txns, report


def _parse_row(
    path: Path, lineno: int, row: dict[str, str], report: ParseReport
) -> list[Transaction]:
    def get(key: str) -> str:
        return (row.get(key) or "").strip()

    def err(msg: str) -> ParserError:
        return ParserError(msg, broker=BROKER, source_file=str(path), source_line=lineno)

    raw_type = get("type").upper()
    category = get("category").upper()
    isin = get("symbol") or None
    if isin and not is_valid_isin(isin):
        raise err(f"Invalid ISIN: {isin!r}")
    shares = parse_decimal(get("shares"))
    amount = parse_decimal(get("amount"))
    fee = parse_decimal(get("fee"))  # negative = cost
    tax = parse_decimal(get("tax"))  # negative = withheld / paid
    base = {
        "broker": BROKER,
        "trade_date": parse_date(get("date")),
        "trade_datetime": parse_datetime(get("datetime")) if get("datetime") else None,
        "currency_native": normalize_currency(get("currency")),
        "source_file": path.name,
        "source_line": lineno,
        "raw_ref": get("transaction_id") or None,
        "isin": isin,
        "symbol": isin,
        "name": get("name") or None,
    }
    asset_class = asset_class_from_isin(isin, get("asset_class") or None) if isin else (
        AssetClass.CASH
    )

    if raw_type in {"BUY", "SELL"}:
        if not isin or shares == 0:
            raise err(f"{raw_type} without ISIN or shares")
        tx = make_tx(
            base, tx_type=TxType.BUY if raw_type == "BUY" else TxType.SELL,
            asset_class=asset_class, quantity=abs(shares), gross_native=amount,
            price_native=parse_decimal(get("price")) or None, fee_native=-fee - tax,
        )
        if tax:
            tx.add_flag("tr.transaction_tax", Severity.INFO,
                        f"tax {tax} on a trade treated as a non-deductible cost.")
        return [tx]

    if raw_type in {"DIVIDEND", "DISTRIBUTION", "DIVIDEND_EQUIVALENT_PAYMENT"}:
        if not isin:
            raise err(f"{raw_type} without ISIN")
        withheld_eur = -tax
        orig_ccy = get("original_currency")
        out: list[Transaction] = []
        if orig_ccy and normalize_currency(orig_ccy) != base["currency_native"]:
            fx = parse_decimal(get("fx_rate"))  # EUR per unit of orig_ccy
            if fx <= 0:
                raise err("Foreign-currency dividend without fx_rate")
            ccy = normalize_currency(orig_ccy)
            gross = parse_decimal(get("original_amount"))
            withheld = withheld_eur / fx
            net_orig = gross - withheld
            dividend = make_tx(base, currency_native=ccy, tx_type=TxType.DIVIDEND_CASH,
                asset_class=asset_class, quantity=ZERO, gross_native=gross,
                tax_withheld_native=withheld, dividend_is_net=False,
                withholding_country=country_from_isin(isin),
            )
            out.append(dividend)
            ref = f"{base['raw_ref']}::fx" if base["raw_ref"] else None
            out.append(make_tx(
                base, currency_native=ccy, raw_ref=ref, tx_type=TxType.FX_CONVERSION,
                asset_class=AssetClass.CASH, quantity=ZERO,
                gross_native=-net_orig, counter_currency=base["currency_native"],
                counter_amount=amount + tax, notes=f"TR conversion at {fx}",
            ))
        else:
            dividend = make_tx(
                base, tx_type=TxType.DIVIDEND_CASH, asset_class=asset_class, quantity=ZERO,
                gross_native=amount, tax_withheld_native=withheld_eur, dividend_is_net=False,
                withholding_country=country_from_isin(isin),
            )
            out.append(dividend)
        if raw_type == "DIVIDEND_EQUIVALENT_PAYMENT":
            from ..model import FLAG_ROC_LABEL

            dividend.add_flag(FLAG_ROC_LABEL, Severity.WARNING,
                              "'Capital distribution': taxed as a dividend unless an "
                              "Einlagenrückzahlung is proven (rules/return_of_capital.yaml).")
        return out

    if raw_type == "STOCKPERK":
        if shares:
            # Shares delivered directly: income at market value = basis.
            return [make_tx(base, tx_type=TxType.BONUS_SHARE, asset_class=asset_class,
                                quantity=abs(shares), gross_native=amount)]
        return [make_tx(base, isin=None, tx_type=TxType.CASHBACK,
                            asset_class=AssetClass.CASH, quantity=ZERO, gross_native=amount,
                            notes=f"Stockperk for {isin} (shares bought by a separate BUY)")]

    if raw_type == "BENEFITS_SAVEBACK":
        if shares:
            raise err("Saveback row with shares: format not verified")
        return [make_tx(base, isin=None, tx_type=TxType.CASHBACK,
                            asset_class=AssetClass.CASH, quantity=ZERO, gross_native=amount,
                            notes=f"Saveback for {isin} (shares bought by a separate BUY)")]

    if raw_type == "GIFT":
        # Private gift between users (send/receive), not income.
        return [make_tx(base, tx_type=TxType.DEPOSIT_CASH if amount >= 0
                            else TxType.WITHDRAWAL_CASH, asset_class=AssetClass.CASH,
                            quantity=ZERO, gross_native=amount)]

    if raw_type == "MIGRATION":
        if not isin or shares == 0:
            raise err("MIGRATION without ISIN or shares")
        return [make_tx(
            base, tx_type=TxType.MIGRATION_IN if shares > 0 else TxType.MIGRATION_OUT,
            asset_class=asset_class, quantity=abs(shares), gross_native=ZERO,
            price_native=parse_decimal(get("price")) or None,
        )]

    if raw_type == "SPLIT" and category == "CORPORATE_ACTION":
        if not isin or shares == 0:
            raise err("SPLIT without ISIN or shares")
        return [make_tx(base, tx_type=TxType.SPLIT, asset_class=asset_class,
                            quantity=shares, gross_native=ZERO)]

    if raw_type in _CASH_TYPES:
        tx = make_tx(base, isin=None, tx_type=_CASH_TYPES[raw_type],
                         asset_class=AssetClass.CASH, quantity=ZERO, gross_native=amount)
        if tax and raw_type == "INTEREST_PAYMENT":
            tx.tax_withheld_native = -tax
            tx.dividend_is_net = False
        return [tx]

    if raw_type in _IGNORED_TYPES:
        report.rows_rejected += 1
        return [make_tx(base, isin=None, tx_type=TxType.IGNORED,
                            asset_class=AssetClass.CASH, quantity=ZERO, gross_native=amount,
                            notes=f"TR {raw_type}")]

    raise err(f"Unknown or unsupported Trade Republic type {raw_type!r} (category={category})")
