"""Trade Republic CSV parser.

Format (comma-separated, all fields quoted):
    datetime, date, account_type, category, type, asset_class, name, symbol,
    shares, price, amount, fee, tax, currency, original_amount,
    original_currency, fx_rate, description, transaction_id, counterparty_name,
    counterparty_iban, payment_reference, mcc_code

We **enforce a hard cutoff** for trade dates >= broker's ``steuereinfach_from``
to avoid double-taxing transactions that TR will report to the AT
Finanzbehörde directly.

Categories ``CASH``/``TRADING`` matter; others are filtered.
Card transactions, SEPA inbound/outbound, card-ordering fees etc. are not
tax-relevant capital-income events but are kept as IGNORED for audit.
"""

from __future__ import annotations

import csv
from datetime import date as DateT
from decimal import Decimal
from pathlib import Path

from ..model import (
    AssetClass,
    CutoffViolationError,
    Flag,
    FxSource,
    ParserError,
    ParseReport,
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
from .base import asset_class_from_isin, file_sha256

BROKER = "trade_republic"

# Map TR `type` (within TRADING/CASH categories) to canonical TxType.
# None means "not tax-relevant — keep as IGNORED".
_TYPE_MAP: dict[str, TxType | None] = {
    "BUY": TxType.BUY,
    "SELL": TxType.SELL,
    "DIVIDEND": TxType.DIVIDEND_CASH,
    "INTEREST_PAYMENT": TxType.INTEREST,
    "STOCKPERK": TxType.BONUS_SHARE,
    "BENEFITS_SAVEBACK": TxType.BONUS_SHARE,
    "CUSTOMER_INBOUND": TxType.DEPOSIT_CASH,
    "CUSTOMER_OUTBOUND_REQUEST": TxType.WITHDRAWAL_CASH,
    "GIFT": TxType.DEPOSIT_CASH,  # promotional cash gift / referral bonus
    "CARD_TRANSACTION": None,
    "CARD_TRANSACTION_INTERNATIONAL": None,
    "CARD_ORDERING_FEE": None,
    "CARD_REFUND": None,
    "CARD_REPAYMENT": None,
    "MIGRATION_OUT": TxType.MIGRATION_OUT,
    "MIGRATION_IN": TxType.MIGRATION_IN,
    "MIGRATION": TxType.MIGRATION_OUT,  # sign of amount disambiguates
}


def parse(
    path: Path,
    *,
    steuereinfach_from: DateT | None = None,
    strict_cutoff: bool = False,
) -> tuple[list[Transaction], ParseReport]:
    """Parse a Trade Republic CSV export.

    Rows with ``trade_date >= steuereinfach_from`` are tax-handled by TR
    directly. By default they are emitted as ``TxType.IGNORED`` with a
    WARNING flag and counted in ``rows_rejected`` so the user sees them
    in the audit but they never affect tax math. Set ``strict_cutoff=True``
    to instead raise :class:`CutoffViolationError` on the first such row.
    """
    report = ParseReport(broker=BROKER, source_file=str(path), file_sha256=file_sha256(path))
    txns: list[Transaction] = []

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for lineno, row in enumerate(reader, start=2):
            report.rows_total += 1
            try:
                tx = _parse_row(
                    path, lineno, row, report, steuereinfach_from, strict_cutoff
                )
            except CutoffViolationError:
                raise
            except (ParserError, ValueError) as e:
                if isinstance(e, ParserError):
                    raise
                raise ParserError(
                    str(e),
                    broker=BROKER,
                    source_file=str(path),
                    source_line=lineno,
                    raw=",".join(f"{k}={v}" for k, v in row.items()),
                ) from e
            if tx is None:
                continue
            txns.append(tx)
            if tx.tx_type is TxType.IGNORED:
                report.rows_ignored += 1
            else:
                report.rows_emitted += 1

    _repair_stockperk_paired_buy(txns)
    return txns, report


def _repair_stockperk_paired_buy(txns: list[Transaction]) -> None:
    """Rewrite the BUY that materializes a Stockperk gift as a zero-cost lot.

    Trade Republic books a promotional free share as two CSV rows:
      1. STOCKPERK (category=CASH, no shares, amount = EUR FMV of the gift).
      2. BUY       (category=TRADING, shares=<fractional>, amount=-EUR FMV).

    Parsed naively this (a) adds real cost basis = FMV to the pool via the
    BUY, (b) surfaces no income for the gift, and (c) leaves the later
    SELL over-reporting nothing but also never reporting the gift as
    income. The pragmatic AT treatment for broker-native Stockperks is a
    zero-cost lot (no receipt income, full proceeds taxable at sale).

    This post-processor finds each tagged STOCKPERK row and, for the
    first BUY on the same ISIN within a 3-day window whose gross value
    matches the Stockperk FMV to the cent, rewrites the BUY to
    BONUS_SHARE with zero gross. Unpaired Stockperks stay untouched
    (defensive — a genuine shares-bearing STOCKPERK row is already a
    zero-cost BONUS_SHARE on its own).
    """
    TOL = Decimal("0.01")
    stockperks = [
        t for t in txns
        if any(f.code == "tr.stockperk" for f in t.flags)
        and t.quantity == 0
        and t.gross_native > 0
        and t.isin
    ]
    if not stockperks:
        return
    for sp in stockperks:
        match: Transaction | None = None
        for tx in txns:
            if tx is sp:
                continue
            if tx.tx_type is not TxType.BUY:
                continue
            if tx.isin != sp.isin:
                continue
            delta_days = abs((tx.trade_date - sp.trade_date).days)
            if delta_days > 3:
                continue
            if (abs(tx.gross_native) - sp.gross_native).copy_abs() > TOL:
                continue
            match = tx
            break
        if match is None:
            continue
        match.tx_type = TxType.BONUS_SHARE
        match.gross_native = Decimal("0")
        match.price_native = None
        match.notes = (
            "Trade Republic Stockperk: converted paired BUY into zero-cost "
            "BONUS_SHARE. FMV of the gift is NOT booked as cost basis."
        )
        match.add_flag(
            "tr.stockperk_paired_buy",
            Severity.INFO,
            f"Paired with Stockperk row {sp.source_file}:{sp.source_line}; "
            "basis set to 0 (Nullkostenzuwendung).",
        )
        sp.add_flag(
            "tr.stockperk_paired",
            Severity.INFO,
            f"Paired BUY row {match.source_file}:{match.source_line} "
            "rewritten to zero-cost BONUS_SHARE.",
        )


def _parse_row(
    path: Path,
    lineno: int,
    row: dict[str, str],
    report: ParseReport,
    cutoff: DateT | None,
    strict_cutoff: bool,
) -> Transaction | None:
    raw_type = (row.get("type") or "").strip().upper()
    category = (row.get("category") or "").strip().upper()
    trade_date = parse_date(row["date"])
    if cutoff is not None and trade_date >= cutoff:
        if strict_cutoff:
            raise CutoffViolationError(
                f"Trade Republic became steuereinfach on {cutoff}; "
                f"row at {path.name}:{lineno} has trade_date {trade_date}. "
                f"Re-export TR data with date <= {cutoff - _ONE_DAY}, or remove that row."
            )
        # Non-strict: emit a stub IGNORED row so the user sees it in the audit
        # tab, count it as rejected, and add a WARNING flag at report level.
        report.rows_rejected += 1
        if not any(f.code == "tr.post_cutoff" for f in report.flags):
            report.flags.append(
                Flag(
                    "tr.post_cutoff",
                    Severity.WARNING,
                    f"Rows on/after {cutoff} are TR-steuereinfach and excluded from tax math.",
                )
            )
        return Transaction(
            broker=BROKER,
            trade_date=trade_date,
            tx_type=TxType.IGNORED,
            asset_class=AssetClass.OTHER,
            quantity=Decimal("0"),
            currency_native=normalize_currency(row.get("currency")),
            gross_native=parse_decimal(row.get("amount")),
            source_file=path.name,
            source_line=lineno,
            raw_ref=(row.get("transaction_id") or "").strip() or None,
            notes=f"Post-cutoff TR row ({raw_type}/{category}) — handled by TR steuereinfach",
            flags=[Flag("tr.post_cutoff", Severity.WARNING, "Excluded: TR steuereinfach.")],
        )

    isin = (row.get("symbol") or "").strip() or None
    # TR's "symbol" is actually the ISIN for securities.
    if isin and not is_valid_isin(isin):
        # Some bonds may not validate cleanly — accept but flag.
        flag = Flag("tr.isin_invalid", Severity.WARNING, f"ISIN failed checksum: {isin!r}")
    else:
        flag = None

    currency = normalize_currency(row.get("currency"))
    trade_dt = None
    if dtv := row.get("datetime"):
        try:
            trade_dt = parse_datetime(dtv)
        except ValueError:
            trade_dt = None

    shares = parse_decimal(row.get("shares"))
    # TR encodes sell quantities as negative; we want absolute quantity in canonical model.
    quantity = abs(shares)
    price = parse_decimal(row.get("price"))
    amount = parse_decimal(row.get("amount"))
    fee = parse_decimal(row.get("fee"))
    tax = parse_decimal(row.get("tax"))
    raw_ref = (row.get("transaction_id") or "").strip() or None
    name = (row.get("name") or "").strip() or None
    asset_hint = (row.get("asset_class") or "").strip()

    # Determine canonical type
    if raw_type not in _TYPE_MAP:
        raise ParserError(
            f"Unknown Trade Republic type {raw_type!r} (category={category})",
            broker=BROKER,
            source_file=str(path),
            source_line=lineno,
        )
    mapped = _TYPE_MAP[raw_type]
    if mapped is None:
        # Card and similar — store as IGNORED for audit
        report.rows_rejected += 1
        return Transaction(
            broker=BROKER,
            trade_date=trade_date,
            trade_datetime=trade_dt,
            tx_type=TxType.IGNORED,
            asset_class=AssetClass.CASH,
            quantity=Decimal("0"),
            currency_native=currency,
            gross_native=amount,
            source_file=path.name,
            source_line=lineno,
            raw_ref=raw_ref,
            isin=isin,
            name=name,
            fee_native=fee,
            tax_withheld_native=tax,
            notes=f"TR {raw_type}",
        )

    tx_type = mapped
    # Disambiguate MIGRATION based on amount sign
    if raw_type == "MIGRATION":
        tx_type = TxType.MIGRATION_OUT if amount < 0 else TxType.MIGRATION_IN

    asset_class = asset_class_from_isin(isin, asset_hint)
    if tx_type in {
        TxType.INTEREST,
        TxType.INTEREST_OTHER,
        TxType.DEPOSIT_CASH,
        TxType.WITHDRAWAL_CASH,
        TxType.FEE,
    }:
        asset_class = AssetClass.CASH

    # Capture broker-supplied FX rate for audit, not for tax math.
    broker_fx = parse_decimal(row.get("fx_rate"))
    notes = None
    if broker_fx > 0 and currency != "EUR":
        notes = f"broker_fx_rate={broker_fx}"

    tx = Transaction(
        broker=BROKER,
        trade_date=trade_date,
        trade_datetime=trade_dt,
        tx_type=tx_type,
        asset_class=asset_class,
        quantity=quantity if tx_type in {TxType.BUY, TxType.SELL, TxType.BONUS_SHARE} else Decimal("0"),
        currency_native=currency,
        gross_native=amount,
        source_file=path.name,
        source_line=lineno,
        raw_ref=raw_ref,
        isin=isin,
        symbol=(row.get("symbol") or "").strip() or None,
        name=name,
        price_native=price if price != 0 else None,
        fee_native=fee,
        tax_withheld_native=tax,
        notes=notes,
    )
    if flag:
        tx.flags.append(flag)
    if raw_type == "STOCKPERK":
        # Tag so the post-processor can pair this BONUS_SHARE with the
        # subsequent BUY that records the promotional shares as if the user
        # had paid for them. The paired BUY is rewritten to a zero-cost
        # BONUS_SHARE (§ 27 Abs. 3 EStG — zero Anschaffungskosten for a
        # Nullkostenzuwendung) so the later SELL's basis is not inflated by
        # the gift value.
        tx.add_flag(
            "tr.stockperk",
            Severity.INFO,
            "Trade Republic Stockperk promotional gift (pool-side repair).",
        )
    if tx_type is TxType.DIVIDEND_CASH:
        # TR reports the NET cash amount in `amount` after foreign withholding;
        # the `tax` field holds the withheld portion separately.
        tx.dividend_is_net = True
        tx.withholding_country = country_from_isin(isin)
    if tx_type is TxType.BONUS_SHARE:
        # Broker-native promo gifts (Stockperk / Saveback) are treated as
        # zero-cost shares by default. Only non-broker sources are flagged.
        if raw_type not in {"STOCKPERK", "BENEFITS_SAVEBACK"}:
            tx.add_flag(
                "bonus_share_non_broker_source",
                Severity.WARNING,
                "Bonus share is not marked as broker-native promotion; review source/tax treatment.",
            )
    return tx


# Local import to avoid pulling in datetime everywhere
from datetime import timedelta as _td  # noqa: E402

_ONE_DAY = _td(days=1)
