"""Trading 212 CSV parser.

Two file variants observed:
  v1 (2024-07 .. 2024-12): no Currency-conversion-fee columns.
  v2 (from 2024-12):       adds 'Currency conversion fee' + 'Currency (..)' columns.

Both share the rest of the schema. ``Action`` field drives the tx type.

USD trades: 'Currency (Price / share)' may be USD while 'Currency (Total)' is
always EUR (Trading 212 settles in EUR). We keep ``currency_native`` = price
currency (USD), so that the FX layer applies the **ECB** rate to the gross,
rather than trusting Trading 212's broker rate. Any conversion fee shown is
emitted as a separate ``FEE`` transaction so it does not contaminate the
trade's gross.
"""

from __future__ import annotations

import csv
from decimal import Decimal
from pathlib import Path

from ..model import (
    AssetClass,
    Flag,
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

BROKER = "trading212"

_ACTION_MAP: dict[str, TxType] = {
    "Market buy": TxType.BUY,
    "Limit buy": TxType.BUY,
    "Stop buy": TxType.BUY,
    "Market sell": TxType.SELL,
    "Limit sell": TxType.SELL,
    "Stop sell": TxType.SELL,
    "Deposit": TxType.DEPOSIT_CASH,
    "Withdrawal": TxType.WITHDRAWAL_CASH,
    # Trading 212 is not an Austrian Kreditinstitut; cash-balance interest
    # falls into the 27,5 %-Topf (§ 27a Abs. 1 Z 2 EStG), not KZ 857.
    "Interest on cash": TxType.INTEREST_OTHER,
    "Dividend (Ordinary)": TxType.DIVIDEND_CASH,
    "Dividend (Dividend)": TxType.DIVIDEND_CASH,
    # Return of capital is NOT taxable as dividend in AT — see TxType doc.
    "Dividend (Return of capital)": TxType.RETURN_OF_CAPITAL,
    "Dividend (Bonus)": TxType.DIVIDEND_CASH,
    "Dividend": TxType.DIVIDEND_CASH,
    "Card cashback": TxType.DEPOSIT_CASH,
    "Currency conversion": TxType.FEE,
    "Result adjustment": TxType.FEE,
    "Stock split": TxType.SPLIT,
    "New card cost": TxType.FEE,
    "Spending cashback": TxType.DEPOSIT_CASH,
    "Card fee": TxType.FEE,
    # Share-lending income is NOT deposit interest — 27,5 %-Topf.
    "Lending interest": TxType.INTEREST_OTHER,
}


def parse(path: Path) -> tuple[list[Transaction], ParseReport]:
    report = ParseReport(broker=BROKER, source_file=str(path), file_sha256=file_sha256(path))
    txns: list[Transaction] = []

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for lineno, row in enumerate(reader, start=2):
            report.rows_total += 1
            try:
                produced = _parse_row(path, lineno, row, report)
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
            for tx in produced:
                txns.append(tx)
                if tx.tx_type is TxType.IGNORED:
                    report.rows_ignored += 1
                else:
                    report.rows_emitted += 1

    return txns, report


def _parse_row(
    path: Path, lineno: int, row: dict[str, str], report: ParseReport
) -> list[Transaction]:
    action = (row.get("Action") or "").strip()
    if action not in _ACTION_MAP:
        # Note: T212 sometimes emits new action variants. Fail loudly.
        raise ParserError(
            f"Unknown Trading 212 Action {action!r}",
            broker=BROKER,
            source_file=str(path),
            source_line=lineno,
        )
    tx_type = _ACTION_MAP[action]

    # Time field is "YYYY-MM-DD HH:MM:SS"
    raw_time = (row.get("Time") or "").strip()
    trade_dt = parse_datetime(raw_time) if raw_time else None
    trade_date = trade_dt.date() if trade_dt else parse_date(row.get("Time", ""))

    isin = (row.get("ISIN") or "").strip() or None
    if isin and not is_valid_isin(isin):
        raise ParserError(
            f"Invalid ISIN: {isin!r}",
            broker=BROKER,
            source_file=str(path),
            source_line=lineno,
        )

    raw_ref = (row.get("ID") or "").strip() or None
    ticker = (row.get("Ticker") or "").strip() or None
    name = (row.get("Name") or "").strip() or None
    notes = (row.get("Notes") or "").strip() or None

    shares = parse_decimal(row.get("No. of shares"))
    price = parse_decimal(row.get("Price / share"))
    price_ccy = normalize_currency(row.get("Currency (Price / share)") or "EUR")
    total = parse_decimal(row.get("Total"))
    total_ccy = normalize_currency(row.get("Currency (Total)") or "EUR")
    conv_fee = parse_decimal(row.get("Currency conversion fee") or "0")
    conv_fee_ccy = normalize_currency(row.get("Currency (Currency conversion fee)") or "EUR")

    asset_class = asset_class_from_isin(isin)
    if tx_type in {
        TxType.DEPOSIT_CASH,
        TxType.WITHDRAWAL_CASH,
        TxType.INTEREST,
        TxType.INTEREST_OTHER,
        TxType.FEE,
    }:
        asset_class = AssetClass.CASH

    txns: list[Transaction] = []

    if tx_type in {TxType.BUY, TxType.SELL}:
        # Use price currency as native, so ECB FX applies on the gross.
        sign = Decimal("-1") if tx_type is TxType.BUY else Decimal("1")
        gross_native = (sign * shares * price).quantize(Decimal("0.000001"))
        tx = Transaction(
            broker=BROKER,
            trade_date=trade_date,
            trade_datetime=trade_dt,
            tx_type=tx_type,
            asset_class=asset_class,
            quantity=shares,
            currency_native=price_ccy,
            gross_native=gross_native,
            source_file=path.name,
            source_line=lineno,
            raw_ref=raw_ref,
            isin=isin,
            symbol=ticker,
            name=name,
            price_native=price,
            fee_native=Decimal("0"),  # T212 charges no per-trade fee; conversion fee handled below
            notes=notes,
        )
        # Audit: store T212's implicit FX in flags
        broker_fx = parse_decimal(row.get("Exchange rate") or "0")
        if price_ccy != "EUR" and broker_fx and broker_fx != 1:
            tx.add_flag(
                "t212.broker_fx_audit",
                Severity.INFO,
                f"Trading 212 implicit fx_rate={broker_fx} {total_ccy}/{price_ccy}; "
                f"ECB rate will be used for tax math.",
            )
        txns.append(tx)

        # Conversion fee (already in EUR) → emit as separate FEE row,
        # linked via raw_ref + suffix to keep dedup deterministic.
        if conv_fee and conv_fee != 0:
            fee_sign = Decimal("-1")  # cost
            fee_amount = (fee_sign * abs(conv_fee)).quantize(Decimal("0.0001"))
            txns.append(
                Transaction(
                    broker=BROKER,
                    trade_date=trade_date,
                    trade_datetime=trade_dt,
                    tx_type=TxType.FEE,
                    asset_class=AssetClass.CASH,
                    quantity=Decimal("0"),
                    currency_native=conv_fee_ccy,
                    gross_native=fee_amount,
                    source_file=path.name,
                    source_line=lineno,
                    raw_ref=f"{raw_ref}::convfee" if raw_ref else None,
                    isin=isin,
                    name=f"FX conversion fee for {action} {ticker or isin or ''}".strip(),
                    notes="Trading 212 currency conversion fee",
                )
            )
        return txns

    if tx_type is TxType.SPLIT:
        # SPLIT rows: shares may carry the new ratio increment. Flag for review.
        tx = Transaction(
            broker=BROKER,
            trade_date=trade_date,
            trade_datetime=trade_dt,
            tx_type=TxType.SPLIT,
            asset_class=asset_class,
            quantity=shares,
            currency_native=total_ccy,
            gross_native=Decimal("0"),
            source_file=path.name,
            source_line=lineno,
            raw_ref=raw_ref,
            isin=isin,
            symbol=ticker,
            name=name,
            notes=notes,
            flags=[
                Flag(
                    "split_review",
                    Severity.WARNING,
                    "Stock split detected; verify pool quantity against post-split position.",
                )
            ],
        )
        return [tx]

    # Cash-side rows (Deposit, Withdrawal, Interest, Fee, Dividend, Return of capital)
    free_share = bool(notes and "Free Shares Promotion" in notes) and tx_type is TxType.DEPOSIT_CASH
    if free_share:
        # Trading 212 free shares promotions show up as a Deposit *and* an
        # immediately following Market buy of the same EUR amount. We keep the
        # Deposit row but flag it for the user.
        flags = [Flag(
            "t212.free_share_promo",
            Severity.WARNING,
            "Trading 212 free-share promotion: review for Sachbezug treatment.",
        )]
    else:
        flags = []

    # Per-broker withholding-tax disclosure: Trading 212 CSV exports do NOT
    # break out the foreign withholding tax separately on dividend rows; the
    # Total column is already net. Flag this loudly so the user knows the
    # gross-up + DBA-credit math cannot be performed reliably from this file
    # alone (they should consult the T212 annual tax report for true gross /
    # withholding figures).
    if tx_type is TxType.DIVIDEND_CASH:
        flags.append(Flag(
            "t212.missing_withholding_detail",
            Severity.WARNING,
            "Trading 212 CSV does not break out withholding tax; gross/credit "
            "figures cannot be reconstructed from this row alone.",
        ))

    # Capital return: not taxable as dividend; basis reduction not auto-applied.
    if tx_type is TxType.RETURN_OF_CAPITAL:
        flags.append(Flag(
            "t212.return_of_capital",
            Severity.WARNING,
            "Return of capital: excluded from dividend income. Manually reduce "
            "the position's cost basis if material.",
        ))

    # Source-country hint for foreign withholding tax (used by credit cap).
    wh_country = country_from_isin(isin) if tx_type is TxType.DIVIDEND_CASH else None

    # T212's Total column is already NET of foreign withholding. Declare the
    # convention explicitly so the engine's gross-up logic can operate
    # correctly when a withholding value is supplied (via annual tax report).
    is_net = True if tx_type is TxType.DIVIDEND_CASH else None

    return [
        Transaction(
            broker=BROKER,
            trade_date=trade_date,
            trade_datetime=trade_dt,
            tx_type=tx_type,
            asset_class=asset_class,
            quantity=Decimal("0"),
            currency_native=total_ccy,
            gross_native=total,
            source_file=path.name,
            source_line=lineno,
            raw_ref=raw_ref,
            isin=isin,
            symbol=ticker,
            name=name,
            notes=notes,
            withholding_country=wh_country,
            dividend_is_net=is_net,
            flags=flags,
        )
    ]
