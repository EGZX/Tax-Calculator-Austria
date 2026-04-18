"""Interactive Brokers Flex Query (CSV) parser.

The user-supplied Flex Query concatenates several report sections into one
CSV. Each section starts with its own header row whose first column header
is ``ClientAccountID``. This parser splits the file into sections by
detecting those header rows, then dispatches on the columns present:

* **Trades** section (has ``Buy/Sell`` + ``TradeDate`` + ``Quantity`` +
  ``Proceeds``): produces ``BUY`` / ``SELL`` for ``STK`` asset class and
  ``IGNORED`` for ``CASH`` rows (IDEALFX currency conversions — informational
  only; the ECB layer handles FX for tax math).
* **Cash Transactions** section (has ``Amount`` + ``Type``): produces
  ``DIVIDEND_CASH`` (``Dividends``), ``INTEREST`` (``Broker Interest
  Received``), ``DEPOSIT_CASH`` / ``WITHDRAWAL_CASH`` (``Deposits/
  Withdrawals`` by sign of ``Amount``), ``FEE`` (``Broker Interest Paid``
  / ``Commissions`` / ``Other Fees``), and folds ``Withholding Tax`` rows
  into the matching dividend via the shared ``ActionID`` so the engine can
  gross-up and credit correctly.

Unknown section headers or unknown Type values cause a loud :class:`ParserError`.
"""

from __future__ import annotations

import csv
from datetime import datetime
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
    parse_decimal,
)
from .base import asset_class_from_isin, file_sha256

BROKER = "ibkr"


def _ibkr_datetime(raw: str) -> datetime | None:
    """Parse IBKR's 'YYYYMMDD;HHMMSS' or 'YYYYMMDD' stamp."""
    if not raw:
        return None
    s = raw.strip().strip('"')
    if ";" in s:
        d, t = s.split(";", 1)
        d = d.strip()
        t = t.strip()
        if len(d) == 8 and d.isdigit() and len(t) == 6 and t.isdigit():
            return datetime.strptime(d + t, "%Y%m%d%H%M%S")
    if len(s) == 8 and s.isdigit():
        return datetime.strptime(s, "%Y%m%d")
    return None


def _ibkr_date(raw: str):
    """Parse IBKR's compact 'YYYYMMDD' (optionally followed by ';HHMMSS') date."""
    s = (raw or "").strip().strip('"')
    if ";" in s:
        s = s.split(";", 1)[0].strip()
    if len(s) == 8 and s.isdigit():
        return datetime.strptime(s, "%Y%m%d").date()
    return parse_date(s)


def parse(path: Path) -> tuple[list[Transaction], ParseReport]:
    report = ParseReport(broker=BROKER, source_file=str(path), file_sha256=file_sha256(path))

    # Split the file into sections, each starting with a header row whose
    # first column is literally "ClientAccountID". Line numbers track the
    # absolute position for accurate error reporting.
    sections: list[tuple[list[str], list[tuple[int, list[str]]]]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        current_header: list[str] | None = None
        current_rows: list[tuple[int, list[str]]] = []
        for lineno, row in enumerate(reader, start=1):
            if not row:
                continue
            if row[0] == "ClientAccountID":
                if current_header is not None:
                    sections.append((current_header, current_rows))
                current_header = row
                current_rows = []
            else:
                if current_header is None:
                    raise ParserError(
                        "IBKR Flex CSV did not start with a section header",
                        broker=BROKER,
                        source_file=str(path),
                        source_line=lineno,
                    )
                current_rows.append((lineno, row))
        if current_header is not None:
            sections.append((current_header, current_rows))

    if not sections:
        raise ParserError(
            "IBKR Flex CSV contains no sections",
            broker=BROKER,
            source_file=str(path),
            source_line=0,
        )

    txns: list[Transaction] = []
    for header, rows in sections:
        report.rows_total += len(rows)
        cols = {name: idx for idx, name in enumerate(header)}
        if "Buy/Sell" in cols and "TradeDate" in cols:
            section_txns = _parse_trades(path, cols, rows)
        elif "Amount" in cols and "Type" in cols:
            section_txns = _parse_cash(path, cols, rows)
        else:
            raise ParserError(
                f"Unrecognized IBKR Flex section (first 5 cols={header[:5]}). "
                "Expected a Trades or Cash-Transactions query.",
                broker=BROKER,
                source_file=str(path),
                source_line=rows[0][0] if rows else 0,
            )
        for tx in section_txns:
            txns.append(tx)
            if tx.tx_type is TxType.IGNORED:
                report.rows_ignored += 1
            else:
                report.rows_emitted += 1

    return txns, report


def _get(row: list[str], cols: dict[str, int], key: str, default: str = "") -> str:
    idx = cols.get(key)
    if idx is None or idx >= len(row):
        return default
    return (row[idx] or "").strip().strip('"')


# ---------------------------------------------------------------- Trades
def _parse_trades(
    path: Path,
    cols: dict[str, int],
    rows: list[tuple[int, list[str]]],
) -> list[Transaction]:
    out: list[Transaction] = []
    for lineno, row in rows:
        try:
            out.append(_parse_trade_row(path, lineno, cols, row))
        except ParserError:
            raise
        except (ValueError, IndexError) as e:
            raise ParserError(
                f"IBKR trade row parse failure: {e}",
                broker=BROKER,
                source_file=str(path),
                source_line=lineno,
                raw=",".join(row),
            ) from e
    return out


def _parse_trade_row(
    path: Path,
    lineno: int,
    cols: dict[str, int],
    row: list[str],
) -> Transaction:
    asset_class_raw = _get(row, cols, "AssetClass").upper()
    buy_sell = _get(row, cols, "Buy/Sell").upper()
    isin = _get(row, cols, "ISIN") or None
    symbol = _get(row, cols, "Symbol") or None
    name = _get(row, cols, "Description") or None
    currency = normalize_currency(_get(row, cols, "CurrencyPrimary") or "USD")
    trade_date = _ibkr_date(_get(row, cols, "TradeDate"))
    trade_dt = _ibkr_datetime(_get(row, cols, "DateTime")) or _ibkr_datetime(
        _get(row, cols, "TradeDate")
    )
    raw_ref = _get(row, cols, "TransactionID") or _get(row, cols, "TradeID") or None
    issuer_country = _get(row, cols, "IssuerCountryCode") or None

    # IDEALFX currency-conversion rows — keep as IGNORED audit trail only.
    if asset_class_raw == "CASH":
        return Transaction(
            broker=BROKER,
            trade_date=trade_date,
            trade_datetime=trade_dt,
            tx_type=TxType.IGNORED,
            asset_class=AssetClass.CASH,
            quantity=Decimal("0"),
            currency_native=currency,
            gross_native=parse_decimal(_get(row, cols, "Proceeds")),
            source_file=path.name,
            source_line=lineno,
            raw_ref=raw_ref,
            name=f"IBKR FX conversion {symbol or ''}".strip(),
            notes=f"IBKR IDEALFX {symbol or ''}",
            flags=[
                Flag(
                    "ibkr.fx_conversion",
                    Severity.INFO,
                    "IBKR IDEALFX currency conversion; tax math uses ECB.",
                )
            ],
        )

    if buy_sell not in {"BUY", "SELL"}:
        raise ParserError(
            f"Unsupported IBKR trade Buy/Sell={buy_sell!r} (asset={asset_class_raw!r})",
            broker=BROKER,
            source_file=str(path),
            source_line=lineno,
        )

    flags: list[Flag] = []
    if isin and not is_valid_isin(isin):
        flags.append(
            Flag("ibkr.isin_invalid", Severity.WARNING, f"ISIN failed checksum: {isin!r}")
        )

    asset_class = asset_class_from_isin(isin, asset_class_raw)
    qty_raw = parse_decimal(_get(row, cols, "Quantity"))
    price = parse_decimal(_get(row, cols, "TradePrice"))
    # Proceeds is already the signed cash flow we want on gross_native:
    # negative for BUY (cash out), positive for SELL (cash in).
    proceeds = parse_decimal(_get(row, cols, "Proceeds"))
    commission = parse_decimal(_get(row, cols, "IBCommission"))  # IBKR reports as negative
    taxes = parse_decimal(_get(row, cols, "Taxes"))  # typically 0 on equity trades

    tx_type = TxType.BUY if buy_sell == "BUY" else TxType.SELL
    tx = Transaction(
        broker=BROKER,
        trade_date=trade_date,
        trade_datetime=trade_dt,
        tx_type=tx_type,
        asset_class=asset_class,
        quantity=abs(qty_raw),
        currency_native=currency,
        gross_native=proceeds,
        source_file=path.name,
        source_line=lineno,
        raw_ref=raw_ref,
        isin=isin,
        symbol=symbol,
        name=name,
        price_native=price if price != 0 else None,
        fee_native=abs(commission),
        tax_withheld_native=abs(taxes),
        withholding_country=issuer_country,
        flags=flags,
    )
    return tx


# ---------------------------------------------------------- Cash transactions
_CASH_TYPE_MAP: dict[str, TxType | None] = {
    "Dividends": TxType.DIVIDEND_CASH,
    "Payment In Lieu Of Dividends": TxType.DIVIDEND_CASH,
    # IBKR is a US broker, not a § 93 EStG Kreditinstitut — cash-balance
    # interest does not qualify for the 25 % Geldeinlage-Topf. Route to
    # 27,5 %-Topf per § 27a Abs. 1 Z 2 EStG.
    "Broker Interest Received": TxType.INTEREST_OTHER,
    "Bond Interest Received": TxType.INTEREST_OTHER,  # bond coupons → 27,5 %
    "Broker Interest Paid": TxType.FEE,  # margin interest cost, not capital income
    "Deposits/Withdrawals": None,  # sign disambiguates DEPOSIT vs WITHDRAWAL
    "Withholding Tax": None,  # folded into the matching Dividends row
    "Commissions": TxType.FEE,
    "Other Fees": TxType.FEE,
}


def _parse_cash(
    path: Path,
    cols: dict[str, int],
    rows: list[tuple[int, list[str]]],
) -> list[Transaction]:
    # First pass: group Withholding Tax by ActionID so we can attach it to
    # the matching dividend emitted in the second pass.
    wh_by_action: dict[str, list[tuple[int, list[str]]]] = {}
    for lineno, row in rows:
        if _get(row, cols, "Type") == "Withholding Tax":
            action = _get(row, cols, "ActionID")
            if action:
                wh_by_action.setdefault(action, []).append((lineno, row))

    consumed_wh: set[int] = set()
    out: list[Transaction] = []
    for lineno, row in rows:
        row_type = _get(row, cols, "Type")
        if not row_type:
            continue
        if row_type == "Withholding Tax":
            continue
        if row_type not in _CASH_TYPE_MAP:
            raise ParserError(
                f"Unknown IBKR cash-transaction Type {row_type!r}",
                broker=BROKER,
                source_file=str(path),
                source_line=lineno,
            )

        try:
            tx = _emit_cash_row(path, lineno, cols, row, row_type, wh_by_action, consumed_wh)
        except ParserError:
            raise
        except (ValueError, IndexError) as e:
            raise ParserError(
                f"IBKR cash row parse failure: {e}",
                broker=BROKER,
                source_file=str(path),
                source_line=lineno,
                raw=",".join(row),
            ) from e
        if tx is not None:
            out.append(tx)

    # Any Withholding Tax rows that never matched a dividend ActionID
    # (e.g. the 20 % interest-credit withholding in the sample): emit as
    # standalone IGNORED rows with a WARNING flag so the audit tab surfaces
    # them and the user can review them.
    for action, wh_rows in wh_by_action.items():
        for lineno, row in wh_rows:
            if lineno in consumed_wh:
                continue
            currency = normalize_currency(_get(row, cols, "CurrencyPrimary") or "EUR")
            amount = parse_decimal(_get(row, cols, "Amount"))
            trade_date = _ibkr_date(
                _get(row, cols, "Date/Time") or _get(row, cols, "SettleDate")
            )
            out.append(
                Transaction(
                    broker=BROKER,
                    trade_date=trade_date,
                    tx_type=TxType.IGNORED,
                    asset_class=AssetClass.CASH,
                    quantity=Decimal("0"),
                    currency_native=currency,
                    gross_native=amount,
                    source_file=path.name,
                    source_line=lineno,
                    raw_ref=_get(row, cols, "TransactionID") or None,
                    name=_get(row, cols, "Description") or None,
                    notes=f"Unmatched IBKR Withholding Tax (ActionID={action or '-'})",
                    flags=[
                        Flag(
                            "ibkr.unmatched_withholding",
                            Severity.WARNING,
                            "IBKR Withholding Tax row without matching Dividends; "
                            "likely interest-credit withholding — review.",
                        )
                    ],
                )
            )

    return out


def _emit_cash_row(
    path: Path,
    lineno: int,
    cols: dict[str, int],
    row: list[str],
    row_type: str,
    wh_by_action: dict[str, list[tuple[int, list[str]]]],
    consumed_wh: set[int],
) -> Transaction | None:
    currency = normalize_currency(_get(row, cols, "CurrencyPrimary") or "EUR")
    date_raw = _get(row, cols, "Date/Time") or _get(row, cols, "SettleDate")
    trade_date = _ibkr_date(date_raw)
    trade_dt = _ibkr_datetime(date_raw)
    amount = parse_decimal(_get(row, cols, "Amount"))
    isin = _get(row, cols, "ISIN") or None
    symbol = _get(row, cols, "Symbol") or None
    description = _get(row, cols, "Description") or None
    issuer_country = _get(row, cols, "IssuerCountryCode") or None
    raw_ref = _get(row, cols, "TransactionID") or None

    mapped = _CASH_TYPE_MAP[row_type]

    if row_type == "Deposits/Withdrawals":
        tx_type = TxType.DEPOSIT_CASH if amount >= 0 else TxType.WITHDRAWAL_CASH
        return Transaction(
            broker=BROKER,
            trade_date=trade_date,
            trade_datetime=trade_dt,
            tx_type=tx_type,
            asset_class=AssetClass.CASH,
            quantity=Decimal("0"),
            currency_native=currency,
            gross_native=amount,
            source_file=path.name,
            source_line=lineno,
            raw_ref=raw_ref,
            name=description,
            notes=row_type,
        )

    assert mapped is not None

    if mapped is TxType.DIVIDEND_CASH:
        action = _get(row, cols, "ActionID")
        tax_total = Decimal("0")
        wh_country: str | None = issuer_country
        for wh_line, wh_row in wh_by_action.get(action, []):
            consumed_wh.add(wh_line)
            tax_total += abs(parse_decimal(_get(wh_row, cols, "Amount")))
            wh_country = wh_country or (_get(wh_row, cols, "IssuerCountryCode") or None)
        wh_country = wh_country or country_from_isin(isin)
        return Transaction(
            broker=BROKER,
            trade_date=trade_date,
            trade_datetime=trade_dt,
            tx_type=TxType.DIVIDEND_CASH,
            asset_class=AssetClass.STOCK if isin else AssetClass.OTHER,
            quantity=Decimal("0"),
            currency_native=currency,
            gross_native=amount,  # IBKR reports NET of withholding on Dividends rows
            source_file=path.name,
            source_line=lineno,
            raw_ref=raw_ref,
            isin=isin,
            symbol=symbol,
            name=description,
            tax_withheld_native=tax_total,
            withholding_country=wh_country,
            dividend_is_net=True,  # IBKR Dividends rows are NET; engine grosses up
            notes=f"IBKR dividend ActionID={action or '-'}",
        )

    if mapped in {TxType.INTEREST, TxType.INTEREST_OTHER}:
        return Transaction(
            broker=BROKER,
            trade_date=trade_date,
            trade_datetime=trade_dt,
            tx_type=mapped,
            asset_class=AssetClass.CASH,
            quantity=Decimal("0"),
            currency_native=currency,
            gross_native=amount,
            source_file=path.name,
            source_line=lineno,
            raw_ref=raw_ref,
            name=description,
            notes=row_type,
        )

    # TxType.FEE (Broker Interest Paid, Commissions, Other Fees)
    return Transaction(
        broker=BROKER,
        trade_date=trade_date,
        trade_datetime=trade_dt,
        tx_type=TxType.FEE,
        asset_class=AssetClass.CASH,
        quantity=Decimal("0"),
        currency_native=currency,
        gross_native=amount,
        source_file=path.name,
        source_line=lineno,
        raw_ref=raw_ref,
        name=description,
        notes=row_type,
    )
