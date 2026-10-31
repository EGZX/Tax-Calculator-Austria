"""Interactive Brokers Flex Query (CSV) parser.

A Flex CSV concatenates report sections; each starts with a header row whose
first column is ``ClientAccountID`` (default "Select All" field order). The
section type is recognised from its columns:

* **Trades** (``Buy/Sell`` + ``TradeDate``): STK/FUND/BOND trades, plus
  ``AssetClass=CASH`` currency conversions (IDEALFX / AutoFX) emitted as
  ``FX_CONVERSION`` legs for the foreign-currency cash ledger.
* **Cash Transactions** (``Amount`` + ``Type`` + ``Date/Time``): dividends
  (GROSS; withholding comes as separate ``Withholding Tax`` rows sharing the
  ``ActionID``), interest, fees, deposits/withdrawals.
* **Corporate Actions** (``Type`` + ``Proceeds`` + ``Value``): splits, issue
  changes, mergers, spin-offs. Stock dividends (``SD``) need a declaration:
  bonus shares out of profits are income, a capital increase from company
  funds is tax-neutral, and the export does not say which.
* **Transfers** (``Direction`` + ``Quantity``): position transfers in/out.

Anything not understood raises :class:`ParserError` — in particular listed
options/futures/CFDs: at a foreign broker they are non-securitised
derivatives taxed at the progressive tariff (§ 27a Abs 2 Z 7 EStG,
E1kv KZ 857), which this tool does not compute.
"""

from __future__ import annotations

import csv
import re
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from ..model import (
    FLAG_PAYMENT_IN_LIEU,
    FLAG_ROC_LABEL,
    FLAG_WHT_COUNTRY_UNCERTAIN,
    AssetClass,
    Flag,
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
    parse_decimal,
)
from .base import (
    asset_class_from_isin,
    count_rows,
    file_sha256,
    make_tx,
    unclassified_share_increase,
)

BROKER = "ibkr"
ZERO = Decimal("0")

Row = list[str]
Cols = dict[str, int]

_SUPPORTED_TRADE_CLASSES = {"STK", "FUND", "BOND", "CASH"}
_TRADE_TYPES = {"ExchTrade", "FracShare", ""}
# WHT row descriptions end with "- US TAX" etc.: IBKR names the taxing state.
_TAX_COUNTRY_RE = re.compile(r"-\s*([A-Z]{2})\s+TAX\s*$")
_INTEREST_PERIOD_RE = re.compile(r"\bFOR ([A-Z]{3}-\d{4})\b")


def _ibkr_datetime(raw: str) -> datetime | None:
    """Parse IBKR's 'YYYYMMDD;HHMMSS' / 'YYYYMMDD' stamp (account-local time)."""
    s = (raw or "").strip().strip('"')
    if not s:
        return None
    d, _, t = s.partition(";")
    d, t = d.strip(), t.strip()
    if len(d) == 8 and d.isdigit():
        if len(t) == 6 and t.isdigit():
            return datetime.strptime(d + t, "%Y%m%d%H%M%S")
        if not t:
            return datetime.strptime(d, "%Y%m%d")
    return None


def _ibkr_date(raw: str) -> date:
    s = (raw or "").strip().strip('"').split(";", 1)[0].strip()
    if len(s) == 8 and s.isdigit():
        return datetime.strptime(s, "%Y%m%d").date()
    return parse_date(s)


def _get(row: Row, cols: Cols, key: str, default: str = "") -> str:
    idx = cols.get(key)
    if idx is None or idx >= len(row):
        return default
    return (row[idx] or "").strip().strip('"')


def _err(path: Path, lineno: int, msg: str, row: Row | None = None) -> ParserError:
    return ParserError(
        msg, broker=BROKER, source_file=str(path), source_line=lineno,
        raw=",".join(row) if row else None,
    )


def parse(path: Path) -> tuple[list[Transaction], ParseReport]:
    report = ParseReport(broker=BROKER, source_file=str(path), file_sha256=file_sha256(path))
    sections: list[tuple[Row, list[tuple[int, Row]]]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        header: Row | None = None
        rows: list[tuple[int, Row]] = []
        for lineno, row in enumerate(csv.reader(f), start=1):
            if not row:
                continue
            if row[0] in {"BOF", "BOA", "EOA", "EOF"} or (len(row) > 1 and row[1] in {
                "BOS", "EOS", "HEADER", "DATA"
            }):
                raise _err(path, lineno, "Flex CSV with header/trailer records or section "
                           "codes is not supported; export with those options disabled.", row)
            if row[0] == "ClientAccountID":
                if header is not None:
                    sections.append((header, rows))
                header, rows = row, []
            elif header is None:
                raise _err(path, lineno, "IBKR Flex CSV did not start with a section header")
            else:
                rows.append((lineno, row))
        if header is not None:
            sections.append((header, rows))
    if not sections:
        raise _err(path, 0, "IBKR Flex CSV contains no sections")

    txns: list[Transaction] = []
    for header, rows in sections:
        report.rows_total += len(rows)
        cols = {name: idx for idx, name in enumerate(header)}
        if "Buy/Sell" in cols and "TradeDate" in cols:
            txns.extend(_parse_trades(path, cols, rows))
        elif {"Type", "Proceeds", "Value", "Quantity"} <= cols.keys():
            txns.extend(_parse_corporate_actions(path, cols, rows))
        elif {"Direction", "Quantity", "Type"} <= cols.keys():
            txns.extend(_parse_transfers(path, cols, rows))
        elif {"Amount", "Type", "Date/Time"} <= cols.keys() and "Quantity" not in cols:
            txns.extend(_parse_cash(path, cols, rows))
        else:
            raise _err(path, rows[0][0] if rows else 0,
                       f"Unrecognized IBKR Flex section (columns {header[:8]}...).")
    count_rows(txns, report)
    return txns, report


def _detail_rows(path: Path, cols: Cols, rows: list[tuple[int, Row]], wanted: str
                 ) -> list[tuple[int, Row]]:
    """Keep one level of detail: mixing levels would double-count."""
    if "LevelOfDetail" not in cols:
        return rows
    levels = {_get(r, cols, "LevelOfDetail") for _, r in rows}
    if wanted in levels:
        return [(n, r) for n, r in rows if _get(r, cols, "LevelOfDetail") == wanted]
    if len(levels) > 1:
        raise _err(path, rows[0][0], f"Mixed LevelOfDetail {sorted(levels)} without {wanted}.")
    return rows


def _asset_class(raw_class: str, sub: str, isin: str | None) -> AssetClass:
    if raw_class == "BOND":
        return asset_class_from_isin(isin, "BOND")
    if raw_class == "FUND" or sub == "ETF":
        return asset_class_from_isin(isin, "ETF")
    return asset_class_from_isin(isin, "STOCK")


# ---------------------------------------------------------------- Trades
def _parse_trades(path: Path, cols: Cols, rows: list[tuple[int, Row]]) -> list[Transaction]:
    out: list[Transaction] = []
    for lineno, row in _detail_rows(path, cols, rows, "EXECUTION"):
        try:
            out.extend(_parse_trade_row(path, lineno, cols, row))
        except ParserError:
            raise
        except (ValueError, IndexError, ArithmeticError) as e:
            raise _err(path, lineno, f"IBKR trade row parse failure: {e}", row) from e
    return out


def _parse_trade_row(path: Path, lineno: int, cols: Cols, row: Row) -> list[Transaction]:
    raw_class = _get(row, cols, "AssetClass").upper()
    buy_sell = _get(row, cols, "Buy/Sell").upper()
    if raw_class not in _SUPPORTED_TRADE_CLASSES:
        raise _err(path, lineno,
                   f"IBKR AssetClass {raw_class!r} is not supported. Listed options, "
                   "futures and CFDs at a foreign broker are non-securitised "
                   "derivatives taxed at the progressive tariff (§ 27a Abs 2 Z 7 "
                   "EStG, KZ 857) and must be declared separately.", row)
    if buy_sell not in {"BUY", "SELL"}:
        raise _err(path, lineno, f"Unsupported IBKR Buy/Sell={buy_sell!r} (cancellations "
                   "and corrections must be resolved in the export).", row)
    ttype = _get(row, cols, "TransactionType")
    if ttype not in _TRADE_TYPES:
        raise _err(path, lineno, f"Unsupported IBKR TransactionType {ttype!r}.", row)
    mult = _get(row, cols, "Multiplier") or "1"
    if parse_decimal(mult) != 1:
        raise _err(path, lineno, f"Multiplier {mult} != 1 is not supported.", row)

    currency = normalize_currency(_get(row, cols, "CurrencyPrimary"))
    trade_date = _ibkr_date(_get(row, cols, "TradeDate"))
    trade_dt = _ibkr_datetime(_get(row, cols, "DateTime"))
    raw_ref = _get(row, cols, "TransactionID") or _get(row, cols, "TradeID") or None
    symbol = _get(row, cols, "Symbol") or None
    qty = parse_decimal(_get(row, cols, "Quantity"))
    proceeds = parse_decimal(_get(row, cols, "Proceeds"))
    commission = parse_decimal(_get(row, cols, "IBCommission"))  # negative = cost
    comm_ccy = normalize_currency(_get(row, cols, "IBCommissionCurrency") or currency)
    taxes = parse_decimal(_get(row, cols, "Taxes"))  # transaction taxes, negative = cost
    out: list[Transaction] = []

    if raw_class == "CASH":
        base, _, quote = (symbol or "").partition(".")
        if len(base) != 3 or quote != currency:
            raise _err(path, lineno, f"Unexpected FX pair {symbol!r} for currency {currency}.",
                       row)
        out.append(Transaction(
            broker=BROKER, trade_date=trade_date, trade_datetime=trade_dt,
            tx_type=TxType.FX_CONVERSION, asset_class=AssetClass.CASH, quantity=ZERO,
            currency_native=currency, gross_native=proceeds,
            counter_currency=normalize_currency(base), counter_amount=qty,
            source_file=path.name, source_line=lineno, raw_ref=raw_ref, symbol=symbol,
            name=f"FX {symbol}", notes=_get(row, cols, "Notes/Codes") or None,
        ))
    else:
        isin = _get(row, cols, "ISIN") or None
        flags: list[Flag] = []
        if isin and not is_valid_isin(isin):
            raise _err(path, lineno, f"Invalid ISIN {isin!r}", row)
        if not isin:
            raise _err(path, lineno, "Trade without ISIN cannot be pooled.", row)
        fee = ZERO
        if comm_ccy == currency:
            fee -= commission
        fee -= taxes
        out.append(Transaction(
            broker=BROKER, trade_date=trade_date, trade_datetime=trade_dt,
            tx_type=TxType.BUY if buy_sell == "BUY" else TxType.SELL,
            asset_class=_asset_class(raw_class, _get(row, cols, "SubCategory").upper(), isin),
            quantity=abs(qty), currency_native=currency, gross_native=proceeds,
            source_file=path.name, source_line=lineno, raw_ref=raw_ref, isin=isin,
            symbol=symbol, name=_get(row, cols, "Description") or None,
            price_native=parse_decimal(_get(row, cols, "TradePrice")) or None,
            fee_native=fee, flags=flags,
        ))
    if commission and comm_ccy != currency:
        out.append(Transaction(
            broker=BROKER, trade_date=trade_date, trade_datetime=trade_dt,
            tx_type=TxType.FEE, asset_class=AssetClass.CASH, quantity=ZERO,
            currency_native=comm_ccy, gross_native=commission, source_file=path.name,
            source_line=lineno, raw_ref=f"{raw_ref}::commission" if raw_ref else None,
            name=f"IBKR commission {symbol or ''}".strip(),
        ))
    return out


# ---------------------------------------------------------- Cash transactions
_DIVIDEND_TYPES = {"Dividends", "Payment In Lieu Of Dividends"}
_WHT_TYPES = {"Withholding Tax", "871(m) Withholding"}
_SIMPLE_TYPES: dict[str, TxType | None] = {
    # IBKR Ireland is not a credit institution: the broker config routes
    # INTEREST to 27,5 % (EStR Rz 6223).
    "Broker Interest Received": TxType.INTEREST,
    "Bond Interest Received": TxType.INTEREST_OTHER,  # coupons
    "Broker Interest Paid": TxType.FEE,  # margin interest: not deductible
    "Commissions": TxType.FEE,
    "Commission Adjustments": TxType.FEE,
    "Other Fees": TxType.FEE,
    "Advisor Fees": TxType.FEE,
    "Deposits/Withdrawals": None,  # sign decides
    "Deposits & Withdrawals": None,
}


def _parse_cash(path: Path, cols: Cols, rows: list[tuple[int, Row]]) -> list[Transaction]:
    rows = _detail_rows(path, cols, rows, "DETAIL")
    out: list[Transaction] = []
    by_action: dict[str, list[tuple[int, Row]]] = defaultdict(list)
    loose_wht: list[tuple[int, Row]] = []
    interest_rows: list[Transaction] = []
    for lineno, row in rows:
        rtype = _get(row, cols, "Type")
        if not rtype:
            raise _err(path, lineno, "Cash row without Type", row)
        if rtype in _DIVIDEND_TYPES or rtype in _WHT_TYPES:
            action = _get(row, cols, "ActionID")
            if action:
                by_action[action].append((lineno, row))
            elif rtype in _WHT_TYPES:
                loose_wht.append((lineno, row))
            else:
                raise _err(path, lineno, f"{rtype} row without ActionID", row)
            continue
        if rtype == "Bond Interest Paid":
            raise _err(path, lineno, "Bond Interest Paid (accrued interest bought with a "
                       "bond) belongs to the bond's cost basis; not supported yet.", row)
        if rtype not in _SIMPLE_TYPES:
            raise _err(path, lineno, f"Unknown IBKR cash-transaction Type {rtype!r}", row)
        try:
            tx = _simple_cash_row(path, lineno, cols, row, rtype)
        except (ValueError, ArithmeticError) as e:
            raise _err(path, lineno, f"IBKR cash row parse failure: {e}", row) from e
        if tx.tx_type is TxType.INTEREST:
            interest_rows.append(tx)
        out.append(tx)
    for action, group in by_action.items():
        out.extend(_dividend_group(path, cols, action, group))
    for lineno, row in loose_wht:
        out.append(_loose_withholding(path, lineno, cols, row, interest_rows))
    return out


def _base_fields(path: Path, lineno: int, cols: Cols, row: Row) -> dict[str, object]:
    date_raw = _get(row, cols, "Date/Time") or _get(row, cols, "SettleDate")
    return {
        "broker": BROKER,
        "trade_date": _ibkr_date(date_raw),
        "trade_datetime": _ibkr_datetime(date_raw),
        "currency_native": normalize_currency(_get(row, cols, "CurrencyPrimary")),
        "source_file": path.name,
        "source_line": lineno,
        "raw_ref": _get(row, cols, "TransactionID") or None,
    }


def _simple_cash_row(path: Path, lineno: int, cols: Cols, row: Row, rtype: str) -> Transaction:
    amount = parse_decimal(_get(row, cols, "Amount"))
    mapped = _SIMPLE_TYPES[rtype]
    if mapped is None:
        mapped = TxType.DEPOSIT_CASH if amount >= 0 else TxType.WITHDRAWAL_CASH
    return make_tx(
            _base_fields(path, lineno, cols, row),
        tx_type=mapped, asset_class=AssetClass.CASH, quantity=ZERO, gross_native=amount,
        name=_get(row, cols, "Description") or None, notes=rtype,
    )


def _wht_country(cols: Cols, wh_rows: list[tuple[int, Row]]) -> str | None:
    found = {
        m.group(1)
        for _, r in wh_rows
        if (m := _TAX_COUNTRY_RE.search(_get(r, cols, "Description")))
    }
    if len(found) == 1:
        return found.pop()
    return None


def _dividend_group(
    path: Path, cols: Cols, action: str, group: list[tuple[int, Row]]
) -> list[Transaction]:
    """One dividend event (ActionID): net dividend and net withholding per
    booking date, so reversals and re-bookings on the same date cancel."""
    by_date: dict[date, list[tuple[int, Row]]] = defaultdict(list)
    for lineno, row in group:
        by_date[_ibkr_date(_get(row, cols, "Date/Time") or _get(row, cols, "SettleDate"))].append(
            (lineno, row)
        )
    out: list[Transaction] = []
    for _d, items in sorted(by_date.items()):
        divs = [(n, r) for n, r in items if _get(r, cols, "Type") in _DIVIDEND_TYPES]
        whts = [(n, r) for n, r in items if _get(r, cols, "Type") in _WHT_TYPES]
        currencies = {_get(r, cols, "CurrencyPrimary") for _, r in items}
        if len(currencies) != 1:
            raise _err(path, items[0][0], f"ActionID {action}: mixed currencies {currencies}")
        withheld = -sum((parse_decimal(_get(r, cols, "Amount")) for _, r in whts), ZERO)
        if not divs:
            # Withholding correction without its dividend on this date: one
            # net booking per ActionID and date.
            lineno, row = whts[0]
            net = sum((parse_decimal(_get(r, cols, "Amount")) for _, r in whts), ZERO)
            if net == 0:
                continue
            tx = make_tx(
            _base_fields(path, lineno, cols, row),
                tx_type=TxType.WITHHOLDING_TAX, asset_class=AssetClass.CASH,
                quantity=ZERO, gross_native=net, tax_withheld_native=-net,
                isin=_get(row, cols, "ISIN") or None,
                name=_get(row, cols, "Description") or None,
                withholding_country=_wht_country(cols, whts),
                link_id=f"ibkr-action:{action}",
            )
            tx.add_flag("ibkr.wht_correction", Severity.WARNING,
                        f"Withholding-tax booking for ActionID {action} without a "
                        "dividend on the same date (correction of an earlier "
                        "dividend?) — correct the credit of the dividend's year.")
            out.append(tx)
            continue
        lineno, first = divs[0]
        gross = sum((parse_decimal(_get(r, cols, "Amount")) for _, r in divs), ZERO)
        desc = _get(first, cols, "Description")
        isin = _get(first, cols, "ISIN") or None
        flags: list[Flag] = []
        if any(_get(r, cols, "Type") == "Payment In Lieu Of Dividends" for _, r in divs):
            flags.append(Flag(FLAG_PAYMENT_IN_LIEU, Severity.WARNING,
                              "Payment in lieu of dividends (securities lending): taxable "
                              "like a dividend, but the foreign tax is not credited "
                              "automatically."))
        if "RETURN OF CAPITAL" in desc.upper():
            flags.append(Flag(FLAG_ROC_LABEL, Severity.WARNING,
                              "Broker labels this distribution 'return of capital'. It is "
                              "taxed as a dividend unless an Einlagenrückzahlung is proven "
                              "(declare in rules/return_of_capital.yaml)."))
        country = _wht_country(cols, whts) if whts else None
        if country is None:
            country = _get(first, cols, "IssuerCountryCode") or country_from_isin(isin)
            if whts and (_get(first, cols, "SubCategory").upper() == "ADR"
                         or "ADR" in desc.upper()):
                flags.append(Flag(FLAG_WHT_COUNTRY_UNCERTAIN, Severity.WARNING,
                                  "Depositary receipt: withholding source country unknown."))
        if gross == 0 and withheld == 0:
            continue  # fully reversed on the same date
        out.append(make_tx(
            _base_fields(path, lineno, cols, first),
            tx_type=TxType.DIVIDEND_CASH,
            asset_class=_asset_class("STK", _get(first, cols, "SubCategory").upper(), isin),
            quantity=ZERO, gross_native=gross, isin=isin,
            symbol=_get(first, cols, "Symbol") or None, name=desc or None,
            tax_withheld_native=withheld, withholding_country=country,
            dividend_is_net=False,  # IBKR "Dividends" rows are GROSS
            link_id=f"ibkr-action:{action}", notes=f"IBKR ActionID={action}", flags=flags,
        ))
    return out


def _loose_withholding(
    path: Path, lineno: int, cols: Cols, row: Row, interest_rows: list[Transaction]
) -> Transaction:
    """Withholding without ActionID, e.g. 'WITHHOLDING @ 20% ON CREDIT INT FOR
    DEC-2025': attached to the matching interest row when unambiguous."""
    fields = _base_fields(path, lineno, cols, row)
    amount = parse_decimal(_get(row, cols, "Amount"))
    desc = _get(row, cols, "Description")
    m = _INTEREST_PERIOD_RE.search(desc)
    if m and "INT" in desc.upper():
        period = m.group(1)
        match = [
            t for t in interest_rows
            if t.currency_native == fields["currency_native"] and period in (t.name or "")
            and t.tax_withheld_native == 0
        ]
        if len(match) == 1:
            tx = match[0]
            tx.tax_withheld_native = -amount
            tx.dividend_is_net = False  # interest row is gross
            tx.add_flag("ibkr.interest_withholding", Severity.INFO,
                        f"Foreign tax {-amount} from {path.name}:{lineno} attached.")
            # The WHT row's cash effect is now part of the interest row.
            return make_tx(
                fields,
                tx_type=TxType.IGNORED, asset_class=AssetClass.CASH, quantity=ZERO,
                gross_native=ZERO, name=desc or None,
                notes=f"Folded into interest row {tx.source_file}:{tx.source_line}",
            )
    return make_tx(
        fields,
        tx_type=TxType.WITHHOLDING_TAX, asset_class=AssetClass.CASH, quantity=ZERO,
        gross_native=amount, tax_withheld_native=-amount, name=desc or None,
        isin=_get(row, cols, "ISIN") or None,
        withholding_country=_get(row, cols, "IssuerCountryCode") or None,
    )


# ---------------------------------------------------------- Corporate actions
def _parse_corporate_actions(
    path: Path, cols: Cols, rows: list[tuple[int, Row]]
) -> list[Transaction]:
    """FS/RS/IC/SO/SD/TC rows. Legs of one event share the ActionID."""
    rows = _detail_rows(path, cols, rows, "DETAIL")
    events: dict[str, list[tuple[int, Row]]] = defaultdict(list)
    for lineno, row in rows:
        key = _get(row, cols, "ActionID") or f"line:{lineno}"
        events[key].append((lineno, row))
    out: list[Transaction] = []
    for key, legs in events.items():
        codes = {_get(r, cols, "Type") for _, r in legs}
        if len(codes) != 1:
            raise _err(path, legs[0][0], f"Corporate action {key}: mixed types {codes}")
        code = codes.pop()
        if code not in {"FS", "RS", "IC", "SO", "SD", "TC"}:
            raise _err(path, legs[0][0], f"IBKR corporate action type {code!r} is not "
                       "supported; resolve it manually.", legs[0][1])
        txs = []
        for lineno, row in legs:
            if _get(row, cols, "AssetClass").upper() not in {"STK", "FUND", ""}:
                raise _err(path, lineno, "Corporate action on a non-stock asset.", row)
            isin = _get(row, cols, "ISIN") or None
            if not isin:
                raise _err(path, lineno, "Corporate action leg without ISIN.", row)
            proceeds = parse_decimal(_get(row, cols, "Proceeds"))
            qty = parse_decimal(_get(row, cols, "Quantity"))
            if proceeds and code != "TC":
                raise _err(path, lineno, f"Cash component in corporate action {code} "
                           "is not supported.", row)
            fields = _base_fields(path, lineno, cols, row)
            common: dict[str, object] = {
                **fields, "isin": isin, "symbol": _get(row, cols, "Symbol") or None,
                "name": _get(row, cols, "Description") or None,
                "asset_class": _asset_class("STK", _get(row, cols, "SubCategory").upper(), isin),
                "notes": f"IBKR corporate action {code}",
            }
            if code == "TC" and proceeds:
                if qty >= 0:
                    raise _err(path, lineno, "Merger leg with cash but no shares given up.", row)
                txs.append(make_tx(common, tx_type=TxType.SELL,
                                       quantity=-qty, gross_native=proceeds))
                continue
            txs.append(make_tx(common, tx_type=TxType.SPLIT,
                                   quantity=qty, gross_native=ZERO))
        if code == "TC" and any(t.tx_type is TxType.SELL for t in txs) and any(
            t.tx_type is TxType.SPLIT and t.quantity > 0 for t in txs
        ):
            raise _err(path, legs[0][0], "Merger with cash and shares (bare Zuzahlung) "
                       "is not supported; resolve it manually.")
        out.extend(_resolve_legs(txs, f"ibkr-ca:{key}", code))
    return out


def _resolve_legs(txs: list[Transaction], link: str, code: str) -> list[Transaction]:
    """Same-ISIN legs → one net SPLIT; one old ISIN out + one new ISIN in →
    tax-neutral EXCHANGE (KapMaßVO §§ 3, 8)."""
    splits = [t for t in txs if t.tx_type is TxType.SPLIT]
    others = [t for t in txs if t.tx_type is not TxType.SPLIT]
    net: dict[str, Transaction] = {}
    for t in splits:
        if t.isin in net:
            net[t.isin].quantity += t.quantity
            t.tx_type = TxType.IGNORED
            t.notes = f"Merged into {net[t.isin].source_file}:{net[t.isin].source_line}"
            others.append(t)
        else:
            net[t.isin or ""] = t
    legs = [t for t in net.values() if t.quantity != 0]
    for t in net.values():
        if t.quantity == 0:
            t.tx_type = TxType.IGNORED
            t.notes = "Corporate action legs cancel out"
            others.append(t)
    outs = [t for t in legs if t.quantity < 0]
    ins = [t for t in legs if t.quantity > 0]
    if len(outs) == 1 and len(ins) == 1 and outs[0].isin != ins[0].isin:
        o, i = outs[0], ins[0]
        o.tx_type, o.quantity, o.link_id = TxType.EXCHANGE_OUT, -o.quantity, link
        i.tx_type, i.link_id = TxType.EXCHANGE_IN, link
        flag = Flag("corporate_action.exchange", Severity.WARNING,
                    f"{code}: {o.isin} exchanged for {i.isin}; cost basis carried over "
                    "(tax-neutral only for an ISIN change or comparable merger — "
                    "declare otherwise).")
        o.flags.append(flag)
        i.flags.append(flag)
    elif code == "SD":
        for t in ins:
            t.flags.append(unclassified_share_increase(
                f"IBKR stock dividend (SD): {t.quantity} shares of {t.isin} received."
            ))
    elif code == "SO":
        for t in ins:
            t.flags.append(Flag("corporate_action.distribution", Severity.WARNING,
                                "SO: shares received. A new security needs a declaration "
                                "(spin-off basis split or taxable distribution); the pool "
                                "rejects it until declared."))
    return legs + others


# ---------------------------------------------------------------- Transfers
def _parse_transfers(path: Path, cols: Cols, rows: list[tuple[int, Row]]) -> list[Transaction]:
    out: list[Transaction] = []
    for lineno, row in rows:
        if _get(row, cols, "AssetClass").upper() not in {"STK", "FUND"}:
            raise _err(path, lineno, "Only stock/fund position transfers are supported.", row)
        isin = _get(row, cols, "ISIN") or None
        if not isin:
            raise _err(path, lineno, "Transfer without ISIN.", row)
        direction = _get(row, cols, "Direction").upper()
        if direction not in {"IN", "OUT"}:
            raise _err(path, lineno, f"Unknown transfer direction {direction!r}", row)
        qty = abs(parse_decimal(_get(row, cols, "Quantity")))
        value = parse_decimal(_get(row, cols, "PositionAmount"))
        out.append(make_tx(
            _base_fields(path, lineno, cols, row),
            tx_type=TxType.MIGRATION_IN if direction == "IN" else TxType.MIGRATION_OUT,
            asset_class=_asset_class("STK", _get(row, cols, "SubCategory").upper(), isin),
            quantity=qty, gross_native=ZERO, isin=isin,
            symbol=_get(row, cols, "Symbol") or None,
            name=_get(row, cols, "Description") or None,
            price_native=(abs(value) / qty) if value and qty else None,
            notes=f"IBKR transfer {_get(row, cols, 'Type')} {direction}",
        ))
    return out
