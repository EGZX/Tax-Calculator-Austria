"""Trading 212 (Invest) CSV parser.

Columns appear only when the export period contains data for them (e.g.
``Withholding tax``, ``Currency conversion fee``, ``Stamp duty``). Values:

* Trades: ``Price / share`` in the instrument currency; ``Exchange rate`` =
  instrument currency per 1 unit of the account currency (T212's rate);
  ``Total`` = account-currency cash incl. fees. The trade is booked in the
  instrument currency (valued at the ECB rate) and, when the account
  currency differs, T212's automatic conversion is booked as an
  ``FX_CONVERSION`` at T212's actual rate (EStR Rz 6201b/c). Fees are
  separate ``FEE`` rows (non-deductible, § 20 Abs 2 EStG).
* Dividends: ``Price / share`` is the dividend per share AFTER foreign tax;
  ``Withholding tax`` is in ``Currency (Withholding tax)``. Gross =
  shares × price + withholding (instrument currency).

T212 reuses IDs across different transactions, so the dedup reference is
``ID|Action|Time``. Unknown actions raise :class:`ParserError`.
"""

from __future__ import annotations

import csv
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

from ..model import (
    FLAG_PAYMENT_IN_LIEU,
    FLAG_ROC_LABEL,
    FLAG_WHT_NOT_DISCLOSED,
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
    parse_datetime,
    parse_decimal,
)
from .base import asset_class_from_isin, count_rows, file_sha256, make_tx

BROKER = "trading212"
ZERO = Decimal("0")

_BUYS = {"Market buy", "Limit buy", "Stop buy", "Stop limit buy"}
_SELLS = {"Market sell", "Limit sell", "Stop sell", "Stop limit sell"}
_DIVIDENDS = {
    "Dividend (Ordinary)",
    "Dividend (Dividend)",
    "Dividend (Dividends paid by us corporations)",
    "Dividend (Dividends paid by foreign corporations)",
    "Dividend (Bonus)",
    "Dividend (Property income distribution)",
    "Dividend (Interest)",  # interest distribution of a bond / money-market fund
}
_DIVIDEND_PIL = {"Dividend (Dividend manufactured payment)"}
# Labels suggesting capital repayment: taxed as dividends unless proven.
_DIVIDEND_ROC_LABEL = {"Dividend (Return of capital)", "Dividend (Tax exempted)",
                       "Dividend (Demerger)"}
_CARD = {"Card debit", "Card credit", "Card refund", "New card cost", "Card fee"}
_FEE_COLUMNS = (
    "Currency conversion fee",
    "Transaction fee",
    "Finra fee",
    "Stamp duty",
    "Stamp duty reserve tax",
    "French transaction tax",
    "Deposit fee",
)


class _Row:
    """Access helper tolerant of old header variants like ``Total (EUR)``."""

    def __init__(self, row: dict[str, str], header: list[str]) -> None:
        self.row = row
        self.header = header

    def get(self, key: str) -> str:
        return (self.row.get(key) or "").strip()

    def has(self, key: str) -> bool:
        return key in self.header

    def money(self, key: str) -> tuple[Decimal, str | None]:
        """Value and currency of ``key`` (``key`` + ``Currency (key)`` or ``key (CCY)``)."""
        if self.has(key):
            ccy = self.get(f"Currency ({key})")
            return parse_decimal(self.get(key)), normalize_currency(ccy) if ccy else None
        for h in self.header:
            if h.startswith(f"{key} (") and h.endswith(")") and len(h) == len(key) + 6:
                return parse_decimal(self.get(h)), normalize_currency(h[-4:-1])
        return ZERO, None


def parse(path: Path) -> tuple[list[Transaction], ParseReport]:
    report = ParseReport(broker=BROKER, source_file=str(path), file_sha256=file_sha256(path))
    txns: list[Transaction] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        header = list(reader.fieldnames or [])
        for lineno, raw in enumerate(reader, start=2):
            report.rows_total += 1
            row = _Row(raw, header)
            try:
                txns.extend(_parse_row(path, lineno, row))
            except ParserError:
                raise
            except (ValueError, ArithmeticError) as e:
                raise ParserError(
                    str(e), broker=BROKER, source_file=str(path), source_line=lineno,
                    raw=",".join(f"{k}={v}" for k, v in raw.items()),
                ) from e
    _pair_split_rows(path, txns)
    count_rows(txns, report)
    return txns, report


def _err(path: Path, lineno: int, msg: str) -> ParserError:
    return ParserError(msg, broker=BROKER, source_file=str(path), source_line=lineno)


def _parse_row(path: Path, lineno: int, row: _Row) -> list[Transaction]:
    action = row.get("Action")
    time_raw = row.get("Time") or row.get("Time (UTC)")
    if not time_raw:
        raise _err(path, lineno, "Row without Time")
    trade_dt = parse_datetime(time_raw)
    isin = row.get("ISIN") or None
    if isin and not is_valid_isin(isin):
        raise _err(path, lineno, f"Invalid ISIN: {isin!r}")
    ident = row.get("ID")
    raw_ref = f"{ident}|{action}|{time_raw}" if ident else None
    base = {
        "broker": BROKER,
        "trade_date": trade_dt.date(),
        "trade_datetime": trade_dt,
        "source_file": path.name,
        "source_line": lineno,
        "raw_ref": raw_ref,
        "isin": isin,
        "symbol": row.get("Ticker") or None,
        "name": row.get("Name") or None,
    }
    broker_note = row.get("Notes") or None
    total, total_ccy = row.money("Total")
    acct_ccy = total_ccy or "EUR"
    shares = parse_decimal(row.get("No. of shares"))
    price = parse_decimal(row.get("Price / share"))
    price_ccy = normalize_currency(row.get("Currency (Price / share)") or acct_ccy)
    rate_raw = row.get("Exchange rate")

    def fx_rate() -> Decimal:
        if price_ccy == acct_ccy:
            return Decimal("1")
        rate = parse_decimal(rate_raw) if rate_raw and rate_raw != "Not available" else ZERO
        if rate <= 0:
            raise _err(path, lineno, f"{action}: no exchange rate for {price_ccy}->{acct_ccy}")
        return rate

    def fees() -> list[Transaction]:
        out = []
        for col in _FEE_COLUMNS:
            amount, ccy = row.money(col)
            if amount:
                out.append(make_tx(
                    base, raw_ref=f"{raw_ref}::{col}" if raw_ref else None,
                    tx_type=TxType.FEE, asset_class=AssetClass.CASH, quantity=ZERO,
                    currency_native=ccy or acct_ccy, gross_native=-abs(amount),
                    notes=f"Trading 212 {col}",
                ))
        return out

    def conversion(instr_amount: Decimal, label: str) -> list[Transaction]:
        """T212's automatic conversion between account and instrument currency.
        ``instr_amount``: signed instrument-currency flow of the conversion."""
        if price_ccy == acct_ccy or instr_amount == 0:
            return []
        acct_amount = -(instr_amount / fx_rate())
        return [make_tx(
            base, raw_ref=f"{raw_ref}::fx" if raw_ref else None,
            tx_type=TxType.FX_CONVERSION, asset_class=AssetClass.CASH, quantity=ZERO,
            currency_native=price_ccy, gross_native=instr_amount,
            counter_currency=acct_ccy, counter_amount=acct_amount,
            notes=f"Trading 212 automatic conversion ({label}) at {rate_raw}",
        )]

    if action in _BUYS or action in _SELLS:
        if not isin or shares <= 0:
            raise _err(path, lineno, f"{action} without ISIN or shares")
        is_buy = action in _BUYS
        gross = shares * price
        tx = make_tx(
            base, tx_type=TxType.BUY if is_buy else TxType.SELL,
            asset_class=asset_class_from_isin(isin), quantity=shares,
            currency_native=price_ccy, gross_native=-gross if is_buy else gross,
            price_native=price, notes=broker_note,
        )
        # Buy: account currency → instrument currency first; sell: after.
        return [tx, *conversion(gross if is_buy else -gross, action), *fees()]

    if action in _DIVIDENDS or action in _DIVIDEND_PIL or action in _DIVIDEND_ROC_LABEL:
        if not isin:
            raise _err(path, lineno, f"{action} without ISIN")
        net = shares * price
        flags: list[Flag] = []
        if row.has("Withholding tax"):
            wht, wht_ccy = row.money("Withholding tax")
            wht = abs(wht)
            if wht and wht_ccy not in (None, price_ccy):
                if wht_ccy != acct_ccy:
                    raise _err(path, lineno, f"Withholding in unexpected currency {wht_ccy}")
                wht = wht * fx_rate()
        else:
            wht = ZERO
            flags.append(Flag(FLAG_WHT_NOT_DISCLOSED, Severity.WARNING,
                              "Export has no 'Withholding tax' column: the gross dividend "
                              "and the creditable foreign tax are unknown."))
        if action in _DIVIDEND_PIL:
            flags.append(Flag(FLAG_PAYMENT_IN_LIEU, Severity.WARNING,
                              "Manufactured dividend (securities lending): foreign tax is "
                              "not credited automatically."))
        if action in _DIVIDEND_ROC_LABEL:
            flags.append(Flag(FLAG_ROC_LABEL, Severity.WARNING,
                              f"'{action}': taxed as a dividend unless an "
                              "Einlagenrückzahlung is proven (rules/return_of_capital.yaml)."))
        tx = make_tx(
            base, tx_type=TxType.DIVIDEND_CASH, asset_class=asset_class_from_isin(isin),
            quantity=ZERO, currency_native=price_ccy, gross_native=net + wht,
            tax_withheld_native=wht, dividend_is_net=False,
            withholding_country=country_from_isin(isin), flags=flags,
        )
        return [tx, *conversion(-net, action)]

    if action == "Deposit":
        promo = "Free Shares Promotion" in (row.get("Notes") or "")
        tx_type = TxType.PROMO_BONUS if promo else TxType.DEPOSIT_CASH
        return [make_tx(base, tx_type=tx_type, asset_class=AssetClass.CASH, quantity=ZERO,
                            currency_native=acct_ccy, gross_native=total, notes=broker_note),
                *fees()]
    simple: dict[str, TxType] = {
        "Withdrawal": TxType.WITHDRAWAL_CASH,
        "Interest on cash": TxType.INTEREST,  # T212 is no credit institution → 27,5 %
        "Lending interest": TxType.INTEREST_OTHER,
        "Spending cashback": TxType.CASHBACK,
        "Card cashback": TxType.CASHBACK,
    }
    if action in simple:
        return [make_tx(base, tx_type=simple[action], asset_class=AssetClass.CASH,
                            quantity=ZERO, currency_native=acct_ccy, gross_native=total,
                            notes=broker_note)]
    if action in _CARD:
        return [make_tx(base, tx_type=TxType.IGNORED, asset_class=AssetClass.CASH,
                            quantity=ZERO, currency_native=acct_ccy, gross_native=total)]
    if action == "Result adjustment":
        tx = make_tx(base, tx_type=TxType.FEE, asset_class=AssetClass.CASH,
                         quantity=ZERO, currency_native=acct_ccy, gross_native=total)
        tx.add_flag("t212.result_adjustment", Severity.WARNING,
                    "Result adjustment (broker correction/compensation): no tax effect "
                    "assumed — verify.")
        return [tx]
    if action == "Currency conversion":
        from_amt, from_ccy = row.money("Currency conversion from amount")
        to_amt, to_ccy = row.money("Currency conversion to amount")
        if not from_ccy or not to_ccy:
            raise _err(path, lineno, "Currency conversion without from/to amount columns")
        return [make_tx(
            base, tx_type=TxType.FX_CONVERSION, asset_class=AssetClass.CASH, quantity=ZERO,
            currency_native=to_ccy, gross_native=abs(to_amt),
            counter_currency=from_ccy, counter_amount=-abs(from_amt),
        ), *fees()]
    if action in {"Stock split open", "Stock split close"}:
        if not isin:
            raise _err(path, lineno, f"{action} without ISIN")
        return [make_tx(base, tx_type=TxType.IGNORED, asset_class=asset_class_from_isin(isin),
                            quantity=shares, currency_native=price_ccy, gross_native=ZERO,
                            price_native=price, link_id=action)]
    if action in {"Spin off", "Stock distribution", "Custom stock distribution"}:
        if not isin or shares <= 0:
            raise _err(path, lineno, f"{action} without ISIN or shares")
        tx = make_tx(base, tx_type=TxType.SPLIT, asset_class=asset_class_from_isin(isin),
                         quantity=shares, currency_native=price_ccy, gross_native=ZERO,
                         price_native=price)
        tx.add_flag("corporate_action.distribution", Severity.WARNING,
                    f"{action}: treated as a tax-neutral split if the security is already "
                    "held; otherwise a declaration (spin-off basis split or taxable "
                    "distribution) is required.")
        return [tx]
    if action in {"Transfer in", "Transfer out"}:
        if not isin or shares <= 0:
            raise _err(path, lineno, f"{action} without ISIN or shares")
        return [make_tx(
            base,
            tx_type=TxType.MIGRATION_IN if action == "Transfer in" else TxType.MIGRATION_OUT,
            asset_class=asset_class_from_isin(isin), quantity=shares,
            currency_native=price_ccy, gross_native=ZERO, price_native=price or None,
        )]
    raise _err(path, lineno, f"Unknown or unsupported Trading 212 Action {action!r}")


def _pair_split_rows(path: Path, txns: list[Transaction]) -> None:
    """``Stock split close`` (position before) + ``Stock split open`` (position
    after) at the same timestamp → one SPLIT delta (or an exchange if the ISIN
    changed)."""
    groups: dict[object, list[Transaction]] = defaultdict(list)
    for t in txns:
        if t.link_id in {"Stock split open", "Stock split close"}:
            groups[t.trade_datetime].append(t)
    for _ts, legs in groups.items():
        closes = [t for t in legs if t.link_id == "Stock split close"]
        opens = [t for t in legs if t.link_id == "Stock split open"]
        if len(closes) != 1 or len(opens) != 1:
            raise _err(path, legs[0].source_line,
                       "Stock split rows must come as one close/open pair per timestamp")
        c, o = closes[0], opens[0]
        c.link_id = o.link_id = None
        if c.isin == o.isin:
            o.tx_type, o.quantity = TxType.SPLIT, o.quantity - c.quantity
            c.notes = f"Split close leg (merged into {o.source_file}:{o.source_line})"
        else:
            link = f"t212-split:{c.isin}->{o.isin}:{o.trade_datetime}"
            c.tx_type, c.link_id = TxType.EXCHANGE_OUT, link
            o.tx_type, o.link_id = TxType.EXCHANGE_IN, link
