"""Scalable Capital CSV parser.

Format (semicolon-separated, decimal comma, thousands dot):
    date;time;status;reference;description;assetType;type;isin;shares;price;
    amount;fee;tax;currency

* Only ``Executed``/``Settled`` rows are processed; others stay IGNORED.
* ``Buy``/``Sell``/``Savings plan``: ``amount`` = shares × price (signed),
  ``fee`` separate (positive = cost). A non-zero ``tax`` on a trade is a
  transaction tax (e.g. Spanish/French/Italian FTT) — a non-deductible cost.
* Asset class: the export has none, so it is derived from the product name
  ("UCITS"/"ETF" → fund; "Optionsschein", "Turbo", "Zertifikat", ETC/ETN,
  leveraged ETPs → securitised derivative; otherwise share). Pin anything
  else in rules/asset_class_overrides.yaml.
* ``Distribution``: ``amount`` is NET of foreign withholding, ``tax`` holds
  the withholding (positive). The ISIN country is used as withholding
  country except for depositary receipts (name contains ADR/GDR), whose
  source country must be declared (rules/withholding_countries.yaml).
* ``Interest``: deposit interest (Baader Bank / Scalable Capital Bank). The
  ``tax`` column on interest rows is not a withholding of this account and
  is ignored with a warning.
* ``Corporate action``: signed share deltas. Legs of one date are resolved
  here: same-ISIN legs are netted into one SPLIT; one ISIN going out and
  another coming in with equal valuation is a tax-neutral exchange (ISIN
  change, KapMaßVO § 8). Scalable books corporate actions under German tax
  law — its valuation (``amount``) is used only to pair legs, never as basis.
  Old shares out + new shares in (same ISIN) is a split. Shares only added
  (no outgoing leg) look the same for a US split effected as a stock
  dividend, a capital increase from company funds and a taxable stock
  dividend / bonus issue (e.g. BYD 2025: 12:10 from capital reserve, 8:10
  out of profits), so those need a declaration (blocker until declared).
* ``Security transfer``: MIGRATION_OUT/IN legs (price = valuation at
  transfer). Their tax treatment needs a declaration
  (rules/depot_transfers.yaml) — the Dec-2025 Baader Bank → Scalable Capital
  Bank move is a notifiable foreign-to-foreign depot transfer.
"""

from __future__ import annotations

import csv
import re
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from ..model import (
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

BROKER = "scalable_capital"
ZERO = Decimal("0")

_TRADES = {"Buy": TxType.BUY, "Savings plan": TxType.BUY, "Sell": TxType.SELL}
_CASH = {
    "Deposit": TxType.DEPOSIT_CASH,
    "Withdrawal": TxType.WITHDRAWAL_CASH,
    "Fee": TxType.FEE,
    "Interest": TxType.INTEREST,
}
_DR_RE = re.compile(r"\b(ADR|GDR|ADS|DEPOSITARY)\b", re.IGNORECASE)
_CA_NOTE = "Scalable corporate action"
_FUND_NAME_RE = re.compile(r"\bUCITS\b|\bETF\b", re.IGNORECASE)
_DERIVATIVE_NAME_RE = re.compile(
    r"Optionsschein|Zertifikat|\bTurbo|Knock[- ]?Out|Mini[- ]?Future|Faktor|"
    r"\bETC\b|\bETN\b|\bLeveraged\b",
    re.IGNORECASE,
)


def parse(path: Path) -> tuple[list[Transaction], ParseReport]:
    report = ParseReport(broker=BROKER, source_file=str(path), file_sha256=file_sha256(path))
    txns: list[Transaction] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for lineno, row in enumerate(csv.DictReader(f, delimiter=";"), start=2):
            report.rows_total += 1
            try:
                txns.append(_parse_row(path, lineno, row, report))
            except ParserError:
                raise
            except (ValueError, ArithmeticError) as e:
                raise ParserError(
                    str(e), broker=BROKER, source_file=str(path), source_line=lineno,
                    raw=";".join(f"{k}={v}" for k, v in row.items()),
                ) from e
    _resolve_corporate_actions(path, txns)
    count_rows(txns, report)
    return txns, report


def _parse_row(path: Path, lineno: int, row: dict[str, str], report: ParseReport) -> Transaction:
    def dec(key: str) -> Decimal:
        return parse_decimal(row.get(key), decimal_sep=",")

    status = (row.get("status") or "").strip()
    raw_type = (row.get("type") or "").strip()
    isin = (row.get("isin") or "").strip() or None
    if isin and not is_valid_isin(isin):
        raise ParserError(f"Invalid ISIN: {isin!r}", broker=BROKER, source_file=str(path),
                          source_line=lineno)
    trade_date = parse_date(row["date"])
    trade_dt = None
    if t := (row.get("time") or "").strip():
        trade_dt = datetime.fromisoformat(f"{trade_date.isoformat()}T{t}")
    name = (row.get("description") or "").strip().strip('"') or None
    base = {
        "broker": BROKER,
        "trade_date": trade_date,
        "trade_datetime": trade_dt,
        "currency_native": normalize_currency(row.get("currency")),
        "source_file": path.name,
        "source_line": lineno,
        "raw_ref": (row.get("reference") or "").strip().strip('"') or None,
        "isin": isin,
        "name": name,
    }
    shares, price, amount = dec("shares"), dec("price"), dec("amount")
    fee, tax = dec("fee"), dec("tax")
    hint = (row.get("assetType") or "").strip()
    if hint in {"Cash", "Security"}:
        # Scalable's export has no instrument type; the product name does.
        # UCITS funds first (a "Leveraged UCITS ETF" is a fund), then
        # securitised derivatives (warrants, certificates, ETC/ETN).
        hint = ""
        if name and _FUND_NAME_RE.search(name):
            hint = "ETF"
        elif name and _DERIVATIVE_NAME_RE.search(name):
            hint = "DERIVATIVE"
    security_class = asset_class_from_isin(isin, hint or None)

    if status not in {"Executed", "Settled"}:
        report.rows_rejected += 1
        return make_tx(base, tx_type=TxType.IGNORED, asset_class=security_class,
                           quantity=shares, gross_native=amount, notes=f"Status={status}")

    if raw_type == "Corporate action":
        if not isin or shares == 0:
            raise ParserError("Corporate action without ISIN or shares", broker=BROKER,
                              source_file=str(path), source_line=lineno)
        return make_tx(base, tx_type=TxType.SPLIT, asset_class=security_class,
                           quantity=shares, gross_native=ZERO, price_native=price or None,
                           notes=f"{_CA_NOTE}: {shares:+} @ {price} (valuation {amount})")

    if raw_type == "Security transfer":
        if not isin or shares == 0:
            raise ParserError("Scalable 'Security transfer' row with zero shares is "
                              "ambiguous (no direction).", broker=BROKER,
                              source_file=str(path), source_line=lineno)
        return make_tx(
            base, tx_type=TxType.MIGRATION_IN if shares > 0 else TxType.MIGRATION_OUT,
            asset_class=security_class, quantity=abs(shares), gross_native=ZERO,
            price_native=price or None,
            notes=f"Security transfer (valuation {amount} {base['currency_native']})",
        )

    if raw_type in _TRADES:
        if not isin or shares <= 0:
            raise ParserError(f"{raw_type} without ISIN or shares", broker=BROKER,
                              source_file=str(path), source_line=lineno)
        tx = make_tx(base, tx_type=_TRADES[raw_type], asset_class=security_class,
                         quantity=shares, gross_native=amount, price_native=price or None,
                         fee_native=fee + tax)
        if tax:
            tx.add_flag("scalable.transaction_tax", Severity.INFO,
                        f"tax {tax} on a trade treated as transaction tax (non-deductible).")
        return tx

    if raw_type == "Distribution":
        if not isin:
            raise ParserError("Distribution without ISIN", broker=BROKER,
                              source_file=str(path), source_line=lineno)
        flags: list[Flag] = []
        if tax and name and _DR_RE.search(name):
            flags.append(Flag(FLAG_WHT_COUNTRY_UNCERTAIN, Severity.WARNING,
                              "Depositary receipt: the foreign tax was levied by the "
                              "issuer's state, not the ISIN country."))
        return make_tx(
            base, tx_type=TxType.DIVIDEND_CASH, asset_class=security_class,
            quantity=ZERO, gross_native=amount, tax_withheld_native=tax,
            dividend_is_net=True, withholding_country=country_from_isin(isin), flags=flags,
        )

    if raw_type in _CASH:
        tx = make_tx(base, tx_type=_CASH[raw_type], asset_class=AssetClass.CASH,
                         quantity=ZERO, gross_native=amount)
        if raw_type == "Interest" and tax:
            tx.add_flag("scalable.interest_tax_ignored", Severity.WARNING,
                        f"'tax' {tax} on an interest row is not a withholding of this "
                        "account and was ignored; verify against the broker tax report.")
        return tx

    raise ParserError(f"Unknown Scalable type {raw_type!r}", broker=BROKER,
                      source_file=str(path), source_line=lineno)


def _resolve_corporate_actions(path: Path, txns: list[Transaction]) -> None:
    """Net same-ISIN legs per date; pair an outgoing and an incoming ISIN of
    equal valuation as a tax-neutral exchange."""
    by_date: dict[date, list[Transaction]] = defaultdict(list)
    for t in txns:
        if t.tx_type is TxType.SPLIT and (t.notes or "").startswith(_CA_NOTE):
            by_date[t.trade_date].append(t)
    for d, legs in by_date.items():
        net: dict[str, Transaction] = {}
        netted: set[int] = set()
        has_out: set[str] = set()  # ISINs with an outgoing leg on this date
        for t in sorted(legs, key=lambda x: x.source_line):
            if t.quantity < 0:
                has_out.add(t.isin or "")
            keep = net.get(t.isin or "")
            if keep is None:
                net[t.isin or ""] = t
                continue
            keep.quantity += t.quantity
            netted.add(keep.source_line)
            t.tx_type = TxType.IGNORED
            t.notes = f"{t.notes} — netted into {keep.source_file}:{keep.source_line}"
        for t in list(net.values()):
            if t.quantity == 0:
                t.tx_type = TxType.IGNORED
                t.notes = f"{t.notes} — legs cancel out (booking + reversal)"
                del net[t.isin or ""]
        outs = [t for t in net.values() if t.quantity < 0]
        ins = [t for t in net.values() if t.quantity > 0]
        for o in outs:
            o_val = _valuation(o) if o.source_line not in netted else None
            match = [
                i for i in ins
                if o_val and i.source_line not in netted and _valuation(i) == o_val
            ]
            if len(match) != 1 and len(outs) == 1 and len(ins) == 1:
                match = ins  # one out, one in, no valuation: still an exchange
                ins[0].add_flag("scalable.exchange_unvalued", Severity.WARNING,
                                "Exchange legs paired without matching valuation; verify.")
            if len(match) != 1:
                continue  # left as SPLIT: the pool rejects it unless declared
            i = match[0]
            ins.remove(i)
            link = f"scalable-ca:{d.isoformat()}:{o.isin}->{i.isin}"
            o.tx_type, o.quantity, o.link_id = TxType.EXCHANGE_OUT, -o.quantity, link
            i.tx_type, i.link_id = TxType.EXCHANGE_IN, link
            for leg in (o, i):
                leg.add_flag("corporate_action.exchange", Severity.WARNING,
                             f"{o.isin} exchanged for {i.isin} on {d}: cost basis carried "
                             "over (tax-neutral ISIN change / comparable merger, KapMaßVO "
                             "§§ 3, 8). Declare otherwise if consideration was paid.")
        for t in net.values():
            if t.tx_type is not TxType.SPLIT or t.quantity <= 0:
                continue
            if (t.isin or "") in has_out:
                t.add_flag("corporate_action.split", Severity.INFO,
                           "Old shares booked out and new shares in: share split, cost "
                           "basis unchanged (KapMaßVO § 6).")
            else:
                t.flags.append(unclassified_share_increase(
                    f"Scalable added {t.quantity} shares of {t.isin} on {d} without "
                    "booking the old shares out (split effected as a stock dividend, "
                    "capital increase from company funds or taxable bonus shares)."
                ))


def _valuation(t: Transaction) -> Decimal | None:
    """Scalable's valuation of a leg (shares × price), used only for pairing."""
    if not t.price_native:
        return None
    return abs(t.quantity * t.price_native).quantize(Decimal("0.01"))
