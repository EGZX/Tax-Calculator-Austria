"""Scalable Capital CSV parser.

Format (semicolon-separated, comma decimal, header row):
    date;time;status;reference;description;assetType;type;isin;shares;price;
    amount;fee;tax;currency

`status` may be 'Executed', 'Rejected', 'Cancelled' — only Executed rows are
processed for tax. Others are kept as IGNORED in the audit trail.

`type` is a small enum: Buy, Sell, Distribution (cash dividends/capital
returns), Interest (KKT savings interest), Deposit, Withdrawal, Fee.
"""

from __future__ import annotations

import csv
from datetime import datetime
from decimal import Decimal
from pathlib import Path
import re

import yaml

from ..model import (
    AssetClass,
    Flag,
    ParserError,
    ParseReport,
    Severity,
    Transaction,
    TxType,
)
from ..normalize import country_from_isin, is_valid_isin, normalize_currency, parse_date, parse_decimal
from .base import asset_class_from_isin, file_sha256

BROKER = "scalable_capital"

# Map Scalable's `type` to canonical TxType. None → IGNORED row.
_TYPE_MAP: dict[str, TxType] = {
    "Buy": TxType.BUY,
    "Savings plan": TxType.BUY,  # recurring buy
    "Sell": TxType.SELL,
    "Distribution": TxType.DIVIDEND_CASH,
    "Interest": TxType.INTEREST,
    "Deposit": TxType.DEPOSIT_CASH,
    "Withdrawal": TxType.WITHDRAWAL_CASH,
    "Fee": TxType.FEE,
}

_DIST_OVERRIDES_FILE = (
    Path(__file__).resolve().parents[3] / "rules" / "scalable_distribution_overrides.yaml"
)
_DIST_OVERRIDES_CACHE: list[dict[str, object]] | None = None
_DIST_OVERRIDES_MTIME: float | None = None


def _load_distribution_overrides() -> list[dict[str, object]]:
    """Load optional Scalable Distribution overrides from YAML.

    The file is optional. If present, each entry may pin specific rows to
    RETURN_OF_CAPITAL based on ISIN/date/reference filters.
    """
    global _DIST_OVERRIDES_CACHE, _DIST_OVERRIDES_MTIME
    if not _DIST_OVERRIDES_FILE.exists():
        _DIST_OVERRIDES_CACHE = []
        _DIST_OVERRIDES_MTIME = None
        return _DIST_OVERRIDES_CACHE
    mtime = _DIST_OVERRIDES_FILE.stat().st_mtime
    if _DIST_OVERRIDES_CACHE is not None and _DIST_OVERRIDES_MTIME == mtime:
        return _DIST_OVERRIDES_CACHE
    raw = yaml.safe_load(_DIST_OVERRIDES_FILE.read_text(encoding="utf-8")) or {}
    entries = raw.get("overrides") or []
    if not isinstance(entries, list):
        raise ValueError(
            f"{_DIST_OVERRIDES_FILE} must contain a top-level 'overrides' list"
        )
    out: list[dict[str, object]] = []
    for i, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise ValueError(
                f"{_DIST_OVERRIDES_FILE} overrides[{i}] must be a mapping"
            )
        tx_type_raw = str(entry.get("tx_type") or "").upper().strip()
        if tx_type_raw != "RETURN_OF_CAPITAL":
            raise ValueError(
                f"{_DIST_OVERRIDES_FILE} overrides[{i}].tx_type must be "
                "'RETURN_OF_CAPITAL'"
            )
        isin = str(entry.get("isin") or "").upper().strip()
        if not isin:
            raise ValueError(
                f"{_DIST_OVERRIDES_FILE} overrides[{i}] requires non-empty 'isin'"
            )
        date_from_raw = str(entry.get("date_from") or "").strip()
        date_to_raw = str(entry.get("date_to") or "").strip()
        ref_pat_raw = str(entry.get("reference_regex") or "").strip()
        desc_pat_raw = str(entry.get("description_regex") or "").strip()
        parsed = {
            "isin": isin,
            "date_from": parse_date(date_from_raw) if date_from_raw else None,
            "date_to": parse_date(date_to_raw) if date_to_raw else None,
            "reference_regex": re.compile(ref_pat_raw) if ref_pat_raw else None,
            "description_regex": re.compile(desc_pat_raw) if desc_pat_raw else None,
            "note": str(entry.get("note") or "").strip(),
        }
        out.append(parsed)
    _DIST_OVERRIDES_CACHE = out
    _DIST_OVERRIDES_MTIME = mtime
    return _DIST_OVERRIDES_CACHE


def _distribution_override_for(
    *,
    isin: str | None,
    trade_date,
    raw_ref: str | None,
    name: str | None,
) -> dict[str, object] | None:
    if not isin:
        return None
    up_isin = isin.upper()
    for ov in _load_distribution_overrides():
        if ov["isin"] != up_isin:
            continue
        date_from = ov["date_from"]
        date_to = ov["date_to"]
        if date_from is not None and trade_date < date_from:
            continue
        if date_to is not None and trade_date > date_to:
            continue
        ref_re = ov["reference_regex"]
        if ref_re is not None and not ref_re.search(raw_ref or ""):
            continue
        desc_re = ov["description_regex"]
        if desc_re is not None and not desc_re.search(name or ""):
            continue
        return ov
    return None


def reset_distribution_overrides_cache() -> None:  # pragma: no cover - test helper
    global _DIST_OVERRIDES_CACHE, _DIST_OVERRIDES_MTIME
    _DIST_OVERRIDES_CACHE = None
    _DIST_OVERRIDES_MTIME = None


def parse(path: Path) -> tuple[list[Transaction], ParseReport]:
    report = ParseReport(broker=BROKER, source_file=str(path), file_sha256=file_sha256(path))
    txns: list[Transaction] = []

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for lineno, row in enumerate(reader, start=2):  # +1 for header, 1-indexed
            report.rows_total += 1
            try:
                tx = _parse_row(path, lineno, row, report)
            except (ParserError, ValueError) as e:
                # Re-raise as ParserError so caller fails loudly.
                if isinstance(e, ParserError):
                    raise
                raise ParserError(
                    str(e),
                    broker=BROKER,
                    source_file=str(path),
                    source_line=lineno,
                    raw=";".join(f"{k}={v}" for k, v in row.items()),
                ) from e
            if tx is None:
                continue
            txns.append(tx)
            if tx.tx_type is TxType.IGNORED:
                report.rows_ignored += 1
            else:
                report.rows_emitted += 1

    # Post-process: detect paired MIGRATION_IN / MIGRATION_OUT rows emitted from
    # "Security transfer" CSV rows. Scalable uses "Security transfer" both for
    # external depot migrations (cost basis not present in the file) AND for
    # its own internal custodian rebookings, where an OUT on day D-1 is
    # followed by an IN on day D with the same ISIN and identical abs(qty).
    # Internal rebookings are pool-neutral: cost basis continues through the
    # pair. Re-classify those as TxType.SPLIT so the pool layer preserves
    # total_cost across the event instead of wiping it and demanding fresh
    # basis at MIGRATION_IN. Orphan (unpaired) rows stay as MIGRATION_* so a
    # genuine external transfer still blocks a later SELL loudly.
    _repair_paired_security_transfers(txns)

    return txns, report


def _repair_paired_security_transfers(txns: list[Transaction]) -> None:
    """Mutate in place: convert matched MIGRATION_* pairs into SPLITs."""
    from datetime import timedelta
    QUANT = Decimal("0.000001")
    candidates_by_isin: dict[str, list[Transaction]] = {}
    for tx in txns:
        if tx.tx_type in (TxType.MIGRATION_IN, TxType.MIGRATION_OUT) and tx.isin:
            if (tx.notes or "").startswith("Security transfer"):
                candidates_by_isin.setdefault(tx.isin, []).append(tx)
    for isin, group in candidates_by_isin.items():
        outs = [t for t in group if t.tx_type is TxType.MIGRATION_OUT]
        ins = [t for t in group if t.tx_type is TxType.MIGRATION_IN]
        consumed_ins: set[int] = set()
        for out_tx in outs:
            match: Transaction | None = None
            for in_tx in ins:
                if id(in_tx) in consumed_ins:
                    continue
                if (in_tx.quantity - out_tx.quantity).copy_abs() > QUANT:
                    continue
                # Internal custodian rebooking always books the OUT leg first
                # (shares leave the old sub-custodian) and the IN leg on or
                # after that date. An IN dated BEFORE the OUT is a different
                # event (e.g. inbound external transfer followed by outbound
                # external transfer of a coincidentally equal quantity) and
                # must NOT be silently paired into a cost-basis-preserving
                # SPLIT — that would falsify basis for a genuine external
                # migration. Require in_date >= out_date, within a 7-day
                # window.
                delta_days = (in_tx.trade_date - out_tx.trade_date).days
                if delta_days < 0 or delta_days > 7:
                    continue
                match = in_tx
                break
            if match is None:
                continue
            # Paired internal rebookings are no longer basis-missing transfers.
            # Drop the original migration warning so issue exports do not
            # incorrectly ask for manual Anschaffungskosten.
            out_tx.flags = [f for f in out_tx.flags if f.code != "scalable.security_transfer"]
            match.flags = [f for f in match.flags if f.code != "scalable.security_transfer"]
            # Convert both legs to SPLIT with signed delta, preserving total cost.
            out_tx.tx_type = TxType.SPLIT
            out_tx.quantity = -out_tx.quantity
            out_tx.notes = (
                "Security transfer (paired outbound) — treated as pool-neutral "
                "internal custodian rebooking."
            )
            out_tx.add_flag(
                "scalable.security_transfer_paired",
                Severity.INFO,
                "Paired with inbound Security transfer; treated as pool-neutral "
                "(internal custodian rebooking, cost basis preserved).",
            )
            match.tx_type = TxType.SPLIT
            match.notes = (
                "Security transfer (paired inbound) — treated as pool-neutral "
                "internal custodian rebooking."
            )
            match.add_flag(
                "scalable.security_transfer_paired",
                Severity.INFO,
                "Paired with outbound Security transfer; treated as pool-neutral.",
            )
            consumed_ins.add(id(match))


def _parse_row(
    path: Path, lineno: int, row: dict[str, str], report: ParseReport
) -> Transaction | None:
    status = (row.get("status") or "").strip()
    raw_type = (row.get("type") or "").strip()
    asset_type_hint = (row.get("assetType") or "").strip()
    isin = (row.get("isin") or "").strip() or None
    if isin and not is_valid_isin(isin):
        # Fail loudly — ISIN field present but bad
        raise ParserError(
            f"Invalid ISIN: {isin!r}",
            broker=BROKER,
            source_file=str(path),
            source_line=lineno,
        )

    currency = normalize_currency(row.get("currency"))
    trade_date = parse_date(row["date"])
    trade_dt = None
    if (t := row.get("time")):
        try:
            trade_dt = datetime.fromisoformat(f"{row['date']}T{t.strip()}")
        except ValueError:
            trade_dt = None

    quantity = parse_decimal(row.get("shares"), decimal_sep=",")
    price = parse_decimal(row.get("price"), decimal_sep=",")
    amount = parse_decimal(row.get("amount"), decimal_sep=",")
    fee = parse_decimal(row.get("fee"), decimal_sep=",")
    tax = parse_decimal(row.get("tax"), decimal_sep=",")
    raw_ref = (row.get("reference") or "").strip().strip('"') or None
    name = (row.get("description") or "").strip().strip('"') or None

    # Rejected/Cancelled rows: keep for audit only.
    if status not in {"Executed", "Settled"}:
        report.rows_rejected += 1
        return Transaction(
            broker=BROKER,
            trade_date=trade_date,
            trade_datetime=trade_dt,
            tx_type=TxType.IGNORED,
            asset_class=asset_class_from_isin(isin, asset_type_hint),
            quantity=quantity,
            currency_native=currency,
            gross_native=amount,
            source_file=path.name,
            source_line=lineno,
            raw_ref=raw_ref,
            isin=isin,
            name=name,
            price_native=price if price != 0 else None,
            fee_native=fee,
            tax_withheld_native=tax,
            notes=f"Status={status}",
            flags=[Flag("scalable.status_not_executed", Severity.INFO, f"status={status}")],
        )

    # "Corporate action" rows are Scalable's catch-all for stock splits, scrip
    # issues, and ISIN-change spinoffs. The `shares` column carries the signed
    # quantity delta (positive for new shares received, negative for shares
    # cancelled on an ISIN change). Cost basis continues on the surviving
    # position (§ 27 Abs. 3 Z 2 EStG Tauschgrundsatz for splits; BMF practice
    # for ISIN changes without monetary consideration). We therefore emit
    # these as TxType.SPLIT so the pool preserves total_cost — the alternative
    # (MIGRATION_IN/OUT) forces the user to supply basis for every split and
    # blocks the next SELL spuriously.
    if raw_type == "Corporate action":
        return Transaction(
            broker=BROKER,
            trade_date=trade_date,
            trade_datetime=trade_dt,
            tx_type=TxType.SPLIT,
            asset_class=asset_class_from_isin(isin, asset_type_hint),
            quantity=quantity,  # signed delta; pool applies additively
            currency_native=currency,
            gross_native=Decimal("0"),
            source_file=path.name,
            source_line=lineno,
            raw_ref=raw_ref,
            isin=isin,
            name=name,
            price_native=price if price != 0 else None,
            fee_native=fee,
            tax_withheld_native=tax,
            notes="Corporate action — treated as SPLIT with preserved basis.",
            flags=[
                Flag(
                    "scalable.corporate_action",
                    Severity.WARNING,
                    "Cost basis preserved as SPLIT; verify against broker tax report.",
                )
            ],
        )

    # "Security transfer" rows are inbound/outbound position transfers from/to
    # another custodian. Cost basis from the originating broker is *not* in
    # this file — emit MIGRATION_IN/OUT with a WARN flag so any later sale
    # fails loudly until the user backfills the basis.
    if raw_type == "Security transfer":
        if quantity == 0:
            # A zero-share Security transfer row is ambiguous (no direction,
            # no effect) and would silently become a no-op MIGRATION_OUT in
            # the pool. Fail loudly so the user investigates the export.
            raise ParserError(
                "Scalable 'Security transfer' row with zero shares is "
                "ambiguous (no direction can be inferred).",
                broker=BROKER,
                source_file=str(path),
                source_line=lineno,
            )
        st_type = TxType.MIGRATION_IN if quantity > 0 else TxType.MIGRATION_OUT
        return Transaction(
            broker=BROKER,
            trade_date=trade_date,
            trade_datetime=trade_dt,
            tx_type=st_type,
            asset_class=asset_class_from_isin(isin, asset_type_hint),
            quantity=abs(quantity),
            currency_native=currency,
            gross_native=Decimal("0"),
            source_file=path.name,
            source_line=lineno,
            raw_ref=raw_ref,
            isin=isin,
            name=name,
            price_native=price if price != 0 else None,
            fee_native=fee,
            tax_withheld_native=tax,
            notes="Security transfer — cost basis from originating custodian required.",
            flags=[
                Flag(
                    "scalable.security_transfer",
                    Severity.WARNING,
                    "Provide original Anschaffungskosten before selling these shares.",
                )
            ],
        )

    if raw_type not in _TYPE_MAP:
        raise ParserError(
            f"Unknown Scalable type {raw_type!r}",
            broker=BROKER,
            source_file=str(path),
            source_line=lineno,
        )
    tx_type = _TYPE_MAP[raw_type]
    asset_class = asset_class_from_isin(isin, asset_type_hint)
    if tx_type in {
        TxType.INTEREST,
        TxType.INTEREST_OTHER,
        TxType.DEPOSIT_CASH,
        TxType.WITHDRAWAL_CASH,
        TxType.FEE,
    }:
        asset_class = AssetClass.CASH

    flags: list[Flag] = []
    # Default policy: treat Distribution as cash dividend. Rare confirmed RoC
    # cases can be pinned explicitly in rules/scalable_distribution_overrides.yaml.
    if raw_type == "Distribution":
        ov = _distribution_override_for(
            isin=isin,
            trade_date=trade_date,
            raw_ref=raw_ref,
            name=name,
        )
        if ov is not None:
            tx_type = TxType.RETURN_OF_CAPITAL
            note = ov["note"] or "manual override"
            flags.append(
                Flag(
                    "scalable.distribution_override_roc",
                    Severity.WARNING,
                    f"Distribution forced to RETURN_OF_CAPITAL via override: {note}",
                )
            )

    # Source-country hint for foreign withholding-tax credit cap.
    wh_country = country_from_isin(isin) if tx_type is TxType.DIVIDEND_CASH else None

    # Scalable's `amount` column is the net cash settlement — for Distribution
    # rows it is already NET of foreign withholding (the `tax` column holds
    # the withholding component separately). Declare the convention so the
    # engine grosses-up correctly.
    is_net = True if tx_type is TxType.DIVIDEND_CASH else None

    # Sign normalization: Scalable already signs `amount` correctly
    # (negative for buys, positive for sells/dividends).
    return Transaction(
        broker=BROKER,
        trade_date=trade_date,
        trade_datetime=trade_dt,
        tx_type=tx_type,
        asset_class=asset_class,
        quantity=quantity if tx_type in {TxType.BUY, TxType.SELL} else Decimal("0"),
        currency_native=currency,
        gross_native=amount,
        source_file=path.name,
        source_line=lineno,
        raw_ref=raw_ref,
        isin=isin,
        name=name,
        price_native=price if price != 0 else None,
        fee_native=fee,
        tax_withheld_native=tax,
        withholding_country=wh_country,
        dividend_is_net=is_net,
        flags=flags,
    )
