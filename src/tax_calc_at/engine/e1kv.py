"""Aggregate transactions into E1kv Kennzahl totals for a tax year.

Inputs: year-filtered canonical transactions (ECB-converted), realized events
from :class:`PoolManager`, the year's :class:`TaxRules` and the broker config.

Reporting follows the BMF form (Beilage E 1kv, Anm 19/22; EStR Rz 7752h):

* Gains and losses are reported separately and gross. The Finanzamt nets all
  section 1.3 Kennzahlen; the engine never moves amounts between Kennzahlen.
* Creditable foreign tax per income item = min(withheld, DBA residual rate ×
  gross, Austrian rate × gross). Tax above the DBA rate is refundable abroad
  only and is reported separately, never in KZ 998/901.
* Income consumed by the loss offset loses its credit. The taxpayer may pick
  the offset order (EStR Rz 7752h; Anm 19 2025); 27,5 % Überschüsse are used
  first (Anm 19), then income without creditable tax, then income with the
  lowest credit ratio, then 25 % Überschüsse — which preserves the most credit.
* The credit is capped per source country at the Austrian tax on that
  country's income left after the offset (Anrechnungshöchstbetrag).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date
from decimal import ROUND_HALF_EVEN, Decimal

from ..model import (
    FLAG_CA_UNCLASSIFIED,
    FLAG_PAYMENT_IN_LIEU,
    FLAG_WHT_COUNTRY_UNCERTAIN,
    FLAG_WHT_NOT_DISCLOSED,
    AssetClass,
    FxSource,
    ReportNotFileableError,
    Severity,
    Transaction,
    TxType,
)
from ..normalize import isin_looks_like_fund
from ..parsers.base import asset_class_override_for
from ..pool import PoolManager, RealizedEvent
from .rules import BrokersConfig, BucketKind, Kennzahl, TaxRules

EUR_Q = Decimal("0.01")
ZERO = Decimal("0")

# Flag codes that withhold the treaty credit pending manual review.
_NO_CREDIT_FLAGS = {
    FLAG_PAYMENT_IN_LIEU: "substitute payment (securities lending), not a "
    "dividend under the DBA — credit only after manual review (EAS 3008)",
}

_DIVIDEND_TYPES = {TxType.DIVIDEND_CASH, TxType.DIVIDEND_STOCK}
# Austrian KESt on dividends of Austrian companies is withheld by the issuer
# even for shares in a foreign depot (§ 95 Abs 2 Z 1 lit a EStG); such
# dividends are final-taxed (§ 97) and not part of the foreign columns.
_KEST_RATE = Decimal("0.275")
_KEST_TOLERANCE = Decimal("0.005")
_INTEREST_TYPES = {TxType.INTEREST, TxType.INTEREST_OTHER}


def _q(d: Decimal) -> Decimal:
    return d.quantize(EUR_Q, rounding=ROUND_HALF_EVEN)


@dataclass
class Contribution:
    """One transaction's contribution to a Kennzahl bucket."""

    trade_date: str
    broker: str
    isin: str | None
    name: str | None
    amount_eur: Decimal
    note: str = ""
    withheld_eur: Decimal = ZERO
    creditable_eur: Decimal = ZERO
    country: str | None = None


@dataclass
class BucketResult:
    bucket: str
    kennzahl: Kennzahl
    total_eur: Decimal = ZERO
    contributions: list[Contribution] = field(default_factory=list)


@dataclass
class ReportHealth:
    """Gatekeeper for whether the report is safe to file.

    ``blockers`` must be empty for ``fileable``. ``warnings`` are
    non-blocking but must be shown to the user.
    """

    blockers: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    excluded_isins: list[tuple[str, str, str, date]] = field(default_factory=list)

    @property
    def fileable(self) -> bool:
        return not self.blockers


@dataclass
class LossOffsetSummary:
    """Informational simulation of the Finanzamt's Verlustausgleich."""

    losses_eur: Decimal = ZERO  # total losses to offset (positive)
    absorbed_eur: Decimal = ZERO
    unabsorbed_eur: Decimal = ZERO  # lost: no carry-forward (§ 27 Abs 8)
    saldo_1_3_eur: Decimal = ZERO  # "Saldo aus Punkt 1.3"


@dataclass
class E1kvReport:
    year: int
    buckets: dict[str, BucketResult] = field(default_factory=dict)
    # Creditable foreign tax per credit bucket, after loss offset.
    creditable_withholding: dict[str, Decimal] = field(default_factory=dict)
    # Withheld above the DBA rate: NOT creditable, refundable abroad only.
    uncreditable_withholding: dict[str, Decimal] = field(default_factory=dict)
    # Credit forfeited because the income was consumed by the loss offset.
    credit_lost_to_loss_offset: dict[str, Decimal] = field(default_factory=dict)
    loss_offset: LossOffsetSummary = field(default_factory=LossOffsetSummary)
    loss_offset_note: str = ""
    health: ReportHealth = field(default_factory=ReportHealth)
    # Filled by build_report so by_kennzahl can resolve credit bucket → KZ.
    credit_kennzahlen: dict[str, Kennzahl] = field(default_factory=dict)

    def by_kennzahl(self, *, allow_partial: bool = False) -> dict[int, Decimal]:
        """All non-zero Kennzahl totals, including the credit Kennzahlen.

        Raises :class:`ReportNotFileableError` when blockers exist unless
        ``allow_partial`` is True.
        """
        if self.health.blockers and not allow_partial:
            raise ReportNotFileableError(
                "E1kv report is not fileable. Blockers:\n  - "
                + "\n  - ".join(self.health.blockers)
                + "\nPass allow_partial=True to acknowledge and see partial figures."
            )
        out: dict[int, Decimal] = {}
        for b in self.buckets.values():
            if b.total_eur != 0:
                out[b.kennzahl.nr] = _q(b.total_eur)
        for cb_name, amt in self.creditable_withholding.items():
            if amt == 0:
                continue
            kz = self.credit_kennzahlen[cb_name]
            out[kz.nr] = out.get(kz.nr, ZERO) + _q(amt)
        return out


@dataclass
class _IncomeItem:
    bucket: str
    kz: Kennzahl
    country: str | None
    isin: str | None
    amount: Decimal = ZERO  # gross income (EUR)
    credit: Decimal = ZERO  # creditable before loss offset


def build_report(
    *,
    year: int,
    rules: TaxRules,
    transactions: Iterable[Transaction],
    realized: Iterable[RealizedEvent],
    brokers: BrokersConfig,
    pool_manager: PoolManager | None = None,
) -> E1kvReport:
    report = E1kvReport(year=year)
    all_txns = list(transactions)
    txns = [t for t in all_txns if t.trade_date.year == year]
    realized_list = [r for r in realized if r.trade_date.year == year]
    used: set[str] = set()

    def credit_institution(broker: str) -> bool | None:
        cfg = brokers.brokers.get(broker)
        return None if cfg is None else cfg.credit_institution

    # ---- Every EUR amount must come from the ECB (or be native EUR).
    for t in txns:
        if t.amount_eur is not None and t.fx_rate_source not in {
            FxSource.ECB,
            FxSource.NATIVE_EUR,
        }:
            report.health.blockers.append(
                f"Transaction {t.broker} {t.source_file}:{t.source_line} "
                f"uses non-ECB FX source {t.fx_rate_source.value!r}."
            )

    # ---- Realized results → gain bucket, or its loss bucket if negative.
    for ev in realized_list:
        base = rules.classify(
            TxType.SELL, ev.asset_class, credit_institution=credit_institution(ev.broker)
        )
        if base is None:
            continue
        kz = rules.kennzahlen[base]
        name = kz.loss_bucket if ev.pnl_eur < 0 and kz.loss_bucket else base
        bucket = _get_or_create(report, rules, name)
        used.add(name)
        bucket.total_eur += ev.pnl_eur
        bucket.contributions.append(
            Contribution(
                trade_date=ev.trade_date.isoformat(),
                broker=ev.broker,
                isin=ev.isin or ev.symbol,
                name=ev.name,
                amount_eur=ev.pnl_eur,
                note=(
                    f"{ev.kind}: proceeds={ev.proceeds_eur} cost={ev.cost_basis_eur} "
                    f"[{ev.source_ref}]"
                ),
            )
        )

    # ---- Income rows.
    items: dict[tuple[str, str | None, str | None], _IncomeItem] = {}
    for tx in txns:
        if tx.tx_type is TxType.SELL:
            continue  # realized via the pool
        if tx.tx_type is TxType.WITHHOLDING_TAX:
            amt = abs(tx.tax_withheld_eur or tx.amount_eur or ZERO)
            report.health.warnings.append(
                f"Foreign tax {_q(amt)} EUR at {tx.broker} {tx.source_file}:"
                f"{tx.source_line} ({tx.name or '-'}) is not attached to any "
                f"income row and is NOT credited; reclaim it at source if "
                f"refundable."
            )
            continue
        bucket_name = rules.classify(
            tx.tx_type, tx.asset_class, credit_institution=credit_institution(tx.broker)
        )
        if bucket_name is None:
            continue
        if tx.tx_type in _DIVIDEND_TYPES and _is_domestic(tx) and _domestic_kest(tx, report):
            continue
        kz = rules.kennzahlen[bucket_name]
        bucket = _get_or_create(report, rules, bucket_name)
        used.add(bucket_name)
        gross, withheld = _gross_and_withholding(tx, report)
        creditable = _creditable(rules, tx, kz, gross, withheld, report)
        excess = withheld - creditable
        bucket.total_eur += gross
        bucket.contributions.append(
            Contribution(
                trade_date=tx.trade_date.isoformat(),
                broker=tx.broker,
                isin=tx.isin,
                name=tx.name,
                amount_eur=_q(gross),
                note=tx.tx_type.value,
                withheld_eur=_q(withheld),
                creditable_eur=_q(creditable),
                country=tx.withholding_country,
            )
        )
        if kz.credit_bucket and excess != 0:
            report.uncreditable_withholding[kz.credit_bucket] = (
                report.uncreditable_withholding.get(kz.credit_bucket, ZERO) + excess
            )
        key = (bucket_name, (tx.withholding_country or "").upper() or None, tx.isin)
        item = items.setdefault(
            key, _IncomeItem(bucket_name, kz, key[1], tx.isin)
        )
        item.amount += gross
        item.credit += creditable

    _apply_loss_offset_and_credit(report, rules, list(items.values()), used)

    # ---- Fund holdings: AgE / non-reporting-fund rules are not computed.
    etf_rows = [t for t in txns if t.asset_class is AssetClass.ETF]
    if etf_rows:
        isins = sorted({t.isin or t.symbol or "?" for t in etf_rows})
        report.health.blockers.append(
            f"{len(etf_rows)} ETF row(s) present for ISINs {isins!r}. "
            "ausschüttungsgleiche Erträge (KZ 937, OeKB Meldefonds), the "
            "AgE cost-basis step-up (§ 186 Abs 3 InvFG), creditable fund-level "
            "foreign tax and the Nicht-Meldefonds lump sum (90 %/10 %) are NOT "
            "computed. The report is incomplete for these funds."
        )
    stock_but_fund = sorted(
        {
            t.isin
            for t in txns
            if t.isin
            and t.asset_class is AssetClass.STOCK
            and asset_class_override_for(t.isin) is None
            and isin_looks_like_fund(t.isin)
        }
    )
    if stock_but_fund:
        report.health.blockers.append(
            f"ISIN(s) classified as STOCK but the ISIN prefix suggests a fund, "
            f"ETC/ETN or certificate: {stock_but_fund!r}. Pin the correct class "
            f"(ETF, DERIVATIVE for ETC/ETN/Zertifikate, BOND, STOCK) in "
            f"rules/asset_class_overrides.yaml."
        )

    # ---- Unverified Kennzahlen that are actually used block filing.
    tbv_used = [rules.kennzahlen[b] for b in sorted(used) if rules.kennzahlen[b].tbv]
    if tbv_used:
        report.health.blockers.append(
            "Unverified Kennzahl numbers: "
            + ", ".join(f"KZ {k.nr} ({k.label})" for k in tbv_used)
            + f". Verify against the official BMF E 1kv {year} form "
            f"({rules.form_source}) and clear `tbv: true` in rules/tax_{year}.yaml."
        )

    # ---- Row-level flags from parsers / declarations.
    _surface_row_flags(report, txns)
    # An unclassified share increase of an earlier year still shapes the cost
    # basis of every later sale.
    earlier = [
        t for t in all_txns
        if t.trade_date.year < year and any(f.code == FLAG_CA_UNCLASSIFIED for f in t.flags)
    ]
    if earlier:
        report.health.blockers.append(
            f"[{FLAG_CA_UNCLASSIFIED}] {len(earlier)} earlier corporate action(s) still "
            "undeclared (stock dividend or split?) — they determine the cost basis: "
            + ", ".join(f"{t.broker} {t.trade_date} {t.isin}" for t in earlier[:5])
        )

    # ---- Pool state checks.
    if pool_manager is not None:
        _pool_checks(report, pool_manager, year)

    report.loss_offset_note = _loss_offset_note(report)
    return report


def _is_domestic(tx: Transaction) -> bool:
    return (tx.withholding_country or "").upper() == "AT" or (tx.isin or "").startswith("AT")


def _domestic_kest(tx: Transaction, report: E1kvReport) -> bool:
    """Austrian dividend: True if final-taxed by Austrian KESt (skip it)."""
    gross, withheld = _gross_and_withholding(tx, report)
    where = f"{tx.broker} {tx.source_file}:{tx.source_line} ({tx.isin or tx.name or '-'})"
    if gross > 0 and abs(withheld / gross - _KEST_RATE) <= _KEST_TOLERANCE:
        report.health.warnings.append(
            f"Austrian dividend {where}: {_q(gross)} EUR with {_q(withheld)} EUR "
            f"Austrian KESt is final-taxed (§ 97 EStG) and not reported. Optional: "
            f"Regelbesteuerungs-/Verlustausgleichsoption via KZ 862/899 to offset "
            f"losses against it."
        )
        return True
    report.health.blockers.append(
        f"Austrian dividend {where}: {_q(gross)} EUR without a 27,5 % KESt deduction "
        f"(withheld {_q(withheld)} EUR) — domestic income without KESt must be "
        f"declared manually."
    )
    return True


def _gross_and_withholding(tx: Transaction, report: E1kvReport) -> tuple[Decimal, Decimal]:
    """Gross income (EUR) and foreign tax withheld (EUR, refunds negative)."""
    amt = tx.amount_eur or ZERO
    withheld = tx.tax_withheld_eur or ZERO
    if withheld == 0:
        return amt, ZERO
    if tx.dividend_is_net is None:
        report.health.blockers.append(
            f"{tx.tx_type.value} {tx.broker} {tx.source_file}:{tx.source_line} has "
            f"withholding {withheld} but no NET/GROSS convention (dividend_is_net) — "
            f"the parser must declare it."
        )
        return amt, withheld
    return (amt + withheld if tx.dividend_is_net else amt), withheld


def _creditable(
    rules: TaxRules,
    tx: Transaction,
    kz: Kennzahl,
    gross: Decimal,
    withheld: Decimal,
    report: E1kvReport,
) -> Decimal:
    """Creditable foreign tax of one row before loss offset (sign of gross)."""
    fw = rules.foreign_withholding
    country = (tx.withholding_country or "").upper()
    flags = {f.code for f in tx.flags}
    where = f"{tx.broker} {tx.source_file}:{tx.source_line} ({tx.isin or tx.name or '-'})"

    if FLAG_WHT_NOT_DISCLOSED in flags and tx.tx_type in _DIVIDEND_TYPES:
        cap = fw.dividend_caps.get(country)
        if cap is None or cap > 0:
            report.health.blockers.append(
                f"Dividend {where}: the export does not disclose the foreign tax "
                f"withheld (country {country or '?'}), so the gross income and the "
                f"KZ 998 credit cannot be computed. Use an export that includes "
                f"the withholding-tax columns."
            )
    if withheld == 0:
        return ZERO
    rate = kz.rate
    if kz.credit_bucket is None or rate is None:
        report.health.warnings.append(
            f"Foreign tax {_q(withheld)} EUR on {where} cannot be credited "
            f"(income type without credit Kennzahl)."
        )
        return ZERO
    cap = ZERO
    for code, why in _NO_CREDIT_FLAGS.items():
        if code in flags:
            report.health.warnings.append(
                f"Foreign tax {_q(withheld)} EUR on {where} not credited: {why}."
            )
            return ZERO
    if tx.asset_class is AssetClass.ETF:
        report.health.warnings.append(
            f"Foreign tax {_q(withheld)} EUR on fund distribution {where} not "
            f"credited: creditable fund-level tax comes from the OeKB report."
        )
        return ZERO
    if tx.tx_type in _INTEREST_TYPES:
        cap = fw.interest_caps.get(country, ZERO) if country else ZERO
        if cap == 0:
            report.health.warnings.append(
                f"Foreign tax {_q(withheld)} EUR on interest {where} is not "
                f"creditable (no DBA source-taxation right on interest configured); "
                f"reclaim it from the source state."
            )
            return ZERO
    elif tx.tx_type not in _DIVIDEND_TYPES:
        report.health.warnings.append(
            f"Foreign tax {_q(withheld)} EUR on {tx.tx_type.value} {where} is not credited."
        )
        return ZERO
    if FLAG_WHT_COUNTRY_UNCERTAIN in flags or not country:
        report.health.blockers.append(
            f"Foreign tax {_q(withheld)} EUR on {where}: the withholding source "
            f"country is unknown (e.g. ADR/GDR whose ISIN country differs from "
            f"the issuer's). Declare it in rules/withholding_countries.yaml."
        )
        return ZERO
    if country in fw.manual_review_countries:
        report.health.blockers.append(
            f"Foreign tax {_q(withheld)} EUR on {where}: treaty with {country} "
            f"is suspended — credit only via § 48 Abs 5 BAO; handle manually."
        )
        return ZERO
    if tx.tx_type in _DIVIDEND_TYPES:
        dcap = fw.dividend_caps.get(country)
        if dcap is None:
            report.health.blockers.append(
                f"Foreign tax {_q(withheld)} EUR on {where}: no DBA dividend rate "
                f"for {country} in rules/tax_{rules.year}.yaml. Add the treaty's "
                f"portfolio-dividend rate (or 0 if not creditable)."
            )
            return ZERO
        cap = dcap
    if cap == 0:
        return ZERO
    limit = abs(gross) * min(cap, rate)
    credit = min(abs(withheld), limit)
    return credit if gross >= 0 else -credit


def _apply_loss_offset_and_credit(
    report: E1kvReport,
    rules: TaxRules,
    items: list[_IncomeItem],
    used: set[str],
) -> None:
    summary = report.loss_offset
    section = {n for n, kz in rules.kennzahlen.items() if kz.loss_offset}
    summary.saldo_1_3_eur = _q(
        sum((b.total_eur for n, b in report.buckets.items() if n in section), ZERO)
    )
    losses = -sum(
        (
            b.total_eur
            for n, b in report.buckets.items()
            if b.kennzahl.kind is BucketKind.LOSS and n in section
        ),
        ZERO,
    )
    # Net-negative income (e.g. reversals) also has to be offset.
    losses += sum((-i.amount for i in items if i.amount < 0 and i.bucket in section), ZERO)
    summary.losses_eur = _q(losses)

    lost_credit: dict[int, Decimal] = defaultdict(lambda: ZERO)
    absorbed_income: dict[int, Decimal] = defaultdict(lambda: ZERO)
    # Anm 19: losses go against Überschüsse at the highest special rate
    # (27,5 %) first; lower-rate (25 %) Überschüsse come last.
    section_gains = [
        b for n, b in report.buckets.items()
        if n in section and b.kennzahl.kind is BucketKind.GAIN and b.kennzahl.rate is not None
    ]
    top_rate = max((b.kennzahl.rate or ZERO for b in section_gains), default=ZERO)
    gains_27 = [b.total_eur for b in section_gains if b.kennzahl.rate == top_rate]
    gains_25 = [
        b.total_eur
        for b in sorted(section_gains, key=lambda b: b.kennzahl.rate or ZERO, reverse=True)
        if b.kennzahl.rate != top_rate
    ]
    offsettable = [
        (idx, it) for idx, it in enumerate(items) if it.bucket in section and it.amount > 0
    ]
    no_credit = [(i, it) for i, it in offsettable if it.credit <= 0]
    with_credit = sorted(
        ((i, it) for i, it in offsettable if it.credit > 0),
        key=lambda p: p[1].credit / p[1].amount,
    )
    remaining = losses
    for g in gains_27:
        take = min(remaining, max(g, ZERO))
        remaining -= take
    for idx, it in no_credit + with_credit:
        take = min(remaining, it.amount)
        if take <= 0:
            break
        remaining -= take
        absorbed_income[idx] = take
        if it.credit > 0:
            lost_credit[idx] = it.credit * take / it.amount
    for g in gains_25:
        take = min(remaining, max(g, ZERO))
        remaining -= take
    summary.absorbed_eur = _q(losses - remaining)
    summary.unabsorbed_eur = _q(remaining)
    if remaining > 0:
        report.health.warnings.append(
            f"{_q(remaining)} EUR of losses exceed the offsettable income and are "
            f"lost (no loss carry-forward for private investors, § 27 Abs 8 EStG)."
        )

    # Per credit bucket and source country: credit after offset, capped at
    # the Austrian tax on that country's remaining income.
    per_country: dict[tuple[str, str | None], list[Decimal]] = defaultdict(
        lambda: [ZERO, ZERO, ZERO]
    )  # credit after offset, tax ceiling, credit lost
    for idx, it in enumerate(items):
        cb = it.kz.credit_bucket
        if cb is None or it.kz.rate is None:
            continue
        after_income = it.amount - absorbed_income[idx]
        acc = per_country[(cb, it.country)]
        acc[0] += it.credit - lost_credit[idx]
        acc[1] += max(after_income, ZERO) * it.kz.rate
        acc[2] += lost_credit[idx]
    for (cb, _country), (credit, ceiling, lost) in sorted(
        per_country.items(), key=lambda kv: (kv[0][0], kv[0][1] or "")
    ):
        capped = min(credit, ceiling) if credit > 0 else credit
        lost += credit - capped
        if capped == 0 and lost == 0:
            continue
        report.creditable_withholding[cb] = report.creditable_withholding.get(cb, ZERO) + capped
        report.credit_kennzahlen[cb] = rules.kennzahlen[cb]
        used.add(cb)
        if lost:
            report.credit_lost_to_loss_offset[cb] = (
                report.credit_lost_to_loss_offset.get(cb, ZERO) + lost
            )
    for cb, amt in report.creditable_withholding.items():
        report.creditable_withholding[cb] = _q(amt)
        if amt < 0:
            report.health.blockers.append(
                f"Net creditable foreign tax for {cb} is negative ({_q(amt)} EUR): "
                f"refunds of tax credited in an earlier year must be corrected there."
            )
    for d in (report.uncreditable_withholding, report.credit_lost_to_loss_offset):
        for cb in d:
            d[cb] = _q(d[cb])


def _loss_offset_note(report: E1kvReport) -> str:
    s = report.loss_offset
    lost = sum(report.credit_lost_to_loss_offset.values(), ZERO)
    return (
        "Gewinne und Verluste sind getrennt (nicht saldiert) eingetragen; der "
        "Verlustausgleich erfolgt durch das Finanzamt (E 1kv Anm 19). "
        f"Saldo aus Punkt 1.3: {s.saldo_1_3_eur} EUR. Verluste {s.losses_eur} EUR, "
        f"davon ausgleichsfähig {s.absorbed_eur} EUR. Durch den Verlustausgleich "
        f"nicht mehr anrechenbare Quellensteuer: {_q(lost)} EUR (EStR Rz 7752h; "
        "Verrechnung zuerst mit 27,5 %-Überschüssen und Einkünften ohne "
        "Quellensteuer)."
    )


def _surface_row_flags(report: E1kvReport, txns: list[Transaction]) -> None:
    grouped: dict[tuple[Severity, str], list[Transaction]] = defaultdict(list)
    for t in txns:
        for f in t.flags:
            if f.severity in (Severity.WARNING, Severity.ERROR):
                grouped[(f.severity, f.code)].append(t)
    for (sev, code), rows in sorted(grouped.items(), key=lambda kv: kv[0][1]):
        first = rows[0]
        msg = next(f.message for f in first.flags if f.code == code)
        refs = ", ".join(f"{t.source_file}:{t.source_line}" for t in rows[:3])
        more = f" (+{len(rows) - 3} more)" if len(rows) > 3 else ""
        text = f"[{code}] {len(rows)} row(s): {msg} — e.g. {refs}{more}"
        (report.health.blockers if sev is Severity.ERROR else report.health.warnings).append(text)


def _pool_checks(report: E1kvReport, pm: PoolManager, year: int) -> None:
    # Errors after the tax year do not affect it (realizations before an
    # error are computed from complete history); unmatched exchange legs
    # (date.max) affect every year.
    relevant = [e for e in pm.errors if e[3].year <= year or e[3] == date.max]
    later = len(pm.errors) - len(relevant)
    if later:
        report.health.warnings.append(
            f"{later} pool error(s) dated after {year} (e.g. undeclared transfers or "
            "corporate actions) — they do not affect this year but block later years."
        )
    if relevant:
        report.health.excluded_isins.extend(relevant)
        report.health.blockers.append(
            f"{len(relevant)} pool error(s) (oversell, unknown basis, undeclared "
            "corporate action or depot transfer). Affected ISINs are excluded from "
            "the error date on; realized totals are INCOMPLETE: "
            + "; ".join(f"{b}/{i}: {m.splitlines()[0][:160]}" for b, i, m, _ in relevant[:5])
        )
    for when, msg in pm.basis_mismatches:
        if when.year <= year:
            report.health.warnings.append(msg)
    isin_to_brokers: dict[str, set[str]] = defaultdict(set)
    for broker, pools in pm.by_broker.items():
        for isin, state in pools.by_isin.items():
            if state.quantity != 0:
                isin_to_brokers[isin].add(broker)
            if state.quantity != 0 and not state.cost_basis_known:
                report.health.warnings.append(
                    f"{broker}/{isin}: {state.quantity} units with UNKNOWN cost basis "
                    "(undeclared transfer-in); a sale will fail until declared."
                )
    for isin, brokers in sorted(isin_to_brokers.items()):
        if len(brokers) > 1:
            report.health.warnings.append(
                f"ISIN {isin} is held at several depots ({', '.join(sorted(brokers))}); "
                "cost basis is kept per depot (§ 27a Abs 4 Z 3 EStG)."
            )
    for broker, pools in pm.by_broker.items():
        for ccy, st in pools.fx.items():
            if st.liability > 0 and st.quantity == 0:
                report.health.warnings.append(
                    f"{broker}: {ccy} balance is negative ({-st.liability}) at the end "
                    "of the replay (margin loan). FX effects on debts are not taxable; "
                    "if the export does not start at account opening, an opening "
                    f"{ccy} balance may be missing and FX gains understated."
                )


def _get_or_create(report: E1kvReport, rules: TaxRules, bucket_name: str) -> BucketResult:
    if bucket_name not in report.buckets:
        report.buckets[bucket_name] = BucketResult(
            bucket=bucket_name, kennzahl=rules.kennzahl(bucket_name)
        )
    return report.buckets[bucket_name]
