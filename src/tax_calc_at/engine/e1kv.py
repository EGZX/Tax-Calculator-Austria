"""Aggregate transactions into E1kv Kennzahl totals for a tax year.

Inputs:
  * Year-filtered list of canonical transactions (already ECB-converted).
  * Realized-gain events from :class:`PoolManager`.
  * :class:`TaxRules` for the year.

Output:
  * :class:`E1kvReport` with per-Kennzahl totals, per-row contributions for
    drill-down, and a :class:`ReportHealth` record listing any blockers or
    warnings that affect whether the report is safe to file.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Iterable

from ..model import (
    AssetClass,
    FxSource,
    ReportNotFileableError,
    Transaction,
    TxType,
)
from ..normalize import isin_looks_like_fund
from ..parsers.base import asset_class_override_for
from ..pool import PoolManager, RealizedEvent
from .rules import Kennzahl, TaxRules

EUR_Q = Decimal("0.01")
MAX_CREDITABLE_WITHHOLDING_CAP = Decimal("0.15")


def _q(d: Decimal) -> Decimal:
    return d.quantize(EUR_Q, rounding=ROUND_HALF_EVEN)


def _realized_bucket_for_pnl(rules: TaxRules, base_bucket: str, pnl: Decimal) -> str:
    """Return effective realized bucket, optionally split by sign.

    When yearly rules define separate gain/loss buckets for realized
    Wertsteigerungen, route positive/zero PnL to the gain bucket and negative
    PnL to the loss bucket.
    """
    if base_bucket != "einkuenfte_realisierte_wertsteigerungen_27_5":
        return base_bucket
    gain_bucket = "einkuenfte_realisierte_wertsteigerungen_27_5_gewinne"
    loss_bucket = "einkuenfte_realisierte_wertsteigerungen_27_5_verluste"
    if gain_bucket in rules.kennzahlen and loss_bucket in rules.kennzahlen:
        return loss_bucket if pnl < 0 else gain_bucket
    return base_bucket


@dataclass
class Contribution:
    """One transaction's contribution to a Kennzahl bucket."""

    trade_date: str
    broker: str
    isin: str | None
    name: str | None
    amount_eur: Decimal
    note: str = ""


@dataclass
class BucketResult:
    bucket: str
    kennzahl: Kennzahl
    total_eur: Decimal = Decimal("0")
    contributions: list[Contribution] = field(default_factory=list)


@dataclass
class ReportHealth:
    """Gatekeeper for whether the report is safe to file.

    ``blockers`` must be empty for ``fileable`` to be True. Callers who
    produce final filing numbers (scripts, export jobs) should check
    ``fileable`` or call :meth:`E1kvReport.by_kennzahl` without
    ``allow_partial=True``, which raises if blockers exist.

    ``warnings`` are non-blocking but must be surfaced in the UI / any
    generated PDF so the user acknowledges them.
    """

    blockers: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    excluded_isins: list[tuple[str, str, str]] = field(default_factory=list)

    @property
    def fileable(self) -> bool:
        return not self.blockers


@dataclass
class E1kvReport:
    year: int
    buckets: dict[str, BucketResult] = field(default_factory=dict)
    # Withholding-tax credit (per bucket)
    creditable_withholding: dict[str, Decimal] = field(default_factory=dict)
    # Excess foreign withholding above the DBA cap (per bucket). Informational
    # only: cannot be credited against Austrian KESt, but the user may be
    # able to reclaim it from the source state (Form 85, CH / 5000, FR / …).
    uncreditable_withholding: dict[str, Decimal] = field(default_factory=dict)
    # Loss-offset summary (informational)
    loss_offset_note: str = ""
    # Gatekeeper: blockers / warnings that affect whether this report may be
    # filed. Populated by ``build_report``.
    health: ReportHealth = field(default_factory=ReportHealth)

    def by_kennzahl(self, *, allow_partial: bool = False) -> dict[int, Decimal]:
        """All non-zero E1kv Kennzahl totals.

        Includes creditable foreign-withholding buckets (e.g. KZ 998 / 799)
        alongside income buckets so non-UI callers cannot accidentally miss
        the credit side.

        Raises :class:`ReportNotFileableError` if the report carries
        blockers (``health.blockers`` non-empty) and ``allow_partial`` is
        False — so a bulk-filing script cannot silently emit an incomplete
        tax return.
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
            kz = self._credit_kennzahlen.get(cb_name)
            if kz is None:
                continue
            out[kz.nr] = out.get(kz.nr, Decimal("0")) + _q(amt)
        return out

    # Filled in by build_report so by_kennzahl can resolve credit-bucket → KZ.
    _credit_kennzahlen: dict[str, Kennzahl] = field(default_factory=dict)


def build_report(
    *,
    year: int,
    rules: TaxRules,
    transactions: Iterable[Transaction],
    realized: Iterable[RealizedEvent],
    pool_manager: PoolManager | None = None,
) -> E1kvReport:
    report = E1kvReport(year=year)
    txns = [t for t in transactions if t.trade_date.year == year]
    realized_list = [r for r in realized if r.trade_date.year == year]
    used_kennzahlen: set[str] = set()

    # ---- Invariant: every EUR amount we sum must come from ECB (or was
    # natively EUR). Any broker-rate leakage is a correctness bug.
    for t in txns:
        if t.amount_eur is None:
            continue
        if t.fx_rate_source not in {FxSource.ECB, FxSource.NATIVE_EUR}:
            report.health.blockers.append(
                f"Transaction {t.broker} {t.source_file}:{t.source_line} "
                f"uses non-ECB FX source {t.fx_rate_source.value!r}."
            )

    # ---- Realized gains/losses (SELL events + RoC excess) → bucket via classify
    for ev in realized_list:
        bucket_name = rules.classify(TxType.SELL, ev.asset_class)
        if bucket_name is None:
            continue
        bucket_name = _realized_bucket_for_pnl(rules, bucket_name, ev.pnl_eur)
        bucket = _get_or_create(report, rules, bucket_name)
        used_kennzahlen.add(bucket_name)
        bucket.total_eur += ev.pnl_eur
        bucket.contributions.append(
            Contribution(
                trade_date=ev.trade_date.isoformat(),
                broker=ev.broker,
                isin=ev.isin,
                name=ev.name,
                amount_eur=ev.pnl_eur,
                note=f"realized: proceeds={ev.proceeds_eur} cost={ev.cost_basis_eur}",
            )
        )

    # ---- Income transactions: dividends, interest
    for tx in txns:
        if tx.tx_type is TxType.SELL:
            continue  # handled above via realized events
        bucket_name = rules.classify(tx.tx_type, tx.asset_class)
        if bucket_name is None:
            continue
        bucket = _get_or_create(report, rules, bucket_name)
        used_kennzahlen.add(bucket_name)
        # gross income amount in EUR (gross before withholding)
        amt = tx.amount_eur or Decimal("0")
        # Dividend gross-up: broker often reports the NET amount + withholding
        # separately. The ``dividend_is_net`` flag disambiguates:
        if tx.tx_type is TxType.DIVIDEND_CASH:
            wh = tx.tax_withheld_eur or Decimal("0")
            if wh != 0:
                if tx.dividend_is_net is None:
                    report.health.blockers.append(
                        f"Dividend {tx.broker} {tx.source_file}:{tx.source_line} has "
                        f"withholding {wh} but dividend_is_net is unset — parser must "
                        f"declare NET or GROSS convention."
                    )
                elif tx.dividend_is_net:
                    amt = amt + wh
                # if dividend_is_net is False, gross_native already includes wh
        bucket.total_eur += amt
        bucket.contributions.append(
            Contribution(
                trade_date=tx.trade_date.isoformat(),
                broker=tx.broker,
                isin=tx.isin,
                name=tx.name,
                amount_eur=_q(amt),
                note=tx.tx_type.value,
            )
        )
        # Withholding-tax credit. Cap = DBA ceiling × grossed-up income (`amt`
        # has already been grossed-up above if the dividend was NET). Amount
        # above the cap is recorded as ``uncreditable_withholding`` so the
        # user knows to chase a refund from the source state.
        if tx.tax_withheld_eur and tx.tax_withheld_eur != 0:
            cap = _credit_cap(rules, tx)
            wh_abs = abs(tx.tax_withheld_eur)
            creditable = min(wh_abs, abs(amt) * cap)
            excess = wh_abs - creditable
            credit_bucket = _credit_bucket_for(rules, bucket_name)
            if credit_bucket and credit_bucket in rules.kennzahlen:
                report.creditable_withholding.setdefault(
                    credit_bucket, Decimal("0")
                )
                report.creditable_withholding[credit_bucket] += _q(creditable)
                report._credit_kennzahlen[credit_bucket] = rules.kennzahlen[credit_bucket]
                used_kennzahlen.add(credit_bucket)
                if excess > 0:
                    report.uncreditable_withholding.setdefault(
                        credit_bucket, Decimal("0")
                    )
                    report.uncreditable_withholding[credit_bucket] += _q(excess)

    # Loss-offset note
    if rules.loss_offset.cross_broker:
        report.loss_offset_note = (
            "Verlustausgleich angewandt: Realisierte Verluste werden mit "
            "realisierten Gewinnen UND Dividenden im 27,5%-Topf saldiert (innerhalb "
            "des Veranlagungsjahres, brokerübergreifend)."
        )

    # Apply intra-bucket loss offset already happens because we sum signed
    # gains/losses into the realized bucket. Cross-bucket offset (losses
    # within 27.5% across realized & dividends) → if the realized bucket is
    # negative, allow it to reduce the dividend bucket within the same basket.
    if rules.loss_offset.cross_bucket_within_275:
        _apply_cross_bucket_within_275(report, rules)

    # ---- ETF Meldefonds / ausschüttungsgleiche Erträge — out of scope for v1
    etf_rows = [t for t in txns if t.asset_class is AssetClass.ETF]
    if etf_rows:
        isins = sorted({t.isin or t.symbol or "?" for t in etf_rows})
        report.health.blockers.append(
            f"{len(etf_rows)} ETF row(s) present for ISINs {isins!r}. "
            "v1 does not compute ausschüttungsgleiche Erträge (OeKB Meldefonds) "
            "or the 90%-Pauschale for Nicht-Meldefonds. The report is incomplete "
            "for ETF positions — consult the OeKB fund-reporting database or a "
            "steuereinfach broker before filing."
        )

    # ---- Fund-prefix heuristic: catch ETFs misclassified as STOCK ----------
    # Brokers that don't supply an asset-class field (Trading 212) fall back to
    # STOCK for every ISIN. If the ISIN prefix strongly suggests a UCITS fund
    # (IE00B*, LU0*, …), the user must add it to asset_class_overrides.yaml so
    # the ETF blocker above can fire and the engine doesn't silently route
    # Meldefonds positions into the stock realised-gains bucket.
    stock_but_fund: list[str] = []
    for t in txns:
        # Explicit ISIN overrides in rules/asset_class_overrides.yaml are an
        # intentional user decision and must silence this heuristic.
        if asset_class_override_for(t.isin) is not None:
            continue
        if (
            t.asset_class is AssetClass.STOCK
            and isin_looks_like_fund(t.isin)
            and t.isin not in stock_but_fund
        ):
            stock_but_fund.append(t.isin)
    if stock_but_fund:
        report.health.blockers.append(
            f"ISIN(s) classified as STOCK but ISIN prefix suggests a UCITS "
            f"fund/ETF: {stock_but_fund!r}. Add them to "
            f"rules/asset_class_overrides.yaml with the correct AssetClass "
            f"(ETF, BOND, …) so the engine applies Meldefonds treatment. "
            f"If the ISIN is genuinely a stock (not a fund), add it as STOCK "
            f"to silence this check."
        )

    # ---- TBV markers: any used Kennzahl that is still to-be-verified blocks
    # final filing. (Unused `tbv: true` Kennzahlen do not block.)
    tbv_used = [
        rules.kennzahlen[bname] for bname in sorted(used_kennzahlen)
        if bname in rules.kennzahlen and rules.kennzahlen[bname].tbv
    ]
    if tbv_used:
        report.health.blockers.append(
            "Unverified Kennzahl numbers: "
            + ", ".join(f"KZ {k.nr} ({k.label})" for k in tbv_used)
            + ". Cross-check against the BMF E1kv-Erläuterungen for "
            f"tax year {year} and clear `tbv: true` in rules/tax_{year}.yaml "
            "before filing."
        )

    # ---- Tolerant-mode exclusions from pool replay
    if pool_manager is not None and pool_manager.errors:
        for broker, isin, msg in pool_manager.errors:
            report.health.excluded_isins.append((broker, isin, msg))
        report.health.blockers.append(
            f"{len(pool_manager.errors)} ISIN(s) excluded from realized-event "
            "aggregation due to oversell/migration errors. Realized-gain totals "
            "are INCOMPLETE. Resolve the errors (typically by importing earlier "
            "data or supplying MIGRATION_IN cost basis) and re-run."
        )

    # ---- Same-ISIN-across-brokers warning. The engine keeps cost basis
    # strictly per broker (EStR Rz 6144: "je Depot"). When the same ISIN
    # appears in more than one broker pool, the user should verify that the
    # per-depot methodology is the one they want — for non-steuereinfache
    # foreign depots the literature is not unanimous on cross-depot pooling.
    if pool_manager is not None:
        isin_to_brokers: dict[str, set[str]] = {}
        for broker, pools in pool_manager.by_broker.items():
            for isin, state in pools.by_isin.items():
                if state.quantity == 0 and state.total_cost_eur == 0:
                    continue
                isin_to_brokers.setdefault(isin, set()).add(broker)
        cross_broker_isins = sorted(
            (isin, sorted(brokers))
            for isin, brokers in isin_to_brokers.items()
            if len(brokers) > 1
        )
        for isin, brokers in cross_broker_isins:
            report.health.warnings.append(
                f"ISIN {isin} is held at multiple brokers ({', '.join(brokers)}). "
                "Cost basis is computed per broker ('je Depot' per EStR Rz 6144). "
                "Verify that this is the methodology you intend to use; "
                "cross-depot aggregation for non-steuereinfache foreign brokers "
                "is not uniformly settled in the literature."
            )

    return report


def _detect_asset_class_for_realized(ev: RealizedEvent):  # pragma: no cover
    # Deprecated: kept only as a backstop. Realized events now carry their
    # originating asset_class so the YAML classifier sees the right value.
    return ev.asset_class


def _get_or_create(report: E1kvReport, rules: TaxRules, bucket_name: str) -> BucketResult:
    if bucket_name not in report.buckets:
        report.buckets[bucket_name] = BucketResult(
            bucket=bucket_name, kennzahl=rules.kennzahl(bucket_name)
        )
    return report.buckets[bucket_name]


def _credit_cap(rules: TaxRules, tx: Transaction) -> Decimal:
    # Filing guardrail: creditable foreign withholding is globally capped at
    # 15% of gross income, independent of source country. Per-country DBA
    # caps (e.g. JP: 10%) are applied when the transaction carries a
    # ``withholding_country``; they can only LOWER the effective cap, never
    # raise it above the Austrian 15% ceiling. A user-configured default
    # above 15% is likewise clamped.
    default_cap = rules.foreign_withholding.default_creditable_cap
    country = (tx.withholding_country or "").upper()
    country_cap = (
        rules.foreign_withholding.country_caps.get(country) if country else None
    )
    effective = country_cap if country_cap is not None else default_cap
    return min(effective, MAX_CREDITABLE_WITHHOLDING_CAP)


def _credit_bucket_for(rules: TaxRules, bucket_name: str) -> str | None:
    """Map an income bucket to its corresponding withholding-credit bucket.

    First consults ``Kennzahl.credit_bucket`` declared in YAML. Falls back to
    a name-substring heuristic so legacy YAMLs without the explicit field keep
    working, but the YAML-declared mapping always wins when present.
    """
    kz = rules.kennzahlen.get(bucket_name)
    if kz is not None and kz.credit_bucket:
        return kz.credit_bucket
    if "27_5" in bucket_name:
        return "anrechenbare_quellensteuer_27_5"
    if "_25" in bucket_name:
        return "anrechenbare_quellensteuer_25"
    return None


def _apply_cross_bucket_within_275(report: E1kvReport, rules: TaxRules) -> None:
    buckets = rules.loss_offset.cross_bucket_within_275_buckets
    if len(buckets) < 2:
        return
    src_name, *targets = buckets
    src = report.buckets.get(src_name)
    if src is None or src.total_eur >= 0:
        return
    for target_name in targets:
        target = report.buckets.get(target_name)
        if target is None or target.total_eur <= 0:
            continue
        offset = min(abs(src.total_eur), target.total_eur)
        target.total_eur -= offset
        src.total_eur += offset
        target.contributions.append(
            Contribution(
                trade_date="-",
                broker="-",
                isin=None,
                name="Verlustausgleich aus realisierten Verlusten",
                amount_eur=-_q(offset),
                note=f"cross-bucket within 27.5% basket (from {src_name})",
            )
        )
        if src.total_eur >= 0:
            return
