"""High-level orchestration: import files, FX-convert, persist, build E1kv report.

This module is the single entry point used by the Streamlit UI and the test
suite. Everything here either succeeds or raises a typed :class:`TaxCalcError`.
"""

from __future__ import annotations

import shutil
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from .declarations import Declarations, apply_declarations, load_declarations
from .engine.e1kv import E1kvReport, build_report
from .engine.rules import BrokersConfig, TaxRules, load_brokers, load_tax_rules
from .export import WorksheetBundle, build_worksheet
from .fx.convert import convert_all
from .model import AssetClass, ParseReport, Severity, Transaction
from .parsers import parse_file
from .parsers.base import asset_class_override_for
from .pool import PoolManager
from .store import (
    connect,
    fetch_transactions,
    record_batch,
    upsert_transactions,
)

DEFAULT_DB = Path("data/tax.db")
DEFAULT_RAW_DIR = Path("data/raw")
_ETF_BLOCKER_MARKER = "ETF row(s) present for ISINs"


@dataclass
class ImportResult:
    batch_id: str
    broker: str
    source_file: str
    rows_total: int
    rows_emitted: int
    rows_inserted: int
    rows_existed: int
    rows_ignored: int
    rows_rejected: int
    archived_to: Path
    parse_report: ParseReport


def import_file(
    *,
    broker_key: str,
    source_path: Path,
    db_path: Path = DEFAULT_DB,
    raw_dir: Path = DEFAULT_RAW_DIR,
    brokers: BrokersConfig | None = None,
    conn: sqlite3.Connection | None = None,
) -> ImportResult:
    """Parse, FX-convert, persist, and archive one broker export file."""
    brokers = brokers or load_brokers()
    bcfg = brokers.get(broker_key)
    txns, report = parse_file(bcfg, source_path)
    for tx in txns:
        if tx.broker != broker_key:
            raise ValueError(
                f"Parser {bcfg.parser!r} emitted broker {tx.broker!r}, configured "
                f"key is {broker_key!r}; broker keys must match parser output."
            )

    own_conn = conn is None
    c = conn or connect(db_path)
    try:
        convert_all(c, txns)
        batch_id = uuid.uuid4().hex
        record_batch(
            c,
            batch_id=batch_id,
            broker=broker_key,
            source_file=str(source_path),
            file_sha256=report.file_sha256,
            imported_at=datetime.now(UTC).isoformat(),
            rows_total=report.rows_total,
            rows_emitted=report.rows_emitted,
            rows_ignored=report.rows_ignored,
            rows_rejected=report.rows_rejected,
            flags=report.flags,
        )
        inserted, existed = upsert_transactions(c, txns, batch_id=batch_id)
        c.commit()
    finally:
        if own_conn:
            c.close()

    archive_dir = raw_dir / broker_key
    archive_dir.mkdir(parents=True, exist_ok=True)
    if source_path.name.startswith(f"{report.file_sha256[:12]}__"):
        archived_to = archive_dir / source_path.name
    else:
        archived_to = archive_dir / f"{report.file_sha256[:12]}__{source_path.name}"
    if not archived_to.exists():
        shutil.copy2(source_path, archived_to)

    return ImportResult(
        batch_id=batch_id,
        broker=broker_key,
        source_file=str(source_path),
        rows_total=report.rows_total,
        rows_emitted=report.rows_emitted,
        rows_inserted=inserted,
        rows_existed=existed,
        rows_ignored=report.rows_ignored,
        rows_rejected=report.rows_rejected,
        archived_to=archived_to,
        parse_report=report,
    )


def reimport_archive(
    *,
    db_path: Path,
    raw_dir: Path = DEFAULT_RAW_DIR,
    brokers: BrokersConfig | None = None,
) -> list[ImportResult]:
    """Rebuild a fresh database from the archived raw exports.

    Use after a :class:`DatabaseVersionError` (parser semantics changed):
    move the old database away, then call this with the new path.
    """
    if db_path.exists():
        raise FileExistsError(f"{db_path} exists; re-import needs a fresh database path")
    brokers = brokers or load_brokers()
    results: list[ImportResult] = []
    for broker_dir in sorted(p for p in raw_dir.iterdir() if p.is_dir()):
        brokers.get(broker_dir.name)  # unknown directory → KeyError
        for f in sorted(p for p in broker_dir.iterdir() if p.is_file()):
            results.append(
                import_file(
                    broker_key=broker_dir.name,
                    source_path=f,
                    db_path=db_path,
                    raw_dir=raw_dir,
                    brokers=brokers,
                )
            )
    return results


def _load_prepared(
    db_path: Path, declarations: Declarations | None
) -> list[Transaction]:
    """All stored transactions with report-time overrides and declarations."""
    conn = connect(db_path)
    try:
        txns = fetch_transactions(conn)
    finally:
        conn.close()
    # rules/asset_class_overrides.yaml always wins and applies retroactively.
    for t in txns:
        override = asset_class_override_for(t.isin)
        if override is not None:
            t.asset_class = override
    _harmonize_asset_classes(txns)
    apply_declarations(txns, declarations if declarations is not None else load_declarations())
    return txns


def _harmonize_asset_classes(txns: list[Transaction]) -> None:
    """One asset class per ISIN.

    Brokers without an asset-class field are classified per row (e.g. from
    the description), so one ISIN can end up STOCK on one row (the fallback)
    and ETF on another. STOCK yields to the single specific class seen for
    the ISIN; two different specific classes are a conflict to resolve in
    rules/asset_class_overrides.yaml.
    """
    by_isin: dict[str, list[Transaction]] = {}
    for t in txns:
        if t.isin and t.asset_class is not AssetClass.CASH:
            by_isin.setdefault(t.isin, []).append(t)
    for isin, rows in by_isin.items():
        classes = {t.asset_class for t in rows}
        specific = classes - {AssetClass.STOCK}
        if len(classes) < 2:
            continue
        if len(specific) == 1:
            target = specific.pop()
            for t in rows:
                if t.asset_class is not target:
                    t.asset_class = target
                    t.add_flag("asset_class.harmonized", Severity.INFO,
                               f"Asset class set to {target.value}, as on other rows of {isin}.")
            continue
        for t in rows:
            t.add_flag("asset_class.conflict", Severity.WARNING,
                       f"{isin} is classified inconsistently ({sorted(c.value for c in classes)}); "
                       "pin it in rules/asset_class_overrides.yaml.")


def _replay(txns: list[Transaction], rules: TaxRules, tolerant: bool) -> PoolManager:
    pm = PoolManager(eur_stable_currencies=frozenset(rules.eur_stable_currencies))
    pm.replay(txns, on_error="collect" if tolerant else "raise")
    return pm


def build_year_report(
    year: int,
    *,
    db_path: Path = DEFAULT_DB,
    rules: TaxRules | None = None,
    brokers: BrokersConfig | None = None,
    declarations: Declarations | None = None,
    tolerant: bool = False,
) -> tuple[E1kvReport, list[Transaction], PoolManager]:
    """Replay all stored transactions, compute pools, build E1kv for ``year``.

    ``tolerant=False`` (filing-safe): the first pool error aborts the run.
    ``tolerant=True`` (audit UI): per-ISIN errors are collected, the ISIN is
    excluded and the report carries a blocker.
    """
    rules = rules or load_tax_rules(year)
    brokers = brokers or load_brokers()
    all_txns = _load_prepared(db_path, declarations)
    pm = _replay(all_txns, rules, tolerant)
    report = build_report(
        year=year,
        rules=rules,
        transactions=all_txns,
        realized=pm.realized_events(),
        brokers=brokers,
        pool_manager=pm,
    )
    _downgrade_etf_blocker_when_no_year_end_etf_holding(
        report=report, year=year, all_txns=all_txns, rules=rules, tolerant=tolerant
    )
    year_txns = [t for t in all_txns if t.trade_date.year == year]
    return report, year_txns, pm


def _downgrade_etf_blocker_when_no_year_end_etf_holding(
    *,
    report: E1kvReport,
    year: int,
    all_txns: list[Transaction],
    rules: TaxRules,
    tolerant: bool,
) -> None:
    etf_blockers = [b for b in report.health.blockers if _ETF_BLOCKER_MARKER in b]
    if not etf_blockers:
        return
    year_end = date(year, 12, 31)
    cutoff_txns = [t for t in all_txns if t.trade_date <= year_end]
    etf_isins = {t.isin for t in cutoff_txns if t.asset_class is AssetClass.ETF and t.isin}
    if not etf_isins:
        return
    if any(t.asset_class is AssetClass.ETF and t.tax_withheld_eur for t in cutoff_txns
           if t.trade_date.year == year):
        return  # fund-level foreign tax needs OeKB data regardless of holdings
    pm_year_end = _replay(cutoff_txns, rules, tolerant)
    for pools in pm_year_end.by_broker.values():
        for isin, state in pools.by_isin.items():
            if isin in etf_isins and state.quantity > 0:
                return
    report.health.blockers = [b for b in report.health.blockers if _ETF_BLOCKER_MARKER not in b]
    report.health.warnings.append(
        "ETF rows detected, but no ETF position is open at year-end, so no "
        "ausschüttungsgleiche Erträge arise at year-end. A fund's AgE reported "
        "during the year (Meldezeitpunkt while held) or Nicht-Meldefonds rules "
        "can still apply — verify with the OeKB fund data (my.oekb.at)."
    )


def build_year_worksheet(
    year: int,
    *,
    db_path: Path = DEFAULT_DB,
    rules: TaxRules | None = None,
    brokers: BrokersConfig | None = None,
    tolerant: bool = True,
) -> WorksheetBundle:
    """Build the Steuerberater-ready Berechnungsblatt ZIP for one year.

    The pool snapshot reflects the state at 31 December of ``year``.
    """
    rules = rules or load_tax_rules(year)
    declarations = load_declarations()
    report, year_txns, _pm = build_year_report(
        year,
        db_path=db_path,
        rules=rules,
        brokers=brokers,
        declarations=declarations,
        tolerant=tolerant,
    )
    all_txns = _load_prepared(db_path, declarations)
    year_end = date(year, 12, 31)
    pm_year_end = _replay([t for t in all_txns if t.trade_date <= year_end], rules, tolerant)
    return build_worksheet(
        year=year,
        rules=rules,
        report=report,
        year_txns=year_txns,
        pool_manager=pm_year_end,
    )
