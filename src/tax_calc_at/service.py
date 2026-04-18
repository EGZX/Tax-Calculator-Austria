"""High-level orchestration: import files, FX-convert, persist, build E1kv report.

This module is the single entry point used by the Streamlit UI and the test
suite. Everything here either succeeds or raises a typed :class:`TaxCalcError`.
"""

from __future__ import annotations

import shutil
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

from .engine.e1kv import E1kvReport, build_report
from .engine.rules import BrokersConfig, TaxRules, load_brokers, load_tax_rules
from .export import WorksheetBundle, build_worksheet
from .fx.convert import convert_all
from .model import AssetClass, ParseReport, Transaction
from .parsers import get_parser
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
) -> ImportResult:
    """Parse, FX-convert, persist, and archive one broker export file."""
    brokers = brokers or load_brokers()
    bcfg = brokers.get(broker_key)
    parser = get_parser(bcfg.parser)

    # Parse
    if bcfg.parser == "trade_republic":
        from .parsers.trade_republic import parse as tr_parse

        txns, report = tr_parse(source_path, steuereinfach_from=bcfg.steuereinfach_from)
    else:
        txns, report = parser(source_path)

    # FX-convert
    conn = connect(db_path)
    try:
        convert_all(conn, txns)
        # Persist — register the batch first so transactions can FK-reference it.
        batch_id = uuid.uuid4().hex
        record_batch(
            conn,
            batch_id=batch_id,
            broker=broker_key,
            source_file=str(source_path),
            file_sha256=report.file_sha256,
            imported_at=datetime.now(timezone.utc).isoformat(),
            rows_total=report.rows_total,
            rows_emitted=report.rows_emitted,
            rows_ignored=report.rows_ignored,
            rows_rejected=report.rows_rejected,
            flags=report.flags,
        )
        inserted, existed = upsert_transactions(conn, txns, batch_id=batch_id)
        conn.commit()
    finally:
        conn.close()

    # Archive raw file
    archive_dir = raw_dir / broker_key
    archive_dir.mkdir(parents=True, exist_ok=True)
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


def build_year_report(
    year: int,
    *,
    db_path: Path = DEFAULT_DB,
    rules: TaxRules | None = None,
    tolerant: bool = False,
) -> tuple[E1kvReport, list[Transaction], PoolManager]:
    """Replay all stored transactions, compute pools, build E1kv for ``year``.

    ``tolerant=False`` (default, the *filing-safe* mode): the first
    :class:`OversellError` / :class:`CostBasisMissingError` aborts the run.

    ``tolerant=True`` (for the audit UI only): per-ISIN errors are collected
    on :class:`PoolManager.errors` and propagated into
    :attr:`E1kvReport.health.excluded_isins`. The resulting report is marked
    ``fileable=False`` and :meth:`E1kvReport.by_kennzahl` will refuse to
    emit numbers without ``allow_partial=True``.
    """
    rules = rules or load_tax_rules(year)
    conn = connect(db_path)
    try:
        all_txns = fetch_transactions(conn)
    finally:
        conn.close()

    # Retroactively apply asset_class_overrides.yaml so edits to the YAML
    # take effect without forcing the user to re-import every CSV. The
    # override always wins; without this, a user who pins an ISIN to ETF
    # after the import would still see the engine treat it as STOCK.
    for t in all_txns:
        override = asset_class_override_for(t.isin)
        if override is not None:
            t.asset_class = override

    pm = PoolManager()
    pm.replay(all_txns, on_error="collect" if tolerant else "raise")

    report = build_report(
        year=year,
        rules=rules,
        transactions=all_txns,
        realized=pm.realized_events(),
        pool_manager=pm,
    )

    # Conservative usability refinement: if ETF rows exist in the year but no
    # ETF position is open at year-end, downgrade the generic ETF blocker to a
    # warning. AGE exposure may still exist depending on each fund's
    # Meldeperiode / record date, so we keep a loud warning for manual review.
    _downgrade_etf_blocker_when_no_year_end_etf_holding(
        report=report,
        year=year,
        all_txns=all_txns,
        tolerant=tolerant,
    )

    year_txns = [t for t in all_txns if t.trade_date.year == year]
    return report, year_txns, pm


def _downgrade_etf_blocker_when_no_year_end_etf_holding(
    *,
    report: E1kvReport,
    year: int,
    all_txns: list[Transaction],
    tolerant: bool,
) -> None:
    etf_blockers = [b for b in report.health.blockers if _ETF_BLOCKER_MARKER in b]
    if not etf_blockers:
        return

    year_end = date(year, 12, 31)
    cutoff_txns = [t for t in all_txns if t.trade_date <= year_end]
    etf_isins = {
        t.isin
        for t in cutoff_txns
        if t.asset_class is AssetClass.ETF and t.isin
    }
    if not etf_isins:
        return

    pm_year_end = PoolManager()
    pm_year_end.replay(cutoff_txns, on_error="collect" if tolerant else "raise")

    open_etf_positions: list[tuple[str, str]] = []
    for broker, pools in pm_year_end.by_broker.items():
        for isin, state in pools.by_isin.items():
            if isin in etf_isins and state.quantity > 0:
                open_etf_positions.append((broker, isin))

    if open_etf_positions:
        return

    report.health.blockers = [
        b for b in report.health.blockers if _ETF_BLOCKER_MARKER not in b
    ]
    report.health.warnings.append(
        "ETF rows detected, but no ETF position is open at year-end. "
        "The generic ETF blocker was downgraded to warning. AGE exposure can "
        "still depend on each fund's Meldeperiode / record date, so verify "
        "with OeKB fund reporting before filing."
    )


def build_year_worksheet(
    year: int,
    *,
    db_path: Path = DEFAULT_DB,
    rules: TaxRules | None = None,
    tolerant: bool = True,
) -> WorksheetBundle:
    """Build the Steuerberater-ready Berechnungsblatt ZIP for one year.

    Runs :func:`build_year_report` in tolerant mode by default so a user can
    still export a partial worksheet for review even when blockers exist —
    the ZIP's ``06_health.csv`` and ``index.html`` clearly mark the report
    as not-fileable in that case.

    The pool snapshot included in the worksheet reflects the **year-end**
    state (Dec 31), not the current replay state. This matters when
    exporting a historical year: a Steuerberater wants to see the closing
    positions that carry into the following year, not today's positions.
    """
    rules = rules or load_tax_rules(year)
    report, year_txns, _pm_current = build_year_report(
        year, db_path=db_path, rules=rules, tolerant=tolerant
    )
    # Re-replay with a Dec-31 cutoff so pool snapshots reflect year-end, not
    # the current state (which would include txns from subsequent years).
    year_end = date(year, 12, 31)
    conn = connect(db_path)
    try:
        all_txns = fetch_transactions(conn)
    finally:
        conn.close()
    cutoff_txns = [t for t in all_txns if t.trade_date <= year_end]
    pm_year_end = PoolManager()
    pm_year_end.replay(cutoff_txns, on_error="collect" if tolerant else "raise")
    return build_worksheet(
        year=year,
        rules=rules,
        report=report,
        year_txns=year_txns,
        pool_manager=pm_year_end,
    )
