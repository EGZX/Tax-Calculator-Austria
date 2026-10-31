"""Shared helpers for parsers."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from ..model import (
    FLAG_CA_UNCLASSIFIED,
    AssetClass,
    Flag,
    ParseReport,
    Severity,
    Transaction,
    TxType,
)


def make_tx(base: Mapping[str, Any], **fields: Any) -> Transaction:
    """Build a Transaction from per-row common fields plus row-specific ones
    (later keys win, so a field of ``base`` can be overridden)."""
    kwargs: dict[str, Any] = {**base, **fields}
    return Transaction(**kwargs)


def unclassified_share_increase(detail: str) -> Flag:
    """Blocker for shares added to a held position whose legal form the export
    does not state. A share split, or a capital increase from company funds of
    a company comparable to an Austrian corporation, is tax-neutral (KapMaßVO
    §§ 4 Abs 1, 6; EStR Rz 306, 308 — the taxpayer must prove comparability);
    a stock dividend or bonus shares out of profits are income at market value
    (§ 27 Abs 2 Z 1 EStG; EStR Rz 307; BFG RV/5100407/2024)."""
    return Flag(
        FLAG_CA_UNCLASSIFIED,
        Severity.ERROR,
        f"{detail} Declare it in rules/corporate_actions.yaml: treatment `split` "
        "(share split / capital increase from company funds) or "
        "`taxable_distribution` (stock dividend, bonus shares out of profits; "
        "`quantity` for the taxable part of a combined booking).",
    )


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


# ---- Per-ISIN asset-class override loader -------------------------------
# Broker exports without an asset-class field would fall back to STOCK for
# every ISIN. `rules/asset_class_overrides.yaml` (public) and
# `rules/local/asset_class_overrides.yaml` (personal, git-ignored; wins on
# conflict) pin the class per ISIN.
_OVERRIDES_CACHE: dict[str, AssetClass] | None = None
_OVERRIDES_KEY: tuple[tuple[str, float], ...] | None = None


def _load_overrides() -> dict[str, AssetClass]:
    """Load the override files, re-reading when any of them changes (the
    long-running Streamlit process picks up edits without a restart)."""
    from ..engine.rules import rule_files

    global _OVERRIDES_CACHE, _OVERRIDES_KEY
    paths = rule_files("asset_class_overrides.yaml")
    key = tuple((str(p), p.stat().st_mtime) for p in paths)
    if _OVERRIDES_CACHE is not None and key == _OVERRIDES_KEY:
        return _OVERRIDES_CACHE
    out: dict[str, AssetClass] = {}
    for path in paths:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for isin, name in (raw.get("overrides") or {}).items():
            try:
                out[str(isin).upper()] = AssetClass(str(name).upper())
            except ValueError as e:
                raise ValueError(
                    f"Invalid asset class {name!r} for ISIN {isin!r} in {path}: {e}"
                ) from e
    _OVERRIDES_CACHE = out
    _OVERRIDES_KEY = key
    return _OVERRIDES_CACHE


def reset_overrides_cache() -> None:  # pragma: no cover - for tests
    """Force the next call to re-read the overrides YAML (test helper)."""
    global _OVERRIDES_CACHE, _OVERRIDES_KEY
    _OVERRIDES_CACHE = None
    _OVERRIDES_KEY = None


def asset_class_override_for(isin: str | None) -> AssetClass | None:
    """Return the explicit override (if any) for an ISIN, else None.

    Exposed so the engine / service layer can retroactively apply overrides
    to already-persisted transactions — a user who edits the YAML after
    importing should not have to re-import to see the new classification.
    """
    if not isin:
        return None
    return _load_overrides().get(isin.upper())


def asset_class_from_isin(isin: str | None, hint: str | None = None) -> AssetClass:
    """Best-effort asset-class detection.

    Precedence: (1) explicit override in asset_class_overrides.yaml;
    (2) broker-supplied ``hint``; (3) fallback.

    Fallback with an ISIN is STOCK — same as before this change — but a
    matching entry in the override YAML always wins, so ETFs can be pinned
    explicitly instead of silently flowing into the stock bucket.
    """
    if isin:
        override = _load_overrides().get(isin.upper())
        if override is not None:
            return override
    if hint:
        h = hint.upper()
        if h in {"BOND", "BONDS"}:
            return AssetClass.BOND
        if h in {"ETF", "FUND", "FONDS"}:
            return AssetClass.ETF
        if h in {"DERIVATIVE", "DERIVATIVES", "WAR", "WARRANT", "CERTIFICATE", "ETC", "ETN"}:
            return AssetClass.DERIVATIVE
        if h in {"STOCK", "STK", "EQUITY", "SHARE", "SHARES"}:
            return AssetClass.STOCK
        if h in {"CRYPTO", "CRYPTOCURRENCY"}:
            return AssetClass.CRYPTO
        if h in {"CASH"}:
            return AssetClass.CASH
    # Fallback: assume STOCK if we have an ISIN (parser can override later).
    if isin:
        return AssetClass.STOCK
    return AssetClass.OTHER


def apply_steuereinfach_cutoff(
    txns: list[Transaction],
    report: ParseReport,
    cutoff: date | None,
    *,
    strict: bool = False,
) -> None:
    """Exclude rows on/after the date the account became steuereinfach.

    From that date the broker withholds Austrian KESt itself; the rows stay
    in the audit trail as IGNORED with a WARNING. ``strict`` raises instead.
    """
    from ..model import CutoffViolationError

    if cutoff is None:
        return
    hit = False
    for tx in txns:
        if tx.trade_date < cutoff or tx.tx_type is TxType.IGNORED:
            continue
        if strict:
            raise CutoffViolationError(
                f"{tx.broker} became steuereinfach on {cutoff}; row "
                f"{tx.source_file}:{tx.source_line} is dated {tx.trade_date}. "
                f"Export only rows before {cutoff}."
            )
        hit = True
        original = tx.tx_type
        tx.tx_type = TxType.IGNORED
        tx.notes = f"Post-cutoff {original.value} — taxed by the broker (steuereinfach)"
        tx.flags.append(
            Flag("broker.post_cutoff", Severity.INFO, "Excluded: broker is steuereinfach.")
        )
        report.rows_rejected += 1
        report.rows_emitted -= 1
        report.rows_ignored += 1
    if hit:
        report.add_flag(
            "broker.post_cutoff",
            Severity.WARNING,
            f"Rows on/after {cutoff} are handled by the broker's Austrian KESt "
            f"(steuereinfach) and excluded from tax math.",
        )


def count_rows(txns: list[Transaction], report: ParseReport) -> None:
    """Fill ``rows_emitted`` / ``rows_ignored`` from the parsed transactions."""
    report.rows_emitted = sum(1 for t in txns if t.tx_type is not TxType.IGNORED)
    report.rows_ignored = sum(1 for t in txns if t.tx_type is TxType.IGNORED)
