"""Shared helpers for parsers."""

from __future__ import annotations

import hashlib
from pathlib import Path

import yaml

from ..model import AssetClass


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


# ---- Per-ISIN asset-class override loader -------------------------------
# Broker exports with no asset-class field (notably Trading 212) would fall
# back to STOCK for every ISIN, letting ETFs silently bypass the engine's
# ETF blocker. `rules/asset_class_overrides.yaml` lets the user pin the
# correct class for specific ISINs.
_OVERRIDES_FILE = (
    Path(__file__).resolve().parents[3] / "rules" / "asset_class_overrides.yaml"
)
_OVERRIDES_CACHE: dict[str, AssetClass] | None = None
_OVERRIDES_MTIME: float | None = None


def _load_overrides() -> dict[str, AssetClass]:
    """Load the overrides YAML, auto-reloading when the file changes.

    Using the file's mtime as the cache key means the long-running Streamlit
    process picks up edits to asset_class_overrides.yaml without a restart —
    important because the user has to iterate on overrides when the engine
    reports 'UCITS misclassified as STOCK'.
    """
    global _OVERRIDES_CACHE, _OVERRIDES_MTIME
    if not _OVERRIDES_FILE.exists():
        _OVERRIDES_CACHE = {}
        _OVERRIDES_MTIME = None
        return _OVERRIDES_CACHE
    mtime = _OVERRIDES_FILE.stat().st_mtime
    if _OVERRIDES_CACHE is not None and _OVERRIDES_MTIME == mtime:
        return _OVERRIDES_CACHE
    raw = yaml.safe_load(_OVERRIDES_FILE.read_text(encoding="utf-8")) or {}
    overrides = raw.get("overrides") or {}
    out: dict[str, AssetClass] = {}
    for isin, name in overrides.items():
        try:
            out[str(isin).upper()] = AssetClass(str(name).upper())
        except ValueError as e:
            raise ValueError(
                f"Invalid asset class {name!r} for ISIN {isin!r} in "
                f"{_OVERRIDES_FILE}: {e}"
            ) from e
    _OVERRIDES_CACHE = out
    _OVERRIDES_MTIME = mtime
    return _OVERRIDES_CACHE


def reset_overrides_cache() -> None:  # pragma: no cover - for tests
    """Force the next call to re-read the overrides YAML (test helper)."""
    global _OVERRIDES_CACHE, _OVERRIDES_MTIME
    _OVERRIDES_CACHE = None
    _OVERRIDES_MTIME = None


def asset_class_override_for(isin: str | None) -> AssetClass | None:
    """Return the explicit override (if any) for an ISIN, else None.

    Exposed so the engine / service layer can retroactively apply overrides
    to already-persisted transactions — a user who edits the YAML after
    importing should not have to re-import to see the new classification.
    """
    if not isin:
        return None
    return _load_overrides().get(isin.upper())


# ISIN prefix heuristic for "likely an Irish/Luxembourg fund or ETF" — used
# only to raise a soft warning, never to override the caller's hint.
_LIKELY_FUND_PREFIXES = ("IE00B", "IE00BD", "LU0", "LU1", "LU2")


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


def isin_looks_like_fund(isin: str | None) -> bool:
    """True for ISINs whose prefix strongly suggests a UCITS fund / ETF.

    Used to raise a soft warning — never to auto-classify."""
    if not isin:
        return False
    up = isin.upper()
    return any(up.startswith(p) for p in _LIKELY_FUND_PREFIXES)
