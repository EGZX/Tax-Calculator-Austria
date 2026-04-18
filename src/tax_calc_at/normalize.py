"""Normalization helpers: numbers, dates, ISINs, currencies."""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from dateutil import parser as _dateparser

ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}\d$")


def parse_decimal(value: str | int | float | Decimal | None, *, decimal_sep: str = ".") -> Decimal:
    """Parse a number into Decimal. Accepts comma or period decimal separator.

    Empty string / None → :class:`Decimal('0')`. Strings with a thousands
    separator opposite to ``decimal_sep`` are tolerated (stripped).
    """
    if value is None or value == "":
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int,)):
        return Decimal(value)
    if isinstance(value, float):
        # parsers should not be passing floats but be defensive
        return Decimal(str(value))
    s = str(value).strip()
    if s == "" or s == "-":
        return Decimal("0")
    # Strip surrounding quotes
    s = s.strip('"').strip("'").strip()
    # Choose decimal sep
    if decimal_sep == ",":
        # Treat '.' as thousands sep, ',' as decimal
        s = s.replace(".", "").replace(",", ".")
    else:
        # Drop only commas used as thousands sep (e.g. "1,234.56")
        if "," in s and "." in s and s.rfind(",") < s.rfind("."):
            s = s.replace(",", "")
    try:
        return Decimal(s)
    except InvalidOperation as e:
        raise ValueError(f"Cannot parse decimal: {value!r}") from e


def parse_date(value: str) -> date:
    """Parse an ISO date or common European date string into :class:`date`."""
    if not value:
        raise ValueError("empty date")
    s = value.strip().strip('"')
    if ISO_DATE_RE.match(s):
        return date.fromisoformat(s)
    # fallback: dateutil (handles 'YYYY-MM-DD HH:MM:SS', 'DD.MM.YYYY', ISO-8601 with TZ)
    dt = _dateparser.parse(s)
    return dt.date()


def parse_datetime(value: str) -> datetime:
    """Parse an ISO 8601 datetime (with or without timezone)."""
    s = value.strip().strip('"')
    return _dateparser.parse(s)


def is_valid_isin(isin: str) -> bool:
    """ISIN checksum (Luhn-style on digit-encoded chars)."""
    if not isin or not ISIN_RE.match(isin):
        return False
    # Encode letters A=10..Z=35
    digits: list[int] = []
    for ch in isin:
        if ch.isalpha():
            v = ord(ch) - ord("A") + 10
            digits.extend(divmod(v, 10))
        else:
            digits.append(int(ch))
    # Luhn: from rightmost, double every second digit
    total = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def country_from_isin(isin: str | None) -> str | None:
    """Return ISO-2 issuer country prefix from an ISIN, or None."""
    if not isin or len(isin) < 2:
        return None
    prefix = isin[:2].upper()
    if prefix.isalpha():
        return prefix
    return None


def normalize_currency(code: str | None) -> str:
    """Uppercase ISO-4217 currency code; empty/None → 'EUR'."""
    if not code:
        return "EUR"
    c = code.strip().upper().strip('"')
    if not re.match(r"^[A-Z]{3}$", c):
        raise ValueError(f"Invalid currency code: {code!r}")
    return c


# ISIN prefix heuristic for "likely an Irish/Luxembourg fund or ETF" — used
# to raise a warning when an ISIN is classified as STOCK but the prefix
# strongly suggests a UCITS fund, never to auto-classify.
_LIKELY_FUND_PREFIXES = ("IE00B", "IE00BD", "LU0", "LU1", "LU2")


def isin_looks_like_fund(isin: str | None) -> bool:
    """True for ISINs whose prefix strongly suggests a UCITS fund / ETF.

    Used to raise a soft warning — never to auto-classify."""
    if not isin:
        return False
    up = isin.upper()
    return any(up.startswith(p) for p in _LIKELY_FUND_PREFIXES)
