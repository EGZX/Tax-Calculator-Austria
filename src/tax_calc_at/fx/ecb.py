"""ECB foreign-exchange reference rate fetcher.

Pulls daily reference rates from the ECB Statistical Data Warehouse (SDMX 2.1)
and caches them in the SQLite store. The ECB publishes one rate per business
day; weekends and holidays inherit the previous business day's rate (we
back-fill on lookup).

ECB CSV endpoint:
    https://data-api.ecb.europa.eu/service/data/EXR/D.<CCY>.EUR.SP00.A?format=csvdata
The rate published is "1 EUR = X CCY", so to convert CCY -> EUR we divide by
the rate.
"""

from __future__ import annotations

import csv
import io
import sqlite3
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import requests

from ..store import get_fx_rate, put_fx_rates

ECB_URL = "https://data-api.ecb.europa.eu/service/data/EXR/D.{ccy}.EUR.SP00.A"
TIMEOUT = 30
USER_AGENT = "tax-calc-at/0.1 (+local)"


def _parse_ecb_csv(text: str) -> dict[date, Decimal]:
    reader = csv.DictReader(io.StringIO(text))
    out: dict[date, Decimal] = {}
    for row in reader:
        d_raw = row.get("TIME_PERIOD") or row.get("TIME PERIOD")
        v_raw = row.get("OBS_VALUE") or row.get("OBS VALUE")
        if not d_raw or not v_raw:
            continue
        try:
            d = date.fromisoformat(d_raw)
            v = Decimal(v_raw)
        except Exception:
            continue
        out[d] = v
    return out


def fetch_ecb_series(currency: str) -> dict[date, Decimal]:
    """Fetch the entire daily series for a currency from the ECB."""
    if currency.upper() == "EUR":
        raise ValueError("EUR/EUR is not a foreign-exchange series")
    url = ECB_URL.format(ccy=currency.upper())
    r = requests.get(
        url,
        headers={"Accept": "text/csv", "User-Agent": USER_AGENT},
        params={"format": "csvdata"},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return _parse_ecb_csv(r.text)


def ensure_currency_cached(
    conn: sqlite3.Connection,
    currency: str,
    *,
    target_date: date | None = None,
) -> None:
    """Ensure ECB rates for ``currency`` cover ``target_date``.

    Fetches the full series on first use. If a cache already exists but does
    not yet cover ``target_date`` (e.g. a user imported 2023 data a year ago
    and is now importing 2024 data), the series is re-fetched so the 7-day
    backoff in :func:`lookup_rate` has fresh data to work with. A small
    grace window of 7 days is tolerated to avoid re-fetching for each
    weekend/holiday past the latest publication.
    """
    if currency.upper() == "EUR":
        return
    row = conn.execute(
        "SELECT MAX(rate_date) FROM fx_rates WHERE currency=?",
        (currency.upper(),),
    ).fetchone()
    max_cached = (
        date.fromisoformat(row[0]) if row and row[0] else None
    )
    if max_cached is not None:
        if target_date is None:
            return
        # Allow a 7-day grace window: ECB does not publish on weekends /
        # holidays, so a target date a few days past the latest cached
        # rate is still serviceable by the lookup backoff.
        if target_date <= max_cached + timedelta(days=7):
            return
    rates = fetch_ecb_series(currency)
    if not rates:
        raise RuntimeError(f"ECB returned no data for {currency}")
    put_fx_rates(
        conn,
        currency.upper(),
        rates,
        source="ECB",
        fetched_at=datetime.now(timezone.utc).isoformat(),
    )


def lookup_rate(conn: sqlite3.Connection, currency: str, on: date) -> Decimal | None:
    """Return the rate for ``currency`` on ``on`` (EUR per 1 unit of currency).

    Backs off up to 7 days to handle weekends/holidays where the ECB publishes
    no rate. Returns ``None`` if no rate is found in that window — caller is
    responsible for raising :class:`FxRateMissingError`.
    """
    if currency.upper() == "EUR":
        return Decimal("1")
    ensure_currency_cached(conn, currency, target_date=on)
    # ECB publishes 1 EUR = X CCY. We want EUR per 1 CCY, so 1 / rate.
    for delta in range(0, 8):
        d = on - timedelta(days=delta)
        raw = get_fx_rate(conn, currency.upper(), d)
        if raw is not None and raw != 0:
            return (Decimal("1") / raw).quantize(Decimal("0.0000000001"))
    return None
