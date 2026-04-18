"""SQLite persistence layer.

Tables (all created on first connect):
    transactions        — canonical rows; primary key = dedup hash
    import_batches      — provenance for each parser run
    pool_snapshots      — optional: per-broker per-ISIN pool state at end of run
    fx_rates            — ECB rate cache (ccy, date) -> rate (EUR per 1 ccy unit)
    errors              — engine-emitted errors per batch (kept for audit UI)

Re-importing the same file is idempotent: matching dedup keys with the same
content_hash are silently kept; matching keys with a different content_hash
raise :class:`DuplicateMismatchError`.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from .model import (
    AssetClass,
    DuplicateMismatchError,
    Flag,
    FxSource,
    Severity,
    Transaction,
    TxType,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS import_batches (
    id            TEXT PRIMARY KEY,
    broker        TEXT NOT NULL,
    source_file   TEXT NOT NULL,
    file_sha256   TEXT NOT NULL,
    imported_at   TEXT NOT NULL,
    rows_total    INTEGER NOT NULL,
    rows_emitted  INTEGER NOT NULL,
    rows_ignored  INTEGER NOT NULL,
    rows_rejected INTEGER NOT NULL,
    flags_json    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
    dedup_key            TEXT PRIMARY KEY,
    content_hash         TEXT NOT NULL,
    broker               TEXT NOT NULL,
    trade_date           TEXT NOT NULL,
    settle_date          TEXT,
    trade_datetime       TEXT,
    tx_type              TEXT NOT NULL,
    asset_class          TEXT NOT NULL,
    isin                 TEXT,
    symbol               TEXT,
    name                 TEXT,
    account              TEXT,
    quantity             TEXT NOT NULL,
    price_native         TEXT,
    currency_native      TEXT NOT NULL,
    gross_native         TEXT NOT NULL,
    fee_native           TEXT NOT NULL,
    tax_withheld_native  TEXT NOT NULL,
    amount_eur           TEXT,
    fee_eur              TEXT,
    tax_withheld_eur     TEXT,
    fx_rate_used         TEXT,
    fx_rate_source       TEXT NOT NULL,
    withholding_country  TEXT,
    dividend_is_net      TEXT,
    raw_ref              TEXT,
    source_file          TEXT NOT NULL,
    source_line          INTEGER NOT NULL,
    notes                TEXT,
    flags_json           TEXT NOT NULL,
    import_batch_id      TEXT NOT NULL,
    FOREIGN KEY (import_batch_id) REFERENCES import_batches(id)
);

CREATE INDEX IF NOT EXISTS ix_tx_broker_date ON transactions(broker, trade_date);
CREATE INDEX IF NOT EXISTS ix_tx_isin        ON transactions(isin);

CREATE TABLE IF NOT EXISTS pool_snapshots (
    snapshot_at TEXT NOT NULL,
    broker      TEXT NOT NULL,
    isin        TEXT NOT NULL,
    quantity    TEXT NOT NULL,
    total_cost_eur TEXT NOT NULL,
    PRIMARY KEY (snapshot_at, broker, isin)
);

CREATE TABLE IF NOT EXISTS fx_rates (
    currency TEXT NOT NULL,
    rate_date TEXT NOT NULL,
    rate     TEXT NOT NULL,             -- EUR per 1 unit of `currency`
    source   TEXT NOT NULL,             -- 'ECB'
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (currency, rate_date)
);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.executescript(SCHEMA)
    # Additive migration: add columns introduced after the initial schema so
    # existing DBs keep working without manual intervention.
    cur = conn.execute("PRAGMA table_info(transactions)")
    cols = {r[1] for r in cur.fetchall()}
    if "dividend_is_net" not in cols:
        conn.execute("ALTER TABLE transactions ADD COLUMN dividend_is_net TEXT")
        conn.commit()
    return conn


# ---------------------------------------------------------------- transactions
def _dec_to_str(d: Decimal | None) -> str | None:
    return None if d is None else format(d, "f")


def _str_to_dec(s: str | None) -> Decimal | None:
    return None if s is None else Decimal(s)


def _flags_to_json(flags: list[Flag]) -> str:
    return json.dumps(
        [{"code": f.code, "severity": f.severity.value, "message": f.message} for f in flags]
    )


def _flags_from_json(s: str) -> list[Flag]:
    return [Flag(code=d["code"], severity=Severity(d["severity"]), message=d["message"]) for d in json.loads(s)]


def upsert_transactions(
    conn: sqlite3.Connection,
    txns: Iterable[Transaction],
    *,
    batch_id: str,
) -> tuple[int, int]:
    """Insert new txns; for existing dedup_keys, verify content_hash matches.

    Returns (inserted_count, already_present_count).
    Raises DuplicateMismatchError on any conflict.
    """
    inserted = 0
    existed = 0
    cur = conn.cursor()
    for tx in txns:
        tx.import_batch_id = batch_id
        key = tx.dedup_key()
        chash = tx.content_hash()
        row = cur.execute(
            "SELECT content_hash, source_file, source_line FROM transactions WHERE dedup_key=?",
            (key,),
        ).fetchone()
        if row is None:
            cur.execute(
                """INSERT INTO transactions (
                    dedup_key, content_hash, broker, trade_date, settle_date, trade_datetime,
                    tx_type, asset_class, isin, symbol, name, account,
                    quantity, price_native, currency_native, gross_native,
                    fee_native, tax_withheld_native,
                    amount_eur, fee_eur, tax_withheld_eur, fx_rate_used, fx_rate_source,
                    withholding_country, dividend_is_net, raw_ref, source_file, source_line,
                    notes, flags_json, import_batch_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    key,
                    chash,
                    tx.broker,
                    tx.trade_date.isoformat(),
                    tx.settle_date.isoformat() if tx.settle_date else None,
                    tx.trade_datetime.isoformat() if tx.trade_datetime else None,
                    tx.tx_type.value,
                    tx.asset_class.value,
                    tx.isin,
                    tx.symbol,
                    tx.name,
                    tx.account,
                    _dec_to_str(tx.quantity),
                    _dec_to_str(tx.price_native),
                    tx.currency_native,
                    _dec_to_str(tx.gross_native),
                    _dec_to_str(tx.fee_native),
                    _dec_to_str(tx.tax_withheld_native),
                    _dec_to_str(tx.amount_eur),
                    _dec_to_str(tx.fee_eur),
                    _dec_to_str(tx.tax_withheld_eur),
                    _dec_to_str(tx.fx_rate_used),
                    tx.fx_rate_source.value,
                    tx.withholding_country,
                    None if tx.dividend_is_net is None else ("1" if tx.dividend_is_net else "0"),
                    tx.raw_ref,
                    tx.source_file,
                    tx.source_line,
                    tx.notes,
                    _flags_to_json(tx.flags),
                    batch_id,
                ),
            )
            inserted += 1
        else:
            existing_hash, existing_file, existing_line = row
            if existing_hash != chash:
                raise DuplicateMismatchError(
                    f"Dedup key collision with different content for tx in "
                    f"{tx.source_file}:{tx.source_line} (existing: {existing_file}:{existing_line}). "
                    f"A previously imported row was modified. Either revert the source file, "
                    f"or delete the old import batch first."
                )
            existed += 1
    conn.commit()
    return inserted, existed


def record_batch(
    conn: sqlite3.Connection,
    *,
    batch_id: str,
    broker: str,
    source_file: str,
    file_sha256: str,
    imported_at: str,
    rows_total: int,
    rows_emitted: int,
    rows_ignored: int,
    rows_rejected: int,
    flags: list[Flag],
) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO import_batches
           (id, broker, source_file, file_sha256, imported_at,
            rows_total, rows_emitted, rows_ignored, rows_rejected, flags_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            batch_id,
            broker,
            source_file,
            file_sha256,
            imported_at,
            rows_total,
            rows_emitted,
            rows_ignored,
            rows_rejected,
            _flags_to_json(flags),
        ),
    )
    conn.commit()


def fetch_transactions(
    conn: sqlite3.Connection,
    *,
    year: int | None = None,
    broker: str | None = None,
) -> list[Transaction]:
    sql = "SELECT * FROM transactions WHERE 1=1"
    params: list[Any] = []
    if year is not None:
        sql += " AND substr(trade_date, 1, 4) = ?"
        params.append(str(year))
    if broker is not None:
        sql += " AND broker = ?"
        params.append(broker)
    sql += " ORDER BY trade_date, source_file, source_line"
    cur = conn.execute(sql, params)
    cols = [c[0] for c in cur.description]
    out: list[Transaction] = []
    for row in cur.fetchall():
        d = dict(zip(cols, row))
        tx = Transaction(
            broker=d["broker"],
            trade_date=date.fromisoformat(d["trade_date"]),
            tx_type=TxType(d["tx_type"]),
            asset_class=AssetClass(d["asset_class"]),
            quantity=Decimal(d["quantity"]),
            currency_native=d["currency_native"],
            gross_native=Decimal(d["gross_native"]),
            source_file=d["source_file"],
            source_line=d["source_line"],
            raw_ref=d["raw_ref"],
            isin=d["isin"],
            symbol=d["symbol"],
            name=d["name"],
            account=d["account"],
            price_native=_str_to_dec(d["price_native"]),
            fee_native=Decimal(d["fee_native"]),
            tax_withheld_native=Decimal(d["tax_withheld_native"]),
            settle_date=date.fromisoformat(d["settle_date"]) if d["settle_date"] else None,
            # Critical for pool replay: without trade_datetime, same-day BUY/SELL
            # tie-breaking collapses to source_line order (often reverse-chrono
            # in broker exports) and causes spurious OversellErrors.
            trade_datetime=(
                datetime.fromisoformat(d["trade_datetime"])
                if d.get("trade_datetime")
                else None
            ),
            amount_eur=_str_to_dec(d["amount_eur"]),
            fee_eur=_str_to_dec(d["fee_eur"]),
            tax_withheld_eur=_str_to_dec(d["tax_withheld_eur"]),
            fx_rate_used=_str_to_dec(d["fx_rate_used"]),
            fx_rate_source=FxSource(d["fx_rate_source"]),
            withholding_country=d["withholding_country"],
            dividend_is_net=(
                None
                if d.get("dividend_is_net") is None
                else (d["dividend_is_net"] == "1")
            ),
            notes=d["notes"],
            flags=_flags_from_json(d["flags_json"]),
            import_batch_id=d["import_batch_id"],
        )
        out.append(tx)
    return out


# ---------------------------------------------------------------- FX rate cache
def get_fx_rate(conn: sqlite3.Connection, currency: str, on: date) -> Decimal | None:
    row = conn.execute(
        "SELECT rate FROM fx_rates WHERE currency=? AND rate_date=?",
        (currency, on.isoformat()),
    ).fetchone()
    return Decimal(row[0]) if row else None


def put_fx_rates(
    conn: sqlite3.Connection,
    currency: str,
    rates: dict[date, Decimal],
    *,
    source: str = "ECB",
    fetched_at: str,
) -> None:
    conn.executemany(
        "INSERT OR REPLACE INTO fx_rates (currency, rate_date, rate, source, fetched_at) VALUES (?, ?, ?, ?, ?)",
        [(currency, d.isoformat(), format(r, "f"), source, fetched_at) for d, r in rates.items()],
    )
    conn.commit()
