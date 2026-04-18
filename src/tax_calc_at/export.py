"""Berechnungsblatt export: Steuerberater-ready audit artifact.

Builds a ZIP archive of CSVs plus an HTML overview summarising one year's
E1kv computation end-to-end. Every Kennzahl total is traceable back to
the contributing transaction rows and, for realized gains, to the pool
event log that produced them.

Kept dependency-free on purpose (``csv`` + ``zipfile`` from stdlib). No
new optional packages — Steuerberater tooling ingests CSV/HTML
universally and an .xlsx writer would add installation friction.

Archive layout::

    e1kv_{year}_{stamp}/
      README.txt                 # what each file is + legal disclaimer
      00_summary.csv             # Kennzahl totals + credit buckets
      01_transactions.csv        # all year transactions, EUR-converted
      02_realized_events.csv     # SELL-side pnl derivation
      03_pool_snapshots.csv      # end-of-year pool state per broker/ISIN
      04_pool_events.csv         # replay log for each broker
      05_kennzahl_contributions.csv  # per-bucket per-row breakdown
      06_health.csv              # blockers / warnings / excluded ISINs
      07_fx_trail.csv            # FX rate used per transaction
      index.html                 # human-readable Berechnungsblatt

The CSVs use ``;`` as delimiter and ``,`` as decimal separator so Excel
at the Austrian locale opens them without re-import gymnastics.
"""

from __future__ import annotations

import csv
import html
import io
import zipfile
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Iterable

from .engine.e1kv import E1kvReport
from .engine.rules import TaxRules
from .model import Transaction
from .pool import PoolManager

_CSV_DELIM = ";"
_DEC_SEP = ","


def _fmt_dec(d: Decimal | None, places: int = 2) -> str:
    if d is None:
        return ""
    q = Decimal(10) ** -places
    return format(d.quantize(q)).replace(".", _DEC_SEP)


def _fmt_raw(d: Decimal | None) -> str:
    if d is None:
        return ""
    return format(d).replace(".", _DEC_SEP)


def _w(buf: io.StringIO, header: list[str], rows: Iterable[list[str]]) -> None:
    writer = csv.writer(buf, delimiter=_CSV_DELIM, lineterminator="\n")
    writer.writerow(header)
    for r in rows:
        writer.writerow(r)


@dataclass
class WorksheetBundle:
    filename: str
    content: bytes


def build_worksheet(
    *,
    year: int,
    rules: TaxRules,
    report: E1kvReport,
    year_txns: list[Transaction],
    pool_manager: PoolManager,
) -> WorksheetBundle:
    """Assemble the Berechnungsblatt ZIP for one tax year."""
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = f"e1kv_{year}_{stamp}"

    files: list[tuple[str, str]] = [
        ("00_summary.csv", _summary_csv(year, rules, report)),
        ("01_transactions.csv", _transactions_csv(year_txns)),
        ("02_realized_events.csv", _realized_csv(pool_manager)),
        ("03_pool_snapshots.csv", _pool_snapshots_csv(pool_manager)),
        ("04_pool_events.csv", _pool_events_csv(pool_manager)),
        ("05_kennzahl_contributions.csv", _contributions_csv(report)),
        ("06_health.csv", _health_csv(report)),
        ("07_fx_trail.csv", _fx_trail_csv(year_txns)),
        ("index.html", _index_html(year, rules, report, year_txns, pool_manager)),
        ("README.txt", _readme_txt(year)),
    ]

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in files:
            zf.writestr(f"{root}/{name}", content.encode("utf-8-sig"))
    return WorksheetBundle(
        filename=f"{root}.zip",
        content=buf.getvalue(),
    )


# ------------------------------------------------------------------- sheets


def _summary_csv(year: int, rules: TaxRules, report: E1kvReport) -> str:
    buf = io.StringIO()
    rows: list[list[str]] = []
    for bname, br in sorted(report.buckets.items(), key=lambda x: x[1].kennzahl.nr):
        rows.append([
            str(br.kennzahl.nr),
            br.kennzahl.label,
            bname,
            _fmt_dec(br.total_eur),
            str(len(br.contributions)),
            "yes" if br.kennzahl.tbv else "",
        ])
    for cb, amt in report.creditable_withholding.items():
        kz = rules.kennzahlen[cb]
        rows.append([
            str(kz.nr),
            kz.label,
            cb,
            _fmt_dec(amt),
            "(Quellensteuer-Anrechnung)",
            "yes" if kz.tbv else "",
        ])
    for cb, amt in report.uncreditable_withholding.items():
        rows.append([
            "-",
            f"Nicht anrechenbarer Überschuss ({cb})",
            cb,
            _fmt_dec(amt),
            "(DBA-Überschreitung; ggf. im Quellenstaat rückforderbar)",
            "",
        ])
    _w(
        buf,
        ["KZ", "Bezeichnung", "Bucket", "Betrag_EUR", "Posten", "TBV"],
        rows,
    )
    buf.write(f"\n# Steuerjahr: {year}\n")
    buf.write(f"# Fileable: {'yes' if report.health.fileable else 'NO'}\n")
    if report.loss_offset_note:
        buf.write(f"# {report.loss_offset_note}\n")
    return buf.getvalue()


def _transactions_csv(txns: list[Transaction]) -> str:
    buf = io.StringIO()
    rows: list[list[str]] = []
    for t in sorted(txns, key=lambda x: (x.trade_date, x.source_file, x.source_line)):
        rows.append([
            t.trade_date.isoformat(),
            t.trade_datetime.isoformat() if t.trade_datetime else "",
            t.broker,
            t.tx_type.value,
            t.asset_class.value,
            t.isin or "",
            t.symbol or "",
            (t.name or "").replace(_CSV_DELIM, ","),
            _fmt_raw(t.quantity),
            t.currency_native,
            _fmt_raw(t.price_native),
            _fmt_raw(t.gross_native),
            _fmt_raw(t.fee_native),
            _fmt_raw(t.tax_withheld_native),
            _fmt_dec(t.amount_eur, 4),
            _fmt_dec(t.fee_eur, 4),
            _fmt_dec(t.tax_withheld_eur, 4),
            _fmt_raw(t.fx_rate_used),
            t.fx_rate_source.value,
            t.withholding_country or "",
            "" if t.dividend_is_net is None else ("NET" if t.dividend_is_net else "GROSS"),
            f"{t.source_file}:{t.source_line}",
            t.raw_ref or "",
            "|".join(f"{f.severity.value}:{f.code}" for f in t.flags),
        ])
    _w(
        buf,
        [
            "trade_date", "trade_datetime", "broker", "tx_type", "asset_class",
            "isin", "symbol", "name",
            "quantity", "currency", "price_native",
            "gross_native", "fee_native", "tax_withheld_native",
            "amount_eur", "fee_eur", "tax_withheld_eur",
            "fx_rate_used", "fx_rate_source",
            "wh_country", "dividend_convention",
            "source", "raw_ref", "flags",
        ],
        rows,
    )
    return buf.getvalue()


def _realized_csv(pm: PoolManager) -> str:
    buf = io.StringIO()
    rows: list[list[str]] = []
    for ev in sorted(
        pm.realized_events(),
        key=lambda e: (e.trade_date, e.broker, e.isin or ""),
    ):
        rows.append([
            ev.trade_date.isoformat(),
            ev.broker,
            ev.isin or "",
            (ev.name or "").replace(_CSV_DELIM, ","),
            ev.asset_class.value,
            _fmt_raw(ev.quantity),
            _fmt_dec(ev.proceeds_eur),
            _fmt_dec(ev.cost_basis_eur),
            _fmt_dec(ev.pnl_eur),
            ev.source_ref,
        ])
    _w(
        buf,
        [
            "trade_date", "broker", "isin", "name", "asset_class",
            "quantity", "proceeds_eur", "cost_basis_eur", "pnl_eur",
            "source_ref",
        ],
        rows,
    )
    return buf.getvalue()


def _pool_snapshots_csv(pm: PoolManager) -> str:
    buf = io.StringIO()
    rows: list[list[str]] = []
    for broker in sorted(pm.by_broker):
        pools = pm.by_broker[broker]
        for isin, state in sorted(pools.by_isin.items()):
            rows.append([
                broker,
                isin,
                _fmt_raw(state.quantity),
                _fmt_dec(state.total_cost_eur, 4),
                _fmt_dec(state.avg_cost_eur, 4),
                "yes" if state.cost_basis_known else "UNKNOWN",
            ])
    _w(
        buf,
        ["broker", "isin", "quantity", "total_cost_eur", "avg_cost_eur", "cost_basis_known"],
        rows,
    )
    return buf.getvalue()


def _pool_events_csv(pm: PoolManager) -> str:
    buf = io.StringIO()
    rows: list[list[str]] = []
    for broker in sorted(pm.by_broker):
        pools = pm.by_broker[broker]
        for idx, line in enumerate(pools.log.events):
            rows.append([broker, str(idx), line.replace(_CSV_DELIM, ",")])
    _w(buf, ["broker", "seq", "event"], rows)
    return buf.getvalue()


def _contributions_csv(report: E1kvReport) -> str:
    buf = io.StringIO()
    rows: list[list[str]] = []
    for bname, br in sorted(report.buckets.items(), key=lambda x: x[1].kennzahl.nr):
        for c in br.contributions:
            rows.append([
                str(br.kennzahl.nr),
                bname,
                c.trade_date,
                c.broker,
                c.isin or "",
                (c.name or "").replace(_CSV_DELIM, ","),
                _fmt_dec(c.amount_eur),
                c.note,
            ])
    _w(
        buf,
        ["KZ", "bucket", "trade_date", "broker", "isin", "name", "amount_eur", "note"],
        rows,
    )
    return buf.getvalue()


def _health_csv(report: E1kvReport) -> str:
    buf = io.StringIO()
    rows: list[list[str]] = []
    for b in report.health.blockers:
        rows.append(["BLOCKER", b.replace(_CSV_DELIM, ",")])
    for w in report.health.warnings:
        rows.append(["WARNING", w.replace(_CSV_DELIM, ",")])
    for broker, isin, msg in report.health.excluded_isins:
        rows.append(["EXCLUDED", f"{broker}/{isin}: {msg.splitlines()[0]}".replace(_CSV_DELIM, ",")])
    _w(buf, ["severity", "message"], rows)
    return buf.getvalue()


def _fx_trail_csv(txns: list[Transaction]) -> str:
    buf = io.StringIO()
    seen: set[tuple[str, str, str]] = set()
    rows: list[list[str]] = []
    for t in sorted(txns, key=lambda x: x.trade_date):
        if t.fx_rate_used is None:
            continue
        ref_date = (t.settle_date or t.trade_date).isoformat()
        key = (t.currency_native, ref_date, t.fx_rate_source.value)
        if key in seen:
            continue
        seen.add(key)
        rows.append([
            ref_date,
            t.currency_native,
            _fmt_raw(t.fx_rate_used),
            t.fx_rate_source.value,
        ])
    _w(buf, ["ref_date", "currency", "rate_eur_per_unit", "source"], rows)
    return buf.getvalue()


# ------------------------------------------------------------------- html


def _index_html(
    year: int,
    rules: TaxRules,
    report: E1kvReport,
    year_txns: list[Transaction],
    pm: PoolManager,
) -> str:
    esc = html.escape
    out: list[str] = []
    out.append(
        "<!doctype html><html lang='de'><head><meta charset='utf-8'>"
        f"<title>E1kv Berechnungsblatt {year}</title>"
        "<style>"
        "body{font-family:system-ui,sans-serif;max-width:960px;margin:2em auto;"
        "padding:0 1em;color:#222}"
        "table{border-collapse:collapse;width:100%;margin:.5em 0 1.5em}"
        "th,td{border:1px solid #ccc;padding:.3em .6em;text-align:left;font-size:.9em}"
        "th{background:#eee}"
        "td.num{text-align:right;font-variant-numeric:tabular-nums}"
        ".blocker{background:#ffd5d5;padding:.5em 1em;border-left:4px solid #c00;margin:.5em 0}"
        ".warning{background:#fff4cc;padding:.5em 1em;border-left:4px solid #c90;margin:.5em 0}"
        ".ok{background:#d7f4d7;padding:.5em 1em;border-left:4px solid #080;margin:.5em 0}"
        "h2{margin-top:2em}"
        "small{color:#666}"
        "</style></head><body>"
    )
    out.append(f"<h1>E1kv Berechnungsblatt {year}</h1>")
    out.append(
        f"<p><small>Generiert {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} · "
        f"Engine v0.1 · Nicht-steuereinfache Auslandsbroker · "
        "Tausende in der CSV werden mit <code>;</code> getrennt, Dezimalzeichen <code>,</code>.</small></p>"
    )
    if report.health.fileable:
        out.append("<div class='ok'>✅ Automatisierte Prüfungen bestanden. <strong>Ersetzt keine "
                   "Kontrolle durch Steuerberater:in.</strong></div>")
    else:
        out.append("<div class='blocker'>🚫 <strong>Bericht ist NICHT zur Abgabe freigegeben.</strong> "
                   "Blocker unten beheben und neu exportieren.</div>")
    for b in report.health.blockers:
        out.append(f"<div class='blocker'>{esc(b)}</div>")
    for w in report.health.warnings:
        out.append(f"<div class='warning'>{esc(w)}</div>")

    # Summary table
    out.append("<h2>1. E1kv Kennzahlen</h2><table><tr>"
               "<th>KZ</th><th>Bezeichnung</th><th>Bucket</th>"
               "<th style='text-align:right'>Betrag (EUR)</th><th>Posten</th><th>TBV</th></tr>")
    for bname, br in sorted(report.buckets.items(), key=lambda x: x[1].kennzahl.nr):
        out.append(
            f"<tr><td>{br.kennzahl.nr}</td><td>{esc(br.kennzahl.label)}</td>"
            f"<td><code>{esc(bname)}</code></td>"
            f"<td class='num'>{esc(_fmt_dec(br.total_eur))}</td>"
            f"<td class='num'>{len(br.contributions)}</td>"
            f"<td>{'yes' if br.kennzahl.tbv else ''}</td></tr>"
        )
    for cb, amt in report.creditable_withholding.items():
        kz = rules.kennzahlen[cb]
        out.append(
            f"<tr><td>{kz.nr}</td><td>{esc(kz.label)}</td>"
            f"<td><code>{esc(cb)}</code></td>"
            f"<td class='num'>{esc(_fmt_dec(amt))}</td>"
            f"<td><em>Quellensteuer-Anrechnung</em></td><td></td></tr>"
        )
    out.append("</table>")
    if report.loss_offset_note:
        out.append(f"<p><small>{esc(report.loss_offset_note)}</small></p>")

    # Pool snapshots
    out.append("<h2>2. Pool-Stände (Ende Jahr)</h2>")
    for broker in sorted(pm.by_broker):
        pools = pm.by_broker[broker]
        non_empty = [
            (isin, s) for isin, s in sorted(pools.by_isin.items())
            if s.quantity != 0 or s.total_cost_eur != 0
        ]
        if not non_empty:
            continue
        out.append(f"<h3>{esc(broker)}</h3><table><tr>"
                   "<th>ISIN</th><th>Qty</th><th>Total cost (EUR)</th>"
                   "<th>Avg cost (EUR)</th><th>Basis bekannt?</th></tr>")
        for isin, s in non_empty:
            out.append(
                f"<tr><td><code>{esc(isin)}</code></td>"
                f"<td class='num'>{esc(_fmt_raw(s.quantity))}</td>"
                f"<td class='num'>{esc(_fmt_dec(s.total_cost_eur, 4))}</td>"
                f"<td class='num'>{esc(_fmt_dec(s.avg_cost_eur, 4))}</td>"
                f"<td>{'yes' if s.cost_basis_known else '<strong>UNKNOWN</strong>'}</td></tr>"
            )
        out.append("</table>")

    # Realized events
    realized = sorted(
        pm.realized_events(),
        key=lambda e: (e.trade_date, e.broker, e.isin or ""),
    )
    realized = [e for e in realized if e.trade_date.year == year]
    if realized:
        out.append("<h2>3. Realisierte Veräußerungsgewinne</h2>"
                   "<table><tr><th>Datum</th><th>Broker</th><th>ISIN</th>"
                   "<th>Name</th><th>Qty</th>"
                   "<th style='text-align:right'>Erlös</th>"
                   "<th style='text-align:right'>Anschaffungskosten</th>"
                   "<th style='text-align:right'>G/V</th></tr>")
        for ev in realized:
            out.append(
                f"<tr><td>{ev.trade_date.isoformat()}</td><td>{esc(ev.broker)}</td>"
                f"<td><code>{esc(ev.isin or '')}</code></td>"
                f"<td>{esc(ev.name or '')}</td>"
                f"<td class='num'>{esc(_fmt_raw(ev.quantity))}</td>"
                f"<td class='num'>{esc(_fmt_dec(ev.proceeds_eur))}</td>"
                f"<td class='num'>{esc(_fmt_dec(ev.cost_basis_eur))}</td>"
                f"<td class='num'>{esc(_fmt_dec(ev.pnl_eur))}</td></tr>"
            )
        out.append("</table>")

    # FX trail
    fx_rows: dict[tuple[str, str], tuple[Decimal, str]] = {}
    for t in year_txns:
        if t.fx_rate_used is None:
            continue
        ref_date = (t.settle_date or t.trade_date).isoformat()
        fx_rows[(ref_date, t.currency_native)] = (t.fx_rate_used, t.fx_rate_source.value)
    if fx_rows:
        out.append("<h2>4. FX-Trail (ECB-Tageskurse, 1 Fremdwährung = x EUR)</h2>"
                   "<table><tr><th>Datum</th><th>CCY</th>"
                   "<th style='text-align:right'>Kurs</th><th>Quelle</th></tr>")
        for (ref_date, ccy), (rate, src) in sorted(fx_rows.items()):
            out.append(
                f"<tr><td>{esc(ref_date)}</td><td>{esc(ccy)}</td>"
                f"<td class='num'>{esc(_fmt_raw(rate))}</td>"
                f"<td>{esc(src)}</td></tr>"
            )
        out.append("</table>")

    out.append(
        "<h2>5. Haftungshinweis</h2>"
        "<p><small>Dieses Berechnungsblatt wurde automatisiert aus CSV-Exporten "
        "erzeugt. Es dient der Unterstützung der Steuererklärung, nicht als "
        "deren Ersatz. ETF / Meldefonds werden nicht berechnet. Die Beträge "
        "sollten vor Abgabe bei FinanzOnline durch eine:n Steuerberater:in "
        "oder anhand eines steuereinfachen Broker-Reports gegengeprüft "
        "werden. § 27 / § 27a EStG und einschlägige DBA sind maßgeblich."
        "</small></p>"
    )
    out.append("</body></html>")
    return "".join(out)


def _readme_txt(year: int) -> str:
    return (
        f"E1kv Berechnungsblatt {year}\n"
        "================================\n\n"
        "Inhalt:\n"
        "  00_summary.csv              Kennzahl-Totale, Quellensteuer-Anrechnung\n"
        "  01_transactions.csv         Alle Transaktionen des Jahres (EUR-konvertiert)\n"
        "  02_realized_events.csv      SELL-seitige G/V-Berechnung (Pool-Basis)\n"
        "  03_pool_snapshots.csv       Pool-Stände pro Broker/ISIN zum Jahresende\n"
        "  04_pool_events.csv          Event-Log des Pool-Replays (BUY/SELL/SPLIT/...)\n"
        "  05_kennzahl_contributions.csv  Jede Kennzahl-Zeile zurück zur Quell-Transaktion\n"
        "  06_health.csv               Blocker / Warnungen / ausgeschlossene ISINs\n"
        "  07_fx_trail.csv             Verwendete ECB-Tageskurse\n"
        "  index.html                  Menschenlesbares Berechnungsblatt\n\n"
        "CSV-Format:\n"
        "  Trennzeichen:       ; (Semikolon)\n"
        "  Dezimalzeichen:     , (Komma)\n"
        "  Encoding:           UTF-8 mit BOM (Excel-kompatibel)\n\n"
        "Methodik (Kurzfassung):\n"
        "  - Gleitender Durchschnittspreis je Broker je ISIN (§ 27a Abs. 4 EStG).\n"
        "  - Fees sind NICHT in den Anschaffungskosten enthalten (§ 20 Abs. 2 EStG,\n"
        "    Werbungskostenabzugsverbot beim Sondersteuersatz).\n"
        "  - FX: ECB-Referenzkurs am Handelstag (Einkünfte: Zufluss-/Settle-Datum\n"
        "    per § 19 EStG).\n"
        "  - Quellensteuer-Anrechnung: gekappt auf DBA-Höchstsatz; Überschuss in\n"
        "    06_health.csv ausgewiesen (ggf. im Quellenstaat rückforderbar).\n\n"
        "Haftung: Automatisierte Auswertung, kein Ersatz für steuerliche Beratung.\n"
        "ETF-/Meldefonds-Erträge werden NICHT berechnet.\n"
    )
