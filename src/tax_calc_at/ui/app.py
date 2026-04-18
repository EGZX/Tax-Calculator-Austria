"""Streamlit entry point: Austrian E1kv tax calculator."""

from __future__ import annotations

import tempfile
import shutil
import traceback
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

from tax_calc_at.engine.rules import load_brokers, load_tax_rules
from tax_calc_at.model import Severity, TaxCalcError
from tax_calc_at.service import (
    DEFAULT_DB,
    DEFAULT_RAW_DIR,
    build_year_report,
    build_year_worksheet,
    import_file,
)
from tax_calc_at.store import connect

st.set_page_config(page_title="AT Tax Calc — E1kv", layout="wide")
st.title("Austrian Capital-Income Tax Calculator (E1kv)")

st.sidebar.header("Setup")
db_path = Path(st.sidebar.text_input("SQLite DB path", value=str(DEFAULT_DB))).resolve()
raw_dir = Path(st.sidebar.text_input("Raw archive dir", value=str(DEFAULT_RAW_DIR))).resolve()
year = st.sidebar.number_input(
    "Tax year", min_value=2020, max_value=date.today().year, value=2024, step=1
)
st.sidebar.caption(
    "Per-year tax rules live in `rules/tax_YYYY.yaml`. Edit there to change Kennzahlen."
)

tab_import, tab_tx, tab_e1kv, tab_audit = st.tabs(
    ["1. Import", "2. Transactions", "3. E1kv Kennzahlen", "4. Audit"]
)

# ----------------------------------------------------------------- IMPORT TAB
with tab_import:
    st.subheader("Import broker exports")
    try:
        brokers = load_brokers()
    except Exception as e:
        st.error(f"Failed to load brokers.yaml: {e}")
        st.stop()

    cols = st.columns(2)
    broker_key = cols[0].selectbox(
        "Broker", options=list(brokers.brokers.keys()), format_func=lambda k: brokers.get(k).display_name
    )
    cutoff = brokers.get(broker_key).steuereinfach_from
    if cutoff:
        cols[1].info(f"Hard cutoff: trade_date >= {cutoff} will be REJECTED.")

    uploaded = st.file_uploader(
        "Choose a CSV export (multiple allowed)",
        type=["csv"],
        accept_multiple_files=True,
    )

    if uploaded and st.button("Import selected files", type="primary"):
        for upl in uploaded:
            # Stage the upload under a fresh temp dir so the original filename
            # is preserved for archival without colliding with re-imports.
            staging = Path(tempfile.mkdtemp(prefix="tax_calc_at_"))
            tmp_path = staging / upl.name
            tmp_path.write_bytes(upl.getbuffer())
            try:
                with st.spinner(f"Importing {upl.name} ..."):
                    res = import_file(
                        broker_key=broker_key,
                        source_path=tmp_path,
                        db_path=db_path,
                        raw_dir=raw_dir,
                        brokers=brokers,
                    )
                st.success(
                    f"{upl.name}: parsed {res.rows_total}, emitted {res.rows_emitted}, "
                    f"inserted {res.rows_inserted}, existed {res.rows_existed}, "
                    f"rejected {res.rows_rejected}, ignored {res.rows_ignored}"
                )
                if res.parse_report.flags:
                    with st.expander(f"Flags ({len(res.parse_report.flags)})"):
                        for f in res.parse_report.flags:
                            st.write(f"- **{f.severity.value}** [{f.code}] {f.message}")
            except TaxCalcError as e:
                st.error(f"Import failed for {upl.name}:\n\n```\n{e}\n```")
            except Exception:  # noqa: BLE001
                st.error(f"Unexpected error:\n\n```\n{traceback.format_exc()}\n```")
            finally:
                shutil.rmtree(staging, ignore_errors=True)

    st.divider()
    st.caption("Recent import batches")
    try:
        conn = connect(db_path)
        df = pd.read_sql_query(
            "SELECT imported_at, broker, source_file, rows_total, rows_emitted, "
            "rows_ignored, rows_rejected FROM import_batches ORDER BY imported_at DESC LIMIT 50",
            conn,
        )
        conn.close()
        st.dataframe(df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.warning(f"No batch history yet: {e}")


# ----------------------------------------------------------- TRANSACTIONS TAB
with tab_tx:
    st.subheader(f"Transactions for {year}")
    try:
        # UI runs in tolerant mode so per-ISIN pool errors don't blank the
        # whole audit view; the E1kv tab renders the resulting blockers.
        rep, year_txns, pm = build_year_report(int(year), db_path=db_path, tolerant=True)
    except TaxCalcError as e:
        st.error(f"Engine error: {e}")
        st.stop()
    except FileNotFoundError as e:
        st.warning(str(e))
        st.stop()
    except Exception:
        st.error(f"```\n{traceback.format_exc()}\n```")
        st.stop()

    # Filter UI
    fcols = st.columns(3)
    sel_brokers = fcols[0].multiselect(
        "Brokers", sorted({t.broker for t in year_txns}), default=None
    )
    sel_types = fcols[1].multiselect(
        "Types", sorted({t.tx_type.value for t in year_txns}), default=None
    )
    only_problems = fcols[2].checkbox("Only rows with errors/warnings", value=False)

    rows = []
    for t in year_txns:
        if sel_brokers and t.broker not in sel_brokers:
            continue
        if sel_types and t.tx_type.value not in sel_types:
            continue
        sev = "OK"
        if t.has_error:
            sev = "ERROR"
        elif t.has_warning:
            sev = "WARN"
        if only_problems and sev == "OK":
            continue
        rows.append(
            {
                "date": t.trade_date.isoformat(),
                "broker": t.broker,
                "type": t.tx_type.value,
                "asset": t.asset_class.value,
                "isin": t.isin or "",
                "name": t.name or "",
                "qty": str(t.quantity),
                "ccy": t.currency_native,
                "gross_native": str(t.gross_native),
                "amount_eur": "" if t.amount_eur is None else str(t.amount_eur),
                "tax_withheld_eur": "" if t.tax_withheld_eur is None else str(t.tax_withheld_eur),
                "fx_src": t.fx_rate_source.value,
                "severity": sev,
                "src": f"{t.source_file}:{t.source_line}",
            }
        )
    df = pd.DataFrame(rows)

    def _color(row: pd.Series) -> list[str]:
        if row["severity"] == "ERROR":
            return ["background-color: #ffd5d5"] * len(row)
        if row["severity"] == "WARN":
            return ["background-color: #fff4cc"] * len(row)
        return [""] * len(row)

    if df.empty:
        st.info("No transactions match the current filter.")
    else:
        st.dataframe(df.style.apply(_color, axis=1), use_container_width=True, hide_index=True)
        st.caption(f"{len(df)} rows")


# ---------------------------------------------------------------- E1KV TAB
with tab_e1kv:
    st.subheader(f"E1kv Kennzahlen — {year}")
    try:
        rules = load_tax_rules(int(year))
        rep, year_txns, pm = build_year_report(
            int(year), db_path=db_path, rules=rules, tolerant=True
        )
    except FileNotFoundError as e:
        st.warning(str(e))
        st.stop()
    except TaxCalcError as e:
        st.error(f"Engine error: {e}")
        st.stop()

    # --- Report health: blockers / warnings must be loud and unmissable ---
    if rep.health.blockers:
        st.error(
            "🚫 **Dieser Bericht ist NICHT zur Abgabe geeignet.** "
            f"{len(rep.health.blockers)} Blocker m\u00fcssen vor der Einreichung "
            "behoben werden:"
        )
        for b in rep.health.blockers:
            st.markdown(f"- {b}")
    else:
        st.success(
            "✅ Report passed the automated fileability checks. "
            "Das ersetzt keine manuelle Pr\u00fcfung durch Steuerberater:in."
        )
    if rep.health.warnings:
        with st.expander(f"⚠ {len(rep.health.warnings)} Warnung(en) (nicht-blockierend)"):
            for w in rep.health.warnings:
                st.markdown(f"- {w}")
    if rep.health.excluded_isins:
        with st.expander(
            f"Ausgeschlossene ISINs ({len(rep.health.excluded_isins)})"
        ):
            st.dataframe(
                pd.DataFrame(
                    rep.health.excluded_isins,
                    columns=["broker", "isin", "error"],
                ),
                use_container_width=True,
                hide_index=True,
            )

    # --- Fileability gate: hide numeric Kennzahl figures when blockers exist.
    # Without this gate a user could read (and transcribe into FinanzOnline)
    # numbers from a report that the engine itself flagged as not fileable —
    # e.g. with ETF rows missing Meldefonds data or ISINs excluded by pool
    # errors. Users can opt in to see partial figures after acknowledging.
    show_numbers = True
    if not rep.health.fileable:
        show_numbers = st.checkbox(
            "Unvollständige Zahlen trotzdem anzeigen "
            "(NUR zur internen Kontrolle — nicht zur Abgabe verwenden)",
            value=False,
            key="show_partial_numbers",
        )

    if not show_numbers:
        st.info(
            "Kennzahlen-Zahlen sind ausgeblendet, bis die obigen Blocker "
            "behoben sind. Partielle Zahlen können zur Abgabe irreführend "
            "sein (fehlende Meldefonds-Daten, ausgeschlossene ISINs, …)."
        )
    else:
        # Summary table
        summary_rows = []
        for bname, br in sorted(rep.buckets.items(), key=lambda x: x[1].kennzahl.nr):
            summary_rows.append(
                {
                    "Kennzahl": br.kennzahl.nr,
                    "Bezeichnung": br.kennzahl.label,
                    "Bucket": bname,
                    "Betrag (EUR)": f"{br.total_eur:.2f}",
                    "Posten": len(br.contributions),
                    "TBV": "yes" if br.kennzahl.tbv else "",
                }
            )
        for cb, amt in rep.creditable_withholding.items():
            kz = rules.kennzahlen[cb]
            summary_rows.append(
                {
                    "Kennzahl": kz.nr,
                    "Bezeichnung": kz.label,
                    "Bucket": cb,
                    "Betrag (EUR)": f"{amt:.2f}",
                    "Posten": "(Quellensteuer-Anrechnung)",
                    "TBV": "yes" if kz.tbv else "",
                }
            )
        if summary_rows:
            st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)
        else:
            st.info("No taxable items for this year (yet).")

        # Uncreditable foreign withholding — above the DBA cap, not creditable
        # against Austrian KESt, but potentially reclaimable from the source
        # state. Surface it so the user knows to chase a refund.
        if rep.uncreditable_withholding:
            st.warning(
                "Nicht anrechenbare Quellensteuer (Überschuss über DBA-Höchstsatz). "
                "Diese Beträge können NICHT in der E1kv geltend gemacht werden, "
                "sind aber ggf. im Quellenstaat rückforderbar "
                "(z.B. CH: Form 85, FR: Form 5000, …)."
            )
            excess_rows = [
                {
                    "Credit-Bucket": cb,
                    "Überschuss (EUR)": f"{amt:.2f}",
                }
                for cb, amt in rep.uncreditable_withholding.items()
            ]
            st.dataframe(
                pd.DataFrame(excess_rows), use_container_width=True, hide_index=True
            )

    if rep.loss_offset_note:
        st.caption(rep.loss_offset_note)

    # ---- Worksheet export (Berechnungsblatt für Steuerberater) ----------
    st.divider()
    st.subheader("Export: Berechnungsblatt")
    st.caption(
        "ZIP mit CSVs und HTML-Übersicht — jede Kennzahl ist bis zur "
        "Quell-Transaktion nachvollziehbar (Steuerberater-ready)."
    )
    export_cols = st.columns([1, 3])
    if export_cols[0].button("Berechnungsblatt erzeugen", key="build_wsheet"):
        try:
            bundle = build_year_worksheet(
                int(year), db_path=db_path, rules=rules, tolerant=True
            )
            st.session_state["wsheet_bundle"] = bundle
        except TaxCalcError as e:
            st.error(f"Export fehlgeschlagen: {e}")
        except Exception:
            st.error(f"```\n{traceback.format_exc()}\n```")
    bundle = st.session_state.get("wsheet_bundle")
    if bundle is not None:
        export_cols[1].download_button(
            label=f"⬇ Download {bundle.filename} ({len(bundle.content)//1024} KB)",
            data=bundle.content,
            file_name=bundle.filename,
            mime="application/zip",
            key="dl_wsheet",
        )

    # ---- Uncertain tax treatment ---------------------------------------
    # Surface rows that were included (or excluded) under ambiguous broker
    # data so the user can manually verify before filing.
    _UNCERTAIN_FLAG_CODES = {
        "t212.missing_withholding_detail",
        "t212.return_of_capital",
        "scalable.distribution_override_roc",
        "scalable.corporate_action",
        "scalable.security_transfer",
        "bonus_share_non_broker_source",
        "split_review",
        "t212.free_share_promo",
    }
    uncertain_rows = []
    for t in year_txns:
        for f in t.flags:
            if f.code in _UNCERTAIN_FLAG_CODES:
                uncertain_rows.append(
                    {
                        "date": t.trade_date.isoformat(),
                        "broker": t.broker,
                        "type": t.tx_type.value,
                        "isin": t.isin or "",
                        "name": t.name or "",
                        "amount_eur": "" if t.amount_eur is None else f"{t.amount_eur:.2f}",
                        "flag": f.code,
                        "message": f.message,
                    }
                )
                break  # one row per tx in the warning table
    if uncertain_rows:
        st.divider()
        st.subheader("⚠ Unsichere steuerliche Behandlung")
        st.caption(
            f"{len(uncertain_rows)} Zeile(n) wurden trotz Mehrdeutigkeit "
            "automatisch klassifiziert oder ausgeschlossen. Bitte vor Abgabe "
            "manuell prüfen (z.B. Trading-212-Steuerreport zu Quellensteuer / "
            "Einlagenrückzahlung)."
        )
        st.dataframe(
            pd.DataFrame(uncertain_rows), use_container_width=True, hide_index=True
        )

    if not show_numbers:
        # Per-bucket breakdown is also gated — same reasoning as the summary
        # table: expanding a KZ 865 group would reveal numeric per-row PnL.
        pass
    else:
        st.divider()
        st.subheader("Per-bucket breakdown")
        for bname, br in sorted(rep.buckets.items(), key=lambda x: x[1].kennzahl.nr):
            with st.expander(f"KZ {br.kennzahl.nr} — {br.kennzahl.label}  →  {br.total_eur:.2f} EUR"):
                df = pd.DataFrame(
                    [
                        {
                            "date": c.trade_date,
                            "broker": c.broker,
                            "isin": c.isin,
                            "name": c.name,
                            "amount_eur": f"{c.amount_eur:.2f}",
                            "note": c.note,
                        }
                        for c in br.contributions
                    ]
                )
                st.dataframe(df, use_container_width=True, hide_index=True)


# ----------------------------------------------------------------- AUDIT TAB
with tab_audit:
    st.subheader(f"Pool snapshots & event log ({year})")
    try:
        rep, year_txns, pm = build_year_report(
            int(year), db_path=db_path, tolerant=True
        )
    except (FileNotFoundError, TaxCalcError) as e:
        st.warning(str(e))
        st.stop()

    for broker, pools in pm.by_broker.items():
        with st.expander(f"Broker: {broker} — {len(pools.by_isin)} ISINs"):
            df = pd.DataFrame(
                [
                    {
                        "isin": isin,
                        "qty": str(s.quantity),
                        "total_cost_eur": str(s.total_cost_eur),
                        "avg_cost_eur": str(s.avg_cost_eur),
                        "cost_basis_known": s.cost_basis_known,
                    }
                    for isin, s in sorted(pools.by_isin.items())
                ]
            )
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.caption("Event log (last 200)")
            st.code("\n".join(pools.log.events[-200:]) or "(no events)")
