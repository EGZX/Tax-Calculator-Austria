# Tax Calculator Austria (E1kv, foreign brokers)

Computes Austrian capital-income tax figures (E1kv Kennzahlen) from broker CSV
exports for non-steuereinfache brokers.

## Important disclaimer

- This software is provided "as is", without any warranty of correctness,
  completeness, fitness for filing, or legal/tax suitability.
- It is not legal or tax advice.
- You remain solely responsible for validating every output before filing.
- Use at your own risk.

See [LICENSE](LICENSE) for full warranty and liability terms.

## Scope (current implementation)

Included:
- Supported CSV parsers: IBKR Flex Query (trades, cash transactions, corporate
  actions, transfers), Scalable Capital, Trade Republic, Trading 212.
- Moving-average cost basis per broker (depot) per ISIN; splits, ISIN changes,
  declared spin-offs/taxable stock distributions, return of capital, declared
  depot transfers (notified = neutral, otherwise deemed sale).
- ECB-based FX; foreign-currency cash balances as their own asset with FX
  gains/losses (EStR Rz 6201–6201c).
- E1kv Kennzahlen verified against the BMF forms 2023–2025 (861, 863, 865,
  892, 896, 898, 937, 994, 995, 998, 901), gross reporting of gains and
  losses, foreign-tax credit capped by DBA rate and reduced by the loss
  offset (EStR Rz 7752h).
- YAML-driven rules (`rules/tax_YYYY.yaml`, `rules/brokers.yaml`) and user
  declarations for facts exports cannot contain (`rules/*.yaml`).

Explicitly not covered (the report blocks instead of guessing):
- ETF / fund ausschüttungsgleiche Erträge (OeKB Meldefonds) and Nicht-Meldefonds rules.
- Non-securitised derivatives (listed options, futures, CFDs — tariff-taxed).
- Crypto, accrued interest on IBKR bond purchases, cash+share mergers.
- PDF generation and filing automation.

## User declarations

Some tax facts are not in any broker export. Declare them in the
git-ignored `rules/local/` directory (same file names as the public templates
in `rules/`, whose entries they extend). Personal data never belongs in
tracked files:

| File | When |
| --- | --- |
| `depot_transfers.yaml` | every transfer out of / into a depot — was it notified to the Finanzamt within one month (§ 27 Abs 6 Z 2 EStG)? |
| `corporate_actions.yaml` | shares added to a holding without a split type (IBKR `SD`, Scalable bookings without outgoing leg): `split` or `taxable_distribution` (with `quantity` for the taxable part); spin-offs (basis split by market values); unpaired exchanges |
| `withholding_countries.yaml` | ADR/GDR and other ISINs whose taxing state differs from the ISIN country |
| `return_of_capital.yaml` | distributions proven to be Einlagenrückzahlung |
| `asset_class_overrides.yaml` | ETF / DERIVATIVE (ETC, ETN, certificates) / BOND / STOCK per ISIN |
| `brokers.yaml` | account-specific broker settings, e.g. your own `steuereinfach_from` date |

For notified transfers, `notified_cost_basis_eur` records what the
notification stated; the tool always carries its own (Austrian) basis and
warns where the two differ, so the receiving broker's records can be fixed.

Shares added to a position are not automatically tax-neutral. A share split
or a capital increase from company funds of a comparable company keeps the
cost basis (KapMaßVO §§ 4 Abs 1, 6; EStR Rz 306, 308 — you must be able to
prove comparability); bonus shares out of profits and stock dividends are
income at market value, which is also their cost (EStR Rz 307,
BFG RV/5100407/2024). One booking can contain both — BYD's June 2025 issue
was 12:10 out of the capital reserve (neutral) plus 8:10 out of profits
(income, HKD 130.10 per share). Exports do not distinguish these, so such
bookings block the report until declared. Broker tax reports prepared under
German law (Scalable/KPMG) are no reference for this.

Scalable's CSV is not always complete: some distributions show no withheld
tax although tax was withheld, and interest credited on 31 December can be
missing from exports taken before it was booked. Reconcile income and
withholding against Scalable's annual tax report.

## Yearly workflow

1. Export every depot that is **not** steuereinfach for the whole tax year
   (plus, once, the full history of every depot whose positions you still
   hold or transferred — cost basis comes from the first purchase).
2. Import all files (UI tab "Import", or `service.import_file`). Overlapping
   exports are fine: rows are de-duplicated by broker transaction id.
3. Declare in `rules/local/` what exports cannot show: depot transfers and
   whether they were notified, spin-offs, ADR source countries, asset
   classes the report asks you to pin.
4. Build the report (UI tab "E1kv" or `service.build_year_report`) and fix
   every blocker; read every warning. Export the Berechnungsblatt for your
   records or your Steuerberater.
5. Check that `rules/tax_<year>.yaml` has no `tbv: true` left. Kennzahlen are
   marked to-be-verified until the BMF publishes that year's E 1kv form;
   compare against it, then clear the markers.

### Interactive Brokers Flex Query setup

Reports → Flex Queries → Activity Flex Query:

- Sections: **Trades** (Level of detail: Execution), **Cash Transactions**
  (all types, Level of detail: Detail), **Corporate Actions**, **Transfers**;
  in each section "Select All" fields (the parser relies on IBKR's default
  column names; each section must start with `ClientAccountID`).
- Delivery: Format CSV, Include column headers **yes**, Include header and
  trailer records **no**, Include section code and line descriptor **no**.
- Dates: date format `yyyyMMdd`, time format `HHmmss`, date/time separator
  `;` (semicolon).
- Period: a custom range of 1 January – 31 December (IBKR allows at most 365
  days per run). Withholding-tax corrections for a year can be booked early
  in the following year — export January–March of the next year as well.

## ECB rates offline

If the ECB API is unreachable, seed the rate cache from the official history
file (https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip):

```python
from tax_calc_at.store import connect
from tax_calc_at.fx.ecb import load_ecb_history_csv
load_ecb_history_csv(connect(Path("data/tax.db")), "eurofxref-hist.csv")
```

## Upgrading an existing database

Parser semantics are versioned (`store.SCHEMA_VERSION`). An older database
raises `DatabaseVersionError`; move it away and rebuild from the archive:

```python
from tax_calc_at.service import reimport_archive
reimport_archive(db_path=Path("data/tax.db"))
```

## Public repository data policy

- Do not commit personal exports, account statements, database snapshots, or
  other personal financial data.
- Keep user data local only (`exports/`, `data/raw/`, `data/*.db` are ignored).
- If sensitive data was committed in the past, rewrite history before publishing.

Example history scrub (already applied in this repository for `data/smoke.db`):

```powershell
git filter-branch --force --index-filter "git rm --cached --ignore-unmatch data/smoke.db" --prune-empty --tag-name-filter cat -- --all
git for-each-ref --format="%(refname)" refs/original/ | ForEach-Object { git update-ref -d $_ }
git reflog expire --expire=now --all
git gc --prune=now --aggressive
```

After rewriting published history, push with lease:

```powershell
git push --force-with-lease origin main
```

## Quick start

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .[dev]
streamlit run src/tax_calc_at/ui/app.py
```

## Design principles

- Strict failure semantics (no silent fallback for unknown rows, missing FX,
  oversells, dedup mismatch, or cutoff violations).
- Cost basis is computed per broker per ISIN.
- Rules and mapping are versioned in YAML under `rules/`.
- ECB rates are used for tax math.

## Project layout

```
rules/                # tax YAMLs + broker config + parser overrides
src/tax_calc_at/      # core package
data/                 # local DB/cache/runtime files (not for publication)
exports/              # local user exports (not for publication)
tests/                # pytest suite
```
