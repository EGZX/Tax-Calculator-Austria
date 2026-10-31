# AGENTS.md

## Purpose
- This project computes Austrian E1kv capital-income tax figures for **foreign / non-steuereinfache brokers** from CSV exports.
- Core entry points are in `src/tax_calc_at/service.py`: `import_file()` for ingest, `build_year_report()` for report generation, `reimport_archive()` to rebuild a store.
- Prefer changing YAML in `rules/` over hardcoding tax-year behavior in Python.

## Architecture map
- `src/tax_calc_at/model.py` defines the canonical `Transaction` model, typed flags/flag codes, and all loud-failure exceptions.
- Import flow: broker parser (`src/tax_calc_at/parsers/`, dispatched by `parsers.parse_file`, which also applies the steuereinfach cutoff) -> ECB FX conversion (`src/tax_calc_at/fx/convert.py`) -> SQLite persistence (`src/tax_calc_at/store.py`).
- Report flow: stored rows -> asset-class overrides + user declarations (`src/tax_calc_at/declarations.py`, report-time only) -> pool replay (`src/tax_calc_at/pool.py`: securities pools + foreign-currency cash ledger) -> E1kv aggregation (`src/tax_calc_at/engine/e1kv.py`).
- `src/tax_calc_at/ui/app.py` is a thin Streamlit shell around the service layer; keep business logic out of the UI.
- `store.py` makes imports idempotent via `Transaction.dedup_key()` + `content_hash()`; re-imported edited rows raise `DuplicateMismatchError`. `SCHEMA_VERSION` must be bumped whenever parser semantics or persisted fields change; an older store raises `DatabaseVersionError` (re-import via `reimport_archive`).

## Repo-specific invariants
- **Strict**: unknown parser rows, unsupported asset classes (options/futures/CFDs are tariff-taxed), missing ECB rates, oversells, unresolved corporate actions, undeclared depot transfers, unknown withholding countries and incomplete classification raise typed errors or report blockers — never silent skips or defaults.
- Cost basis is **per broker (depot) per ISIN** (§ 27a Abs 4 Z 3 EStG). Basis only moves between depots through a declared transfer (`rules/depot_transfers.yaml`).
- Fees and transaction taxes never enter cost basis or proceeds (§ 27a Abs 4 Z 2, § 20 Abs 2 EStG); `fee_native` is a cost (rebates negative) used only by the FX cash ledger.
- FX (EStR Rz 6201–6201c): securities and income are valued at the ECB reference rate (cache in `fx/ecb.py`; offline seeding via `load_ecb_history_csv`). Foreign-currency cash is its own asset: every non-EUR cash flow runs through a per-broker per-currency moving-average ledger; actual conversions into EUR (IBKR AutoFX, Trading 212 automatic conversion, TR dividends) are `FX_CONVERSION` rows valued at the actual rate. Negative balances are debts (no taxable FX effect).
- `Transaction.gross_native` uses signed cash-flow semantics: BUY negative, SELL/income positive. `dividend_is_net` must be declared by the parser for any income row with withholding (IBKR, TR, T212 rows are GROSS; Scalable distributions are NET).
- E1kv reporting is **gross**: gains and losses in separate Kennzahlen, never netted (the Finanzamt nets, Anm 19). The loss offset is only simulated to reduce the creditable foreign tax (EStR Rz 7752h).
- Creditable foreign tax = min(withheld, DBA dividend rate × gross, Austrian rate × gross), per-country ceiling after loss offset. No default treaty rate. Interest withholding is not credited unless `interest_caps` lists the country.

## Parser conventions
- Parsers are registered in `src/tax_calc_at/parsers/__init__.py`; each exposes `parse(path) -> tuple[list[Transaction], ParseReport]`. Build rows with `parsers.base.make_tx`.
- Fail loudly on new broker enum values.
- Broker-specific facts (verified against real exports and broker tax reports):
  - IBKR Flex: `Dividends` are gross; `Withholding Tax` rows are summed **signed** per ActionID and booking date (reversals net out); the taxing country comes from the "- XX TAX" description. Only STK/FUND/BOND/CASH trades with multiplier 1. Corporate Actions (FS/RS/IC/SO/SD/TC) and Transfers sections are parsed into split/exchange/transfer legs; `SD` (stock dividend) carries the `corporate_action.unclassified` blocker until declared.
  - Trading 212: trades in the instrument currency + `FX_CONVERSION` at T212's rate; dividend gross = shares × net price + `Withholding tax`; IDs are reused, so `raw_ref = ID|Action|Time`; split close/open rows are paired.
  - Scalable Capital: same-ISIN corporate-action legs are netted into one SPLIT; an outgoing and an incoming ISIN with equal valuation become an EXCHANGE (basis carried). Old shares out + new in is a split; shares only added (no outgoing leg) carry the `corporate_action.unclassified` blocker (split effected as stock dividend vs taxable bonus shares cannot be told apart). Scalable books corporate actions under German law — its valuations are only used for pairing. `tax` on trades is a transaction tax (cost); `tax` on interest rows is ignored.
  - Scalable CSV gaps (verified against the KPMG 2025 report): some distributions show `tax` 0 although tax was withheld, and 31 Dec interest is missing from exports taken earlier. The parser cannot detect this; reconcile with the annual tax report.
  - Trade Republic: `amount` on DIVIDEND is gross EUR, `tax` negative = withheld; foreign dividends are booked in `original_currency`. STOCKPERK and BENEFITS_SAVEBACK are card cashback (EStR Rz 6134); MIGRATION direction is the sign of `shares`.
- Keep non-tax rows as `TxType.IGNORED` for auditability.

## Rules and reporting
- `rules/brokers.yaml`: parser, `credit_institution` (decides 25 % KZ 861 vs 27,5 % KZ 863 for cash interest/cashback, KZ 865 vs 994 for FX gains) and the account-specific `steuereinfach_from`.
- `rules/tax_YYYY.yaml`: Kennzahlen (kind, rate, loss/credit bucket), DBA dividend caps, EUR-stable currencies and classification (first match wins; keys `tx_type`, `asset_class`, `credit_institution`). Numbers verified against the BMF E 1kv 2023–2025 forms; `tbv: true` blocks filing (all of 2026 until the form is published).
- User declarations (report-time, must match data): `withholding_countries.yaml`, `return_of_capital.yaml`, `corporate_actions.yaml` (`split`, `spinoff`, `taxable_distribution` with optional partial `quantity`, `exchange`), `depot_transfers.yaml`, plus `asset_class_overrides.yaml`. Public files in `rules/` are empty templates; personal entries go into the git-ignored `rules/local/` (same names, combined; `brokers.yaml` there overrides per-broker fields). Never commit personal facts; tests are isolated from `rules/local/` by an autouse fixture.
- Transfer declarations carry the computed Austrian basis; `notified_cost_basis_eur` only triggers a warning when the notification stated a different figure.
- Not computed (blockers): ETF/fund ausschüttungsgleiche Erträge and Nicht-Meldefonds rules, non-securitised derivatives, crypto, accrued interest on IBKR bonds, mixed cash+share mergers.

## Data and persistence
- Runtime DB defaults to `data/tax.db`; smoke runs use `data/smoke.db`.
- Imported raw files are archived to `data/raw/<broker>/<sha12>__<original_name>`; `exports/` holds user-supplied read-only exports (git-ignored, never commit).
- If you add persisted fields, update insert and fetch paths and bump `SCHEMA_VERSION`.

## Useful workflows
- Setup / UI:
  ```powershell
  python -m venv .venv
  .\\.venv\\Scripts\\Activate.ps1
  pip install -e .[dev]
  streamlit run src/tax_calc_at/ui/app.py
  ```
- Tests: `pytest -q` (export-based smoke tests skip when `exports/` is absent).
- End-to-end smoke run against actual exports: `.\\.venv\\Scripts\\python.exe scripts\\smoke_e2e.py`
- Static checks: `ruff check src tests`, `mypy` (strict).

## When making changes
- Start from `service.py` and tests to understand behavior before editing internals.
- Add tests near the affected layer (`tests/test_pool.py`, `tests/test_parsers_smoke.py`, `tests/test_engine_tax_behavior.py`, `tests/test_declarations.py`, `tests/test_store_and_engine.py`).
- Preserve auditability: if a row is ignored or a treatment is assumed, encode why in `notes`/`flags`; WARNING/ERROR flags surface in the report.
