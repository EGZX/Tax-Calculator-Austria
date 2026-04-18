# AGENTS.md

## Purpose
- This project computes Austrian E1kv capital-income tax figures for **foreign / non-steuereinfache brokers** from CSV exports.
- Core entry points are in `src/tax_calc_at/service.py`: use `import_file()` for ingest and `build_year_report()` for report generation.
- Prefer changing YAML in `rules/` over hardcoding tax-year behavior in Python.

## Architecture map
- `src/tax_calc_at/model.py` defines the canonical `Transaction` model, typed flags, and all loud-failure exceptions.
- Import flow is: broker parser (`src/tax_calc_at/parsers/`) -> ECB FX conversion (`src/tax_calc_at/fx/convert.py`) -> SQLite persistence (`src/tax_calc_at/store.py`) -> pool replay (`src/tax_calc_at/pool.py`) -> E1kv aggregation (`src/tax_calc_at/engine/e1kv.py`).
- `src/tax_calc_at/ui/app.py` is a thin Streamlit shell around the service layer; keep business logic out of the UI.
- `store.py` makes imports idempotent via `Transaction.dedup_key()` + `content_hash()`; re-imported edited rows must raise `DuplicateMismatchError`.

## Repo-specific invariants
- The project is intentionally **strict**: unknown parser rows, missing ECB rates, oversells, broker cutoff violations, and incomplete classification should raise typed errors, not be silently skipped.
- Cost basis is **per broker per ISIN** only. Never merge pools across brokers; see `pool.py` and `tests/test_pool.py`.
- FX for tax math must come from the ECB cache in `src/tax_calc_at/fx/ecb.py`; broker FX rates may be stored in flags/notes for audit, but not used for EUR tax amounts.
- `Transaction.gross_native` uses signed cash-flow semantics: BUY is negative, SELL/dividend is positive.
- `MIGRATION_IN` intentionally opens a pool with unknown basis; later SELLs must fail with `CostBasisMissingError` until basis is supplied.

## Parser conventions
- Parsers are registered in `src/tax_calc_at/parsers/__init__.py`; each exposes `parse(path) -> tuple[list[Transaction], ParseReport]`.
- Fail loudly on new broker enum values. Existing parsers treat unknown `Action`/`type` values as errors, not warnings.
- Keep non-tax rows as `TxType.IGNORED` when the repo already does that for auditability (for example Trade Republic post-cutoff rows or Scalable rejected rows).
- Follow broker-specific patterns already in code:
  - Trade Republic: enforce `steuereinfach_from` from `rules/brokers.yaml`; post-cutoff rows become `IGNORED` unless strict mode is requested.
  - Trading 212: use price currency as `currency_native` so ECB FX is applied; emit currency conversion fees as separate `FEE` rows with `raw_ref + "::convfee"`.
  - Scalable Capital:
    - `Distribution` defaults to `DIVIDEND_CASH`; rare confirmed RoC can be pinned via `rules/scalable_distribution_overrides.yaml`.
    - `Corporate action` rows are modeled as `SPLIT` (signed quantity delta, basis preserved).
    - `Security transfer` rows are modeled as `MIGRATION_IN`/`MIGRATION_OUT`, with paired in/out repairs to `SPLIT` for internal rebookings.
  - IBKR Flex parser is implemented in `src/tax_calc_at/parsers/ibkr_flex.py` (Trades + Cash sections, withholding fold-in by ActionID).

## Rules and reporting
- `rules/brokers.yaml` is the authority for parser selection and broker cutoffs.
- `rules/tax_YYYY.yaml` is the authority for rates, Kennzahlen, withholding caps, and transaction classification. `TaxRules.classify()` is first-match-wins and raises on missing coverage.
- `rules/scalable_distribution_overrides.yaml` is the authority for manual Scalable `Distribution` -> `RETURN_OF_CAPITAL` exceptions.
- `engine/e1kv.py` builds reports from realized sell events plus income transactions; realized events come from pool replay, not directly from SELL rows.
- `tbv: true` in tax YAML means Kennzahl numbers are still to-be-verified; preserve that metadata in UI/reporting.

## Data and persistence
- Runtime DB defaults to `data/tax.db`; smoke runs use `data/smoke.db`.
- Imported raw files are archived to `data/raw/<broker>/<sha12>__<original_name>` by `service.import_file()`; `exports/` acts as user-supplied read-only fixtures.
- SQLite tables are created on first connect in `store.py`; if you add persisted fields, update both insert and fetch paths.

## Useful workflows
- Setup / UI:
  ```powershell
  python -m venv .venv
  .\\.venv\\Scripts\\Activate.ps1
  pip install -e .[dev]
  streamlit run src/tax_calc_at/ui/app.py
  ```
- Tests use real fixture exports under `exports/`: run `pytest -q`.
- End-to-end smoke run against actual exports: `.\\.venv\\Scripts\\python.exe scripts\\smoke_e2e.py`
- Static checks are configured in `pyproject.toml`: `ruff`, strict `mypy`, and `pytest`.

## When making changes
- Start from `service.py` and tests to understand behavior before editing internals.
- Add/adjust tests near the affected layer (`tests/test_pool.py`, `tests/test_parsers_smoke.py`, `tests/test_store_and_engine.py`).
- Preserve auditability: if you must ignore a broker row, encode why in `notes`/`flags` rather than dropping it silently.

