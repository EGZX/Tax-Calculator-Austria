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
- Supported CSV parsers: IBKR Flex Query, Scalable Capital, Trade Republic, Trading 212.
- Rolling average cost basis per broker per ISIN.
- ECB-based FX conversion.
- YAML-driven classification and Kennzahl mapping (`rules/tax_YYYY.yaml`).

Explicitly not covered:
- ETF Meldefonds AGE / OeKB reporting logic.
- Crypto.
- Derivatives.
- PDF generation and filing automation.

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
