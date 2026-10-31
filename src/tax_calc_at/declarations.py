"""User declarations: facts that broker exports cannot contain.

Loaded from the public templates in ``rules/`` plus the personal, git-ignored
copies in ``rules/local/`` (entries are combined), and applied to the
transactions at report time (never persisted), so editing a file takes effect
without re-importing:

* ``withholding_countries.yaml`` — source country of foreign tax for ISINs
  whose ISIN country is not the taxing state (ADR/GDR, dual listings).
* ``return_of_capital.yaml`` — distributions proven to be Einlagenrückzahlung
  (§ 4 Abs 12 EStG). Broker "return of capital" labels alone are not proof.
* ``corporate_actions.yaml`` — treatment of corporate-action bookings the
  parser cannot resolve (tax-neutral split, spin-off basis split, taxable
  distribution — also for part of a combined booking — or exchange).
* ``depot_transfers.yaml`` — whether a transfer out of a foreign depot was
  notified to the Finanzamt within one month (§ 27 Abs 6 Z 2 EStG): neutral
  with basis carry-over if so, deemed sale at market value otherwise.

Every declaration must match at least one transaction; an unmatched entry is
an error so typos cannot silently disable a declaration.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field, replace
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

from .engine.rules import rule_files
from .model import (
    FLAG_CA_UNCLASSIFIED,
    FLAG_ROC_LABEL,
    FLAG_WHT_COUNTRY_UNCERTAIN,
    CorporateActionError,
    DepotTransferError,
    Severity,
    TaxCalcError,
    Transaction,
    TxType,
)


class DeclarationError(TaxCalcError):
    """A declaration file is invalid or does not match the data."""


# ----------------------------------------------------------------- schemas
class _Window(BaseModel):
    broker: str
    date_from: date
    date_to: date

    @model_validator(mode="after")
    def _order(self) -> _Window:
        if self.date_to < self.date_from:
            raise ValueError("date_to before date_from")
        return self

    def matches(self, tx: Transaction) -> bool:
        return tx.broker == self.broker and self.date_from <= tx.trade_date <= self.date_to


class TransferDecl(BaseModel):
    id: str
    notified: bool
    out: _Window | None = None
    in_: _Window | None = Field(default=None, alias="in")
    isins: list[str] = Field(min_length=1)  # ["*"] = all ISINs in the windows
    # ISINs to leave out of a "*" declaration (e.g. securities whose
    # notification contained wrong data and are declared separately).
    exclude_isins: list[str] = Field(default_factory=list)
    # notified + no `out` side: carried-over basis per ISIN (EUR).
    cost_basis_eur: dict[str, Decimal] = Field(default_factory=dict)
    # notified: Anschaffungskosten per ISIN as stated in the notification.
    # Only compared with the computed Austrian basis (which is always the one
    # carried over); a difference is reported so the records get corrected.
    notified_cost_basis_eur: dict[str, Decimal] = Field(default_factory=dict)
    # not notified: market value per ISIN (EUR); defaults to the row's own
    # price × quantity when the export carries a valuation price.
    market_value_eur: dict[str, Decimal] = Field(default_factory=dict)
    note: str = ""

    @model_validator(mode="after")
    def _sides(self) -> TransferDecl:
        if self.out is None and self.in_ is None:
            raise ValueError("needs at least one of `out` / `in`")
        return self

    def covers(self, isin: str | None) -> bool:
        if isin is not None and isin in self.exclude_isins:
            return False
        return "*" in self.isins or (isin is not None and isin in self.isins)


class CorporateActionDecl(BaseModel):
    broker: str
    date: date
    isin: str
    treatment: Literal["split", "spinoff", "taxable_distribution", "exchange"]
    parent_isin: str | None = None  # spinoff
    basis_fraction: Decimal | None = None  # spinoff (KapMaßVO § 4 Abs 2)
    market_value_eur: Decimal | None = None  # taxable_distribution (total)
    # taxable_distribution: shares of the booking that are the taxable part
    # (default: all). The rest stays a tax-neutral split, e.g. BYD 2025:
    # 8:10 bonus shares out of profits taxable, 12:10 from capital reserve not.
    quantity: Decimal | None = None
    from_isin: str | None = None  # exchange: security given up
    note: str = ""

    @model_validator(mode="after")
    def _fields(self) -> CorporateActionDecl:
        need = {
            "split": (),
            "spinoff": ("parent_isin", "basis_fraction"),
            "taxable_distribution": ("market_value_eur",),
            "exchange": ("from_isin",),
        }[self.treatment]
        missing = [n for n in need if getattr(self, n) is None]
        if missing:
            raise ValueError(f"treatment {self.treatment} requires {missing}")
        if self.basis_fraction is not None and not (
            Decimal("0") <= self.basis_fraction < Decimal("1")
        ):
            raise ValueError("basis_fraction must be in [0, 1)")
        if self.quantity is not None:
            if self.treatment != "taxable_distribution":
                raise ValueError("quantity is only valid for taxable_distribution")
            if self.quantity <= 0:
                raise ValueError("quantity must be positive")
        if self.market_value_eur is not None and self.market_value_eur <= 0:
            raise ValueError("market_value_eur must be positive")
        return self


class RocDecl(BaseModel):
    broker: str
    isin: str
    date_from: date | None = None
    date_to: date | None = None
    reference_regex: str | None = None
    evidence: str = Field(min_length=3)  # what proves the Einlagenrückzahlung

    @field_validator("reference_regex")
    @classmethod
    def _regex(cls, v: str | None) -> str | None:
        if v is not None:
            re.compile(v)
        return v

    def matches(self, tx: Transaction) -> bool:
        if tx.broker != self.broker or (tx.isin or "").upper() != self.isin.upper():
            return False
        if self.date_from and tx.trade_date < self.date_from:
            return False
        if self.date_to and tx.trade_date > self.date_to:
            return False
        return not (
            self.reference_regex and not re.search(self.reference_regex, tx.raw_ref or "")
        )


@dataclass
class Declarations:
    withholding_countries: dict[str, str] = field(default_factory=dict)
    return_of_capital: list[RocDecl] = field(default_factory=list)
    corporate_actions: list[CorporateActionDecl] = field(default_factory=list)
    transfers: list[TransferDecl] = field(default_factory=list)


def _read(path: Path, key: str) -> object:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise DeclarationError(f"{path}: top level must be a mapping")
    return raw.get(key)


def _read_list(name: str, key: str, rules_dir: Path | None) -> list[object]:
    out: list[object] = []
    for path in rule_files(name, rules_dir):
        value = _read(path, key) or []
        if not isinstance(value, list):
            raise DeclarationError(f"{path}: `{key}` must be a list")
        out.extend(value)
    return out


def load_declarations(rules_dir: Path | None = None) -> Declarations:
    """Declarations from ``rules_dir`` (default ``rules/``) and its ``local/``."""
    try:
        wc: dict[str, str] = {}
        for path in rule_files("withholding_countries.yaml", rules_dir):
            raw = _read(path, "withholding_countries") or {}
            if not isinstance(raw, dict):
                raise DeclarationError(f"{path}: must map ISIN -> country")
            for k, v in raw.items():
                isin, c = str(k).upper(), str(v).upper()
                if len(c) != 2 or not c.isalpha():
                    raise DeclarationError(f"{path}: bad country {c!r} for {isin}")
                if wc.get(isin, c) != c:
                    raise DeclarationError(f"{path}: conflicting country for {isin}")
                wc[isin] = c
        roc = [
            RocDecl.model_validate(e)
            for e in _read_list("return_of_capital.yaml", "return_of_capital", rules_dir)
        ]
        ca = [
            CorporateActionDecl.model_validate(e)
            for e in _read_list("corporate_actions.yaml", "corporate_actions", rules_dir)
        ]
        tr = [
            TransferDecl.model_validate(e)
            for e in _read_list("depot_transfers.yaml", "transfers", rules_dir)
        ]
    except (ValueError, TypeError) as e:
        if isinstance(e, DeclarationError):
            raise
        raise DeclarationError(f"Invalid declaration file: {e}") from e
    ids = [t.id for t in tr]
    if len(ids) != len(set(ids)):
        raise DeclarationError("depot_transfers.yaml: transfer ids must be unique")
    return Declarations(wc, roc, ca, tr)


# ----------------------------------------------------------------- apply
def apply_declarations(txns: list[Transaction], decl: Declarations) -> None:
    """Resolve declared facts on ``txns`` in place (report-time only)."""
    _apply_withholding_countries(txns, decl.withholding_countries)
    _apply_return_of_capital(txns, decl.return_of_capital)
    _apply_corporate_actions(txns, decl.corporate_actions)
    _apply_transfers(txns, decl.transfers)


def _drop_flag(tx: Transaction, code: str) -> None:
    tx.flags = [f for f in tx.flags if f.code != code]


def _apply_withholding_countries(txns: list[Transaction], wc: dict[str, str]) -> None:
    used: set[str] = set()
    for tx in txns:
        country = wc.get((tx.isin or "").upper())
        if country is None:
            continue
        used.add((tx.isin or "").upper())
        if tx.tx_type in (TxType.DIVIDEND_CASH, TxType.DIVIDEND_STOCK, TxType.RETURN_OF_CAPITAL):
            tx.withholding_country = country
            _drop_flag(tx, FLAG_WHT_COUNTRY_UNCERTAIN)
            tx.add_flag("declared.withholding_country", Severity.INFO,
                        f"Withholding country {country} declared by the user.")
    unused = sorted(set(wc) - used)
    if unused:
        raise DeclarationError(f"withholding_countries.yaml: no transactions for {unused}")


def _apply_return_of_capital(txns: list[Transaction], decls: list[RocDecl]) -> None:
    for d in decls:
        hits = [t for t in txns if t.tx_type is TxType.DIVIDEND_CASH and d.matches(t)]
        if not hits:
            raise DeclarationError(
                f"return_of_capital.yaml: {d.broker}/{d.isin} matches no distribution"
            )
        for tx in hits:
            tx.tx_type = TxType.RETURN_OF_CAPITAL
            tx.declared = True
            _drop_flag(tx, FLAG_ROC_LABEL)
            tx.add_flag("declared.return_of_capital", Severity.INFO,
                        f"Einlagenrückzahlung declared (evidence: {d.evidence}).")


def _apply_corporate_actions(txns: list[Transaction], decls: list[CorporateActionDecl]) -> None:
    added: list[Transaction] = []
    for d in decls:
        legs = [
            t for t in txns
            if t.broker == d.broker and t.trade_date == d.date and t.tx_type is TxType.SPLIT
        ]
        target = [t for t in legs if (t.isin or "") == d.isin and t.quantity > 0]
        if len(target) != 1:
            raise CorporateActionError(
                f"corporate_actions.yaml {d.broker} {d.date} {d.isin}: expected exactly "
                f"one incoming corporate-action leg, found {len(target)}."
            )
        tx = target[0]
        tx.declared = True
        _drop_flag(tx, FLAG_CA_UNCLASSIFIED)
        if d.treatment == "split":
            pass  # stays a SPLIT: basis unchanged (KapMaßVO §§ 4 Abs 1, 6)
        elif d.treatment == "spinoff":
            tx.tx_type = TxType.SPINOFF_IN
            tx.link_id = d.parent_isin
            tx.basis_fraction = d.basis_fraction
        elif d.treatment == "taxable_distribution":
            assert d.market_value_eur is not None
            qty = d.quantity if d.quantity is not None else tx.quantity
            if qty > tx.quantity:
                raise CorporateActionError(
                    f"corporate_actions.yaml {d.broker} {d.date} {d.isin}: taxable "
                    f"quantity {qty} exceeds the booked {tx.quantity} shares."
                )
            if qty < tx.quantity:
                # Combined booking: the declared part is income, the rest a
                # tax-neutral split of the same date.
                income = replace(tx, quantity=qty, flags=list(tx.flags),
                                 raw_ref=f"{tx.raw_ref or ''}#taxable")
                tx.quantity -= qty
                tx.add_flag("declared.corporate_action", Severity.INFO,
                            f"{tx.quantity} shares tax-neutral, {qty} declared as taxable "
                            f"distribution. {d.note}".strip())
                added.append(income)
                tx = income
            tx.tx_type = TxType.DIVIDEND_STOCK
            tx.amount_eur = d.market_value_eur
            tx.tax_withheld_eur = Decimal("0")
        else:
            out = [t for t in legs if (t.isin or "") == d.from_isin and t.quantity < 0]
            if len(out) != 1:
                raise CorporateActionError(
                    f"corporate_actions.yaml {d.broker} {d.date}: exchange needs exactly "
                    f"one outgoing leg on {d.from_isin}, found {len(out)}."
                )
            link = f"decl:{d.broker}:{d.date}:{d.from_isin}->{d.isin}"
            out[0].tx_type, out[0].quantity, out[0].link_id = (
                TxType.EXCHANGE_OUT, -out[0].quantity, link
            )
            out[0].declared = True
            tx.tx_type, tx.link_id = TxType.EXCHANGE_IN, link
        tx.add_flag("declared.corporate_action", Severity.INFO,
                    f"Treatment '{d.treatment}' declared by the user. {d.note}".strip())
    txns.extend(added)


def _market_value(tx: Transaction, d: TransferDecl) -> Decimal:
    v = d.market_value_eur.get(tx.isin or "")
    if v is not None:
        return v
    if tx.price_native and tx.fx_rate_used:
        return (tx.price_native * tx.quantity * tx.fx_rate_used).quantize(Decimal("0.0001"))
    raise DepotTransferError(
        f"depot_transfers.yaml {d.id}: no market value for {tx.isin} "
        f"({tx.source_file}:{tx.source_line}); add market_value_eur."
    )


def _apply_transfers(txns: list[Transaction], decls: list[TransferDecl]) -> None:
    claimed: set[int] = set()
    for d in decls:
        outs = [
            t for t in txns
            if t.tx_type is TxType.MIGRATION_OUT and d.out and d.out.matches(t) and d.covers(t.isin)
        ]
        ins = [
            t for t in txns
            if t.tx_type is TxType.MIGRATION_IN and d.in_ and d.in_.matches(t) and d.covers(t.isin)
        ]
        legs = outs + ins
        if not legs:
            raise DeclarationError(f"depot_transfers.yaml {d.id}: matches no transfer rows")
        for t in legs:
            if id(t) in claimed:
                raise DeclarationError(
                    f"depot_transfers.yaml {d.id}: {t.source_file}:{t.source_line} is "
                    f"already covered by another declaration"
                )
            claimed.add(id(t))
            t.declared = True
            t.add_flag("declared.depot_transfer", Severity.INFO,
                       f"Transfer '{d.id}' declared ({'notified' if d.notified else 'NOT notified'}"
                       f" to the Finanzamt). {d.note}".strip())
        if not d.notified:
            # Deemed sale at the gemeiner Wert of the transfer date; the
            # receiving depot takes that value over as cost (EStR Rz 6160a).
            out_value: dict[str, Decimal] = defaultdict(Decimal)
            out_qty: dict[str, Decimal] = defaultdict(Decimal)
            for t in outs:
                t.deemed_sale_eur = _market_value(t, d)
                out_value[t.isin or ""] += t.deemed_sale_eur
                out_qty[t.isin or ""] += t.quantity
            in_qty: dict[str, Decimal] = defaultdict(Decimal)
            for t in ins:
                in_qty[t.isin or ""] += t.quantity
            for t in ins:
                key = t.isin or ""
                if key in out_value:
                    if in_qty[key] != out_qty[key]:
                        raise DepotTransferError(
                            f"depot_transfers.yaml {d.id}: {key} out {out_qty[key]} "
                            f"vs in {in_qty[key]} units"
                        )
                    t.declared_cost_eur = out_value[key] * t.quantity / in_qty[key]
                else:
                    t.declared_cost_eur = _market_value(t, d)
            continue
        if d.out is not None and d.in_ is not None:
            out_q: dict[str, Decimal] = defaultdict(Decimal)
            in_q: dict[str, Decimal] = defaultdict(Decimal)
            for t in outs:
                out_q[t.isin or ""] += t.quantity
            for t in ins:
                in_q[t.isin or ""] += t.quantity
            if out_q != in_q:
                diff = {
                    k: (out_q.get(k), in_q.get(k))
                    for k in set(out_q) | set(in_q)
                    if out_q.get(k) != in_q.get(k)
                }
                raise DepotTransferError(
                    f"depot_transfers.yaml {d.id}: outgoing and incoming quantities "
                    f"differ per ISIN (out, in): {diff}"
                )
            for t in legs:
                t.link_id = f"transfer:{d.id}:{t.isin}"
            for t in outs:
                stated = d.notified_cost_basis_eur.get(t.isin or "")
                if stated is not None:
                    t.notified_cost_eur = stated * t.quantity / out_q[t.isin or ""]
        if d.out is not None:
            unknown = set(d.notified_cost_basis_eur) - {t.isin for t in outs}
            if unknown:
                raise DeclarationError(
                    f"depot_transfers.yaml {d.id}: notified_cost_basis_eur for ISINs "
                    f"without outgoing rows: {sorted(unknown)}"
                )
        if d.out is not None and d.in_ is None:
            out_only: dict[str, Decimal] = defaultdict(Decimal)
            for t in outs:
                out_only[t.isin or ""] += t.quantity
            for t in outs:
                stated = d.notified_cost_basis_eur.get(t.isin or "")
                if stated is not None:
                    t.notified_cost_eur = stated * t.quantity / out_only[t.isin or ""]
        elif d.out is None:
            qty_by_isin: dict[str, Decimal] = defaultdict(Decimal)
            for t in ins:
                qty_by_isin[t.isin or ""] += t.quantity
            for t in ins:
                basis = d.cost_basis_eur.get(t.isin or "")
                if basis is None:
                    raise DepotTransferError(
                        f"depot_transfers.yaml {d.id}: incoming {t.isin} needs "
                        f"cost_basis_eur (the Anschaffungskosten reported to the Finanzamt)."
                    )
                # Total basis per ISIN, spread over the incoming legs by quantity.
                t.declared_cost_eur = basis * t.quantity / qty_by_isin[t.isin or ""]
