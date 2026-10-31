"""YAML rule loader.

Loads ``rules/brokers.yaml`` and ``rules/tax_YYYY.yaml`` into typed pydantic
models. ``TaxRules.classify(...)`` returns the matching bucket name (or
``None`` for non-income rows) and raises :class:`ClassificationError` when the
rule file does not cover a combination.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator

from ..model import AssetClass, ClassificationError, TxType

DEFAULT_RULES_DIR = Path(__file__).resolve().parents[3] / "rules"
# Personal, git-ignored overlay for account-specific facts (declarations,
# asset-class pins, broker cutoffs). Files here extend the public templates
# of the same name and must never be committed.
LOCAL_RULES_DIR = DEFAULT_RULES_DIR / "local"


def rule_files(name: str, rules_dir: Path | None = None) -> list[Path]:
    """Existing public and local copies of a rules file, public first."""
    base = rules_dir if rules_dir is not None else DEFAULT_RULES_DIR
    local = LOCAL_RULES_DIR if rules_dir is None else base / "local"
    return [p for p in (base / name, local / name) if p.exists()]


# ------------------------------------------------------------------- brokers
class BrokerConfig(BaseModel):
    parser: str
    display_name: str
    jurisdiction: str = "foreign"
    default_currency: str = "EUR"
    steuereinfach_from: date | None = None
    # True if cash held at this broker is a "Geldeinlage bei einem
    # Kreditinstitut" (§ 27a Abs 1 Z 1 EStG, EStR Rz 7705: credit institution
    # in the sense of § 1 BWG / CRR). Drives the 25 % vs 27,5 % split of cash
    # interest, cashback and FX gains on cash balances. Required: there is no
    # safe default.
    credit_institution: bool

    @field_validator("jurisdiction")
    @classmethod
    def _check_jurisdiction(cls, v: str) -> str:
        if v not in {"foreign", "domestic"}:
            raise ValueError(f"jurisdiction must be 'foreign' or 'domestic', got {v!r}")
        return v


class BrokersConfig(BaseModel):
    brokers: dict[str, BrokerConfig]

    def get(self, key: str) -> BrokerConfig:
        if key not in self.brokers:
            raise KeyError(f"Unknown broker: {key!r}. Known: {list(self.brokers)}")
        return self.brokers[key]


def load_brokers(path: Path | None = None) -> BrokersConfig:
    """Public ``rules/brokers.yaml`` merged with ``rules/local/brokers.yaml``
    (per broker key; local fields win, e.g. an account's own
    ``steuereinfach_from``). An explicit ``path`` loads only that file."""
    paths = [path] if path is not None else rule_files("brokers.yaml")
    merged: dict[str, dict[str, Any]] = {}
    for p in paths:
        raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
        for key, fields in (raw.get("brokers") or {}).items():
            if not isinstance(fields, dict):
                raise ValueError(f"{p}: brokers.{key} must be a mapping")
            merged.setdefault(str(key), {}).update(fields)
    return BrokersConfig.model_validate({"brokers": merged})


# ------------------------------------------------------------------ tax year
class BucketKind(StrEnum):
    INCOME = "income"  # current income (dividends, interest, fund distributions)
    GAIN = "gain"  # realized Überschüsse
    LOSS = "loss"  # realized Verluste (reported negative)
    CREDIT = "credit"  # creditable foreign tax


class Kennzahl(BaseModel):
    nr: int
    label: str
    kind: BucketKind
    # Special tax rate of the income in this bucket (§ 27a Abs 1). Required
    # for income/gain buckets: used for the per-item credit ceiling
    # (Anrechnungshöchstbetrag) and the loss-offset order (E1kv Anm 19:
    # losses go against 27,5 % Überschüsse first).
    rate: Decimal | None = None
    # Member of E1kv section 1.3 (Verlustausgleich zulässig, § 27 Abs 8).
    loss_offset: bool = False
    tbv: bool = False  # to-be-verified marker: blocks filing when used
    # Bucket receiving the creditable foreign tax on income of THIS bucket.
    credit_bucket: str | None = None
    # Gain buckets only: negative results are reported in this loss bucket
    # (E1kv: "Überschüsse und Verluste sind getrennt (nicht saldiert)
    # einzutragen").
    loss_bucket: str | None = None


class ForeignWithholding(BaseModel):
    """DBA residual source-state rates (fraction of the GROSS income).

    There is deliberately no default: a source country missing from the
    table makes the credit (KZ 998/901) uncomputable and blocks the report.
    """

    # Portfolio dividends (< 10 % holding), DBA Art 10.
    dividend_caps: dict[str, Decimal]
    # Interest (DBA Art 11). Empty = no credit.
    interest_caps: dict[str, Decimal] = Field(default_factory=dict)
    # Treaty suspended / special handling (e.g. RU, BY): always blocks.
    manual_review_countries: list[str] = Field(default_factory=list)


_CLASSIFICATION_KEYS = frozenset({"tx_type", "asset_class", "credit_institution"})


class ClassificationRule(BaseModel):
    when: dict[str, Any]
    bucket: str | None = None
    note: str | None = None

    def matches(
        self, tx_type: TxType, asset_class: AssetClass, credit_institution: bool | None
    ) -> bool:
        for key, expected in self.when.items():
            if key not in _CLASSIFICATION_KEYS:
                # Unknown match keys are a hard error — never silently ignored.
                raise ValueError(f"Unsupported classification key: {key!r}")
            if key == "credit_institution":
                if not isinstance(expected, bool):
                    raise ValueError("classification key credit_institution must be a bool")
                if credit_institution is None:
                    raise ClassificationError(
                        f"Rule for {tx_type.value} depends on the broker's "
                        f"credit_institution status, which is unknown (broker "
                        f"missing from rules/brokers.yaml?)."
                    )
                if credit_institution is not expected:
                    return False
                continue
            actual = tx_type.value if key == "tx_type" else asset_class.value
            allowed = expected if isinstance(expected, list) else [expected]
            if actual not in allowed:
                return False
        return True


class TaxRules(BaseModel):
    year: int
    form_source: str
    foreign_withholding: ForeignWithholding
    kennzahlen: dict[str, Kennzahl]
    # Currencies treated as "zum Euro wechselkursstabil" (EStR Rz 6201):
    # converting a foreign-currency balance into one of them is a taxable
    # exchange, like converting into EUR.
    eur_stable_currencies: list[str] = Field(default_factory=list)
    classification: list[ClassificationRule]

    def validate_schema(self) -> None:
        """Cross-check integrity that pydantic cannot express on its own."""
        declared = self.kennzahlen
        fname = f"tax_{self.year}.yaml"
        for name, kz in declared.items():
            if kz.kind in (BucketKind.INCOME, BucketKind.GAIN) and kz.rate is None:
                raise ValueError(f"{fname} kennzahlen[{name!r}] needs a rate")
            if kz.credit_bucket is not None:
                cb = declared.get(kz.credit_bucket)
                if cb is None or cb.kind is not BucketKind.CREDIT:
                    raise ValueError(
                        f"{fname} kennzahlen[{name!r}].credit_bucket "
                        f"{kz.credit_bucket!r} is not a declared credit bucket."
                    )
            if kz.loss_bucket is not None:
                if kz.kind is not BucketKind.GAIN:
                    raise ValueError(f"{fname} kennzahlen[{name!r}]: loss_bucket on non-gain")
                lb = declared.get(kz.loss_bucket)
                if lb is None or lb.kind is not BucketKind.LOSS:
                    raise ValueError(
                        f"{fname} kennzahlen[{name!r}].loss_bucket "
                        f"{kz.loss_bucket!r} is not a declared loss bucket."
                    )
            if kz.kind is BucketKind.GAIN and kz.loss_bucket is None:
                raise ValueError(f"{fname} kennzahlen[{name!r}]: gain bucket needs loss_bucket")
        for i, rule in enumerate(self.classification):
            if rule.bucket is None:
                continue
            kz_or_none = declared.get(rule.bucket)
            if kz_or_none is None:
                raise ValueError(
                    f"{fname} classification[{i}] references unknown bucket "
                    f"{rule.bucket!r}. Known: {sorted(declared)}"
                )
            if kz_or_none.kind not in (BucketKind.INCOME, BucketKind.GAIN):
                raise ValueError(
                    f"{fname} classification[{i}] routes into {rule.bucket!r} "
                    f"(kind {kz_or_none.kind.value}); only income/gain buckets "
                    f"may be classification targets."
                )
        for country, cap in {
            **self.foreign_withholding.dividend_caps,
            **self.foreign_withholding.interest_caps,
        }.items():
            if len(country) != 2 or not country.isupper():
                raise ValueError(f"{fname}: invalid country code {country!r}")
            if not Decimal("0") <= cap <= Decimal("1"):
                raise ValueError(f"{fname}: cap for {country} out of range: {cap}")
        # Every TxType must have at least one classification rule, else
        # classify() would raise unexpectedly at report time.
        covered: set[str] = set()
        for rule in self.classification:
            expected = rule.when.get("tx_type")
            if expected is None:
                continue
            covered.update(expected if isinstance(expected, list) else [expected])
        missing = sorted({t.value for t in TxType} - covered)
        if missing:
            raise ValueError(
                f"{fname} classification is missing rules for TxType values: "
                f"{missing}. Every enum value must be covered."
            )

    def classify(
        self,
        tx_type: TxType,
        asset_class: AssetClass,
        *,
        credit_institution: bool | None = None,
    ) -> str | None:
        """Return the bucket for this row, or None for non-income rows.

        First matching rule wins. Raises :class:`ClassificationError` if no
        rule matches (the rule file is incomplete for this combination)."""
        for rule in self.classification:
            if rule.matches(tx_type, asset_class, credit_institution):
                return rule.bucket
        raise ClassificationError(
            f"No classification rule matched ({tx_type.value}, {asset_class.value}) "
            f"in tax_{self.year}.yaml"
        )

    def kennzahl(self, bucket: str) -> Kennzahl:
        if bucket not in self.kennzahlen:
            raise KeyError(f"Unknown bucket {bucket!r} in tax_{self.year}.yaml")
        return self.kennzahlen[bucket]


def load_tax_rules(year: int, path: Path | None = None) -> TaxRules:
    p = path or (DEFAULT_RULES_DIR / f"tax_{year}.yaml")
    if not p.exists():
        raise FileNotFoundError(f"No tax rules for year {year}: {p}")
    raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    rules = TaxRules.model_validate(raw)
    if rules.year != year:
        raise ValueError(f"tax_{year}.yaml declares year {rules.year}")
    rules.validate_schema()
    return rules
