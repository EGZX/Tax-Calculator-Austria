"""YAML rule loader.

Loads ``rules/brokers.yaml`` and ``rules/tax_YYYY.yaml`` files into typed
pydantic models. ``TaxRules.classify(tx_type, asset_class)`` returns the
matching bucket name or raises :class:`ClassificationError`.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator

from ..model import AssetClass, ClassificationError, TxType

DEFAULT_RULES_DIR = Path(__file__).resolve().parents[3] / "rules"


# ------------------------------------------------------------------- brokers
class BrokerConfig(BaseModel):
    parser: str
    display_name: str
    jurisdiction: str = "foreign"
    default_currency: str = "EUR"
    steuereinfach_from: date | None = None

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
    p = path or (DEFAULT_RULES_DIR / "brokers.yaml")
    raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    return BrokersConfig.model_validate(raw)


# ------------------------------------------------------------------ tax year
class Kennzahl(BaseModel):
    nr: int
    label: str
    tbv: bool = False  # to-be-verified marker
    # Optional name of the bucket that holds the creditable foreign withholding
    # tax for income classified into THIS bucket. Drives the credit map at
    # report time so renaming a bucket no longer silently breaks logic.
    credit_bucket: str | None = None


class LossOffset(BaseModel):
    # NOTE: cross_broker / cross_year are currently INFORMATIONAL only —
    # cross_broker aggregation is always performed and there is no carry-
    # forward implementation yet. The flags drive UI/report notes only.
    cross_broker: bool = True
    cross_year: bool = False
    cross_bucket_within_275: bool = True
    # Buckets eligible to absorb realized losses inside the 27.5% basket.
    # First entry is treated as the source of losses; remaining entries are
    # offset against in order. Defaults preserve previous hardcoded behavior.
    cross_bucket_within_275_buckets: list[str] = Field(
        default_factory=lambda: [
            "einkuenfte_realisierte_wertsteigerungen_27_5",
            "einkuenfte_ueberlassung_27_5",
        ]
    )


class ForeignWithholding(BaseModel):
    default_creditable_cap: Decimal
    country_caps: dict[str, Decimal] = Field(default_factory=dict)


class ClassificationRule(BaseModel):
    when: dict[str, Any]
    bucket: str | None = None
    note: str | None = None

    def matches(self, tx_type: TxType, asset_class: AssetClass) -> bool:
        for key, expected in self.when.items():
            actual: str
            if key == "tx_type":
                actual = tx_type.value
            elif key == "asset_class":
                actual = asset_class.value
            else:
                # Unknown match keys cause a hard error — we don't silently ignore rules.
                # Currently supported: tx_type, asset_class.
                raise ValueError(f"Unsupported classification key: {key!r}")
            if isinstance(expected, list):
                if actual not in expected:
                    return False
            else:
                if actual != expected:
                    return False
        return True


class TaxRules(BaseModel):
    year: int
    rates: dict[str, Decimal]
    foreign_withholding: ForeignWithholding
    kennzahlen: dict[str, Kennzahl]
    loss_offset: LossOffset
    classification: list[ClassificationRule]

    def validate_schema(self) -> None:
        """Cross-check integrity that Pydantic cannot express on its own.

        Catches YAML typos that would otherwise silently mis-route income
        into a ``None`` bucket at report time:
        * every ``classification[*].bucket`` must exist in ``kennzahlen`` or
          be ``None``;
        * every ``credit_bucket`` referenced from a ``Kennzahl`` must exist;
        * every loss-offset bucket reference must exist;
        * every ``TxType`` enum value must appear in at least one
          classification rule so the classifier never raises unexpectedly.
        """
        declared = set(self.kennzahlen.keys())
        for i, rule in enumerate(self.classification):
            if rule.bucket is not None and rule.bucket not in declared:
                raise ValueError(
                    f"tax_{self.year}.yaml classification[{i}] references "
                    f"unknown bucket {rule.bucket!r}. Known: {sorted(declared)}"
                )
        for name, kz in self.kennzahlen.items():
            if kz.credit_bucket and kz.credit_bucket not in declared:
                raise ValueError(
                    f"tax_{self.year}.yaml kennzahlen[{name!r}].credit_bucket "
                    f"references unknown bucket {kz.credit_bucket!r}."
                )
        for i, b in enumerate(self.loss_offset.cross_bucket_within_275_buckets):
            if b not in declared:
                raise ValueError(
                    f"tax_{self.year}.yaml loss_offset"
                    f".cross_bucket_within_275_buckets[{i}] = {b!r} is unknown."
                )
        # Every TxType must have at least one classification rule, else
        # classify() will raise unexpectedly at report time.
        covered: set[str] = set()
        for rule in self.classification:
            expected = rule.when.get("tx_type")
            if expected is None:
                # Rules with only asset_class match don't guarantee coverage.
                continue
            if isinstance(expected, list):
                covered.update(expected)
            else:
                covered.add(expected)
        missing = sorted({t.value for t in TxType} - covered)
        if missing:
            raise ValueError(
                f"tax_{self.year}.yaml classification is missing rules for "
                f"TxType values: {missing}. Every enum value must be covered."
            )

    def classify(self, tx_type: TxType, asset_class: AssetClass) -> str | None:
        """Return bucket name for this transaction, or None for non-income txns.

        Raises :class:`ClassificationError` if no rule matches at all (means
        the rule file is incomplete for this combination)."""
        for rule in self.classification:
            if rule.matches(tx_type, asset_class):
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
