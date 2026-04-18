"""Canonical transaction model and shared enums.

All money is carried as :class:`decimal.Decimal`. Native amounts use the broker's
original currency; ``*_eur`` fields are the ECB-converted EUR equivalents and
are populated by the FX layer, not by parsers.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum


class TxType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    DIVIDEND_CASH = "DIVIDEND_CASH"
    DIVIDEND_STOCK = "DIVIDEND_STOCK"
    # Capital return / Einlagenrückzahlung. NOT taxable as Kapitalertrag in AT;
    # it reduces the cost basis of the position. v1 does NOT auto-adjust the
    # pool basis — the row is excluded from income buckets and surfaces with a
    # WARNING flag so the user manually corrects basis if material.
    RETURN_OF_CAPITAL = "RETURN_OF_CAPITAL"
    # Interest from bank deposits at a Kreditinstitut — 25 % special rate
    # (§ 27a Abs. 1 Z 1 EStG, KZ 857). Use ONLY when the payer is clearly a
    # bank / Kreditinstitut.
    INTEREST = "INTEREST"
    # Interest from any other source: broker cash balances, share-lending
    # income, bond coupons, P2P lending, … Falls into the 27,5 %-Topf per
    # § 27a Abs. 1 Z 2 EStG. Routed to KZ 863 by default.
    INTEREST_OTHER = "INTEREST_OTHER"
    SPLIT = "SPLIT"
    MIGRATION_IN = "MIGRATION_IN"
    MIGRATION_OUT = "MIGRATION_OUT"
    DEPOSIT_CASH = "DEPOSIT_CASH"
    WITHDRAWAL_CASH = "WITHDRAWAL_CASH"
    FEE = "FEE"
    BONUS_SHARE = "BONUS_SHARE"  # TR Stockperk, T212 free shares
    IGNORED = "IGNORED"  # Rejected/Cancelled, kept for audit only


class AssetClass(str, Enum):
    STOCK = "STOCK"
    ETF = "ETF"
    BOND = "BOND"
    CASH = "CASH"
    CRYPTO = "CRYPTO"
    OTHER = "OTHER"


class FxSource(str, Enum):
    NATIVE_EUR = "NATIVE_EUR"  # transaction was already in EUR
    ECB = "ECB"
    BROKER = "BROKER"  # only for audit; tax math should use ECB
    NONE = "NONE"  # not yet converted


# Severity for parser/engine flags.
class Severity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


@dataclass(frozen=True)
class Flag:
    code: str
    severity: Severity
    message: str


@dataclass
class Transaction:
    """Canonical transaction. One row of broker truth, normalized.

    Either ``raw_ref`` (broker's transaction id) is set, or the dedup key falls
    back to a content hash. See :meth:`dedup_key`.
    """

    broker: str
    trade_date: date
    tx_type: TxType
    asset_class: AssetClass
    quantity: Decimal
    currency_native: str
    gross_native: Decimal  # signed: negative for cash outflow (BUY), positive for SELL/DIV
    source_file: str
    source_line: int

    # Optional identifiers
    raw_ref: str | None = None
    isin: str | None = None
    symbol: str | None = None
    name: str | None = None
    account: str | None = None

    # Optional pricing/fees (native ccy)
    price_native: Decimal | None = None
    fee_native: Decimal = Decimal("0")
    tax_withheld_native: Decimal = Decimal("0")

    # Settlement / timing
    settle_date: date | None = None
    trade_datetime: datetime | None = None  # if broker provides time

    # FX-converted (filled in by fx layer)
    amount_eur: Decimal | None = None
    fee_eur: Decimal | None = None
    tax_withheld_eur: Decimal | None = None
    fx_rate_used: Decimal | None = None
    fx_rate_source: FxSource = FxSource.NONE

    # Source-country hint for foreign withholding tax (ISO-2). May be derived
    # from ISIN prefix if broker doesn't supply it.
    withholding_country: str | None = None

    # For ``DIVIDEND_CASH`` rows: does ``gross_native`` already exclude the
    # foreign withholding (i.e. is it the *net* amount the broker paid us)?
    # True  = gross_native is NET; engine grosses it up by adding
    #         tax_withheld_native to recover the pre-tax income (§ 27 EStG).
    # False = gross_native is GROSS; tax_withheld_native was already included.
    # None  = convention unknown for a dividend that has a withholding
    #         component — engine raises ReportHealth blocker, because silently
    #         defaulting either way risks under- or double-counting income.
    # The dataclass default is ``None`` to FORCE each parser to declare its
    # convention explicitly; a silent default was masking parser bugs.
    dividend_is_net: bool | None = None

    # Metadata
    notes: str | None = None
    flags: list[Flag] = field(default_factory=list)
    import_batch_id: str | None = None

    # ------------------------------------------------------------------ helpers
    def add_flag(self, code: str, severity: Severity, message: str) -> None:
        self.flags.append(Flag(code=code, severity=severity, message=message))

    @property
    def has_error(self) -> bool:
        return any(f.severity is Severity.ERROR for f in self.flags)

    @property
    def has_warning(self) -> bool:
        return any(f.severity is Severity.WARNING for f in self.flags)

    def dedup_key(self) -> str:
        """Deterministic dedup hash.

        Prefer broker-supplied ``raw_ref``. Fallback uses normalized content so
        that re-importing the same file is idempotent and a *modified* row on
        the same key raises :class:`DuplicateMismatchError` upstream.
        """
        if self.raw_ref:
            payload = f"{self.broker}|{self.raw_ref}"
        else:
            payload = "|".join(
                [
                    self.broker,
                    self.trade_date.isoformat(),
                    self.tx_type.value,
                    self.isin or "",
                    f"{self.quantity:.10f}",
                    f"{self.gross_native:.6f}",
                    self.currency_native,
                    self.source_file,
                    str(self.source_line),
                ]
            )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def content_hash(self) -> str:
        """Hash of all tax-relevant fields, used to detect mismatches on re-import."""
        payload = "|".join(
            [
                self.broker,
                self.trade_date.isoformat(),
                self.tx_type.value,
                self.asset_class.value,
                self.isin or "",
                self.symbol or "",
                f"{self.quantity:.10f}",
                f"{self.gross_native:.6f}",
                f"{self.fee_native:.6f}",
                f"{self.tax_withheld_native:.6f}",
                self.currency_native,
            ]
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass
class ParseReport:
    """Summary of a parsing run for one file."""

    broker: str
    source_file: str
    file_sha256: str
    rows_total: int = 0
    rows_emitted: int = 0
    rows_ignored: int = 0
    rows_rejected: int = 0  # broker order status Rejected/Cancelled
    flags: list[Flag] = field(default_factory=list)

    def add_flag(self, code: str, severity: Severity, message: str) -> None:
        self.flags.append(Flag(code=code, severity=severity, message=message))


# --------------------------------------------------------------------- errors
class TaxCalcError(Exception):
    """Base class for all tax-calc errors. Always raised loudly, never caught
    in core code paths."""


class ParserError(TaxCalcError):
    """Parser could not understand a row."""

    def __init__(
        self,
        message: str,
        *,
        broker: str,
        source_file: str,
        source_line: int,
        raw: str | None = None,
    ) -> None:
        super().__init__(
            f"[{broker}] {source_file}:{source_line} {message}"
            + (f"\n  raw: {raw}" if raw else "")
        )
        self.broker = broker
        self.source_file = source_file
        self.source_line = source_line
        self.raw = raw


class DuplicateMismatchError(TaxCalcError):
    """Same dedup key but different content_hash → user edited a CSV."""


class FxRateMissingError(TaxCalcError):
    """No ECB rate available for (currency, date)."""


class OversellError(TaxCalcError):
    """SELL exceeds the broker pool's quantity for an ISIN."""


class CutoffViolationError(TaxCalcError):
    """Transaction date past a broker's steuereinfach cutoff."""


class CostBasisMissingError(TaxCalcError):
    """MIGRATION_IN with no provided cost basis was later sold."""


class ClassificationError(TaxCalcError):
    """No tax-rule bucket matched a transaction."""


class ReportNotFileableError(TaxCalcError):
    """The E1kv report has unresolved blockers and is not safe to file.

    Raised by :meth:`E1kvReport.by_kennzahl` when strict output is requested
    but the report carries blockers (unverified Kennzahlen, excluded ISINs
    from tolerant pool replay, unclassifiable income, etc.). The UI still
    renders a partial view, but programmatic callers must explicitly opt-in
    to partial output via ``allow_partial=True``.
    """
