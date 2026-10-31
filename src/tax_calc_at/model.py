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
from enum import StrEnum


class TxType(StrEnum):
    BUY = "BUY"
    SELL = "SELL"
    DIVIDEND_CASH = "DIVIDEND_CASH"
    # Taxable dividend paid in shares: stock dividend / bonus shares out of
    # profits, scrip dividend, distribution in kind (§ 27 Abs 2 Z 1 EStG,
    # EStR Rz 307; BFG RV/5100407/2024). ``amount_eur`` is the market value of
    # the shares received; it is income at receipt and becomes the shares'
    # Anschaffungskosten. A capital increase from company funds of a
    # comparable company (§ 3 Abs 1 Z 29 EStG, EStR Rz 306/308) is a SPLIT.
    DIVIDEND_STOCK = "DIVIDEND_STOCK"
    # Einlagenrückzahlung (§ 4 Abs 12 EStG, KapMaßVO § 2 Abs 2 Z 1): not
    # income; reduces the pool's cost basis, any excess over the basis is a
    # realized gain (§ 27 Abs 3). Only valid with evidence — brokers' "return
    # of capital" labels are not accepted automatically (see parsers).
    RETURN_OF_CAPITAL = "RETURN_OF_CAPITAL"
    # Interest on a cash deposit held at the broker. Whether it is "Zinsen aus
    # Geldeinlagen bei Kreditinstituten" (25 %, § 27a Abs 1 Z 1) or ordinary
    # 27,5 % income is decided by the broker's ``credit_institution`` flag in
    # rules/brokers.yaml via the tax-year classification rules.
    INTEREST = "INTEREST"
    # Interest that is never deposit interest: bond coupons, share-lending
    # fees, … (27,5 %, § 27a Abs 1 Z 2).
    INTEREST_OTHER = "INTEREST_OTHER"
    # Card cashback / Saveback credited by the account provider (EStR
    # Rz 6134: capital income at receipt; 25 % if linked to a deposit at a
    # credit institution, else 27,5 %).
    CASHBACK = "CASHBACK"
    # Promotional cash bonus from the broker (account opening, free-share
    # promotion; EStR Rz 6132, § 27 Abs 5 Z 1): income at receipt. Shares
    # bought with it are an ordinary BUY at full cost.
    PROMO_BONUS = "PROMO_BONUS"
    # Same-ISIN quantity change with unchanged total cost basis (split,
    # reverse split, capital increase from company funds; KapMaßVO §§ 4 Abs 1,
    # 6). Signed quantity delta. Never valid on an empty position. Shares
    # added whose economic nature the export does not state (stock dividend
    # vs split) carry FLAG_CA_UNCLASSIFIED until declared.
    SPLIT = "SPLIT"
    # Tax-neutral exchange of one security for another (ISIN change,
    # comparable merger; KapMaßVO §§ 3, 8): the OUT legs' cost basis moves to
    # the IN legs that share the same ``link_id``.
    EXCHANGE_OUT = "EXCHANGE_OUT"
    EXCHANGE_IN = "EXCHANGE_IN"
    # Spin-off receipt (KapMaßVO § 4 Abs 2): moves ``basis_fraction`` of the
    # parent pool (``link_id`` = parent ISIN) to the received shares. Only
    # created from an explicit declaration (rules/corporate_actions.yaml).
    SPINOFF_IN = "SPINOFF_IN"
    # Depot transfer legs (§ 27 Abs 6 Z 2 EStG). Tax treatment depends on the
    # Finanzamt notification and must be declared (rules/depot_transfers.yaml).
    MIGRATION_IN = "MIGRATION_IN"
    MIGRATION_OUT = "MIGRATION_OUT"
    DEPOSIT_CASH = "DEPOSIT_CASH"
    WITHDRAWAL_CASH = "WITHDRAWAL_CASH"
    # Non-deductible costs (§ 20 Abs 2 EStG). Kept for audit and for the
    # foreign-currency cash ledger.
    FEE = "FEE"
    # Shares delivered free of charge (no cash leg): income at receipt at
    # market value (``gross_native`` > 0), which is also the basis.
    BONUS_SHARE = "BONUS_SHARE"
    # Exchange of one currency for another at the broker (IBKR IDEALFX /
    # AutoFX). ``currency_native``/``gross_native`` is the quote-currency leg,
    # ``counter_currency``/``counter_amount`` the base-currency leg (signed
    # cash-flow semantics on both).
    FX_CONVERSION = "FX_CONVERSION"
    # Foreign withholding tax that could not be attached to an income row.
    WITHHOLDING_TAX = "WITHHOLDING_TAX"
    IGNORED = "IGNORED"  # Rejected/Cancelled/out-of-scope, kept for audit only


class AssetClass(StrEnum):
    STOCK = "STOCK"
    # Investmentfonds (UCITS/AIF, incl. ETFs) — InvFG 2011 § 186.
    ETF = "ETF"
    BOND = "BOND"
    # Securitised derivatives (Zertifikate, Optionsscheine, ETC/ETN, Faktor-
    # Zertifikate) — § 27 Abs 4 EStG, E1kv KZ 995/896. Non-securitised
    # derivatives (listed options, futures, CFDs at a foreign broker) are
    # tariff-taxed (§ 27a Abs 2 Z 7) and are rejected by the parsers.
    DERIVATIVE = "DERIVATIVE"
    CASH = "CASH"
    CRYPTO = "CRYPTO"
    OTHER = "OTHER"


class FxSource(StrEnum):
    NATIVE_EUR = "NATIVE_EUR"  # transaction was already in EUR
    ECB = "ECB"
    BROKER = "BROKER"  # only for audit; tax math should use ECB
    NONE = "NONE"  # not yet converted


# Flag codes shared by parsers, declarations and the engine.
FLAG_WHT_COUNTRY_UNCERTAIN = "withholding.country_uncertain"  # ADR/GDR etc.
FLAG_WHT_NOT_DISCLOSED = "withholding.not_disclosed"  # export lacks WHT amount
FLAG_ROC_LABEL = "distribution.roc_label_unproven"  # broker says "return of capital"
FLAG_PAYMENT_IN_LIEU = "dividend.payment_in_lieu"  # securities-lending substitute
# Shares added to a held position without a split type (bonus shares,
# stock dividend or split effected as a stock dividend): tax-neutral or
# income depends on the legal form, so it must be declared. ERROR = blocker.
FLAG_CA_UNCLASSIFIED = "corporate_action.unclassified"


# Severity for parser/engine flags.
class Severity(StrEnum):
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

    # Optional pricing/fees (native ccy). ``fee_native`` is a cost (positive;
    # negative for rebates) and is never tax-deductible (§ 20 Abs 2 EStG); it
    # matters only for the foreign-currency cash ledger.
    # ``tax_withheld_native``: foreign tax withheld on income (positive;
    # negative for refunds/reversals).
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

    # For income rows with foreign tax (dividends, interest): does
    # ``gross_native`` already exclude the withholding (NET cash received)?
    # True  = gross_native is NET; engine grosses it up by adding
    #         tax_withheld_native to recover the pre-tax income (§ 27 EStG).
    # False = gross_native is GROSS; tax_withheld_native was already included.
    # None  = convention unknown for a dividend that has a withholding
    #         component — engine raises ReportHealth blocker, because silently
    #         defaulting either way risks under- or double-counting income.
    # The dataclass default is ``None`` to FORCE each parser to declare its
    # convention explicitly; a silent default was masking parser bugs.
    dividend_is_net: bool | None = None

    # Groups the legs of one event (exchange / transfer / spin-off parent).
    link_id: str | None = None
    # FX_CONVERSION only: the other currency leg (signed cash flow).
    counter_currency: str | None = None
    counter_amount: Decimal | None = None
    # ---- In-memory only: set at report time from user declarations in
    # rules/*.yaml (see tax_calc_at.declarations); never persisted.
    # SPINOFF_IN: share of the parent's cost basis moved to the new shares.
    basis_fraction: Decimal | None = None
    # True once a declaration resolved this row's tax treatment.
    declared: bool = False
    # MIGRATION_IN: cost basis carried in (notified transfer whose sending
    # depot is not imported) or market value (deemed sale + step-up).
    declared_cost_eur: Decimal | None = None
    # MIGRATION_OUT without timely notification: deemed sale proceeds
    # (gemeiner Wert, § 27a Abs 3 Z 2 lit b EStG).
    deemed_sale_eur: Decimal | None = None
    # MIGRATION_OUT: cost basis stated in the Finanzamt notification for this
    # leg — only compared against the computed basis, never used.
    notified_cost_eur: Decimal | None = None

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
                self.link_id or "",
                self.counter_currency or "",
                "" if self.counter_amount is None else f"{self.counter_amount:.6f}",
                "" if self.dividend_is_net is None else str(self.dividend_is_net),
                self.withholding_country or "",
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
    """A position without known cost basis (undeclared transfer-in) was sold."""


class CorporateActionError(TaxCalcError):
    """A corporate-action booking cannot be resolved into a tax treatment
    without an explicit declaration (spin-off, new security, unpaired leg)."""


class DepotTransferError(TaxCalcError):
    """A depot transfer has no declaration of its tax treatment
    (§ 27 Abs 6 Z 2 EStG: neutral only with timely Finanzamt notification)."""


class DatabaseVersionError(TaxCalcError):
    """The SQLite store was written by an incompatible parser version."""


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
