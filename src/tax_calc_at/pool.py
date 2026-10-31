"""Per-broker cost-basis pools and foreign-currency cash ledger.

Securities (§ 27a Abs 4 Z 3 EStG): one moving-average pool ("gleitender
Durchschnittspreis") per broker (depot) and ISIN, kept in EUR. Pools are
never merged across brokers; basis only moves between depots through an
explicitly declared depot transfer (see :mod:`tax_calc_at.declarations`).

Fees (§ 20 Abs 2, § 27a Abs 4 Z 2 EStG; EStR Rz 6105a/6106) are never part
of cost basis or proceeds: ``gross_native`` is the trade value before fees.

Events
    BUY               qty += q; cost += |amount_eur|
    SELL              realized = proceeds − q·avg; qty −= q (avg unchanged)
    SPLIT             qty += Δ, cost unchanged (KapMaßVO § 6); requires an
                      existing position that stays positive
    EXCHANGE_OUT/IN   tax-neutral exchange (ISIN change, comparable merger;
                      KapMaßVO §§ 3, 8): the OUT legs' basis moves to the IN
                      leg with the same ``link_id``
    SPINOFF_IN        ``basis_fraction`` of the parent pool (``link_id``)
                      moves to the received shares (KapMaßVO § 4 Abs 2)
    BONUS_SHARE /     shares received as income: basis = market value
    DIVIDEND_STOCK    (= the taxable amount)
    RETURN_OF_CAPITAL cost −= payout; excess over basis is a realized gain
                      (KapMaßVO § 2 Abs 2 Z 1, § 27 Abs 3)
    MIGRATION_OUT/IN  declared depot transfers (§ 27 Abs 6 Z 2): basis moves
                      with the shares when notified (``link_id``), otherwise
                      the service layer has turned the OUT leg into a deemed
                      SELL and the IN leg into an acquisition at market value

Foreign-currency cash (EStR Rz 6201–6201c, Rz 6145c): every non-EUR cash
flow of a broker runs through a per-currency moving-average ledger. Spending
or converting a positive balance realizes an FX gain/loss (valued at the ECB
rate, or at the actual rate when converted into EUR). Negative balances are
liabilities: FX effects on debts are not taxable in the private sphere
(EStR Rz 6201, VwGH 18.12.2017, Ro 2016/15/0026). Converting into another
floating currency moves the basis without realization (Rz 6201).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, time
from decimal import ROUND_HALF_EVEN, Decimal
from zoneinfo import ZoneInfo

from .model import (
    AssetClass,
    CorporateActionError,
    CostBasisMissingError,
    DepotTransferError,
    OversellError,
    TaxCalcError,
    Transaction,
    TxType,
)

ZERO = Decimal("0")
EUR_Q = Decimal("0.0001")
_VIENNA = ZoneInfo("Europe/Vienna")

# Errors that tolerant replay collects per (broker, ISIN) instead of raising.
POOL_ERRORS: tuple[type[TaxCalcError], ...] = (
    OversellError,
    CostBasisMissingError,
    CorporateActionError,
    DepotTransferError,
)


def _q(d: Decimal) -> Decimal:
    return d.quantize(EUR_Q, rounding=ROUND_HALF_EVEN)


def _ref(tx: Transaction) -> str:
    return f"{tx.broker} {tx.source_file}:{tx.source_line}"


@dataclass
class PoolState:
    quantity: Decimal = ZERO
    total_cost_eur: Decimal = ZERO
    cost_basis_known: bool = True

    @property
    def avg_cost_eur(self) -> Decimal:
        if self.quantity == 0:
            return ZERO
        return _q(self.total_cost_eur / self.quantity)


@dataclass
class FxPoolState:
    """Foreign-currency balance of one broker. ``quantity`` ≥ 0 is the asset
    part (with EUR cost), ``liability`` ≥ 0 the borrowed part."""

    quantity: Decimal = ZERO
    total_cost_eur: Decimal = ZERO
    liability: Decimal = ZERO

    @property
    def balance(self) -> Decimal:
        return self.quantity - self.liability


@dataclass
class RealizedEvent:
    """One realization, ready to be classified for E1kv."""

    trade_date: date
    broker: str
    isin: str | None
    symbol: str | None
    name: str | None
    asset_class: AssetClass  # CASH for FX-balance disposals
    quantity: Decimal
    proceeds_eur: Decimal
    cost_basis_eur: Decimal
    pnl_eur: Decimal
    source_ref: str
    kind: str = "sale"  # sale | deemed_sale | roc_excess | fx


@dataclass
class _Parcel:
    """Basis in transit between legs that share a ``link_id``."""

    quantity: Decimal
    cost_eur: Decimal
    cost_basis_known: bool
    source: str
    kind: TxType


@dataclass
class PoolEventLog:
    events: list[str] = field(default_factory=list)

    def log(self, msg: str) -> None:
        self.events.append(msg)


class BrokerPools:
    """Pool state for ONE broker (depot), keyed by ISIN, plus its FX ledger."""

    def __init__(self, broker: str, manager: PoolManager) -> None:
        self.broker = broker
        self.manager = manager
        self.by_isin: dict[str, PoolState] = {}
        self.fx: dict[str, FxPoolState] = {}
        self.realized: list[RealizedEvent] = []
        self.log = PoolEventLog()

    # ----------------------------------------------------------- dispatch
    def apply(self, tx: Transaction) -> None:
        if tx.amount_eur is None:
            raise RuntimeError(f"Transaction {_ref(tx)} not converted to EUR")
        handler = {
            TxType.BUY: self._apply_buy,
            TxType.SELL: self._apply_sell,
            TxType.SPLIT: self._apply_split,
            TxType.EXCHANGE_OUT: self._apply_exchange_out,
            TxType.EXCHANGE_IN: self._apply_exchange_in,
            TxType.SPINOFF_IN: self._apply_spinoff_in,
            TxType.BONUS_SHARE: self._apply_income_shares,
            TxType.DIVIDEND_STOCK: self._apply_income_shares,
            TxType.RETURN_OF_CAPITAL: self._apply_return_of_capital,
            TxType.MIGRATION_IN: self._apply_migration_in,
            TxType.MIGRATION_OUT: self._apply_migration_out,
        }.get(tx.tx_type)
        if handler is not None:
            handler(tx)
        self._apply_cash_flows(tx)

    def _key(self, tx: Transaction) -> str:
        if not tx.isin:
            raise ValueError(
                f"Transaction without ISIN cannot be pooled: {_ref(tx)} type={tx.tx_type}"
            )
        return tx.isin

    # ------------------------------------------------------- securities
    def _apply_buy(self, tx: Transaction) -> None:
        key = self._key(tx)
        state = self.by_isin.setdefault(key, PoolState())
        cost = abs(tx.amount_eur or ZERO)
        state.quantity += tx.quantity
        state.total_cost_eur += cost
        self.log.log(
            f"{tx.trade_date} BUY {tx.quantity} {key} +cost {_q(cost)} EUR "
            f"→ qty={state.quantity} avg={state.avg_cost_eur}"
        )

    def _apply_sell(self, tx: Transaction) -> None:
        self._dispose(tx, tx.amount_eur or ZERO, "sale")

    def _dispose(self, tx: Transaction, proceeds: Decimal, kind: str) -> None:
        key = self._key(tx)
        state = self.by_isin.get(key)
        if state is None or state.quantity < tx.quantity:
            raise OversellError(
                f"Oversell: SELL {tx.quantity} {key} on {tx.trade_date} but pool has "
                f"{state.quantity if state else 0}. Source: {_ref(tx)}. Likely cause: "
                f"missing earlier import or an undeclared transfer/corporate action."
            )
        if not state.cost_basis_known:
            raise CostBasisMissingError(
                f"Pool {self.broker}/{key} has no known cost basis (undeclared "
                f"transfer-in). Declare it in rules/depot_transfers.yaml before "
                f"selling. Source: {_ref(tx)}."
            )
        cost_unq = state.total_cost_eur * tx.quantity / state.quantity
        pnl = proceeds - cost_unq
        state.total_cost_eur -= cost_unq
        state.quantity -= tx.quantity
        if state.quantity == 0:
            state.total_cost_eur = ZERO
        self.log.log(
            f"{tx.trade_date} {'DEEMED SALE' if kind == 'deemed_sale' else 'SELL'} "
            f"{tx.quantity} {key} proceeds={_q(proceeds)} cost={_q(cost_unq)} "
            f"pnl={_q(pnl)} → qty={state.quantity} avg={state.avg_cost_eur}"
        )
        self._realize(tx, key, tx.quantity, proceeds, cost_unq, kind)

    def _realize(
        self,
        tx: Transaction,
        isin: str | None,
        qty: Decimal,
        proceeds: Decimal,
        cost: Decimal,
        kind: str,
        asset_class: AssetClass | None = None,
    ) -> None:
        self.realized.append(
            RealizedEvent(
                trade_date=tx.trade_date,
                broker=self.broker,
                isin=isin,
                symbol=tx.symbol,
                name=tx.name,
                asset_class=asset_class or tx.asset_class,
                quantity=qty,
                proceeds_eur=_q(proceeds),
                cost_basis_eur=_q(cost),
                pnl_eur=_q(proceeds) - _q(cost),
                source_ref=f"{tx.source_file}:{tx.source_line}",
                kind=kind,
            )
        )

    def _apply_split(self, tx: Transaction) -> None:
        key = self._key(tx)
        state = self.by_isin.get(key)
        if state is None or state.quantity <= 0:
            raise CorporateActionError(
                f"Corporate action adds {tx.quantity} {key} on {tx.trade_date} "
                f"but {self.broker} holds no position in it: this is not a split "
                f"(spin-off, stock distribution of another security or an "
                f"unpaired exchange leg). Declare its treatment in "
                f"rules/corporate_actions.yaml. Source: {_ref(tx)}."
            )
        new_qty = state.quantity + tx.quantity
        if new_qty <= 0:
            raise CorporateActionError(
                f"Corporate action removes {-tx.quantity} of {state.quantity} "
                f"{key} on {tx.trade_date}: a split cannot eliminate a position "
                f"(unpaired exchange/merger leg, delisting or cash merger). "
                f"Declare its treatment in rules/corporate_actions.yaml. "
                f"Source: {_ref(tx)}."
            )
        state.quantity = new_qty
        self.log.log(
            f"{tx.trade_date} SPLIT {tx.quantity:+} {key} → qty={state.quantity} "
            f"avg={state.avg_cost_eur}"
        )

    def _take_out(self, tx: Transaction, verb: str) -> _Parcel:
        key = self._key(tx)
        state = self.by_isin.get(key)
        if state is None or state.quantity < tx.quantity or tx.quantity <= 0:
            raise OversellError(
                f"{verb} {tx.quantity} {key} on {tx.trade_date} but pool has "
                f"{state.quantity if state else 0}. Source: {_ref(tx)}."
            )
        cost = state.total_cost_eur * tx.quantity / state.quantity
        state.total_cost_eur -= cost
        state.quantity -= tx.quantity
        if state.quantity == 0:
            state.total_cost_eur = ZERO
        self.log.log(
            f"{tx.trade_date} {verb} {tx.quantity} {key} basis {_q(cost)} EUR "
            f"→ qty={state.quantity} avg={state.avg_cost_eur}"
        )
        return _Parcel(tx.quantity, cost, state.cost_basis_known, _ref(tx), tx.tx_type)

    def _put_in(self, tx: Transaction, parcel: _Parcel, verb: str) -> None:
        key = self._key(tx)
        state = self.by_isin.setdefault(key, PoolState())
        state.quantity += tx.quantity
        state.total_cost_eur += parcel.cost_eur
        state.cost_basis_known = state.cost_basis_known and parcel.cost_basis_known
        self.log.log(
            f"{tx.trade_date} {verb} {tx.quantity} {key} basis {_q(parcel.cost_eur)} EUR "
            f"(from {parcel.source}) → qty={state.quantity} avg={state.avg_cost_eur}"
        )

    def _apply_exchange_out(self, tx: Transaction) -> None:
        if not tx.link_id:
            raise CorporateActionError(f"EXCHANGE_OUT without link_id: {_ref(tx)}")
        self.manager.deposit_parcel(tx.link_id, self._take_out(tx, "EXCHANGE_OUT"))

    def _apply_exchange_in(self, tx: Transaction) -> None:
        if not tx.link_id:
            raise CorporateActionError(f"EXCHANGE_IN without link_id: {_ref(tx)}")
        parcel = self.manager.withdraw_parcel(tx.link_id, tx)
        self._put_in(tx, parcel, "EXCHANGE_IN")

    def _apply_spinoff_in(self, tx: Transaction) -> None:
        parent = tx.link_id
        frac = tx.basis_fraction
        if not parent or frac is None or not ZERO <= frac < 1:
            raise CorporateActionError(
                f"SPINOFF_IN {_ref(tx)} needs parent ISIN and basis_fraction in [0, 1)."
            )
        pstate = self.by_isin.get(parent)
        if pstate is None or pstate.quantity <= 0:
            raise CorporateActionError(
                f"Spin-off {tx.isin} on {tx.trade_date}: parent {parent} not held "
                f"at {self.broker}. Source: {_ref(tx)}."
            )
        moved = pstate.total_cost_eur * frac
        pstate.total_cost_eur -= moved
        parcel = _Parcel(tx.quantity, moved, pstate.cost_basis_known, f"spin-off from {parent}",
                         tx.tx_type)
        self._put_in(tx, parcel, f"SPINOFF_IN ({frac} of {parent})")

    def _apply_income_shares(self, tx: Transaction) -> None:
        key = self._key(tx)
        value = abs(tx.amount_eur or ZERO)
        if value == 0 or tx.quantity <= 0:
            raise CorporateActionError(
                f"{tx.tx_type.value} {_ref(tx)} needs a positive quantity and "
                f"market value (the taxable amount is also the cost basis)."
            )
        state = self.by_isin.setdefault(key, PoolState())
        state.quantity += tx.quantity
        state.total_cost_eur += value
        self.log.log(
            f"{tx.trade_date} {tx.tx_type.value} +{tx.quantity} {key} at market "
            f"value {_q(value)} EUR → qty={state.quantity} avg={state.avg_cost_eur}"
        )

    def _apply_return_of_capital(self, tx: Transaction) -> None:
        key = self._key(tx)
        state = self.by_isin.get(key)
        payout = abs(tx.amount_eur or ZERO)
        if tx.dividend_is_net:
            payout += abs(tx.tax_withheld_eur or ZERO)
        if state is None or state.quantity == 0:
            # No position (sold before payment): no basis left to reduce.
            self.log.log(
                f"{tx.trade_date} RETURN_OF_CAPITAL {key} payout={_q(payout)} "
                f"without position → realized gain."
            )
            if payout > 0:
                self._realize(tx, key, ZERO, payout, ZERO, "roc_excess")
            return
        if payout <= state.total_cost_eur:
            state.total_cost_eur -= payout
            self.log.log(
                f"{tx.trade_date} RETURN_OF_CAPITAL {key} payout={_q(payout)} "
                f"→ total_cost={_q(state.total_cost_eur)} avg={state.avg_cost_eur}"
            )
            return
        excess = payout - state.total_cost_eur
        self.log.log(
            f"{tx.trade_date} RETURN_OF_CAPITAL {key} payout={_q(payout)} exceeds "
            f"basis {_q(state.total_cost_eur)} by {_q(excess)} → basis 0, excess realized."
        )
        state.total_cost_eur = ZERO
        self._realize(tx, key, ZERO, excess, ZERO, "roc_excess")

    def _apply_migration_out(self, tx: Transaction) -> None:
        if not tx.declared:
            raise DepotTransferError(
                f"Depot transfer out of {self.broker}: {tx.quantity} {tx.isin} on "
                f"{tx.trade_date} has no declaration. A transfer out of a foreign "
                f"depot is a deemed sale (§ 27 Abs 6 Z 2 EStG) unless the "
                f"Finanzamt was notified within one month. Declare it in "
                f"rules/depot_transfers.yaml. Source: {_ref(tx)}."
            )
        if tx.deemed_sale_eur is not None:
            # Not notified in time: deemed sale at market value (Rz 6166a).
            self._dispose(tx, tx.deemed_sale_eur, "deemed_sale")
            return
        parcel = self._take_out(tx, "TRANSFER_OUT")
        stated = tx.notified_cost_eur
        if stated is not None and abs(parcel.cost_eur - stated) >= Decimal("0.01"):
            self.manager.basis_mismatches.append(
                (
                    tx.trade_date,
                    f"Depot transfer {self.broker} → {tx.isin} on {tx.trade_date}: the "
                    f"Austrian cost basis {_q(parcel.cost_eur)} EUR ({tx.quantity} units) "
                    f"is carried over; the Finanzamt notification stated "
                    f"{_q(stated)} EUR. All later sales use the computed "
                    f"basis — correct the receiving broker's records.",
                )
            )
        if tx.link_id:
            self.manager.deposit_parcel(tx.link_id, parcel)

    def _apply_migration_in(self, tx: Transaction) -> None:
        key = self._key(tx)
        if tx.link_id:
            parcel = self.manager.withdraw_parcel(tx.link_id, tx)
            self._put_in(tx, parcel, "TRANSFER_IN")
            return
        if tx.declared_cost_eur is not None:
            parcel = _Parcel(tx.quantity, tx.declared_cost_eur, True, "declaration", tx.tx_type)
            self._put_in(tx, parcel, "TRANSFER_IN (declared basis)")
            return
        state = self.by_isin.setdefault(key, PoolState())
        state.quantity += tx.quantity
        state.cost_basis_known = False
        self.log.log(f"{tx.trade_date} TRANSFER_IN {tx.quantity} {key} (cost basis UNKNOWN)")

    # ------------------------------------------------------ FX ledger
    def _apply_cash_flows(self, tx: Transaction) -> None:
        for ccy, amount, eur_value, actual in _cash_flows(tx):
            if ccy == "EUR" or amount == 0:
                continue
            if amount > 0:
                self._fx_inflow(tx, ccy, amount, eur_value)
            else:
                self._fx_outflow(tx, ccy, -amount, eur_value, actual)
        if tx.tx_type is TxType.FX_CONVERSION:
            self._fx_conversion(tx)

    def _fx_inflow(self, tx: Transaction, ccy: str, units: Decimal, eur_value: Decimal) -> None:
        st = self.fx.setdefault(ccy, FxPoolState())
        repay = min(units, st.liability)
        st.liability -= repay
        rest = units - repay
        if rest > 0:
            st.quantity += rest
            st.total_cost_eur += eur_value * rest / units

    def _fx_outflow(
        self, tx: Transaction, ccy: str, units: Decimal, eur_value: Decimal, actual: bool
    ) -> None:
        st = self.fx.setdefault(ccy, FxPoolState())
        disposed = min(units, st.quantity)
        if disposed > 0:
            cost = st.total_cost_eur * disposed / st.quantity
            proceeds = eur_value * disposed / units
            st.total_cost_eur -= cost
            st.quantity -= disposed
            if st.quantity == 0:
                st.total_cost_eur = ZERO
            if _q(proceeds) != _q(cost):
                self.realized.append(
                    RealizedEvent(
                        trade_date=tx.trade_date,
                        broker=self.broker,
                        isin=None,
                        symbol=ccy,
                        name=f"Fremdwährungsguthaben {ccy}"
                        + (" (Konvertierung, tatsächlicher Kurs)" if actual else ""),
                        asset_class=AssetClass.CASH,
                        quantity=disposed,
                        proceeds_eur=_q(proceeds),
                        cost_basis_eur=_q(cost),
                        pnl_eur=_q(proceeds) - _q(cost),
                        source_ref=f"{tx.source_file}:{tx.source_line}",
                        kind="fx",
                    )
                )
        st.liability += units - disposed

    def _fx_conversion(self, tx: Transaction) -> None:
        """FX→FX conversion between two non-EUR currencies."""
        a_ccy, a_amt = tx.currency_native.upper(), tx.gross_native
        b_ccy = (tx.counter_currency or "").upper()
        b_amt = tx.counter_amount or ZERO
        if "EUR" in (a_ccy, b_ccy):
            return  # handled by _cash_flows with the actual EUR leg
        if (a_amt > 0) == (b_amt > 0):
            raise ValueError(f"FX_CONVERSION legs must have opposite signs: {_ref(tx)}")
        out_ccy, out_units, in_ccy, in_units = (
            (a_ccy, -a_amt, b_ccy, b_amt) if a_amt < 0 else (b_ccy, -b_amt, a_ccy, a_amt)
        )
        ecb_value = abs(tx.amount_eur or ZERO)  # ECB value of the quote leg
        stable = in_ccy in self.manager.eur_stable_currencies
        src = self.fx.setdefault(out_ccy, FxPoolState())
        disposed = min(out_units, src.quantity)
        basis_moved = ZERO
        if disposed > 0:
            basis_moved = src.total_cost_eur * disposed / src.quantity
        if stable:
            # Taxable exchange (Rz 6201): dispose at market value.
            self._fx_outflow(tx, out_ccy, out_units, ecb_value, False)
            self._fx_inflow(tx, in_ccy, in_units, ecb_value)
            return
        # Floating → floating: same asset "Fremdwährung", basis carries over.
        src.total_cost_eur -= basis_moved
        src.quantity -= disposed
        if src.quantity == 0:
            src.total_cost_eur = ZERO
        src.liability += out_units - disposed
        funded = ecb_value * (out_units - disposed) / out_units
        self._fx_inflow(tx, in_ccy, in_units, basis_moved + funded)


def _cash_flows(tx: Transaction) -> list[tuple[str, Decimal, Decimal, bool]]:
    """(currency, signed units, EUR value of |units|, actual-rate?) per leg.

    EUR values use the transaction's ECB rate; legs of a conversion into or
    out of EUR are valued at the actual EUR amount (EStR Rz 6201c).
    """
    ccy = tx.currency_native.upper()
    rate = tx.fx_rate_used or ZERO
    t = tx.tx_type
    if t is TxType.FX_CONVERSION:
        cc = (tx.counter_currency or "").upper()
        ca = tx.counter_amount or ZERO
        if cc == "EUR" and ccy != "EUR":
            return [(ccy, tx.gross_native, abs(ca), True)]
        if ccy == "EUR" and cc != "EUR":
            return [(cc, ca, abs(tx.gross_native), True)]
        return []  # FX→FX handled separately
    if ccy == "EUR" or t in _NO_CASH_TYPES:
        return []
    flows: list[tuple[str, Decimal, Decimal, bool]] = []
    gross = tx.gross_native
    flows.append((ccy, gross, abs(gross) * rate, False))
    if (
        t is not TxType.WITHHOLDING_TAX
        and tx.dividend_is_net is False
        and tx.tax_withheld_native
    ):
        wh = tx.tax_withheld_native  # gross row: the tax leaves the account
        flows.append((ccy, -wh, abs(wh) * rate, False))
    if tx.fee_native:
        fee = tx.fee_native  # cost > 0 leaves the account; rebate < 0 comes in
        flows.append((ccy, -fee, abs(fee) * rate, False))
    return flows


_NO_CASH_TYPES = frozenset(
    {
        TxType.SPLIT,
        TxType.EXCHANGE_OUT,
        TxType.EXCHANGE_IN,
        TxType.SPINOFF_IN,
        TxType.BONUS_SHARE,
        TxType.DIVIDEND_STOCK,
        TxType.MIGRATION_IN,
        TxType.MIGRATION_OUT,
        TxType.IGNORED,
    }
)

# Same-timestamp ordering: legs that release basis before legs that take it.
_ORDER = {
    TxType.EXCHANGE_OUT: 0,
    TxType.MIGRATION_OUT: 0,
    TxType.EXCHANGE_IN: 3,
    TxType.MIGRATION_IN: 3,
}


def _order(tx: Transaction) -> int:
    """Conversions bringing foreign currency in run before the purchase they
    fund; conversions taking it out run after the sale/dividend that
    produced it."""
    if tx.tx_type is TxType.FX_CONVERSION:
        foreign = (
            tx.gross_native
            if tx.currency_native.upper() != "EUR"
            else (tx.counter_amount or ZERO)
        )
        return 1 if foreign > 0 else 3
    return _ORDER.get(tx.tx_type, 2)


class PoolManager:
    """Holds one BrokerPools per broker. Replays sorted transactions."""

    def __init__(self, *, eur_stable_currencies: frozenset[str] = frozenset()) -> None:
        self.by_broker: dict[str, BrokerPools] = {}
        self.eur_stable_currencies = eur_stable_currencies
        # Tolerant replay: (broker, isin, message, date); the ISIN is excluded
        # from that date on. Results realized before the error stay valid.
        self.errors: list[tuple[str, str, str, date]] = []
        self._excluded: set[tuple[str, str]] = set()
        self._parcels: dict[str, _Parcel] = {}
        # (date, message): notified transfer basis differs from computed basis.
        self.basis_mismatches: list[tuple[date, str]] = []

    def deposit_parcel(self, link_id: str, parcel: _Parcel) -> None:
        existing = self._parcels.get(link_id)
        if existing is not None:
            existing.quantity += parcel.quantity
            existing.cost_eur += parcel.cost_eur
            existing.cost_basis_known &= parcel.cost_basis_known
            return
        self._parcels[link_id] = parcel

    def withdraw_parcel(self, link_id: str, tx: Transaction) -> _Parcel:
        parcel = self._parcels.pop(link_id, None)
        if parcel is None:
            raise CorporateActionError(
                f"{tx.tx_type.value} {tx.quantity} {tx.isin} ({_ref(tx)}) has no "
                f"matching outgoing leg with link {link_id!r}; its cost basis "
                f"cannot be determined."
            )
        return parcel

    def replay(self, txns: list[Transaction], *, on_error: str = "raise") -> None:
        """Replay transactions in deterministic order.

        on_error="raise"   : first pool error aborts.
        on_error="collect" : record the error against (broker, isin), exclude
                             that ISIN from further events, continue.
        """
        if on_error not in {"raise", "collect"}:
            raise ValueError(f"on_error must be 'raise' or 'collect', got {on_error!r}")

        def _sort_key(t: Transaction) -> tuple[date, time, int, str, int]:
            # Intra-day order by local (Vienna) wall-clock time; tz-naive
            # times are taken as broker-local.
            if t.trade_datetime is not None:
                dt = t.trade_datetime
                if dt.tzinfo is not None:
                    dt = dt.astimezone(_VIENNA)
                tod = dt.time().replace(tzinfo=None)
            else:
                tod = time(0, 0)
            return (t.trade_date, tod, _order(t), t.source_file, t.source_line)

        for tx in sorted(txns, key=_sort_key):
            key = (tx.broker, tx.isin or "")
            if on_error == "collect" and key in self._excluded:
                continue
            pools = self.by_broker.setdefault(tx.broker, BrokerPools(tx.broker, self))
            try:
                pools.apply(tx)
            except POOL_ERRORS as e:
                if on_error == "raise":
                    raise
                self.errors.append((tx.broker, tx.isin or "", str(e), tx.trade_date))
                self._excluded.add(key)
                if tx.isin and tx.isin in pools.by_isin:
                    pools.by_isin[tx.isin] = PoolState()
        for link_id, parcel in sorted(self._parcels.items()):
            msg = (
                f"Outgoing {parcel.kind.value} leg {parcel.source} (link {link_id!r}, "
                f"{parcel.quantity} units, basis {_q(parcel.cost_eur)} EUR) was never "
                f"matched by an incoming leg: its cost basis would be lost."
            )
            if on_error == "raise":
                raise CorporateActionError(msg)
            self.errors.append(("-", link_id, msg, date.max))
        self._parcels.clear()

    def realized_events(self) -> list[RealizedEvent]:
        out: list[RealizedEvent] = []
        for p in self.by_broker.values():
            out.extend(p.realized)
        return out
