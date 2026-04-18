"""Per-broker, per-ISIN moving-average cost-basis pools.

Implements the Austrian "gleitender Durchschnittspreis" rule (§ 27a Abs. 4
EStG) for non-steuereinfache foreign brokers. Pools are kept STRICTLY per
broker and never merged, since cost basis can only follow the depot.

Fee policy (§ 20 Abs. 2 + § 27a Abs. 4 Z 2 EStG):
    For private investors at the Sondersteuersatz (25 % / 27,5 %) transaction
    fees (Anschaffungs-/Veräußerungsnebenkosten) are NOT deductible. The pool
    therefore uses ``gross_native`` (shares × price) only and never folds
    ``fee_native`` into cost basis or proceeds. Parsers must guarantee that
    ``gross_native`` is the raw trade value before broker fees — any fees
    stay on their own FEE rows for audit.

Behaviour:
    BUY              quantity += q;  total_cost_eur += |amount_eur|
                     (fees excluded — see fee policy above)
    SELL             realized = q*sale_price_eur - q*avg_cost_eur
                     (fees excluded — see fee policy above)
                     quantity -= q   (avg unchanged; rolling-average rule)
    SPLIT            quantity += q (signed delta; total_cost preserved so avg
                     updates correctly for forward- and reverse-splits)
    BONUS_SHARE      quantity += q   (zero cost; avg drops accordingly)
    RETURN_OF_CAPITAL
                     Reduces total_cost_eur by the EUR amount paid out
                     (Einlagenrückzahlung per § 4 Abs. 12 EStG). If the
                     distribution exceeds remaining basis, the excess is
                     emitted as a synthetic realized capital gain per
                     § 27 Abs. 5 Z 1 EStG so it still lands in the 27,5 %
                     basket — never silently discarded.
    MIGRATION_OUT    quantity := 0   (warning emitted)
    MIGRATION_IN     requires explicit cost basis (else CostBasisMissing on
                     subsequent SELL)

A full event log is kept, so the audit page can replay every change.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal, ROUND_HALF_EVEN

from .model import (
    AssetClass,
    CostBasisMissingError,
    OversellError,
    Transaction,
    TxType,
)

ZERO = Decimal("0")
EUR_Q = Decimal("0.0001")


def _q(d: Decimal) -> Decimal:
    return d.quantize(EUR_Q, rounding=ROUND_HALF_EVEN)


@dataclass
class PoolState:
    quantity: Decimal = ZERO
    total_cost_eur: Decimal = ZERO
    cost_basis_known: bool = True  # False if pool was opened by a MIGRATION_IN

    @property
    def avg_cost_eur(self) -> Decimal:
        if self.quantity == 0:
            return ZERO
        return _q(self.total_cost_eur / self.quantity)


@dataclass
class RealizedEvent:
    """One sell-side realization, ready to be classified for E1kv."""

    trade_date: date
    broker: str
    isin: str | None
    symbol: str | None
    name: str | None
    asset_class: "AssetClass"   # carried from the SELL Transaction
    quantity: Decimal
    proceeds_eur: Decimal       # net of fees on the sell side
    cost_basis_eur: Decimal
    pnl_eur: Decimal
    source_ref: str             # for traceability back to the SELL Transaction


@dataclass
class PoolEventLog:
    events: list[str] = field(default_factory=list)

    def log(self, msg: str) -> None:
        self.events.append(msg)


class BrokerPools:
    """Pool state for ONE broker, keyed by ISIN."""

    def __init__(self, broker: str) -> None:
        self.broker = broker
        self.by_isin: dict[str, PoolState] = {}
        self.realized: list[RealizedEvent] = []
        self.log = PoolEventLog()

    def _key(self, tx: Transaction) -> str:
        if not tx.isin:
            raise ValueError(
                f"Transaction without ISIN cannot be pooled: "
                f"{tx.broker} {tx.source_file}:{tx.source_line} type={tx.tx_type}"
            )
        return tx.isin

    def apply(self, tx: Transaction) -> None:
        if tx.amount_eur is None:
            raise RuntimeError(
                f"Transaction {tx.broker} {tx.source_file}:{tx.source_line} not converted to EUR"
            )

        if tx.tx_type is TxType.BUY:
            self._apply_buy(tx)
        elif tx.tx_type is TxType.SELL:
            self._apply_sell(tx)
        elif tx.tx_type is TxType.SPLIT:
            self._apply_split(tx)
        elif tx.tx_type is TxType.BONUS_SHARE:
            self._apply_bonus(tx)
        elif tx.tx_type is TxType.RETURN_OF_CAPITAL:
            self._apply_return_of_capital(tx)
        elif tx.tx_type is TxType.MIGRATION_IN:
            self._apply_migration_in(tx)
        elif tx.tx_type is TxType.MIGRATION_OUT:
            self._apply_migration_out(tx)
        # Other tx types don't affect pools.

    # --------------------------------------------------------- handlers
    def _apply_buy(self, tx: Transaction) -> None:
        key = self._key(tx)
        state = self.by_isin.setdefault(key, PoolState())
        # gross_native is negative for buys; cost is its absolute value.
        # Fees are NOT added: § 20 Abs. 2 + § 27a Abs. 4 Z 2 EStG forbid
        # Werbungskostenabzug at the Sondersteuersatz.
        cost_gross = abs(tx.amount_eur or ZERO)
        state.quantity += tx.quantity
        state.total_cost_eur += cost_gross
        self.log.log(
            f"{tx.trade_date} BUY {tx.quantity} {key} "
            f"+cost {_q(cost_gross)} EUR  →  qty={state.quantity} avg={state.avg_cost_eur}"
        )

    def _apply_sell(self, tx: Transaction) -> None:
        key = self._key(tx)
        state = self.by_isin.get(key)
        if state is None or state.quantity < tx.quantity:
            raise OversellError(
                f"Oversell: SELL {tx.quantity} {key} on {tx.trade_date} but pool has "
                f"{state.quantity if state else 0}. "
                f"Source: {tx.broker} {tx.source_file}:{tx.source_line}. "
                f"Likely cause: missing earlier import or broker migration without cost basis."
            )
        if not state.cost_basis_known:
            raise CostBasisMissingError(
                f"Pool {self.broker}/{key} has no cost basis "
                f"(opened by MIGRATION_IN). Provide cost basis before selling. "
                f"Source: {tx.broker} {tx.source_file}:{tx.source_line}."
            )
        avg = state.avg_cost_eur
        cost_basis_unq = avg * tx.quantity
        cost_basis = cost_basis_unq.quantize(EUR_Q, rounding=ROUND_HALF_EVEN)
        # Proceeds: gross is positive. Fees NOT subtracted (§ 20 Abs. 2 +
        # § 27a Abs. 4 Z 2 EStG).
        proceeds = tx.amount_eur or ZERO
        pnl = proceeds - cost_basis
        # Reduce state by the exact unquantized basis so the pool is
        # arithmetically self-consistent (no residual drift after many
        # partial sells).
        state.total_cost_eur -= cost_basis_unq
        state.quantity -= tx.quantity
        if state.quantity == 0:
            state.total_cost_eur = ZERO
        self.log.log(
            f"{tx.trade_date} SELL {tx.quantity} {key} "
            f"proceeds={_q(proceeds)} cost={_q(cost_basis)} pnl={_q(pnl)} "
            f"→ qty={state.quantity} avg={state.avg_cost_eur}"
        )
        self.realized.append(
            RealizedEvent(
                trade_date=tx.trade_date,
                broker=tx.broker,
                isin=key,
                symbol=tx.symbol,
                name=tx.name,
                asset_class=tx.asset_class,
                quantity=tx.quantity,
                proceeds_eur=_q(proceeds),
                cost_basis_eur=_q(cost_basis),
                pnl_eur=_q(pnl),
                source_ref=f"{tx.source_file}:{tx.source_line}",
            )
        )

    def _apply_split(self, tx: Transaction) -> None:
        # ``tx.quantity`` is interpreted as an additive delta of shares
        # (broker-style). Positive for forward splits; negative for reverse
        # splits (e.g. 10:1 reverse on 100 shares would emit quantity=-90).
        # Total cost is preserved so the average moves inversely with qty.
        key = self._key(tx)
        state = self.by_isin.setdefault(key, PoolState())
        new_qty = state.quantity + tx.quantity
        if new_qty < 0:
            raise OversellError(
                f"SPLIT would drive {self.broker}/{key} quantity negative "
                f"({state.quantity} + {tx.quantity} = {new_qty}). "
                f"Source: {tx.broker} {tx.source_file}:{tx.source_line}."
            )
        state.quantity = new_qty
        if state.quantity == 0:
            # Reverse-split collapsing the entire pool: keep total_cost for
            # the pending SELL but wipe on next buy.
            self.log.log(
                f"{tx.trade_date} SPLIT {tx.quantity} {key} collapsed pool "
                f"to 0 qty; retaining total_cost={_q(state.total_cost_eur)}."
            )
        else:
            self.log.log(
                f"{tx.trade_date} SPLIT {tx.quantity:+} {key} → "
                f"qty={state.quantity} avg={state.avg_cost_eur}"
            )

    def _apply_bonus(self, tx: Transaction) -> None:
        key = self._key(tx)
        state = self.by_isin.setdefault(key, PoolState())
        state.quantity += tx.quantity  # zero cost contribution
        self.log.log(
            f"{tx.trade_date} BONUS +{tx.quantity} {key} → qty={state.quantity} avg={state.avg_cost_eur}"
        )

    def _apply_return_of_capital(self, tx: Transaction) -> None:
        """Reduce cost basis by an Einlagenrückzahlung payout (§ 4 Abs. 12).

        If the payout exceeds remaining cost basis, the excess becomes a
        realized capital gain per § 27 Abs. 5 Z 1 EStG and is emitted as a
        :class:`RealizedEvent` so it lands in the 27,5 %-Wertsteigerungs-
        bucket — never silently dropped.
        """
        key = self._key(tx)
        state = self.by_isin.get(key)
        # gross_native is positive for a cash inflow (payout received).
        payout = abs(tx.amount_eur or ZERO)
        if state is None or state.quantity == 0:
            # Broker reported a RoC with no open position — surface as an
            # immediate realized gain with zero cost so the user sees it.
            self.log.log(
                f"{tx.trade_date} RETURN_OF_CAPITAL {key} payout={_q(payout)} "
                f"but pool empty → emitting as realized gain."
            )
            if payout > 0:
                self.realized.append(
                    RealizedEvent(
                        trade_date=tx.trade_date,
                        broker=tx.broker,
                        isin=key,
                        symbol=tx.symbol,
                        name=tx.name,
                        asset_class=tx.asset_class,
                        quantity=ZERO,
                        proceeds_eur=_q(payout),
                        cost_basis_eur=ZERO,
                        pnl_eur=_q(payout),
                        source_ref=f"{tx.source_file}:{tx.source_line}"
                        + "::roc_no_basis",
                    )
                )
            return
        if payout <= state.total_cost_eur:
            state.total_cost_eur -= payout
            self.log.log(
                f"{tx.trade_date} RETURN_OF_CAPITAL {key} payout={_q(payout)} "
                f"→ total_cost={_q(state.total_cost_eur)} avg={state.avg_cost_eur}"
            )
            return
        # Excess over remaining basis: basis → 0, excess is realized gain.
        excess = payout - state.total_cost_eur
        self.log.log(
            f"{tx.trade_date} RETURN_OF_CAPITAL {key} payout={_q(payout)} exceeds "
            f"basis {_q(state.total_cost_eur)} by {_q(excess)} → basis=0, "
            f"excess emitted as realized gain."
        )
        state.total_cost_eur = ZERO
        self.realized.append(
            RealizedEvent(
                trade_date=tx.trade_date,
                broker=tx.broker,
                isin=key,
                symbol=tx.symbol,
                name=tx.name,
                asset_class=tx.asset_class,
                quantity=ZERO,
                proceeds_eur=_q(excess),
                cost_basis_eur=ZERO,
                pnl_eur=_q(excess),
                source_ref=f"{tx.source_file}:{tx.source_line}::roc_excess",
            )
        )

    def _apply_migration_in(self, tx: Transaction) -> None:
        key = self._key(tx)
        state = self.by_isin.setdefault(key, PoolState())
        state.quantity += tx.quantity
        # Cost basis is unknown until user supplies it. Sells on this pool
        # will raise CostBasisMissingError until resolved.
        state.cost_basis_known = False
        self.log.log(
            f"{tx.trade_date} MIGRATION_IN {tx.quantity} {key} (cost basis UNKNOWN)"
        )

    def _apply_migration_out(self, tx: Transaction) -> None:
        key = self._key(tx)
        state = self.by_isin.get(key)
        if state is None:
            return
        out_qty = tx.quantity
        # Partial-transfer support: reduce quantity and total_cost_eur
        # proportionally so the remaining shares keep their original avg cost.
        # Previous behaviour zeroed the pool unconditionally, which silently
        # wiped basis for the shares that stayed when a user moved only part
        # of a position externally.
        if out_qty >= state.quantity or state.quantity == ZERO:
            self.log.log(
                f"{tx.trade_date} MIGRATION_OUT {out_qty} {key} (pool zeroed; "
                f"transfer cost basis to receiving broker manually)"
            )
            state.quantity = ZERO
            state.total_cost_eur = ZERO
            return
        ratio = out_qty / state.quantity
        cost_removed = (state.total_cost_eur * ratio).quantize(
            EUR_Q, rounding=ROUND_HALF_EVEN
        )
        state.total_cost_eur -= cost_removed
        state.quantity -= out_qty
        self.log.log(
            f"{tx.trade_date} MIGRATION_OUT {out_qty} {key} (partial; "
            f"cost basis reduced by {_q(cost_removed)} EUR, "
            f"remaining qty={state.quantity} avg={state.avg_cost_eur})"
        )


class PoolManager:
    """Holds one BrokerPools per broker. Replays sorted transactions."""

    def __init__(self) -> None:
        self.by_broker: dict[str, BrokerPools] = {}
        # When replay is run in tolerant mode, per-(broker, isin) errors are
        # collected here and the offending ISIN is excluded from further
        # processing (no realized events emitted for it).
        self.errors: list[tuple[str, str, str]] = []  # (broker, isin, message)
        self._excluded: set[tuple[str, str]] = set()

    def replay(self, txns: list[Transaction], *, on_error: str = "raise") -> None:
        """Replay transactions in deterministic order.

        on_error="raise"   : first OversellError / CostBasisMissingError aborts.
        on_error="collect" : record the error against (broker, isin), exclude
                             that ISIN from further events, continue replaying.
        """
        if on_error not in {"raise", "collect"}:
            raise ValueError(f"on_error must be 'raise' or 'collect', got {on_error!r}")

        def _sort_key(t: Transaction) -> tuple:
            # Mixing rows that have a ``trade_datetime`` with rows that don't
            # must not break sorting: Python cannot compare ``datetime`` with
            # ``date``. Normalize to (date, intra-day time) — tz-aware times
            # are stripped of tzinfo to stay comparable across brokers that
            # emit different formats.
            if t.trade_datetime is not None:
                tod = t.trade_datetime.time().replace(tzinfo=None)
            else:
                tod = time(0, 0)
            return (t.trade_date, tod, t.source_file, t.source_line)

        ordered = sorted(txns, key=_sort_key)
        for tx in ordered:
            key = (tx.broker, tx.isin or "")
            if on_error == "collect" and key in self._excluded:
                continue
            pools = self.by_broker.setdefault(tx.broker, BrokerPools(tx.broker))
            try:
                pools.apply(tx)
            except (OversellError, CostBasisMissingError) as e:
                if on_error == "raise":
                    raise
                self.errors.append((tx.broker, tx.isin or "", str(e)))
                self._excluded.add(key)
                # Reset the affected pool so any prior partial state is removed
                # from realized events for this ISIN.
                if tx.isin and tx.isin in pools.by_isin:
                    state = pools.by_isin[tx.isin]
                    state.quantity = ZERO
                    state.total_cost_eur = ZERO
                pools.realized = [
                    r for r in pools.realized if r.isin != (tx.isin or "")
                ]

    def realized_events(self) -> list[RealizedEvent]:
        out: list[RealizedEvent] = []
        for p in self.by_broker.values():
            out.extend(p.realized)
        return out
