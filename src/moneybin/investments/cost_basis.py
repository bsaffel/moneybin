"""Pure-Python cost-basis engine: derive tax lots and realized gains.

Walks an investment ledger per ``(account_id, security_id)`` group, opening a
lot on each acquisition and consuming open lots on each disposal to produce the
1099-B reconciliation grain (``core.fct_investment_lots`` and
``core.fct_realized_gains``). This module owns the FIFO (Task 8) and HIFO
(Task 9) methods; specific-ID and average-cost extend the same machinery in
later tasks.

Correctness over speed: all monetary outputs quantize to two places with
``ROUND_HALF_UP`` and disposal proceeds are penny-conserved (the per-slice
rounding residual is assigned to the last slice) so the row sums reconcile to a
broker statement to the cent. Quantities and prices keep full ``Decimal``
precision. The engine never raises on an oversold position — it emits a
zero-basis ``basis_incomplete`` slice instead (worst case for the taxpayer,
conservative for the IRS).

IDs are content hashes so lots and gains are stable across rebuilds and
referenceable by ``app.lot_selections``.
"""

import hashlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal

# Event types this task handles. Everything else (reinvest, split,
# return_of_capital, cash-only types) is skipped without error — later tasks
# add them. Kept as local literals: there is no canonical taxonomy constant yet
# and the service boundary (Task 14) validates the vocabulary.
_ACQUISITION_TYPES = frozenset({"buy", "transfer_in"})
_DISPOSAL_TYPES = frozenset({"sell", "transfer_out"})

_CENTS = Decimal("0.01")
_ZERO_MONEY = Decimal("0.00")

# Sentinel lot id for the unmatched (oversold) slice — no real lot backs it.
# Deterministic and, paired with the unique disposal txn id, yields a unique
# realized_gain_id.
_UNMATCHED_LOT_ID = ""

# Holding-period boundary: strictly more than 365 days held is long-term.
_LONG_TERM_DAYS = 365


@dataclass(frozen=True)
class LedgerEvent:
    """One normalized investment ledger event fed to the engine.

    Sign conventions (accounting): ``quantity`` positive acquires, negative
    disposes; ``amount`` negative is cash out (a buy), positive is cash in (a
    sell). ``fees`` are already folded into ``amount``.
    """

    investment_transaction_id: str
    account_id: str
    security_id: str | None
    trade_date: date
    original_acquisition_date: date | None
    type: str
    quantity: Decimal | None
    price: Decimal | None
    amount: Decimal | None
    fees: Decimal | None
    currency_code: str | None


@dataclass
class Lot:
    """An open (or closed) tax lot. Mutated in place as disposals consume it."""

    lot_id: str
    account_id: str
    security_id: str
    acquisition_date: date
    acquisition_type: str
    original_quantity: Decimal
    remaining_quantity: Decimal
    cost_basis_total: Decimal
    cost_basis_remaining: Decimal
    cost_basis_method: str
    currency_code: str | None
    source_transaction_id: str


@dataclass(frozen=True)
class RealizedGain:
    """One (disposal, consumed lot) pair — the 1099-B reconciliation grain."""

    realized_gain_id: str
    account_id: str
    security_id: str
    disposal_txn_id: str
    lot_id: str
    quantity: Decimal
    acquisition_date: date
    disposal_date: date
    proceeds: Decimal
    cost_basis: Decimal
    gain_loss: Decimal
    term: str
    cost_basis_method: str
    basis_incomplete: bool
    currency_code: str | None


@dataclass
class _Slice:
    """A quantity drawn from one lot (or the unmatched remainder) for a disposal."""

    lot_id: str
    acquisition_date: date
    quantity: Decimal
    cost_basis: Decimal
    basis_incomplete: bool
    proceeds: Decimal = _ZERO_MONEY


def _money(value: Decimal) -> Decimal:
    """Quantize a monetary value to two places, rounding half up."""
    return value.quantize(_CENTS, rounding=ROUND_HALF_UP)


def _abs_or_zero(value: Decimal | None) -> Decimal:
    """Absolute magnitude of an optional signed value; ``None`` -> zero."""
    return abs(value) if value is not None else Decimal("0")


def _lot_id(
    account_id: str,
    security_id: str,
    acquisition_date: date,
    source_transaction_id: str,
) -> str:
    raw = f"{account_id}|{security_id}|{acquisition_date.isoformat()}|{source_transaction_id}"
    return "lot_" + hashlib.sha256(raw.encode()).hexdigest()[:16]


def _realized_gain_id(disposal_txn_id: str, lot_id: str) -> str:
    raw = f"{disposal_txn_id}|{lot_id}"
    return "rg_" + hashlib.sha256(raw.encode()).hexdigest()[:16]


def _term(acquisition_date: date, disposal_date: date) -> str:
    held_days = (disposal_date - acquisition_date).days
    return "long" if held_days > _LONG_TERM_DAYS else "short"


def _open_lot(
    event: LedgerEvent,
    account_id: str,
    security_id: str,
    method: str,
) -> Lot:
    """Open a lot from an acquisition event (buy or transfer_in)."""
    if event.type == "transfer_in":
        # Holding period transfers with the shares: keep the original date.
        acquisition_date = event.original_acquisition_date or event.trade_date
    else:
        acquisition_date = event.trade_date

    quantity = _abs_or_zero(event.quantity)
    # Basis is |amount|; fees are already folded into amount — do not re-add.
    cost_basis_total = _money(_abs_or_zero(event.amount))

    return Lot(
        lot_id=_lot_id(
            account_id,
            security_id,
            acquisition_date,
            event.investment_transaction_id,
        ),
        account_id=account_id,
        security_id=security_id,
        acquisition_date=acquisition_date,
        acquisition_type=event.type,
        original_quantity=quantity,
        remaining_quantity=quantity,
        cost_basis_total=cost_basis_total,
        cost_basis_remaining=cost_basis_total,
        cost_basis_method=method,
        currency_code=event.currency_code,
        source_transaction_id=event.investment_transaction_id,
    )


def _fifo_sort_key(lot: Lot) -> tuple[date, str]:
    """FIFO order: ascending acquisition date, then lot id."""
    return (lot.acquisition_date, lot.lot_id)


def _hifo_sort_key(lot: Lot) -> tuple[Decimal, date, str]:
    """HIFO order: descending per-unit basis, ties break oldest-first.

    ``unit_cost`` is full-precision ``Decimal`` division used only to rank
    lots for this disposal — never quantized, never stored on the lot.
    """
    unit_cost = lot.cost_basis_remaining / lot.remaining_quantity
    return (-unit_cost, lot.acquisition_date, lot.lot_id)


def _ordered_open_lots(lots: list[Lot], method: str) -> list[Lot]:
    """Lots with remaining quantity, ordered per the elected ``method``.

    Sorted once per disposal call — a lot's unit cost is stable across every
    slice drawn from it within a single disposal. Specific-ID (Task 10) and
    average-cost (Task 11) extend this dispatch with their own branches.
    """
    open_lots = [lot for lot in lots if lot.remaining_quantity > 0]
    if method == "hifo":
        return sorted(open_lots, key=_hifo_sort_key)
    return sorted(open_lots, key=_fifo_sort_key)


def _consume(
    event: LedgerEvent,
    account_id: str,
    security_id: str,
    lots: list[Lot],
    method: str,
) -> list[RealizedGain]:
    """Consume open lots for a disposal (sell or transfer_out).

    ``sell`` produces one realized-gain row per consumed slice with proceeds
    allocated pro-rata by quantity (penny-conserved). ``transfer_out`` consumes
    lots without producing any gains. Consumption order over lots with
    remaining quantity is determined by ``method`` (see
    ``_ordered_open_lots``).
    """
    disposal_quantity = _abs_or_zero(event.quantity)
    disposal_date = event.trade_date
    is_sell = event.type == "sell"

    open_lots = _ordered_open_lots(lots, method)

    slices: list[_Slice] = []
    remaining = disposal_quantity
    for lot in open_lots:
        if remaining <= 0:
            break
        take = min(lot.remaining_quantity, remaining)
        if take == lot.remaining_quantity:
            # Fully closes the lot: take all remaining basis (conserves basis
            # exactly rather than re-deriving pro-rata).
            slice_basis = lot.cost_basis_remaining
            lot.cost_basis_remaining = _ZERO_MONEY
            lot.remaining_quantity = Decimal("0")
        else:
            slice_basis = _money(
                lot.cost_basis_remaining * take / lot.remaining_quantity
            )
            lot.cost_basis_remaining = _money(lot.cost_basis_remaining - slice_basis)
            lot.remaining_quantity = lot.remaining_quantity - take
        slices.append(
            _Slice(
                lot_id=lot.lot_id,
                acquisition_date=lot.acquisition_date,
                quantity=take,
                cost_basis=slice_basis,
                basis_incomplete=False,
            )
        )
        remaining -= take

    # transfer_out never realizes a gain; drop any unmatched remainder silently.
    if not is_sell:
        return []

    # Oversold: emit one zero-basis slice for the unmatched remainder so the
    # rebuild never blocks and basis is never invented.
    if remaining > 0:
        slices.append(
            _Slice(
                lot_id=_UNMATCHED_LOT_ID,
                acquisition_date=disposal_date,
                quantity=remaining,
                cost_basis=_ZERO_MONEY,
                basis_incomplete=True,
            )
        )

    _allocate_proceeds(slices, _money(_abs_or_zero(event.amount)), disposal_quantity)

    return [
        RealizedGain(
            realized_gain_id=_realized_gain_id(
                event.investment_transaction_id, s.lot_id
            ),
            account_id=account_id,
            security_id=security_id,
            disposal_txn_id=event.investment_transaction_id,
            lot_id=s.lot_id,
            quantity=s.quantity,
            acquisition_date=s.acquisition_date,
            disposal_date=disposal_date,
            proceeds=s.proceeds,
            cost_basis=s.cost_basis,
            gain_loss=_money(s.proceeds - s.cost_basis),
            term=_term(s.acquisition_date, disposal_date),
            cost_basis_method=method,
            basis_incomplete=s.basis_incomplete,
            currency_code=event.currency_code,
        )
        for s in slices
    ]


def _allocate_proceeds(
    slices: list[_Slice],
    proceeds_total: Decimal,
    total_quantity: Decimal,
) -> None:
    """Split proceeds across slices pro-rata by quantity, conserving pennies.

    Each slice rounds to two places; the last slice absorbs the residual so the
    slice proceeds sum to ``proceeds_total`` exactly.
    """
    if not slices:
        return
    allocated = _ZERO_MONEY
    for s in slices[:-1]:
        share = _money(proceeds_total * s.quantity / total_quantity)
        s.proceeds = share
        allocated += share
    slices[-1].proceeds = proceeds_total - allocated


def compute_lots_and_gains(
    events: Sequence[LedgerEvent],
    *,
    method_for: Callable[[str, str], str],
    selections_for: Callable[[str], list[tuple[str, Decimal]]],
) -> tuple[list[Lot], list[RealizedGain]]:
    """Derive tax lots and realized gains from a ledger.

    Events are processed independently per ``(account_id, security_id)`` group,
    sorted within a group by ``(trade_date, investment_transaction_id)``.
    Acquisitions (``buy``, ``transfer_in``) open lots; disposals (``sell``,
    ``transfer_out``) consume them per the elected method. Cash-only events and
    any event with ``security_id is None`` are ignored, as are types this task
    does not yet handle (they are skipped, never raised on).

    Args:
        events: The ledger events to process. Order is not assumed; the engine
            sorts within each group.
        method_for: ``(account_id, security_id) -> method`` returning the
            elected cost-basis method for a position (e.g. ``"fifo"``).
        selections_for: ``disposal_txn_id -> [(lot_id, quantity), ...]`` for
            specific-identification. Accepted for interface stability; FIFO
            does not use it.

    Returns:
        A ``(lots, gains)`` tuple. ``lots`` includes fully-closed lots
        (``remaining_quantity == 0``); ``gains`` has one row per consumed slice
        of each ``sell`` (``transfer_out`` yields none).
    """
    _ = selections_for  # Unused under FIFO; part of the Task 9-13 contract.

    groups: dict[tuple[str, str], list[LedgerEvent]] = {}
    for event in events:
        if event.security_id is None:
            continue
        groups.setdefault((event.account_id, event.security_id), []).append(event)

    all_lots: list[Lot] = []
    all_gains: list[RealizedGain] = []

    for (account_id, security_id), group_events in groups.items():
        method = method_for(account_id, security_id)
        ordered = sorted(
            group_events,
            key=lambda e: (e.trade_date, e.investment_transaction_id),
        )
        lots: list[Lot] = []
        for event in ordered:
            if event.type in _ACQUISITION_TYPES:
                lots.append(_open_lot(event, account_id, security_id, method))
            elif event.type in _DISPOSAL_TYPES:
                all_gains.extend(_consume(event, account_id, security_id, lots, method))
            # Any other type is intentionally skipped (Tasks 11-12).
        all_lots.extend(lots)

    return all_lots, all_gains
