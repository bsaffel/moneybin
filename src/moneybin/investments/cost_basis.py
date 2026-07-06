"""Pure-Python cost-basis engine: derive tax lots and realized gains.

Walks an investment ledger per ``(account_id, security_id)`` group, opening a
lot on each acquisition and consuming open lots on each disposal to produce the
1099-B reconciliation grain (``core.fct_investment_lots`` and
``core.fct_realized_gains``). This module owns the FIFO (Task 8), HIFO
(Task 9), specific-identification (Task 10), and average-cost (Task 11)
methods. Average cost is the one genuinely distinct computation: it keeps a
running per-group pool (two scalars) and draws each disposal's basis from the
pooled average rather than from the consumed lot — the other three methods take
the lot's own basis.

Corporate actions (Task 12) are typed ledger events applied in place at their
trade-date position, never rewrites of history: ``split`` scales open-lot
quantities by a multiplier while preserving total basis, ``return_of_capital``
reduces open-lot basis pro-rata (clamped at zero) without a disposal, and
``reinvest`` opens a lot exactly like a buy.

Correctness over speed: all monetary outputs quantize to two places with
``ROUND_HALF_UP`` and disposal proceeds are penny-conserved (the per-slice
rounding residual is assigned to the last slice) so the row sums reconcile to a
broker statement to the cent. Quantities and prices keep full ``Decimal``
precision. The engine never raises on an oversold position — it emits a
zero-basis ``basis_incomplete`` slice instead (worst case for the taxpayer,
conservative for the IRS). The same flag applies on the acquisition side: a
``transfer_in`` with no supplied basis opens a zero-basis ``basis_incomplete``
lot rather than inventing a number, and that lot's flag survives onto any
realized gain a later disposal draws from it.

IDs are content hashes so lots and gains are stable across rebuilds and
referenceable by ``app.lot_selections``.
"""

import hashlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal

# Event types the engine models. Acquisitions open lots (``reinvest`` is a buy
# leg that records ``acquisition_type='reinvest'``); disposals consume them; the
# corporate actions ``split`` and ``return_of_capital`` adjust open lots in place
# in the group loop (neither is a disposal, neither realizes a gain). Cash-only
# types (deposit/dividend/fee/...) and any type the engine does not model are
# skipped without error. Kept as local literals: there is no canonical taxonomy
# constant yet and the service boundary (Task 14) validates the vocabulary.
_ACQUISITION_TYPES = frozenset({"buy", "transfer_in", "reinvest"})
_DISPOSAL_TYPES = frozenset({"sell", "transfer_out"})

_CENTS = Decimal("0.01")
_ZERO_MONEY = Decimal("0.00")

# Sentinel lot id for the unmatched (oversold) slice — no real lot backs it.
# Deterministic and, paired with the unique disposal txn id, yields a unique
# realized_gain_id.
_UNMATCHED_LOT_ID = ""

# Same-day event ordering. Events sharing a trade_date are applied in a
# deterministic, economically-ordered sequence — never arbitrary content-hash
# order. Corporate actions take effect at the ex-date (start of day) so they
# precede same-day trades; acquisitions precede disposals so a same-day buy is
# an available lot for a same-day sell. Ties within a bucket fall back to the
# stable content-hash id. Cash-only / unmodeled types are skipped in the loop,
# so their bucket (the default) is immaterial.
_SAME_DAY_TYPE_ORDER: dict[str, int] = {
    "split": 0,
    "return_of_capital": 0,
    "buy": 1,
    "transfer_in": 1,
    "reinvest": 1,
    "sell": 2,
    "transfer_out": 2,
}
_DEFAULT_SAME_DAY_ORDER = 1


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
    basis_incomplete: bool


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


@dataclass
class _Pool:
    """Average-cost running pool for one ``(account, security)`` group.

    Two scalars per the CRA/HMRC pooled-average model (NOT runtime lot-merging,
    the Beancount anti-pattern the spec warns against): ``units`` is the total
    open quantity (full ``Decimal`` precision) and ``cost`` the total remaining
    basis (money, 2dp). Every acquisition adds to both; every disposal draws both
    down at the pooled average. Basis lives here, not on any ``Lot``, because
    under average cost no lot has a basis of its own.
    """

    units: Decimal = Decimal("0")
    cost: Decimal = _ZERO_MONEY


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


def _one_year_anniversary(acquisition_date: date) -> date:
    """The calendar date one year after acquisition (the long-term boundary).

    A Feb 29 acquisition has no calendar anniversary; by convention its
    anniversary is the non-leap Feb 28, making the position long-term on/after
    the following Mar 1 (consistent with the non-leap rule that long-term begins
    the day after the anniversary).
    """
    try:
        return acquisition_date.replace(year=acquisition_date.year + 1)
    except ValueError:
        return date(acquisition_date.year + 1, 2, 28)


def _term(acquisition_date: date, disposal_date: date) -> str:
    """Long- vs short-term per the IRS "more than one year" rule.

    Calendar-based, NOT a 365-day count: a lot held exactly one year whose span
    crosses a leap day is 366 days but still short-term. Long-term requires the
    disposal to fall strictly after the one-year anniversary.
    """
    return (
        "long" if disposal_date > _one_year_anniversary(acquisition_date) else "short"
    )


def _open_lot(
    event: LedgerEvent,
    account_id: str,
    security_id: str,
    method: str,
) -> Lot:
    """Open a lot from an acquisition event (buy, transfer_in, or reinvest).

    ``acquisition_type`` records the event type verbatim, so a ``reinvest`` lot
    is distinguishable from a plain ``buy`` while behaving identically otherwise.
    ``buy``/``reinvest`` always carry a non-``None`` amount (service-layer
    validation rejects otherwise); ``transfer_in`` may not (basis is often
    unknown at transfer time) — a ``None`` amount opens a zero-basis lot
    flagged ``basis_incomplete`` rather than inventing a number.
    """
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
        basis_incomplete=event.amount is None,
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


def _consumption_plan(
    lots: list[Lot],
    method: str,
    disposal_txn_id: str,
    selections_for: Callable[[str], list[tuple[str, Decimal]]],
) -> list[tuple[Lot, Decimal]]:
    """Build the (lot, cap) draw order for a disposal.

    Each entry caps how many units may be drawn from that lot at that
    position; the live ``lot.remaining_quantity`` (mutated by prior entries)
    still governs the actual ``take`` — the cap only ever narrows it further.
    Sorted/selected once per disposal call — a lot's unit cost is stable
    across every slice drawn from it within a single disposal.

    - FIFO: every open lot, oldest-first, capped at its own remaining
      quantity (no extra limit).
    - HIFO: every open lot, highest-per-unit-basis-first, same uncapped rule.
    - Specific-ID: the selections from ``selections_for`` (in selection
      order, capped at the selected quantity) first, then a FIFO fallback
      over ALL open lots for any remainder. A selection naming an unknown or
      already-closed (``remaining_quantity == 0``) lot is silently skipped —
      the engine stays total and never raises; validating selections is a
      Task 14 service concern. Because the fallback re-lists every open lot
      (including ones already drawn from above) and reads live remaining
      quantity, a partially-selected lot correctly reappears in the fallback
      for whatever it has left; a fully-drawn lot reappears with 0 remaining
      and is skipped by the ``take <= 0`` guard in ``_consume``.
    - Average-cost reuses the FIFO draw order unchanged: averaging alters only
      each slice's basis number (drawn from the pool in ``_consume``), never the
      order lots are traversed for quantity and holding-period attribution.
    """
    open_lots = [lot for lot in lots if lot.remaining_quantity > 0]
    if method == "hifo":
        ordered = sorted(open_lots, key=_hifo_sort_key)
        return [(lot, lot.remaining_quantity) for lot in ordered]

    fifo_plan = [
        (lot, lot.remaining_quantity) for lot in sorted(open_lots, key=_fifo_sort_key)
    ]
    if method != "specific":
        return fifo_plan

    by_lot_id = {lot.lot_id: lot for lot in lots}
    selected_plan: list[tuple[Lot, Decimal]] = []
    for lot_id, quantity in selections_for(disposal_txn_id):
        lot = by_lot_id.get(lot_id)
        if lot is None or lot.remaining_quantity <= 0:
            continue
        selected_plan.append((lot, quantity))
    return selected_plan + fifo_plan


def _consume(
    event: LedgerEvent,
    account_id: str,
    security_id: str,
    lots: list[Lot],
    method: str,
    selections_for: Callable[[str], list[tuple[str, Decimal]]],
    pool: _Pool | None,
) -> list[RealizedGain]:
    """Consume open lots for a disposal (sell or transfer_out).

    ``sell`` produces one realized-gain row per consumed slice with proceeds
    allocated pro-rata by quantity (penny-conserved). ``transfer_out`` consumes
    lots without producing any gains. Consumption order (and, for
    specific-ID, the per-lot cap) is determined by ``method`` (see
    ``_consumption_plan``).

    ``pool`` is non-``None`` only for the average-cost method. When set, each
    slice's basis is drawn from the pooled average (captured before the pool is
    reduced) instead of the consumed lot's own basis, and the pool is drawn down
    by the disposal's matched quantity and blended basis.
    """
    disposal_quantity = _abs_or_zero(event.quantity)
    disposal_date = event.trade_date
    is_sell = event.type == "sell"

    # Average cost: capture the pooled per-unit basis BEFORE the pool is reduced
    # (every prior acquisition has already shifted it). Zero when the pool is
    # empty — such a disposal is fully oversold and realizes at zero basis.
    avg = Decimal("0")
    if pool is not None and pool.units > 0:
        avg = pool.cost / pool.units

    plan = _consumption_plan(
        lots, method, event.investment_transaction_id, selections_for
    )

    slices: list[_Slice] = []
    remaining = disposal_quantity
    for lot, cap in plan:
        if remaining <= 0:
            break
        take = min(lot.remaining_quantity, remaining, cap)
        if take <= 0:
            # Already-drained lot reappearing in the specific-ID FIFO
            # fallback phase (or a fully-capped selection) — nothing to draw.
            continue
        if pool is not None:
            # Average cost: basis comes from the pool, not the lot. Draw down
            # only the lot's quantity here; the pooled basis is allocated across
            # the disposal's slices after the loop, and each open lot's
            # cost_basis_remaining is reset to its average share when the group
            # finishes (_reconcile_average_lots).
            slice_basis = _ZERO_MONEY
            lot.remaining_quantity = lot.remaining_quantity - take
        elif take == lot.remaining_quantity:
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
                # Carries an incomplete-basis acquisition (e.g. a no-basis
                # transfer_in) forward onto the realized gain it produces.
                basis_incomplete=lot.basis_incomplete,
            )
        )
        remaining -= take

    # Average cost: draw the pool down and stamp each matched slice with its
    # pooled basis. A disposal that empties the pool takes all remaining pooled
    # cost (the full-close-takes-all rule applied to the pool) so no penny is
    # stranded. Runs for transfer_out too — the shares leave the pool even though
    # no gain is realized. The oversold remainder (below) keeps its zero basis.
    if pool is not None:
        matched_quantity = disposal_quantity - remaining
        if matched_quantity > 0:
            if pool.units == matched_quantity:
                disposal_basis = pool.cost
                pool.cost = _ZERO_MONEY
                pool.units = Decimal("0")
            else:
                disposal_basis = _money(matched_quantity * avg)
                pool.cost = _money(pool.cost - disposal_basis)
                pool.units = pool.units - matched_quantity
            _allocate_basis(slices, disposal_basis, matched_quantity)

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

    # Grain is one row per (disposal, consumed lot): a lot that appears twice
    # in one disposal (a partial specific-ID selection whose remainder falls to
    # the FIFO phase and lands back on the same lot) must merge to a single row
    # before proceeds are allocated, or two rows would share a realized_gain_id
    # (hash of disposal_txn_id + lot_id) and collide on the fct_realized_gains PK.
    slices = _merge_slices_by_lot(slices)

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


def _merge_slices_by_lot(slices: list[_Slice]) -> list[_Slice]:
    """Collapse slices drawn from the same lot into one per lot.

    Enforces the ``core.fct_realized_gains`` grain (one row per disposal ×
    consumed lot). Only specific-ID produces same-lot repeats within a
    disposal; every other path yields one slice per lot, so each group is a
    singleton and passes through unchanged. First-appearance order is preserved
    so ``_allocate_proceeds``'s residual-to-last-slice rounding stays
    deterministic. ``acquisition_date`` and ``basis_incomplete`` are identical
    across a lot group (same lot, same disposal), so the first slice's values
    carry. The oversold slice's unique ``_UNMATCHED_LOT_ID`` makes it its own
    singleton group — one per disposal by construction — so it passes through.
    """
    merged: dict[str, _Slice] = {}
    for s in slices:
        existing = merged.get(s.lot_id)
        if existing is None:
            merged[s.lot_id] = _Slice(
                lot_id=s.lot_id,
                acquisition_date=s.acquisition_date,
                quantity=s.quantity,
                cost_basis=s.cost_basis,
                basis_incomplete=s.basis_incomplete,
            )
        else:
            existing.quantity += s.quantity
            existing.cost_basis = _money(existing.cost_basis + s.cost_basis)
    return list(merged.values())


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


def _allocate_basis(
    slices: list[_Slice],
    basis_total: Decimal,
    total_quantity: Decimal,
) -> None:
    """Split a disposal's pooled basis across its slices, conserving pennies.

    The average-cost analogue of ``_allocate_proceeds``: each slice takes its
    pro-rata share of the pooled ``basis_total`` (rounded to cents) and the last
    slice absorbs the residual, so the slice bases sum to ``basis_total`` (the
    amount the pool was drawn down by) with no stranded penny. Overwrites the
    placeholder basis set during consumption; ``slices`` here are the matched
    slices only (the oversold remainder keeps its zero basis).
    """
    if not slices:
        return
    allocated = _ZERO_MONEY
    for s in slices[:-1]:
        share = _money(basis_total * s.quantity / total_quantity)
        s.cost_basis = share
        allocated += share
    slices[-1].cost_basis = basis_total - allocated


def _reconcile_average_lots(lots: list[Lot], pool: _Pool) -> None:
    """Reset average-cost lots' remaining basis to their pooled-average share.

    Under average cost a lot has no basis of its own; the meaningful figure is
    the position's pooled cost. Closed lots hold zero; each open lot takes
    ``remaining_quantity * avg`` (the last open lot absorbs the rounding
    residual) so ``SUM(cost_basis_remaining)`` over the open lots equals the
    remaining pooled cost exactly — the reconciliation ``dim_holdings`` relies
    on. ``cost_basis_total`` is left untouched: it records what was actually paid
    at acquisition, which averaging does not rewrite (so an open lot's remaining
    basis can differ from — even exceed — its own ``cost_basis_total``).
    """
    open_lots = [lot for lot in lots if lot.remaining_quantity > 0]
    for lot in lots:
        if lot.remaining_quantity <= 0:
            lot.cost_basis_remaining = _ZERO_MONEY
    if not open_lots:
        return
    # pool.units == sum of the open lots' remaining quantities, so this is
    # exactly the remaining pooled average and the residual below is <= a cent.
    avg = pool.cost / pool.units
    allocated = _ZERO_MONEY
    for lot in open_lots[:-1]:
        basis = _money(lot.remaining_quantity * avg)
        lot.cost_basis_remaining = basis
        allocated += basis
    open_lots[-1].cost_basis_remaining = _money(pool.cost - allocated)


def _apply_split(
    lots: list[Lot], multiplier: Decimal | None, pool: _Pool | None
) -> None:
    """Apply a stock split to every open lot of the group's security.

    Encoding (Decision D6): a ``split`` event carries the split MULTIPLIER ``M``
    in its ``quantity`` field — new shares per old share, e.g. ``2`` for 2:1,
    ``Decimal("1.5")`` for 3:2, ``Decimal("0.5")`` for a 1:2 reverse split;
    ``price``/``amount``/``fees`` are unused. This is a deliberate, cleaner
    simplification of THE SPEC's under-specified split note (which frames the
    ratio as ``new_units_added``); the plan authorized it and Task 18 updates the
    spec to match — do not "restore" the ratio-as-added-units reading here.

    A split is NOT a disposal and produces no realized gain. Each OPEN lot
    (``remaining_quantity > 0``) has its ``original_quantity`` and
    ``remaining_quantity`` scaled by ``M`` at FULL Decimal precision (never
    quantized) while ``cost_basis_total``/``cost_basis_remaining`` are UNCHANGED —
    total basis is preserved and per-unit basis divides by ``M``. Under average
    cost the pool's ``units`` scale by ``M`` (its ``cost`` is unchanged), so the
    pooled average per unit divides by ``M`` too. Closed lots (already fully
    consumed pre-split) are left untouched as historical record.
    """
    if multiplier is None:
        return
    for lot in lots:
        if lot.remaining_quantity > 0:
            lot.original_quantity = lot.original_quantity * multiplier
            lot.remaining_quantity = lot.remaining_quantity * multiplier
    if pool is not None:
        pool.units = pool.units * multiplier


def _apply_return_of_capital(
    lots: list[Lot], roc_amount: Decimal, pool: _Pool | None
) -> None:
    """Reduce cost basis for a return-of-capital distribution (no disposal).

    A ``return_of_capital`` event carries the cash returned in ``amount``
    (``abs`` = the distribution); ``roc_amount`` is that magnitude. RoC is NOT a
    disposal and realizes no gain — it only lowers basis.

    Lot-based methods (fifo/hifo/specific): the distribution is spread across the
    security's OPEN lots pro-rata by remaining quantity, each lot's
    ``cost_basis_remaining`` reduced and clamped at zero (basis never goes
    negative). Every open lot except the last takes ``min(its quantity-share, its
    own basis)``; the last open lot absorbs the residual (``target`` minus what
    the others took), itself clamped at its own basis. When no lot clamps this is
    penny-conserved and the total reduction equals ``min(roc, Σ basis)``.

    v1 clamp-overflow is DROPPED (a known simplification, not a target behavior —
    revisit in a follow-up): when a lot's quantity-share exceeds its own basis,
    the overflow spills onto the last open lot; if the last lot cannot absorb it —
    or the clamping lot IS the last lot — that overflow is silently dropped, and
    likewise any aggregate RoC beyond ``Σ basis`` is dropped. This can happen even
    when the aggregate RoC is within total basis (uneven per-unit basis across
    lots). In every drop case the position retains MORE basis than economically
    correct, so a later sale UNDER-reports realized gain — which is
    taxpayer-favorable and IRS-UNfavorable (audit-exposing), NOT conservative.
    Worked example pinned in ``test_return_of_capital_clamp_overflow_is_dropped_v1``:
    lots (10u/$200) + (100u/$10), RoC $100 with aggregate ($100) ≤ Σ basis ($210)
    leaves $190.91 + $0.00 (only $19.09 reduced; ~$80.91 dropped) where $110 is
    correct. The proper fix redistributes clamp-overflow so the total reduction
    always equals ``min(roc, Σ basis)``.

    Average cost: reduce the pool's ``cost`` by ``min(roc, pool.cost)`` (clamped
    at zero); ``_reconcile_average_lots`` spreads the reduced pool across the open
    lots at group end. Lots are not touched directly here.
    """
    if pool is not None:
        pool.cost = pool.cost - min(roc_amount, pool.cost)
        return
    open_lots = [lot for lot in lots if lot.remaining_quantity > 0]
    if not open_lots:
        return
    total_quantity = sum((lot.remaining_quantity for lot in open_lots), Decimal("0"))
    total_basis = sum((lot.cost_basis_remaining for lot in open_lots), _ZERO_MONEY)
    # Aggregate clamp: never reduce more than the position holds in basis.
    target = min(roc_amount, total_basis)
    allocated = _ZERO_MONEY
    for lot in open_lots[:-1]:
        share = min(
            _money(target * lot.remaining_quantity / total_quantity),
            lot.cost_basis_remaining,
        )
        lot.cost_basis_remaining = _money(lot.cost_basis_remaining - share)
        allocated += share
    # Residual to the last open lot conserves pennies; still clamped at its basis.
    last = open_lots[-1]
    last_share = min(target - allocated, last.cost_basis_remaining)
    last.cost_basis_remaining = _money(last.cost_basis_remaining - last_share)


def compute_lots_and_gains(
    events: Sequence[LedgerEvent],
    *,
    method_for: Callable[[str, str], str],
    selections_for: Callable[[str], list[tuple[str, Decimal]]],
) -> tuple[list[Lot], list[RealizedGain]]:
    """Derive tax lots and realized gains from a ledger.

    Events are processed independently per ``(account_id, security_id)`` group,
    sorted within a group by ``(trade_date, same-day-type-order,
    investment_transaction_id)`` — corporate actions apply before same-day
    trades and acquisitions before disposals (see ``_SAME_DAY_TYPE_ORDER``).
    Acquisitions (``buy``, ``transfer_in``, ``reinvest``) open lots; disposals
    (``sell``, ``transfer_out``) consume them per the elected method; the
    corporate actions ``split`` and ``return_of_capital`` adjust the group's open
    lots in place without realizing a gain. Cash-only events and any event with
    ``security_id is None`` are ignored, as is any type the engine does not model
    (skipped, never raised on).

    Args:
        events: The ledger events to process. Order is not assumed; the engine
            sorts within each group.
        method_for: ``(account_id, security_id) -> method`` returning the
            elected cost-basis method for a position (e.g. ``"fifo"``).
        selections_for: ``disposal_txn_id -> [(lot_id, quantity), ...]`` for
            specific-identification, called once per disposal with the
            disposing event's ``investment_transaction_id``. FIFO and HIFO
            never call it.

    Returns:
        A ``(lots, gains)`` tuple. ``lots`` includes fully-closed lots
        (``remaining_quantity == 0``); ``gains`` has one row per consumed slice
        of each ``sell`` (``transfer_out`` yields none).
    """
    groups: dict[tuple[str, str], list[LedgerEvent]] = {}
    for event in events:
        if event.security_id is None:
            continue
        groups.setdefault((event.account_id, event.security_id), []).append(event)

    all_lots: list[Lot] = []
    all_gains: list[RealizedGain] = []

    for (account_id, security_id), group_events in groups.items():
        method = method_for(account_id, security_id)
        # Average cost keeps a running pool for the group; other methods don't.
        pool = _Pool() if method == "average" else None
        ordered = sorted(
            group_events,
            key=lambda e: (
                e.trade_date,
                _SAME_DAY_TYPE_ORDER.get(e.type, _DEFAULT_SAME_DAY_ORDER),
                e.investment_transaction_id,
            ),
        )
        lots: list[Lot] = []
        for event in ordered:
            if event.type in _ACQUISITION_TYPES:
                lot = _open_lot(event, account_id, security_id, method)
                lots.append(lot)
                if pool is not None:
                    pool.units = pool.units + lot.original_quantity
                    pool.cost = pool.cost + lot.cost_basis_total
            elif event.type in _DISPOSAL_TYPES:
                all_gains.extend(
                    _consume(
                        event,
                        account_id,
                        security_id,
                        lots,
                        method,
                        selections_for,
                        pool,
                    )
                )
            elif event.type == "split":
                _apply_split(lots, event.quantity, pool)
            elif event.type == "return_of_capital":
                _apply_return_of_capital(lots, _money(_abs_or_zero(event.amount)), pool)
            # Any other (unknown / out-of-scope) type is intentionally skipped.
        if pool is not None:
            _reconcile_average_lots(lots, pool)
        all_lots.extend(lots)

    return all_lots, all_gains
