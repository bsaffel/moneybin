"""Read-only proof that a proposed Golden ledger preserves complete elections."""

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields, replace
from datetime import date
from decimal import Decimal
from typing import Any

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.investments.cost_basis import (
    LedgerEvent,
    Lot,
    compute_lots_and_gains,
    event_order_key,
)
from moneybin.tables import LOT_SELECTIONS

Selection = list[tuple[str, Decimal]]


@dataclass(frozen=True)
class CurationRemap:
    """Complete replacements and their exact pre-publication collections."""

    replacements: dict[str, Selection]
    before: dict[str, Selection]


def _refuse() -> UserError:
    return UserError(
        "Cannot change investment membership: complete lot selection preservation "
        "cannot be proved. Correct unresolved selections before retrying.",
        code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
    )


def _events(rows: Sequence[Mapping[str, Any]]) -> list[LedgerEvent]:
    if any(
        row.get("trade_date") is None or row.get("account_id") is None for row in rows
    ):
        raise _refuse()
    names = {field.name for field in fields(LedgerEvent)}
    events = [
        LedgerEvent(**{key: value for key, value in row.items() if key in names})
        for row in rows
    ]
    if len({e.investment_transaction_id for e in events}) != len(events):
        raise _refuse()
    return events


def validate_complete_selections(
    events: Sequence[LedgerEvent],
    selections: Mapping[str, Selection],
    checked: set[str],
) -> list[Lot]:
    """Replay without allowing the engine's fallback to hide lost elections."""
    # Transfer-out consumes the same lots but normally emits no realized gain.
    replay = [
        replace(e, type="sell") if e.type == "transfer_out" else e for e in events
    ]
    lots, gains = compute_lots_and_gains(
        replay,
        method_for=lambda _account, _security: "specific",
        selections_for=lambda disposal: selections.get(disposal, []),
    )
    consumed: dict[tuple[str, str], Decimal] = defaultdict(Decimal)
    for gain in gains:
        consumed[gain.disposal_txn_id, gain.lot_id] += gain.quantity
    for disposal in checked:
        for lot, quantity in selections.get(disposal, []):
            if quantity <= 0 or consumed[disposal, lot] < quantity:
                raise _refuse()
    return lots


def preflight_membership_transition(
    db: Database,
    current_ledger: Sequence[Mapping[str, Any]],
    proposed_ledger: Sequence[Mapping[str, Any]],
    semantic_id_map: Mapping[str, str],
    *,
    restored_selections: Mapping[str, Selection] | None = None,
    restoration_ledger: Sequence[Mapping[str, Any]] | None = None,
) -> CurationRemap:
    """Remap whole collections and replay every affected downstream disposal."""
    selections: dict[str, Selection] = {}
    for disposal, lot, quantity in db.execute(
        f"""SELECT investment_transaction_id, lot_id, quantity
        FROM {LOT_SELECTIONS.full_name}
        ORDER BY investment_transaction_id, lot_id"""  # noqa: S608  # TableRef constant
    ).fetchall():
        selections.setdefault(disposal, []).append((lot, Decimal(quantity)))
    actual = dict(selections)
    if restored_selections is not None:
        validate_complete_selections(_events(current_ledger), actual, set(actual))
        selections = {**selections, **restored_selections}
        selections = {
            disposal: collection
            for disposal, collection in selections.items()
            if collection
        }
    if not selections:
        return CurationRemap({disposal: [] for disposal in actual}, actual)
    current = _events(
        restoration_ledger if restoration_ledger is not None else current_ledger
    )
    proposed = _events(proposed_ledger)
    old = {e.investment_transaction_id: e for e in current}
    new = {e.investment_transaction_id: e for e in proposed}
    boundaries: dict[tuple[str, str | None], tuple[date, int, str]] = {}
    for identity in old.keys() | new.keys():
        if old.get(identity) == new.get(identity):
            continue
        for event in (old.get(identity), new.get(identity)):
            if event is not None:
                position = (event.account_id, event.security_id)
                key = event_order_key(event)
                boundaries[position] = min(boundaries.get(position, key), key)
    checked: set[str] = set()
    for disposal in selections:
        event = old.get(disposal)
        if event is None:
            raise _refuse()
        position = (event.account_id, event.security_id)
        if (
            restored_selections is not None
            or disposal in semantic_id_map
            or position in boundaries
            and event_order_key(event) >= boundaries[position]
        ):
            checked.add(disposal)
    old_lots = {
        lot.lot_id: lot
        for lot in validate_complete_selections(current, selections, checked)
    }
    proposed_lots, _gains = compute_lots_and_gains(
        proposed, method_for=lambda _a, _s: "specific", selections_for=lambda _d: []
    )
    by_source: dict[str, list[Lot]] = {}
    for lot in proposed_lots:
        by_source.setdefault(lot.source_transaction_id, []).append(lot)
    remapped: dict[str, Selection] = {}
    for disposal, collection in selections.items():
        if disposal not in checked:
            remapped[disposal] = collection
            continue
        target = semantic_id_map.get(disposal, disposal)
        if target not in new or new[target].type not in {"sell", "transfer_out"}:
            raise _refuse()
        mapped: Selection = []
        for lot_id, quantity in collection:
            prior = old_lots.get(lot_id)
            if prior is None:
                raise _refuse()
            source = semantic_id_map.get(
                prior.source_transaction_id, prior.source_transaction_id
            )
            candidates = by_source.get(source, [])
            # A lineage change requires a complete transfer-aware correspondence;
            # never guess a lot from its date or remaining quantity.
            candidates = [
                lot
                for lot in candidates
                if lot.source_transfer_id == prior.source_transfer_id
                and lot.transfer_lineage == prior.transfer_lineage
                and lot.account_id == new[target].account_id
                and lot.security_id == new[target].security_id
                and lot.currency_code == new[target].currency_code
            ]
            if len(candidates) != 1:
                raise _refuse()
            mapped.append((candidates[0].lot_id, quantity))
        mapped.sort()
        if len({lot for lot, _q in mapped}) != len(mapped):
            raise _refuse()
        if target in remapped and remapped[target] != mapped:
            raise _refuse()
        remapped[target] = mapped
    validate_complete_selections(
        proposed, remapped, {semantic_id_map.get(d, d) for d in checked}
    )
    replacements = {
        disposal: remapped.get(disposal, [])
        for disposal in actual.keys() | remapped.keys()
        if actual.get(disposal, []) != remapped.get(disposal, [])
    }
    return CurationRemap(replacements, {d: actual.get(d, []) for d in replacements})
