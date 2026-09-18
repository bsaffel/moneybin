"""Observed field choices and whole-event accounting feasibility."""

from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal
from itertools import combinations, product
from typing import Any

from moneybin import error_codes
from moneybin.errors import UserError
from moneybin.investments.event_assignment import MAX_COMPONENT_STATES
from moneybin.investments.event_fingerprints import fingerprint

_FIELDS = (
    "trade_date",
    "settlement_date",
    "original_acquisition_date",
    "subtype",
    "quantity",
    "price",
    "amount",
    "fees",
)
_ACCOUNTING_FIELDS = ("quantity", "price", "amount", "fees")
_CENT = Decimal("0.01")


def _precedence(row: Mapping[str, Any]) -> tuple[bool, str, str, str]:
    return (
        row["source_type"] == "manual",
        row["source_type"],
        row["source_origin"],
        row["native_reference"],
    )


def _different(
    field: str, left: Mapping[str, Any], right: Mapping[str, Any], days: int
) -> bool:
    a, b = left.get(field), right.get(field)
    if a is None or b is None or a == b:
        return False
    if field == "original_acquisition_date":
        return True
    if (
        field == "trade_date"
        and left["trade_date_basis"] == right["trade_date_basis"] == "explicit"
    ):
        return True
    if isinstance(a, date) and isinstance(b, date):
        return abs((a - b).days) > days
    if field == "subtype":
        return {a, b} in ({"qualified", "non_qualified"}, {"short_term", "long_term"})
    if field in _ACCOUNTING_FIELDS:
        if field == "price" and all(
            row.get("quantity") is not None
            and row.get("price") is not None
            and row.get("amount") is not None
            and abs(
                row["quantity"] * row["price"] + row["amount"] + (row.get("fees") or 0)
            )
            <= _CENT
            for row in (left, right)
        ):
            return False
        a, b = Decimal(str(a)), Decimal(str(b))
        floor = Decimal("0.000001") if field == "quantity" else _CENT
        relative = (
            Decimal("0.00000001")
            if field == "quantity"
            else Decimal("0.0001")
            if field == "price"
            else Decimal("0")
        )
        return abs(a - b) > max(floor, max(abs(a), abs(b)) * relative)
    return False


def _coherent(rows: Sequence[Mapping[str, Any]]) -> bool:
    for row in rows:
        if row["type"] in {"buy", "sell", "reinvest"}:
            quantity, amount, price = (
                row.get(field) for field in ("quantity", "amount", "price")
            )
            if quantity is None or amount is None:
                return False
            if price is not None:
                if abs(quantity * price + amount + (row.get("fees") or 0)) > _CENT:
                    return False
    acquisitions = [row for row in rows if row["type"] == "reinvest"]
    if acquisitions:
        incomes = [row for row in rows if row["leg_role"] == "income"]
        if len(acquisitions) != 1 or len(incomes) != 1:
            return False
        if (
            abs(
                acquisitions[0]["amount"]
                + incomes[0]["amount"]
                + (acquisitions[0].get("fees") or 0)
            )
            > _CENT
        ):
            return False
    return True


def issue_choices(
    legs: Sequence[Mapping[str, Any]],
    *,
    relationship: str,
    date_threshold_days: int,
) -> tuple[dict[str, Any], ...] | None:
    """Return stable conflicts, or None when no offered combination validates."""
    by_role: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for leg in legs:
        by_role[leg["leg_role"]].append(leg)
    defaults: dict[str, dict[str, Any]] = {}
    conflicts: set[tuple[str, str]] = set()
    for role, rows in by_role.items():
        ordered = sorted(rows, key=_precedence)
        default = dict(ordered[0])
        for field in _FIELDS:
            present = [row for row in ordered if row.get(field) is not None]
            if field == "trade_date":
                present.sort(key=lambda row: row["trade_date_basis"] != "explicit")
            default[field] = present[0][field] if present else None
            if any(
                _different(field, left, right, date_threshold_days)
                for left, right in combinations(present, 2)
            ):
                conflicts.add((role, field))
        defaults[role] = default
    if not _coherent(list(defaults.values())):
        for role, rows in by_role.items():
            for field in _ACCOUNTING_FIELDS:
                if (
                    len({row.get(field) for row in rows if row.get(field) is not None})
                    > 1
                ):
                    conflicts.add((role, field))
    issued: list[dict[str, Any]] = []
    for role, field in sorted(conflicts):
        conflict_id = fingerprint("investment_conflict", (relationship, role, field))
        choices: list[dict[str, Any]] = []
        seen: set[Any] = set()
        for row in sorted(by_role[role], key=_precedence):
            value = row.get(field)
            if value is None or value in seen:
                continue
            seen.add(value)
            choices.append({
                "choice_id": fingerprint(
                    "investment_choice",
                    (
                        conflict_id,
                        row["source_type"],
                        row["source_origin"],
                        row["native_reference"],
                        row["observation_version"],
                        value,
                    ),
                ),
                "value": value,
                "source_type": row["source_type"],
                "source_origin": row["source_origin"],
                "native_reference": row["native_reference"],
                "observation_version": row["observation_version"],
            })
        issued.append({
            "conflict_id": conflict_id,
            "leg_role": role,
            "field": field,
            "choices": choices,
        })
    # A coherent selection short-circuits on the first match, so this bounds
    # actual combinations *examined* — not the theoretical product size — the
    # same shape as event_assignment.MAX_COMPONENT_STATES (reused, plus its
    # error code): many conflicting fields with no bearing on `_coherent`
    # (e.g. differing dates/subtype while amounts agree) return at index 0
    # regardless of how large the product is, while a compound event with
    # several *accounting*-field conflicts and no coherent choice must search
    # exhaustively — that search is what needs the bound. Measured: a
    # 4-origin, 2-role reinvest with no coherent combination reaches ~67M
    # combinations; this refuses well before that, in tens of milliseconds.
    for index, selected in enumerate(
        product(*(conflict["choices"] for conflict in issued))
    ):
        if index >= MAX_COMPONENT_STATES:
            raise UserError(
                f"Investment field-choice search ({len(issued)} conflicting "
                f"fields) exceeded {MAX_COMPONENT_STATES} combinations while "
                "resolving accounting coherence — its cost is exponential "
                "when several fields conflict with no coherent selection, "
                "so it refuses rather than risk a long stall. This happens "
                "when a multi-source event has several material "
                "differences across sources.",
                code=error_codes.INVESTMENT_MATCH_COMPONENT_TOO_LARGE,
            )
        proposed = {role: dict(row) for role, row in defaults.items()}
        for conflict, choice in zip(issued, selected, strict=True):
            proposed[conflict["leg_role"]][conflict["field"]] = choice["value"]
        if _coherent(list(proposed.values())):
            return tuple(issued)
    return None
