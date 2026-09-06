"""Which of a payload's fields is the collection it returned.

``summary.returned_count``, the display-currency derivation, and the CLI's
``--json-fields`` projection all need one answer to that question, and they all
have to agree — a payload whose count describes one field and whose projection
narrows another is worse than either alone.

The answer used to be inferred: the sole list field not named in a shared
auxiliary set. Two collections meant "several, so neither", so a report result
counted 1 however many rows it returned, and ``--json-fields`` silently did
nothing on the same payloads. Widening the auxiliary set to fix one payload
silently re-ranked every other payload sharing a field name.

Each payload now states the answer once, at the type:

    @row_set("rows")
    @dataclass(frozen=True, slots=True)
    class ThingListPayload:
        rows: list[ThingRow]
        warnings: list[str]

    @row_set(NO_ROW_SET)
    @dataclass(frozen=True, slots=True)
    class SnapshotPayload:
        per_currency: list[Segment]
        per_account: list[Row]

``NO_ROW_SET`` is a declaration, not an absence: it says no single field is the
collection, so the count describes the payload itself — one snapshot, one sync
run, one write result. A payload that carries a collection and declares nothing
raises, because a declaration that can quietly go missing or out of date is the
inference this replaced, one level up.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import fields, is_dataclass
from typing import Any, Final, TypeVar, cast

from pydantic import BaseModel


class RowSetContractError(Exception):
    """A payload's row-set declaration is missing, stale, or names a non-list."""


class _NoRowSet:
    """The declared absence of a row set. Use the ``NO_ROW_SET`` singleton."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "NO_ROW_SET"


NO_ROW_SET: Final[_NoRowSet] = _NoRowSet()

# Set on the decorated class itself. Read out of ``__dict__`` rather than by
# attribute lookup so a subclass never inherits its parent's answer: a subclass
# that adds or renames a collection has to say so for itself.
_DECLARATION: Final = "__moneybin_row_set__"

_PayloadType = TypeVar("_PayloadType", bound=type)


def row_set(declaration: str | _NoRowSet) -> Callable[[_PayloadType], _PayloadType]:
    """Declare which field of a payload is the collection it returns.

    Args:
        declaration: The field name holding the returned rows, or
            ``NO_ROW_SET`` when no single field is that collection.

    Raises:
        RowSetContractError: The named field is not one of the payload's
            fields — the declaration was written against a field that has
            since been renamed or removed. Raised at class definition, so a
            rename cannot reach a caller as a wrong count.
    """

    def decorate(payload_type: _PayloadType) -> _PayloadType:
        if isinstance(declaration, str):
            names = _field_names(payload_type)
            if declaration not in names:
                raise RowSetContractError(
                    f"{payload_type.__name__} declares row set "
                    f"{declaration!r}, which is not one of its fields "
                    f"({', '.join(names)})"
                )
        setattr(payload_type, _DECLARATION, declaration)
        return payload_type

    return decorate


def declared_row_set(payload_type: type) -> str | _NoRowSet | None:
    """The declaration on this exact type, or ``None`` when it declares nothing."""
    return cast("str | _NoRowSet | None", vars(payload_type).get(_DECLARATION))


def row_set_field(payload: Any) -> str | None:
    """The name of the field holding ``payload``'s rows, or ``None`` for no row set.

    Raises:
        RowSetContractError: ``payload`` carries a collection but its type
            declares nothing, or the declared field does not hold a list.
    """
    payload_type: type = type(payload)
    declaration = declared_row_set(payload_type)
    if declaration is NO_ROW_SET:
        return None
    if isinstance(declaration, str):
        if not isinstance(getattr(payload, declaration), list):
            raise RowSetContractError(
                f"{payload_type.__name__} declares row set {declaration!r}, "
                "which does not hold a list"
            )
        return declaration
    carried = [
        name
        for name in _field_names(payload_type)
        if isinstance(getattr(payload, name), list)
    ]
    if carried:
        raise RowSetContractError(
            f"{payload_type.__name__} carries {', '.join(carried)} but declares "
            "no row set. Add @row_set('<field>') for the collection it returns, "
            "or @row_set(NO_ROW_SET) when no single field is that collection."
        )
    return None


def row_set_rows(payload: Any) -> list[Any] | None:
    """``payload``'s declared rows, or ``None`` when it declares no row set."""
    name = row_set_field(payload)
    return None if name is None else cast("list[Any]", getattr(payload, name))


def _field_names(payload_type: type) -> list[str]:
    """The payload's field names, however the type declares them."""
    if is_dataclass(payload_type):
        return [item.name for item in fields(payload_type)]
    if issubclass(payload_type, BaseModel):
        return list(payload_type.model_fields)
    return []
