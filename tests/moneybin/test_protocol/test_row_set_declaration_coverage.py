"""Every payload that carries a collection declares which one is its row set.

The runtime raise in ``moneybin.protocol.row_set`` is the backstop; this is the
guard that fires first. It runs over whole modules rather than over the payloads
some test happened to build, because the defect it prevents is a new payload
shipping with no declaration — which is not a wrong count but a
``RowSetContractError`` on its first production call, on any command whose
envelope derives its own display currency.

The scanned roots are every module that defines a type used as envelope ``data``:
the payload package, plus the two modules that define envelope payloads outside
it. A new such module has to be added here, which is why the anti-vacuity test
below names a class from each root.

A payload is in scope for carrying a collection *or* for declaring one, and the
two sets are deliberately not nested. Carrying a list means the class owes a
declaration; declaring a name means that name owes a list. A class doing
neither is exempt — there is nothing to name, and adding a list later brings it
into scope in the same commit — but a declaration naming a scalar leaves its
class carrying no collection at all, so scoping this file to collections alone
would let exactly that case through.
"""

from __future__ import annotations

import importlib
import pkgutil
import typing
from dataclasses import fields, is_dataclass

import pytest
from pydantic import BaseModel

from moneybin.protocol.row_set import declared_row_set


def _is_list_hint(hint: object) -> bool:
    """True when ``hint`` is a list, or a union / Annotated wrapping one.

    Counts exactly what ``row_set_field`` counts — it decides with
    ``isinstance(value, list)`` — so ``tuple`` and other sequences are not
    collections here. Making the two disagree about what a collection is is
    the defect, whichever direction the disagreement runs.
    """
    # Bare ``list``: ``get_origin`` reports ``None`` for an unparameterized
    # annotation, so testing origin alone would call ``rows: list`` a scalar
    # and drop its class out of the sweep.
    if hint is list:
        return True
    origin = typing.get_origin(hint)
    if origin is list:
        return True
    if origin is None:
        return False
    return any(_is_list_hint(arg) for arg in typing.get_args(hint))


def _list_field_names(cls: type) -> list[str]:
    """The names of ``cls``'s list-typed fields, by declared type not value."""
    hints = typing.get_type_hints(cls, include_extras=True)
    if is_dataclass(cls):
        names = [item.name for item in fields(cls)]
    elif issubclass(cls, BaseModel):
        names = list(cls.model_fields)
    else:  # pragma: no cover - filtered out by _payload_classes_with_collections
        return []
    return [name for name in names if _is_list_hint(hints.get(name))]


def _payload_module_names() -> list[str]:
    """Every module defining a type that can be an envelope payload."""
    import moneybin.privacy.payloads as pkg

    return [
        f"moneybin.privacy.payloads.{mod_info.name}"
        for mod_info in pkgutil.iter_modules(pkg.__path__)
    ] + [
        # The export outputs the CLI renders through the same envelope.
        "moneybin.cli.output",
        # `ReportResult` / `CatalogReportResult`, which the MCP surface builds
        # envelopes over directly.
        "moneybin.reports._framework.execute",
    ]


def _payload_classes() -> list[type]:
    """Every payload dataclass / model defined in those modules."""
    out: list[type] = []
    for module_name in _payload_module_names():
        mod = importlib.import_module(module_name)
        for name in dir(mod):
            obj = getattr(mod, name)
            if not isinstance(obj, type) or obj.__module__ != mod.__name__:
                continue
            if not (is_dataclass(obj) or issubclass(obj, BaseModel)):
                continue
            out.append(obj)
    return out


_ALL_PAYLOADS = _payload_classes()

# Carries a collection, so it owes a declaration.
_PAYLOADS_WITH_COLLECTIONS = [cls for cls in _ALL_PAYLOADS if _list_field_names(cls)]

# Declares a named row set, so that name owes a collection. Deliberately not a
# subset of the list above: a declaration naming a scalar leaves its class with
# no collection at all, which is exactly the case that would otherwise escape.
_PAYLOADS_WITH_DECLARATIONS = [
    cls for cls in _ALL_PAYLOADS if isinstance(declared_row_set(cls), str)
]


@pytest.mark.unit
def test_the_scan_reaches_the_payloads_it_is_meant_to_guard() -> None:
    """A discovery bug would make every case below vacuously pass.

    The three payloads the declaration was designed against must be in the
    scanned set, one class from each non-package root must be too — a root
    dropped from the list above is otherwise invisible — and the set must be
    substantial rather than a handful the import walk happened to reach.
    """
    names = {cls.__name__ for cls in _PAYLOADS_WITH_COLLECTIONS}
    assert {
        "ReportResultPayload",
        "ImportInboxSyncPayload",
        "NetWorthSnapshotPayload",
        "ExportDestinationsOutput",
        "ReportResult",
    } <= names
    assert len(_PAYLOADS_WITH_COLLECTIONS) > 100


@pytest.mark.unit
@pytest.mark.parametrize(
    "payload_cls", _PAYLOADS_WITH_COLLECTIONS, ids=lambda c: c.__name__
)
def test_payload_declares_its_row_set(payload_cls: type) -> None:
    """Each payload names its row set, or states that it has none."""
    declaration = declared_row_set(payload_cls)
    assert declaration is not None, (
        f"{payload_cls.__name__} carries "
        f"{_list_field_names(payload_cls)} but declares no row set. Add "
        f"@row_set('<field>') for the collection it returns, or "
        f"@row_set(NO_ROW_SET) when no single field is that collection."
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "payload_cls", _PAYLOADS_WITH_DECLARATIONS, ids=lambda c: c.__name__
)
def test_a_declared_row_set_names_a_collection(payload_cls: type) -> None:
    """A declaration names a *list* field, not merely a field that exists.

    ``@row_set`` checks at class definition that the name is one of the
    payload's fields, and ``row_set_field`` checks at call time that the value
    is a list. Between those sits a payload whose declaration names a real but
    scalar field: the name check passes, and the class carries no list field so
    the sweep above never collects it. Nothing fails until the first production
    call.

    That is the shape a rebound decorator leaves behind. When a class inserted
    directly above a decorated one steals its ``@row_set``, the class that lost
    the decorator is caught twice over — undeclared here, and raising at
    runtime — but the thief keeps a declaration nobody wrote for it. If the
    thief happens to carry a field of the declared name, the steal is invisible
    from its side, and a diagnostic list that should have been ``NO_ROW_SET``
    silently becomes the row set: a wrong ``returned_count``, which is the
    defect this whole contract exists to remove.
    """
    declaration = declared_row_set(payload_cls)
    assert isinstance(declaration, str)
    list_fields = _list_field_names(payload_cls)
    assert declaration in list_fields, (
        f"{payload_cls.__name__} declares row set {declaration!r}, which is "
        f"not one of its collection fields ({list_fields or 'none'}). Either "
        f"the declaration names the wrong field, or it was rebound to this "
        f"class by an edit that inserted it below an existing @row_set."
    )


@pytest.mark.unit
def test_a_bare_list_annotation_counts_as_a_collection() -> None:
    """``rows: list`` is a collection, though ``get_origin`` reports ``None``.

    ``typing.get_origin(list)`` is ``None`` for an unparameterized annotation,
    so a helper keyed only on ``get_origin(hint) is list`` filters such a class
    out of the sweep entirely and reports it exempt. ``row_set_field`` still
    raises, because it tests the value with ``isinstance``, not the annotation
    — which inverts this module's stated design: the sweep is meant to fire
    first, and for this shape it never fires at all.

    No payload is annotated this way today and no lint rule forbids it, so the
    guard has to carry the case rather than rely on the tree staying clean.
    """
    assert _is_list_hint(list)
    assert _is_list_hint(list[str])
    assert _is_list_hint(list[str] | None)


@pytest.mark.unit
def test_a_tuple_annotation_is_not_a_collection() -> None:
    """The sweep counts exactly what ``row_set_field`` counts, and no more.

    ``row_set_field`` decides with ``isinstance(value, list)``, so a tuple
    field can never be a row set and can never trigger its raise. Counting
    tuples here would make the sweep demand a declaration for a class the
    runtime would never object to — a second disagreement in place of the one
    being fixed.
    """
    assert not _is_list_hint(tuple)
    assert not _is_list_hint(tuple[str, ...])
