"""Every payload that carries a collection declares which one is its row set.

The runtime raise in ``moneybin.protocol.row_set`` is the backstop; this is the
guard that fires first. It runs over the whole payload package rather than over
the payloads some test happened to build, because the defect it prevents is a
new payload shipping with no declaration and picking up a count nobody chose.

Payloads with no list field are exempt: there is nothing to name, and adding a
list later brings the class into this test's scope in the same commit.
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
    """True when ``hint`` is a list, or a union / Annotated wrapping one."""
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


def _payload_classes_with_collections() -> list[type]:
    """Every payload dataclass / model in the package that carries a list."""
    import moneybin.privacy.payloads as pkg

    out: list[type] = []
    for mod_info in pkgutil.iter_modules(pkg.__path__):
        mod = importlib.import_module(f"moneybin.privacy.payloads.{mod_info.name}")
        for name in dir(mod):
            obj = getattr(mod, name)
            if not isinstance(obj, type) or obj.__module__ != mod.__name__:
                continue
            if not (is_dataclass(obj) or issubclass(obj, BaseModel)):
                continue
            if _list_field_names(obj):
                out.append(obj)
    return out


_PAYLOADS_WITH_COLLECTIONS = _payload_classes_with_collections()


@pytest.mark.unit
def test_the_scan_reaches_the_payloads_it_is_meant_to_guard() -> None:
    """A discovery bug would make every case below vacuously pass.

    The three payloads the declaration was designed against must be in the
    scanned set, and the set must be substantial rather than a handful the
    import walk happened to reach.
    """
    names = {cls.__name__ for cls in _PAYLOADS_WITH_COLLECTIONS}
    assert {
        "ReportResultPayload",
        "ImportInboxSyncPayload",
        "NetWorthSnapshotPayload",
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
