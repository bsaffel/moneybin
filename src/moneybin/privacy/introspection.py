"""Type-hint walking for privacy classification.

Surface-crossing return types declare per-field privacy classes via
``Annotated[T, DataClass.X]``. This module walks those annotations
recursively (through ``list``, ``dict``, ``Optional``, ``tuple``,
nested dataclasses, Pydantic models, and TypedDicts) and returns the
set of ``DataClass`` values that appear in the type. ``derive_tier``
then takes the max tier across the set — that's the tool's effective
sensitivity.

A return type whose walk produces an empty set is a contract
violation: every surface-crossing return type must classify every
field, by construction. ``PrivacyContractError`` is raised — the
``@mcp_tool`` decorator catches it at registration time so a typo or
forgotten annotation fails at import, not at call time.

Per-type results are cached: types are hashable, derivation walks the
full tree once.
"""

from __future__ import annotations

import functools
import types
import typing
from collections.abc import Mapping
from typing import Annotated, Any, get_args, get_origin, get_type_hints

from moneybin.privacy.taxonomy import DataClass, Tier


class PrivacyContractError(Exception):
    """A surface-crossing return type failed the privacy classification contract."""


@functools.cache
def extract_data_classes(return_type: Any) -> frozenset[DataClass]:
    """Walk ``return_type`` and return every ``DataClass`` reachable via ``Annotated``.

    Recurses through ``list``, ``tuple``, ``set``, ``dict`` value types,
    ``Optional`` / ``Union``, and nested classes that themselves have
    type hints. Class walking treats dataclasses, Pydantic models, and
    TypedDicts uniformly via ``typing.get_type_hints(..., include_extras=True)``.

    Cached per-type — the full graph walk is expensive (``get_type_hints``
    resolves forward references) and the result is referentially stable for
    any given class object. Returns ``frozenset`` so the cached value is
    safe to share across callers; callers that need ordering use
    ``sorted(...)``.
    """
    found: set[DataClass] = set()
    _walk(return_type, found, seen=set())
    return frozenset(found)


def _walk(tp: Any, found: set[DataClass], seen: set[Any]) -> None:
    """Recursive helper. ``seen`` short-circuits cyclic / repeated types."""
    if tp is None or tp is type(None):
        return
    # Annotated[T, DataClass.X, ...] — extract DataClass metadata, then recurse into T.
    origin = get_origin(tp)
    if origin is Annotated:
        args = get_args(tp)
        # First arg is the underlying type; rest is metadata.
        for meta in args[1:]:
            if isinstance(meta, DataClass):
                found.add(meta)
        _walk(args[0], found, seen)
        return
    # Union / Optional — walk every arm.
    # Handle both typing.Union (Union[X, Y]) and types.UnionType (X | Y, Python 3.10+).
    if origin is typing.Union or isinstance(tp, types.UnionType):
        for arm in get_args(tp):
            _walk(arm, found, seen)
        return
    # list[T], set[T], frozenset[T], tuple[T, ...], etc. — walk the element type(s).
    if origin in (list, set, frozenset, tuple):
        for arg in get_args(tp):
            _walk(arg, found, seen)
        return
    # dict[K, V] / Mapping[K, V] — walk the value type (keys are RECORD_ID by convention).
    if origin in (dict, Mapping):
        args = get_args(tp)
        if len(args) >= 2:
            _walk(args[1], found, seen)
        return
    # User-defined class: dataclass, Pydantic model, TypedDict, plain class.
    # get_type_hints handles all three uniformly when include_extras=True.
    if isinstance(tp, type):
        if tp in seen:
            return
        seen.add(tp)
        try:
            hints = get_type_hints(tp, include_extras=True)
        except (NameError, TypeError):
            # Forward references that can't resolve in this scope, or
            # plain classes with no annotations — nothing to walk.
            return
        for hint in hints.values():
            _walk(hint, found, seen)


@functools.cache
def derive_tier(return_type: Any) -> Tier:
    """Return the max ``Tier`` across all classes found in ``return_type``.

    Raises ``PrivacyContractError`` if no classes are found — every
    surface-crossing return type must classify every field.
    """
    classes = extract_data_classes(return_type)
    if not classes:
        name = getattr(return_type, "__name__", repr(return_type))
        raise PrivacyContractError(
            f"Return type {name!r} has no Annotated[T, DataClass] metadata; "
            "every surface-crossing return type must classify every field."
        )
    return max(c.tier for c in classes)
