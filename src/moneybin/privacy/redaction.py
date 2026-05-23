"""Per-class field redaction for surface-crossing payloads.

The redactor walks a typed payload, inspects each field's
``Annotated[..., DataClass.X]`` metadata, and applies the per-class
transform. The output is structurally identical to the input — same
dataclass shape, same nesting — with values replaced where the class
demands it.

PR 2 scope: CRITICAL-tier classes (ACCOUNT_IDENTIFIER, ROUTING_NUMBER,
INSTITUTION_ACCOUNT_NUMBER) are always masked. HIGH / MEDIUM / LOW tiers
pass through. PR 3 will add consent-gated transforms (bucketing for
amounts, hash placeholders for free text, date-shifting for txn_date)
that consult the ``ConsentSet`` argument.

The signature accepts ``consent: ConsentSet | None`` even in PR 2 so
the PR-3 changes are call-site updates only — the redactor's shape
doesn't change between PRs.

This module is the single bottleneck for "what gets masked." Future
classes added to ``DataClass`` MUST have an entry in
``_TRANSFORMS`` — the unit tests will fail otherwise.
"""

from __future__ import annotations

import functools
import logging
import types
import typing
from collections.abc import Mapping
from dataclasses import dataclass, fields, is_dataclass, replace
from typing import Annotated, Any, cast, get_args, get_origin, get_type_hints

from pydantic import BaseModel

from moneybin.privacy.taxonomy import DataClass

logger = logging.getLogger(__name__)


@functools.cache
def _cached_type_hints(cls: type) -> dict[str, Any]:
    """Return ``get_type_hints(cls, include_extras=True)``, cached per-class.

    ``redact_typed`` walks every dataclass / TypedDict / annotated class on
    every call. ``get_type_hints`` is not cheap — it resolves forward
    references, walks ``__annotations__``, and unwraps ``Annotated``. For
    a 100-row list payload the same class is hit 100 times. Caching
    collapses that to one call per unique class for the process lifetime.
    Type annotations on a class do not change at runtime, so the cache is
    safe across the entire test suite.
    """
    return get_type_hints(cls, include_extras=True)


@dataclass(frozen=True)
class ConsentSet:
    """Active consent grants for the current call.

    PR 2 placeholder — no fields. PR 3 fills this in (e.g.
    ``mcp_data_sharing: bool``, ``ai_external_calls: bool``,
    ``granted_at: datetime``). Defined now so per-class transforms
    have a stable signature.
    """


def _mask_account_identifier(
    value: str | None, _consent: ConsentSet | None
) -> str | None:
    """ACCOUNT_IDENTIFIER → ``"****" + value[-4:]`` (or ``"****"`` if shorter)."""
    if value is None:
        return None
    return "****" + value[-4:] if len(value) >= 4 else "****"


def _mask_routing_number(value: str | None, _consent: ConsentSet | None) -> str | None:
    """ROUTING_NUMBER → constant ``"*****"`` (or ``None`` for nullable)."""
    if value is None:
        return None
    return "*****"


def _passthrough(value: Any, _consent: ConsentSet | None) -> Any:
    return value


_TRANSFORMS: dict[DataClass, Any] = {
    DataClass.ACCOUNT_IDENTIFIER: _mask_account_identifier,
    DataClass.INSTITUTION_ACCOUNT_NUMBER: _mask_account_identifier,
    DataClass.ROUTING_NUMBER: _mask_routing_number,
    # HIGH-tier — pass through in PR 2 (PR 3 adds bucketing).
    DataClass.BALANCE: _passthrough,
    DataClass.TXN_AMOUNT: _passthrough,
    DataClass.INCOME_AMOUNT: _passthrough,
    # MEDIUM-tier — pass through in PR 2 (PR 3 adds hash placeholders / date shift).
    DataClass.MERCHANT_NAME: _passthrough,
    DataClass.DESCRIPTION: _passthrough,
    DataClass.USER_NOTE: _passthrough,
    DataClass.TXN_DATE: _passthrough,
    # LOW-tier — always pass through.
    DataClass.CATEGORY: _passthrough,
    DataClass.INSTITUTION: _passthrough,
    DataClass.CURRENCY: _passthrough,
    DataClass.TXN_TYPE: _passthrough,
    DataClass.AGGREGATE: _passthrough,
    DataClass.RECORD_ID: _passthrough,
    DataClass.TIMESTAMP_OBSERVABILITY: _passthrough,
}


def has_active_transform(payload_type: Any) -> bool:
    """Return True if redact_typed would actually change a value in ``payload_type``.

    True when any field's DataClass maps to a non-passthrough transform.
    The decorator/CLI use this to skip the redaction walk for payloads that
    would pass through unchanged. Deriving from ``_TRANSFORMS`` (not from
    ``tier == CRITICAL``) keeps the gate correct automatically when PR3 wires
    real HIGH/MEDIUM transforms: the moment a class stops being ``_passthrough``,
    every payload carrying it starts being walked. Avoids the documented trap
    where a CRITICAL-only gate silently skips newly-maskable HIGH/MEDIUM tools.
    """
    from moneybin.privacy.introspection import (  # noqa: PLC0415 — avoid import cycle
        extract_data_classes,
    )

    return any(
        _TRANSFORMS.get(dc, _passthrough) is not _passthrough
        for dc in extract_data_classes(payload_type)
    )


def _scrub_embedded_pii(text: str) -> str:  # pyright: ignore[reportUnusedFunction]
    """No-op identity for v1.

    Reserved for a future Presidio integration that detects and masks
    SSNs, phone numbers, etc. that may appear inside ``DESCRIPTION``
    or ``USER_NOTE`` strings even when their containing field's class
    doesn't require redaction. Tracked as a deferred follow-up.

    Prefixed with underscore to signal "internal, subject to change." The
    test suite imports it directly to document the PR-2 no-op contract and
    to catch the moment a future PR changes the behaviour.
    """
    return text


def redact_typed(obj: Any, consent: ConsentSet | None) -> Any:
    """Walk ``obj`` recursively and return a redacted copy.

    The structure is preserved (dataclasses → dataclasses with the
    same shape, lists → lists, etc.). Only field values where the
    field's ``Annotated`` metadata includes a ``DataClass`` are
    transformed; everything else is copied through.
    """
    return _redact(obj, consent, type(obj))


def _redact(value: Any, consent: ConsentSet | None, declared_type: Any) -> Any:
    """Internal recursion: ``declared_type`` carries Annotated metadata."""
    if value is None:
        return None
    origin = get_origin(declared_type)
    # Annotated[T, DataClass.X, ...] — apply the first DataClass transform we find.
    if origin is Annotated:
        args = get_args(declared_type)
        for meta in args[1:]:
            if isinstance(meta, DataClass):
                fn = _TRANSFORMS.get(meta, _passthrough)
                return fn(value, consent)
        # No DataClass metadata — recurse into the underlying type.
        return _redact(value, consent, args[0])
    # Fixed-length heterogeneous tuple[A, B, ...] — redact each position with its
    # own declared type. tuple[T, ...] (homogeneous, Ellipsis) and the other
    # sequence origins fall through to the single-element-type branch below.
    if origin is tuple:
        targs = get_args(declared_type)
        is_variadic = len(targs) == 2 and targs[1] is Ellipsis
        if targs and not is_variadic:
            return tuple(
                _redact(v, consent, t) for v, t in zip(value, targs, strict=False)
            )
    # list[T] / set[T] / frozenset[T] / tuple[T, ...] — recurse per-element.
    # Must mirror introspection._walk, which classifies all four origins; if
    # _redact handled fewer, a CRITICAL field inside a set/frozenset would be
    # flagged has_critical by the walk but never actually masked here.
    if origin in (list, set, frozenset, tuple):
        elem_type = get_args(declared_type)[0] if get_args(declared_type) else Any
        redacted = [_redact(v, consent, elem_type) for v in value]
        # Reconstruct the original container type (list→list, set→set, etc.).
        return redacted if origin is list else type(value)(redacted)
    # dict[K, V] / Mapping[K, V] — recurse per-value (keys are not redacted).
    # Mapping included to mirror introspection._walk's (dict, Mapping) origins.
    if origin in (dict, Mapping):
        args = get_args(declared_type)
        val_type = args[1] if len(args) >= 2 else Any
        return {k: _redact(v, consent, val_type) for k, v in value.items()}
    # Union / Optional — pick the arm matching the runtime type.
    # PEP 604 unions (X | None) use types.UnionType; typing.Union uses typing.Union.
    if origin is typing.Union or isinstance(declared_type, types.UnionType):
        for arm in get_args(declared_type):
            if arm is type(None):
                if value is None:
                    return None
                continue
            # Outer-Optional Annotated arm, e.g. Optional[Annotated[str,
            # DataClass.ROUTING_NUMBER]]. get_origin(arm) is typing.Annotated,
            # which isinstance() rejects — so without this branch the arm is
            # skipped and a CRITICAL value falls through UNREDACTED. Match the
            # underlying type, then recurse with the full Annotated alias so its
            # DataClass still drives masking.
            if get_origin(arm) is Annotated:
                underlying = get_args(arm)[0]
                underlying_check = get_origin(underlying) or underlying
                try:
                    if isinstance(value, underlying_check):
                        return _redact(value, consent, arm)
                except TypeError:
                    pass
                continue
            # Parameterized generic aliases (list[X], dict[K, V]) raise on
            # isinstance — match against the origin, then recurse with the full
            # alias so element/value types are still redacted. Without this the
            # arm is silently skipped and the value falls through unredacted.
            arm_check = get_origin(arm) or arm
            try:
                if isinstance(value, arm_check):
                    return _redact(value, consent, arm)
            except TypeError:
                continue
        return value
    # Dataclass — rebuild with redacted fields.
    if is_dataclass(value) and isinstance(value, type) is False:
        # cast: is_dataclass(x) with isinstance(x, type) is False guarantees
        # x is a dataclass instance, but pyright can't narrow through is_dataclass().
        dc_instance = cast(Any, value)
        # pyright: ignore[reportUnknownArgumentType] — dc_instance is Any by cast design.
        hints = _cached_type_hints(type(dc_instance))  # pyright: ignore[reportUnknownArgumentType]
        kwargs: dict[str, Any] = {}
        for f in fields(dc_instance):
            field_value = getattr(dc_instance, f.name)
            field_type = hints.get(f.name, Any)
            kwargs[f.name] = _redact(field_value, consent, field_type)
        return replace(dc_instance, **kwargs)
    # Pydantic BaseModel — rebuild with redacted fields. build_envelope and
    # _count_pydantic_payload accept BaseModel payloads, so redaction must
    # traverse them too; otherwise a Pydantic payload with CRITICAL Annotated
    # fields leaks raw values. isinstance(BaseModel) — not a duck-type check —
    # to avoid the MagicMock-chain trap (see _PayloadEncoder).
    if isinstance(value, BaseModel):
        model_hints = _cached_type_hints(type(value))
        updates: dict[str, Any] = {}
        for name in type(value).model_fields:
            field_value = getattr(value, name)
            field_type = model_hints.get(name, Any)
            updates[name] = _redact(field_value, consent, field_type)
        # model_copy(update=...) substitutes same-typed redacted values without
        # re-validating — the shape is unchanged, only leaf values are masked.
        return value.model_copy(update=updates)
    # TypedDict instance (which is just a dict at runtime).
    if isinstance(value, dict) and isinstance(declared_type, type):
        try:
            hints: dict[str, Any] = _cached_type_hints(declared_type)
        except (NameError, TypeError):
            # Unresolved hints → we can't tell which fields are CRITICAL. Falling
            # through silently would return the dict unredacted with no signal
            # (the plain-class branch below only warns for non-dataclasses with
            # resolvable hints). Warn here so a misconfigured TypedDict payload
            # surfaces instead of leaking.
            logger.warning(
                f"redact_typed: could not resolve type hints for TypedDict "
                f"{declared_type.__name__}; passing through unredacted"
            )
            hints = {}
        if hints:
            return {
                k: _redact(v, consent, hints.get(str(k), Any))  # pyright: ignore[reportUnknownArgumentType]
                for k, v in value.items()
            }
    # Plain class with annotations — try the same dataclass-style rebuild.
    if isinstance(declared_type, type) and hasattr(declared_type, "__annotations__"):
        try:
            hints = _cached_type_hints(declared_type)
        except (NameError, TypeError):
            hints = {}
        if hints and not is_dataclass(value):  # pyright: ignore[reportUnknownArgumentType]
            # Without dataclass support we can't safely rebuild — warn and pass through.
            logger.warning(
                f"redact_typed: cannot rebuild {declared_type.__name__} "
                "(not a dataclass / TypedDict); passing through unredacted"
            )
            return value
    # Primitive / unhandled — pass through.
    return value
