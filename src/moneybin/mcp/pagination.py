"""Shared compact cursor contract for stateless MCP keyset pagination."""

from __future__ import annotations

import base64
import binascii
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, cast

type KeysetScalar = str | int | float | bool | None
type SortDirection = Literal["asc", "desc"]

_CURSOR_VERSION = 1
_CURSOR_FIELDS = {
    "after",
    "namespace",
    "scope",
    "snapshot",
    "total",
    "version",
}


@dataclass(frozen=True)
class KeysetPosition:
    """Immutable high-water and continuation keys decoded from one cursor."""

    snapshot: tuple[KeysetScalar, ...]
    after: tuple[KeysetScalar, ...]
    total: int


def encode_keyset_cursor(
    *,
    namespace: str,
    scope: Mapping[str, object],
    snapshot: tuple[KeysetScalar, ...],
    after: tuple[KeysetScalar, ...],
    total: int,
) -> str:
    """Encode one versioned cursor bound to its namespace and canonical scope."""
    if (
        not snapshot
        or len(snapshot) != len(after)
        or not all(_is_scalar(item) for item in (*snapshot, *after))
        or isinstance(total, bool)
        or total < 1
    ):
        raise ValueError("invalid keyset cursor position")
    raw = json.dumps(
        {
            "after": list(after),
            "namespace": namespace,
            "scope": dict(scope),
            "snapshot": list(snapshot),
            "total": total,
            "version": _CURSOR_VERSION,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return base64.urlsafe_b64encode(raw).decode()


def decode_keyset_cursor(
    cursor: str,
    *,
    namespace: str,
    scope: Mapping[str, object],
) -> KeysetPosition:
    """Decode an exact namespace/scope-bound cursor or raise ``ValueError``."""
    try:
        decoded = base64.b64decode(cursor.encode(), altchars=b"-_", validate=True)
        value = json.loads(decoded)
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid keyset cursor") from exc
    if not isinstance(value, dict):
        raise ValueError("invalid keyset cursor")
    payload = cast(dict[str, object], value)
    snapshot = payload.get("snapshot")
    after = payload.get("after")
    total = payload.get("total")
    if (
        set(payload) != _CURSOR_FIELDS
        or payload.get("version") != _CURSOR_VERSION
        or payload.get("namespace") != namespace
        or payload.get("scope") != dict(scope)
        or isinstance(total, bool)
        or not isinstance(total, int)
        or total < 1
        or not isinstance(snapshot, list)
        or not isinstance(after, list)
    ):
        raise ValueError("invalid keyset cursor")
    snapshot_values = cast(list[object], snapshot)
    after_values = cast(list[object], after)
    if (
        not snapshot_values
        or len(snapshot_values) != len(after_values)
        or not all(_is_scalar(item) for item in (*snapshot_values, *after_values))
    ):
        raise ValueError("invalid keyset cursor")
    return KeysetPosition(
        snapshot=tuple(cast(list[KeysetScalar], snapshot_values)),
        after=tuple(cast(list[KeysetScalar], after_values)),
        total=total,
    )


def compare_keyset(
    left: tuple[KeysetScalar, ...],
    right: tuple[KeysetScalar, ...],
    directions: tuple[SortDirection, ...],
) -> int:
    """Compare keys in display order: negative means ``left`` comes first."""
    if len(left) != len(right) or len(left) != len(directions):
        raise ValueError("keyset shape does not match sort directions")
    for left_value, right_value, direction in zip(left, right, directions, strict=True):
        if left_value == right_value:
            continue
        if (
            left_value is None
            or right_value is None
            or type(left_value) is not type(right_value)
        ):
            raise ValueError("keyset values are not comparable")
        before = left_value < right_value  # type: ignore[operator]  # same scalar type checked above
        result = -1 if before else 1
        return result if direction == "asc" else -result
    return 0


def _is_scalar(value: object) -> bool:
    """Return whether a decoded cursor key value is a supported JSON scalar."""
    return (
        value is None
        or isinstance(value, (str, int, bool))
        or (isinstance(value, float) and math.isfinite(value))
    )
