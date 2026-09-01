"""Shared compact cursor contract for stateless keyset pagination.

Lives beside :mod:`moneybin.protocol.envelope` because both surfaces page:
an agent driving ``moneybin transactions list --output json`` needs the same
skip-and-duplicate-free continuation an agent driving the ``transactions``
tool gets. Nothing here is MCP-specific — the namespace and scope a cursor
binds to are supplied by the caller, so each surface binds its own public
filter names.
"""

from __future__ import annotations

import base64
import binascii
import json
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from functools import cmp_to_key
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


def build_keyset_page[T](
    rows: Sequence[T],
    *,
    limit: int,
    key_of: Callable[[T], tuple[KeysetScalar, ...]],
    namespace: str,
    scope: Mapping[str, object],
    snapshot: tuple[KeysetScalar, ...] | None,
    total: int,
) -> tuple[list[T], str | None]:
    """Trim a ``limit + 1`` fetch to one page and mint its continuation.

    ``rows`` must be the over-fetch: one extra row is how a caller learns there
    is a next page without a second count. ``snapshot`` is the high-water key
    carried by the incoming cursor, or ``None`` on the first page — in which
    case the first row of this page becomes it, freezing the top of the walk.
    """
    page = list(rows[:limit])
    if len(rows) <= limit or not page:
        return page, None
    return page, encode_keyset_cursor(
        namespace=namespace,
        scope=scope,
        snapshot=snapshot if snapshot is not None else key_of(page[0]),
        after=key_of(page[-1]),
        total=total,
    )


class InvalidKeysetCursorError(ValueError):
    """A decoded cursor no page could have minted.

    Distinct from a bare ``ValueError`` so a surface can map a rejected cursor
    to its caller-facing error without also swallowing a programming error —
    an unorderable row key or a broken encode — raised from the same block.
    """


def validate_keyset_position(
    position: KeysetPosition,
    *,
    key_types: tuple[type, ...],
    directions: tuple[SortDirection, ...],
) -> None:
    """Reject a decoded position with a misshapen key or an inverted continuation.

    Cursors are unsigned base64 JSON, so a caller can forge one no page ever
    minted. An ``after`` that sorts *ahead* of its snapshot makes the
    continuation predicate weaker than the snapshot predicate instead of
    narrower, and the page re-serves rows page one already returned.

    Every surface paging from a *head* snapshot calls this, so that rejection
    is uniform across them. It does not fit the tail-snapshot surfaces; see
    :func:`paginate_keyset` for that distinction.
    """
    if len(key_types) != len(directions):
        raise ValueError("key_types and directions describe different keys")
    validate_keyset_shape(position, key_types=key_types)
    reject_inverted_keyset(position, directions)


def canonical_iso_date(value: str) -> str:
    """Return the one extended-ISO spelling of a day that sorts as the day does."""
    return date.fromisoformat(value).isoformat()


def canonical_iso_timestamp(value: str) -> str:
    """Return the one space-separated ISO spelling of an instant, or raise.

    Space-separated because that is what ``str()`` on a database timestamp
    produces, so a cursor this surface minted normalizes to itself unchanged.
    An offset-bearing value is refused rather than converted: an offset suffix
    makes the string sort by its wall-clock reading rather than its instant,
    which is the same defect canonicalizing exists to remove, and every
    timestamp these walks order is naive.
    """
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        raise ValueError("keyset cursor timestamp must be naive")
    return parsed.isoformat(sep=" ")


def canonicalize_keyset_element(
    position: KeysetPosition,
    *,
    index: int,
    canonicalize: Callable[[str], str],
) -> KeysetPosition:
    """Return the position with one key element replaced by its canonical form.

    Ordering is only sound when a key compares the way the value it denotes
    compares. A temporal element breaks that on its own: ISO 8601 admits
    several spellings of one day or instant whose text order contradicts their
    chronological order, so a forged cursor pairing two spellings can look
    like a valid continuation while being inverted. Every walk with a temporal
    element routes through here before its keys are ordered or handed to SQL.
    """

    def rewrite(key: tuple[KeysetScalar, ...]) -> tuple[KeysetScalar, ...]:
        value = key[index]
        if not isinstance(value, str):
            raise InvalidKeysetCursorError("invalid keyset cursor shape")
        return (*key[:index], canonicalize(value), *key[index + 1 :])

    return KeysetPosition(
        snapshot=rewrite(position.snapshot),
        after=rewrite(position.after),
        total=position.total,
    )


def validate_keyset_shape(
    position: KeysetPosition,
    *,
    key_types: tuple[type, ...],
) -> None:
    """Reject a decoded position whose keys are not the declared shape."""
    for key in (position.snapshot, position.after):
        if len(key) != len(key_types) or any(
            type(value) is not expected
            for value, expected in zip(key, key_types, strict=True)
        ):
            raise InvalidKeysetCursorError("invalid keyset cursor shape")


def reject_inverted_keyset(
    position: KeysetPosition,
    directions: tuple[SortDirection, ...],
) -> None:
    """Raise when the continuation key sorts ahead of the snapshot it continues.

    Compares the key values exactly as given, so a surface whose key is a
    *temporal string* must canonicalize it before calling this. ``date`` and
    ``datetime.fromisoformat`` each accept several spellings of one instant —
    ``T`` or space separator, basic ``20250101`` or extended ``2025-01-02``,
    with or without fractional seconds — and those spellings do not sort
    against each other the way the database sorts the values they denote. A
    forged cursor pairing two spellings would otherwise slip past this check
    while still being inverted, which is the very bug the check exists to stop.
    Surfaces with a temporal key therefore normalize first and pass the
    canonical form to both this guard and the SQL predicate.
    """
    if compare_keyset(position.after, position.snapshot, directions) < 0:
        raise InvalidKeysetCursorError("keyset continuation precedes its snapshot")


def paginate_keyset[T](
    rows: Sequence[T],
    *,
    limit: int,
    key_of: Callable[[T], tuple[KeysetScalar, ...]],
    directions: tuple[SortDirection, ...],
    namespace: str,
    scope: Mapping[str, object],
    position: KeysetPosition | None,
) -> tuple[list[T], str | None, int]:
    """Sort a whole result set into display order and serve one keyset page.

    The counterpart to :func:`build_keyset_page` for surfaces whose rows arrive
    as a complete in-memory list rather than a SQL over-fetch: the snapshot
    frozen on page one bounds the walk, so rows prepended mid-walk are excluded
    and the total stays stable. Returns the page, its continuation, and the
    total the walk is pinned to.

    This is the *head*-snapshot convention: the snapshot is the first row in
    display order and the walk runs away from it. ``investments``, ``taxonomy``
    and ``privacy`` page the mirror convention — the snapshot is the *last* row
    and the walk runs back toward it — which is self-consistent rather than
    unguarded, and is deliberately not folded in here: doing so would change
    which rows their cursors select.

    Raises :class:`InvalidKeysetCursorError` only for a cursor this page rejects.
    A row key that cannot be ordered, or a failed encode, raises a plain
    ``ValueError``: those are defects in the caller's rows, not in the cursor.
    """

    def compare_rows(left: T, right: T) -> int:
        return compare_keyset(key_of(left), key_of(right), directions)

    ordered = sorted(rows, key=cmp_to_key(compare_rows))
    if position is None:
        if not ordered:
            return [], None, 0
        snapshot = key_of(ordered[0])
        eligible = ordered
        total_count = len(ordered)
    else:
        try:
            reject_inverted_keyset(position, directions)
            eligible = [
                row
                for row in ordered
                if compare_keyset(key_of(row), position.snapshot, directions) >= 0
                and compare_keyset(key_of(row), position.after, directions) > 0
            ]
        except ValueError as exc:
            raise InvalidKeysetCursorError(str(exc)) from exc
        snapshot = position.snapshot
        total_count = position.total

    page = eligible[:limit]
    if len(eligible) <= limit or not page:
        return page, None, total_count
    return (
        page,
        encode_keyset_cursor(
            namespace=namespace,
            scope=scope,
            snapshot=snapshot,
            after=key_of(page[-1]),
            total=total_count,
        ),
        total_count,
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
