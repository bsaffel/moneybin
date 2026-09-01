"""Tests for the shared MCP keyset cursor contract."""

from __future__ import annotations

import base64
import json

import pytest

from moneybin.protocol.pagination import (
    KeysetPosition,
    compare_keyset,
    decode_keyset_cursor,
    encode_keyset_cursor,
    paginate_keyset,
    validate_keyset_position,
)


def test_keyset_cursor_round_trips_and_binds_namespace_and_scope() -> None:
    cursor = encode_keyset_cursor(
        namespace="transactions",
        scope={"account_id": "ACC001", "category": None},
        snapshot=("2026-07-19", "txn-a"),
        after=("2026-07-18", "txn-b"),
        total=87,
    )

    assert decode_keyset_cursor(
        cursor,
        namespace="transactions",
        scope={"account_id": "ACC001", "category": None},
    ) == KeysetPosition(
        snapshot=("2026-07-19", "txn-a"),
        after=("2026-07-18", "txn-b"),
        total=87,
    )
    with pytest.raises(ValueError, match="invalid keyset cursor"):
        decode_keyset_cursor(
            cursor,
            namespace="reviews",
            scope={"account_id": "ACC001", "category": None},
        )
    with pytest.raises(ValueError, match="invalid keyset cursor"):
        decode_keyset_cursor(
            cursor,
            namespace="transactions",
            scope={"account_id": "ACC002", "category": None},
        )


@pytest.mark.parametrize(
    "cursor",
    [
        "not-base64",
        base64.urlsafe_b64encode(b"[]").decode(),
        base64.urlsafe_b64encode(
            json.dumps({
                "after": ["2026-07-18", "txn-b"],
                "namespace": "transactions",
                "scope": {},
                "snapshot": ["2026-07-19", "txn-a"],
                "total": 2,
                "version": 2,
            }).encode()
        ).decode(),
        base64.urlsafe_b64encode(
            json.dumps({
                "after": [["nested"]],
                "namespace": "transactions",
                "scope": {},
                "snapshot": ["2026-07-19"],
                "total": 2,
                "version": 1,
            }).encode()
        ).decode(),
    ],
)
def test_keyset_cursor_rejects_malformed_or_unknown_versions(cursor: str) -> None:
    with pytest.raises(ValueError, match="invalid keyset cursor"):
        decode_keyset_cursor(cursor, namespace="transactions", scope={})


def test_keyset_cursor_rejects_non_finite_float_keys() -> None:
    with pytest.raises(ValueError, match="invalid keyset cursor position"):
        encode_keyset_cursor(
            namespace="reviews",
            scope={},
            snapshot=(float("nan"), "decision-a"),
            after=(float("nan"), "decision-a"),
            total=1,
        )

    cursor = base64.urlsafe_b64encode(
        json.dumps({
            "after": [float("inf"), "decision-a"],
            "namespace": "reviews",
            "scope": {},
            "snapshot": [float("inf"), "decision-a"],
            "total": 1,
            "version": 1,
        }).encode()
    ).decode()
    with pytest.raises(ValueError, match="invalid keyset cursor"):
        decode_keyset_cursor(cursor, namespace="reviews", scope={})


def test_validate_keyset_position_rejects_continuation_before_its_snapshot() -> None:
    """An `after` ahead of its snapshot re-serves rows page one already gave.

    Rows sort `date DESC, id ASC`, so the newer date sorts *ahead*. The
    snapshot predicate keeps rows at or below the snapshot and the `after`
    predicate is supposed to narrow that further; inverted, it widens it back
    to the whole first page.
    """
    inverted = KeysetPosition(
        snapshot=("2026-07-18", "txn-b"),
        after=("2026-07-19", "txn-a"),
        total=2,
    )

    with pytest.raises(ValueError, match="continuation precedes"):
        validate_keyset_position(
            inverted, key_types=(str, str), directions=("desc", "asc")
        )


def test_validate_keyset_position_accepts_a_continuation_at_its_snapshot() -> None:
    """A one-row first page mints `after == snapshot`; that is not inverted."""
    boundary = KeysetPosition(
        snapshot=("2026-07-19", "txn-a"),
        after=("2026-07-19", "txn-a"),
        total=2,
    )

    validate_keyset_position(boundary, key_types=(str, str), directions=("desc", "asc"))


@pytest.mark.parametrize(
    ("snapshot", "after", "key_types"),
    [
        (("2026-07-19",), ("2026-07-18",), (str, str)),
        (("2026-07-19", 1), ("2026-07-18", 2), (str, str)),
        ((True, "txn-a"), (True, "txn-b"), (int, str)),
    ],
    ids=["short-key", "wrong-scalar-type", "bool-is-not-int"],
)
def test_validate_keyset_position_rejects_keys_of_the_wrong_shape(
    snapshot: tuple[object, ...],
    after: tuple[object, ...],
    key_types: tuple[type, ...],
) -> None:
    position = KeysetPosition(
        snapshot=snapshot,  # type: ignore[arg-type]
        after=after,  # type: ignore[arg-type]
        total=2,
    )

    with pytest.raises(ValueError, match="invalid keyset cursor shape"):
        validate_keyset_position(
            position, key_types=key_types, directions=("asc",) * len(key_types)
        )


def _first_element(row: str) -> tuple[str]:
    return (row,)


def test_paginate_keyset_freezes_the_snapshot_and_walks_without_repeats() -> None:
    """The frozen snapshot excludes a later prepend and preserves the total."""
    page, cursor, total = paginate_keyset(
        ["c", "a", "b"],
        limit=2,
        key_of=_first_element,
        directions=("asc",),
        namespace="reviews",
        scope={},
        position=None,
    )

    assert page == ["a", "b"]
    assert total == 3
    assert cursor is not None

    position = decode_keyset_cursor(cursor, namespace="reviews", scope={})
    second, next_cursor, second_total = paginate_keyset(
        ["0", "c", "a", "b"],
        limit=2,
        key_of=_first_element,
        directions=("asc",),
        namespace="reviews",
        scope={},
        position=position,
    )

    assert second == ["c"]
    assert next_cursor is None
    assert second_total == 3


def test_paginate_keyset_rejects_continuation_before_its_snapshot() -> None:
    inverted = KeysetPosition(snapshot=("b",), after=("a",), total=3)

    with pytest.raises(ValueError, match="continuation precedes"):
        paginate_keyset(
            ["a", "b", "c"],
            limit=2,
            key_of=_first_element,
            directions=("asc",),
            namespace="reviews",
            scope={},
            position=inverted,
        )


def test_compare_keyset_supports_mixed_sort_directions() -> None:
    directions = ("desc", "asc")

    assert (
        compare_keyset(
            ("2026-07-19", "txn-a"),
            ("2026-07-18", "txn-b"),
            directions,
        )
        < 0
    )
    assert (
        compare_keyset(
            ("2026-07-19", "txn-a"),
            ("2026-07-19", "txn-b"),
            directions,
        )
        < 0
    )
    assert (
        compare_keyset(
            ("2026-07-19", "txn-b"),
            ("2026-07-19", "txn-a"),
            directions,
        )
        > 0
    )
