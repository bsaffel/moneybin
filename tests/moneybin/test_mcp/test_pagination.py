"""Tests for the shared MCP keyset cursor contract."""

from __future__ import annotations

import base64
import json

import pytest

from moneybin.mcp.pagination import (
    KeysetPosition,
    compare_keyset,
    decode_keyset_cursor,
    encode_keyset_cursor,
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
