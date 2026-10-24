"""Tests for the shared MCP keyset cursor contract."""

from __future__ import annotations

import base64
import json

import pytest

from moneybin.protocol.pagination import (
    InvalidKeysetCursorError,
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

    with pytest.raises(InvalidKeysetCursorError, match="continuation precedes"):
        paginate_keyset(
            ["a", "b", "c"],
            limit=2,
            key_of=_first_element,
            directions=("asc",),
            namespace="reviews",
            scope={},
            position=inverted,
        )


def test_paginate_keyset_does_not_blame_the_cursor_for_unorderable_rows() -> None:
    """A first page carries no cursor, so its failure must not read as one.

    Mixed-type keys make ``compare_keyset`` raise while sorting. That is a
    defect in the caller's rows; reporting it as an invalid cursor would send
    a caller chasing a cursor they never sent.
    """
    with pytest.raises(ValueError, match="not comparable") as caught:
        paginate_keyset(
            ["a", 1],
            limit=2,
            key_of=lambda row: (row,),
            directions=("asc",),
            namespace="reviews",
            scope={},
            position=None,
        )

    assert not isinstance(caught.value, InvalidKeysetCursorError)


def test_transaction_bounds_reject_an_inversion_spelled_in_two_iso_forms() -> None:
    """Mixing ISO spellings must not smuggle an inverted continuation through.

    `date.fromisoformat` accepts basic `20250101` and extended `2025-01-02`
    alike, and those two sort against each other backwards from the days they
    denote: as raw strings the later day looks like it sorts behind. The keys
    are canonicalized before the ordering guard, so the comparison sees the
    days DuckDB will see rather than the characters the caller chose.
    """
    from moneybin.services.transaction_service import transaction_keyset_bounds

    inverted = KeysetPosition(
        snapshot=("20250101", "txn_a"),
        after=("2025-01-02", "txn_z"),
        total=5,
    )

    with pytest.raises(InvalidKeysetCursorError, match="continuation precedes"):
        transaction_keyset_bounds(inverted)


def test_transaction_bounds_canonicalize_an_accepted_key() -> None:
    """A valid basic-form day still pages, normalized to the extended form."""
    from moneybin.services.transaction_service import transaction_keyset_bounds

    snapshot, after = transaction_keyset_bounds(
        KeysetPosition(
            snapshot=("20250102", "txn_a"),
            after=("2025-01-01", "txn_z"),
            total=5,
        )
    )

    assert snapshot == ("2025-01-02", "txn_a")
    assert after == ("2025-01-01", "txn_z")


def test_validate_keyset_position_rejects_a_mismatched_contract() -> None:
    """Declaring more key types than directions is a bug, not a bad cursor."""
    position = KeysetPosition(snapshot=("a", "b"), after=("a", "c"), total=2)

    with pytest.raises(ValueError, match="describe different keys") as caught:
        validate_keyset_position(position, key_types=(str, str), directions=("asc",))
    assert not isinstance(caught.value, InvalidKeysetCursorError)


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


def test_review_position_rejects_an_inversion_spelled_in_two_iso_forms() -> None:
    """The history queue keys on an instant, so it needs the same guard.

    `2025-06-01 02:00:00` is the later instant but sorts behind the `T` form as
    raw text, so an uncanonicalized guard reads this pair as a continuation.
    """
    from moneybin.errors import UserError
    from moneybin.mcp.tools.reviews import (
        _review_position,  # pyright: ignore[reportPrivateUsage]
    )

    cursor = encode_keyset_cursor(
        namespace="reviews",
        scope={"kind": "matches", "status": "history", "temporal_contract": 2},
        snapshot=("2025-06-01T01:00:00Z", "dec-a"),
        after=("2025-06-01 02:00:00+00:00", "dec-b"),
        total=2,
    )

    with pytest.raises(UserError) as caught:
        _review_position(cursor, kind="matches", status="history")

    assert caught.value.code == "review_cursor_invalid"


def test_review_position_keeps_the_empty_timestamp_a_page_can_mint() -> None:
    """`_review_ordering` emits "" for a row with no timestamp; it must page."""
    from moneybin.mcp.tools.reviews import (
        _review_position,  # pyright: ignore[reportPrivateUsage]
    )

    cursor = encode_keyset_cursor(
        namespace="reviews",
        scope={"kind": "matches", "status": "history", "temporal_contract": 2},
        snapshot=("", "dec-a"),
        after=("", "dec-b"),
        total=2,
    )

    position = _review_position(cursor, kind="matches", status="history")

    assert position is not None
    assert position.snapshot == ("", "dec-a")


def test_balance_position_rejects_an_inversion_spelled_in_two_iso_forms() -> None:
    """The balances history walk keys on a day and needs the same guard.

    Ascending by date, `20250601` is the earlier day and so sorts ahead of the
    `2025-06-02` snapshot — inverted — yet as raw text it reads as behind it.
    """
    from moneybin.errors import UserError
    from moneybin.mcp.tools.accounts import (
        _BALANCE_KEY_DIRECTIONS,  # pyright: ignore[reportPrivateUsage]
        _coarse_position,  # pyright: ignore[reportPrivateUsage]
    )

    cursor = encode_keyset_cursor(
        namespace="accounts_balances",
        scope={"filters": {}, "view": "history"},
        snapshot=("2025-06-02", "acct-a"),
        after=("20250601", "acct-b"),
        total=2,
    )

    with pytest.raises(UserError) as caught:
        _coarse_position(
            cursor,
            tool="accounts_balances",
            view="history",
            filters={},
            directions=_BALANCE_KEY_DIRECTIONS["history"],
            date_index=0,
        )

    assert caught.value.code == "account_balance_cursor_invalid"


def test_canonical_iso_timestamp_normalizes_offsets_and_refuses_naive() -> None:
    """Strict awareness and one UTC spelling make temporal text sortable."""
    from moneybin.protocol.pagination import canonical_iso_timestamp

    assert (
        canonical_iso_timestamp("2025-06-01T09:00:00+09:00")
        == "2025-06-01T00:00:00.000000Z"
    )
    with pytest.raises(ValueError, match="explicit UTC offset"):
        canonical_iso_timestamp("2025-06-01T05:00:00")


def test_audit_bounds_normalize_an_offset_bearing_timestamp() -> None:
    from moneybin.mcp.tools.system import (
        _audit_bounds,  # pyright: ignore[reportPrivateUsage]
    )

    aware = KeysetPosition(
        snapshot=("2025-06-01T10:00:00+09:00", "audit-a"),
        after=("2025-06-01T09:00:00+09:00", "audit-b"),
        total=2,
    )
    assert _audit_bounds(aware) == (
        ("2025-06-01T01:00:00.000000Z", "audit-a"),
        ("2025-06-01T00:00:00.000000Z", "audit-b"),
    )


def test_import_position_normalizes_an_offset_bearing_timestamp() -> None:
    from moneybin.mcp.tools.import_tools import (
        _import_status_position,  # pyright: ignore[reportPrivateUsage]
    )

    cursor = encode_keyset_cursor(
        namespace="import_status.imports",
        scope={"import_id": None, "sections": ["imports"], "temporal_contract": 2},
        snapshot=("2025-06-01T10:00:00+09:00", "imp_a", 2),
        after=("2025-06-01T09:00:00+09:00", "imp_b", 2),
        total=2,
    )
    position = _import_status_position(cursor, sections=["imports"])
    assert position == KeysetPosition(
        snapshot=("2025-06-01T01:00:00.000000Z", "imp_a", 2),
        after=("2025-06-01T00:00:00.000000Z", "imp_b", 2),
        total=2,
    )


def test_canonicalize_keyset_element_fails_closed_on_a_short_key() -> None:
    """A forged short key must be refused, never crash on the index.

    The temporal element is addressed by position, so a key with fewer
    elements than the index reaches a raw `key[index]`. An IndexError is not a
    ValueError, so it would escape every cursor handler and surface as a crash
    instead of the refusal a forged cursor is supposed to get.
    """
    from moneybin.protocol.pagination import (
        canonical_iso_date,
        canonicalize_keyset_element,
    )

    short = KeysetPosition(snapshot=("acct-a",), after=("acct-b",), total=2)

    with pytest.raises(InvalidKeysetCursorError, match="shape"):
        canonicalize_keyset_element(short, index=1, canonicalize=canonical_iso_date)


def test_balance_position_refuses_a_short_forged_key() -> None:
    """The assertions view keys its day second, so a one-element key is short."""
    from moneybin.errors import UserError
    from moneybin.mcp.tools.accounts import (
        _BALANCE_KEY_DIRECTIONS,  # pyright: ignore[reportPrivateUsage]
        _coarse_position,  # pyright: ignore[reportPrivateUsage]
    )

    cursor = encode_keyset_cursor(
        namespace="accounts_balances",
        scope={"filters": {}, "view": "assertions"},
        snapshot=("acct-a",),
        after=("acct-b",),
        total=2,
    )

    with pytest.raises(UserError) as caught:
        _coarse_position(
            cursor,
            tool="accounts_balances",
            view="assertions",
            filters={},
            directions=_BALANCE_KEY_DIRECTIONS["assertions"],
            date_index=1,
        )

    assert caught.value.code == "account_balance_cursor_invalid"


def test_temporal_cursor_offset_equivalence_and_ties() -> None:
    """Equivalent instant keys keep their id tie-breaker across every page."""
    from types import SimpleNamespace

    from moneybin.mcp.tools.reviews import (
        _review_page,  # pyright: ignore[reportPrivateUsage]
        _review_position,  # pyright: ignore[reportPrivateUsage]
    )

    rows = [
        SimpleNamespace(created_at="2026-10-01T09:00:00-04:00", decision_id="a"),
        SimpleNamespace(created_at="2026-10-01T13:00:00.000000Z", decision_id="b"),
        SimpleNamespace(created_at="2026-10-01T13:00:00.1Z", decision_id="c"),
    ]
    position = None
    seen: list[str] = []
    while True:
        page, cursor = _review_page(
            rows, kind="matches", status="history", limit=1, position=position
        )
        seen.extend(row.decision_id for row in page)
        if cursor is None:
            break
        position = _review_position(cursor, kind="matches", status="history")
        assert position is not None
        minted = decode_keyset_cursor(
            cursor,
            namespace="reviews",
            scope={"kind": "matches", "status": "history", "temporal_contract": 2},
        )
        assert position == minted
    assert seen == ["c", "a", "b"]


@pytest.mark.parametrize("surface", ["audit", "imports", "reviews"])
def test_legacy_temporal_cursor_requires_restart(surface: str) -> None:
    """Legacy timestamp scopes cannot silently continue under UTC ordering."""
    from moneybin.errors import UserError
    from moneybin.mcp.tools.import_tools import (
        _import_status_position,  # pyright: ignore[reportPrivateUsage]
    )
    from moneybin.mcp.tools.reviews import (
        _review_position,  # pyright: ignore[reportPrivateUsage]
    )
    from moneybin.mcp.tools.system import (
        _audit_position,  # pyright: ignore[reportPrivateUsage]
    )

    namespaces = {
        "audit": ("system_audit", {"view": "events"}),
        "imports": (
            "import_status.imports",
            {"import_id": None, "sections": ["imports"]},
        ),
        "reviews": ("reviews", {"kind": "matches", "status": "history"}),
    }
    namespace, scope = namespaces[surface]
    key = (
        ("2026-10-01 13:00:00", "a", 2)
        if surface == "imports"
        else ("2026-10-01 13:00:00", "a")
    )
    cursor = encode_keyset_cursor(
        namespace=namespace, scope=scope, snapshot=key, after=key, total=2
    )
    with pytest.raises((ValueError, UserError), match="[Rr]estart"):
        if surface == "audit":
            _audit_position(cursor, "events")
        elif surface == "imports":
            _import_status_position(cursor, sections=["imports"])
        else:
            _review_position(cursor, kind="matches", status="history")


@pytest.mark.parametrize(
    "kind,key", [("categorization", ("2026-10-01", "a")), ("auto_rules", ("a",))]
)
def test_noninstant_review_cursor_keeps_legacy_scope(
    kind: str, key: tuple[str, ...]
) -> None:
    from typing import Any, cast

    from moneybin.mcp.tools.reviews import (
        _review_position,  # pyright: ignore[reportPrivateUsage]
    )

    cursor = encode_keyset_cursor(
        namespace="reviews",
        scope={"kind": kind, "status": "pending"},
        snapshot=key,
        after=key,
        total=2,
    )
    assert _review_position(cursor, kind=cast(Any, kind), status="pending") is not None
