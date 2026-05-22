"""Tests for soft-delete diff logic."""

from __future__ import annotations

from moneybin.connectors.gsheet.diff import compute_diff


def test_new_rows_only_returns_inserts():
    """When current has rows and active is empty, all are inserts."""
    diff = compute_diff(current_ids={"a", "b", "c"}, active_ids=set())
    assert diff.to_insert == {"a", "b", "c"}
    assert diff.to_soft_delete == set()


def test_no_change_returns_empty():
    """When current and active are identical, diff is empty."""
    diff = compute_diff(current_ids={"a", "b"}, active_ids={"a", "b"})
    assert diff.to_insert == set()
    assert diff.to_soft_delete == set()


def test_missing_ids_get_soft_deleted():
    """When active has ids not in current, they are soft-deleted."""
    diff = compute_diff(current_ids={"a"}, active_ids={"a", "b", "c"})
    assert diff.to_insert == set()
    assert diff.to_soft_delete == {"b", "c"}


def test_undelete_when_id_returns():
    """When a soft-deleted id reappears in current, it's in to_insert."""
    # 'b' was soft-deleted (not in active_ids), but reappears in current
    diff = compute_diff(current_ids={"a", "b"}, active_ids={"a"})
    assert diff.to_insert == {"b"}
    assert diff.to_soft_delete == set()


def test_mixed_scenario():
    """Mixed scenario with new, unchanged, and deleted rows."""
    diff = compute_diff(
        current_ids={"a", "b", "c", "d"},
        active_ids={"a", "b", "e"},  # 'e' is gone, 'c' and 'd' are new
    )
    assert diff.to_insert == {"c", "d"}
    assert diff.to_soft_delete == {"e"}


def test_both_empty_returns_empty_diff():
    """When both sets are empty, diff is empty."""
    diff = compute_diff(current_ids=set(), active_ids=set())
    assert diff.to_insert == set()
    assert diff.to_soft_delete == set()


def test_diff_result_is_frozen():
    """DiffResult is frozen; field reassignment raises FrozenInstanceError."""
    import dataclasses

    diff = compute_diff(current_ids={"a"}, active_ids=set())
    try:
        diff.to_insert = {"b"}  # type: ignore
        raise AssertionError("Should have raised FrozenInstanceError")
    except dataclasses.FrozenInstanceError:
        pass
