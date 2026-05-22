"""Tests for ``UserMerchantsRepo``.

Every mutating test asserts both the row mutation and the paired
``app.audit_log`` entry land in one transaction, and that ``before_value``
captures the FULL prior row (Req 4).
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest
from prometheus_client import REGISTRY

from moneybin.database import Database
from moneybin.repositories.user_merchants_repo import UserMerchantsRepo


def _audit_rows_for(db: Database, target_id: str) -> list[tuple[Any, ...]]:
    return db.conn.execute(
        """
        SELECT action, target_schema, target_table, target_id,
               before_value, after_value, actor, parent_audit_id
          FROM app.audit_log
         WHERE target_id = ?
         ORDER BY occurred_at ASC, audit_id ASC
        """,
        [target_id],
    ).fetchall()


def _metric(action: str) -> float:
    return (
        REGISTRY.get_sample_value(
            "moneybin_app_mutation_audit_emitted_total",
            {"repository": "user_merchants", "action": action},
        )
        or 0.0
    )


# ---------------------------------------------------------------------------
# insert
# ---------------------------------------------------------------------------


def test_insert_writes_merchant_and_audit_row(db: Database) -> None:
    repo = UserMerchantsRepo(db)
    before_metric = _metric("user_merchant.insert")

    event = repo.insert(
        raw_pattern="AMZN",
        match_type="contains",
        canonical_name="Amazon",
        category="Shopping",
        subcategory=None,
        category_id=None,
        created_by="ai",
        exemplars=[],
        actor="mcp",
    )
    mid = event.target_id
    assert mid is not None
    assert len(mid) == 12  # truncated UUID4 hex per identifiers.md

    rows = db.conn.execute(
        "SELECT raw_pattern, match_type, canonical_name, category, created_by "
        "FROM app.user_merchants WHERE merchant_id = ?",
        [mid],
    ).fetchall()
    assert rows == [("AMZN", "contains", "Amazon", "Shopping", "ai")]

    audit = _audit_rows_for(db, mid)
    assert len(audit) == 1
    action, schema, table, target_id, before, after, actor, parent = audit[0]
    assert action == "user_merchant.insert"
    assert (schema, table, target_id) == ("app", "user_merchants", mid)
    assert before is None
    after_decoded = json.loads(after)
    assert after_decoded["canonical_name"] == "Amazon"
    assert after_decoded["raw_pattern"] == "AMZN"
    assert after_decoded["exemplars"] == []
    assert actor == "mcp"
    assert parent is None

    assert _metric("user_merchant.insert") - before_metric == 1.0


def test_insert_records_parent_audit_id_when_supplied(db: Database) -> None:
    repo = UserMerchantsRepo(db)
    event = repo.insert(
        raw_pattern=None,
        match_type="oneOf",
        canonical_name="Costco",
        category=None,
        subcategory=None,
        category_id=None,
        created_by="ai",
        exemplars=[],
        actor="system",
        parent_audit_id="parent123",
    )
    audit = _audit_rows_for(db, event.target_id or "")
    assert audit[0][7] == "parent123"  # parent_audit_id column


# ---------------------------------------------------------------------------
# append_exemplar
# ---------------------------------------------------------------------------


def _insert_oneof(repo: UserMerchantsRepo, name: str) -> str:
    event = repo.insert(
        raw_pattern=None,
        match_type="oneOf",
        canonical_name=name,
        category=None,
        subcategory=None,
        category_id=None,
        created_by="ai",
        exemplars=[],
        actor="system",
    )
    assert event.target_id is not None
    return event.target_id


def test_append_exemplar_captures_before_and_after(db: Database) -> None:
    repo = UserMerchantsRepo(db)
    mid = _insert_oneof(repo, "Trader Joes")

    event = repo.append_exemplar(mid, "TRADER JOE'S #123", actor="system")
    assert event.after_value is not None
    assert event.after_value["exemplars"] == ["TRADER JOE'S #123"]

    row = db.conn.execute(
        "SELECT exemplars FROM app.user_merchants WHERE merchant_id = ?", [mid]
    ).fetchone()
    assert row == (["TRADER JOE'S #123"],)

    append_audit = next(
        r for r in _audit_rows_for(db, mid) if r[0] == "user_merchant.append_exemplar"
    )
    before = json.loads(append_audit[4])
    after = json.loads(append_audit[5])
    assert before["exemplars"] == []
    assert after["exemplars"] == ["TRADER JOE'S #123"]


def test_append_exemplar_is_idempotent(db: Database) -> None:
    repo = UserMerchantsRepo(db)
    mid = _insert_oneof(repo, "Whole Foods")
    repo.append_exemplar(mid, "WHOLEFDS", actor="system")
    # Re-appending the same exemplar leaves the set unchanged (list_distinct).
    event = repo.append_exemplar(mid, "WHOLEFDS", actor="system")
    assert event.after_value is not None
    assert event.after_value["exemplars"] == ["WHOLEFDS"]


def test_append_exemplar_raises_on_missing_row(db: Database) -> None:
    repo = UserMerchantsRepo(db)
    with pytest.raises(ValueError, match="not found"):
        repo.append_exemplar("nope", "X", actor="system")
    assert _audit_rows_for(db, "nope") == []


# ---------------------------------------------------------------------------
# Atomicity: audit failure rolls back the mutation
# ---------------------------------------------------------------------------


def test_insert_rolls_back_when_audit_raises(db: Database) -> None:
    audit = MagicMock()
    audit.record_audit_event.side_effect = RuntimeError("simulated audit failure")
    repo = UserMerchantsRepo(db, audit=audit)

    with pytest.raises(RuntimeError, match="simulated audit failure"):
        repo.insert(
            raw_pattern=None,
            match_type="oneOf",
            canonical_name="GhostMerchant",
            category=None,
            subcategory=None,
            category_id=None,
            created_by="ai",
            exemplars=[],
            actor="system",
        )

    rows = db.conn.execute(
        "SELECT 1 FROM app.user_merchants WHERE canonical_name = ?", ["GhostMerchant"]
    ).fetchall()
    assert rows == []
