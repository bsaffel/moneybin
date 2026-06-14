"""Tests for ``AccountLinksRepo``.

Mirrors test_match_decisions_repo.py: every mutating test asserts both the row
mutation and the paired ``app.audit_log`` entry land in one transaction, plus
the M1S finding-#3 uniqueness guards (one accepted source_native per
(source_type, source_origin, ref_value); strong-ref uniqueness for
full_number/persistent_token).
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import duckdb
import pytest
from prometheus_client import REGISTRY

from moneybin.database import Database
from moneybin.repositories.account_links_repo import AccountLinksRepo


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
            {"repository": "account_links", "action": action},
        )
        or 0.0
    )


def _insert(repo: AccountLinksRepo, **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "link_id": "lnk00000001",
        "account_id": "acct_canonical_1",
        "ref_kind": "source_native",
        "ref_value": "checking",
        "source_type": "ofx",
        "source_origin": "wells_fargo",
        "decided_by": "auto",
        "actor": "system",
    }
    kwargs.update(overrides)
    return repo.insert(**kwargs)


def test_insert_writes_row_and_audit_row(db: Database) -> None:
    repo = AccountLinksRepo(db)
    before_metric = _metric("account_link.insert")

    event = _insert(repo)
    assert event.target_id == "lnk00000001"

    row = db.conn.execute(
        "SELECT status, decided_by, account_id, ref_kind "
        "FROM app.account_links WHERE link_id = ?",
        ["lnk00000001"],
    ).fetchone()
    assert row == ("accepted", "auto", "acct_canonical_1", "source_native")

    audit = _audit_rows_for(db, "lnk00000001")
    assert len(audit) == 1
    action, schema, table, target_id, before, after, actor, _parent = audit[0]
    assert action == "account_link.insert"
    assert (schema, table, target_id) == ("app", "account_links", "lnk00000001")
    assert before is None
    assert json.loads(after)["status"] == "accepted"
    assert actor == "system"

    assert _metric("account_link.insert") - before_metric == 1.0


def test_insert_rolls_back_when_audit_raises(db: Database) -> None:
    audit = MagicMock()
    audit.record_audit_event.side_effect = RuntimeError("simulated audit failure")
    repo = AccountLinksRepo(db, audit=audit)

    with pytest.raises(RuntimeError):
        _insert(repo, link_id="ghost_link")

    rows = db.conn.execute(
        "SELECT 1 FROM app.account_links WHERE link_id = ?", ["ghost_link"]
    ).fetchall()
    assert rows == []


# -- Finding #3 uniqueness guards (app-layer; DuckDB has no partial unique index) --


def test_insert_rejects_duplicate_accepted_source_native(db: Database) -> None:
    """One accepted source_native per (source_type, source_origin, ref_value)."""
    repo = AccountLinksRepo(db)
    _insert(repo, link_id="l1")
    with pytest.raises(ValueError, match="source_native"):
        _insert(repo, link_id="l2", account_id="acct_other")


def test_allows_source_native_same_value_different_origin(db: Database) -> None:
    """source_origin scopes the source_native key — same value, different bank, OK."""
    repo = AccountLinksRepo(db)
    _insert(repo, link_id="l1")
    _insert(repo, link_id="l2", source_origin="chase")
    n = db.conn.execute(
        "SELECT COUNT(*) FROM app.account_links WHERE ref_kind = 'source_native'"
    ).fetchone()
    assert n is not None and n[0] == 2


@pytest.mark.parametrize("kind", ["full_number", "persistent_token"])
def test_insert_rejects_duplicate_accepted_strong_ref(db: Database, kind: str) -> None:
    """One strong ref (full_number OR persistent_token) -> one canonical account."""
    repo = AccountLinksRepo(db)
    _insert(repo, link_id="l1", ref_kind=kind, ref_value="wells_fargo:123456")
    with pytest.raises(ValueError, match="strong"):
        _insert(
            repo,
            link_id="l2",
            account_id="acct_other",
            ref_kind=kind,
            ref_value="wells_fargo:123456",
        )


def test_allows_relink_after_reversal(db: Database) -> None:
    """A reversed link frees its (source_type, source_origin, ref_value) slot.

    The guard filters on status='accepted', so a reversed mapping must not block a
    fresh accepted one — the link-undo-relink invariant (reachable today via the
    status param; reverse() lands in a later phase).
    """
    repo = AccountLinksRepo(db)
    _insert(repo, link_id="l1")
    db.conn.execute(
        "UPDATE app.account_links SET status = 'reversed' WHERE link_id = 'l1'"
    )
    _insert(repo, link_id="l2")  # same source_type/origin/ref_value -> must succeed
    n = db.conn.execute(
        "SELECT COUNT(*) FROM app.account_links WHERE status = 'accepted'"
    ).fetchone()
    assert n is not None and n[0] == 1


def test_insert_rejects_invalid_decided_by(db: Database) -> None:
    """decided_by is constrained to the documented domain (auto/user/system)."""
    repo = AccountLinksRepo(db)
    with pytest.raises(duckdb.ConstraintException):
        _insert(repo, decided_by="bogus")


def test_allows_strong_ref_same_value_different_kind(db: Database) -> None:
    """The strong-ref guard keys on (ref_kind, ref_value), not ref_value alone."""
    repo = AccountLinksRepo(db)
    _insert(repo, link_id="l1", ref_kind="full_number", ref_value="tok123")
    _insert(
        repo,
        link_id="l2",
        account_id="acct_other",
        ref_kind="persistent_token",
        ref_value="tok123",
    )
    n = db.conn.execute("SELECT COUNT(*) FROM app.account_links").fetchone()
    assert n is not None and n[0] == 2
