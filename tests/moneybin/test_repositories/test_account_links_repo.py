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


def test_rejects_invalid_reversed_by(db: Database) -> None:
    """reversed_by is domain-constrained even though reverse() lands later."""
    repo = AccountLinksRepo(db)
    _insert(repo, link_id="l1")
    with pytest.raises(duckdb.ConstraintException):
        db.conn.execute(  # noqa: S608  # test input, not executing user SQL
            "UPDATE app.account_links SET reversed_by = 'bogus' WHERE link_id = 'l1'"
        )


def test_allows_relink_after_reversal(db: Database) -> None:
    """A reversed link frees its (source_type, source_origin, ref_value) slot.

    The guard filters on status='accepted', so a reversed mapping must not block a
    fresh accepted one — the link-undo-relink invariant (reachable today via the
    status param; reverse() lands in a later phase).
    """
    repo = AccountLinksRepo(db)
    _insert(repo, link_id="l1")
    # bypass the repo: reverse() method is deferred to a later phase
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


def test_undo_reinsert_respects_uniqueness_guard(db: Database) -> None:
    """Undo-the-undo re-insert must not bypass the app-layer uniqueness guard.

    insert A → undo (delete) → insert B (same source_native key) → undo-the-undo
    of A re-inserts A via BaseRepo._insert_row; without a restore-time guard that
    leaves two accepted mappings for one native ref (the staging JOIN becomes
    non-1:1). Unreachable until link writers land, but the dispatch path exists.
    """
    repo = AccountLinksRepo(db)
    ev_insert = _insert(repo, link_id="A")
    ev_undo = repo.undo_event(ev_insert, actor="system")  # deletes A
    assert ev_undo is not None
    _insert(repo, link_id="B")  # same source_type/origin/ref_value — now allowed
    with pytest.raises(ValueError, match="source_native"):
        repo.undo_event(ev_undo, actor="system")  # re-insert A must hit the guard


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


# -- repoint --


def test_repoint_net_effect(db: Database) -> None:
    """After repoint: old row is reversed.

    Exactly one accepted row for the same ref coordinates now points to
    new_account_id.
    """
    repo = AccountLinksRepo(db)
    _insert(repo, link_id="lnk00000001", account_id="acct_canonical_1")

    repo.repoint(
        link_id="lnk00000001",
        new_account_id="acct_canonical_2",
        decided_by="user",
        actor="cli",
    )

    old_row = db.conn.execute(
        "SELECT status, account_id FROM app.account_links WHERE link_id = ?",
        ["lnk00000001"],
    ).fetchone()
    assert old_row == ("reversed", "acct_canonical_1")

    accepted = db.conn.execute(
        "SELECT account_id FROM app.account_links "
        "WHERE status = 'accepted' AND ref_kind = ? AND ref_value = ? "
        "AND source_type = ? AND source_origin = ?",
        ["source_native", "checking", "ofx", "wells_fargo"],
    ).fetchall()
    assert len(accepted) == 1
    assert accepted[0][0] == "acct_canonical_2"


def test_repoint_raises_for_same_account(db: Database) -> None:
    """Re-pointing a link onto its current account_id is a caller bug."""
    repo = AccountLinksRepo(db)
    _insert(repo, link_id="lnk00000001", account_id="acct_canonical_1")
    with pytest.raises(ValueError, match="already points"):
        repo.repoint(
            link_id="lnk00000001",
            new_account_id="acct_canonical_1",
            decided_by="user",
            actor="cli",
        )


def test_repoint_raises_for_non_accepted_link(db: Database) -> None:
    """Can only re-point an accepted link; a reversed link raises."""
    repo = AccountLinksRepo(db)
    _insert(repo, link_id="lnk00000001", account_id="acct_canonical_1")
    db.conn.execute(  # noqa: S608  # test input, not executing user SQL
        "UPDATE app.account_links SET status = 'reversed' WHERE link_id = 'lnk00000001'"
    )
    with pytest.raises(ValueError, match="can only re-point"):
        repo.repoint(
            link_id="lnk00000001",
            new_account_id="acct_canonical_2",
            decided_by="user",
            actor="cli",
        )


def test_repoint_emits_both_audit_rows(db: Database) -> None:
    """Repoint emits one audit for the old-row reversal AND one for the new insert."""
    repo = AccountLinksRepo(db)
    _insert(repo, link_id="lnk00000001", account_id="acct_canonical_1")

    repo.repoint(
        link_id="lnk00000001",
        new_account_id="acct_canonical_2",
        decided_by="user",
        actor="cli",
    )

    # Old row's repoint audit — before=accepted, after=reversed
    old_audits = _audit_rows_for(db, "lnk00000001")
    repoint_audit = next(r for r in old_audits if r[0] == "account_link.repoint")
    assert json.loads(repoint_audit[4])["account_id"] == "acct_canonical_1"
    assert json.loads(repoint_audit[5])["status"] == "reversed"

    # New row's insert audit — keyed on the freshly minted link_id
    new_link_id = db.conn.execute(
        "SELECT link_id FROM app.account_links WHERE account_id = 'acct_canonical_2'"
    ).fetchone()
    assert new_link_id is not None
    new_audits = _audit_rows_for(db, new_link_id[0])
    assert any(r[0] == "account_link.insert" for r in new_audits)
