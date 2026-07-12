"""Tests for ``MerchantLinkDecisionsRepo``.

Mirrors test_account_link_decisions_repo.py: insert writes a pending proposal
and a paired ``app.audit_log`` row in one transaction, with ``match_signals``
stored as JSON and decoded (not doubly-encoded) in list/history results.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.repositories.merchant_link_decisions_repo import MerchantLinkDecisionsRepo


def _insert(repo: MerchantLinkDecisionsRepo, **overrides: object) -> object:
    kwargs: dict[str, object] = {
        "decision_id": "d1",
        "ref_kind": "merchant_entity_id",
        "ref_value": "ent_q",
        "source_type": "plaid",
        "provider_merchant_name": "Starbucks",
        "candidate_merchant_id": "mA",
        "confidence_score": 0.6,
        "match_signals": {"signal": "fuzzy_name", "value": "Starbucks"},
        "decided_by": "auto",
        "actor": "system",
        "match_reason": "fuzzy_name",
    }
    kwargs.update(overrides)
    return repo.insert(**kwargs)  # type: ignore[arg-type]


def test_insert_then_accept(db: Database) -> None:
    repo = MerchantLinkDecisionsRepo(db)
    _insert(repo)
    assert [d["decision_id"] for d in repo.list_pending()] == ["d1"]
    repo.update_status("d1", status="accepted", decided_by="user", actor="cli")
    assert repo.list_pending() == []


def test_list_pending_match_signals_is_dict(db: Database) -> None:
    """list_pending decodes JSON match_signals to a dict (not a doubly-encoded string)."""
    repo = MerchantLinkDecisionsRepo(db)
    _insert(repo, match_signals={"signal": "fuzzy_name", "value": "Starbucks"})

    pending = repo.list_pending()
    assert len(pending) == 1
    assert isinstance(pending[0]["match_signals"], dict)
    assert pending[0]["match_signals"]["signal"] == "fuzzy_name"


def test_reverse_drops_from_pending_and_emits_audit(db: Database) -> None:
    """reverse() of a decided decision writes a reverse audit row.

    ``rev1`` is inserted pending and confirmed present in list_pending, then
    accepted (which is what actually drops it from list_pending) before being
    reversed — reverse() itself only accepts accepted/rejected rows.
    """
    repo = MerchantLinkDecisionsRepo(db)
    _insert(repo, decision_id="rev1")
    assert any(r["decision_id"] == "rev1" for r in repo.list_pending())

    repo.update_status("rev1", status="accepted", decided_by="user", actor="cli")
    assert not any(r["decision_id"] == "rev1" for r in repo.list_pending())

    repo.reverse("rev1", reversed_by="user", actor="cli")

    assert not any(r["decision_id"] == "rev1" for r in repo.list_pending())

    audit_row = db.conn.execute(
        "SELECT action FROM app.audit_log WHERE target_id = ? AND action = ?",
        ["rev1", "merchant_link_decision.reverse"],
    ).fetchone()
    assert audit_row is not None, "expected merchant_link_decision.reverse audit row"


def test_reverse_raises_when_pending(db: Database) -> None:
    """A pending decision has no accept/reject decision yet to undo.

    Reversing it would silently dequeue a review item with no decision ever
    recorded — the guarantee the merchant-link review queue exists to enforce.
    """
    repo = MerchantLinkDecisionsRepo(db)
    _insert(repo, decision_id="d_pending")

    with pytest.raises(ValueError, match="accepted/rejected decisions can be reversed"):
        repo.reverse("d_pending", reversed_by="user", actor="cli")

    assert [r["decision_id"] for r in repo.list_pending()] == ["d_pending"]
    still_pending = repo.fetch_by_id("d_pending")
    assert still_pending is not None
    assert still_pending["status"] == "pending"
    assert still_pending["reversed_at"] is None


def test_history_returns_newest_first(db: Database) -> None:
    """history() returns all decisions newest-first by decided_at."""
    repo = MerchantLinkDecisionsRepo(db)
    _insert(repo, decision_id="older")
    _insert(repo, decision_id="newer")

    # Pin timestamps so ordering is deterministic regardless of execution speed.
    db.conn.execute(
        "UPDATE app.merchant_link_decisions SET decided_at = ? WHERE decision_id = ?",
        ["2026-01-01 00:00:00", "older"],
    )
    db.conn.execute(
        "UPDATE app.merchant_link_decisions SET decided_at = ? WHERE decision_id = ?",
        ["2026-06-01 00:00:00", "newer"],
    )

    ids = [r["decision_id"] for r in repo.history()]
    assert ids == ["newer", "older"]
