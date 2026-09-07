"""Audited writes to app.rule_conflicts (Invariant 10)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest
from prometheus_client import REGISTRY

from moneybin.database import Database
from moneybin.repositories.rule_conflicts_repo import RuleConflictsRepo
from moneybin.tables import RULE_CONFLICTS
from tests.moneybin.test_repositories.conftest import audit_rows_for, metric_for

_UPDATED_AT = datetime(2026, 9, 1, 10, 0, 0)
_LATER = datetime(2026, 9, 2, 10, 0, 0)

metric = metric_for("rule_conflicts")


def _set(
    repo: RuleConflictsRepo,
    conflict_id: str,
    *,
    existing_rule_id: str = "0123456789ab",
    updated_at: datetime = _UPDATED_AT,
    proposed_category: str = "Travel",
    actor: str = "cli",
) -> Any:
    return repo.set(
        conflict_id=conflict_id,
        matcher_digest="a" * 64,
        existing_rule_id=existing_rule_id,
        existing_rule_updated_at=updated_at,
        existing_name="Coffee",
        existing_category="Food & Drink",
        existing_subcategory=None,
        existing_priority=100,
        proposed_name="Coffee travel",
        proposed_merchant_pattern="STARBUCKS",
        proposed_match_type="contains",
        proposed_min_amount=None,
        proposed_max_amount=None,
        proposed_account_id=None,
        proposed_category=proposed_category,
        proposed_subcategory=None,
        proposed_priority=100,
        proposed_created_by="ai",
        actor=actor,
    )


@pytest.mark.unit
def test_set_writes_the_row_and_pairs_an_audit(db: Database) -> None:
    repo = RuleConflictsRepo(db)
    before = metric("rule_conflict.set")

    event = _set(repo, "conf_aaaaaaaaaaaaaaaa")

    assert event.target_id == "conf_aaaaaaaaaaaaaaaa"
    rows = audit_rows_for(db, "conf_aaaaaaaaaaaaaaaa")
    assert [row[0] for row in rows] == ["rule_conflict.set"]
    assert rows[0][1:4] == (RULE_CONFLICTS.schema, RULE_CONFLICTS.name, event.target_id)
    assert rows[0][4] is None, "no prior row, so before_value is null"
    assert metric("rule_conflict.set") == before + 1


@pytest.mark.unit
def test_set_reopens_a_cancelled_conflict(db: Database) -> None:
    """Submitting the same refused rule again is a fresh ask, not a settled one."""
    repo = RuleConflictsRepo(db)
    _set(repo, "conf_aaaaaaaaaaaaaaaa")
    repo.resolve(
        "conf_aaaaaaaaaaaaaaaa",
        resolution="cancel",
        resolved_rule_id=None,
        actor="cli",
    )

    _set(repo, "conf_aaaaaaaaaaaaaaaa")

    row = db.execute(
        "SELECT status, resolution, resolved_at FROM app.rule_conflicts"
    ).fetchone()
    assert row == ("pending", None, None)


@pytest.mark.unit
def test_resolve_records_the_decision_and_pairs_an_audit(db: Database) -> None:
    repo = RuleConflictsRepo(db)
    _set(repo, "conf_aaaaaaaaaaaaaaaa")
    before = metric("rule_conflict.resolve")

    repo.resolve(
        "conf_aaaaaaaaaaaaaaaa",
        resolution="replace",
        resolved_rule_id="ba9876543210",
        actor="mcp",
    )

    row = db.execute(
        "SELECT status, resolution, resolved_rule_id FROM app.rule_conflicts"
    ).fetchone()
    assert row == ("resolved", "replace", "ba9876543210")
    # Order-free: `audit_id` is a UUID4 and both rows can share one
    # `occurred_at`, so the pair has no reliable ordering key.
    actions = sorted(r[0] for r in audit_rows_for(db, "conf_aaaaaaaaaaaaaaaa"))
    assert actions == ["rule_conflict.resolve", "rule_conflict.set"]
    assert metric("rule_conflict.resolve") == before + 1


@pytest.mark.unit
def test_resolve_refuses_a_missing_conflict(db: Database) -> None:
    with pytest.raises(ValueError, match="conflict_id="):
        RuleConflictsRepo(db).resolve(
            "conf_missing",
            resolution="cancel",
            resolved_rule_id=None,
            actor="cli",
        )


@pytest.mark.unit
def test_prune_stale_removes_rows_bound_to_an_older_rule_version(
    db: Database,
) -> None:
    repo = RuleConflictsRepo(db)
    _set(repo, "conf_aaaaaaaaaaaaaaaa", updated_at=_UPDATED_AT)
    _set(repo, "conf_bbbbbbbbbbbbbbbb", updated_at=_LATER)
    _set(repo, "conf_cccccccccccccccc", existing_rule_id="other12345ab")

    events = repo.prune_stale("0123456789ab", keep_updated_at=_LATER, actor="cli")

    remaining = {
        str(row[0])
        for row in db.execute("SELECT conflict_id FROM app.rule_conflicts").fetchall()
    }
    assert remaining == {"conf_bbbbbbbbbbbbbbbb", "conf_cccccccccccccccc"}
    assert [event.action for event in events] == ["rule_conflict.delete"]


@pytest.mark.unit
def test_refresh_pending_gauge_counts_pending_rows(db: Database) -> None:
    repo = RuleConflictsRepo(db)
    _set(repo, "conf_aaaaaaaaaaaaaaaa")
    _set(repo, "conf_bbbbbbbbbbbbbbbb", updated_at=_LATER)
    repo.resolve(
        "conf_bbbbbbbbbbbbbbbb",
        resolution="cancel",
        resolved_rule_id=None,
        actor="cli",
    )

    repo.refresh_pending_gauge()

    assert REGISTRY.get_sample_value("moneybin_rule_conflicts_pending") == 1.0
