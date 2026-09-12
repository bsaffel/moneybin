"""Tests for ``AccountSettingsRepo``.

Every mutating test asserts both the row mutation and the paired
``app.audit_log`` entry land in one transaction, and that ``before_value``
captures the FULL prior row (Req 4).
"""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import duckdb
import pytest

from moneybin.database import Database
from moneybin.repositories.account_settings_repo import AccountSettingsRepo
from moneybin.services.audit_service import AuditEvent
from tests.moneybin.test_repositories.conftest import audit_rows_for as _audit_rows_for
from tests.moneybin.test_repositories.conftest import metric_for

_metric = metric_for("account_settings")


def _set(repo: AccountSettingsRepo, **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "account_id": "acct_a",
        "display_name": "Checking",
        "official_name": None,
        "last_four": "1234",
        "account_subtype": "checking",
        "holder_category": "personal",
        "currency_code": "USD",
        "credit_limit": None,
        "archived": False,
        "archived_at": None,
        "include_in_net_worth": True,
        "default_cost_basis_method": None,
        "actor": "cli",
    }
    kwargs.update(overrides)
    return repo.set(**kwargs)


def test_set_inserts_row_and_audit(db: Database) -> None:
    repo = AccountSettingsRepo(db)
    before_metric = _metric("account_settings.set")

    event = _set(repo)
    assert event.target_id == "acct_a"

    row = db.conn.execute(
        "SELECT display_name, last_four, archived FROM app.account_settings "
        "WHERE account_id = ?",
        ["acct_a"],
    ).fetchone()
    assert row == ("Checking", "1234", False)

    audit = _audit_rows_for(db, "acct_a")
    assert len(audit) == 1
    action, schema, table, target_id, before, after, actor, _parent = audit[0]
    assert action == "account_settings.set"
    assert (schema, table, target_id) == ("app", "account_settings", "acct_a")
    assert before is None
    assert json.loads(after)["display_name"] == "Checking"
    assert actor == "cli"

    assert _metric("account_settings.set") - before_metric == 1.0


def test_set_update_captures_full_prior_row(db: Database) -> None:
    repo = AccountSettingsRepo(db)
    _set(repo, display_name="Old", credit_limit=Decimal("500.00"))
    _set(repo, display_name="New", credit_limit=Decimal("750.00"))

    row = db.conn.execute(
        "SELECT display_name FROM app.account_settings WHERE account_id = ?",
        ["acct_a"],
    ).fetchone()
    assert row == ("New",)

    # The second set's audit row captures the FULL prior row in before.
    update_audit = _audit_rows_for(db, "acct_a")[1]
    before = json.loads(update_audit[4])
    after = json.loads(update_audit[5])
    assert before["display_name"] == "Old"
    assert before["credit_limit"] == "500.00"  # Decimal serialized losslessly as str
    assert after["display_name"] == "New"
    assert after["credit_limit"] == "750.00"


def test_set_records_parent_audit_id(db: Database) -> None:
    repo = AccountSettingsRepo(db)
    event = _set(repo, parent_audit_id="p1")
    assert _audit_rows_for(db, event.target_id or "")[0][7] == "p1"


def test_set_persists_default_cost_basis_method(db: Database) -> None:
    repo = AccountSettingsRepo(db)
    _set(repo, default_cost_basis_method="average")

    row = db.conn.execute(
        "SELECT default_cost_basis_method FROM app.account_settings "
        "WHERE account_id = ?",
        ["acct_a"],
    ).fetchone()
    assert row == ("average",)


def test_set_invalid_default_cost_basis_method_raises_constraint_exception(
    db: Database,
) -> None:
    repo = AccountSettingsRepo(db)
    with pytest.raises(duckdb.ConstraintException):
        _set(repo, default_cost_basis_method="lifo")


def test_delete_captures_before_and_returns_event(db: Database) -> None:
    repo = AccountSettingsRepo(db)
    _set(repo, display_name="ToDelete")

    event = repo.delete("acct_a", actor="cli")
    assert event is not None

    assert (
        db.conn.execute(
            "SELECT 1 FROM app.account_settings WHERE account_id = ?", ["acct_a"]
        ).fetchone()
        is None
    )

    delete_audit = next(
        r for r in _audit_rows_for(db, "acct_a") if r[0] == "account_settings.delete"
    )
    assert json.loads(delete_audit[4])["display_name"] == "ToDelete"
    assert delete_audit[5] is None  # after is None for DELETE


def test_delete_returns_none_for_missing_row(db: Database) -> None:
    repo = AccountSettingsRepo(db)
    assert repo.delete("nope", actor="cli") is None
    assert _audit_rows_for(db, "nope") == []


def test_set_rolls_back_when_audit_raises(db: Database) -> None:
    audit = MagicMock()
    audit.record_audit_event.side_effect = RuntimeError("simulated audit failure")
    repo = AccountSettingsRepo(db, audit=audit)

    with pytest.raises(RuntimeError):
        _set(repo, account_id="ghost", display_name="Ghost")

    rows = db.conn.execute(
        "SELECT 1 FROM app.account_settings WHERE account_id = ?", ["ghost"]
    ).fetchall()
    assert rows == []


def _legacy_row(*, account_id: str, archived: bool) -> dict[str, Any]:
    """A full-row capture shaped like a pre-V060 audit image (no archived_at key)."""
    return {
        "account_id": account_id,
        "display_name": "Legacy Account",
        "official_name": None,
        "last_four": None,
        "account_subtype": None,
        "holder_category": None,
        "currency_code": None,
        "credit_limit": None,
        "archived": archived,
        "include_in_net_worth": True,
        "default_cost_basis_method": None,
        "updated_at": "2025-06-01T00:00:00",
    }


def _legacy_undo_event(
    *, account_id: str, before_archived: bool, after_archived: bool
) -> AuditEvent:
    return AuditEvent(
        audit_id="aud-legacy1",
        occurred_at="2025-06-01T00:00:00",
        actor="cli",
        action="account_settings.set",
        target_schema="app",
        target_table="account_settings",
        target_id=account_id,
        before_value=_legacy_row(account_id=account_id, archived=before_archived),
        after_value=_legacy_row(account_id=account_id, archived=after_archived),
        parent_audit_id=None,
        operation_id="op-legacy1",
    )


def test_undo_of_legacy_archive_row_derives_archived_at_from_today(
    db: Database,
) -> None:
    """Undo of a legacy 'archive' row must clear a stale archived_at, not skip it."""
    repo = AccountSettingsRepo(db)
    # Current DB state matches the event's "after" image: archived=True with
    # a stale date, standing in for whatever archived_at happens to hold —
    # proving the clear fires, not just that None was already there.
    _set(repo, account_id="acct_legacy", archived=True, archived_at=date(2025, 1, 1))

    # Forward mutation this reverses was the original archive: False -> True.
    event = _legacy_undo_event(
        account_id="acct_legacy", before_archived=False, after_archived=True
    )
    repo.undo_event(event, actor="cli")

    row = db.conn.execute(
        "SELECT archived, archived_at FROM app.account_settings WHERE account_id = ?",
        ["acct_legacy"],
    ).fetchone()
    assert row == (False, None)


def test_undo_of_legacy_unarchive_row_derives_archived_at_today_on_rearchive(
    db: Database,
) -> None:
    """Undo of a legacy 'unarchive' row re-archives; archived_at must become today."""
    repo = AccountSettingsRepo(db)
    _set(repo, account_id="acct_legacy2", archived=False, archived_at=None)

    # Forward mutation this reverses was an unarchive: True -> False.
    event = _legacy_undo_event(
        account_id="acct_legacy2", before_archived=True, after_archived=False
    )
    repo.undo_event(event, actor="cli")

    row = db.conn.execute(
        "SELECT archived, archived_at FROM app.account_settings WHERE account_id = ?",
        ["acct_legacy2"],
    ).fetchone()
    assert row == (True, date.today())


def test_undo_of_current_capture_leaves_archived_at_override_untouched(
    db: Database,
) -> None:
    """A post-V060 capture already carries archived_at; the override must not clobber it."""
    repo = AccountSettingsRepo(db)
    _set(repo, account_id="acct_current", archived=True, archived_at=date(2026, 1, 10))
    set_event = _set(repo, account_id="acct_current", archived=False, archived_at=None)

    repo.undo_event(set_event, actor="cli")

    row = db.conn.execute(
        "SELECT archived, archived_at FROM app.account_settings WHERE account_id = ?",
        ["acct_current"],
    ).fetchone()
    # Restored to the captured before-image exactly: archived=True with the
    # real historical date, not today's.
    assert row == (True, date(2026, 1, 10))
