"""Tests for ``AccountSettingsRepo``.

Every mutating test asserts both the row mutation and the paired
``app.audit_log`` entry land in one transaction, and that ``before_value``
captures the FULL prior row (Req 4).
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import duckdb
import pytest
from prometheus_client import REGISTRY

from moneybin.database import Database
from moneybin.repositories.account_settings_repo import AccountSettingsRepo


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
            {"repository": "account_settings", "action": action},
        )
        or 0.0
    )


def _set(repo: AccountSettingsRepo, **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "account_id": "acct_a",
        "display_name": "Checking",
        "official_name": None,
        "last_four": "1234",
        "account_subtype": "checking",
        "holder_category": "personal",
        "iso_currency_code": "USD",
        "credit_limit": None,
        "archived": False,
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
