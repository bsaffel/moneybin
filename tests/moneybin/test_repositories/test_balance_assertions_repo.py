"""Tests for ``BalanceAssertionsRepo``.

Every mutating test asserts both the row mutation and the paired
``app.audit_log`` entry land in one transaction, that ``before_value`` captures
the FULL prior row (Req 4), and that the composite ``(account_id,
assertion_date)`` primary key maps to a single composite ``target_id``.
"""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

from prometheus_client import REGISTRY

from moneybin.database import Database
from moneybin.repositories.balance_assertions_repo import BalanceAssertionsRepo


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
            {"repository": "balance_assertions", "action": action},
        )
        or 0.0
    )


_DAY = date(2026, 5, 1)
_TARGET = "acct_a|2026-05-01"


def test_set_inserts_row_and_audit(db: Database) -> None:
    repo = BalanceAssertionsRepo(db)
    before_metric = _metric("balance_assertion.set")

    event = repo.set(
        "acct_a", _DAY, balance=Decimal("100.00"), notes="paper", actor="cli"
    )
    assert event.target_id == _TARGET

    row = db.conn.execute(
        "SELECT balance, notes FROM app.balance_assertions "
        "WHERE account_id = ? AND assertion_date = ?",
        ["acct_a", _DAY],
    ).fetchone()
    assert row == (Decimal("100.00"), "paper")

    audit = _audit_rows_for(db, _TARGET)
    assert len(audit) == 1
    action, schema, table, target_id, before, after, actor, _parent = audit[0]
    assert action == "balance_assertion.set"
    assert (schema, table, target_id) == ("app", "balance_assertions", _TARGET)
    assert before is None
    assert json.loads(after)["balance"] == "100.00"  # Decimal serialized as str
    assert actor == "cli"

    assert _metric("balance_assertion.set") - before_metric == 1.0


def test_set_update_captures_full_prior_row(db: Database) -> None:
    repo = BalanceAssertionsRepo(db)
    repo.set("acct_a", _DAY, balance=Decimal("100.00"), notes="first", actor="cli")
    repo.set("acct_a", _DAY, balance=Decimal("250.00"), notes="second", actor="cli")

    row = db.conn.execute(
        "SELECT balance FROM app.balance_assertions "
        "WHERE account_id = ? AND assertion_date = ?",
        ["acct_a", _DAY],
    ).fetchone()
    assert row == (Decimal("250.00"),)

    update_audit = _audit_rows_for(db, _TARGET)[1]
    before = json.loads(update_audit[4])
    after = json.loads(update_audit[5])
    assert before["balance"] == "100.00"
    assert before["notes"] == "first"
    assert after["balance"] == "250.00"
    assert after["notes"] == "second"


def test_set_records_parent_audit_id(db: Database) -> None:
    repo = BalanceAssertionsRepo(db)
    repo.set(
        "acct_a",
        _DAY,
        balance=Decimal("1.00"),
        notes=None,
        parent_audit_id="p1",
        actor="cli",
    )
    assert _audit_rows_for(db, _TARGET)[0][7] == "p1"


def test_delete_captures_before_and_returns_event(db: Database) -> None:
    repo = BalanceAssertionsRepo(db)
    repo.set("acct_a", _DAY, balance=Decimal("100.00"), notes="x", actor="cli")

    event = repo.delete("acct_a", _DAY, actor="cli")
    assert event is not None

    assert (
        db.conn.execute(
            "SELECT 1 FROM app.balance_assertions "
            "WHERE account_id = ? AND assertion_date = ?",
            ["acct_a", _DAY],
        ).fetchone()
        is None
    )

    delete_audit = next(
        r for r in _audit_rows_for(db, _TARGET) if r[0] == "balance_assertion.delete"
    )
    assert json.loads(delete_audit[4])["balance"] == "100.00"
    assert delete_audit[5] is None


def test_delete_returns_none_for_missing_row(db: Database) -> None:
    repo = BalanceAssertionsRepo(db)
    assert repo.delete("nope", _DAY, actor="cli") is None
    assert _audit_rows_for(db, "nope|2026-05-01") == []


def test_set_rolls_back_when_audit_raises(db: Database) -> None:
    audit = MagicMock()
    audit.record_audit_event.side_effect = RuntimeError("simulated audit failure")
    repo = BalanceAssertionsRepo(db, audit=audit)

    try:
        repo.set("ghost", _DAY, balance=Decimal("9.00"), notes=None, actor="cli")
    except RuntimeError:
        pass

    rows = db.conn.execute(
        "SELECT 1 FROM app.balance_assertions WHERE account_id = ?", ["ghost"]
    ).fetchall()
    assert rows == []
