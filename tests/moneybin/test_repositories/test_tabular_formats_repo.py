"""Tests for ``TabularFormatsRepo``.

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
from moneybin.repositories.tabular_formats_repo import TabularFormatsRepo


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
            {"repository": "tabular_formats", "action": action},
        )
        or 0.0
    )


def _set(repo: TabularFormatsRepo, **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "name": "chase_credit",
        "institution_name": "Chase",
        "file_type": "csv",
        "delimiter": ",",
        "encoding": "utf-8",
        "skip_rows": 0,
        "sheet": None,
        "header_signature": ["Transaction Date", "Description", "Amount"],
        "field_mapping": {"date": "Transaction Date", "amount": "Amount"},
        "sign_convention": "negative_is_expense",
        "date_format": "%m/%d/%Y",
        "number_format": "us",
        "skip_trailing_patterns": None,
        "multi_account": False,
        "source": "detected",
        "times_used": 0,
        "last_used_at": None,
        "actor": "cli",
    }
    kwargs.update(overrides)
    return repo.set(**kwargs)


def test_set_writes_row_and_audit_row(db: Database) -> None:
    repo = TabularFormatsRepo(db)
    before_metric = _metric("tabular_format.set")

    event = _set(repo)
    assert event.target_id == "chase_credit"

    row = db.conn.execute(
        "SELECT institution_name, sign_convention, multi_account "
        "FROM app.tabular_formats WHERE name = ?",
        ["chase_credit"],
    ).fetchone()
    assert row == ("Chase", "negative_is_expense", False)

    audit = _audit_rows_for(db, "chase_credit")
    assert len(audit) == 1
    action, schema, table, target_id, before, after, actor, _parent = audit[0]
    assert action == "tabular_format.set"
    assert (schema, table, target_id) == ("app", "tabular_formats", "chase_credit")
    assert before is None
    after_json = json.loads(after)
    assert after_json["institution_name"] == "Chase"
    # JSON columns round-trip through the audit payload as parseable JSON text.
    assert json.loads(after_json["field_mapping"]) == {
        "date": "Transaction Date",
        "amount": "Amount",
    }
    assert actor == "cli"

    assert _metric("tabular_format.set") - before_metric == 1.0


def test_set_records_parent_audit_id(db: Database) -> None:
    repo = TabularFormatsRepo(db)
    event = _set(repo, parent_audit_id="p1")
    assert _audit_rows_for(db, event.target_id or "")[0][7] == "p1"


def test_set_upsert_captures_full_before_and_after(db: Database) -> None:
    repo = TabularFormatsRepo(db)
    _set(repo, institution_name="Chase")
    event = _set(repo, institution_name="Chase Bank", times_used=5)
    assert event.target_id == "chase_credit"

    row = db.conn.execute(
        "SELECT institution_name, times_used FROM app.tabular_formats WHERE name = ?",
        ["chase_credit"],
    ).fetchone()
    assert row == ("Chase Bank", 5)

    upsert = _audit_rows_for(db, "chase_credit")[-1]
    before, after = json.loads(upsert[4]), json.loads(upsert[5])
    assert before["institution_name"] == "Chase"
    assert after["institution_name"] == "Chase Bank"
    assert after["times_used"] == 5


def test_delete_captures_before_and_returns_event(db: Database) -> None:
    repo = TabularFormatsRepo(db)
    _set(repo)

    event = repo.delete("chase_credit", actor="cli")
    assert event is not None

    assert (
        db.conn.execute(
            "SELECT 1 FROM app.tabular_formats WHERE name = ?", ["chase_credit"]
        ).fetchone()
        is None
    )

    deleted = next(
        r
        for r in _audit_rows_for(db, "chase_credit")
        if r[0] == "tabular_format.delete"
    )
    assert json.loads(deleted[4])["institution_name"] == "Chase"
    assert deleted[5] is None


def test_delete_returns_none_for_missing_format(db: Database) -> None:
    repo = TabularFormatsRepo(db)
    assert repo.delete("nope", actor="cli") is None
    assert _audit_rows_for(db, "nope") == []


def test_set_rolls_back_when_audit_raises(db: Database) -> None:
    audit = MagicMock()
    audit.record_audit_event.side_effect = RuntimeError("simulated audit failure")
    repo = TabularFormatsRepo(db, audit=audit)

    with pytest.raises(RuntimeError):
        _set(repo, name="ghost_format")

    rows = db.conn.execute(
        "SELECT 1 FROM app.tabular_formats WHERE name = ?", ["ghost_format"]
    ).fetchall()
    assert rows == []
