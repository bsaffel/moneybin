"""Tests for ``ImportsRepo``.

Every mutating test asserts both the row mutation and the paired
``app.audit_log`` entry land in one transaction, and that ``before_value``
captures the FULL prior row (Req 4).
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.repositories.imports_repo import ImportsRepo
from tests.moneybin.test_repositories.conftest import audit_rows_for as _audit_rows_for
from tests.moneybin.test_repositories.conftest import metric_for

_metric = metric_for("imports")


def test_set_writes_row_and_audit_row(db: Database) -> None:
    repo = ImportsRepo(db)
    before_metric = _metric("import.set")

    event = repo.set("imp1", labels=["budget-2026", "tax:2026"], actor="cli")
    assert event.target_id == "imp1"

    row = db.conn.execute(
        "SELECT labels, updated_by FROM app.imports WHERE import_id = ?",
        ["imp1"],
    ).fetchone()
    assert row == (["budget-2026", "tax:2026"], "cli")

    audit = _audit_rows_for(db, "imp1")
    assert len(audit) == 1
    action, schema, table, target_id, before, after, actor, _parent = audit[0]
    assert action == "import.set"
    assert (schema, table, target_id) == ("app", "imports", "imp1")
    assert before is None
    after_json = json.loads(after)
    assert after_json["labels"] == ["budget-2026", "tax:2026"]
    assert after_json["updated_by"] == "cli"
    assert actor == "cli"

    assert _metric("import.set") - before_metric == 1.0


def test_set_records_parent_audit_id(db: Database) -> None:
    repo = ImportsRepo(db)
    event = repo.set("imp1", labels=["a"], actor="cli", parent_audit_id="p1")
    assert _audit_rows_for(db, event.target_id or "")[0][7] == "p1"


def test_set_upsert_captures_full_before_and_after(db: Database) -> None:
    repo = ImportsRepo(db)
    repo.set("imp1", labels=["old"], actor="cli")
    event = repo.set("imp1", labels=["new", "fresh"], actor="mcp")
    assert event.target_id == "imp1"

    row = db.conn.execute(
        "SELECT labels FROM app.imports WHERE import_id = ?", ["imp1"]
    ).fetchone()
    assert row == (["new", "fresh"],)

    upsert = _audit_rows_for(db, "imp1")[-1]
    before, after = json.loads(upsert[4]), json.loads(upsert[5])
    assert before["labels"] == ["old"]
    assert after["labels"] == ["new", "fresh"]
    assert after["updated_by"] == "mcp"


def test_set_joins_outer_transaction(db: Database) -> None:
    repo = ImportsRepo(db)
    db.begin()
    repo.set("imp1", labels=["a"], actor="cli", in_outer_txn=True)
    db.commit()
    row = db.conn.execute(
        "SELECT labels FROM app.imports WHERE import_id = ?", ["imp1"]
    ).fetchone()
    assert row == (["a"],)


def test_set_rolls_back_when_audit_raises(db: Database) -> None:
    audit = MagicMock()
    audit.record_audit_event.side_effect = RuntimeError("simulated audit failure")
    repo = ImportsRepo(db, audit=audit)

    with pytest.raises(RuntimeError):
        repo.set("ghost_import", labels=["x"], actor="cli")

    rows = db.conn.execute(
        "SELECT 1 FROM app.imports WHERE import_id = ?", ["ghost_import"]
    ).fetchall()
    assert rows == []
