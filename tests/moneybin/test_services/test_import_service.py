"""Tests for ImportService import-label methods (spec Req 22–24)."""

from __future__ import annotations

import json
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.services._validators import InvalidSlugError
from moneybin.services.import_service import ImportService


@pytest.fixture()
def db(tmp_path: Path) -> Generator[Database, None, None]:
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-key"
    database = Database(
        tmp_path / "labels.duckdb",
        secret_store=mock_store,
        no_auto_upgrade=True,
    )
    yield database
    database.close()


@pytest.mark.unit
def test_add_labels_creates_app_imports_row(db: Database) -> None:
    svc = ImportService(db)
    out = svc.add_labels("imp-1", ["budget-2026", "tax:2026"], actor="cli")
    assert out == ["budget-2026", "tax:2026"]
    row = db.conn.execute(
        "SELECT labels, updated_by FROM app.imports WHERE import_id = ?",
        ["imp-1"],
    ).fetchone()
    assert row is not None
    assert list(row[0]) == ["budget-2026", "tax:2026"]
    assert row[1] == "cli"

    # Two add events emitted, each scoped to imports table.
    audits = db.conn.execute(
        "SELECT action, target_table, target_id, after_value FROM app.audit_log "
        "WHERE action = 'import_label.add' ORDER BY occurred_at"
    ).fetchall()
    assert len(audits) == 2
    assert all(a[1] == "imports" and a[2] == "imp-1" for a in audits)
    payloads = [json.loads(a[3]) for a in audits]
    assert {p["label"] for p in payloads} == {"budget-2026", "tax:2026"}


@pytest.mark.unit
def test_add_labels_validates_pattern(db: Database) -> None:
    svc = ImportService(db)
    with pytest.raises(InvalidSlugError):
        svc.add_labels("imp-1", ["Bad Label!"], actor="cli")
    # Nothing committed.
    count = db.conn.execute("SELECT COUNT(*) FROM app.imports").fetchone()
    assert count is not None and count[0] == 0


@pytest.mark.unit
def test_add_labels_appends_to_existing(db: Database) -> None:
    svc = ImportService(db)
    svc.add_labels("imp-1", ["a"], actor="cli")
    out = svc.add_labels("imp-1", ["b", "a"], actor="cli")  # 'a' is dup
    assert out == ["a", "b"]
    add_count = db.conn.execute(
        "SELECT COUNT(*) FROM app.audit_log WHERE action = 'import_label.add'"
    ).fetchone()
    # Only the new label produces an audit event (a + b initial, b on second).
    assert add_count is not None and add_count[0] == 2


@pytest.mark.unit
def test_remove_labels_drops_from_list(db: Database) -> None:
    svc = ImportService(db)
    svc.add_labels("imp-1", ["a", "b", "c"], actor="cli")
    out = svc.remove_labels("imp-1", ["b", "missing"], actor="cli")
    assert out == ["a", "c"]
    remove_count = db.conn.execute(
        "SELECT COUNT(*) FROM app.audit_log WHERE action = 'import_label.remove'"
    ).fetchone()
    # Only 'b' was actually present, so only one remove event.
    assert remove_count is not None and remove_count[0] == 1


@pytest.mark.unit
def test_set_labels_replaces_full_list(db: Database) -> None:
    svc = ImportService(db)
    svc.add_labels("imp-1", ["a", "b"], actor="cli")
    out = svc.set_labels("imp-1", ["b", "c"], actor="mcp")
    assert out == ["b", "c"]
    row = db.conn.execute(
        "SELECT labels, updated_by FROM app.imports WHERE import_id = ?",
        ["imp-1"],
    ).fetchone()
    assert row is not None
    assert list(row[0]) == ["b", "c"]
    assert row[1] == "mcp"

    # set_labels emits 1 add ('c') + 1 remove ('a').
    adds = db.conn.execute(
        "SELECT COUNT(*) FROM app.audit_log "
        "WHERE action = 'import_label.add' AND actor = 'mcp'"
    ).fetchone()
    removes = db.conn.execute(
        "SELECT COUNT(*) FROM app.audit_log "
        "WHERE action = 'import_label.remove' AND actor = 'mcp'"
    ).fetchone()
    assert adds is not None and adds[0] == 1
    assert removes is not None and removes[0] == 1


@pytest.mark.unit
def test_set_labels_validates_before_mutating(db: Database) -> None:
    svc = ImportService(db)
    svc.add_labels("imp-1", ["a"], actor="cli")
    with pytest.raises(InvalidSlugError):
        svc.set_labels("imp-1", ["ok", "BAD!"], actor="cli")
    # Prior state preserved.
    row = db.conn.execute(
        "SELECT labels FROM app.imports WHERE import_id = ?", ["imp-1"]
    ).fetchone()
    assert row is not None and list(row[0]) == ["a"]


@pytest.mark.unit
def test_list_labels_returns_empty_when_no_row(db: Database) -> None:
    svc = ImportService(db)
    assert svc.list_labels("nonexistent") == []


@pytest.mark.unit
def test_list_distinct_labels_returns_counts(db: Database) -> None:
    svc = ImportService(db)
    svc.add_labels("imp-1", ["budget-2026", "tax:2026"], actor="cli")
    svc.add_labels("imp-2", ["budget-2026"], actor="cli")
    svc.add_labels("imp-3", ["budget-2026", "loan"], actor="cli")
    distinct = svc.list_distinct_labels()
    counts = dict(distinct)
    assert counts == {"budget-2026": 3, "tax:2026": 1, "loan": 1}
    # Ordering: highest count first.
    assert distinct[0][0] == "budget-2026"
