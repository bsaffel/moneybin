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
        read_only=False,
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

    # One full-row import.set event per write (Invariant 10, full before/after).
    audits = db.conn.execute(
        "SELECT target_table, target_id, after_value FROM app.audit_log "
        "WHERE action = 'import.set' ORDER BY occurred_at"
    ).fetchall()
    assert len(audits) == 1
    assert audits[0][0] == "imports" and audits[0][1] == "imp-1"
    assert json.loads(audits[0][2])["labels"] == ["budget-2026", "tax:2026"]


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
    set_count = db.conn.execute(
        "SELECT COUNT(*) FROM app.audit_log WHERE action = 'import.set'"
    ).fetchone()
    # Each add_labels call emits one full-row import.set audit event.
    assert set_count is not None and set_count[0] == 2


@pytest.mark.unit
def test_remove_labels_drops_from_list(db: Database) -> None:
    svc = ImportService(db)
    svc.add_labels("imp-1", ["a", "b", "c"], actor="cli")
    out = svc.remove_labels("imp-1", ["b", "missing"], actor="cli")
    assert out == ["a", "c"]
    # add + remove = two import.set events; the latest captures the result.
    audits = db.conn.execute(
        "SELECT after_value FROM app.audit_log WHERE action = 'import.set' "
        "ORDER BY occurred_at"
    ).fetchall()
    assert len(audits) == 2
    assert json.loads(audits[-1][0])["labels"] == ["a", "c"]


@pytest.mark.unit
def test_remove_labels_on_unlabeled_import_is_noop(db: Database) -> None:
    # Removing from a never-labeled import changes nothing, so it must NOT
    # materialize a spurious app.imports row or an import.set audit event.
    svc = ImportService(db)
    out = svc.remove_labels("imp-x", ["whatever"], actor="cli")
    assert out == []
    rows = db.conn.execute(
        "SELECT COUNT(*) FROM app.imports WHERE import_id = ?", ["imp-x"]
    ).fetchone()
    assert rows is not None and rows[0] == 0
    audits = db.conn.execute(
        "SELECT COUNT(*) FROM app.audit_log WHERE action = 'import.set'"
    ).fetchone()
    assert audits is not None and audits[0] == 0


@pytest.mark.unit
def test_add_labels_noop_when_all_already_present(db: Database) -> None:
    # Re-adding labels the import already has changes nothing, so the second
    # call emits no new import.set audit row (only the first, real write does).
    svc = ImportService(db)
    svc.add_labels("imp-1", ["a", "b"], actor="cli")
    out = svc.add_labels("imp-1", ["a", "b"], actor="cli")  # no change
    assert out == ["a", "b"]
    count = db.conn.execute(
        "SELECT COUNT(*) FROM app.audit_log WHERE action = 'import.set'"
    ).fetchone()
    assert count is not None and count[0] == 1


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

    # set_labels emits one full-row import.set with the new actor + state.
    audits = db.conn.execute(
        "SELECT before_value, after_value FROM app.audit_log "
        "WHERE action = 'import.set' AND actor = 'mcp' ORDER BY occurred_at"
    ).fetchall()
    assert len(audits) == 1
    before, after = json.loads(audits[0][0]), json.loads(audits[0][1])
    assert before["labels"] == ["a", "b"]
    assert after["labels"] == ["b", "c"]


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
