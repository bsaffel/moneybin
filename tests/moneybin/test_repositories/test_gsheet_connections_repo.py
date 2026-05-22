"""Tests for ``GSheetConnectionsRepo`` — audited writes to app.gsheet_connections.

Every mutating test asserts both the row mutation and the paired
``app.audit_log`` entry land in one transaction.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import duckdb
import pytest
from prometheus_client import REGISTRY

from moneybin.database import Database
from moneybin.repositories.gsheet_connections_repo import GSheetConnectionsRepo


def _insert_default(
    repo: GSheetConnectionsRepo,
    *,
    spreadsheet_id: str = "ss1",
    sheet_gid: int = 0,
    adapter: str = "transactions",
    alias: str | None = None,
    actor: str = "cli",
) -> str:
    """Insert one connection with minimum-required defaults; return cid."""
    return repo.insert(
        spreadsheet_id=spreadsheet_id,
        sheet_gid=sheet_gid,
        sheet_name="Sheet1",
        workbook_name="WB",
        adapter=adapter,
        alias=alias,
        account_id=None,
        account_name=None,
        column_mapping={"Date": "transaction_date"},
        header_signature=["Date"],
        date_format=None,
        sign_convention=None,
        number_format=None,
        skip_rows=0,
        skip_trailing_patterns=None,
        actor=actor,
    )


def _audit_rows_for(db: Database, connection_id: str) -> list[tuple[Any, ...]]:
    return db.conn.execute(
        """
        SELECT action, target_schema, target_table, target_id,
               before_value, after_value, actor
          FROM app.audit_log
         WHERE target_id = ?
         ORDER BY occurred_at ASC, audit_id ASC
        """,
        [connection_id],
    ).fetchall()


# ---------------------------------------------------------------------------
# 1. insert + paired audit
# ---------------------------------------------------------------------------


def test_insert_writes_connection_and_audit_row(db: Database) -> None:
    repo = GSheetConnectionsRepo(db)
    before_metric = (
        REGISTRY.get_sample_value(
            "moneybin_app_mutation_audit_emitted_total",
            {"repository": "gsheet_connections", "action": "gsheet_connection.insert"},
        )
        or 0.0
    )
    cid = repo.insert(
        spreadsheet_id="ss1",
        sheet_gid=0,
        sheet_name="S",
        workbook_name="WB",
        adapter="transactions",
        alias=None,
        account_id="acct1",
        account_name="Checking",
        column_mapping={"Date": "transaction_date"},
        header_signature=["Date"],
        date_format=None,
        sign_convention=None,
        number_format=None,
        skip_rows=0,
        skip_trailing_patterns=None,
        actor="cli",
    )
    assert cid is not None
    assert len(cid) == 12  # truncated UUID4 hex per identifiers.md strategy 3

    rows = db.conn.execute(
        "SELECT connection_id FROM app.gsheet_connections WHERE connection_id = ?",
        [cid],
    ).fetchall()
    assert len(rows) == 1

    audit = _audit_rows_for(db, cid)
    assert len(audit) == 1
    action, schema, table, target_id, before, after, actor = audit[0]
    assert action == "gsheet_connection.insert"
    assert (schema, table, target_id) == ("app", "gsheet_connections", cid)
    assert before is None
    assert after is not None
    after_decoded = json.loads(after)
    assert after_decoded["connection_id"] == cid
    assert after_decoded["spreadsheet_id"] == "ss1"
    assert after_decoded["column_mapping"] == {"Date": "transaction_date"}
    assert actor == "cli"

    # BaseRepo._emit_audit increments the repo-boundary mutation metric.
    after_metric = REGISTRY.get_sample_value(
        "moneybin_app_mutation_audit_emitted_total",
        {"repository": "gsheet_connections", "action": "gsheet_connection.insert"},
    )
    assert (after_metric or 0.0) - before_metric == 1.0


# ---------------------------------------------------------------------------
# 2. update_status
# ---------------------------------------------------------------------------


def test_update_status_writes_audit_row(db: Database) -> None:
    repo = GSheetConnectionsRepo(db)
    cid = _insert_default(repo)
    repo.update_status(
        cid, status="drift_detected", reason="Column missing", actor="cli"
    )
    audit = _audit_rows_for(db, cid)
    assert [r[0] for r in audit] == [
        "gsheet_connection.insert",
        "gsheet_connection.update_status",
    ]
    # Verify the row reflects the new status + reason
    row = repo.get(cid)
    assert row is not None
    assert row["status"] == "drift_detected"
    assert row["last_status_reason"] == "Column missing"


# ---------------------------------------------------------------------------
# 3. unique (spreadsheet_id, sheet_gid)
# ---------------------------------------------------------------------------


def test_unique_spreadsheet_gid_constraint(db: Database) -> None:
    repo = GSheetConnectionsRepo(db)
    _insert_default(repo, spreadsheet_id="ss1", sheet_gid=0)
    with pytest.raises(duckdb.ConstraintException):
        _insert_default(repo, spreadsheet_id="ss1", sheet_gid=0)


# ---------------------------------------------------------------------------
# 4. unique alias (seed connections)
# ---------------------------------------------------------------------------


def test_unique_alias_constraint_for_seed_connections(db: Database) -> None:
    repo = GSheetConnectionsRepo(db)
    _insert_default(
        repo, spreadsheet_id="ss1", sheet_gid=1, adapter="seed", alias="subs"
    )
    with pytest.raises(duckdb.ConstraintException):
        _insert_default(
            repo, spreadsheet_id="ss2", sheet_gid=2, adapter="seed", alias="subs"
        )


# ---------------------------------------------------------------------------
# 5. unique connection_id per call
# ---------------------------------------------------------------------------


def test_insert_returns_unique_connection_id_per_call(db: Database) -> None:
    repo = GSheetConnectionsRepo(db)
    cid_a = _insert_default(repo, spreadsheet_id="ss1", sheet_gid=0)
    cid_b = _insert_default(repo, spreadsheet_id="ss2", sheet_gid=0)
    assert cid_a != cid_b


# ---------------------------------------------------------------------------
# 6. get returns inserted row, JSON columns decoded
# ---------------------------------------------------------------------------


def test_get_returns_inserted_row(db: Database) -> None:
    repo = GSheetConnectionsRepo(db)
    cid = repo.insert(
        spreadsheet_id="ssX",
        sheet_gid=42,
        sheet_name="Tiller",
        workbook_name="WB",
        adapter="transactions",
        alias=None,
        account_id="acct1",
        account_name="Chase",
        column_mapping={"Date": "transaction_date", "Amount": "amount"},
        header_signature=["Date", "Amount"],
        date_format="%Y-%m-%d",
        sign_convention="negative_is_expense",
        number_format="us",
        skip_rows=1,
        skip_trailing_patterns=["^Total"],
        actor="cli",
    )
    row = repo.get(cid)
    assert row is not None
    assert row["connection_id"] == cid
    assert row["spreadsheet_id"] == "ssX"
    assert row["sheet_gid"] == 42
    # JSON columns must be decoded to Python objects, not raw strings
    assert row["column_mapping"] == {"Date": "transaction_date", "Amount": "amount"}
    assert row["header_signature"] == ["Date", "Amount"]
    assert row["skip_trailing_patterns"] == ["^Total"]


# ---------------------------------------------------------------------------
# 7. get returns None for unknown id
# ---------------------------------------------------------------------------


def test_get_returns_none_for_unknown_id(db: Database) -> None:
    repo = GSheetConnectionsRepo(db)
    assert repo.get("does_not_exist") is None


# ---------------------------------------------------------------------------
# 8. update_after_pull writes audit row
# ---------------------------------------------------------------------------


def test_update_after_pull_writes_audit_row(db: Database) -> None:
    repo = GSheetConnectionsRepo(db)
    cid = _insert_default(repo)
    pulled_at = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
    repo.update_after_pull(
        cid,
        last_pull_at=pulled_at,
        last_pull_import_id="imp_123",
        last_success_at=pulled_at,
        status="healthy",
        consecutive_failure_count=0,
        actor="system",
    )
    audit = _audit_rows_for(db, cid)
    actions = [r[0] for r in audit]
    assert "gsheet_connection.update_after_pull" in actions
    pull_audit = next(r for r in audit if r[0] == "gsheet_connection.update_after_pull")
    assert pull_audit[6] == "system"  # actor
    row = repo.get(cid)
    assert row is not None
    assert row["last_pull_import_id"] == "imp_123"
    assert row["consecutive_failure_count"] == 0


# ---------------------------------------------------------------------------
# 9. update_mapping resets status to healthy
# ---------------------------------------------------------------------------


def test_update_mapping_resets_status_to_healthy(db: Database) -> None:
    repo = GSheetConnectionsRepo(db)
    cid = _insert_default(repo)
    repo.update_status(cid, status="drift_detected", reason="missing Amount column")
    assert repo.get(cid)["status"] == "drift_detected"  # type: ignore[index]

    repo.update_mapping(
        cid,
        column_mapping={"Date": "transaction_date", "Amount": "amount"},
        header_signature=["Date", "Amount"],
        date_format="%Y-%m-%d",
        sign_convention="negative_is_expense",
        number_format="us",
        skip_rows=0,
        skip_trailing_patterns=[],
        actor="cli",
    )
    row = repo.get(cid)
    assert row is not None
    assert row["status"] == "healthy"
    assert row["last_status_reason"] is None
    assert row["column_mapping"] == {"Date": "transaction_date", "Amount": "amount"}

    actions = [r[0] for r in _audit_rows_for(db, cid)]
    assert "gsheet_connection.reconnect" in actions


# ---------------------------------------------------------------------------
# 10. delete captures before_value
# ---------------------------------------------------------------------------


def test_delete_writes_audit_row_with_before_value(db: Database) -> None:
    repo = GSheetConnectionsRepo(db)
    cid = _insert_default(repo, spreadsheet_id="ss_delete", sheet_gid=0)
    repo.delete(cid, actor="cli")

    assert repo.get(cid) is None

    audit = _audit_rows_for(db, cid)
    delete_audit = next(r for r in audit if r[0] == "gsheet_connection.delete")
    before_value, after_value = delete_audit[4], delete_audit[5]
    assert before_value is not None
    before_decoded = json.loads(before_value)
    assert before_decoded["connection_id"] == cid
    assert before_decoded["spreadsheet_id"] == "ss_delete"
    assert after_value is None


# ---------------------------------------------------------------------------
# 11. atomicity: audit failure rolls back the mutation
# ---------------------------------------------------------------------------


def test_audit_rows_are_in_same_transaction_as_mutation(db: Database) -> None:
    """If AuditService raises, the paired insert must roll back."""
    audit = MagicMock()
    audit.record_audit_event.side_effect = RuntimeError("simulated audit failure")
    repo = GSheetConnectionsRepo(db, audit=audit)

    with pytest.raises(RuntimeError, match="simulated audit failure"):
        _insert_default(repo, spreadsheet_id="ss_atomic", sheet_gid=99)

    # No row landed in app.gsheet_connections
    rows = db.conn.execute(
        "SELECT 1 FROM app.gsheet_connections WHERE spreadsheet_id = ?",
        ["ss_atomic"],
    ).fetchall()
    assert rows == []


# ---------------------------------------------------------------------------
# 12. list_healthy filters by status
# ---------------------------------------------------------------------------


def test_list_healthy_excludes_non_healthy(db: Database) -> None:
    repo = GSheetConnectionsRepo(db)
    cid_healthy = _insert_default(repo, spreadsheet_id="ss_healthy", sheet_gid=0)
    cid_expired = _insert_default(repo, spreadsheet_id="ss_expired", sheet_gid=0)
    repo.update_status(cid_expired, status="auth_expired")

    healthy_ids = {row["connection_id"] for row in repo.list_healthy()}
    assert healthy_ids == {cid_healthy}

    all_ids = {row["connection_id"] for row in repo.list_all()}
    assert all_ids == {cid_healthy, cid_expired}


# ---------------------------------------------------------------------------
# Soft-disconnect convenience method
# ---------------------------------------------------------------------------


def test_soft_disconnect_sets_status_disconnected(db: Database) -> None:
    repo = GSheetConnectionsRepo(db)
    cid = _insert_default(repo)
    repo.soft_disconnect(cid, actor="cli")
    row = repo.get(cid)
    assert row is not None
    assert row["status"] == "disconnected"
