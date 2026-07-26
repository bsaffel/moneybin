"""Tests for audited user-created report storage."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import (
    OutputColumn,
    ReportQuery,
    ReportSemantics,
    ReportSpec,
)
from moneybin.repositories.user_reports_repo import (
    UserReportsRepo,
    mint_user_report_id,
)
from moneybin.services.undo_service import UndoService
from moneybin.sql.migrations.V045__create_app_user_reports import migrate
from moneybin.tables import USER_REPORTS
from tests.moneybin.migration_helpers import run_migration

_UNKNOWN_SEMANTICS = ReportSemantics(
    unit="rows",
    currency=None,
    sign="natural",
    kind="count",
    valuation_basis=None,
    fx_basis=None,
    time_basis="none",
    denominator=None,
    comparison_window=None,
    exclusions=(),
    provenance=(),
)


def _stub_runner(db: object) -> ReportQuery:
    """Stand in for a synthesized runner; the id is what these tests exercise."""
    return ReportQuery(sql="SELECT 1 AS n")


def _spec_with_id(report_id: str) -> ReportSpec:
    """Build the minimal spec whose only interesting field is ``report_id``."""
    return ReportSpec(
        report_id=report_id,
        name="saved_report",
        description="A saved report",
        view=USER_REPORTS,
        runner=_stub_runner,
        classes={"n": DataClass.AGGREGATE},
        columns=(
            OutputColumn(name="n", description="n", data_class=DataClass.AGGREGATE),
        ),
        semantics=_UNKNOWN_SEMANTICS,
    )


@pytest.mark.parametrize("nibble", list("0123456789abcdef"))
def test_minted_id_constructs_a_spec_for_every_possible_uuid_nibble(
    monkeypatch: pytest.MonkeyPatch, nibble: str
) -> None:
    """A minted id is letter-led whichever hex char uuid4 happens to start with.

    Exhausts the leading nibble rather than sampling generated ids: ``uuid4().hex``
    begins with a digit for 10 of 16 values, so a sampling test would pass
    roughly a third of the time with the letter prefix removed.
    """
    monkeypatch.setattr(
        "moneybin.repositories.user_reports_repo.uuid4",
        lambda: _StubUUID(nibble + "0123456789abcdef0123456789abcde"),
    )

    minted = mint_user_report_id()

    assert _spec_with_id(minted).report_id == minted


def test_minted_id_is_namespaced_to_the_user_tier() -> None:
    """The namespace keeps a saved report from colliding with a built-in id."""
    namespace, _, local = mint_user_report_id().partition(":")

    assert namespace == "user"
    assert len(local) == 13


def test_minted_ids_are_distinct() -> None:
    """Each mint is a fresh identity, so two saves cannot share a primary key."""
    assert len({mint_user_report_id() for _ in range(50)}) == 50


@dataclass(frozen=True, slots=True)
class _StubUUID:
    """Minimal stand-in for ``uuid.UUID`` exposing only the ``hex`` the mint reads."""

    hex: str


@pytest.fixture()
def repo(db: Database) -> UserReportsRepo:
    run_migration(db, migrate)
    return UserReportsRepo(db)


def _audit_rows_for(db: Database, report_id: str) -> list[tuple[Any, ...]]:
    return db.execute(
        """
        SELECT action, target_schema, target_table, target_id,
               before_value, after_value, actor
          FROM app.audit_log
         WHERE target_id = ?
         ORDER BY occurred_at ASC, audit_id ASC
        """,
        [report_id],
    ).fetchall()


def _create(repo: UserReportsRepo, **overrides: Any) -> str:
    """Save one report through the repo and return its minted ``report_id``."""
    kwargs: dict[str, Any] = {
        "name": "coffee-spend",
        "description": "Monthly coffee outlay",
        "query_sql": "SELECT COUNT(*) AS n FROM core.fct_transactions",
        "params": [{"name": "month", "annotation": "str", "required": True}],
        "classes": {"n": "AGGREGATE"},
        "semantics": {"kind": "unknown"},
        "class_fingerprint": "fp-0001",
        "actor": "cli",
    }
    event = repo.create(**(kwargs | overrides))
    assert event.target_id is not None
    return event.target_id


def test_create_stores_the_report_and_audits_the_complete_row(
    db: Database, repo: UserReportsRepo
) -> None:
    """A save writes one row and pairs it with one complete audit image."""
    report_id = _create(repo)

    stored = repo.get(report_id)
    assert stored is not None
    assert stored["name"] == "coffee-spend"
    assert stored["classes"] == {"n": "AGGREGATE"}
    assert stored["params"] == [
        {"name": "month", "annotation": "str", "required": True}
    ]
    assert stored["is_active"] is True

    audit = _audit_rows_for(db, report_id)
    assert len(audit) == 1
    action, schema, table, target_id, before, after, actor = audit[0]
    assert action == "user_report.create"
    assert (schema, table, target_id) == ("app", "user_reports", report_id)
    assert before is None
    assert json.loads(after)["class_fingerprint"] == "fp-0001"
    assert actor == "cli"


def test_set_captures_every_column_of_the_pre_mutation_row(
    db: Database, repo: UserReportsRepo
) -> None:
    """``before_value`` is the full prior row, not a diff of what changed."""
    report_id = _create(repo)

    repo.set(report_id, description="Rewritten", actor="cli")

    before = json.loads(_audit_rows_for(db, report_id)[-1][4])
    assert set(before) == {
        "report_id",
        "name",
        "description",
        "query_sql",
        "params",
        "classes",
        "semantics",
        "class_downgrades",
        "class_fingerprint",
        "is_active",
        "created_at",
        "updated_at",
    }
    assert before["description"] == "Monthly coffee outlay"


def test_set_leaves_columns_it_was_not_given_intact(repo: UserReportsRepo) -> None:
    """A partial update must not null the columns the caller omitted.

    The failure this catches is a single ``UPDATE`` that lists every column and
    binds ``None`` for the absent ones — which would silently discard the SQL,
    the class map, and the fingerprint on a description edit.
    """
    report_id = _create(repo)

    repo.set(report_id, description="Rewritten", actor="cli")

    stored = repo.get(report_id)
    assert stored is not None
    assert stored["description"] == "Rewritten"
    assert stored["query_sql"] == "SELECT COUNT(*) AS n FROM core.fct_transactions"
    assert stored["classes"] == {"n": "AGGREGATE"}
    assert stored["semantics"] == {"kind": "unknown"}
    assert stored["params"] == [
        {"name": "month", "annotation": "str", "required": True}
    ]
    assert stored["class_fingerprint"] == "fp-0001"
    assert stored["is_active"] is True


def test_set_can_write_a_null_description(repo: UserReportsRepo) -> None:
    """Clearing a nullable column is distinguishable from omitting it."""
    report_id = _create(repo)

    repo.set(report_id, description=None, actor="cli")

    stored = repo.get(report_id)
    assert stored is not None
    assert stored["description"] is None
    assert stored["query_sql"] == "SELECT COUNT(*) AS n FROM core.fct_transactions"


def test_set_of_metadata_alone_leaves_the_fingerprint_untouched(
    repo: UserReportsRepo,
) -> None:
    """A fresh fingerprint over a stale class map is the leak, not a stale one.

    Storing the current fingerprint on a metadata-only write would put the next
    run on the Match branch and serve the stale map with no re-resolution.
    """
    report_id = _create(repo)

    repo.set(report_id, description="Rewritten", is_active=False, actor="cli")

    stored = repo.get(report_id)
    assert stored is not None
    assert stored["class_fingerprint"] == "fp-0001"
    assert stored["is_active"] is False


def test_archiving_keeps_the_report_and_its_name(repo: UserReportsRepo) -> None:
    """Archive is visibility state: the row and its unique name both survive."""
    report_id = _create(repo)

    repo.set(report_id, is_active=False, actor="cli")

    assert repo.find_by_name("coffee-spend") is not None


def test_delete_captures_the_row_it_removed(
    db: Database, repo: UserReportsRepo
) -> None:
    """A hard delete audits the complete row so the generic undo can restore it."""
    report_id = _create(repo)

    repo.delete(report_id, actor="cli")

    assert repo.get(report_id) is None
    action, _, _, _, before, after, _ = _audit_rows_for(db, report_id)[-1]
    assert action == "user_report.delete"
    assert after is None
    assert json.loads(before)["query_sql"] == (
        "SELECT COUNT(*) AS n FROM core.fct_transactions"
    )


def test_a_deleted_report_is_restored_by_undo(
    db: Database, repo: UserReportsRepo
) -> None:
    """Full-row capture is why the table needs no ``deleted_at`` of its own."""
    report_id = _create(repo)
    event = repo.delete(report_id, actor="cli")

    UndoService(db).undo(event.operation_id, actor="cli")

    restored = repo.get(report_id)
    assert restored is not None
    assert restored["name"] == "coffee-spend"
    assert restored["classes"] == {"n": "AGGREGATE"}
    assert restored["class_fingerprint"] == "fp-0001"


def test_create_rolls_back_when_audit_raises(db: Database) -> None:
    """An unaudited saved report must not survive the transaction."""
    run_migration(db, migrate)
    audit = MagicMock()
    audit.record_audit_event.side_effect = RuntimeError("simulated audit failure")
    repo = UserReportsRepo(db, audit=audit)

    with pytest.raises(RuntimeError):
        _create(repo)

    assert db.execute("SELECT 1 FROM app.user_reports").fetchall() == []
