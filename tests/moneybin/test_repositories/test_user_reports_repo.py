"""Tests for audited user-created report storage."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import pytest
from prometheus_client import REGISTRY

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


def _metric(action: str) -> float:
    return (
        REGISTRY.get_sample_value(
            "moneybin_app_mutation_audit_emitted_total",
            {"repository": "user_reports", "action": action},
        )
        or 0.0
    )


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
    before_metric = _metric("user_report.create")

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
    assert _metric("user_report.create") == before_metric + 1


def test_set_captures_every_column_of_the_pre_mutation_row(
    db: Database, repo: UserReportsRepo
) -> None:
    """``before_value`` is the full prior row, not a diff of what changed."""
    report_id = _create(repo)
    before_metric = _metric("user_report.set")

    repo.set(report_id, description="Rewritten", actor="cli")

    assert _metric("user_report.set") == before_metric + 1
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


def test_the_repo_does_not_enforce_the_masking_floor(
    repo: UserReportsRepo,
) -> None:
    """Pins where the "only reclassify may lower a class" rule does *not* live.

    ``set`` and ``create`` take whatever ``classes`` map they are handed. The
    floor — confirmation, ``is_weaker_class``, and the audited reason — is
    enforced one layer up in ``UserReportsService.reclassify``, deliberately: the
    repo is the audited write primitive for every mutation, and a class check here
    would have to be satisfied by the save path too, which legitimately writes any
    derived class.

    So this test asserts a *weakening lands* — not because that is desirable, but
    because a future caller reaching this method directly (a script, a migration,
    a new service method) bypasses the gate entirely and nothing here would say
    so. If a floor is ever added at this layer, this test should fail and be
    replaced by its inverse.
    """
    report_id = _create(repo, classes={"n": DataClass.ROUTING_NUMBER.value})

    repo.set(report_id, classes={"n": DataClass.AGGREGATE.value}, actor="script")

    stored = repo.get(report_id)
    assert stored is not None
    assert stored["classes"] == {"n": DataClass.AGGREGATE.value}
    # The audit row is the only trace such a call leaves: no reason, no
    # confirmation, no `class_downgrades` entry naming the column.
    assert stored["class_downgrades"] == {}


def test_list_filters_archived_reports_at_the_repo_layer(
    repo: UserReportsRepo,
) -> None:
    """``list()``'s own SQL, exercised here rather than only through the service."""
    active = _create(repo, name="active-report")
    archived = _create(repo, name="archived-report")
    repo.set(archived, is_active=False, actor="cli")

    default = {str(row["report_id"]) for row in repo.list()}
    including = {str(row["report_id"]) for row in repo.list(include_archived=True)}

    assert default == {active}
    assert including == {active, archived}


def test_list_returns_nothing_when_the_table_is_absent(db: Database) -> None:
    """``list()`` answers empty rather than raising when the table is not there.

    A read-only surface can open a database whose schema predates this table, and
    it must be able to enumerate an empty catalog instead of failing — so the
    method catches DuckDB's ``CatalogException``.

    The table is dropped explicitly rather than by skipping ``run_migration``:
    ``init_schemas`` creates ``app.user_reports`` from the checked-in DDL, so
    omitting the migration leaves the table in place and the test passes with the
    fallback deleted — proving nothing. Verified by removing the ``except``.
    """
    db.execute(f"DROP TABLE {USER_REPORTS.full_name}")

    assert UserReportsRepo(db).list() == []


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
    before_metric = _metric("user_report.delete")

    repo.delete(report_id, actor="cli")

    assert _metric("user_report.delete") == before_metric + 1
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
