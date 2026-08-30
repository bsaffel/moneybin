"""Tests for V045 user-created report persistence."""

from __future__ import annotations

import duckdb
import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V045__create_app_user_reports import migrate
from tests.moneybin.migration_helpers import run_migration

#: The columns a save must supply; everything else carries a default.
_REQUIRED = (
    "report_id",
    "name",
    "query_sql",
    "classes",
    "semantics",
    "class_fingerprint",
)
_VALUES: dict[str, str] = {
    "report_id": "user:r0123456789ab",
    "name": "coffee-spend",
    "query_sql": "SELECT COUNT(*) AS n FROM core.fct_transactions",
    "classes": '{"n": "AGGREGATE"}',
    "semantics": '{"kind": "unknown"}',
    "class_fingerprint": "fp-0001",
}


def _insert(db: Database, columns: tuple[str, ...], **overrides: str) -> None:
    """Insert one saved-report row using ``columns`` and the shared defaults."""
    values = _VALUES | overrides
    db.execute(
        f"INSERT INTO app.user_reports ({', '.join(columns)}) "  # noqa: S608  # allowlisted column names, parameterized values
        f"VALUES ({', '.join('?' * len(columns))})",
        [values[column] for column in columns],
    )


def test_v045_creates_user_reports(db: Database) -> None:
    """The migration creates the saved-report registry with its full shape."""
    run_migration(db, migrate)

    db.execute("SELECT * FROM app.user_reports LIMIT 0")
    columns = db.execute(
        """SELECT column_name FROM information_schema.columns
               WHERE table_schema = 'app'
                 AND table_name = 'user_reports'
             ORDER BY ordinal_position"""
    ).fetchall()
    assert [row[0] for row in columns] == [
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
    ]


def test_v045_defaults_leave_a_saved_report_addressable(db: Database) -> None:
    """A save declaring no parameters still stores maps the run path can key on."""
    run_migration(db, migrate)

    _insert(db, _REQUIRED)

    assert db.execute(
        "SELECT params, class_downgrades, is_active, description FROM app.user_reports"
    ).fetchone() == ("[]", "{}", True, None)


def test_v045_refuses_two_reports_sharing_one_name(db: Database) -> None:
    """A duplicate name would make the catalog and its runner ambiguous."""
    run_migration(db, migrate)
    _insert(db, _REQUIRED)

    with pytest.raises(duckdb.ConstraintException):
        _insert(db, _REQUIRED, report_id="user:rba9876543210")


@pytest.mark.parametrize(
    "omitted", ["query_sql", "classes", "semantics", "class_fingerprint"]
)
def test_v045_requires_every_derivation_output(db: Database, omitted: str) -> None:
    """A row missing a derivation output could serve columns it never classified."""
    run_migration(db, migrate)

    kept = tuple(column for column in _REQUIRED if column != omitted)

    with pytest.raises(duckdb.ConstraintException):
        _insert(db, kept)


def test_v045_is_idempotent(db: Database) -> None:
    """Fresh installs and migration upgrades may both invoke the DDL."""
    run_migration(db, migrate)
    run_migration(db, migrate)

    assert db.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'app' AND table_name = 'user_reports'
        """
    ).fetchone() == (1,)
