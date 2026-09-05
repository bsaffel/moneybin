"""Tests for V054 categorization rule-conflict persistence."""

from __future__ import annotations

import duckdb
import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V054__create_app_rule_conflicts import migrate
from tests.moneybin.migration_helpers import run_migration

#: The columns a detection must supply; everything else carries a default.
_REQUIRED = (
    "conflict_id",
    "matcher_digest",
    "existing_rule_id",
    "existing_rule_updated_at",
    "existing_name",
    "existing_category",
    "existing_priority",
    "proposed_name",
    "proposed_merchant_pattern",
    "proposed_match_type",
    "proposed_category",
    "proposed_priority",
    "proposed_created_by",
)
_VALUES: dict[str, object] = {
    "conflict_id": "conf_0123456789abcdef",
    "matcher_digest": "a" * 64,
    "existing_rule_id": "0123456789ab",
    "existing_rule_updated_at": "2026-09-01 10:00:00",
    "existing_name": "Coffee",
    "existing_category": "Food & Drink",
    "existing_priority": 100,
    "proposed_name": "Coffee travel",
    "proposed_merchant_pattern": "STARBUCKS",
    "proposed_match_type": "contains",
    "proposed_category": "Travel",
    "proposed_priority": 100,
    "proposed_created_by": "ai",
}


def _insert(db: Database, columns: tuple[str, ...], **overrides: object) -> None:
    """Insert one conflict row using ``columns`` and the shared defaults."""
    values = _VALUES | overrides
    db.execute(
        f"INSERT INTO app.rule_conflicts ({', '.join(columns)}) "  # noqa: S608  # allowlisted column names, parameterized values
        f"VALUES ({', '.join('?' * len(columns))})",
        [values[column] for column in columns],
    )


def test_v054_creates_rule_conflicts(db: Database) -> None:
    """The migration creates the conflict queue with its full shape."""
    run_migration(db, migrate)

    columns = db.execute(
        """SELECT column_name FROM information_schema.columns
               WHERE table_schema = 'app'
                 AND table_name = 'rule_conflicts'
             ORDER BY ordinal_position"""
    ).fetchall()
    assert [row[0] for row in columns] == [
        "conflict_id",
        "matcher_digest",
        "existing_rule_id",
        "existing_rule_updated_at",
        "existing_name",
        "existing_category",
        "existing_subcategory",
        "existing_priority",
        "proposed_name",
        "proposed_merchant_pattern",
        "proposed_match_type",
        "proposed_min_amount",
        "proposed_max_amount",
        "proposed_account_id",
        "proposed_category",
        "proposed_subcategory",
        "proposed_priority",
        "proposed_created_by",
        "status",
        "resolution",
        "resolved_rule_id",
        "detected_at",
        "resolved_at",
    ]


def test_v054_defaults_leave_a_conflict_pending(db: Database) -> None:
    """A detection that states no decision is queued, not silently settled."""
    run_migration(db, migrate)

    _insert(db, _REQUIRED)

    assert db.execute(
        "SELECT status, resolution, resolved_rule_id, resolved_at "
        "FROM app.rule_conflicts"
    ).fetchone() == ("pending", None, None, None)


def test_v054_amount_bounds_store_at_the_rule_grain(db: Database) -> None:
    """The bounds are part of matcher identity, so they match the rule column."""
    run_migration(db, migrate)

    _insert(
        db,
        (*_REQUIRED, "proposed_min_amount", "proposed_max_amount"),
        proposed_min_amount="5.005",
        proposed_max_amount="50",
    )

    row = db.execute(
        "SELECT proposed_min_amount, proposed_max_amount FROM app.rule_conflicts"
    ).fetchone()
    assert row is not None
    assert str(row[0]) == "5.01"
    assert str(row[1]) == "50.00"


def test_v054_refuses_two_rows_for_one_conflict(db: Database) -> None:
    """Re-detection must land on the queued row, not queue a second decision."""
    run_migration(db, migrate)
    _insert(db, _REQUIRED)

    with pytest.raises(duckdb.ConstraintException):
        _insert(db, _REQUIRED, proposed_name="Coffee travel again")


@pytest.mark.parametrize(
    "omitted",
    [
        "matcher_digest",
        "existing_rule_id",
        "existing_rule_updated_at",
        "existing_category",
        "proposed_merchant_pattern",
        "proposed_category",
    ],
)
def test_v054_requires_every_side_of_the_comparison(db: Database, omitted: str) -> None:
    """A row missing either side cannot explain the disagreement it records."""
    run_migration(db, migrate)

    kept = tuple(column for column in _REQUIRED if column != omitted)

    with pytest.raises(duckdb.ConstraintException):
        _insert(db, kept)


def test_v054_is_idempotent(db: Database) -> None:
    """Fresh installs and migration upgrades may both invoke the DDL."""
    run_migration(db, migrate)
    run_migration(db, migrate)

    assert db.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'app' AND table_name = 'rule_conflicts'
        """
    ).fetchone() == (1,)
