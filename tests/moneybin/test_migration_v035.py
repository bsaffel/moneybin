"""V035: created_by rebuild preserves rows and enforces the CHECK.

V035 rebuilds ``app.securities`` to add the ``created_by`` provenance column
(DuckDB cannot ``ADD COLUMN`` with a CHECK constraint). Per
``.claude/rules/database.md`` "Migration test data realism", the fixture
seeds >=3 rows with non-trivial values across the nullable columns the
rebuild's INSERT...SELECT must preserve, and the mutation tests drive
``migrate()`` through ``run_migration()`` to reproduce the runner's
enclosing BEGIN/COMMIT transaction (the V034 idiom for this same rebuild
pattern). Also asserts the ``security_type`` and ``cost_basis_method``
CHECKs the rebuild re-creates still bite post-migration (mirroring
``test_migration_v034.py::test_check_constraint_enforced_after_migration``),
and exercises every CHECK on the two new fresh-schema tables,
``app.security_links`` and ``app.security_link_decisions`` — the CHECKs
are the entire correctness content of those DDL files.
"""

from __future__ import annotations

from datetime import datetime

import duckdb
import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V035__add_securities_created_by import migrate
from tests.moneybin.migration_helpers import run_migration

pytestmark = pytest.mark.fresh_db

_OLD_SHAPE = """
CREATE TABLE app.securities (
    security_id VARCHAR NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    security_type VARCHAR NOT NULL,
    ticker VARCHAR, exchange VARCHAR, cusip VARCHAR, isin VARCHAR, figi VARCHAR,
    coingecko_id VARCHAR, is_cash_equivalent BOOLEAN, cost_basis_method VARCHAR,
    currency_code VARCHAR NOT NULL DEFAULT 'USD',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""


@pytest.fixture
def old_shape_db(db: Database) -> Database:
    """The pre-V035 app.securities shape, populated with >=3 realistic rows."""
    db.execute("DROP TABLE app.securities")
    db.execute(_OLD_SHAPE)
    db.execute(
        """
        INSERT INTO app.securities (
            security_id, name, security_type, ticker, exchange, cusip, isin,
            figi, coingecko_id, is_cash_equivalent, cost_basis_method,
            currency_code, created_at, updated_at
        ) VALUES
        ('abc123def456', 'Apple Inc.', 'equity', 'AAPL', 'NASDAQ',
         '037833100', 'US0378331005', 'BBG000B9XRY4', NULL, FALSE, 'fifo',
         'USD', TIMESTAMP '2024-01-02 03:04:05', TIMESTAMP '2024-01-02 03:04:05'),
        ('bitcoin000001', 'Bitcoin', 'crypto', NULL, NULL, NULL, NULL, NULL,
         'bitcoin', FALSE, NULL, 'USD',
         TIMESTAMP '2024-02-03 04:05:06', TIMESTAMP '2024-02-03 04:05:06'),
        ('moneymkt000001', 'Fidelity Government MM', 'cash', NULL, NULL, NULL,
         NULL, NULL, NULL, TRUE, NULL, 'USD',
         TIMESTAMP '2024-03-04 05:06:07', TIMESTAMP '2024-03-04 05:06:07')
        """
    )
    return db


def test_v035_backfills_user_and_preserves_rows(old_shape_db: Database) -> None:
    run_migration(old_shape_db, migrate)
    rows = old_shape_db.execute(
        "SELECT security_id, created_by FROM app.securities ORDER BY security_id"
    ).fetchall()
    assert rows == [
        ("abc123def456", "user"),
        ("bitcoin000001", "user"),
        ("moneymkt000001", "user"),
    ]


def test_v035_preserves_all_existing_columns(old_shape_db: Database) -> None:
    """The rebuild's INSERT...SELECT column list must not silently drop a column."""
    run_migration(old_shape_db, migrate)

    row = old_shape_db.execute(
        """
        SELECT name, security_type, ticker, exchange, cusip, isin, figi,
               coingecko_id, is_cash_equivalent, cost_basis_method,
               currency_code, created_at, updated_at
          FROM app.securities
         WHERE security_id = 'abc123def456'
        """
    ).fetchone()
    assert row == (
        "Apple Inc.",
        "equity",
        "AAPL",
        "NASDAQ",
        "037833100",
        "US0378331005",
        "BBG000B9XRY4",
        None,
        False,
        "fifo",
        "USD",
        datetime(2024, 1, 2, 3, 4, 5),
        datetime(2024, 1, 2, 3, 4, 5),
    )


def test_v035_is_idempotent(old_shape_db: Database) -> None:
    run_migration(old_shape_db, migrate)
    run_migration(old_shape_db, migrate)
    row = old_shape_db.execute("SELECT COUNT(*) FROM app.securities").fetchone()
    assert row is not None and row[0] == 3


def test_v035_check_rejects_unknown_provenance(old_shape_db: Database) -> None:
    run_migration(old_shape_db, migrate)
    with pytest.raises(duckdb.ConstraintException):
        old_shape_db.execute(
            "INSERT INTO app.securities (security_id, name, security_type, created_by) "
            "VALUES ('x1y2z3a4b5c6', 'X Corp', 'equity', 'ofx')"
        )


def test_v035_security_type_check_preserved_after_rebuild(
    old_shape_db: Database,
) -> None:
    """A future edit dropping the security_type CHECK from the rebuild must fail here."""
    run_migration(old_shape_db, migrate)
    with pytest.raises(duckdb.ConstraintException):
        old_shape_db.execute(
            "INSERT INTO app.securities (security_id, name, security_type) "
            "VALUES ('z9y8x7w6v5u4', 'Bogus Corp', 'not_a_real_type')"
        )


def test_v035_cost_basis_method_check_preserved_after_rebuild(
    old_shape_db: Database,
) -> None:
    """A future edit dropping the cost_basis_method CHECK from the rebuild must fail here."""
    run_migration(old_shape_db, migrate)
    old_shape_db.execute(
        "UPDATE app.securities SET cost_basis_method = 'specific' "
        "WHERE security_id = 'bitcoin000001'"
    )
    with pytest.raises(duckdb.ConstraintException):
        old_shape_db.execute(
            "UPDATE app.securities SET cost_basis_method = 'lifo' "
            "WHERE security_id = 'moneymkt000001'"
        )


def test_fresh_schema_has_security_link_tables(db: Database) -> None:
    for table in ("app.security_links", "app.security_link_decisions"):
        row = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()  # noqa: S608  # fixed table list
        assert row is not None and row[0] == 0


def _security_link_row(link_id: str, **overrides: object) -> dict[str, object]:
    """A valid app.security_links row; override columns under test."""
    row: dict[str, object] = {
        "link_id": link_id,
        "security_id": "sec000000001",
        "ref_kind": "plaid_security_id",
        "ref_value": "eq_plaid_sec_1",
        "source_type": "plaid",
        "status": "accepted",
        "decided_by": "auto",
        "decided_at": datetime(2024, 1, 2, 3, 4, 5),
        "reversed_at": None,
        "reversed_by": None,
    }
    row.update(overrides)
    return row


def _insert_security_link(db: Database, link_id: str, **overrides: object) -> None:
    row = _security_link_row(link_id, **overrides)
    columns = list(row)
    placeholders = ", ".join("?" * len(columns))
    db.execute(
        "INSERT INTO app.security_links "  # noqa: S608  # fixed column list, not user input
        f"({', '.join(columns)}) VALUES ({placeholders})",
        list(row.values()),
    )


def _security_link_decision_row(
    decision_id: str, **overrides: object
) -> dict[str, object]:
    """A valid app.security_link_decisions row; override columns under test."""
    row: dict[str, object] = {
        "decision_id": decision_id,
        "ref_kind": "plaid_security_id",
        "ref_value": "eq_plaid_sec_1",
        "source_type": "plaid",
        "candidate_security_id": "sec000000001",
        "status": "pending",
        "decided_by": "auto",
        "decided_at": datetime(2024, 1, 2, 3, 4, 5),
        "reversed_at": None,
        "reversed_by": None,
    }
    row.update(overrides)
    return row


def _insert_security_link_decision(
    db: Database, decision_id: str, **overrides: object
) -> None:
    row = _security_link_decision_row(decision_id, **overrides)
    columns = list(row)
    placeholders = ", ".join("?" * len(columns))
    db.execute(
        "INSERT INTO app.security_link_decisions "  # noqa: S608  # fixed column list, not user input
        f"({', '.join(columns)}) VALUES ({placeholders})",
        list(row.values()),
    )


class TestSecurityLinksChecks:
    """Every enum column on app.security_links is CHECK-constrained -- the only test coverage those constraints have before SecurityLinksRepo lands."""

    def test_ref_kind_accepts_valid_values(self, db: Database) -> None:
        for i, value in enumerate(("plaid_security_id", "institution_security_id")):
            _insert_security_link(db, f"linkrefkind{i:03d}", ref_kind=value)

    def test_ref_kind_rejects_unknown_value(self, db: Database) -> None:
        with pytest.raises(duckdb.ConstraintException):
            _insert_security_link(db, "linkrefkindbad", ref_kind="bogus_ref_kind")

    def test_status_accepts_valid_values(self, db: Database) -> None:
        for i, value in enumerate(("accepted", "reversed")):
            _insert_security_link(db, f"linkstatus{i:03d}", status=value)

    def test_status_rejects_unknown_value(self, db: Database) -> None:
        # 'pending' is a valid status on the sibling decisions table but not
        # here -- proves the CHECK is scoped to this table, not copy-pasted.
        with pytest.raises(duckdb.ConstraintException):
            _insert_security_link(db, "linkstatusbad", status="pending")

    def test_decided_by_accepts_valid_values(self, db: Database) -> None:
        for i, value in enumerate(("auto", "user", "system")):
            _insert_security_link(db, f"linkdecby{i:03d}", decided_by=value)

    def test_decided_by_rejects_unknown_value(self, db: Database) -> None:
        with pytest.raises(duckdb.ConstraintException):
            _insert_security_link(db, "linkdecbybad", decided_by="admin")

    def test_reversed_by_accepts_valid_values(self, db: Database) -> None:
        for i, value in enumerate((None, "auto", "user", "system")):
            _insert_security_link(db, f"linkrevby{i:03d}", reversed_by=value)

    def test_reversed_by_rejects_unknown_value(self, db: Database) -> None:
        with pytest.raises(duckdb.ConstraintException):
            _insert_security_link(db, "linkrevbybad", reversed_by="admin")


class TestSecurityLinkDecisionsChecks:
    """Every enum column on app.security_link_decisions is CHECK-constrained, including reversed_by."""

    def test_ref_kind_accepts_valid_values(self, db: Database) -> None:
        for i, value in enumerate(("plaid_security_id", "institution_security_id")):
            _insert_security_link_decision(db, f"decrefkind{i:03d}", ref_kind=value)

    def test_ref_kind_rejects_unknown_value(self, db: Database) -> None:
        with pytest.raises(duckdb.ConstraintException):
            _insert_security_link_decision(
                db, "decrefkindbad", ref_kind="bogus_ref_kind"
            )

    def test_status_accepts_valid_values(self, db: Database) -> None:
        for i, value in enumerate(("pending", "accepted", "rejected", "reversed")):
            _insert_security_link_decision(db, f"decstatus{i:03d}", status=value)

    def test_status_rejects_unknown_value(self, db: Database) -> None:
        with pytest.raises(duckdb.ConstraintException):
            _insert_security_link_decision(db, "decstatusbad", status="merged")

    def test_decided_by_accepts_valid_values(self, db: Database) -> None:
        for i, value in enumerate(("auto", "user")):
            _insert_security_link_decision(db, f"decdecby{i:03d}", decided_by=value)

    def test_decided_by_rejects_unknown_value(self, db: Database) -> None:
        # 'system' is a valid decided_by on the sibling security_links table
        # but not here -- proves the CHECK is scoped to this table.
        with pytest.raises(duckdb.ConstraintException):
            _insert_security_link_decision(db, "decdecbybad", decided_by="system")

    def test_reversed_by_accepts_valid_values(self, db: Database) -> None:
        for i, value in enumerate((None, "auto", "user")):
            _insert_security_link_decision(db, f"decrevby{i:03d}", reversed_by=value)

    def test_reversed_by_rejects_unknown_value(self, db: Database) -> None:
        # Discriminating case for the reversed_by CHECK added to
        # app_security_link_decisions.sql: before that fix this column was
        # unconstrained VARCHAR, so this insert would have succeeded.
        with pytest.raises(duckdb.ConstraintException):
            _insert_security_link_decision(db, "decrevbybad", reversed_by="system")
