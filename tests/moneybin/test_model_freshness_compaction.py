"""Model execution uses physical alteration time, including compacted intervals."""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlmesh.core.dialect import parse

from moneybin.database import Database

_EXECUTED = 1762061400456
_COMPACTED = _EXECUTED + 86_400_000


@pytest.fixture
def interval_state(db: Database) -> Database:
    db.execute("CREATE SCHEMA IF NOT EXISTS sqlmesh")
    db.execute("""CREATE TABLE sqlmesh._snapshots (
        name VARCHAR, version VARCHAR, updated_ts BIGINT, kind_name VARCHAR
    )""")
    db.execute("""CREATE TABLE sqlmesh._intervals (
        name VARCHAR, version VARCHAR, created_ts BIGINT, last_altered_ts BIGINT,
        is_dev BOOLEAN, is_removed BOOLEAN, is_pending_restatement BOOLEAN,
        is_compacted BOOLEAN
    )""")
    db.execute(
        """INSERT INTO sqlmesh._snapshots
        VALUES ('core.example', 'v1', ?, 'FULL')""",
        [_EXECUTED],
    )
    return db


def _execution(db: Database) -> datetime | None:
    source = (
        Path(__file__).resolve().parents[2]
        / "src/moneybin/sqlmesh/models/meta/model_freshness.sql"
    ).read_text()
    query = parse(source, default_dialect="duckdb")[-1]
    assert query is not None
    row = db.execute(query.sql(dialect="duckdb")).fetchone()
    assert row is not None
    return row[3]


def test_compaction_time_does_not_replace_physical_execution_time(
    interval_state: Database,
) -> None:
    db = interval_state
    # SQLMesh compaction replaces created_ts, preserving last_altered_ts.
    db.execute(
        """INSERT INTO sqlmesh._intervals
        VALUES ('core.example', 'v1', ?, ?, FALSE, FALSE, FALSE, TRUE)""",
        [_COMPACTED, _EXECUTED],
    )
    assert _execution(db) == datetime(2025, 11, 2, 5, 30, 0, 456000, tzinfo=UTC)


def test_legacy_compacted_interval_without_execution_evidence_stays_unknown(
    interval_state: Database,
) -> None:
    db = interval_state
    # SQLMesh's nullable-column migration cannot recover a historical execution.
    db.execute(
        """INSERT INTO sqlmesh._intervals
        VALUES ('core.example', 'v1', ?, NULL, FALSE, FALSE, FALSE, TRUE)""",
        [_COMPACTED],
    )
    assert _execution(db) is None


@pytest.mark.parametrize(
    ("is_dev", "is_removed", "is_pending"),
    [(True, False, False), (False, True, False), (False, False, True)],
    ids=["development", "removed", "pending-restatement"],
)
def test_non_production_or_unfinished_intervals_cannot_advance_execution(
    interval_state: Database, is_dev: bool, is_removed: bool, is_pending: bool
) -> None:
    db = interval_state
    db.execute(
        """INSERT INTO sqlmesh._intervals VALUES
        ('core.example', 'v1', ?, ?, FALSE, FALSE, FALSE, TRUE),
        ('core.example', 'v1', ?, ?, ?, ?, ?, FALSE)""",
        [_EXECUTED, _EXECUTED, _COMPACTED, _COMPACTED, is_dev, is_removed, is_pending],
    )
    assert _execution(db) == datetime(2025, 11, 2, 5, 30, 0, 456000, tzinfo=UTC)
