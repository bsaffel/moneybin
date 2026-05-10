"""Tests for source priority seeding."""

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.config import MatchingSettings
from moneybin.database import Database
from moneybin.matching.priority import seed_source_priority


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Generator[Database, None, None]:
    """Provide a temporary test database with app schema initialized."""
    database = Database(
        tmp_path / "test.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    yield database
    database.close()


class TestSeedSourcePriority:
    """Tests for seed_source_priority."""

    def test_writes_default_priorities(self, db: Database) -> None:
        settings = MatchingSettings()
        seed_source_priority(db, settings)
        rows = db.execute(
            "SELECT source_type, priority FROM app.seed_source_priority "
            "ORDER BY priority"
        ).fetchall()
        assert len(rows) == 9
        assert rows[0] == ("manual", 1)
        assert rows[1] == ("plaid", 2)
        assert rows[2] == ("csv", 3)
        assert rows[-1] == ("ofx", 9)

    def test_replaces_on_rerun(self, db: Database) -> None:
        settings = MatchingSettings()
        seed_source_priority(db, settings)
        custom = MatchingSettings(source_priority=["ofx", "csv"])
        seed_source_priority(db, custom)
        rows = db.execute(
            "SELECT source_type, priority FROM app.seed_source_priority "
            "ORDER BY priority"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0] == ("ofx", 1)
        assert rows[1] == ("csv", 2)

    def test_idempotent(self, db: Database) -> None:
        settings = MatchingSettings()
        seed_source_priority(db, settings)
        seed_source_priority(db, settings)
        row = db.execute("SELECT COUNT(*) FROM app.seed_source_priority").fetchone()
        assert row is not None
        assert row[0] == 9
