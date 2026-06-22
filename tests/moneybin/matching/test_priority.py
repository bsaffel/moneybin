"""Tests for source priority seeding."""

from moneybin.config import MatchingSettings
from moneybin.database import Database
from moneybin.matching.priority import seed_source_priority


class TestSeedSourcePriority:
    """Tests for seed_source_priority."""

    def test_writes_default_priorities(self, db: Database) -> None:
        settings = MatchingSettings()
        seed_source_priority(db, settings)
        rows = db.execute(
            "SELECT source_type, priority FROM app.seed_source_priority "
            "ORDER BY priority"
        ).fetchall()
        assert len(rows) == 10
        assert rows[0] == ("manual", 1)
        assert rows[1] == ("gsheet", 2)
        assert rows[2] == ("ofx", 3)
        assert rows[3] == ("plaid", 4)
        assert rows[-1] == ("pipe", 10)

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
        assert row[0] == 10
