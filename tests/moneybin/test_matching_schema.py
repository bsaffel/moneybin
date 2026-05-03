"""Tests for matching schema initialization."""

from moneybin.database import Database
from moneybin.tables import MATCH_DECISIONS, SEED_SOURCE_PRIORITY


class TestMatchingSchema:
    """Tests for app.match_decisions and app.seed_source_priority schema initialization."""

    def test_match_decisions_table_exists(self, module_db: Database) -> None:
        result = module_db.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'app' AND table_name = 'match_decisions'"
        ).fetchone()
        assert result is not None
        assert result[0] == 1

    def test_match_decisions_columns(self, module_db: Database) -> None:
        cols = module_db.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'app' AND table_name = 'match_decisions' "
            "ORDER BY ordinal_position"
        ).fetchall()
        col_names = [c[0] for c in cols]
        assert "match_id" in col_names
        assert "source_transaction_id_a" in col_names
        assert "source_type_a" in col_names
        assert "source_origin_a" in col_names
        assert "confidence_score" in col_names
        assert "match_status" in col_names
        assert "match_tier" in col_names
        assert "reversed_at" in col_names

    def test_seed_source_priority_table_exists(self, module_db: Database) -> None:
        result = module_db.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'app' AND table_name = 'seed_source_priority'"
        ).fetchone()
        assert result is not None
        assert result[0] == 1

    def test_table_ref_constants(self) -> None:
        assert MATCH_DECISIONS.full_name == "app.match_decisions"
        assert SEED_SOURCE_PRIORITY.full_name == "app.seed_source_priority"
