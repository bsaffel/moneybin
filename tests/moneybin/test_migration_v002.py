"""Tests for V002 migration: backfill app FK gold keys."""

from pathlib import Path

from moneybin.migrations import Migration


class TestV002Migration:
    """Verify V002 migration file exists and contains expected SQL."""

    def test_migration_file_parses(self) -> None:
        migration_dir = (
            Path(__file__).resolve().parents[2]
            / "src"
            / "moneybin"
            / "sql"
            / "migrations"
        )
        path = migration_dir / "V002__backfill_gold_keys.sql"
        assert path.exists(), f"Migration file not found: {path}"
        migration = Migration.from_file(path)
        assert migration.version == 2
        assert migration.file_type == "sql"

    def test_migration_contains_sha256_hash(self) -> None:
        migration_dir = (
            Path(__file__).resolve().parents[2]
            / "src"
            / "moneybin"
            / "sql"
            / "migrations"
        )
        path = migration_dir / "V002__backfill_gold_keys.sql"
        content = path.read_text()
        assert "sha256" in content.lower()
        assert "transaction_categories" in content
        assert "transaction_notes" in content
