"""Tests for V001 migration: OFX transaction_id → source_transaction_id rename."""

from pathlib import Path

from moneybin.migrations import Migration


class TestV001Migration:
    """Tests for the V001 migration file and OFX schema DDL."""

    def test_migration_file_parses(self) -> None:
        migration_dir = (
            Path(__file__).resolve().parents[2]
            / "src"
            / "moneybin"
            / "sql"
            / "migrations"
        )
        path = migration_dir / "V001__rename_ofx_transaction_id.py"
        assert path.exists(), f"Migration file not found: {path}"
        migration = Migration.from_file(path)
        assert migration.version == 1
        assert migration.file_type == "py"

    def test_ofx_schema_uses_source_transaction_id(self) -> None:
        schema_path = (
            Path(__file__).resolve().parents[2]
            / "src"
            / "moneybin"
            / "sql"
            / "schema"
            / "raw_ofx_transactions.sql"
        )
        content = schema_path.read_text()
        assert "source_transaction_id" in content
        lines = [
            line
            for line in content.split("\n")
            if not line.strip().startswith("--") and not line.strip().startswith("/*")
        ]
        ddl_text = "\n".join(lines)
        assert "source_transaction_id VARCHAR" in ddl_text


class TestOFXExtractorColumnName:
    """Tests that OFXExtractor outputs the renamed source_transaction_id column."""

    def test_extractor_outputs_source_transaction_id(self) -> None:
        """OFX extractor DataFrame must use source_transaction_id column."""
        from moneybin.extractors.ofx_extractor import OFXExtractor

        extractor = OFXExtractor()
        empty_df = extractor._build_empty_transactions_df()
        assert "source_transaction_id" in empty_df.columns
        assert "transaction_id" not in empty_df.columns
