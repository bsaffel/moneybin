"""Tests for updated fct_transactions model."""

from pathlib import Path


class TestFctTransactionsModel:
    """Structural tests for the rewritten fct_transactions model."""

    def test_reads_from_merged_layer(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "core"
            / "fct_transactions.sql"
        )
        content = model_path.read_text()
        assert "int_transactions__merged" in content
        # Should NOT have the old UNION ALL of staging CTEs
        assert "stg_ofx__transactions" not in content
        assert "stg_tabular__transactions" not in content

    def test_has_new_columns(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "core"
            / "fct_transactions.sql"
        )
        content = model_path.read_text()
        assert "canonical_source_type" in content
        assert "source_count" in content
        assert "match_confidence" in content
