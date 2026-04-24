"""Tests for int_transactions__merged model structure."""

from pathlib import Path


class TestIntTransactionsMergedModel:
    """Tests for the int_transactions__merged SQLMesh VIEW model."""

    def test_model_file_exists(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "prep"
            / "int_transactions__merged.sql"
        )
        assert model_path.exists()

    def test_model_has_merge_logic(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "prep"
            / "int_transactions__merged.sql"
        )
        content = model_path.read_text()
        assert "seed_source_priority" in content
        assert "GROUP BY" in content
        assert "transaction_id" in content
        assert "canonical_source_type" in content
        assert "source_count" in content
        assert (
            "MIN(m.transaction_date)" in content or "MIN(transaction_date)" in content
        )

    def test_model_is_view(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "prep"
            / "int_transactions__merged.sql"
        )
        content = model_path.read_text()
        assert "kind VIEW" in content
