"""Tests for int_transactions__unioned model structure."""

from pathlib import Path


class TestIntTransactionsUnionedModel:
    """Structural tests for the int_transactions__unioned SQLMesh model."""

    def test_model_file_exists(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "prep"
            / "int_transactions__unioned.sql"
        )
        assert model_path.exists()

    def test_model_has_required_columns(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "prep"
            / "int_transactions__unioned.sql"
        )
        content = model_path.read_text()
        assert "source_transaction_id" in content
        assert "source_type" in content
        assert "source_origin" in content
        assert "account_id" in content
        assert "transaction_date" in content
        assert "amount" in content
        assert "description" in content
        assert "UNION ALL" in content

    def test_model_is_view(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "prep"
            / "int_transactions__unioned.sql"
        )
        content = model_path.read_text()
        assert "kind VIEW" in content
