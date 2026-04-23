"""Tests for int_transactions__matched model structure."""

from pathlib import Path


class TestIntTransactionsMatchedModel:
    """Tests for the int_transactions__matched SQLMesh VIEW model."""

    def test_model_file_exists(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "prep"
            / "int_transactions__matched.sql"
        )
        assert model_path.exists()

    def test_model_outputs_transaction_id(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "prep"
            / "int_transactions__matched.sql"
        )
        content = model_path.read_text()
        assert "transaction_id" in content
        assert "sha256" in content.lower()
        assert "source_transaction_id" in content
        assert "match_decisions" in content

    def test_model_is_view(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "prep"
            / "int_transactions__matched.sql"
        )
        content = model_path.read_text()
        assert "kind VIEW" in content
