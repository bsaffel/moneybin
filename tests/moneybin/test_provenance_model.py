"""Tests for meta.fct_transaction_provenance model structure."""

from pathlib import Path


class TestProvenanceModel:
    """Structural tests for the provenance model."""

    def test_model_file_exists(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "meta"
            / "fct_transaction_provenance.sql"
        )
        assert model_path.exists()

    def test_model_has_required_columns(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "meta"
            / "fct_transaction_provenance.sql"
        )
        content = model_path.read_text()
        assert "transaction_id" in content
        assert "source_transaction_id" in content
        assert "source_type" in content
        assert "source_origin" in content
        assert "source_file" in content
        assert "source_extracted_at" in content
        assert "match_id" in content

    def test_model_is_view(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "meta"
            / "fct_transaction_provenance.sql"
        )
        content = model_path.read_text()
        assert "kind VIEW" in content
