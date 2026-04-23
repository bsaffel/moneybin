"""Tests for OFX staging model Tier 2a dedup and new columns."""

from pathlib import Path


class TestStgOfxTransactionsModel:
    """Tests for OFX staging model Tier 2a dedup and new columns."""

    def test_model_has_row_number_dedup(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "prep"
            / "stg_ofx__transactions.sql"
        )
        content = model_path.read_text()
        assert "ROW_NUMBER()" in content
        assert "PARTITION BY" in content
        assert "source_transaction_id" in content
        assert "_row_num = 1" in content

    def test_model_has_source_columns(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "prep"
            / "stg_ofx__transactions.sql"
        )
        content = model_path.read_text()
        assert "'ofx' AS source_type" in content
        assert "source_origin" in content
        assert "source_transaction_id" in content
