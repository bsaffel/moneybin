"""Tests for int_transactions__merged model structure."""

from moneybin.database import SQLMESH_ROOT


class TestIntTransactionsMergedModel:
    """Tests for the int_transactions__merged SQLMesh VIEW model."""

    def test_model_file_exists(self) -> None:
        model_path = SQLMESH_ROOT / "models" / "prep" / "int_transactions__merged.sql"
        assert model_path.exists()

    def test_model_has_merge_logic(self) -> None:
        model_path = SQLMESH_ROOT / "models" / "prep" / "int_transactions__merged.sql"
        content = model_path.read_text()
        assert "seed_source_priority" in content
        assert "GROUP BY" in content
        assert "transaction_id" in content
        assert "canonical_source_type" in content
        assert "source_count" in content
        assert "FIRST(" in content
        ordered_conversion_fields = (
            "{'conversion_source_transaction_id': m.source_transaction_id, "
            "'conversion_from_currency': m.currency_code, "
            "'to_currency': m.to_currency, "
            "'conversion_source_type': m.source_type, "
            "'conversion_source_origin': m.source_origin, "
            "'conversion_from_amount': m.amount, "
            "'to_amount': m.to_amount, "
            "'conversion_from_date': m.transaction_date}"
        )
        assert ordered_conversion_fields in content
        assert content.count("prep.int_transactions__matched") == 1
        assert "conversion_source_type" in content
        assert "conversion_source_origin" in content
        assert "conversion_source_transaction_id" in content
        assert (
            "MIN(m.transaction_date)" in content or "MIN(transaction_date)" in content
        )

    def test_model_is_view(self) -> None:
        model_path = SQLMESH_ROOT / "models" / "prep" / "int_transactions__merged.sql"
        content = model_path.read_text()
        assert "kind VIEW" in content
