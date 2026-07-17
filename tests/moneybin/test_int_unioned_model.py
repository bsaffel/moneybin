"""Tests for int_transactions__unioned model structure."""

from moneybin.database import SQLMESH_ROOT


class TestIntTransactionsUnionedModel:
    """Structural tests for the int_transactions__unioned SQLMesh model."""

    def test_model_file_exists(self) -> None:
        model_path = SQLMESH_ROOT / "models" / "prep" / "int_transactions__unioned.sql"
        assert model_path.exists()

    def test_model_has_required_columns(self) -> None:
        model_path = SQLMESH_ROOT / "models" / "prep" / "int_transactions__unioned.sql"
        content = model_path.read_text()
        assert "source_transaction_id" in content
        assert "source_type" in content
        assert "source_origin" in content
        assert "account_id" in content
        assert "source_account_key" in content
        assert "transaction_date" in content
        assert "amount" in content
        assert "description" in content
        assert "UNION ALL" in content
        assert "currency_code" in content

    def test_model_is_view(self) -> None:
        model_path = SQLMESH_ROOT / "models" / "prep" / "int_transactions__unioned.sql"
        content = model_path.read_text()
        assert "kind VIEW" in content

    def test_model_never_defaults_currency_to_usd(self) -> None:
        """Regression guard: currency_code must never default/COALESCE to 'USD'.

        All four union arms must pass through whatever currency was captured
        and leave it NULL when unknown, per Requirement 2 of multi-currency.md.
        """
        model_path = SQLMESH_ROOT / "models" / "prep" / "int_transactions__unioned.sql"
        content = model_path.read_text()
        assert "'USD' AS currency_code" not in content
        assert "COALESCE(currency_code, 'USD')" not in content
        assert "COALESCE(currency, 'USD')" not in content
        assert "COALESCE(iso_currency_code, 'USD')" not in content
