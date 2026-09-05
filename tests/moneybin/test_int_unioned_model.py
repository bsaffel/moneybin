"""Tests for int_transactions__unioned model structure."""

from moneybin.database import SQLMESH_ROOT


def test_received_leg_strings_precede_numerics_across_prep_chain() -> None:
    """New received-leg fields follow prep's string-before-numeric order."""
    model_names = (
        "stg_manual__transactions.sql",
        "stg_ofx__transactions.sql",
        "stg_plaid__transactions.sql",
        "stg_tabular__transactions.sql",
        "int_transactions__unioned.sql",
        "int_transactions__matched.sql",
    )
    for model_name in model_names:
        lines = (SQLMESH_ROOT / "models" / "prep" / model_name).read_text().splitlines()
        currencies = [i for i, line in enumerate(lines) if "to_currency" in line]
        amounts = [i for i, line in enumerate(lines) if "to_amount" in line]
        assert len(currencies) == len(amounts)
        assert all(
            currency < amount
            for currency, amount in zip(currencies, amounts, strict=True)
        )


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
