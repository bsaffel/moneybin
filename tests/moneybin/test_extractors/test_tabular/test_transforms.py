"""Tests for Stage 4 transform and validation."""

import polars as pl

from moneybin.extractors.tabular.transforms import (
    transform_dataframe,
)

_SOURCE_FILE = "test.csv"  # noqa: S108  # not a real temp file path, just a test fixture value


def _make_df(**columns: list[str]) -> pl.DataFrame:
    return pl.DataFrame(columns)


def _base_kwargs() -> dict:
    return {
        "field_mapping": {
            "transaction_date": "Date",
            "amount": "Amount",
            "description": "Description",
        },
        "date_format": "%m/%d/%Y",
        "sign_convention": "negative_is_expense",
        "number_format": "us",
        "account_id": "test",
        "source_file": _SOURCE_FILE,
        "source_type": "csv",
        "source_origin": "test",
        "import_id": "test-123",
    }


class TestTransformBasic:
    """Tests for basic transform operations."""

    def test_basic_transform(self) -> None:
        df = _make_df(
            Date=["01/15/2026", "02/20/2026"],
            Amount=["-42.50", "100.00"],
            Description=["KROGER #1234", "DIRECT DEPOSIT"],
        )
        result = transform_dataframe(
            df=df,
            field_mapping={
                "transaction_date": "Date",
                "amount": "Amount",
                "description": "Description",
            },
            date_format="%m/%d/%Y",
            sign_convention="negative_is_expense",
            number_format="us",
            account_id="test-checking",
            source_file=_SOURCE_FILE,
            source_type="csv",
            source_origin="test_bank",
            import_id="test-import-123",
        )
        assert len(result.transactions) == 2
        assert float(result.transactions["amount"][0]) == -42.50
        assert float(result.transactions["amount"][1]) == 100.00
        assert result.transactions["description"][0] == "KROGER #1234"

    def test_original_values_preserved(self) -> None:
        df = _make_df(
            Date=["01/15/2026"],
            Amount=["-42.50"],
            Description=["Test"],
        )
        result = transform_dataframe(df=df, **_base_kwargs())
        assert result.transactions["original_amount"][0] == "-42.50"
        assert result.transactions["original_date_str"][0] == "01/15/2026"

    def test_row_numbers_assigned(self) -> None:
        df = _make_df(
            Date=["01/15/2026", "01/16/2026", "01/17/2026"],
            Amount=["-10", "-20", "-30"],
            Description=["A", "B", "C"],
        )
        result = transform_dataframe(df=df, **_base_kwargs())
        assert result.transactions["row_number"].to_list() == [1, 2, 3]

    def test_transaction_id_deterministic(self) -> None:
        df = _make_df(
            Date=["01/15/2026"],
            Amount=["-42.50"],
            Description=["KROGER"],
        )
        kwargs = _base_kwargs()
        r1 = transform_dataframe(df=df, **kwargs)
        r2 = transform_dataframe(df=df, **kwargs)
        assert (
            r1.transactions["transaction_id"][0] == r2.transactions["transaction_id"][0]
        )

    def test_source_transaction_id_used_when_present(self) -> None:
        df = _make_df(
            Date=["01/15/2026"],
            Amount=["-42.50"],
            Description=["KROGER"],
            TxnID=["TXN90812"],
        )
        result = transform_dataframe(
            df=df,
            field_mapping={
                "transaction_date": "Date",
                "amount": "Amount",
                "description": "Description",
                "source_transaction_id": "TxnID",
            },
            date_format="%m/%d/%Y",
            sign_convention="negative_is_expense",
            number_format="us",
            account_id="test",
            source_file=_SOURCE_FILE,
            source_type="csv",
            source_origin="test",
            import_id="test-123",
        )
        assert result.transactions["transaction_id"][0] == "test:TXN90812"


class TestSignConventionTransform:
    """Tests for sign convention handling in transforms."""

    def test_negative_is_income_inverts(self) -> None:
        df = _make_df(
            Date=["01/15/2026"],
            Amount=["42.50"],
            Description=["PURCHASE"],
        )
        kwargs = {**_base_kwargs(), "sign_convention": "negative_is_income"}
        result = transform_dataframe(df=df, **kwargs)
        assert float(result.transactions["amount"][0]) == -42.50

    def test_split_debit_credit(self) -> None:
        df = _make_df(
            Date=["01/15/2026", "01/16/2026"],
            Debit=["42.50", ""],
            Credit=["", "100.00"],
            Description=["KROGER", "DEPOSIT"],
        )
        result = transform_dataframe(
            df=df,
            field_mapping={
                "transaction_date": "Date",
                "debit_amount": "Debit",
                "credit_amount": "Credit",
                "description": "Description",
            },
            date_format="%m/%d/%Y",
            sign_convention="split_debit_credit",
            number_format="us",
            account_id="test",
            source_file=_SOURCE_FILE,
            source_type="csv",
            source_origin="test",
            import_id="test-123",
        )
        assert float(result.transactions["amount"][0]) == -42.50
        assert float(result.transactions["amount"][1]) == 100.00
