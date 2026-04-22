"""Tests for Stage 4 transform and validation."""

from typing import TypedDict

import polars as pl

from moneybin.extractors.tabular.transforms import (
    transform_dataframe,
)

_SOURCE_FILE = "test.csv"  # noqa: S108  # not a real temp file path, just a test fixture value


def _make_df(**columns: list[str]) -> pl.DataFrame:
    return pl.DataFrame(columns)


class _BaseKwargs(TypedDict):
    field_mapping: dict[str, str]
    date_format: str
    sign_convention: str
    number_format: str
    account_id: str
    source_file: str
    source_type: str
    source_origin: str
    import_id: str


def _base_kwargs() -> _BaseKwargs:
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
        base = _base_kwargs()
        result = transform_dataframe(
            df=df,
            field_mapping=base["field_mapping"],
            date_format=base["date_format"],
            sign_convention="negative_is_income",
            number_format=base["number_format"],
            account_id=base["account_id"],
            source_file=base["source_file"],
            source_type=base["source_type"],
            source_origin=base["source_origin"],
            import_id=base["import_id"],
        )
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


class TestRunningBalanceValidation:
    """Tests for running balance validation in transforms."""

    def test_balance_validates_amounts(self) -> None:
        """Sequential balance deltas match amounts → balance_validated=True."""
        df = _make_df(
            Date=["01/15/2026", "01/16/2026", "01/17/2026"],
            Amount=["-42.50", "100.00", "-10.00"],
            Description=["A", "B", "C"],
            Balance=["957.50", "1057.50", "1047.50"],
        )
        result = transform_dataframe(
            df=df,
            field_mapping={
                "transaction_date": "Date",
                "amount": "Amount",
                "description": "Description",
                "balance": "Balance",
            },
            date_format="%m/%d/%Y",
            sign_convention="negative_is_expense",
            number_format="us",
            account_id="test",
            source_file="/tmp/test.csv",  # noqa: S108  # test fixture path, not a real temp file
            source_type="csv",
            source_origin="test",
            import_id="test-123",
        )
        assert result.balance_validated is True

    def test_balance_detects_wrong_sign(self) -> None:
        """Balance validates after sign inversion → auto-correct sign convention."""
        df = _make_df(
            Date=["01/15/2026", "01/16/2026"],
            Amount=["42.50", "-100.00"],
            Description=["A", "B"],
            Balance=["957.50", "1057.50"],
        )
        result = transform_dataframe(
            df=df,
            field_mapping={
                "transaction_date": "Date",
                "amount": "Amount",
                "description": "Description",
                "balance": "Balance",
            },
            date_format="%m/%d/%Y",
            sign_convention="negative_is_expense",
            number_format="us",
            account_id="test",
            source_file="/tmp/test.csv",  # noqa: S108  # test fixture path, not a real temp file
            source_type="csv",
            source_origin="test",
            import_id="test-123",
        )
        # Should auto-correct the sign and validate
        assert result.balance_validated is True

    def test_balance_inconsistent_warns(self) -> None:
        """Balance doesn't match in either direction → balance_validated=False."""
        df = _make_df(
            Date=["01/15/2026", "01/16/2026"],
            Amount=["-42.50", "100.00"],
            Description=["A", "B"],
            Balance=["500.00", "999.99"],
        )
        result = transform_dataframe(
            df=df,
            field_mapping={
                "transaction_date": "Date",
                "amount": "Amount",
                "description": "Description",
                "balance": "Balance",
            },
            date_format="%m/%d/%Y",
            sign_convention="negative_is_expense",
            number_format="us",
            account_id="test",
            source_file="/tmp/test.csv",  # noqa: S108  # test fixture path, not a real temp file
            source_type="csv",
            source_origin="test",
            import_id="test-123",
        )
        assert result.balance_validated is False
