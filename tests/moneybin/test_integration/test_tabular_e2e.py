"""End-to-end pipeline tests for the tabular import system.

These tests exercise the full detect → read → map → transform pipeline
using real fixture files. The load stage uses a mock database.
"""

from decimal import Decimal
from pathlib import Path

FIXTURES = Path(__file__).parents[2] / "fixtures" / "tabular"


class TestCSVImportPipeline:
    """Test the full pipeline with standard CSV files."""

    def test_standard_csv_pipeline(self) -> None:
        """Standard CSV → detect → read → map → transform produces valid output."""
        from moneybin.extractors.tabular.column_mapper import map_columns
        from moneybin.extractors.tabular.format_detector import detect_format
        from moneybin.extractors.tabular.readers import read_file
        from moneybin.extractors.tabular.transforms import transform_dataframe

        path = FIXTURES / "standard.csv"

        # Stage 1: Detect
        format_info = detect_format(path)
        assert format_info.file_type == "csv"

        # Stage 2: Read
        read_result = read_file(path, format_info)
        assert len(read_result.df) >= 5

        # Stage 3: Map
        mapping = map_columns(read_result.df)
        assert "transaction_date" in mapping.field_mapping
        assert "amount" in mapping.field_mapping
        assert "description" in mapping.field_mapping

        # Stage 4: Transform
        result = transform_dataframe(
            df=read_result.df,
            field_mapping=mapping.field_mapping,
            date_format=mapping.date_format or "%Y-%m-%d",
            sign_convention=mapping.sign_convention,
            number_format=mapping.number_format,
            account_id="test-acct",
            source_file=str(path),
            source_type="csv",
            source_origin="test",
            import_id="test-import-123",
        )
        assert len(result.transactions) > 0
        # Verify required columns exist
        assert "transaction_id" in result.transactions.columns
        assert "amount" in result.transactions.columns
        assert "transaction_date" in result.transactions.columns
        assert "description" in result.transactions.columns
        assert "account_id" in result.transactions.columns
        # All rows should reference the correct account
        account_ids = result.transactions["account_id"].to_list()
        assert all(a == "test-acct" for a in account_ids)

    def test_standard_csv_amounts_sign_preserved(self) -> None:
        """Expenses are negative, income is positive in transformed output."""
        from moneybin.extractors.tabular.column_mapper import map_columns
        from moneybin.extractors.tabular.format_detector import detect_format
        from moneybin.extractors.tabular.readers import read_file
        from moneybin.extractors.tabular.transforms import transform_dataframe

        path = FIXTURES / "standard.csv"
        format_info = detect_format(path)
        read_result = read_file(path, format_info)
        mapping = map_columns(read_result.df)
        result = transform_dataframe(
            df=read_result.df,
            field_mapping=mapping.field_mapping,
            date_format=mapping.date_format or "%Y-%m-%d",
            sign_convention=mapping.sign_convention,
            number_format=mapping.number_format,
            account_id="test-acct",
            source_file=str(path),
            source_type="csv",
            source_origin="test",
            import_id="test-import-sign",
        )
        amounts = result.transactions["amount"].to_list()
        # Standard CSV has a mix of negative (expenses) and positive (income)
        assert any(a < 0 for a in amounts), "Expected at least one expense (negative)"
        assert any(a > 0 for a in amounts), "Expected at least one income (positive)"

    def test_standard_csv_transaction_ids_unique(self) -> None:
        """Every row gets a unique transaction ID."""
        from moneybin.extractors.tabular.column_mapper import map_columns
        from moneybin.extractors.tabular.format_detector import detect_format
        from moneybin.extractors.tabular.readers import read_file
        from moneybin.extractors.tabular.transforms import transform_dataframe

        path = FIXTURES / "standard.csv"
        format_info = detect_format(path)
        read_result = read_file(path, format_info)
        mapping = map_columns(read_result.df)
        result = transform_dataframe(
            df=read_result.df,
            field_mapping=mapping.field_mapping,
            date_format=mapping.date_format or "%Y-%m-%d",
            sign_convention=mapping.sign_convention,
            number_format=mapping.number_format,
            account_id="test-acct",
            source_file=str(path),
            source_type="csv",
            source_origin="test",
            import_id="test-import-ids",
        )
        ids = result.transactions["transaction_id"].to_list()
        assert len(ids) == len(set(ids)), "Transaction IDs must be unique"

    def test_chase_credit_format_match(self) -> None:
        """Chase credit CSV matches built-in format and transforms correctly."""
        from moneybin.extractors.tabular.format_detector import detect_format
        from moneybin.extractors.tabular.formats import load_builtin_formats
        from moneybin.extractors.tabular.readers import read_file
        from moneybin.extractors.tabular.transforms import transform_dataframe

        path = FIXTURES / "chase_credit.csv"
        format_info = detect_format(path)
        read_result = read_file(path, format_info)

        # Should match chase_credit format
        builtin = load_builtin_formats()
        matched = None
        headers = list(read_result.df.columns)
        for fmt in builtin.values():
            if fmt.matches_headers(headers):
                matched = fmt
                break

        assert matched is not None
        assert matched.name == "chase_credit"

        result = transform_dataframe(
            df=read_result.df,
            field_mapping=matched.field_mapping,
            date_format=matched.date_format,
            sign_convention=matched.sign_convention,
            number_format=matched.number_format,
            account_id="chase-checking",
            source_file=str(path),
            source_type="csv",
            source_origin="chase_credit",
            import_id="test-chase-123",
        )
        assert len(result.transactions) > 0
        assert result.rows_rejected == 0

    def test_citi_credit_split_debit_credit(self) -> None:
        """Citi credit CSV with split Debit/Credit columns transforms correctly."""
        from moneybin.extractors.tabular.format_detector import detect_format
        from moneybin.extractors.tabular.formats import load_builtin_formats
        from moneybin.extractors.tabular.readers import read_file
        from moneybin.extractors.tabular.transforms import transform_dataframe

        path = FIXTURES / "citi_credit.csv"
        format_info = detect_format(path)
        read_result = read_file(path, format_info)

        builtin = load_builtin_formats()
        headers = list(read_result.df.columns)
        matched = None
        for fmt in builtin.values():
            if fmt.matches_headers(headers):
                matched = fmt
                break

        assert matched is not None
        assert matched.name == "citi_credit"
        assert matched.sign_convention == "split_debit_credit"

        result = transform_dataframe(
            df=read_result.df,
            field_mapping=matched.field_mapping,
            date_format=matched.date_format,
            sign_convention=matched.sign_convention,
            number_format=matched.number_format,
            account_id="citi-card",
            source_file=str(path),
            source_type="csv",
            source_origin="citi_credit",
            import_id="test-citi-123",
        )
        assert len(result.transactions) > 0
        amounts = result.transactions["amount"].to_list()
        # Debits should become negative, credits positive
        assert any(a < 0 for a in amounts), "Debit rows should produce negative amounts"
        assert any(a > 0 for a in amounts), (
            "Credit rows should produce positive amounts"
        )

    def test_tiller_format_with_source_transaction_id(self) -> None:
        """Tiller CSV uses source-provided transaction IDs instead of content hashes."""
        from moneybin.extractors.tabular.format_detector import detect_format
        from moneybin.extractors.tabular.formats import load_builtin_formats
        from moneybin.extractors.tabular.readers import read_file
        from moneybin.extractors.tabular.transforms import transform_dataframe

        path = FIXTURES / "tiller.csv"
        format_info = detect_format(path)
        read_result = read_file(path, format_info)

        builtin = load_builtin_formats()
        headers = list(read_result.df.columns)
        matched = None
        for fmt in builtin.values():
            if fmt.matches_headers(headers):
                matched = fmt
                break

        assert matched is not None
        assert matched.name == "tiller"

        result = transform_dataframe(
            df=read_result.df,
            field_mapping=matched.field_mapping,
            date_format=matched.date_format,
            sign_convention=matched.sign_convention,
            number_format=matched.number_format,
            account_id="tiller-acct",
            source_file=str(path),
            source_type="csv",
            source_origin="tiller",
            import_id="test-tiller-123",
        )
        assert len(result.transactions) > 0
        # Tiller provides source transaction IDs — IDs should use "acct_id:source_id" format
        ids = result.transactions["transaction_id"].to_list()
        assert all(":" in txn_id for txn_id in ids), (
            "Tiller IDs should use source-provided format (account_id:source_txn_id)"
        )

    def test_tsv_pipeline(self) -> None:
        """TSV file detected and read correctly."""
        from moneybin.extractors.tabular.format_detector import detect_format
        from moneybin.extractors.tabular.readers import read_file

        path = FIXTURES / "standard.tsv"
        format_info = detect_format(path)
        assert format_info.file_type == "tsv"

        read_result = read_file(path, format_info)
        assert len(read_result.df) >= 5
        assert "Date" in read_result.df.columns

    def test_pipe_delimited_detection(self) -> None:
        """Pipe-delimited .txt file is detected as pipe format."""
        from moneybin.extractors.tabular.format_detector import detect_format
        from moneybin.extractors.tabular.readers import read_file

        path = FIXTURES / "pipe_delimited.txt"
        format_info = detect_format(path)
        assert format_info.file_type == "pipe"
        assert format_info.delimiter == "|"

        read_result = read_file(path, format_info)
        assert len(read_result.df) >= 5
        assert "Date" in read_result.df.columns


class TestEdgeCases:
    """Tests for edge-case file fixtures."""

    def test_preamble_rows_handled(self) -> None:
        """Files with preamble rows before the header are handled."""
        from moneybin.extractors.tabular.format_detector import detect_format
        from moneybin.extractors.tabular.readers import read_file

        path = FIXTURES / "preamble_rows.csv"
        format_info = detect_format(path)
        read_result = read_file(path, format_info)
        # Should have found the data rows (not the preamble)
        assert len(read_result.df) >= 3
        assert read_result.skip_rows > 0
        # Proper header columns should be detected
        assert "Date" in read_result.df.columns
        assert "Amount" in read_result.df.columns

    def test_trailing_totals_removed(self) -> None:
        """Trailing total rows are stripped from the DataFrame."""
        from moneybin.extractors.tabular.format_detector import detect_format
        from moneybin.extractors.tabular.readers import read_file

        path = FIXTURES / "trailing_totals.csv"
        format_info = detect_format(path)
        read_result = read_file(path, format_info)
        # Total row should be removed
        assert read_result.rows_skipped_trailing > 0
        # Verify no row starts with "Total" in the first column
        first_col = read_result.df.columns[0]
        values = read_result.df[first_col].cast(str).to_list()
        assert not any(str(v).strip().lower().startswith("total") for v in values if v)

    def test_dd_mm_date_format_detected(self) -> None:
        """DD/MM/YYYY date format is correctly identified."""
        from moneybin.extractors.tabular.column_mapper import map_columns
        from moneybin.extractors.tabular.format_detector import detect_format
        from moneybin.extractors.tabular.readers import read_file

        path = FIXTURES / "dd_mm_dates.csv"
        format_info = detect_format(path)
        read_result = read_file(path, format_info)
        mapping = map_columns(read_result.df)

        assert mapping.date_format == "%d/%m/%Y", (
            f"Expected DD/MM/YYYY format, got {mapping.date_format!r}"
        )

    def test_dd_mm_full_pipeline(self) -> None:
        """DD/MM/YYYY file processes through the full pipeline correctly."""
        from moneybin.extractors.tabular.column_mapper import map_columns
        from moneybin.extractors.tabular.format_detector import detect_format
        from moneybin.extractors.tabular.readers import read_file
        from moneybin.extractors.tabular.transforms import transform_dataframe

        path = FIXTURES / "dd_mm_dates.csv"
        format_info = detect_format(path)
        read_result = read_file(path, format_info)
        mapping = map_columns(read_result.df)

        result = transform_dataframe(
            df=read_result.df,
            field_mapping=mapping.field_mapping,
            date_format=mapping.date_format or "%d/%m/%Y",
            sign_convention=mapping.sign_convention,
            number_format=mapping.number_format,
            account_id="test-acct",
            source_file=str(path),
            source_type="csv",
            source_origin="test",
            import_id="test-ddmm-123",
        )
        assert len(result.transactions) > 0
        assert result.rows_rejected == 0
        # Dates should parse into January 2026 (day values are 15-25)
        dates = result.transactions["transaction_date"].to_list()
        assert all(d.month == 1 for d in dates), "All dates should be in January 2026"
        assert all(d.year == 2026 for d in dates), "All dates should be in 2026"
        # Day values should match the DD part (15, 16, 18, 19, 20, 22, 25)
        days = sorted(d.day for d in dates)
        assert days == [15, 16, 18, 19, 20, 22, 25]

    def test_european_number_format_detected(self) -> None:
        """European number format (1.234,56) is correctly detected."""
        from moneybin.extractors.tabular.column_mapper import map_columns
        from moneybin.extractors.tabular.format_detector import detect_format
        from moneybin.extractors.tabular.readers import read_file

        path = FIXTURES / "european_amounts.csv"
        format_info = detect_format(path)
        read_result = read_file(path, format_info)
        mapping = map_columns(read_result.df)

        assert mapping.number_format == "european", (
            f"Expected 'european' number format, got {mapping.number_format!r}"
        )

    def test_european_amounts_parse_correctly(self) -> None:
        """European amounts (comma decimal, period thousands) parse to correct values."""
        from moneybin.extractors.tabular.column_mapper import map_columns
        from moneybin.extractors.tabular.format_detector import detect_format
        from moneybin.extractors.tabular.readers import read_file
        from moneybin.extractors.tabular.transforms import transform_dataframe

        path = FIXTURES / "european_amounts.csv"
        format_info = detect_format(path)
        read_result = read_file(path, format_info)
        mapping = map_columns(read_result.df)

        result = transform_dataframe(
            df=read_result.df,
            field_mapping=mapping.field_mapping,
            date_format=mapping.date_format or "%d.%m.%Y",
            sign_convention=mapping.sign_convention,
            number_format=mapping.number_format,
            account_id="test-acct",
            source_file=str(path),
            source_type="csv",
            source_origin="test",
            import_id="test-eu-123",
        )
        assert len(result.transactions) > 0
        amounts = result.transactions["amount"].to_list()
        # Grocery purchase should be -52.30, not -5230.0
        assert Decimal("-52.30") in amounts, (
            f"Expected -52.30 in amounts, got {amounts}"
        )
        # Payroll should be 2500.00
        assert Decimal("2500.00") in amounts, (
            f"Expected 2500.00 in amounts, got {amounts}"
        )


class TestMappingConfidence:
    """Tests that mapping confidence tiers are assigned correctly."""

    def test_standard_csv_high_confidence(self) -> None:
        """Standard CSV with clear headers produces high confidence mapping."""
        from moneybin.extractors.tabular.column_mapper import map_columns
        from moneybin.extractors.tabular.format_detector import detect_format
        from moneybin.extractors.tabular.readers import read_file

        path = FIXTURES / "standard.csv"
        format_info = detect_format(path)
        read_result = read_file(path, format_info)
        mapping = map_columns(read_result.df)

        assert mapping.confidence == "high", (
            f"Expected high confidence for standard CSV, got {mapping.confidence!r}"
        )

    def test_all_required_fields_mapped_for_chase(self) -> None:
        """Chase credit format maps all three required fields."""
        from moneybin.extractors.tabular.format_detector import detect_format
        from moneybin.extractors.tabular.formats import load_builtin_formats
        from moneybin.extractors.tabular.readers import read_file

        path = FIXTURES / "chase_credit.csv"
        format_info = detect_format(path)
        read_result = read_file(path, format_info)

        builtin = load_builtin_formats()
        headers = list(read_result.df.columns)
        matched = next(
            (fmt for fmt in builtin.values() if fmt.matches_headers(headers)), None
        )
        assert matched is not None

        # Built-in format must cover all three required fields
        assert "transaction_date" in matched.field_mapping
        assert "description" in matched.field_mapping
        assert "amount" in matched.field_mapping

    def test_tiller_is_multi_account(self) -> None:
        """Tiller format is correctly identified as multi-account."""
        from moneybin.extractors.tabular.format_detector import detect_format
        from moneybin.extractors.tabular.formats import load_builtin_formats
        from moneybin.extractors.tabular.readers import read_file

        path = FIXTURES / "tiller.csv"
        format_info = detect_format(path)
        read_result = read_file(path, format_info)

        builtin = load_builtin_formats()
        headers = list(read_result.df.columns)
        matched = next(
            (fmt for fmt in builtin.values() if fmt.matches_headers(headers)), None
        )
        assert matched is not None
        assert matched.multi_account is True


class TestIdempotency:
    """Tests that re-importing the same file produces identical transaction IDs."""

    def test_reimport_produces_same_ids(self) -> None:
        """Importing standard.csv twice yields identical transaction IDs."""
        from moneybin.extractors.tabular.column_mapper import map_columns
        from moneybin.extractors.tabular.format_detector import detect_format
        from moneybin.extractors.tabular.readers import read_file
        from moneybin.extractors.tabular.transforms import transform_dataframe

        path = FIXTURES / "standard.csv"

        def _run_pipeline() -> list[str]:
            format_info = detect_format(path)
            read_result = read_file(path, format_info)
            mapping = map_columns(read_result.df)
            result = transform_dataframe(
                df=read_result.df,
                field_mapping=mapping.field_mapping,
                date_format=mapping.date_format or "%Y-%m-%d",
                sign_convention=mapping.sign_convention,
                number_format=mapping.number_format,
                account_id="test-acct",
                source_file=str(path),
                source_type="csv",
                source_origin="test",
                import_id="stable-import-id",
            )
            return result.transactions["transaction_id"].to_list()

        ids_first = _run_pipeline()
        ids_second = _run_pipeline()
        assert ids_first == ids_second, "Re-importing same file must produce same IDs"

    def test_chase_reimport_produces_same_ids(self) -> None:
        """Re-importing chase_credit.csv yields identical transaction IDs."""
        from moneybin.extractors.tabular.format_detector import detect_format
        from moneybin.extractors.tabular.formats import load_builtin_formats
        from moneybin.extractors.tabular.readers import read_file
        from moneybin.extractors.tabular.transforms import transform_dataframe

        path = FIXTURES / "chase_credit.csv"

        def _run_pipeline() -> list[str]:
            format_info = detect_format(path)
            read_result = read_file(path, format_info)
            builtin = load_builtin_formats()
            headers = list(read_result.df.columns)
            matched = next(
                (fmt for fmt in builtin.values() if fmt.matches_headers(headers)), None
            )
            assert matched is not None
            result = transform_dataframe(
                df=read_result.df,
                field_mapping=matched.field_mapping,
                date_format=matched.date_format,
                sign_convention=matched.sign_convention,
                number_format=matched.number_format,
                account_id="chase-checking",
                source_file=str(path),
                source_type="csv",
                source_origin="chase_credit",
                import_id="stable-chase-id",
            )
            return result.transactions["transaction_id"].to_list()

        ids_first = _run_pipeline()
        ids_second = _run_pipeline()
        assert ids_first == ids_second, "Re-importing chase CSV must produce same IDs"
