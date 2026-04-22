"""Tests for date format detection and number format detection."""

from moneybin.extractors.tabular.date_detection import (
    detect_date_format,
    detect_number_format,
    parse_amount_str,
)


class TestDetectDateFormat:
    """Tests for date format detection."""

    def test_us_format(self) -> None:
        values: list[str | None] = ["01/15/2026", "02/20/2026", "03/31/2026"]
        fmt, _confidence = detect_date_format(values)
        assert fmt == "%m/%d/%Y"

    def test_iso_format(self) -> None:
        values: list[str | None] = ["2026-01-15", "2026-02-20", "2026-03-31"]
        fmt, _confidence = detect_date_format(values)
        assert fmt == "%Y-%m-%d"

    def test_dd_mm_yyyy_with_day_over_12(self) -> None:
        values: list[str | None] = ["15/01/2026", "20/02/2026", "31/03/2026"]
        fmt, _confidence = detect_date_format(values)
        assert fmt == "%d/%m/%Y"

    def test_mm_dd_yyyy_with_day_over_12(self) -> None:
        values: list[str | None] = ["01/15/2026", "02/20/2026", "03/31/2026"]
        fmt, _confidence = detect_date_format(values)
        assert fmt == "%m/%d/%Y"

    def test_two_digit_year(self) -> None:
        values: list[str | None] = ["01/15/26", "02/20/26", "03/31/26"]
        fmt, _confidence = detect_date_format(values)
        assert fmt == "%m/%d/%y"

    def test_named_month(self) -> None:
        values: list[str | None] = ["15-Mar-2026", "20-Apr-2026", "31-May-2026"]
        fmt, _confidence = detect_date_format(values)
        assert fmt == "%d-%b-%Y"

    def test_long_month(self) -> None:
        values: list[str | None] = ["Mar 15, 2026", "Apr 20, 2026", "May 31, 2026"]
        fmt, _confidence = detect_date_format(values)
        assert fmt == "%b %d, %Y"

    def test_ambiguous_returns_medium_confidence(self) -> None:
        values: list[str | None] = ["01/02/2026", "03/04/2026", "05/06/2026"]
        _fmt, confidence = detect_date_format(values)
        assert confidence in ("medium", "high")

    def test_empty_values_handled(self) -> None:
        values: list[str | None] = ["", None, "01/15/2026", "", "02/20/2026"]
        fmt, _confidence = detect_date_format(values)
        assert fmt is not None


class TestDetectNumberFormat:
    """Tests for number format detection."""

    def test_us_format(self) -> None:
        values: list[str | None] = ["1,234.56", "42.50", "1,000.00"]
        assert detect_number_format(values) == "us"

    def test_european_format(self) -> None:
        values: list[str | None] = ["1.234,56", "42,50", "1.000,00"]
        assert detect_number_format(values) == "european"

    def test_swiss_french_format(self) -> None:
        values: list[str | None] = ["1 234,56", "42,50", "1 000,00"]
        assert detect_number_format(values) == "swiss_french"

    def test_zero_decimal(self) -> None:
        values: list[str | None] = ["1,234", "42", "1,000"]
        assert detect_number_format(values) == "zero_decimal"

    def test_plain_numbers_default_us(self) -> None:
        values: list[str | None] = ["42.50", "10.00", "100.25"]
        assert detect_number_format(values) == "us"


class TestParseAmountStr:
    """Tests for amount string parsing."""

    def test_us_basic(self) -> None:
        assert parse_amount_str("1,234.56", "us") == 1234.56

    def test_european_basic(self) -> None:
        assert parse_amount_str("1.234,56", "european") == 1234.56

    def test_swiss_french_basic(self) -> None:
        assert parse_amount_str("1 234,56", "swiss_french") == 1234.56

    def test_zero_decimal(self) -> None:
        assert parse_amount_str("1,234", "zero_decimal") == 1234.0

    def test_currency_symbol_stripped(self) -> None:
        assert parse_amount_str("$1,234.56", "us") == 1234.56
        assert parse_amount_str("€1.234,56", "european") == 1234.56
        assert parse_amount_str("¥1,234", "zero_decimal") == 1234.0

    def test_parentheses_as_negative(self) -> None:
        assert parse_amount_str("(42.50)", "us") == -42.50

    def test_dr_suffix(self) -> None:
        assert parse_amount_str("42.50 DR", "us") == -42.50

    def test_cr_suffix(self) -> None:
        assert parse_amount_str("42.50 CR", "us") == 42.50

    def test_negative_sign(self) -> None:
        assert parse_amount_str("-42.50", "us") == -42.50

    def test_empty_returns_none(self) -> None:
        assert parse_amount_str("", "us") is None
        assert parse_amount_str("  ", "us") is None
