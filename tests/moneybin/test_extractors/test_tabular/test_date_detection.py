"""Tests for date format detection and number format detection."""

from decimal import Decimal

from moneybin.extractors.tabular.date_detection import (
    detect_date_format,
    detect_number_format,
    format_parses,
    parse_amount_str,
)


class TestFormatParses:
    """Tests for validating a caller-supplied date format against real values."""

    def test_accepts_a_format_the_detector_does_not_carry(self) -> None:
        """%Y%m%d is absent from _DATE_FORMATS but is a real bank date format."""
        values: list[str | None] = ["20260105", "20260212", "20260331"]
        assert format_parses(values, "%Y%m%d") is True

    def test_rejects_a_format_that_cannot_read_the_column(self) -> None:
        values: list[str | None] = ["20260105", "20260212", "20260331"]
        assert format_parses(values, "%d/%m/%Y") is False

    def test_a_dirty_column_still_clears_the_override_bar(self) -> None:
        """An explicit override answers a different question than detection.

        The detector's 0.9 is a confidence bar for guessing unasked. An
        override is the caller asserting the format, and the transform imports
        valid rows while counting the rest in rows_rejected — which `import
        status` shows. Holding the override to 0.9 made a file with 8 good
        dates in 10 unimportable by either route: detection refused it for
        falling short, and the documented --date-format recovery refused it
        by the same number.
        """
        # 8 of 10 — under the detector's bar, comfortably over a majority.
        assert format_parses(["20260105"] * 8 + ["not-a-date"] * 2, "%Y%m%d") is True

    def test_a_format_reading_a_minority_is_still_refused(self) -> None:
        """Below a majority the format is likelier wrong than the data dirty."""
        assert format_parses(["20260105"] * 4 + ["not-a-date"] * 6, "%Y%m%d") is False
        # Exactly half clears: the bar is inclusive.
        assert format_parses(["20260105"] * 5 + ["not-a-date"] * 5, "%Y%m%d") is True

    def test_no_values_to_check_is_not_a_pass(self) -> None:
        """Fail closed: an unverifiable override must not clear the gate."""
        assert format_parses([], "%Y%m%d") is False
        assert format_parses([None, "", "   "], "%Y%m%d") is False

    def test_a_format_that_will_not_compile_is_refused_not_raised(self) -> None:
        """A repeated directive raises re.error, which is not a ValueError.

        strptime builds a regex from the format, so "%Y %Y" fails as a
        duplicate group name. Catching only ValueError let a malformed caller
        format escape validation and reach the user as an internal traceback
        instead of IMPORT_INVALID_DATE_FORMAT.
        """
        assert format_parses(["2026 2026"], "%Y %Y") is False
        assert format_parses(["01/02/2026"], "%d/%d/%Y") is False


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

    def test_unreadable_values_name_no_format(self) -> None:
        """The detector must say "no format", never guess one.

        A fabricated `"%Y-%m-%d"` here is what let a column of unparseable
        dates reject every row and report the import as a success.
        """
        values: list[str | None] = ["foo", "bar", "baz"]
        assert detect_date_format(values) == (None, "low")


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
        assert parse_amount_str("1,234.56", "us") == Decimal("1234.56")

    def test_european_basic(self) -> None:
        assert parse_amount_str("1.234,56", "european") == Decimal("1234.56")

    def test_swiss_french_basic(self) -> None:
        assert parse_amount_str("1 234,56", "swiss_french") == Decimal("1234.56")

    def test_zero_decimal(self) -> None:
        assert parse_amount_str("1,234", "zero_decimal") == Decimal("1234")

    def test_currency_symbol_stripped(self) -> None:
        assert parse_amount_str("$1,234.56", "us") == Decimal("1234.56")
        assert parse_amount_str("€1.234,56", "european") == Decimal("1234.56")
        assert parse_amount_str("¥1,234", "zero_decimal") == Decimal("1234")

    def test_parentheses_as_negative(self) -> None:
        assert parse_amount_str("(42.50)", "us") == Decimal("-42.50")

    def test_dr_suffix(self) -> None:
        assert parse_amount_str("42.50 DR", "us") == Decimal("-42.50")

    def test_cr_suffix(self) -> None:
        assert parse_amount_str("42.50 CR", "us") == Decimal("42.50")

    def test_negative_sign(self) -> None:
        assert parse_amount_str("-42.50", "us") == Decimal("-42.50")

    def test_empty_returns_none(self) -> None:
        assert parse_amount_str("", "us") is None
        assert parse_amount_str("  ", "us") is None
