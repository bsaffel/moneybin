"""Tests for SanitizedLogFormatter — PII pattern detection and masking."""

import logging
from collections.abc import Callable

import pytest

from moneybin.log_sanitizer import SanitizedLogFormatter


@pytest.fixture()
def formatter() -> SanitizedLogFormatter:
    """Create a SanitizedLogFormatter for testing."""
    return SanitizedLogFormatter("%(message)s")


@pytest.fixture()
def make_record() -> Callable[[str], logging.LogRecord]:
    """Factory for log records."""
    logger = logging.getLogger("test.sanitizer")

    def _make(msg: str, level: int = logging.INFO) -> logging.LogRecord:
        return logger.makeRecord("test.sanitizer", level, "test.py", 1, msg, (), None)

    return _make


class TestSSNMasking:
    """Test SSN pattern detection and masking."""

    def test_masks_ssn_pattern(
        self,
        formatter: SanitizedLogFormatter,
        make_record: Callable[[str], logging.LogRecord],
    ) -> None:
        record = make_record("User SSN is 123-45-6789")
        result = formatter.format(record)
        assert "123-45-6789" not in result
        assert "***-**-****" in result

    def test_does_not_mask_non_ssn_dashes(
        self,
        formatter: SanitizedLogFormatter,
        make_record: Callable[[str], logging.LogRecord],
    ) -> None:
        record = make_record("Date is 2026-04-20")
        result = formatter.format(record)
        assert "2026-04-20" in result


class TestAccountNumberMasking:
    """Test account number pattern detection and masking."""

    def test_masks_long_digit_sequence(
        self,
        formatter: SanitizedLogFormatter,
        make_record: Callable[[str], logging.LogRecord],
    ) -> None:
        record = make_record("Account 12345678901234")
        result = formatter.format(record)
        assert "12345678901234" not in result
        assert "****...1234" in result

    def test_does_not_mask_short_numbers(
        self,
        formatter: SanitizedLogFormatter,
        make_record: Callable[[str], logging.LogRecord],
    ) -> None:
        record = make_record("Loaded 142 transactions")
        result = formatter.format(record)
        assert "142" in result


class TestDollarAmountMasking:
    """Test dollar amount pattern detection and masking."""

    def test_masks_dollar_amount(
        self,
        formatter: SanitizedLogFormatter,
        make_record: Callable[[str], logging.LogRecord],
    ) -> None:
        record = make_record("Balance is $1,234.56")
        result = formatter.format(record)
        assert "$1,234.56" not in result
        assert "$***" in result

    def test_masks_simple_dollar(
        self,
        formatter: SanitizedLogFormatter,
        make_record: Callable[[str], logging.LogRecord],
    ) -> None:
        record = make_record("Amount: $500.00")
        result = formatter.format(record)
        assert "$500.00" not in result
        assert "$***" in result


class TestCleanPassthrough:
    """Test that clean logs pass through unchanged."""

    def test_clean_log_passes_unchanged(
        self,
        formatter: SanitizedLogFormatter,
        make_record: Callable[[str], logging.LogRecord],
    ) -> None:
        msg = "Loaded 142 transactions for account_id abc-123"
        record = make_record(msg)
        result = formatter.format(record)
        assert result == msg

    def test_record_counts_pass(
        self,
        formatter: SanitizedLogFormatter,
        make_record: Callable[[str], logging.LogRecord],
    ) -> None:
        msg = "Processed 50 records in 2.3 seconds"
        record = make_record(msg)
        result = formatter.format(record)
        assert result == msg


class TestDebugOnMask:
    """Test that masking emits a debug audit record."""

    def test_emits_debug_when_masking(
        self,
        formatter: SanitizedLogFormatter,
        make_record: Callable[[str], logging.LogRecord],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """When masking occurs, a DEBUG record is emitted identifying the source.

        DEBUG (not WARNING) so the audit trail stays in file logs without
        cluttering the user's console at default verbosity.
        """
        record = make_record("SSN: 123-45-6789")
        with caplog.at_level(logging.DEBUG, logger="moneybin.log_sanitizer"):
            formatter.format(record)
        assert any("PII pattern detected" in r.message for r in caplog.records)
