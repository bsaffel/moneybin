"""Tests for log formatters."""

import json
import logging

import pytest

from moneybin.logging.formatters import HumanFormatter, JSONFormatter


class TestHumanFormatter:
    """Tests for HumanFormatter variants."""

    @pytest.mark.unit
    def test_cli_variant_message_only(self) -> None:
        """CLI variant should output only the message, no timestamp."""
        formatter = HumanFormatter(variant="cli")
        record = logging.LogRecord(
            name="moneybin.cli",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Hello world",
            args=(),
            exc_info=None,
        )
        result = formatter.format(record)
        assert result == "Hello world"

    @pytest.mark.unit
    def test_full_variant_includes_timestamp_and_name(self) -> None:
        """Full variant should include timestamp, logger name, and level."""
        formatter = HumanFormatter(variant="full")
        record = logging.LogRecord(
            name="moneybin.mcp",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Server started",
            args=(),
            exc_info=None,
        )
        result = formatter.format(record)
        assert "moneybin.mcp" in result
        assert "INFO" in result
        assert "Server started" in result


class TestJSONFormatter:
    """Tests for JSONFormatter."""

    @pytest.mark.unit
    def test_output_is_valid_json(self) -> None:
        """Each formatted line should be valid JSON."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="moneybin.import",
            level=logging.WARNING,
            pathname="",
            lineno=0,
            msg="Duplicate found",
            args=(),
            exc_info=None,
        )
        result = formatter.format(record)
        parsed = json.loads(result)
        assert parsed["message"] == "Duplicate found"
        assert parsed["level"] == "WARNING"
        assert parsed["logger"] == "moneybin.import"
        assert "timestamp" in parsed

    @pytest.mark.unit
    def test_extra_fields_included(self) -> None:
        """Extra dict fields on the record should appear in JSON output."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="moneybin.import",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Loaded records",
            args=(),
            exc_info=None,
        )
        record.source_type = "csv"  # type: ignore[attr-defined]  # intentional extra field for test
        result = formatter.format(record)
        parsed = json.loads(result)
        assert parsed["source_type"] == "csv"
