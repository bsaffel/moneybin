# tests/moneybin/test_mcp/test_decorator.py
"""Tests for MCP tool decorator and sensitivity middleware."""

import pytest

from moneybin.mcp.privacy import Sensitivity, log_tool_call


class TestSensitivity:
    """Tests for the Sensitivity enum."""

    @pytest.mark.unit
    def test_values(self) -> None:
        assert Sensitivity.LOW == "low"
        assert Sensitivity.MEDIUM == "medium"
        assert Sensitivity.HIGH == "high"

    @pytest.mark.unit
    def test_ordering(self) -> None:
        # Sensitivity levels should be orderable for middleware checks
        tiers = [Sensitivity.LOW, Sensitivity.MEDIUM, Sensitivity.HIGH]
        assert tiers == sorted(tiers, key=lambda s: list(Sensitivity).index(s))


class TestLogToolCall:
    """Tests for the tool call logging stub."""

    @pytest.mark.unit
    def test_log_tool_call_returns_none(self, caplog: pytest.LogCaptureFixture) -> None:
        """log_tool_call is a stub — it logs but doesn't block."""
        with caplog.at_level("DEBUG"):
            result = log_tool_call("spending.summary", Sensitivity.LOW)
        assert result is None

    @pytest.mark.unit
    def test_log_tool_call_logs_sensitivity(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level("DEBUG"):
            log_tool_call("transactions.search", Sensitivity.MEDIUM)
        assert "transactions.search" in caplog.text
        assert "medium" in caplog.text
