# tests/moneybin/test_mcp/test_decorator.py
"""Tests for MCP tool decorator and sensitivity middleware."""

import json
from unittest.mock import patch

import pytest

from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.privacy import Sensitivity, log_tool_call
from moneybin.protocol.envelope import ResponseEnvelope, SummaryMeta


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


class TestMCPToolDecorator:
    """Tests for the @mcp_tool decorator."""

    @pytest.mark.unit
    def test_decorator_sets_sensitivity_attribute(self) -> None:
        @mcp_tool(sensitivity="low")
        def my_tool() -> str:
            return "result"

        assert my_tool._mcp_sensitivity == "low"  # type: ignore[attr-defined]

    @pytest.mark.unit
    def test_decorator_preserves_function_name(self) -> None:
        @mcp_tool(sensitivity="medium")
        def spending_summary() -> str:
            return "data"

        assert spending_summary.__name__ == "spending_summary"

    @pytest.mark.unit
    def test_decorator_calls_log_tool_call(self) -> None:
        @mcp_tool(sensitivity="medium")
        def my_tool() -> str:
            return "result"

        with patch("moneybin.mcp.decorator.log_tool_call") as mock_log:
            my_tool()
            mock_log.assert_called_once()
            args = mock_log.call_args[0]
            assert args[0] == "my_tool"
            assert args[1] == Sensitivity.MEDIUM

    @pytest.mark.unit
    def test_decorator_returns_json_when_envelope(self) -> None:
        """When a tool returns a ResponseEnvelope, decorator serializes to JSON."""

        @mcp_tool(sensitivity="low")
        def my_tool() -> ResponseEnvelope:
            return ResponseEnvelope(
                summary=SummaryMeta(total_count=1, returned_count=1),
                data=[{"value": 42}],
            )

        result = my_tool()
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert "summary" in parsed
        assert "data" in parsed

    @pytest.mark.unit
    def test_decorator_passes_through_string(self) -> None:
        """When a tool returns a plain string, decorator passes it through."""

        @mcp_tool(sensitivity="low")
        def my_tool() -> str:
            return "plain string result"

        result = my_tool()
        assert result == "plain string result"
