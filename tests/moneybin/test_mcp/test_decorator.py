# tests/moneybin/test_mcp/test_decorator.py
"""Tests for MCP tool decorator and sensitivity middleware."""

import asyncio
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
            result = log_tool_call("reports_spending_summary", Sensitivity.LOW)
        assert result is None

    @pytest.mark.unit
    def test_log_tool_call_logs_sensitivity(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level("DEBUG"):
            log_tool_call("transactions_search", Sensitivity.MEDIUM)
        assert "transactions_search" in caplog.text
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
        def reports_spending_summary() -> str:
            return "data"

        assert reports_spending_summary.__name__ == "reports_spending_summary"

    @pytest.mark.unit
    def test_decorator_calls_log_tool_call(self) -> None:

        @mcp_tool(sensitivity="medium")
        def my_tool() -> ResponseEnvelope:
            return ResponseEnvelope(
                summary=SummaryMeta(total_count=0, returned_count=0),
                data=[],
            )

        with patch("moneybin.mcp.decorator.log_tool_call") as mock_log:
            asyncio.run(my_tool())
            mock_log.assert_called_once()
            args = mock_log.call_args[0]
            assert args[0] == "my_tool"
            assert args[1] == Sensitivity.MEDIUM

    @pytest.mark.unit
    def test_decorator_supports_domain(self) -> None:
        """The mcp_tool decorator carries the domain string as an attribute."""

        @mcp_tool(sensitivity="medium", domain="categorize")
        def example_tool() -> ResponseEnvelope:  # type: ignore[return]
            ...

        assert example_tool._mcp_domain == "categorize"  # type: ignore[attr-defined]

    @pytest.mark.unit
    def test_decorator_default_domain_is_none(self) -> None:
        """Tools without an explicit domain are core tools (always visible)."""

        @mcp_tool(sensitivity="low")
        def example_tool() -> ResponseEnvelope:  # type: ignore[return]
            ...

        assert example_tool._mcp_domain is None  # type: ignore[attr-defined]

    @pytest.mark.unit
    def test_decorator_returns_response_envelope(self) -> None:
        """When a tool returns a ResponseEnvelope, the decorator returns it directly."""

        @mcp_tool(sensitivity="low")
        def my_tool() -> ResponseEnvelope:
            return ResponseEnvelope(
                summary=SummaryMeta(total_count=1, returned_count=1),
                data=[{"value": 42}],
            )

        result = asyncio.run(my_tool())
        assert isinstance(result, ResponseEnvelope)
        assert result.summary.total_count == 1
        assert result.data == [{"value": 42}]

    @pytest.mark.unit
    def test_decorator_raises_type_error_for_non_envelope(self) -> None:
        """Tools that return non-ResponseEnvelope raise TypeError."""
        import pytest

        @mcp_tool(sensitivity="low")
        def my_tool() -> str:  # type: ignore[return]
            return "plain string result"  # type: ignore[return-value]

        with pytest.raises(TypeError, match="expected ResponseEnvelope"):
            asyncio.run(my_tool())
