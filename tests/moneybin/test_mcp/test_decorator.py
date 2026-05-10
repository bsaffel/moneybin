# tests/moneybin/test_mcp/test_decorator.py
"""Tests for MCP tool decorator and sensitivity middleware."""

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
    async def test_decorator_calls_log_tool_call(self) -> None:

        @mcp_tool(sensitivity="medium")
        def my_tool() -> ResponseEnvelope:
            return ResponseEnvelope(
                summary=SummaryMeta(total_count=0, returned_count=0),
                data=[],
            )

        with patch("moneybin.mcp.decorator.log_tool_call") as mock_log:
            await my_tool()
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
    async def test_decorator_returns_response_envelope(self) -> None:
        """When a tool returns a ResponseEnvelope, the decorator returns it directly."""

        @mcp_tool(sensitivity="low")
        def my_tool() -> ResponseEnvelope:
            return ResponseEnvelope(
                summary=SummaryMeta(total_count=1, returned_count=1),
                data=[{"value": 42}],
            )

        result = await my_tool()
        assert isinstance(result, ResponseEnvelope)
        assert result.summary.total_count == 1
        assert result.data == [{"value": 42}]

    @pytest.mark.unit
    async def test_decorator_raises_type_error_for_non_envelope(self) -> None:
        """Tools that return non-ResponseEnvelope raise TypeError."""
        import pytest

        @mcp_tool(sensitivity="low")
        def my_tool() -> str:  # type: ignore[return]
            return "plain string result"  # type: ignore[return-value]

        with pytest.raises(TypeError, match="expected ResponseEnvelope"):
            await my_tool()


@pytest.mark.unit
def test_mcp_tool_default_annotations() -> None:
    """Defaults: read_only=True, destructive=False, idempotent=True, open_world=False."""

    @mcp_tool(sensitivity="low")
    def example() -> ResponseEnvelope:  # type: ignore[return]
        ...

    assert example._mcp_read_only is True  # type: ignore[attr-defined]
    assert example._mcp_destructive is False  # type: ignore[attr-defined]
    assert example._mcp_idempotent is True  # type: ignore[attr-defined]
    assert example._mcp_open_world is False  # type: ignore[attr-defined]


@pytest.mark.unit
def test_find_list_params_no_lists() -> None:
    """A signature with no list params yields empty list."""
    from moneybin.mcp.decorator import _find_list_params

    def fn(name: str, count: int) -> None: ...

    assert _find_list_params(fn) == []


@pytest.mark.unit
def test_find_list_params_single_list() -> None:
    """A list[str] param is detected."""
    from moneybin.mcp.decorator import _find_list_params

    def fn(items: list[str]) -> None: ...

    assert _find_list_params(fn) == ["items"]


@pytest.mark.unit
def test_find_list_params_sequence() -> None:
    """Sequence[Mapping[...]] is detected (transactions_categorize_apply shape)."""
    from collections.abc import Mapping, Sequence

    from moneybin.mcp.decorator import _find_list_params

    def fn(items: Sequence[Mapping[str, str]]) -> None: ...

    assert _find_list_params(fn) == ["items"]


@pytest.mark.unit
def test_find_list_params_multiple() -> None:
    """Multiple list params are all returned (e.g. accept + reject)."""
    from moneybin.mcp.decorator import _find_list_params

    def fn(accept: list[str], reject: list[str]) -> None: ...

    assert sorted(_find_list_params(fn)) == ["accept", "reject"]


@pytest.mark.unit
def test_find_list_params_optional_list() -> None:
    """list[X] | None is detected (Optional list arg)."""
    from moneybin.mcp.decorator import _find_list_params

    def fn(items: list[str] | None = None) -> None: ...

    assert _find_list_params(fn) == ["items"]


@pytest.mark.unit
def test_find_list_params_str_not_a_list() -> None:
    """Str is not treated as a list even though it's a Sequence."""
    from moneybin.mcp.decorator import _find_list_params

    def fn(name: str) -> None: ...

    assert _find_list_params(fn) == []


@pytest.mark.unit
def test_mcp_tool_explicit_annotations() -> None:
    """Explicit kwargs override defaults."""

    @mcp_tool(
        sensitivity="medium",
        read_only=False,
        destructive=True,
        idempotent=False,
        open_world=True,
    )
    def example() -> ResponseEnvelope:  # type: ignore[return]
        ...

    assert example._mcp_read_only is False  # type: ignore[attr-defined]
    assert example._mcp_destructive is True  # type: ignore[attr-defined]
    assert example._mcp_idempotent is False  # type: ignore[attr-defined]
    assert example._mcp_open_world is True  # type: ignore[attr-defined]
