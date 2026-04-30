"""Tests for the mcp_tool decorator — sensitivity tier and domain field."""

from __future__ import annotations

from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope


def test_mcp_tool_supports_domain() -> None:
    """The mcp_tool decorator carries the domain string as an attribute.

    Tools in extended namespaces (categorize, budget, tax, privacy,
    transactions.matches) declare a domain; the registration layer translates
    it into mcp.tool(tags={domain}).
    """

    @mcp_tool(sensitivity="medium", domain="categorize")
    def example_tool() -> ResponseEnvelope:  # type: ignore[return]
        ...

    assert getattr(example_tool, "_mcp_domain", None) == "categorize"


def test_mcp_tool_default_domain_is_none() -> None:
    """Tools without an explicit domain are core tools (always visible)."""

    @mcp_tool(sensitivity="low")
    def example_tool() -> ResponseEnvelope:  # type: ignore[return]
        ...

    assert getattr(example_tool, "_mcp_domain", None) is None


def test_mcp_tool_sensitivity_attribute_still_set() -> None:
    """Existing _mcp_sensitivity attribute is preserved alongside domain."""

    @mcp_tool(sensitivity="high", domain="tax")
    def example_tool() -> ResponseEnvelope:  # type: ignore[return]
        ...

    assert getattr(example_tool, "_mcp_sensitivity", None) == "high"
    assert getattr(example_tool, "_mcp_domain", None) == "tax"
