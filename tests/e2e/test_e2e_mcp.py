# ruff: noqa: S101
"""E2E MCP server tests — verify the server boots and responds to protocol requests.

Uses the MCP SDK client to connect to `moneybin mcp serve` via stdio transport,
exercising the full startup path: encrypted DB connection, schema init, tool
registration, and JSON-RPC protocol handling.
"""

from __future__ import annotations

import json
import os

import pytest

from tests.e2e.conftest import FAST_ARGON2_ENV, make_workflow_env

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


@pytest.fixture(scope="module")
def mcp_env(tmp_path_factory: pytest.TempPathFactory) -> dict[str, str]:
    """Create an isolated profile for MCP server tests."""
    home = tmp_path_factory.mktemp("e2e_mcp")
    return make_workflow_env(home, "mcp-test")


def _server_env(mcp_env: dict[str, str]) -> dict[str, str]:
    """Build the full environment for the MCP server subprocess."""
    return {**os.environ, **FAST_ARGON2_ENV, **mcp_env}


class TestMCPServerBoot:
    """Verify the MCP server starts, registers tools, and responds to protocol requests."""

    async def test_server_initializes_and_lists_tools(
        self, mcp_env: dict[str, str]
    ) -> None:
        """MCP server boots on an encrypted DB and reports registered tools."""
        from mcp import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client

        server_params = StdioServerParameters(
            command="uv",  # noqa: S607 — uv is on PATH in dev environments
            args=["run", "moneybin", "mcp", "serve"],
            env=_server_env(mcp_env),
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                result = await session.initialize()

                # Server should identify itself
                assert result.serverInfo.name == "MoneyBin"

                # List tools — core namespaces should be registered
                tools_result = await session.list_tools()
                tool_names = [t.name for t in tools_result.tools]

                assert len(tool_names) > 0, "No tools registered"

                # Core tools that should always be present
                assert "spending.summary" in tool_names
                assert "accounts.list" in tool_names
                assert "moneybin.discover" in tool_names

    async def test_server_invokes_tool(self, mcp_env: dict[str, str]) -> None:
        """MCP server can invoke a tool and return a valid response envelope."""
        from mcp import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client
        from mcp.types import TextContent

        server_params = StdioServerParameters(
            command="uv",  # noqa: S607 — uv is on PATH in dev environments
            args=["run", "moneybin", "mcp", "serve"],
            env=_server_env(mcp_env),
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()

                # Call moneybin.discover — always works, no data needed
                result = await session.call_tool(
                    "moneybin.discover", {"domain": "categorize"}
                )

                assert not result.isError, f"Tool returned error: {result.content}"
                assert len(result.content) > 0
                content = result.content[0]
                assert isinstance(content, TextContent)
                envelope = json.loads(content.text)

                assert "summary" in envelope
                assert "data" in envelope
                assert "actions" in envelope
                assert envelope["summary"]["sensitivity"] == "low"
                assert envelope["data"]["domain"] == "categorize"
                assert envelope["actions"], "discover should return next-step hints"
