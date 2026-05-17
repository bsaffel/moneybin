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

                # Tools that should always be present (v2 names) — full surface
                # is visible at connect (mcp-architecture.md §3).
                assert "reports_spending" in tool_names
                assert "accounts_list" in tool_names
                assert "system_status" in tool_names
                # Formerly extended-namespace tools must also be visible at connect:
                assert "transactions_categorize_apply" in tool_names
                assert "budget_set" in tool_names

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

                # Call system_status — read-only, no inputs, exercises the
                # envelope contract end-to-end.
                result = await session.call_tool("system_status", {})

                assert not result.isError, f"Tool returned error: {result.content}"
                assert len(result.content) > 0
                content = result.content[0]
                assert isinstance(content, TextContent)
                envelope = json.loads(content.text)

                assert "summary" in envelope
                assert "data" in envelope
                assert "actions" in envelope
                assert envelope["summary"]["sensitivity"] == "low"

    async def test_accounts_v2_tools_registered(self, mcp_env: dict[str, str]) -> None:
        """All 14 v2 accounts namespace tools are registered on the server."""
        from mcp import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client

        server_params = StdioServerParameters(
            command="uv",  # noqa: S607
            args=["run", "moneybin", "mcp", "serve"],
            env=_server_env(mcp_env),
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools_result = await session.list_tools()
                tool_names = {t.name for t in tools_result.tools}

                v2_accounts_tools = {
                    "accounts_list",
                    "accounts_get",
                    "accounts_summary",
                    "accounts_rename",
                    "accounts_include",
                    "accounts_archive",
                    "accounts_unarchive",
                    "accounts_set",
                    "accounts_balance_list",
                    "accounts_balance_history",
                    "accounts_balance_reconcile",
                    "accounts_balance_assertions_list",
                    "accounts_balance_assert",
                    "accounts_balance_assertion_delete",
                }
                missing = v2_accounts_tools - tool_names
                assert not missing, f"Missing v2 accounts tools: {missing}"

                # v1 tool removed — confirm it is gone
                assert "accounts_balances" not in tool_names

    async def test_accounts_balance_assertions_list_invocable(
        self, mcp_env: dict[str, str]
    ) -> None:
        """accounts_balance_assertions_list returns a valid medium-sensitivity envelope.

        Uses the app.balance_assertions table (created at DB init, not SQLMesh),
        so this works on a fresh database and returns an empty list.
        """
        from mcp import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client
        from mcp.types import TextContent

        server_params = StdioServerParameters(
            command="uv",  # noqa: S607
            args=["run", "moneybin", "mcp", "serve"],
            env=_server_env(mcp_env),
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()

                result = await session.call_tool("accounts_balance_assertions_list", {})
                assert not result.isError, (
                    f"accounts_balance_assertions_list returned error: {result.content}"
                )
                content = result.content[0]
                assert isinstance(content, TextContent)
                envelope = json.loads(content.text)
                assert envelope["summary"]["sensitivity"] == "medium"
                # Fresh DB has no assertions — empty list is correct
                assert isinstance(envelope["data"], list)


class TestReportsNetworthTools:
    """v2 reports_networth_* tool smoke tests."""

    async def test_reports_networth_tools_registered(
        self, mcp_env: dict[str, str]
    ) -> None:
        """Both reports_networth_* tools are registered on the server."""
        from mcp import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client

        server_params = StdioServerParameters(
            command="uv",  # noqa: S607
            args=["run", "moneybin", "mcp", "serve"],
            env=_server_env(mcp_env),
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools_result = await session.list_tools()
                tool_names = {t.name for t in tools_result.tools}

                assert "reports_networth" in tool_names
                assert "reports_networth_history" in tool_names

    async def test_reports_view_backed_tools_registered(
        self, mcp_env: dict[str, str]
    ) -> None:
        """The seven view-backed `reports_*` tools (recipe library) are registered."""
        from mcp import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client

        server_params = StdioServerParameters(
            command="uv",  # noqa: S607
            args=["run", "moneybin", "mcp", "serve"],
            env=_server_env(mcp_env),
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools_result = await session.list_tools()
                tool_names = {t.name for t in tools_result.tools}

                expected = {
                    "reports_spending",
                    "reports_cashflow",
                    "reports_recurring",
                    "reports_merchants",
                    "reports_uncategorized",
                    "reports_large_transactions",
                    "reports_balance_drift",
                }
                missing = expected - tool_names
                assert not missing, f"Missing reports view-backed tools: {missing}"

                # v1 tools removed
                assert "reports_spending_summary" not in tool_names
                assert "reports_spending_by_category" not in tool_names


class TestCurationTools:
    """Curation MCP tools (notes, tags, splits, manual create, audit) are registered."""

    async def test_curation_tools_registered(self, mcp_env: dict[str, str]) -> None:
        from mcp import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client

        server_params = StdioServerParameters(
            command="uv",  # noqa: S607
            args=["run", "moneybin", "mcp", "serve"],
            env=_server_env(mcp_env),
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools_result = await session.list_tools()
                tool_names = {t.name for t in tools_result.tools}

                expected = {
                    "transactions_create",
                    "transactions_notes_add",
                    "transactions_notes_edit",
                    "transactions_notes_delete",
                    "transactions_tags_set",
                    "transactions_tags_rename",
                    "transactions_splits_set",
                    "import_labels_set",
                    "system_audit_list",
                }
                missing = expected - tool_names
                assert not missing, f"Missing curation tools: {missing}"


class TestNamespaceResources:
    """MCP resource registration smoke tests."""

    async def test_accounts_summary_resource_registered(
        self, mcp_env: dict[str, str]
    ) -> None:
        """accounts://summary resource is registered on the server."""
        from mcp import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client

        server_params = StdioServerParameters(
            command="uv",  # noqa: S607
            args=["run", "moneybin", "mcp", "serve"],
            env=_server_env(mcp_env),
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                resources_result = await session.list_resources()
                resource_uris = {str(r.uri) for r in resources_result.resources}
                assert "accounts://summary" in resource_uris

    async def test_recent_curation_resource_registered(
        self, mcp_env: dict[str, str]
    ) -> None:
        """moneybin://recent-curation resource is registered on the server."""
        from mcp import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client

        server_params = StdioServerParameters(
            command="uv",  # noqa: S607
            args=["run", "moneybin", "mcp", "serve"],
            env=_server_env(mcp_env),
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                resources_result = await session.list_resources()
                resource_uris = {str(r.uri) for r in resources_result.resources}
                assert "moneybin://recent-curation" in resource_uris

    async def test_networth_summary_resource_registered(
        self, mcp_env: dict[str, str]
    ) -> None:
        """net-worth://summary resource is registered on the server."""
        from mcp import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client

        server_params = StdioServerParameters(
            command="uv",  # noqa: S607
            args=["run", "moneybin", "mcp", "serve"],
            env=_server_env(mcp_env),
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                resources_result = await session.list_resources()
                resource_uris = {str(r.uri) for r in resources_result.resources}
                assert "net-worth://summary" in resource_uris
