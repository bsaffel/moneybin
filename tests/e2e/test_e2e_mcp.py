# ruff: noqa: S101
"""E2E MCP server tests — verify the server boots and responds to protocol requests.

Uses the MCP SDK client to connect to `moneybin mcp serve` via stdio transport,
exercising the full startup path: encrypted DB connection, schema init, tool
registration, and JSON-RPC protocol handling.
"""

from __future__ import annotations

import json
import os
from typing import Any

import pytest

from tests.e2e.conftest import (
    FAST_ARGON2_ENV,
    make_workflow_env,
    seed_pending_match,
)

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
                assert "accounts" in tool_names
                assert "system_status" in tool_names
                # Formerly extended-namespace tools must also be visible at connect:
                assert "transactions_categorize_commit" in tool_names

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
        """All v2 accounts namespace tools are registered on the server.

        The narrow write tools (rename/include/archive/unarchive) were folded
        into ``accounts_set``; confirm both that ``accounts_set`` is present
        and that the removed names are gone.
        """
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
                    "accounts",
                    "accounts_get",
                    "accounts_summary",
                    "accounts_set",
                    "accounts_balances",
                    "accounts_balance_history",
                    "accounts_balance_reconcile",
                    "accounts_balance_assertions",
                    "accounts_balance_assert",
                    "accounts_balance_assertion_delete",
                }
                missing = v2_accounts_tools - tool_names
                assert not missing, f"Missing v2 accounts tools: {missing}"

                # Narrow write tools folded into accounts_set — must be absent
                for removed in (
                    "accounts_rename",
                    "accounts_include",
                    "accounts_archive",
                    "accounts_unarchive",
                ):
                    assert removed not in tool_names, (
                        f"{removed} should be folded into accounts_set"
                    )

    async def test_accounts_balance_assertions_invocable(
        self, mcp_env: dict[str, str]
    ) -> None:
        """accounts_balance_assertions returns a valid critical-sensitivity envelope.

        Each row carries account_id (ACCOUNT_IDENTIFIER → CRITICAL); the
        middleware masks it to ``****<last4>``. Uses app.balance_assertions
        (created at DB init, not SQLMesh) so this works on a fresh DB and
        returns an empty assertions list.
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

                result = await session.call_tool("accounts_balance_assertions", {})
                assert not result.isError, (
                    f"accounts_balance_assertions returned error: {result.content}"
                )
                content = result.content[0]
                assert isinstance(content, TextContent)
                envelope = json.loads(content.text)
                assert envelope["summary"]["sensitivity"] == "critical"
                # Fresh DB has no assertions — payload shape is {"assertions": []}
                assert isinstance(envelope["data"], dict)
                assert isinstance(envelope["data"]["assertions"], list)


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
                    "reports_large_transactions",
                    "reports_balance_drift",
                }
                missing = expected - tool_names
                assert not missing, f"Missing reports view-backed tools: {missing}"

                # reports_uncategorized removed — use transactions_categorize_pending instead
                assert "reports_uncategorized" not in tool_names
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
                    "system_audit",
                }
                missing = expected - tool_names
                assert not missing, f"Missing curation tools: {missing}"


class TestNamespaceResources:
    """MCP resource registration smoke tests.

    Only moneybin://schema remains after PR #185 removed seven duplicate resources
    (status, accounts, privacy, tools, accounts://summary, recent-curation,
    net-worth://summary) whose data is reachable via tools.
    """

    async def test_schema_resource_registered(self, mcp_env: dict[str, str]) -> None:
        """moneybin://schema resource is registered on the server."""
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
                assert "moneybin://schema" in resource_uris
                # Removed resources must not appear.
                assert "accounts://summary" not in resource_uris
                assert "moneybin://recent-curation" not in resource_uris
                assert "net-worth://summary" not in resource_uris
                assert "moneybin://status" not in resource_uris
                assert "moneybin://accounts" not in resource_uris
                assert "moneybin://privacy" not in resource_uris
                assert "moneybin://tools" not in resource_uris


class TestMatchesTools:
    """transactions_matches_pending and transactions_matches_set smoke tests."""

    @pytest.fixture(scope="class")
    def matches_env(self, tmp_path_factory: pytest.TempPathFactory) -> dict[str, str]:
        """Isolated profile for matches tool tests."""
        home = tmp_path_factory.mktemp("e2e_matches")
        return make_workflow_env(home, "matches-test")

    async def test_matches_tools_registered(self, matches_env: dict[str, str]) -> None:
        """Both matches tools are registered on the server."""
        from mcp import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client

        server_params = StdioServerParameters(
            command="uv",  # noqa: S607
            args=["run", "moneybin", "mcp", "serve"],
            env=_server_env(matches_env),
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools_result = await session.list_tools()
                tool_names = {t.name for t in tools_result.tools}
                assert "transactions_matches_pending" in tool_names
                assert "transactions_matches_set" in tool_names

    async def test_transactions_matches_pending_returns_seeded_match(
        self, matches_env: dict[str, str]
    ) -> None:
        """transactions_matches_pending returns a seeded pending match."""
        from mcp import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client
        from mcp.types import TextContent

        seeded_match_id = "e2e_pending_match_001"
        seed_pending_match(matches_env, seeded_match_id)

        server_params = StdioServerParameters(
            command="uv",  # noqa: S607
            args=["run", "moneybin", "mcp", "serve"],
            env=_server_env(matches_env),
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool("transactions_matches_pending", {})

                assert not result.isError, f"Tool returned error: {result.content}"
                content = result.content[0]
                assert isinstance(content, TextContent)
                envelope = json.loads(content.text)

                assert "data" in envelope
                matches = envelope["data"]["matches"]
                assert isinstance(matches, list)
                match_ids = [m["match_id"] for m in matches]
                assert seeded_match_id in match_ids

    async def test_transactions_matches_set_accepts_pending_match(
        self, matches_env: dict[str, str]
    ) -> None:
        """transactions_matches_set accepts a pending match and returns accepted status."""
        from mcp import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client
        from mcp.types import TextContent

        seeded_match_id = "e2e_set_match_001"
        seed_pending_match(matches_env, seeded_match_id)

        server_params = StdioServerParameters(
            command="uv",  # noqa: S607
            args=["run", "moneybin", "mcp", "serve"],
            env=_server_env(matches_env),
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(
                    "transactions_matches_set",
                    {"match_id": seeded_match_id, "status": "accepted"},
                )

                assert not result.isError, f"Tool returned error: {result.content}"
                content = result.content[0]
                assert isinstance(content, TextContent)
                envelope = json.loads(content.text)

                assert "data" in envelope
                assert envelope["data"]["match_id"] == seeded_match_id
                assert envelope["data"]["match_status"] == "accepted"

    async def test_transactions_matches_history_returns_envelope(
        self, matches_env: dict[str, str]
    ) -> None:
        """transactions_matches_history returns decisions with decided_at timestamps."""
        from mcp import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client
        from mcp.types import TextContent

        # History excludes pending — accept the match first so it's a decision.
        seed_pending_match(matches_env, "e2e_hist_001")
        # A second match left pending must NOT surface in history (pins the
        # get_match_log pending-exclusion at the MCP layer).
        seed_pending_match(matches_env, "e2e_hist_pending_002")
        server_params = StdioServerParameters(
            command="uv",  # noqa: S607
            args=["run", "moneybin", "mcp", "serve"],
            env=_server_env(matches_env),
        )
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                set_result = await session.call_tool(
                    "transactions_matches_set",
                    {"match_id": "e2e_hist_001", "status": "accepted"},
                )
                assert not set_result.isError, f"set failed: {set_result.content}"
                result = await session.call_tool(
                    "transactions_matches_history", {"limit": 50}
                )
                assert not result.isError, f"Tool returned error: {result.content}"
                content = result.content[0]
                assert isinstance(content, TextContent)
                envelope = json.loads(content.text)
                matches = envelope["data"]["matches"]
                ids = [m["match_id"] for m in matches]
                assert "e2e_hist_001" in ids, "accepted decision must appear in history"
                assert "e2e_hist_pending_002" not in ids, (
                    "pending proposals must be excluded from history"
                )
                entry = next(m for m in matches if m["match_id"] == "e2e_hist_001")
                # A time-series view must carry the decision timestamp.
                assert entry["decided_at"]

    async def test_transactions_matches_run_registered(
        self, matches_env: dict[str, str]
    ) -> None:
        """transactions_matches_run is registered and returns an envelope."""
        from mcp import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client

        server_params = StdioServerParameters(
            command="uv",  # noqa: S607
            args=["run", "moneybin", "mcp", "serve"],
            env=_server_env(matches_env),
        )
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools = {t.name for t in (await session.list_tools()).tools}
                assert "transactions_matches_run" in tools
                assert "transactions_matches_history" in tools


class TestMCPFirstRunSetup:
    """First-run setup over real stdio with no pre-existing profile.

    These tests drive `moneybin mcp serve` with an empty MONEYBIN_HOME and no
    MONEYBIN_PROFILE. The regression being locked: interactive wizard output
    on stdout corrupted the JSON-RPC stream, making initialize() fail.
    Completing the JSON-RPC handshake (initialize + call_tool) is the
    assertion — a corrupted stream would blow up the SDK transport layer.
    """

    @pytest.fixture
    def unconfigured_env(
        self, tmp_path_factory: pytest.TempPathFactory
    ) -> dict[str, str]:
        """Server env pointing at an empty MONEYBIN_HOME, no profile set."""
        home = tmp_path_factory.mktemp("e2e_first_run")
        env = {**os.environ, **FAST_ARGON2_ENV, "MONEYBIN_HOME": str(home)}
        env.pop("MONEYBIN_PROFILE", None)
        # Remove any inherited encryption key — ProfileService generates one
        # in-process and stores it in the MemoryKeyring. A conflicting env-var
        # key would shadow the generated key and open a different database.
        env.pop("MONEYBIN_DATABASE__ENCRYPTION_KEY", None)
        return env

    async def test_tools_only_client_gets_setup_envelope(
        self, unconfigured_env: dict[str, str]
    ) -> None:
        """No elicitation capability → structured setup_required, stream intact.

        The client does not pass elicitation_callback, so initialize() omits
        the elicitation capability. FirstRunSetupMiddleware sees no capability
        and returns the setup_required envelope instead of eliciting. The key
        assertion is that the JSON-RPC handshake completes at all — a corrupted
        stdout stream would blow up the SDK transport before call_tool() returns.
        """
        from mcp import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client
        from mcp.types import TextContent

        server_params = StdioServerParameters(
            command="uv",  # noqa: S607 — uv is on PATH in dev environments
            args=["run", "moneybin", "mcp", "serve"],
            env=unconfigured_env,
        )
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                # system_status is used (not accounts_summary) because it
                # gracefully handles a fresh DB without SQLMesh transforms.
                result = await session.call_tool("system_status", {})

        # Assert after the session closes: a clean shutdown is itself evidence the
        # JSON-RPC stream stayed intact end to end (the original-bug regression).
        # Middleware returned a ToolResult (not raised), so isError is False.
        assert not result.isError, f"Unexpected MCP-level error: {result.content}"
        content = result.content[0]
        assert isinstance(content, TextContent)
        payload = json.loads(content.text)
        assert payload["error"]["code"] == "infra_setup_required", (
            f"Expected infra_setup_required error envelope, got: {payload}"
        )

    async def test_elicitation_client_creates_profile_and_proceeds(
        self, unconfigured_env: dict[str, str]
    ) -> None:
        """Elicitation-capable client supplies a name → profile created, call works.

        ClientSession passes elicitation_callback so initialize() declares the
        elicitation capability. FirstRunSetupMiddleware elicits the profile name,
        creates the encrypted DB (via ProfileService.create), activates the profile
        in-process, and re-executes the original tool call. The second call proves
        the middleware is no longer active (self._configured is True).
        """
        from mcp import ClientSession, types
        from mcp.client.stdio import StdioServerParameters, stdio_client
        from mcp.shared.context import RequestContext
        from mcp.types import TextContent

        # FastMCP wraps response_type=str as ScalarElicitationType[str], sending a
        # schema with a single "value" property. The accept content must match:
        # {"value": "<profile_name>"}. "e2e-first-run" normalizes to itself.
        async def elicit_cb(
            context: RequestContext[ClientSession, Any],
            params: types.ElicitRequestParams,
        ) -> types.ElicitResult:
            return types.ElicitResult(
                action="accept", content={"value": "e2e-first-run"}
            )

        server_params = StdioServerParameters(
            command="uv",  # noqa: S607 — uv is on PATH in dev environments
            args=["run", "moneybin", "mcp", "serve"],
            env=unconfigured_env,
        )
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(
                read, write, elicitation_callback=elicit_cb
            ) as session:
                await session.initialize()
                # system_status is used (not accounts_summary): it works on a fresh
                # DB without SQLMesh transforms; accounts_summary queries
                # core.dim_accounts which doesn't exist until transforms run.
                first = await session.call_tool("system_status", {})
                second = await session.call_tool("system_status", {})

        # Assert after the session closes (clean shutdown = stream stayed intact).
        # Wire format is pydantic_core serialization of ResponseEnvelope — the
        # "status" field only appears in to_dict(), not in the Pydantic model
        # itself. Success is indicated by error: null + data present.
        first_content = first.content[0]
        assert isinstance(first_content, TextContent)
        first_payload = json.loads(first_content.text)
        assert first_payload.get("error") is None, (
            f"First call after elicitation returned error: {first_payload}"
        )
        assert "data" in first_payload, (
            f"First call missing data key — expected real tool response: {first_payload}"
        )

        second_content = second.content[0]
        assert isinstance(second_content, TextContent)
        second_payload = json.loads(second_content.text)
        assert second_payload.get("error") is None, (
            f"Second call (already configured) returned error: {second_payload}"
        )
        assert "data" in second_payload, (
            f"Second call missing data key — expected real tool response: {second_payload}"
        )

    async def test_resource_read_unconfigured_does_not_invoke_wizard(
        self, unconfigured_env: dict[str, str]
    ) -> None:
        """Reading moneybin://schema with no profile fails cleanly, not via the wizard.

        The schema resource reaches get_database() directly, bypassing
        FirstRunSetupMiddleware (which only guards tool calls). Clearing the
        profile resolver on the unconfigured boot path makes get_settings()
        raise a clean MCP error instead of running the stdout-writing wizard.
        Regression signal: read_resource raises a normal McpError AND the
        session stays responsive afterward — a corrupted stdio stream would
        hang or kill the transport, not return an error.
        """
        from mcp import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client
        from mcp.shared.exceptions import McpError
        from pydantic import AnyUrl

        server_params = StdioServerParameters(
            command="uv",  # noqa: S607 — uv is on PATH in dev environments
            args=["run", "moneybin", "mcp", "serve"],
            env=unconfigured_env,
        )
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                with pytest.raises(McpError):
                    await session.read_resource(AnyUrl("moneybin://schema"))
                # Stream survived the failed read: the session still answers.
                # A wizard write would have corrupted the JSON-RPC channel.
                tools = await session.list_tools()
                assert any(t.name == "system_status" for t in tools.tools)
