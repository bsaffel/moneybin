"""Durable recovery is reachable through the existing CLI and MCP capabilities."""

from contextlib import nullcontext
from unittest.mock import MagicMock

import pytest
from fastmcp import FastMCP
from typer.testing import CliRunner

from moneybin.cli.commands import sync as cli_sync
from moneybin.cli.main import app
from moneybin.connectors.sync_models import PullResult
from moneybin.database import Database
from moneybin.mcp.tools import sync as mcp_sync
from moneybin.orchestration.refresh import RefreshResult
from moneybin.services import sync_service
from tests.moneybin.test_services.test_simplefin_sync_service import (
    JOB,
    _client,  # pyright: ignore[reportPrivateUsage]  # shared synthetic fixture builder
    _envelope,  # pyright: ignore[reportPrivateUsage]  # shared synthetic fixture builder
    _service,  # pyright: ignore[reportPrivateUsage]  # shared synthetic fixture builder
)


def _result() -> PullResult:
    return PullResult(
        job_id=str(JOB),
        transactions_loaded=1,
        accounts_loaded=1,
        balances_loaded=1,
        transactions_removed=0,
        institutions=[],
        ack_state="unknown",
        setup_state="failed",
        refresh_state="deferred",
    )


@pytest.mark.parametrize("surface", ["cli", "mcp"])
async def test_recovery_surface_preserves_phases_and_uses_exact_job(
    surface: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Incomplete committed work remains visible and gets a no-refetch recovery action."""
    service = MagicMock()
    service.recover.return_value = _result()
    module = cli_sync if surface == "cli" else mcp_sync
    monkeypatch.setattr(
        module, "_build_sync_service", MagicMock(return_value=nullcontext(service))
    )
    if surface == "cli":
        import json

        response = CliRunner().invoke(
            app,
            [
                "sync",
                "pull",
                "--job-id",
                str(JOB),
                "--local-only",
                "--no-refresh",
                "--output",
                "json",
            ],
        )
        assert response.exit_code == 1, response.output
        body = json.loads(response.stdout)
        assert body["data"]["ack_state"] == "unknown"
        assert body["data"]["setup_state"] == "failed"
        assert body["data"]["ingestion_state"] == "committed"
        assert body["data"]["complete"] is False
        assert any(
            str(JOB) in action and "--job-id" in action for action in body["actions"]
        )
    else:
        body = (
            await mcp_sync.sync_pull_coarse(
                job_id=str(JOB), local_only=True, refresh=False
            )
        ).to_dict()
        assert body["data"]["ack_state"] == "unknown"
        assert body["data"]["setup_state"] == "failed"
        assert body["data"]["ingestion_state"] == "committed"
        assert body["data"]["complete"] is False
        assert any(
            str(JOB) in action and "job_id=" in action for action in body["actions"]
        )
    service.pull.assert_not_called()
    assert service.recover.call_args.args == (str(JOB),)
    assert service.recover.call_args.kwargs["local_only"] is True
    assert service.recover.call_args.kwargs["refresh"] is False


@pytest.mark.parametrize("surface", ["cli", "mcp"])
async def test_local_only_public_repair_needs_no_broker_client(
    surface: str, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Offline repair constructs no authenticated transport, including when URL is unset."""
    client = _client(_envelope())
    client.ack.side_effect = OSError("synthetic lost ack")
    _service(db, client).pull(refresh=False)
    module = cli_sync if surface == "cli" else mcp_sync
    builder = MagicMock(
        side_effect=AssertionError("offline repair constructed broker transport")
    )
    monkeypatch.setattr(module, "_build_sync_client", builder)
    monkeypatch.setattr(
        "moneybin.database.get_database", MagicMock(return_value=nullcontext(db))
    )
    monkeypatch.setattr(
        "moneybin.utils.user_config.get_or_create_profile_id",
        MagicMock(return_value="synthetic-profile"),
    )
    monkeypatch.setattr(
        sync_service,
        "_refresh",
        MagicMock(return_value=RefreshResult(applied=True, duration_seconds=0)),
    )
    if surface == "cli":
        response = CliRunner().invoke(
            app,
            ["sync", "pull", "--job-id", str(JOB), "--local-only", "--output", "json"],
        )
        assert response.exit_code == 1, response.output
        assert '"ack_state": "unknown"' in response.stdout
        assert '"refresh_state": "complete"' in response.stdout
    else:
        body = (
            await mcp_sync.sync_pull_coarse(job_id=str(JOB), local_only=True)
        ).to_dict()
        assert body["data"]["ack_state"] == "unknown"
        assert body["data"]["refresh_state"] == "complete"
    builder.assert_not_called()


@pytest.mark.parametrize(
    "arguments",
    [
        {"local_only": True},
        {"job_id": str(JOB), "institution": "Synthetic Bank"},
        {"job_id": str(JOB), "provider": "simplefin"},
        {"job_id": str(JOB), "history_start": "2026-07-01"},
        {"job_id": str(JOB), "force": True},
    ],
)
async def test_recovery_conflicts_fail_before_building_service(
    arguments: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A repair request cannot silently become a new upstream job."""
    builder = MagicMock(side_effect=AssertionError("invalid request opened service"))
    monkeypatch.setattr(mcp_sync, "_build_sync_service", builder)
    response = await mcp_sync.sync_pull_coarse(**arguments)
    assert response.error is not None
    builder.assert_not_called()


async def test_four_sync_tools_expose_adopted_recovery_and_provider_selectors() -> None:
    """Agents discover recovery and provider selection without another capability."""
    server = FastMCP("synthetic")
    mcp_sync.register_sync_tools(server)
    tools = {tool.name: tool for tool in await server._list_tools()}  # pyright: ignore[reportPrivateUsage]  # inspect registration contract
    assert set(tools) == {"sync_link", "sync_status", "sync_pull", "sync_disconnect"}
    properties = tools["sync_pull"].parameters["properties"]
    assert {
        "job_id",
        "local_only",
        "provider",
        "connection_id",
        "history_start",
        "force",
        "refresh",
    } <= set(properties)
