"""`system status --output json` and `system_status` share one data shape."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.mcp.tools.system import system_status_coarse


@pytest.mark.unit
async def test_cli_json_matches_mcp_overview_and_exports_sections(
    mcp_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same database, same `data`, same sensitivity — only `actions` may differ.

    The CLI names commands and MCP names tools, so `actions` stay per surface;
    everything an agent parses for state is one shape on both.
    """

    def no_blockers(_db_path: Path) -> list[dict[str, object]]:
        return []

    monkeypatch.setattr(
        "moneybin.adapters.system_status_adapters.find_blocking_processes",
        no_blockers,
    )

    mcp_envelope = (
        await system_status_coarse(sections=["overview", "exports"])
    ).to_dict()
    result = CliRunner().invoke(app, ["system", "status", "--output", "json"])
    assert result.exit_code == 0, result.output
    cli_envelope = json.loads(result.stdout)

    assert cli_envelope["data"]["sections"][0]["overview"]["accounts"]["count"] == 2
    assert cli_envelope["data"] == mcp_envelope["data"]
    assert cli_envelope["summary"] == mcp_envelope["summary"]
