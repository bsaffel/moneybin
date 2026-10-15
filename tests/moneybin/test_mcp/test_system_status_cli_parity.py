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
        "moneybin.services.system_service.find_blocking_processes",
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


@pytest.mark.unit
async def test_rejected_attempt_keeps_system_uncategorized_inventory_on_both_surfaces(
    mcp_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from moneybin.database import get_database
    from moneybin.repositories.categorization_decisions_repo import (
        CategorizationDecisionsRepo,
    )
    from tests.moneybin.db_helpers import derive_is_categorizable

    def no_blockers(_db_path: Path) -> list[dict[str, object]]:
        return []

    monkeypatch.setattr(
        "moneybin.services.system_service.find_blocking_processes", no_blockers
    )
    with get_database(read_only=False) as db:
        db.execute("""INSERT INTO core.fct_transactions
            (transaction_id, account_id, transaction_date, amount, currency_code,
             description, source_type, is_transfer)
            VALUES ('synthetic', 'ACC001', DATE '2026-10-15', -1, 'USD',
                    'Synthetic purchase', 'ofx', FALSE)""")
        derive_is_categorizable(db)
        repo = CategorizationDecisionsRepo(db)
        attempt = repo.ensure_pending("synthetic", actor="test")
        repo.update_status(
            attempt["decision_id"],
            status="rejected",
            category_id=None,
            merchant_id=None,
            decided_by="user",
            actor="test",
        )
    mcp = (await system_status_coarse(sections=["overview", "exports"])).to_dict()
    overview = mcp["data"]["sections"][0]["overview"]
    assert overview["categorization"]["uncategorized"] == 1
    result = CliRunner().invoke(app, ["system", "status", "--output", "json"])
    assert result.exit_code == 0, result.output
    cli = json.loads(result.stdout)
    assert cli["data"] == mcp["data"]
    pending = CliRunner().invoke(
        app, ["review", "--type", "categorize", "--output", "json"]
    )
    assert pending.exit_code == 0, pending.output
    assert json.loads(pending.stdout)["data"]["categorize_pending"] == 0
