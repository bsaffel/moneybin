"""New sync projections must not disclose accepted legacy account targets."""

from __future__ import annotations

import json
import logging
from contextlib import nullcontext
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.adapters.sync_adapters import (
    sync_pull_envelope,
    sync_repair_actions,
    sync_status_envelope,
)
from moneybin.cli.commands import sync as cli_sync
from moneybin.cli.main import app
from moneybin.connectors.sync_models import (
    ConnectedInstitution,
    ScopeOutcome,
    SimpleFINHistorySnapshot,
    SimpleFINSource,
)
from moneybin.database import Database
from moneybin.extractors.account_identity import SourceAccount
from moneybin.extractors.simplefin.identity import source_origin
from moneybin.mcp.tools import sync as mcp_sync
from moneybin.services.account_resolver import AccountResolver
from moneybin.services.sync_service import SyncService
from tests.moneybin.db_helpers import create_core_tables
from tests.moneybin.test_services.test_simplefin_sync_service import (
    _client,  # pyright: ignore[reportPrivateUsage]  # shared synthetic broker builder
    _envelope,  # pyright: ignore[reportPrivateUsage]  # shared synthetic broker builder
    _service,  # pyright: ignore[reportPrivateUsage]  # real service construction
)

SAFE_NAME = "Synthetic approved account"


def prepared(db: Database, target: str | None) -> tuple[SyncService, MagicMock]:
    data = _envelope(empty=target is None)
    source = data.sources[0]
    assert isinstance(source, SimpleFINSource)
    window = {"start": "2026-09-01T00:00:00Z", "end": "2026-09-20T00:00:00Z"}
    scope = {"kind": "simplefin_v2_connection", "conn_id": "native-connection"}
    source.history = SimpleFINHistorySnapshot.model_validate({
        "observed_at": "2026-10-01T12:00:00Z",
        "replay_required": False,
        "discovery_history_target": None,
        "accounts": [
            {
                "account_id": "acct",
                "native_scope": scope,
                "acknowledged_intervals": [window],
                "history_target": None,
            }
        ],
    })
    source.outcomes = [
        ScopeOutcome.model_validate({
            "account_id": "acct",
            "native_scope": scope,
            "window": window,
            "status": "complete",
            "error_code": None,
        })
    ]
    if target is not None:
        create_core_tables(db)
        db.execute(
            """INSERT INTO core.dim_accounts
            (account_id, display_name, account_type, account_subtype, currency_code)
            VALUES (?, ?, 'depository', 'checking', 'USD')""",
            [target, SAFE_NAME],
        )
        resolver = AccountResolver(db, actor="user")
        assert resolver.knows_account_id(target)
        resolved = resolver.resolve(
            SourceAccount(
                source_type="simplefin",
                source_origin=source_origin(
                    source.connection_id,
                    source.protocol_version,
                    {"conn_id": "native-connection"},
                ),
                source_account_key="acct",
                account_name="Synthetic source account",
                explicit_account_id=target,
            )
        )
        assert resolved.account_id == target
    client = _client(data)
    client.list_institutions.return_value = [
        ConnectedInstitution(
            id=str(source.connection_id),
            provider="simplefin",
            provider_item_id=None,
            institution_name="Synthetic Bank",
            status="active",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            credential_generation=1,
            protocol_version=source.protocol_version,
            history=source.history,
        )
    ]
    return _service(db, client), client


@pytest.mark.parametrize("target", ["123456789012", "native-ledger-key"])
@pytest.mark.parametrize("operation", ["status", "pull"])
@pytest.mark.parametrize("surface", ["adapter", "cli", "mcp"])
async def test_accepted_legacy_target_is_withheld_from_sync_public_history_and_outcomes(
    db: Database,
    target: str,
    operation: str,
    surface: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    service, _broker = prepared(db, target)
    caplog.clear()
    caplog.set_level(logging.DEBUG, logger="moneybin.services.sync_service")
    if surface == "adapter":
        if operation == "status":
            body = sync_status_envelope(
                service.list_connections(), actions=[]
            ).to_dict()
        else:
            result = service.pull(refresh=False)
            assert result.complete is True
            body = sync_pull_envelope(
                result,
                actions=sync_repair_actions(result),
            ).to_dict()
    elif surface == "cli":
        monkeypatch.setattr(
            cli_sync,
            "_build_sync_service",
            MagicMock(return_value=nullcontext(service)),
        )
        arguments = ["sync", operation, "--output", "json"]
        if operation == "pull":
            arguments.append("--no-refresh")
        response = CliRunner().invoke(app, arguments)
        assert response.exit_code == 0, response.output
        body = json.loads(response.stdout)
        text_arguments = ["sync", operation]
        text_arguments += ["--no-refresh"] if operation == "pull" else ["--no-pager"]
        text = CliRunner().invoke(app, text_arguments)
        assert text.exit_code == 0, text.output
        assert target not in text.output
    else:
        monkeypatch.setattr(
            mcp_sync,
            "_build_sync_service",
            MagicMock(return_value=nullcontext(service)),
        )
        response = (
            await mcp_sync.sync_status_coarse()
            if operation == "status"
            else await mcp_sync.sync_pull_coarse(refresh=False)
        )
        body = response.to_dict()
    encoded = json.dumps(body)
    assert target not in encoded
    assert target not in caplog.text
    assert SAFE_NAME in encoded
    assert "native-connection" not in encoded
    row = (
        body["data"]["connections"][0]
        if operation == "status"
        else body["data"]["sources"][0]
    )
    account = row["history"]["accounts"][0]
    assert account["account_id"] is None
    assert account["display_name"] == SAFE_NAME
    assert len(account["acknowledged_intervals"]) == 1
    assert account["history_target"] is None
    if operation == "pull":
        assert body["data"]["complete"] is True
        outcome = row["outcomes"][0]
        assert outcome["account_id"] is None
        assert outcome["display_name"] == SAFE_NAME
        assert outcome["status"] == "complete"
        assert not any("Unresolved account" in action for action in body["actions"])
        receipt = db.execute(
            "SELECT source_metadata FROM raw.simplefin_sync_receipts"
        ).fetchone()
        assert receipt is not None
        assert json.loads(receipt[0])["history"]["accounts"][0]["account_id"] == "acct"
    assert (
        db.execute("""SELECT account_id FROM app.account_links
        WHERE source_type='simplefin' AND ref_value='acct' AND status='accepted'""").fetchone()
        == (target,)
    )


def test_unresolved_history_stays_incomplete_despite_identical_public_null_id(
    db: Database,
) -> None:
    service, _broker = prepared(db, None)
    result = service.pull(refresh=False)
    assert result.complete is False
    body = sync_pull_envelope(
        result,
        actions=sync_repair_actions(result),
    ).to_dict()
    assert body["data"]["complete"] is False
    source = body["data"]["sources"][0]
    account = source["history"]["accounts"][0]
    assert account["account_id"] is None
    assert account["display_name"] == "Unresolved account"
    assert len(account["acknowledged_intervals"]) == 1
    assert any("Unresolved account" in action for action in body["actions"])
