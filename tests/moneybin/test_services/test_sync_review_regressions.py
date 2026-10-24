"""Final-review regressions for safe disconnect labels and actionable retry times."""

import json
from contextlib import nullcontext
from dataclasses import replace
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands import sync as cli_sync
from moneybin.cli.main import app
from moneybin.connectors.sync_models import ScopeOutcome, SyncDisconnectResult
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.mcp.tools import sync as mcp_sync
from tests.moneybin.test_services.test_simplefin_sync_service import (
    _client,  # pyright: ignore[reportPrivateUsage]  # shared broker boundary
    _envelope,  # pyright: ignore[reportPrivateUsage]  # shared envelope
    _service,  # pyright: ignore[reportPrivateUsage]  # real ingestion service
)
from tests.moneybin.test_services.test_sync_grant_surfaces import (
    CONNECTION,
    _grant,  # pyright: ignore[reportPrivateUsage]  # shared synthetic grant
)


@pytest.mark.parametrize("surface", ["cli-text", "cli-json", "mcp"])
@pytest.mark.parametrize("name", ["Checking 123456789012", "123456789012", "   "])
async def test_disconnect_masks_native_labels_without_changing_confirmation_identity(
    surface: str, name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    grant = _grant()
    grant.accounts[0].name = name
    native_accounts = [account.model_dump(mode="json") for account in grant.accounts]
    renamed = grant.model_copy(deep=True)
    renamed.accounts[0].name += " changed"
    renamed.accounts.reverse()
    changed = grant.model_copy(deep=True)
    changed.accounts[0].account_id += "-changed"
    disconnected = SyncDisconnectResult(
        **grant.model_dump(), remote_revocation="user_action_required"
    )
    service = MagicMock()
    service.plan_disconnect.return_value = grant
    service.disconnect.return_value = disconnected
    service.disconnect_confirmed.return_value = disconnected
    module = mcp_sync if surface == "mcp" else cli_sync
    monkeypatch.setattr(
        module, "_build_sync_service", MagicMock(return_value=nullcontext(service))
    )
    if surface == "mcp":
        confirmation = AsyncMock(return_value=MagicMock())
        monkeypatch.setattr(mcp_sync, "grant_confirmation_or_raise", confirmation)
        body = (await mcp_sync.sync_disconnect(connection_id=CONNECTION)).to_dict()
        rendered = json.dumps(body) + confirmation.call_args.kwargs["message"]
        binding = confirmation.call_args.kwargs["binding"]
        assert binding.arguments["account_identities"] == [
            list(identity)
            for identity in sorted({account.identity for account in grant.accounts})
        ]
        assert binding == mcp_sync._sync_disconnect_binding(renamed)  # pyright: ignore[reportPrivateUsage]  # display changes preserve confirmation identity
        assert binding != mcp_sync._sync_disconnect_binding(changed)  # pyright: ignore[reportPrivateUsage]  # native identity changes invalidate confirmation
    else:
        if surface == "cli-text":
            policy = cli_sync.get_terminal_policy
            monkeypatch.setattr(
                cli_sync,
                "get_terminal_policy",
                lambda: replace(policy(), interactive=True),
            )
            prompts: list[str] = []

            def confirm(message: str, **_kwargs: object) -> bool:
                prompts.append(message)
                return True

            monkeypatch.setattr(cli_sync.typer, "confirm", confirm)
            response = CliRunner().invoke(
                app, ["sync", "disconnect", "--connection-id", CONNECTION]
            )
            assert response.exit_code == 0, response.output
            rendered = response.output + " ".join(prompts)
            verify = service.disconnect_confirmed.call_args.kwargs["verify"]
            verify(grant)
            verify(renamed)
            with pytest.raises(UserError, match="changed"):
                verify(changed)
        else:
            response = CliRunner().invoke(
                app,
                [
                    "sync",
                    "disconnect",
                    "--connection-id",
                    CONNECTION,
                    "--yes",
                    "--output",
                    "json",
                ],
            )
            assert response.exit_code == 0, response.output
            rendered = response.stdout
    assert "123456789012" not in rendered
    assert "private-native" not in rendered and "private-scope" not in rendered
    if name.strip():
        assert "****9012" in rendered
    else:
        assert "Unnamed account" in rendered
    assert [
        account.model_dump(mode="json") for account in grant.accounts
    ] == native_accounts


@pytest.mark.parametrize("code", ["request_budget", "rate_limited"])
@pytest.mark.parametrize("surface", ["cli-json", "cli-text", "mcp"])
async def test_deferred_retry_deadline_survives_service_and_public_receipt(
    db: Database, monkeypatch: pytest.MonkeyPatch, code: str, surface: str
) -> None:
    deadline = datetime(2099, 10, 2, 12, 34, 56, tzinfo=UTC)
    data = _envelope(status="deferred")
    source = data.sources[0]
    window = source.batches[0].window
    source.batches = []
    source.outcomes = [
        ScopeOutcome(
            account_id=None,
            native_scope=None,
            window=window,
            status="deferred",
            error_code=code,
            retry_at=deadline,
        )
    ]
    result = _service(db, _client(data)).pull(refresh=False)
    projected = result.sources[0].outcomes[0].model_dump(mode="json")
    assert (
        datetime.fromisoformat(projected["retry_at"].replace("Z", "+00:00")) == deadline
    )
    service = MagicMock()
    service.pull.return_value = result
    module = mcp_sync if surface == "mcp" else cli_sync
    monkeypatch.setattr(
        module, "_build_sync_service", MagicMock(return_value=nullcontext(service))
    )
    if surface == "mcp":
        body = (await mcp_sync.sync_pull_coarse(refresh=False)).to_dict()
        rendered = json.dumps(body)
    else:
        arguments = ["sync", "pull", "--no-refresh"]
        if surface == "cli-json":
            arguments += ["--output", "json"]
        response = CliRunner().invoke(app, arguments)
        assert response.exit_code == 1, response.output
        rendered = response.output
        body = json.loads(response.stdout) if surface == "cli-json" else None
    assert "2099-10-02" in rendered and "12:34:56" in rendered
    if body is not None:
        outcome = body["data"]["sources"][0]["outcomes"][0]
        assert (
            datetime.fromisoformat(outcome["retry_at"].replace("Z", "+00:00"))
            == deadline
        )
        actions = " ".join(body["actions"]).lower()
        assert "2099-10-02" in actions and ("wait" in actions or "after" in actions)
        if surface == "mcp":
            import re

            repairs = re.findall(r"sync_pull\(([^)]*)\)", actions)
            assert repairs
            assert all(
                "job_id=" in call and "local_only=true" in call for call in repairs
            )
        else:
            assert "use moneybin sync pull --connection-id" not in actions


def test_payment_attention_guidance_requires_subscription_remediation_and_relink(
    db: Database,
) -> None:
    broker = _client(_envelope())
    grant = _grant().model_copy(
        update={"status": "error", "error_code": "payment_required"}
    )
    broker.list_institutions.return_value = [grant]
    result = _service(db, broker).list_connections()[0]
    assert result.guidance is not None
    assert "subscription" in result.guidance.lower()
    assert "sync link" in result.guidance and CONNECTION in result.guidance


async def test_explicit_sync_pull_can_outlast_generic_mcp_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit pull has its own machine budget, leaving ordinary tools unchanged."""
    import time

    from moneybin.connectors.sync_models import PullResult

    service = MagicMock()

    def delayed_pull(**_kwargs: object) -> PullResult:
        time.sleep(0.05)
        return PullResult(
            job_id="synthetic-job",
            transactions_loaded=0,
            accounts_loaded=0,
            balances_loaded=0,
            transactions_removed=0,
            institutions=[],
        )

    service.pull.side_effect = delayed_pull
    monkeypatch.setattr(
        mcp_sync, "_build_sync_service", MagicMock(return_value=nullcontext(service))
    )
    monkeypatch.setattr("moneybin.mcp.decorator._get_timeout_seconds", lambda: 0.01)
    body = (await mcp_sync.sync_pull_coarse(refresh=False)).to_dict()
    assert body.get("error") is None
    assert body["data"]["job_id"] == "synthetic-job"
