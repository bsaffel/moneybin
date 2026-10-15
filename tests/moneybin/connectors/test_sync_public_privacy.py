"""Sync account displays and provider diagnostics retain existing privacy rules."""

import json
from unittest.mock import MagicMock

import httpx
import pytest
import respx

from moneybin.connectors.sync_client import SyncClient
from moneybin.connectors.sync_errors import SyncAPIError, SyncLinkError
from moneybin.database import Database
from moneybin.privacy.classified_envelope import build_classified_envelope
from moneybin.privacy.introspection import derive_tier, extract_data_classes
from moneybin.privacy.payloads.sync import SyncHistoryAccountRow, SyncScopeRow
from moneybin.privacy.taxonomy import DataClass, Tier
from tests.moneybin.connectors import test_sync_client_v2 as synthetic_client

client = synthetic_client.client
PRIVATE = "private-987654 https://credential.invalid/secret?token=synthetic"


@pytest.mark.parametrize(
    "row",
    [
        SyncHistoryAccountRow(
            account_id="canonical",
            display_name="Synthetic nickname",
            acknowledged_intervals=[],
            history_target=None,
        ),
        SyncScopeRow(
            account_id="canonical",
            display_name="Synthetic nickname",
            scope_kind="account",
            status="partial",
            error_code="provider_error",
            window=None,
        ),
    ],
)
def test_canonical_account_display_keeps_user_note_classification(row: object) -> None:
    assert DataClass.USER_NOTE in extract_data_classes(type(row))
    assert derive_tier(type(row)) is Tier.MEDIUM
    envelope = build_classified_envelope(row)
    assert envelope.summary.sensitivity == "medium"
    assert "Synthetic nickname" in json.dumps(envelope.to_dict())


@pytest.mark.parametrize("poll", [False, True])
@respx.mock
def test_link_status_provider_error_is_never_public_text(
    client: SyncClient, poll: bool
) -> None:
    respx.get("https://test.api/sync/link/status").respond(
        200,
        json={
            "session_id": "synthetic",
            "status": "failed",
            "error": PRIVATE,
            "expiration": "2026-10-01T12:00:00Z",
        },
    )
    if poll:
        client._sleep = MagicMock()  # pyright: ignore[reportPrivateUsage]  # avoid polling delay in deterministic test
        with pytest.raises(SyncLinkError) as raised:
            client.poll_link_status("synthetic")
        public = str(raised.value)
    else:
        public = str(client.get_link_status("synthetic").error)
    assert "private-987654" not in public and "credential.invalid" not in public
    assert "link" in public.lower()


@respx.mock
def test_malformed_link_status_does_not_expose_validation_input(
    client: SyncClient,
) -> None:
    respx.get("https://test.api/sync/link/status").mock(
        return_value=httpx.Response(
            200,
            json={
                "session_id": "synthetic",
                "status": PRIVATE,
                "expiration": "2026-10-01T12:00:00Z",
            },
        )
    )
    with pytest.raises(SyncAPIError) as raised:
        client.get_link_status("synthetic")
    assert "private-987654" not in str(raised.value)


@pytest.mark.parametrize("surface", ["cli", "mcp"])
async def test_current_status_bounds_provider_codes_and_terminal_names(
    db: Database,
    surface: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from contextlib import nullcontext

    from typer.testing import CliRunner

    from moneybin.cli.commands import sync as cli_sync
    from moneybin.cli.main import app
    from moneybin.connectors.sync_models import ConnectedInstitution
    from moneybin.mcp.tools import sync as mcp_sync
    from tests.moneybin.test_services.test_simplefin_sync_service import (
        NOW,
        _client,  # pyright: ignore[reportPrivateUsage]  # synthetic broker fixture
        _envelope,  # pyright: ignore[reportPrivateUsage]  # synthetic broker fixture
        _service,  # pyright: ignore[reportPrivateUsage]  # synthetic broker fixture
    )

    broker = _client(_envelope())
    broker.list_institutions.return_value = [
        ConnectedInstitution(
            id="00000000-0000-0000-0000-000000000064",
            provider="simplefin",
            provider_item_id=None,
            institution_name="\x1b[31mSynthetic Bank\x1b[0m",
            status="error",
            error_code=PRIVATE,
            created_at=NOW,
        )
    ]
    real_service = _service(db, broker)
    module = cli_sync if surface == "cli" else mcp_sync
    monkeypatch.setattr(
        module, "_build_sync_service", MagicMock(return_value=nullcontext(real_service))
    )
    if surface == "cli":
        result = CliRunner().invoke(app, ["sync", "status", "--output", "json"])
        assert result.exit_code == 0, result.output
        public = result.stdout
        text = CliRunner().invoke(app, ["sync", "status"])
        assert "\x1b" not in text.stdout
    else:
        public = json.dumps((await mcp_sync.sync_status_coarse()).to_dict())
    assert "private-987654" not in public and "credential.invalid" not in public
    assert "provider_error" in public


def test_link_auto_pull_failure_does_not_log_provider_exception(
    db: Database,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from moneybin.connectors.sync_models import LinkStatusResponse
    from tests.moneybin.test_services.test_simplefin_sync_service import (
        NOW,
        _client,  # pyright: ignore[reportPrivateUsage]  # synthetic broker fixture
        _envelope,  # pyright: ignore[reportPrivateUsage]  # synthetic broker fixture
        _service,  # pyright: ignore[reportPrivateUsage]  # synthetic broker fixture
    )

    broker = _client(_envelope())
    broker.list_institutions.return_value = []
    broker.poll_link_status.return_value = LinkStatusResponse(
        session_id="synthetic",
        status="linked",
        provider_item_id="synthetic-item",
        expiration=NOW,
    )
    service = _service(db, broker)
    service.pull = MagicMock(side_effect=ValueError(PRIVATE))
    service.link()
    assert (
        "private-987654" not in caplog.text and "credential.invalid" not in caplog.text
    )


@respx.mock
def test_malformed_connection_listing_does_not_expose_native_input(
    client: SyncClient,
) -> None:
    respx.get("https://test.api/institutions").respond(
        200,
        json=[
            {
                "id": "00000000-0000-0000-0000-000000000064",
                "provider": "simplefin",
                "provider_item_id": None,
                "institution_name": None,
                "status": "active",
                "created_at": "2026-10-01T12:00:00Z",
                "accounts": [{"account_id": {"private": PRIVATE}}],
            }
        ],
    )
    with pytest.raises(SyncAPIError) as raised:
        client.list_institutions()
    assert "private-987654" not in str(
        raised.value
    ) and "credential.invalid" not in str(raised.value)


@pytest.mark.parametrize("surface", ["cli", "mcp"])
@respx.mock
async def test_malformed_link_initiation_is_safe_on_public_surfaces_and_logs(
    client: SyncClient,
    db: Database,
    surface: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from contextlib import nullcontext

    from typer.testing import CliRunner

    from moneybin.cli.commands import sync as cli_sync
    from moneybin.cli.main import app
    from moneybin.extractors.plaid import PlaidExtractor
    from moneybin.mcp.tools import sync as mcp_sync
    from moneybin.services.sync_service import SyncService

    respx.post("https://test.api/sync/link/initiate").respond(
        200,
        json={
            "session_id": "synthetic",
            "link_url": "https://broker.invalid/browser",
            "link_type": PRIVATE,
            "expiration": "2026-10-01T12:00:00Z",
        },
    )
    service = SyncService(client=client, db=db, loader=PlaidExtractor(db))
    module = cli_sync if surface == "cli" else mcp_sync
    monkeypatch.setattr(
        module, "_build_sync_service", MagicMock(return_value=nullcontext(service))
    )
    if surface == "cli":
        response = CliRunner().invoke(
            app, ["sync", "link", "--provider", "simplefin", "--output", "json"]
        )
        assert response.exit_code != 0
        public = response.output
    else:
        envelope = await mcp_sync.sync_link_coarse(provider="simplefin")
        assert envelope.error is not None
        public = json.dumps(envelope.to_dict())
    assert "private-987654" not in public and "credential.invalid" not in public
    assert (
        "private-987654" not in caplog.text and "credential.invalid" not in caplog.text
    )
