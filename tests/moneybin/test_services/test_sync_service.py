"""Unit tests for SyncService — business logic orchestrating client + loader."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from moneybin.connectors.sync_models import (
    ConnectedInstitution,
    ConnectInitiateResponse,
    ConnectStatusResponse,
    SyncDataResponse,
    SyncTriggerResponse,
)
from moneybin.database import Database
from moneybin.loaders.plaid_loader import PlaidLoader
from moneybin.services.sync_service import SyncService

FIXTURE = (
    Path(__file__).parent.parent
    / "test_loaders"
    / "fixtures"
    / "plaid_sync_response.yaml"
)


@pytest.fixture
def sync_data() -> SyncDataResponse:
    with FIXTURE.open() as f:
        return SyncDataResponse.model_validate(yaml.safe_load(f))


@pytest.fixture
def loader(db: Database) -> PlaidLoader:
    return PlaidLoader(db)


@pytest.fixture
def mock_client(sync_data: SyncDataResponse) -> MagicMock:
    client = MagicMock()
    client.trigger_sync.return_value = SyncTriggerResponse(
        job_id=sync_data.metadata.job_id,
        status="completed",
        transaction_count=3,
    )
    client.get_data.return_value = sync_data
    return client


def test_pull_happy_path(
    mock_client: MagicMock,
    db: Database,
    loader: PlaidLoader,
    sync_data: SyncDataResponse,
) -> None:
    service = SyncService(client=mock_client, db=db, loader=loader)
    result = service.pull()

    mock_client.trigger_sync.assert_called_once_with(
        provider_item_id=None,
        reset_cursor=False,
    )
    mock_client.get_data.assert_called_once_with(sync_data.metadata.job_id)
    assert result.transactions_loaded == 3
    assert result.accounts_loaded == 2
    assert result.balances_loaded == 2
    # The fixture's removed_transactions=["txn_removed_old"] references a
    # transaction that was never loaded, so the actual deleted rowcount is 0.
    # PullResult.transactions_removed reflects rows touched, not IDs requested.
    assert result.transactions_removed == 0
    assert result.institutions[0].provider_item_id == "item_chase_abc"


def test_pull_with_institution_resolves_to_provider_item_id(
    mock_client: MagicMock, db: Database, loader: PlaidLoader
) -> None:
    mock_client.list_institutions.return_value = [
        ConnectedInstitution(
            id="u1",
            provider_item_id="item_chase_abc",
            provider="plaid",
            institution_name="Chase",
            status="active",
            created_at=datetime(2026, 3, 15, tzinfo=UTC),
        ),
    ]
    service = SyncService(client=mock_client, db=db, loader=loader)
    service.pull(institution="Chase")
    mock_client.trigger_sync.assert_called_once_with(
        provider_item_id="item_chase_abc",
        reset_cursor=False,
    )


def test_pull_with_unknown_institution_raises(
    mock_client: MagicMock, db: Database, loader: PlaidLoader
) -> None:
    mock_client.list_institutions.return_value = []
    service = SyncService(client=mock_client, db=db, loader=loader)
    with pytest.raises(ValueError, match="no connected institution"):
        service.pull(institution="UnknownBank")


def test_pull_with_provider_item_id_skips_resolution(
    mock_client: MagicMock, db: Database, loader: PlaidLoader
) -> None:
    service = SyncService(client=mock_client, db=db, loader=loader)
    service.pull(provider_item_id="item_direct")
    mock_client.list_institutions.assert_not_called()
    mock_client.trigger_sync.assert_called_once_with(
        provider_item_id="item_direct",
        reset_cursor=False,
    )


def test_pull_rejects_both_institution_and_provider_item_id(
    mock_client: MagicMock, db: Database, loader: PlaidLoader
) -> None:
    service = SyncService(client=mock_client, db=db, loader=loader)
    with pytest.raises(ValueError, match="mutually exclusive"):
        service.pull(institution="Chase", provider_item_id="item_x")


def test_pull_with_force_passes_reset_cursor(
    mock_client: MagicMock, db: Database, loader: PlaidLoader
) -> None:
    service = SyncService(client=mock_client, db=db, loader=loader)
    service.pull(force=True)
    mock_client.trigger_sync.assert_called_once_with(
        provider_item_id=None,
        reset_cursor=True,
    )


def test_connect_new_institution_auto_pulls(
    mock_client: MagicMock,
    db: Database,
    loader: PlaidLoader,
    sync_data: SyncDataResponse,
) -> None:
    mock_client.initiate_connect.return_value = ConnectInitiateResponse(
        session_id="sess_x",
        link_url="https://hosted.plaid.com/link/x",
        connect_type="widget_flow",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    mock_client.poll_connect_status.return_value = ConnectStatusResponse(
        session_id="sess_x",
        status="connected",
        provider_item_id="item_chase_abc",
        institution_name="Chase",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    service = SyncService(client=mock_client, db=db, loader=loader)
    result = service.connect(auto_pull=True)
    assert result.provider_item_id == "item_chase_abc"
    assert result.institution_name == "Chase"
    assert result.pull_result is not None
    assert result.pull_result.transactions_loaded == 3
    mock_client.trigger_sync.assert_called_once_with(
        provider_item_id="item_chase_abc",
        reset_cursor=False,
    )


def test_connect_no_pull_returns_without_pull_result(
    mock_client: MagicMock, db: Database, loader: PlaidLoader
) -> None:
    mock_client.initiate_connect.return_value = ConnectInitiateResponse(
        session_id="sess_x",
        link_url="https://hosted.plaid.com/link/x",
        connect_type="widget_flow",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    mock_client.poll_connect_status.return_value = ConnectStatusResponse(
        session_id="sess_x",
        status="connected",
        provider_item_id="item_new",
        institution_name="Bank",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    service = SyncService(client=mock_client, db=db, loader=loader)
    result = service.connect(auto_pull=False)
    assert result.pull_result is None
    mock_client.trigger_sync.assert_not_called()


def test_connect_re_auth_resolves_institution_name(
    mock_client: MagicMock, db: Database, loader: PlaidLoader
) -> None:
    mock_client.list_institutions.return_value = [
        ConnectedInstitution(
            id="u1",
            provider_item_id="item_existing",
            provider="plaid",
            institution_name="Chase",
            status="error",
            created_at=datetime(2026, 3, 15, tzinfo=UTC),
        ),
    ]
    mock_client.initiate_connect.return_value = ConnectInitiateResponse(
        session_id="sess_x",
        link_url="https://hosted.plaid.com/link/x",
        connect_type="widget_flow",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    mock_client.poll_connect_status.return_value = ConnectStatusResponse(
        session_id="sess_x",
        status="connected",
        provider_item_id="item_existing",
        institution_name="Chase",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    service = SyncService(client=mock_client, db=db, loader=loader)
    service.connect(institution="Chase", auto_pull=False)
    mock_client.initiate_connect.assert_called_once_with(
        provider_item_id="item_existing",
        return_to=None,
    )


def test_connect_falls_through_to_new_when_institution_not_matched(
    mock_client: MagicMock, db: Database, loader: PlaidLoader
) -> None:
    """Unknown institution name falls through to new-connection flow.

    Per design Section 8: an unknown institution name is a new-connection
    intent, not an error — let the server's Link flow name the institution.
    """
    mock_client.list_institutions.return_value = []  # no existing connections
    mock_client.initiate_connect.return_value = ConnectInitiateResponse(
        session_id="sess_x",
        link_url="https://hosted.plaid.com/link/x",
        connect_type="widget_flow",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    mock_client.poll_connect_status.return_value = ConnectStatusResponse(
        session_id="sess_x",
        status="connected",
        provider_item_id="item_new",
        institution_name="Bank",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    service = SyncService(client=mock_client, db=db, loader=loader)
    service.connect(institution="Wells Fargo", auto_pull=False)
    # provider_item_id is None — new-connection flow, not update mode
    mock_client.initiate_connect.assert_called_once_with(
        provider_item_id=None,
        return_to=None,
    )


def test_connect_invokes_on_initiate_callback_before_polling(
    mock_client: MagicMock, db: Database, loader: PlaidLoader
) -> None:
    """on_initiate fires after initiate_connect, before polling.

    The CLI uses on_initiate to print link_url + open the browser. Verify
    the service invokes it between initiate_connect and poll_connect_status.
    """
    initiate_resp = ConnectInitiateResponse(
        session_id="sess_x",
        link_url="https://hosted.plaid.com/link/x",
        connect_type="widget_flow",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    mock_client.initiate_connect.return_value = initiate_resp
    mock_client.poll_connect_status.return_value = ConnectStatusResponse(
        session_id="sess_x",
        status="connected",
        provider_item_id="item_new",
        institution_name="Bank",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    captured: list[ConnectInitiateResponse] = []
    service = SyncService(client=mock_client, db=db, loader=loader)
    service.connect(auto_pull=False, on_initiate=captured.append)
    assert captured == [initiate_resp]


def test_resolve_institution_raises_on_ambiguous_name(
    mock_client: MagicMock, db: Database, loader: PlaidLoader
) -> None:
    """Two connections sharing institution_name must not silently map to one."""
    mock_client.list_institutions.return_value = [
        ConnectedInstitution(
            id="u1",
            provider_item_id="item_a",
            provider="plaid",
            institution_name="Chase",
            status="active",
            created_at=datetime(2026, 3, 15, tzinfo=UTC),
        ),
        ConnectedInstitution(
            id="u2",
            provider_item_id="item_b",
            provider="plaid",
            institution_name="Chase",
            status="active",
            created_at=datetime(2026, 3, 15, tzinfo=UTC),
        ),
    ]
    service = SyncService(client=mock_client, db=db, loader=loader)
    with pytest.raises(ValueError, match="multiple connected institutions match"):
        service.pull(institution="Chase")


def test_list_connections_returns_views_with_guidance(
    mock_client: MagicMock, db: Database, loader: PlaidLoader
) -> None:
    mock_client.list_institutions.return_value = [
        ConnectedInstitution(
            id="u1",
            provider_item_id="item_a",
            provider="plaid",
            institution_name="Chase",
            status="active",
            created_at=datetime(2026, 3, 15, tzinfo=UTC),
        ),
        ConnectedInstitution(
            id="u2",
            provider_item_id="item_b",
            provider="plaid",
            institution_name="Schwab",
            status="error",
            created_at=datetime(2026, 3, 15, tzinfo=UTC),
        ),
    ]
    service = SyncService(client=mock_client, db=db, loader=loader)
    views = service.list_connections()
    assert len(views) == 2
    chase = next(v for v in views if v.institution_name == "Chase")
    schwab = next(v for v in views if v.institution_name == "Schwab")
    assert chase.guidance is None
    assert chase.error_code is None
    assert schwab.guidance is not None
    assert "Schwab" in schwab.guidance
    assert "sync connect" in schwab.guidance


def test_list_connections_threads_error_code(
    mock_client: MagicMock, db: Database, loader: PlaidLoader
) -> None:
    """error_code from ConnectedInstitution is surfaced in SyncConnectionView."""
    mock_client.list_institutions.return_value = [
        ConnectedInstitution(
            id="u1",
            provider_item_id="item_a",
            provider="plaid",
            institution_name="BankA",
            status="error",
            error_code="ITEM_LOGIN_REQUIRED",
            created_at=datetime(2026, 3, 15, tzinfo=UTC),
        ),
        ConnectedInstitution(
            id="u2",
            provider_item_id="item_b",
            provider="plaid",
            institution_name="BankB",
            status="error",
            error_code=None,
            created_at=datetime(2026, 3, 15, tzinfo=UTC),
        ),
    ]
    service = SyncService(client=mock_client, db=db, loader=loader)
    views = service.list_connections()
    bank_a = next(v for v in views if v.institution_name == "BankA")
    bank_b = next(v for v in views if v.institution_name == "BankB")

    assert bank_a.error_code == "ITEM_LOGIN_REQUIRED"
    # Known error code should trigger specific guidance (not the generic fallback)
    assert bank_a.guidance is not None
    assert "BankA" in bank_a.guidance

    assert bank_b.error_code is None
    # Unknown/absent error code falls back to generic guidance
    assert bank_b.guidance is not None
    assert "sync connect" in bank_b.guidance


def test_list_connections_unrecognized_error_code_falls_back_to_generic_guidance(
    mock_client: MagicMock, db: Database, loader: PlaidLoader
) -> None:
    """An error_code not in _ERROR_GUIDANCE falls back to generic guidance without raising."""
    mock_client.list_institutions.return_value = [
        ConnectedInstitution(
            id="u1",
            provider_item_id="item_a",
            provider="plaid",
            institution_name="BankA",
            status="error",
            error_code="WEIRD_NEW_CODE",
            created_at=datetime(2026, 3, 15, tzinfo=UTC),
        ),
    ]
    service = SyncService(client=mock_client, db=db, loader=loader)
    views = service.list_connections()
    bank_a = views[0]

    assert bank_a.error_code == "WEIRD_NEW_CODE"
    assert bank_a.guidance is not None
    assert "sync connect" in bank_a.guidance


def test_disconnect_resolves_institution_and_calls_client(
    mock_client: MagicMock, db: Database, loader: PlaidLoader
) -> None:
    mock_client.list_institutions.return_value = [
        ConnectedInstitution(
            id="conn_uuid",
            provider_item_id="item_a",
            provider="plaid",
            institution_name="Chase",
            status="active",
            created_at=datetime(2026, 3, 15, tzinfo=UTC),
        ),
    ]
    service = SyncService(client=mock_client, db=db, loader=loader)
    service.disconnect(institution="Chase")
    mock_client.disconnect.assert_called_once_with("conn_uuid")


def test_disconnect_unknown_institution_raises(
    mock_client: MagicMock, db: Database, loader: PlaidLoader
) -> None:
    mock_client.list_institutions.return_value = []
    service = SyncService(client=mock_client, db=db, loader=loader)
    with pytest.raises(ValueError, match="no connected institution"):
        service.disconnect(institution="UnknownBank")


class TestPullAutoRefreshes:
    """sync.pull() runs the post-load refresh pipeline by default.

    Mirrors the contract established for the import path in
    smart-import-transform.md: callers opt out with refresh=False, and the
    result envelope reports transforms_applied / transforms_error from the
    SQLMesh step of the pipeline.
    """

    def test_pull_refreshes_by_default_when_rows_loaded(
        self,
        mock_client: MagicMock,
        db: Database,
        loader: PlaidLoader,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from moneybin.services import sync_service as mod
        from moneybin.services.refresh import RefreshResult

        calls = 0

        def fake_refresh(_db: object) -> RefreshResult:
            nonlocal calls
            calls += 1
            return RefreshResult(applied=True, duration_seconds=0.05)

        monkeypatch.setattr(mod, "_refresh", fake_refresh)
        service = SyncService(client=mock_client, db=db, loader=loader)
        result = service.pull()

        assert calls == 1
        assert result.transforms_applied is True
        assert result.transforms_duration_seconds == 0.05
        assert result.transforms_error is None

    def test_pull_no_refresh_skips_pipeline(
        self,
        mock_client: MagicMock,
        db: Database,
        loader: PlaidLoader,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from moneybin.services import sync_service as mod
        from moneybin.services.refresh import RefreshResult

        calls = 0

        def fake_refresh(_db: object) -> RefreshResult:
            nonlocal calls
            calls += 1
            return RefreshResult(applied=True, duration_seconds=0.0)

        monkeypatch.setattr(mod, "_refresh", fake_refresh)
        service = SyncService(client=mock_client, db=db, loader=loader)
        result = service.pull(refresh=False)

        assert calls == 0
        assert result.transforms_applied is False
        assert result.transforms_duration_seconds is None
        assert result.transforms_error is None

    def test_pull_skips_refresh_when_no_rows_loaded(
        self,
        db: Database,
        loader: PlaidLoader,
        sync_data: SyncDataResponse,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """No raw rows landed → nothing to refresh."""
        from moneybin.services import sync_service as mod
        from moneybin.services.refresh import RefreshResult

        calls = 0

        def fake_refresh(_db: object) -> RefreshResult:
            nonlocal calls
            calls += 1
            return RefreshResult(applied=True, duration_seconds=0.0)

        monkeypatch.setattr(mod, "_refresh", fake_refresh)

        empty_client = MagicMock()
        empty_client.trigger_sync.return_value = SyncTriggerResponse(
            job_id="job_empty", status="completed", transaction_count=0
        )
        empty_data = sync_data.model_copy(
            update={"accounts": [], "transactions": [], "balances": []}
        )
        empty_client.get_data.return_value = empty_data

        service = SyncService(client=empty_client, db=db, loader=loader)
        result = service.pull()

        assert calls == 0
        assert result.transforms_applied is False
        assert result.transforms_error is None

    def test_pull_refreshes_when_only_removals_landed(
        self,
        db: Database,
        loader: PlaidLoader,
        sync_data: SyncDataResponse,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Removal-only sync: deletions are a state change too.

        Guards against a bug where the gate counted only loaded rows: a
        sync that only carried ``removed_transactions`` would delete from
        raw but skip refresh, leaving the deleted row visible in
        core.fct_transactions.
        """
        from moneybin.services import sync_service as mod
        from moneybin.services.refresh import RefreshResult

        calls = 0

        def fake_refresh(_db: object) -> RefreshResult:
            nonlocal calls
            calls += 1
            return RefreshResult(applied=True, duration_seconds=0.04)

        monkeypatch.setattr(mod, "_refresh", fake_refresh)

        # First populate raw with a transaction whose id matches what the
        # next sync will mark removed — otherwise handle_removed_transactions
        # returns 0 and the gate stays closed for legitimate reasons.
        seed_data = sync_data.model_copy(update={"removed_transactions": []})
        seed_client = MagicMock()
        seed_client.trigger_sync.return_value = SyncTriggerResponse(
            job_id="job_seed", status="completed", transaction_count=3
        )
        seed_client.get_data.return_value = seed_data
        seed_service = SyncService(client=seed_client, db=db, loader=loader)
        seed_service.pull(refresh=False)
        calls = 0  # reset after seeding

        # Now: removal-only sync — empty accounts/transactions/balances,
        # but a real removed_transactions id pointing at a seeded row.
        removal_data = sync_data.model_copy(
            update={
                "accounts": [],
                "transactions": [],
                "balances": [],
                "removed_transactions": ["txn_001"],
            }
        )
        removal_client = MagicMock()
        removal_client.trigger_sync.return_value = SyncTriggerResponse(
            job_id="job_removal", status="completed", transaction_count=0
        )
        removal_client.get_data.return_value = removal_data
        service = SyncService(client=removal_client, db=db, loader=loader)
        result = service.pull()

        assert result.transactions_loaded == 0
        assert result.accounts_loaded == 0
        assert result.balances_loaded == 0
        assert result.transactions_removed == 1
        assert calls == 1, (
            "pure-removal sync must still refresh so deletes propagate from "
            "raw.plaid_transactions to core.fct_transactions"
        )
        assert result.transforms_applied is True

    def test_pull_surfaces_refresh_error_on_failure(
        self,
        mock_client: MagicMock,
        db: Database,
        loader: PlaidLoader,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Refresh soft-fails — the SQLMesh error must reach the envelope."""
        from moneybin.services import sync_service as mod
        from moneybin.services.refresh import RefreshResult

        def fake_refresh(_db: object) -> RefreshResult:
            return RefreshResult(
                applied=False, duration_seconds=0.12, error="SQLMeshError"
            )

        monkeypatch.setattr(mod, "_refresh", fake_refresh)
        service = SyncService(client=mock_client, db=db, loader=loader)
        result = service.pull()

        assert result.transactions_loaded == 3
        assert result.transforms_applied is False
        assert result.transforms_duration_seconds == 0.12
        assert result.transforms_error == "SQLMeshError"
