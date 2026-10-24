"""Unit tests for SyncService — business logic orchestrating client + loader."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal
from unittest.mock import MagicMock, patch
from uuid import NAMESPACE_URL, UUID, uuid5

import httpx
import pytest
import yaml

from moneybin import error_codes
from moneybin.connectors.sync_client import SyncAPIError, SyncClient
from moneybin.connectors.sync_models import (
    ConnectedInstitution,
    LinkInitiateResponse,
    LinkStatusResponse,
    PlaidSyncDataResponse,
    SyncAccountScope,
    SyncAckV2Response,
    SyncDisconnectResponse,
    SyncEnvelopeV2,
    SyncStatusV2Response,
    SyncTriggerV2Response,
)
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.extractors.plaid import PlaidExtractor
from moneybin.metrics.registry import SYNC_DISCONNECT_OUTCOMES
from moneybin.orchestration.refresh import RefreshResult
from moneybin.secrets import SecretStore
from moneybin.services import sync_service
from moneybin.services.sync_service import SyncService
from moneybin.tables import ACCOUNT_LINKS, SECURITY_PRICES
from tests.moneybin.db_helpers import spy_restate

FIXTURE = (
    Path(__file__).parent.parent
    / "test_extractors"
    / "fixtures"
    / "plaid_sync_response.yaml"
)

INVESTMENTS_FIXTURE = (
    Path(__file__).parent.parent
    / "test_extractors"
    / "fixtures"
    / "plaid_investments_sync_response.yaml"
)

DISCONNECT_A_ID = str(uuid5(NAMESPACE_URL, "sync-disconnect:a"))
DISCONNECT_B_ID = str(uuid5(NAMESPACE_URL, "sync-disconnect:b"))


@pytest.fixture
def sync_data() -> PlaidSyncDataResponse:
    with FIXTURE.open() as f:
        return PlaidSyncDataResponse.model_validate(yaml.safe_load(f))


@pytest.fixture
def loader(db: Database) -> PlaidExtractor:
    return PlaidExtractor(db)


def _set_v2_job(
    client: MagicMock,
    native: PlaidSyncDataResponse,
    *,
    job_label: str | None = None,
) -> None:
    """Serve one native Plaid payload through the accepted broker v2 job."""
    client.consumer_id = "test-consumer"
    label = job_label or native.metadata.job_id
    job_id = uuid5(NAMESPACE_URL, f"sync-job:{label}")
    payload = native.model_dump(mode="json")
    payload["metadata"]["job_id"] = str(job_id)
    source = {
        "provider": "plaid",
        "connection_id": uuid5(NAMESPACE_URL, f"sync-connection:{label}"),
        "credential_generation": 1,
        "batches": [
            {
                "batch_id": uuid5(NAMESPACE_URL, f"sync-batch:{label}"),
                "fetched_at": datetime(2026, 4, 8, tzinfo=UTC),
                "window": None,
                "payload": payload,
            }
        ],
        "outcomes": [
            {
                "account_id": None,
                "native_scope": {
                    "kind": "plaid_item",
                    "provider_item_id": item.provider_item_id,
                },
                "window": None,
                "status": "failed" if item.status == "failed" else "complete",
                "error_code": item.error_code,
            }
            for item in native.metadata.institutions
        ],
    }
    envelope = SyncEnvelopeV2.model_validate({
        "job_id": job_id,
        "contract_version": 2,
        "status": "completed",
        "sources": [source],
    })
    results = [item.model_dump(exclude={"batches"}) for item in envelope.sources]
    client.trigger_sync.return_value = SyncTriggerV2Response(
        job_id=job_id,
        contract_version=2,
        status="completed",
        transaction_count=len(native.transactions),
    )
    client.get_data.return_value = envelope
    client.get_status.return_value = SyncStatusV2Response.model_validate({
        "job_id": job_id,
        "contract_version": 2,
        "status": "completed",
        "transaction_count": len(native.transactions),
        "error": None,
        "started_at": datetime(2026, 4, 8, tzinfo=UTC),
        "completed_at": datetime(2026, 4, 8, 0, 1, tzinfo=UTC),
        "acked_at": None,
        "results": results,
    })
    client.ack.return_value = SyncAckV2Response.model_validate({
        "job_id": job_id,
        "contract_version": 2,
        "status": "acked",
        "acked_at": datetime(2026, 4, 8, 0, 2, tzinfo=UTC),
        "results": results,
    })
    client.disconnect.return_value = SyncDisconnectResponse(
        connection_id=UUID(DISCONNECT_A_ID),
        credential_generation=1,
        accounts=[],
        remote_revocation="revoked",
    )


@pytest.fixture
def mock_client(sync_data: PlaidSyncDataResponse) -> MagicMock:
    client = MagicMock()
    _set_v2_job(client, sync_data)
    return client


@pytest.fixture
def investments_sync_data() -> PlaidSyncDataResponse:
    with INVESTMENTS_FIXTURE.open() as f:
        return PlaidSyncDataResponse.model_validate(yaml.safe_load(f))


@pytest.fixture
def mock_investments_client(investments_sync_data: PlaidSyncDataResponse) -> MagicMock:
    client = MagicMock()
    _set_v2_job(client, investments_sync_data)
    return client


@pytest.fixture
def mock_sync_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep pull tests focused on sync orchestration, not SQLMesh execution."""

    def fake_refresh(_db: Database, **_kwargs: object) -> RefreshResult:
        return RefreshResult(applied=True, duration_seconds=0.0)

    monkeypatch.setattr(sync_service, "_refresh", fake_refresh)


def _payload(accounts: list[dict[str, Any]], *, item_id: str) -> PlaidSyncDataResponse:
    """A minimal one-institution /sync/data response carrying only accounts."""
    return PlaidSyncDataResponse.model_validate({
        "accounts": accounts,
        "transactions": [],
        "balances": [],
        "removed_transactions": [],
        "metadata": {
            "job_id": f"job-{item_id}",
            "synced_at": "2026-08-15T12:00:00Z",
            "institutions": [
                {
                    "provider_item_id": item_id,
                    "institution_name": "Chase",
                    "status": "completed",
                    "transaction_count": 0,
                }
            ],
        },
    })


def _relinked_payload() -> PlaidSyncDataResponse:
    """The same card after a relink: new account_id, new item, same persistent id."""
    return _payload(
        [
            {
                "account_id": "acc_chase_check_reissued",
                "persistent_account_id": "ppa_chase_check_0001",
                "account_type": "depository",
                "account_subtype": "checking",
                "institution_name": "Chase",
                "name": "Chase Checking",
                "official_name": "Total Checking",
                "mask": "1234",
            }
        ],
        item_id="item_chase_xyz",
    )


def _payload_with_shared_official_name() -> PlaidSyncDataResponse:
    """Two Chase cards whose official_name is the same product label."""
    return _payload(
        [
            {
                "account_id": "acc_card_a",
                "persistent_account_id": "ppa_card_a",
                "account_type": "credit",
                "account_subtype": "credit card",
                "institution_name": "Chase",
                "name": "Freedom Unlimited",
                "official_name": "Ultimate Rewards®",
                "mask": "1111",
            },
            {
                "account_id": "acc_card_b",
                "persistent_account_id": "ppa_card_b",
                "account_type": "credit",
                "account_subtype": "credit card",
                "institution_name": "Chase",
                "name": "Sapphire Preferred",
                "official_name": "Ultimate Rewards®",
                "mask": "2222",
            },
        ],
        item_id="item_chase_cards",
    )


def _payload_with_and_without_a_nickname() -> PlaidSyncDataResponse:
    """One account with a holder-set nickname, one with none at all."""
    return _payload(
        [
            {
                "account_id": "acc_named",
                "persistent_account_id": "ppa_named",
                "account_type": "depository",
                "account_subtype": "checking",
                "institution_name": "Chase",
                "name": "Everyday Checking",
                "official_name": "Total Checking",
                "mask": "1111",
            },
            {
                "account_id": "acc_unnamed",
                "persistent_account_id": "ppa_unnamed",
                "account_type": "depository",
                "account_subtype": "savings",
                "institution_name": "Chase",
                "name": None,
                "official_name": "Premier Savings",
                "mask": "2222",
            },
        ],
        item_id="item_chase_nicknames",
    )


@pytest.mark.usefixtures("mock_sync_refresh")
def test_pull_happy_path(
    mock_client: MagicMock,
    db: Database,
    loader: PlaidExtractor,
    sync_data: PlaidSyncDataResponse,
) -> None:
    service = SyncService(client=mock_client, db=db, loader=loader)
    result = service.pull()

    mock_client.trigger_sync.assert_called_once_with(
        provider_item_id=None,
        reset_cursor=False,
        provider=None,
        connection_id=None,
        history_start=None,
    )
    job_id = str(mock_client.trigger_sync.return_value.job_id)
    mock_client.get_data.assert_called_once_with(job_id)
    # Canonical happy-path witness also asserts the ack, so an accidental removal
    # of the cursor-ack call is caught here, not only in the dedicated ack test.
    mock_client.ack.assert_called_once_with(job_id)
    assert result.transactions_loaded == 3
    assert result.accounts_loaded == 2
    assert result.balances_loaded == 2
    # The fixture's removed_transactions=["txn_removed_old"] references a
    # transaction that was never loaded, so the actual deleted rowcount is 0.
    # PullResult.transactions_removed reflects rows touched, not IDs requested.
    assert result.transactions_removed == 0
    assert result.institutions[0].provider_item_id == "item_chase_abc"


@pytest.mark.usefixtures("mock_sync_refresh")
def test_pull_acks_after_successful_load(
    mock_client: MagicMock,
    db: Database,
    loader: PlaidExtractor,
    sync_data: PlaidSyncDataResponse,
) -> None:
    """A successful pull acks the broker so it advances cursors.

    Ack is unconditional once the load is durable — it persists the broker's
    held cursors and releases the served window. The job_id passed is the
    trigger response's job_id.
    """
    service = SyncService(client=mock_client, db=db, loader=loader)
    service.pull()
    mock_client.ack.assert_called_once_with(
        str(mock_client.trigger_sync.return_value.job_id)
    )


@pytest.mark.usefixtures("mock_sync_refresh")
def test_pull_ack_failure_does_not_fail_the_pull(
    mock_client: MagicMock,
    db: Database,
    loader: PlaidExtractor,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An ack failure leaves committed Raw and an explicit recovery obligation.

    Ack is best-effort post-load: the data is already durable, so a broker
    blip leaves the cursor un-advanced. The result must report uncertainty
    without hiding durable ingestion or raising after the commit.
    """
    mock_client.ack.side_effect = SyncAPIError("broker unreachable")
    service = SyncService(client=mock_client, db=db, loader=loader)
    with caplog.at_level("WARNING"):
        result = service.pull()

    assert result.transactions_loaded == 3
    assert result.accounts_loaded == 2
    assert result.balances_loaded == 2
    assert result.ingestion_state == "committed"
    assert result.ack_state == "unknown"
    assert result.complete is False
    assert any("ack" in r.message.lower() for r in caplog.records)


@pytest.mark.usefixtures("mock_sync_refresh")
def test_pull_with_institution_resolves_to_connection_id(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    connection_id = str(mock_client.get_data.return_value.sources[0].connection_id)
    mock_client.list_institutions.return_value = [
        ConnectedInstitution(
            id=connection_id,
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
        provider_item_id=None,
        reset_cursor=False,
        provider=None,
        connection_id=connection_id,
        history_start=None,
    )


def test_pull_with_unknown_institution_raises(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    mock_client.list_institutions.return_value = []
    service = SyncService(client=mock_client, db=db, loader=loader)
    with pytest.raises(UserError, match="Connection not found") as exc_info:
        service.pull(institution="UnknownBank")
    assert exc_info.value.code == error_codes.MUTATION_NOT_FOUND


@pytest.mark.usefixtures("mock_sync_refresh")
def test_pull_with_provider_item_id_skips_resolution(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    service = SyncService(client=mock_client, db=db, loader=loader)
    service.pull(provider_item_id="item_chase_abc")
    mock_client.list_institutions.assert_not_called()
    mock_client.trigger_sync.assert_called_once_with(
        provider_item_id="item_chase_abc",
        reset_cursor=False,
        provider=None,
        connection_id=None,
        history_start=None,
    )


def test_pull_rejects_both_institution_and_provider_item_id(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    service = SyncService(client=mock_client, db=db, loader=loader)
    with pytest.raises(UserError, match="Choose one institution") as exc_info:
        service.pull(institution="Chase", provider_item_id="item_x")
    assert exc_info.value.code == error_codes.MUTATION_INVALID_INPUT


@pytest.mark.usefixtures("mock_sync_refresh")
def test_pull_with_force_passes_reset_cursor(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    service = SyncService(client=mock_client, db=db, loader=loader)
    service.pull(force=True)
    mock_client.trigger_sync.assert_called_once_with(
        provider_item_id=None,
        reset_cursor=True,
        provider=None,
        connection_id=None,
        history_start=None,
    )


def test_pull_loads_investments_and_resolves_securities(
    db: Database,
    loader: PlaidExtractor,
    mock_investments_client: MagicMock,
) -> None:
    """A pull that carries investment arrays loads them and resolves securities.

    Resolution is a distinct, reviewable stage (R12) — its per-outcome counts
    must appear in the envelope, not just a bindings side effect.
    """
    service = SyncService(client=mock_investments_client, db=db, loader=loader)
    result = service.pull(refresh=False)

    # Counts from the Task 4 loader (plaid_investments_sync_response.yaml):
    # 3 distinct provider-scoped securities, 4 investment transactions, 3
    # holdings — established independently by test_plaid_investments_loader.py.
    assert result.securities_loaded == 3
    assert result.investment_transactions_loaded == 4
    assert result.holdings_loaded == 3
    bindings = db.execute(
        "SELECT COUNT(*) FROM app.security_links WHERE status = 'accepted'"
    ).fetchone()
    assert bindings is not None and bindings[0] >= 2  # resolver ran post-load
    # Stage-one (identity) outcomes are first-class in the envelope (R12).
    # SecurityResolver explicitly coalesces every raw.plaid_securities row
    # sharing one security_id into ONE resolver candidate (merging ticker/
    # name/cusip/isin so a non-null value from either institution's row
    # wins) — so the fixture's two "sec_dup" rows (same security_id, but
    # DIFFERENT institution_id/institution_security_id per institution, per
    # test_colliding_provider_ids_stay_distinct in the loader tests) collapse
    # to ONE candidate for identity resolution, even though both raw rows
    # persist distinctly in raw.plaid_securities and their institution refs
    # are realistically populated (Plaid's normal shape for a security held
    # at two brokerages) — collapse holds regardless of whether those fields
    # are null, because the resolver groups by security_id, not because
    # DISTINCT happens to dedup an all-null projection. 3 raw rows -> 2
    # resolver candidates (sec_aapl, sec_dup).
    assert sum(result.security_resolution.values()) == 2


def test_pull_resolves_raw_securities_left_unbound_by_an_earlier_pull(
    mock_client: MagicMock,
    db: Database,
    loader: PlaidExtractor,
    sync_data: PlaidSyncDataResponse,
) -> None:
    """Self-heal: an empty securities delta must not skip resolution.

    Pull #1 loaded raw securities and then the resolver raised, so every
    investment row landed in core with ``security_id = NULL``. Pull #2 carries
    no securities of its own — but ``resolve_all()`` reads the whole
    ``raw.plaid_securities`` table, so gating it on this pull's delta strands
    the earlier rows unbound forever, and the cost-basis engine drops every
    event on them. The ``stg_plaid__securities`` docstring's promise that "NULL
    here ... self-heals on the next sync" only holds if resolution always runs.
    """
    assert not sync_data.securities  # the non-investment fixture: empty delta
    db.execute(
        """
        INSERT INTO raw.plaid_securities (
            security_id, security_name, security_type, is_cash_equivalent,
            iso_currency_code, source_file, source_origin
        ) VALUES ('sec_stranded', 'Obscure Widget Corp', 'equity', FALSE,
                  'USD', 'sync_j0', 'item_1')
        """
    )

    service = SyncService(client=mock_client, db=db, loader=loader)
    result = service.pull(refresh=False)

    assert result.security_resolution == {"minted": 1}
    bound = db.execute(
        "SELECT security_id FROM app.security_links "
        "WHERE ref_value = 'sec_stranded' AND status = 'accepted'"
    ).fetchone()
    assert bound is not None


def test_pull_security_resolution_failure_reports_setup_error_after_commit(
    db: Database,
    loader: PlaidExtractor,
    mock_investments_client: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A resolver failure keeps Raw durable and reports failed local setup.

    Securities cannot safely enter cost-basis calculations without identity.
    The durable phase state must expose this recovery obligation while the
    service keeps the already committed provider rows available.
    """
    with patch(
        "moneybin.services.sync_service.SecurityResolver",
        side_effect=RuntimeError("boom"),
    ):
        service = SyncService(client=mock_investments_client, db=db, loader=loader)
        with caplog.at_level("WARNING"):
            result = service.pull(refresh=False)

    assert result.securities_loaded == 3
    assert result.security_resolution == {}
    assert result.ingestion_state == "committed"
    assert result.setup_state == "failed"
    assert result.setup_error_code == "local_setup_failed"
    assert result.complete is False
    assert any("local setup requires repair" in r.message for r in caplog.records)
    count = db.execute("SELECT COUNT(*) FROM raw.plaid_securities").fetchone()
    assert count is not None and count[0] == 3


def test_pull_flags_manual_plaid_overlap(
    db: Database,
    loader: PlaidExtractor,
    mock_investments_client: MagicMock,
) -> None:
    """An account carrying both manual and Plaid investment rows is flagged.

    Drives the real sequence: a first pull resolves Plaid's provider-native
    account_id to a canonical one in `app.account_links`; the user then
    hand-records a trade against that canonical account; the next pull sees
    both sources on one account. Manual rows store canonical ids and raw Plaid
    rows store provider-native ids, so the overlap join only matches once the
    link exists — which is why the manual row must be keyed on the canonical
    id, not on Plaid's 'acc_1'.
    """
    service = SyncService(client=mock_investments_client, db=db, loader=loader)
    service.pull(refresh=False)

    link = db.execute(
        """
        SELECT account_id FROM app.account_links
        WHERE status = 'accepted' AND ref_kind = 'source_native'
          AND source_type = 'plaid' AND ref_value = 'acc_1'
        """
    ).fetchone()
    assert link is not None, "first pull must resolve acc_1 to a canonical account"
    canonical_account_id = link[0]

    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions (
            source_transaction_id, import_id, account_id, type, trade_date, created_by
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        ["manual_test1", "imp_test1", canonical_account_id, "buy", "2026-07-01", "cli"],
    )

    result = service.pull(refresh=False)
    assert result.investment_source_overlap_accounts == [canonical_account_id]


def test_holdings_only_pull_flags_manual_overlap(
    db: Database,
    loader: PlaidExtractor,
    mock_investments_client: MagicMock,
    investments_sync_data: PlaidSyncDataResponse,
) -> None:
    holdings_only = investments_sync_data.model_copy(
        update={"investment_transactions": []}
    )
    _set_v2_job(mock_investments_client, holdings_only)
    service = SyncService(client=mock_investments_client, db=db, loader=loader)
    service.pull(refresh=False)
    link = db.execute("""
        SELECT account_id FROM app.account_links
        WHERE status = 'accepted' AND ref_kind = 'source_native'
          AND source_type = 'plaid' AND ref_value = 'acc_1'
    """).fetchone()
    assert link is not None
    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions (
            source_transaction_id, import_id, account_id, type, trade_date, created_by
        ) VALUES ('manual_holdings', 'imp_holdings', ?, 'buy', '2026-07-01', 'cli')
    """,
        [link[0]],
    )
    assert db.execute(
        "SELECT COUNT(*) FROM raw.plaid_investment_transactions"
    ).fetchone() == (0,)
    result = service.pull(refresh=False)
    assert result.investment_source_overlap_accounts == [link[0]]


@pytest.mark.usefixtures("mock_sync_refresh")
def test_link_new_institution_auto_pulls(
    mock_client: MagicMock,
    db: Database,
    loader: PlaidExtractor,
    sync_data: PlaidSyncDataResponse,
) -> None:
    mock_client.initiate_link.return_value = LinkInitiateResponse(
        session_id="sess_x",
        link_url="https://hosted.plaid.com/link/x",
        link_type="widget_flow",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    mock_client.poll_link_status.return_value = LinkStatusResponse(
        session_id="sess_x",
        status="linked",
        provider_item_id="item_chase_abc",
        institution_name="Chase",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    service = SyncService(client=mock_client, db=db, loader=loader)
    result = service.link(auto_pull=True)
    assert result.provider_item_id == "item_chase_abc"
    assert result.institution_name == "Chase"
    assert result.pull_result is not None
    assert result.pull_result.transactions_loaded == 3
    mock_client.trigger_sync.assert_called_once_with(
        provider_item_id="item_chase_abc",
        reset_cursor=False,
        provider=None,
        connection_id=None,
        history_start=None,
    )


@pytest.mark.parametrize("provider", ["plaid", "simplefin"])
@pytest.mark.parametrize("selector", [None, " \t\n"])
def test_link_rejects_missing_terminal_selector_before_auto_pull(
    db: Database,
    loader: PlaidExtractor,
    monkeypatch: pytest.MonkeyPatch,
    provider: str,
    selector: str | None,
) -> None:
    client = SyncClient(
        server_url="https://synthetic.invalid", secret_store=MagicMock(spec=SecretStore)
    )
    field = "connection_id" if provider == "simplefin" else "provider_item_id"
    monkeypatch.setattr(
        client,
        "_authed_request",
        MagicMock(
            return_value=httpx.Response(
                200,
                json={
                    "session_id": "synthetic",
                    "status": "linked",
                    "provider": provider,
                    field: selector,
                    "institution_name": "private-synthetic-institution",
                    "expiration": "2026-10-01T12:00:00Z",
                },
            )
        ),
    )
    monkeypatch.setattr(client, "_sleep", MagicMock())
    service = SyncService(client=client, db=db, loader=loader)
    monkeypatch.setattr(
        service,
        "initiate_link",
        MagicMock(
            return_value=LinkInitiateResponse(
                session_id="synthetic",
                link_url="https://synthetic.invalid/link",
                link_type="widget_flow",
                expiration=datetime(2026, 10, 1, tzinfo=UTC),
            )
        ),
    )
    pull = MagicMock(return_value=None)
    monkeypatch.setattr(service, "pull", pull)

    with pytest.raises(SyncAPIError, match="Invalid link status from broker") as raised:
        service.link(provider=provider)

    pull.assert_not_called()
    assert "private-synthetic-institution" not in str(raised.value)
    assert raised.value.__suppress_context__


def test_link_simplefin_auto_pull_uses_completed_connection(
    mock_client: MagicMock,
    db: Database,
    loader: PlaidExtractor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client.initiate_link.return_value = LinkInitiateResponse(
        session_id="synthetic",
        link_url="https://synthetic.invalid/link",
        link_type="widget_flow",
        expiration=datetime(2026, 10, 1, tzinfo=UTC),
    )
    connection_id = "00000000-0000-4000-8000-000000000123"
    mock_client.poll_link_status.return_value = LinkStatusResponse(
        session_id="synthetic",
        status="linked",
        provider="simplefin",
        connection_id=connection_id,
        expiration=datetime(2026, 10, 1, tzinfo=UTC),
    )
    service = SyncService(client=mock_client, db=db, loader=loader)
    pull = MagicMock(return_value=None)
    monkeypatch.setattr(service, "pull", pull)

    result = service.link(provider="simplefin")

    pull.assert_called_once_with(provider="simplefin", connection_id=connection_id)
    assert result.status == "linked"
    assert result.connection_id == connection_id


def test_link_no_pull_returns_without_pull_result(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    mock_client.initiate_link.return_value = LinkInitiateResponse(
        session_id="sess_x",
        link_url="https://hosted.plaid.com/link/x",
        link_type="widget_flow",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    mock_client.poll_link_status.return_value = LinkStatusResponse(
        session_id="sess_x",
        status="linked",
        provider_item_id="item_new",
        institution_name="Bank",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    service = SyncService(client=mock_client, db=db, loader=loader)
    result = service.link(auto_pull=False)
    assert result.pull_result is None
    mock_client.trigger_sync.assert_not_called()


def test_link_keeps_connection_result_when_default_auto_pull_raises(
    mock_client: MagicMock,
    db: Database,
    loader: PlaidExtractor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed follow-up pull leaves the completed link available to the CLI."""
    mock_client.initiate_link.return_value = LinkInitiateResponse(
        session_id="sess_x",
        link_url="https://hosted.plaid.com/link/x",
        link_type="widget_flow",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    mock_client.poll_link_status.return_value = LinkStatusResponse(
        session_id="sess_x",
        status="linked",
        provider_item_id="item_new",
        institution_name="Bank",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    service = SyncService(client=mock_client, db=db, loader=loader)
    pull = MagicMock(side_effect=RuntimeError("unavailable"))
    monkeypatch.setattr(service, "pull", pull)

    result = service.link(auto_pull=True)

    pull.assert_called_once_with(provider_item_id="item_new")
    assert result.provider_item_id == "item_new"
    assert result.institution_name == "Bank"
    assert result.pull_result is None


def test_link_re_auth_resolves_institution_name(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
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
    mock_client.initiate_link.return_value = LinkInitiateResponse(
        session_id="sess_x",
        link_url="https://hosted.plaid.com/link/x",
        link_type="widget_flow",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    mock_client.poll_link_status.return_value = LinkStatusResponse(
        session_id="sess_x",
        status="linked",
        provider_item_id="item_existing",
        institution_name="Chase",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    service = SyncService(client=mock_client, db=db, loader=loader)
    service.link(institution="Chase", auto_pull=False)
    mock_client.initiate_link.assert_called_once_with(
        provider="plaid",
        provider_item_id="item_existing",
        return_to=None,
    )


def test_link_falls_through_to_new_when_institution_not_matched(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    """Unknown institution name falls through to new-connection flow.

    Per design Section 8: an unknown institution name is a new-connection
    intent, not an error — let the server's Link flow name the institution.
    """
    mock_client.list_institutions.return_value = []  # no existing connections
    mock_client.initiate_link.return_value = LinkInitiateResponse(
        session_id="sess_x",
        link_url="https://hosted.plaid.com/link/x",
        link_type="widget_flow",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    mock_client.poll_link_status.return_value = LinkStatusResponse(
        session_id="sess_x",
        status="linked",
        provider_item_id="item_new",
        institution_name="Bank",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    service = SyncService(client=mock_client, db=db, loader=loader)
    service.link(institution="Wells Fargo", auto_pull=False)
    # provider_item_id is None — new-connection flow, not update mode
    mock_client.initiate_link.assert_called_once_with(
        provider="plaid",
        provider_item_id=None,
        return_to=None,
    )


def test_link_invokes_on_initiate_callback_before_polling(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    """on_initiate fires after initiate_link, before polling.

    The CLI uses on_initiate to print link_url + open the browser. Verify
    the service invokes it between initiate_link and poll_link_status.
    """
    initiate_resp = LinkInitiateResponse(
        session_id="sess_x",
        link_url="https://hosted.plaid.com/link/x",
        link_type="widget_flow",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    mock_client.initiate_link.return_value = initiate_resp
    mock_client.poll_link_status.return_value = LinkStatusResponse(
        session_id="sess_x",
        status="linked",
        provider_item_id="item_new",
        institution_name="Bank",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    captured: list[LinkInitiateResponse] = []
    service = SyncService(client=mock_client, db=db, loader=loader)
    service.link(auto_pull=False, on_initiate=captured.append)
    assert captured == [initiate_resp]


def test_resolve_institution_raises_on_ambiguous_name(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    """Two connections sharing institution_name must not silently map to one.

    The message must be self-sufficient: it names both connection ids and
    linked timestamps, then points to an exact connection selector.
    """
    mock_client.list_institutions.return_value = [
        ConnectedInstitution(
            id="u1",
            provider_item_id="item_a",
            provider="plaid",
            institution_name="Chase",
            status="active",
            created_at=datetime(2026, 1, 5, 9, 0, tzinfo=UTC),
        ),
        ConnectedInstitution(
            id="u2",
            provider_item_id="item_b",
            provider="plaid",
            institution_name="Chase",
            status="active",
            created_at=datetime(2026, 2, 10, 14, 30, tzinfo=UTC),
        ),
    ]
    service = SyncService(client=mock_client, db=db, loader=loader)
    with pytest.raises(UserError, match="Multiple connections match") as exc_info:
        service.pull(institution="Chase")
    assert exc_info.value.code == error_codes.SYNC_INSTITUTION_AMBIGUOUS
    message = str(exc_info.value)
    assert "u1 (linked 2026-01-05)" in message
    assert "u2 (linked 2026-02-10)" in message
    assert "--connection-id or connection_id" in message


def test_list_connections_returns_views_with_guidance(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
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
    assert "this connection" in schwab.guidance
    assert "Schwab" not in schwab.guidance
    assert "sync link" in schwab.guidance


def test_list_connections_threads_error_code(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
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
    assert "re-authentication" in bank_a.guidance
    assert "BankA" not in bank_a.guidance

    assert bank_b.error_code is None
    # Unknown/absent error code falls back to generic guidance
    assert bank_b.guidance is not None
    assert "sync link" in bank_b.guidance


def test_list_connections_unrecognized_error_code_falls_back_to_generic_guidance(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
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

    assert bank_a.error_code == "provider_error"
    assert "WEIRD_NEW_CODE" not in bank_a.model_dump_json()
    assert bank_a.guidance is not None
    assert "sync link" in bank_a.guidance


def test_disconnect_resolves_institution_and_calls_client(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    account = SyncAccountScope.model_validate({
        "account_id": "account_a",
        "native_scope": {"kind": "plaid_item", "provider_item_id": "item_a"},
        "name": "Checking",
    })
    mock_client.list_institutions.return_value = [
        ConnectedInstitution(
            id=DISCONNECT_A_ID,
            provider_item_id="item_a",
            provider="plaid",
            institution_name="Chase",
            status="active",
            created_at=datetime(2026, 3, 15, tzinfo=UTC),
            credential_generation=2,
            accounts=[account],
        ),
    ]
    mock_client.disconnect.return_value = SyncDisconnectResponse(
        connection_id=UUID(DISCONNECT_A_ID),
        credential_generation=2,
        accounts=[account],
        remote_revocation="revoked",
    )
    service = SyncService(client=mock_client, db=db, loader=loader)
    remote_before = _remote_disconnects()
    service.disconnect(institution="Chase")
    assert _remote_disconnects() == remote_before + 1
    mock_client.disconnect.assert_called_once_with(
        DISCONNECT_A_ID, credential_generation=2, accounts=[account]
    )


def test_disconnect_confirmed_verifies_live_connection_before_client_delete(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    connection = ConnectedInstitution(
        id=DISCONNECT_A_ID,
        provider_item_id="item_a",
        provider="plaid",
        institution_name="Chase",
        status="active",
        created_at=datetime(2026, 3, 15, tzinfo=UTC),
    )
    mock_client.list_institutions.return_value = [connection]
    service = SyncService(client=mock_client, db=db, loader=loader)
    verified: list[ConnectedInstitution] = []

    def verify(live: ConnectedInstitution) -> None:
        mock_client.disconnect.assert_not_called()
        verified.append(live)

    remote_before = _remote_disconnects()
    result = service.disconnect_confirmed(institution="Chase", verify=verify)

    assert _remote_disconnects() == remote_before + 1
    assert result.id == connection.id
    assert result.provider_item_id == connection.provider_item_id
    assert result.remote_revocation == "revoked"
    assert verified == [connection]
    mock_client.disconnect.assert_called_once_with(
        DISCONNECT_A_ID, credential_generation=1, accounts=[]
    )


def test_disconnect_confirmed_does_not_delete_when_live_verification_fails(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    mock_client.list_institutions.return_value = [
        ConnectedInstitution(
            id=DISCONNECT_A_ID,
            provider_item_id="item_a",
            provider="plaid",
            institution_name="Chase",
            status="active",
            created_at=datetime(2026, 3, 15, tzinfo=UTC),
        ),
    ]
    service = SyncService(client=mock_client, db=db, loader=loader)

    def refuse(_: ConnectedInstitution) -> None:
        raise ValueError("confirmation mismatch")

    with pytest.raises(ValueError, match="confirmation mismatch"):
        service.disconnect_confirmed(institution="Chase", verify=refuse)

    mock_client.disconnect.assert_not_called()


def test_disconnect_unknown_institution_raises(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    mock_client.list_institutions.return_value = []
    service = SyncService(client=mock_client, db=db, loader=loader)
    with pytest.raises(UserError, match="no connected institution") as exc_info:
        service.disconnect(institution="UnknownBank")
    assert exc_info.value.code == error_codes.MUTATION_NOT_FOUND


def _two_items_same_institution() -> list[ConnectedInstitution]:
    """Two connections sharing an institution name (post-relink), one per item."""
    return [
        ConnectedInstitution(
            id=DISCONNECT_A_ID,
            provider_item_id="item_a",
            provider="plaid",
            institution_name="Chase",
            status="revoked",
            created_at=datetime(2026, 1, 5, tzinfo=UTC),
        ),
        ConnectedInstitution(
            id=DISCONNECT_B_ID,
            provider_item_id="item_b",
            provider="plaid",
            institution_name="Chase",
            status="active",
            created_at=datetime(2026, 6, 1, tzinfo=UTC),
        ),
    ]


def test_plan_disconnect_by_provider_item_id_resolves_among_duplicates(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    """provider_item_id targets one exact item without hitting the name ambiguity guard."""
    mock_client.list_institutions.return_value = _two_items_same_institution()
    service = SyncService(client=mock_client, db=db, loader=loader)

    resolved = service.plan_disconnect(provider_item_id="item_b")

    assert resolved.id == DISCONNECT_B_ID
    assert resolved.provider_item_id == "item_b"


def test_disconnect_by_provider_item_id_deletes_the_correct_internal_id(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    """The client is called with the resolved internal id, not the item id."""
    mock_client.list_institutions.return_value = _two_items_same_institution()
    service = SyncService(client=mock_client, db=db, loader=loader)

    result = service.disconnect(provider_item_id="item_a")

    assert result.id == DISCONNECT_A_ID
    mock_client.disconnect.assert_called_once_with(
        DISCONNECT_A_ID, credential_generation=1, accounts=[]
    )


def test_plan_disconnect_unknown_provider_item_id_raises(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    mock_client.list_institutions.return_value = _two_items_same_institution()
    service = SyncService(client=mock_client, db=db, loader=loader)
    with pytest.raises(
        UserError, match="no connected institution with provider_item_id"
    ) as exc_info:
        service.plan_disconnect(provider_item_id="item_missing")
    assert exc_info.value.code == error_codes.MUTATION_NOT_FOUND


def test_plan_disconnect_rejects_both_institution_and_provider_item_id(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    service = SyncService(client=mock_client, db=db, loader=loader)
    with pytest.raises(UserError, match="mutually exclusive") as exc_info:
        service.plan_disconnect(institution="Chase", provider_item_id="item_a")
    assert exc_info.value.code == error_codes.MUTATION_INVALID_INPUT
    mock_client.list_institutions.assert_not_called()


def test_plan_disconnect_requires_institution_or_provider_item_id(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    service = SyncService(client=mock_client, db=db, loader=loader)
    with pytest.raises(UserError, match="required to disconnect") as exc_info:
        service.plan_disconnect()
    assert exc_info.value.code == error_codes.SYNC_INSTITUTION_REQUIRED


def test_ambiguous_institution_error_names_connection_id(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    """The dead-end message must name a parameter the caller can actually use."""
    mock_client.list_institutions.return_value = _two_items_same_institution()
    service = SyncService(client=mock_client, db=db, loader=loader)
    with pytest.raises(UserError, match="connection_id") as exc_info:
        service.plan_disconnect(institution="Chase")
    assert exc_info.value.code == error_codes.SYNC_INSTITUTION_AMBIGUOUS


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
        loader: PlaidExtractor,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from moneybin.orchestration.refresh import RefreshResult
        from moneybin.services import sync_service as mod

        calls = 0

        def fake_refresh(_db: object, **_kwargs: object) -> RefreshResult:
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

    def test_pull_reports_a_transfer_the_refresh_retired(
        self,
        mock_client: MagicMock,
        db: Database,
        loader: PlaidExtractor,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A pull runs the matcher, so it can reverse a transfer the user accepted.

        `pull` calls `_refresh` with no `steps`, which is the full cascade
        including `match`, and the match step now reconciles — so an ordinary
        sync can retire a standing transfer. Reporting only the transform
        fields would let that land inside a routine "sync succeeded" with
        nothing pointing at the reversal or the way back.
        """
        from moneybin.orchestration.refresh import RefreshResult
        from moneybin.services import sync_service as mod

        def fake_refresh(_db: object, **_kwargs: object) -> RefreshResult:
            return RefreshResult(
                applied=True, duration_seconds=0.05, transfers_retired=2
            )

        monkeypatch.setattr(mod, "_refresh", fake_refresh)
        service = SyncService(client=mock_client, db=db, loader=loader)
        result = service.pull()

        assert result.transfers_retired == 2

    def test_pull_forwards_progress_to_refresh(
        self,
        mock_client: MagicMock,
        db: Database,
        loader: PlaidExtractor,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from moneybin.orchestration.refresh import RefreshResult
        from moneybin.services import sync_service as mod

        received: list[object] = []

        def fake_refresh(
            _db: object, *, progress: object, **_kwargs: object
        ) -> RefreshResult:
            received.append(progress)
            return RefreshResult(applied=True, duration_seconds=0.05)

        monkeypatch.setattr(mod, "_refresh", fake_refresh)
        events: list[str] = []
        service = SyncService(client=mock_client, db=db, loader=loader)

        service.pull(progress=lambda event: events.append(event.stage))

        assert received
        assert events[:3] == [
            "Syncing institutions",
            "Loading synced data",
            "Refreshing reports",
        ]

    def test_pull_no_refresh_skips_pipeline(
        self,
        mock_client: MagicMock,
        db: Database,
        loader: PlaidExtractor,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from moneybin.orchestration.refresh import RefreshResult
        from moneybin.services import sync_service as mod

        calls = 0

        def fake_refresh(_db: object, **_kwargs: object) -> RefreshResult:
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
        loader: PlaidExtractor,
        sync_data: PlaidSyncDataResponse,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """No raw rows landed → nothing to refresh."""
        from moneybin.orchestration.refresh import RefreshResult
        from moneybin.services import sync_service as mod

        calls = 0

        def fake_refresh(_db: object, **_kwargs: object) -> RefreshResult:
            nonlocal calls
            calls += 1
            return RefreshResult(applied=True, duration_seconds=0.0)

        monkeypatch.setattr(mod, "_refresh", fake_refresh)

        empty_client = MagicMock()
        empty_data = sync_data.model_copy(
            update={"accounts": [], "transactions": [], "balances": []}
        )
        _set_v2_job(empty_client, empty_data, job_label="job_empty")

        service = SyncService(client=empty_client, db=db, loader=loader)
        result = service.pull()

        assert calls == 0
        assert result.transforms_applied is False
        assert result.transforms_error is None

    def test_unchanged_new_job_refreshes_advanced_raw_metadata(
        self,
        db: Database,
        loader: PlaidExtractor,
        sync_data: PlaidSyncDataResponse,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Zero content changes still advance the raw observations models read."""
        snapshots = sync_data.model_copy(
            update={"transactions": [], "removed_transactions": []}
        )
        client = MagicMock()
        _set_v2_job(client, snapshots, job_label="unchanged-first")
        service = SyncService(client=client, db=db, loader=loader)
        service.pull(refresh=False)
        before = db.execute(
            "SELECT source_file FROM raw.plaid_accounts ORDER BY account_id"
        ).fetchall()
        observed_sources: list[str] = []

        def refresh_written_data(
            database: Database, **_kwargs: object
        ) -> RefreshResult:
            observed_sources.extend(
                row[0]
                for row in database.execute(
                    "SELECT source_file FROM raw.plaid_accounts ORDER BY account_id"
                ).fetchall()
            )
            return RefreshResult(applied=True, duration_seconds=0.0)

        monkeypatch.setattr(sync_service, "_refresh", refresh_written_data)
        _set_v2_job(client, snapshots, job_label="unchanged-second")
        result = service.pull()
        after = db.execute(
            "SELECT source_file FROM raw.plaid_accounts ORDER BY account_id"
        ).fetchall()

        assert result.accounts_loaded == result.balances_loaded == 0
        assert result.transactions_loaded == 0
        assert before != after
        assert observed_sources == [row[0] for row in after]
        assert result.transforms_applied
        assert result.refresh_state == "complete"

    def test_pull_refreshes_when_only_a_holdings_receipt_landed(
        self,
        db: Database,
        loader: PlaidExtractor,
        sync_data: PlaidSyncDataResponse,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Snapshot-receipt-only sync: a broker reporting NOTHING is a state change.

        A liquidated broker returns no holdings, no transactions, no balances —
        the receipt is the only durable write in the pull. It is also the sole
        input core.dim_holdings reads to decide which snapshot is newest, so a
        gate that ignores it skips the refresh and leaves dim_holdings showing
        the previous, non-empty snapshot: the fully-liquidated broker still
        reads as holding its old positions, which is precisely the phantom the
        receipt was added to expose.
        """
        from moneybin.orchestration.refresh import RefreshResult
        from moneybin.services import sync_service as mod

        calls = 0

        def fake_refresh(_db: object, **_kwargs: object) -> RefreshResult:
            nonlocal calls
            calls += 1
            return RefreshResult(applied=True, duration_seconds=0.0)

        monkeypatch.setattr(mod, "_refresh", fake_refresh)

        # Nothing loadable in the payload, but the item declares an investments
        # window on a completed result — so the loader writes its receipt.
        receipt_only = sync_data.model_copy(
            update={
                "accounts": [],
                "transactions": [],
                "balances": [],
                "removed_transactions": [],
                "securities": [],
                "investment_transactions": [],
                "investment_holdings": [],
                "metadata": sync_data.metadata.model_copy(
                    update={
                        "institutions": [
                            inst.model_copy(
                                update={"transactions_window_start": date(2024, 7, 8)}
                            )
                            for inst in sync_data.metadata.institutions
                        ]
                    }
                ),
            }
        )
        client = MagicMock()
        _set_v2_job(client, receipt_only, job_label="job_liquidated")

        service = SyncService(client=client, db=db, loader=loader)
        result = service.pull()

        receipts = db.execute(
            "SELECT COUNT(*) FROM raw.plaid_investment_holdings_snapshots"
        ).fetchone()
        assert receipts is not None and receipts[0] > 0, (
            "no receipt written — bad setup"
        )
        assert calls == 1
        assert result.transforms_applied is True

    def test_pull_refreshes_when_only_price_observations_landed(
        self,
        db: Database,
        loader: PlaidExtractor,
        sync_data: PlaidSyncDataResponse,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A price-only write is a durable raw write and must gate the refresh.

        raw.security_prices is append-only, and its rows only reach
        core.fct_security_prices — and from there dim_holdings' market_value —
        through the post-pull SQLMesh refresh. Today closes ride along with a
        securities upsert, so the securities term happens to cover them; a
        price source that does not ride along would write rows that never
        propagate. The gate counts every durable raw write, so this term is
        not optional.
        """
        from moneybin.extractors.plaid.extractor import LoadResult
        from moneybin.orchestration.refresh import RefreshResult
        from moneybin.services import sync_service as mod

        calls = 0

        def fake_refresh(_db: object, **_kwargs: object) -> RefreshResult:
            nonlocal calls
            calls += 1
            return RefreshResult(applied=True, duration_seconds=0.0)

        monkeypatch.setattr(mod, "_refresh", fake_refresh)

        # A price-only LoadResult is not reachable through today's loader (see
        # the docstring), so the collaborator is stubbed to produce the shape
        # the gate must survive.
        def fake_load(*_args: object, **_kwargs: object) -> LoadResult:
            return LoadResult(
                accounts_loaded=0,
                transactions_loaded=0,
                balances_loaded=0,
                security_prices_loaded=3,
                rows_written=3,
            )

        monkeypatch.setattr(loader, "load", fake_load)

        price_only = sync_data.model_copy(
            update={
                "accounts": [],
                "transactions": [],
                "balances": [],
                "removed_transactions": [],
            }
        )
        client = MagicMock()
        _set_v2_job(client, price_only, job_label="job_prices")

        service = SyncService(client=client, db=db, loader=loader)
        result = service.pull()

        assert calls == 1, "a price-only pull must still refresh"
        assert result.transforms_applied is True
        assert result.security_prices_loaded == 3, (
            "the count must reach the pull receipt, or the user cannot tell "
            "from the sync output whether the price feed captured anything"
        )

    def test_pull_reports_price_observations_on_the_receipt(
        self,
        db: Database,
        loader: PlaidExtractor,
        mock_client: MagicMock,
    ) -> None:
        """security_prices_loaded rides the ordinary pull receipt beside its siblings."""
        service = SyncService(client=mock_client, db=db, loader=loader)
        result = service.pull(refresh=False)

        assert result.security_prices_loaded == 0, (
            "the base fixture carries no priced securities"
        )
        assert "security_prices_loaded" in result.model_dump(), (
            "the field must be part of the serialized envelope agents read"
        )

    def test_pull_refreshes_when_only_security_resolution_wrote(
        self,
        db: Database,
        loader: PlaidExtractor,
        sync_data: PlaidSyncDataResponse,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Self-heal-only sync: a binding the resolver lands is a state change.

        ``resolve_all()`` deliberately sweeps the WHOLE raw.plaid_securities
        table on every pull, so it can bind rows an earlier pull left stranded
        (the resolver raised, or the catalog has since grown a matching CUSIP).
        That self-heal is the only durable write in such a pull: no securities
        came down, no transactions changed, nothing was loaded.

        A gate that counts only LOADED rows skips the refresh on exactly that
        pull, so the binding never reaches core and every investment leg on the
        security keeps its NULL ``security_id`` — the cost-basis engine goes on
        dropping it — until some unrelated future pull happens to change other
        rows. The self-heal the resolver promises is only real if the refresh
        it depends on actually runs.
        """
        from moneybin.orchestration.refresh import RefreshResult
        from moneybin.services import sync_service as mod

        calls = 0

        def fake_refresh(_db: object, **_kwargs: object) -> RefreshResult:
            nonlocal calls
            calls += 1
            return RefreshResult(applied=True, duration_seconds=0.0)

        monkeypatch.setattr(mod, "_refresh", fake_refresh)

        # Stranded by an earlier pull: raw row present, never bound.
        db.execute(
            """
            INSERT INTO raw.plaid_securities (
                security_id, security_name, security_type, is_cash_equivalent,
                iso_currency_code, source_file, source_origin
            ) VALUES ('sec_stranded', 'Obscure Widget Corp', 'equity', FALSE,
                      'USD', 'sync_j0', 'item_1')
            """
        )

        empty_client = MagicMock()
        empty_data = sync_data.model_copy(
            update={
                "accounts": [],
                "transactions": [],
                "balances": [],
                "removed_transactions": [],
                "securities": [],
                "investment_transactions": [],
                "investment_holdings": [],
            }
        )
        _set_v2_job(empty_client, empty_data, job_label="job_selfheal")

        service = SyncService(client=empty_client, db=db, loader=loader)
        result = service.pull()

        assert result.security_resolution == {"minted": 1}, "bad setup: no bind"
        assert calls == 1
        assert result.transforms_applied is True

    def test_pull_skips_refresh_when_resolution_bound_nothing_new(
        self,
        db: Database,
        loader: PlaidExtractor,
        sync_data: PlaidSyncDataResponse,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The converse: a re-resolve that writes nothing must NOT refresh.

        ``adopted`` is the steady state — every ref already bound, so the
        resolver re-derives the same answer and inserts nothing. Counting the
        OUTCOME rather than the WRITE would fire a full transform run on every
        cash-only pull for the rest of the database's life, which is the exact
        cost the rows-changed gate exists to avoid.
        """
        from moneybin.orchestration.refresh import RefreshResult
        from moneybin.services import sync_service as mod

        calls = 0

        def fake_refresh(_db: object, **_kwargs: object) -> RefreshResult:
            nonlocal calls
            calls += 1
            return RefreshResult(applied=True, duration_seconds=0.0)

        monkeypatch.setattr(mod, "_refresh", fake_refresh)

        db.execute(
            """
            INSERT INTO raw.plaid_securities (
                security_id, security_name, security_type, is_cash_equivalent,
                iso_currency_code, source_file, source_origin
            ) VALUES ('sec_stranded', 'Obscure Widget Corp', 'equity', FALSE,
                      'USD', 'sync_j0', 'item_1')
            """
        )

        empty_payload = sync_data.model_copy(
            update={
                "accounts": [],
                "transactions": [],
                "balances": [],
                "removed_transactions": [],
                "securities": [],
                "investment_transactions": [],
                "investment_holdings": [],
            }
        )
        empty_client = MagicMock()
        _set_v2_job(empty_client, empty_payload, job_label="job_selfheal")

        service = SyncService(client=empty_client, db=db, loader=loader)
        service.pull()  # first pull binds it
        assert calls == 1, "bad setup: the binding pull must refresh"

        _set_v2_job(empty_client, empty_payload, job_label="job_selfheal_second")
        result = service.pull()  # a new job re-derives the same binding

        assert result.security_resolution == {"adopted": 1}
        assert calls == 1, "re-resolving an already-bound security wrote nothing"
        assert result.transforms_applied is False

    def test_pull_refreshes_when_only_removals_landed(
        self,
        db: Database,
        loader: PlaidExtractor,
        sync_data: PlaidSyncDataResponse,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Removal-only sync: deletions are a state change too.

        Guards against a bug where the gate counted only loaded rows: a
        sync that only carried ``removed_transactions`` would delete from
        raw but skip refresh, leaving the deleted row visible in
        core.fct_transactions.
        """
        from moneybin.orchestration.refresh import RefreshResult
        from moneybin.services import sync_service as mod

        calls = 0

        def fake_refresh(_db: object, **_kwargs: object) -> RefreshResult:
            nonlocal calls
            calls += 1
            return RefreshResult(applied=True, duration_seconds=0.04)

        monkeypatch.setattr(mod, "_refresh", fake_refresh)

        # First populate raw with a transaction whose id matches what the
        # next sync will mark removed — otherwise handle_removed_transactions
        # returns 0 and the gate stays closed for legitimate reasons.
        seed_data = sync_data.model_copy(update={"removed_transactions": []})
        seed_client = MagicMock()
        _set_v2_job(seed_client, seed_data, job_label="job_seed")
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
        _set_v2_job(removal_client, removal_data, job_label="job_removal")
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
        loader: PlaidExtractor,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Refresh failure preserves Raw and returns safe local recovery state."""
        from moneybin.orchestration.refresh import RefreshResult
        from moneybin.services import sync_service as mod

        def fake_refresh(_db: object, **_kwargs: object) -> RefreshResult:
            return RefreshResult(
                applied=False, duration_seconds=0.12, error="SQLMeshError"
            )

        monkeypatch.setattr(mod, "_refresh", fake_refresh)
        service = SyncService(client=mock_client, db=db, loader=loader)
        result = service.pull()

        assert result.transactions_loaded == 3
        assert result.transforms_applied is False
        assert result.transforms_duration_seconds == 0.12
        assert result.transforms_error == "Local refresh failed; retry this job locally"
        assert result.refresh_state == "failed"
        assert result.ingestion_state == "committed"
        assert result.complete is False


class TestPullResolvesAccounts:
    """sync.pull() populates app.account_links via AccountResolver (A8).

    Mirrors the import path (A6/A7): the SERVICE layer resolves each loaded
    account to a canonical id, writing the accepted source_native mapping the
    B1 staging translation JOIN keys on. The Plaid native key is the source
    account_id token already stamped on raw.plaid_accounts (unchanged here).
    """

    def test_pull_writes_accepted_source_native_link_per_account(
        self,
        mock_client: MagicMock,
        db: Database,
        loader: PlaidExtractor,
        sync_data: PlaidSyncDataResponse,
    ) -> None:
        """Each Plaid account yields an accepted source_native link.

        The fixture has two accounts (acc_chase_check, acc_chase_save) under
        one institution (provider_item_id=item_chase_abc). After pull(), each
        gets an accepted source_native row keyed by its Plaid account_id, with
        source_origin == the provider_item_id — identical to raw.plaid_accounts
        so the later staging JOIN is total. refresh=False skips the SQLMesh apply.
        """
        service = SyncService(client=mock_client, db=db, loader=loader)
        service.pull(refresh=False)

        rows = db.execute(
            """
            SELECT ref_value, account_id, source_origin
            FROM app.account_links
            WHERE status = 'accepted' AND ref_kind = 'source_native'
              AND source_type = 'plaid'
            ORDER BY ref_value
            """,
        ).fetchall()
        assert [r[0] for r in rows] == ["acc_chase_check", "acc_chase_save"]
        # source_origin must equal the provider_item_id stamped on raw (B1 JOIN key).
        assert all(r[2] == "item_chase_abc" for r in rows)
        # Each resolves to a freshly minted canonical uuid4[:12].
        assert all(len(r[1]) == 12 for r in rows)

    def test_pull_records_persistent_account_id_as_a_strong_ref(
        self,
        mock_client: MagicMock,
        db: Database,
        loader: PlaidExtractor,
    ) -> None:
        """Plaid's persistent_account_id must land as a persistent_token ref.

        ``AccountResolver`` already treats ``persistent_token`` as a strong
        confirmer that auto-adopts (step 1 of ``resolve``); the field was simply
        never handed to it, so every Plaid account minted with the ref column
        empty and cross-connection identity could never fire.
        """
        service = SyncService(client=mock_client, db=db, loader=loader)
        service.pull(refresh=False)

        rows = db.execute(
            """
            SELECT ref_value, account_id
            FROM app.account_links
            WHERE status = 'accepted' AND ref_kind = 'persistent_token'
              AND source_type = 'plaid'
            ORDER BY ref_value
            """,
        ).fetchall()
        assert [r[0] for r in rows] == ["ppa_chase_check_0001", "ppa_chase_save_0002"]

    def test_relinked_account_adopts_the_same_canonical_id(
        self,
        mock_client: MagicMock,
        db: Database,
        loader: PlaidExtractor,
    ) -> None:
        """A relink reissues account_id and item_id; persistent_account_id survives.

        This is the whole reason the field exists, and the failure it prevents
        is the 2026-08-08 incident: Chase relinked, Plaid minted a fresh
        ``account_id`` under a fresh ``provider_item_id``, and with no strong
        ref to confirm identity the resolver minted a *second* canonical
        account for a card the user already had — splitting one ledger in two.

        Both source_native keys must end up pointing at one canonical id.
        """
        service = SyncService(client=mock_client, db=db, loader=loader)
        service.pull(refresh=False)
        before = db.execute(
            "SELECT account_id FROM app.account_links "
            "WHERE status = 'accepted' AND ref_kind = 'source_native' "
            "AND ref_value = 'acc_chase_check'",
        ).fetchone()
        assert before is not None

        _set_v2_job(mock_client, _relinked_payload())
        service.pull(refresh=False)

        after = db.execute(
            "SELECT account_id FROM app.account_links "
            "WHERE status = 'accepted' AND ref_kind = 'source_native' "
            "AND ref_value = 'acc_chase_check_reissued'",
        ).fetchone()
        assert after is not None, "relinked account never resolved"
        assert after[0] == before[0], (
            "relinked account minted a second canonical id instead of adopting "
            "the one its persistent_account_id already confirms"
        )
        # A strong ref adopts silently (step 1 of resolve, ahead of the
        # candidate pass), so the relink must leave the review queue empty —
        # a pending row here means it resolved through a weak rung instead.
        pending = db.execute(
            "SELECT COUNT(*) FROM app.account_link_decisions WHERE status = 'pending'"
        ).fetchone()
        assert pending is not None and pending[0] == 0

    def test_pull_prefers_the_account_name_over_the_official_name(
        self,
        mock_client: MagicMock,
        db: Database,
        loader: PlaidExtractor,
    ) -> None:
        """``name`` distinguishes siblings that share one ``official_name``.

        Plaid's ``official_name`` is the product's marketing label, so two
        different Chase cards both report "Ultimate Rewards®" while ``name``
        keeps them apart. The resolver feeds this string to ``match_account``
        on the name rung of the candidate pass, so a label shared by two
        accounts cannot discriminate between them there.

        Asserts on the ``SourceAccount`` the service constructs — the resolver
        itself is the collaborator boundary here, and its own adoption
        behaviour is covered above.
        """
        captured: list[object] = []

        class _Capturing:
            def __init__(self, *_a: object, **_kw: object) -> None:
                pass

            def accepted_native_account_id(self, src: object) -> None:
                return None

            def resolve(self, src: object, *, in_outer_txn: bool) -> object:
                assert in_outer_txn is True
                captured.append(src)
                return SimpleNamespace(account_id="acct_1", outcome="minted_new")

        _set_v2_job(mock_client, _payload_with_shared_official_name())
        with patch("moneybin.services.sync_service.AccountResolver", _Capturing):
            service = SyncService(client=mock_client, db=db, loader=loader)
            service.pull(refresh=False)

        names = [getattr(src, "account_name", None) for src in captured]
        assert names == ["Freedom Unlimited", "Sapphire Preferred"]

    def test_pull_sets_account_name_is_user_set_from_the_holders_nickname(
        self,
        mock_client: MagicMock,
        db: Database,
        loader: PlaidExtractor,
    ) -> None:
        """``account_name_is_user_set`` follows ``acc.name`` alone.

        Direct-wiring proof for ``_resolve_accounts``: resolver-level tests
        cover what the gate does with the flag, not that this call site
        actually sets it. ``official_name`` (the product's marketing label)
        must not read as authored even when it is all Plaid gives an account.
        """
        captured: list[object] = []

        class _Capturing:
            def __init__(self, *_a: object, **_kw: object) -> None:
                pass

            def accepted_native_account_id(self, src: object) -> None:
                return None

            def resolve(self, src: object, *, in_outer_txn: bool) -> object:
                assert in_outer_txn is True
                captured.append(src)
                return SimpleNamespace(account_id="acct_1", outcome="minted_new")

        _set_v2_job(mock_client, _payload_with_and_without_a_nickname())
        with patch("moneybin.services.sync_service.AccountResolver", _Capturing):
            service = SyncService(client=mock_client, db=db, loader=loader)
            service.pull(refresh=False)

        flags = {
            getattr(src, "source_account_key", None): getattr(
                src, "account_name_is_user_set", None
            )
            for src in captured
        }
        assert flags == {"acc_named": True, "acc_unnamed": False}

    def test_pull_resolver_failure_does_not_fail_the_pull(
        self,
        mock_client: MagicMock,
        db: Database,
        loader: PlaidExtractor,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A resolver failure preserves Raw and reports local setup failure.

        The service must expose the recovery obligation without losing the
        already durable provider rows.
        """
        from moneybin.services import sync_service as mod

        class _Boom:
            def __init__(self, *_a: object, **_kw: object) -> None:
                pass

            def resolve(self, *_a: object, **_kw: object) -> None:
                raise RuntimeError("resolver boom")

        monkeypatch.setattr(mod, "AccountResolver", _Boom)
        service = SyncService(client=mock_client, db=db, loader=loader)
        result = service.pull(refresh=False)

        assert result.accounts_loaded == 2
        assert result.transactions_loaded == 3
        assert result.ingestion_state == "committed"
        assert result.setup_state == "failed"
        assert result.complete is False


# ---------------------------------------------------------------------------
# Purge: `sync disconnect --purge` / `sync_disconnect(state="absent")`
# ---------------------------------------------------------------------------

_CHASE_ITEM = "item_chase_abc"  # the fixture's item: 2 accounts, 3 txns, 2 balances
_OTHER_ITEM = "item_other_xyz"


def _chase_connection(
    status: Literal["active", "error", "revoked"] = "active",
) -> ConnectedInstitution:
    return ConnectedInstitution(
        id=DISCONNECT_A_ID,
        provider_item_id=_CHASE_ITEM,
        provider="plaid",
        institution_name="Chase",
        status=status,
        created_at=datetime(2026, 3, 15, tzinfo=UTC),
    )


def _other_item_payload() -> PlaidSyncDataResponse:
    """A second, unrelated item whose rows a purge of the fixture item must keep."""
    return PlaidSyncDataResponse.model_validate({
        "accounts": [
            {
                "account_id": "acc_other_check",
                "persistent_account_id": "ppa_other_0001",
                "account_type": "depository",
                "account_subtype": "checking",
                "institution_name": "Other Bank",
                "name": "Other Checking",
                "official_name": "Other Total Checking",
                "mask": "9999",
            }
        ],
        "transactions": [
            {
                "transaction_id": "txn_other_1",
                "account_id": "acc_other_check",
                "transaction_date": "2026-04-07",
                "amount": "5.00",
                "description": "OTHER COFFEE",
                "pending": False,
            }
        ],
        "balances": [],
        "removed_transactions": [],
        "metadata": {
            "job_id": "job-other",
            "synced_at": "2026-08-15T12:00:00Z",
            "institutions": [
                {
                    "provider_item_id": _OTHER_ITEM,
                    "institution_name": "Other Bank",
                    "status": "completed",
                    "transaction_count": 1,
                }
            ],
        },
    })


def _pull_both_items(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> SyncService:
    """Pull the fixture item and a second item so a purge has a bystander."""
    service = SyncService(client=mock_client, db=db, loader=loader)
    service.pull(refresh=False)
    other = MagicMock()
    _set_v2_job(other, _other_item_payload())
    SyncService(client=other, db=db, loader=loader).pull(refresh=False)
    return service


def _raw_counts(db: Database, item_id: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in ("plaid_accounts", "plaid_transactions", "plaid_balances"):
        row = db.execute(
            f"SELECT COUNT(*) FROM raw.{table} WHERE source_origin = ?",  # noqa: S608  # test table name
            [item_id],
        ).fetchone()
        counts[table] = int(row[0]) if row else 0
    return counts


def _accepted_link_rows(db: Database, item_id: str) -> list[tuple[str, str]]:
    return [
        (str(r[0]), str(r[1]))
        for r in db.execute(
            "SELECT link_id, account_id FROM app.account_links "
            "WHERE source_type = 'plaid' AND source_origin = ? "
            "AND status = 'accepted' ORDER BY link_id",
            [item_id],
        ).fetchall()
    ]


def _propose_merge(db: Database, provisional: str, candidate: str) -> str:
    from moneybin.repositories.account_link_decisions_repo import (
        AccountLinkDecisionsRepo,
    )

    decision_id = f"dec_{provisional[:4]}_{candidate[:4]}"
    AccountLinkDecisionsRepo(db).insert(
        decision_id=decision_id,
        provisional_account_id=provisional,
        candidate_account_id=candidate,
        confidence_score=0.5,
        match_signals={"signal": "institution_last4", "value": "1234"},
        decided_by="auto",
        actor="system",
        match_reason="institution_last4",
    )
    return decision_id


def _remote_disconnects() -> float:
    return SYNC_DISCONNECT_OUTCOMES.labels(scope="remote")._value.get()  # pyright: ignore[reportPrivateUsage]  # no public counter getter


def _decision_status(db: Database, decision_id: str) -> tuple[str, str]:
    row = db.execute(
        "SELECT status, decided_by FROM app.account_link_decisions "
        "WHERE decision_id = ?",
        [decision_id],
    ).fetchone()
    assert row is not None
    return str(row[0]), str(row[1])


def test_plan_purge_counts_exactly_the_items_rows_and_links(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    """The plan quotes the fixture item's rows per table and nothing of the other's."""
    service = _pull_both_items(mock_client, db, loader)
    mock_client.list_institutions.return_value = [_chase_connection()]

    plan = service.plan_purge(institution="Chase")

    counts = dict(plan.table_counts)
    assert plan.provider_item_id == _CHASE_ITEM
    assert plan.connection == _chase_connection()
    assert counts["raw.plaid_accounts"] == 2
    assert counts["raw.plaid_transactions"] == 3
    assert counts["raw.plaid_balances"] == 2
    assert counts["raw.security_prices"] == 0
    assert plan.rows_to_delete == 7
    assert (
        len(plan.account_link_ids) == 4
    )  # source_native + persistent_token per account
    assert plan.decision_ids == ()
    assert plan.blast_radius["institutions"] == 1
    assert plan.blast_radius["total_rows"] == 7
    assert plan.blast_radius["account_links_reversed"] == 4


def test_purge_confirmed_removes_the_item_and_keeps_the_bystander(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    """Server disconnect, raw deletes, link reversal, and proposal withdrawal in one go."""
    service = _pull_both_items(mock_client, db, loader)
    mock_client.list_institutions.return_value = [_chase_connection()]
    chase_links = _accepted_link_rows(db, _CHASE_ITEM)
    other_links = _accepted_link_rows(db, _OTHER_ITEM)
    chase_account = chase_links[0][1]
    other_account = other_links[0][1]
    withdrawn = _propose_merge(db, chase_account, other_account)
    verified: list[object] = []

    live = service.purge_confirmed(
        provider_item_id=_CHASE_ITEM, verify=verified.append, actor="cli"
    )

    assert verified == [live]
    mock_client.disconnect.assert_called_once_with(
        DISCONNECT_A_ID, credential_generation=1, accounts=[]
    )
    assert live.rows_to_delete == 7
    assert live.decision_ids == (withdrawn,)
    assert _raw_counts(db, _CHASE_ITEM) == {
        "plaid_accounts": 0,
        "plaid_transactions": 0,
        "plaid_balances": 0,
    }
    assert _raw_counts(db, _OTHER_ITEM)["plaid_accounts"] == 1
    assert _raw_counts(db, _OTHER_ITEM)["plaid_transactions"] == 1
    assert _accepted_link_rows(db, _CHASE_ITEM) == []
    assert _accepted_link_rows(db, _OTHER_ITEM) == other_links
    reversed_rows = db.execute(
        "SELECT reversed_by FROM app.account_links "
        "WHERE source_origin = ? AND status = 'reversed'",
        [_CHASE_ITEM],
    ).fetchall()
    assert [str(r[0]) for r in reversed_rows] == ["user"] * 4
    assert _decision_status(db, withdrawn) == ("rejected", "user")


def test_purge_keeps_a_proposal_another_source_still_feeds(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    """An account an OFX statement also feeds is not orphaned, so its proposal stays."""
    from moneybin.repositories.account_links_repo import AccountLinksRepo

    service = _pull_both_items(mock_client, db, loader)
    mock_client.list_institutions.return_value = [_chase_connection()]
    chase_account = _accepted_link_rows(db, _CHASE_ITEM)[0][1]
    other_account = _accepted_link_rows(db, _OTHER_ITEM)[0][1]
    AccountLinksRepo(db).insert(
        link_id="lnk_ofx_same",
        account_id=chase_account,
        ref_kind="source_native",
        ref_value="statement-acct",
        source_type="ofx",
        source_origin="chase_statements",
        decided_by="user",
        actor="cli",
    )
    kept = _propose_merge(db, chase_account, other_account)

    plan = service.plan_purge(provider_item_id=_CHASE_ITEM)

    assert plan.decision_ids == ()
    service.purge_confirmed(provider_item_id=_CHASE_ITEM, verify=lambda _: None)
    assert _decision_status(db, kept) == ("pending", "auto")


def test_purge_reaches_an_item_the_server_no_longer_lists(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    """The orphan a plain disconnect leaves behind is purgeable by its item id."""
    service = _pull_both_items(mock_client, db, loader)
    mock_client.list_institutions.return_value = []

    plan = service.plan_purge(provider_item_id=_CHASE_ITEM)
    assert plan.connection is None
    assert plan.blast_radius["institutions"] == 0
    assert plan.rows_to_delete == 7

    live = service.purge_confirmed(
        provider_item_id=_CHASE_ITEM, verify=lambda _: None, actor="mcp"
    )

    assert live.connection is None
    mock_client.disconnect.assert_not_called()
    assert _raw_counts(db, _CHASE_ITEM)["plaid_transactions"] == 0
    assert _raw_counts(db, _OTHER_ITEM)["plaid_transactions"] == 1


def test_purge_restates_the_tables_it_deleted_from_after_the_commit(
    mock_client: MagicMock,
    db: Database,
    loader: PlaidExtractor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The materialized models read the purged raw rows and the reversed links."""
    service = _pull_both_items(mock_client, db, loader)
    mock_client.list_institutions.return_value = [_chase_connection()]
    calls = spy_restate(
        monkeypatch, lambda: _raw_counts(db, _CHASE_ITEM)["plaid_transactions"]
    )

    service.purge_confirmed(
        provider_item_id=_CHASE_ITEM, verify=lambda _live: None, actor="cli"
    )

    purged = sync_service._PURGE_TABLES  # type: ignore[reportPrivateUsage]  # the list the purge deletes from
    assert calls == [((ACCOUNT_LINKS, *purged, SECURITY_PRICES), "purge", 0)]


def test_a_purge_that_deletes_nothing_restates_nothing(
    mock_client: MagicMock,
    db: Database,
    loader: PlaidExtractor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A connection the server lists but nothing was ever pulled from."""
    service = SyncService(client=mock_client, db=db, loader=loader)
    mock_client.list_institutions.return_value = [_chase_connection()]
    calls = spy_restate(
        monkeypatch, lambda: _raw_counts(db, _CHASE_ITEM)["plaid_transactions"]
    )

    live = service.purge_confirmed(
        provider_item_id=_CHASE_ITEM, verify=lambda _live: None, actor="cli"
    )

    assert live.rows_to_delete == 0
    assert live.account_link_ids == ()
    mock_client.disconnect.assert_called_once()
    assert calls == []


def test_plan_purge_refuses_an_item_with_neither_connection_nor_rows(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    service = _pull_both_items(mock_client, db, loader)
    mock_client.list_institutions.return_value = []
    with pytest.raises(UserError, match="no locally pulled rows") as exc_info:
        service.plan_purge(provider_item_id="item_never_seen")
    assert exc_info.value.code == error_codes.MUTATION_NOT_FOUND


def test_plan_purge_by_name_needs_the_server_and_hints_at_the_item_id(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    """A name only resolves through the server; the hint names the orphan route."""
    service = _pull_both_items(mock_client, db, loader)
    mock_client.list_institutions.return_value = []
    with pytest.raises(UserError, match="no connected institution") as exc_info:
        service.plan_purge(institution="Chase")
    assert exc_info.value.code == error_codes.MUTATION_NOT_FOUND
    assert "provider_item_id" in (exc_info.value.hint or "")


def test_plan_purge_rejects_both_or_neither_selector(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    service = SyncService(client=mock_client, db=db, loader=loader)
    with pytest.raises(UserError, match="mutually exclusive"):
        service.plan_purge(institution="Chase", provider_item_id=_CHASE_ITEM)
    with pytest.raises(UserError, match="required to disconnect"):
        service.plan_purge()
    mock_client.list_institutions.assert_not_called()


def test_purge_confirmed_removes_nothing_when_verification_fails(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    service = _pull_both_items(mock_client, db, loader)
    mock_client.list_institutions.return_value = [_chase_connection()]
    before_links = _accepted_link_rows(db, _CHASE_ITEM)

    def refuse(_: object) -> None:
        raise ValueError("confirmation mismatch")

    with pytest.raises(ValueError, match="confirmation mismatch"):
        service.purge_confirmed(provider_item_id=_CHASE_ITEM, verify=refuse)

    mock_client.disconnect.assert_not_called()
    assert _raw_counts(db, _CHASE_ITEM)["plaid_transactions"] == 3
    assert _accepted_link_rows(db, _CHASE_ITEM) == before_links


def test_purge_confirmed_rolls_back_when_the_server_disconnect_fails(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    """The remote half goes first, so a remote failure leaves local state untouched."""
    service = _pull_both_items(mock_client, db, loader)
    mock_client.list_institutions.return_value = [_chase_connection()]
    mock_client.disconnect.side_effect = SyncAPIError("server 503")
    before_links = _accepted_link_rows(db, _CHASE_ITEM)

    with pytest.raises(SyncAPIError):
        service.purge_confirmed(provider_item_id=_CHASE_ITEM, verify=lambda _: None)

    assert _raw_counts(db, _CHASE_ITEM)["plaid_transactions"] == 3
    assert _accepted_link_rows(db, _CHASE_ITEM) == before_links
    pending = db.execute(
        "SELECT COUNT(*) FROM app.account_links WHERE status = 'reversed'"
    ).fetchone()
    assert pending is not None and pending[0] == 0


def test_purge_confirmed_names_the_orphan_when_the_local_half_fails(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    """After the server half lands, the error carries the id that still reaches the rows."""
    service = _pull_both_items(mock_client, db, loader)
    mock_client.list_institutions.return_value = [_chase_connection()]
    before_links = _accepted_link_rows(db, _CHASE_ITEM)
    remote_before = _remote_disconnects()

    with (
        patch.object(sync_service, "AccountLinksRepo") as links_repo,
        pytest.raises(UserError) as exc_info,
    ):
        links_repo.return_value.reverse.side_effect = RuntimeError("disk full")
        service.purge_confirmed(provider_item_id=_CHASE_ITEM, verify=lambda _: None)

    assert exc_info.value.code == error_codes.SYNC_ERROR
    assert _CHASE_ITEM in str(exc_info.value)
    assert exc_info.value.hint is not None
    assert f"--provider-item-id {_CHASE_ITEM} --purge" in exc_info.value.hint
    assert isinstance(exc_info.value.__cause__, RuntimeError)
    mock_client.disconnect.assert_called_once_with(
        DISCONNECT_A_ID, credential_generation=1, accounts=[]
    )
    assert _raw_counts(db, _CHASE_ITEM)["plaid_transactions"] == 3
    assert _accepted_link_rows(db, _CHASE_ITEM) == before_links
    # The server half did happen, so the metric records it under its own scope.
    assert _remote_disconnects() == remote_before + 1


def test_purge_confirmed_logs_the_orphan_when_interrupted_after_the_server_half(
    mock_client: MagicMock,
    db: Database,
    loader: PlaidExtractor,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An interrupt cannot be wrapped, so the log names the id to re-run with."""
    service = _pull_both_items(mock_client, db, loader)
    mock_client.list_institutions.return_value = [_chase_connection()]

    with (
        patch.object(sync_service, "AccountLinksRepo") as links_repo,
        caplog.at_level("WARNING", logger="moneybin.services.sync_service"),
        pytest.raises(KeyboardInterrupt),
    ):
        links_repo.return_value.reverse.side_effect = KeyboardInterrupt()
        service.purge_confirmed(provider_item_id=_CHASE_ITEM, verify=lambda _: None)

    mock_client.disconnect.assert_called_once_with(
        DISCONNECT_A_ID, credential_generation=1, accounts=[]
    )
    assert _raw_counts(db, _CHASE_ITEM)["plaid_transactions"] == 3
    assert any(
        _CHASE_ITEM in record.getMessage() and "did not complete" in record.getMessage()
        for record in caplog.records
    )


def test_purge_tables_cover_every_plaid_raw_schema_file() -> None:
    """A raw.plaid_* table the loader writes but the purge skips would survive a purge that reports everything gone."""
    import moneybin.extractors.plaid as plaid_package

    schema_dir = Path(plaid_package.__file__).parent / "schema"
    declared = {
        path.stem.removeprefix("raw_"): path
        for path in schema_dir.glob("raw_plaid_*.sql")
    }
    purged = {
        table.full_name.removeprefix("raw.")
        for table in sync_service._PURGE_TABLES  # pyright: ignore[reportPrivateUsage]
    }

    assert declared, "the Plaid raw schema directory moved; point this guard at it"
    assert set(declared) == purged
    for name, path in declared.items():
        assert "source_origin" in path.read_text(), (
            f"{name} has no source_origin column"
        )


def test_purge_confirmed_propagates_a_local_failure_for_an_orphan_as_is(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    """With no server half to report, the local error is the whole story."""
    service = _pull_both_items(mock_client, db, loader)
    mock_client.list_institutions.return_value = []

    with (
        patch.object(sync_service, "AccountLinksRepo") as links_repo,
        pytest.raises(RuntimeError, match="disk full"),
    ):
        links_repo.return_value.reverse.side_effect = RuntimeError("disk full")
        service.purge_confirmed(provider_item_id=_CHASE_ITEM, verify=lambda _: None)

    mock_client.disconnect.assert_not_called()
    assert _raw_counts(db, _CHASE_ITEM)["plaid_transactions"] == 3


@pytest.mark.parametrize(
    "selector", ["connection_id", "institution", "provider_item_id"]
)
def test_purge_rejects_simplefin_before_remote_or_local_mutation(
    selector: str, mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    service = _pull_both_items(mock_client, db, loader)
    connection = _chase_connection().model_copy(update={"provider": "simplefin"})
    mock_client.list_institutions.return_value = [connection]
    value = {
        "connection_id": connection.id,
        "institution": "Chase",
        "provider_item_id": _CHASE_ITEM,
    }[selector]

    with pytest.raises(UserError, match="SimpleFIN.*purge|purge.*SimpleFIN") as error:
        service.purge_confirmed(**{selector: value}, verify=lambda _: None)

    assert error.value.code == error_codes.MUTATION_INVALID_INPUT
    mock_client.disconnect.assert_not_called()
    assert _raw_counts(db, _CHASE_ITEM)["plaid_transactions"] == 3
    assert len(_accepted_link_rows(db, _CHASE_ITEM)) == 4


@pytest.mark.parametrize("other", ["institution", "provider_item_id"])
def test_plan_purge_connection_selector_is_exclusive(
    other: str, mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    service = SyncService(client=mock_client, db=db, loader=loader)
    with pytest.raises(UserError, match="mutually exclusive"):
        service.plan_purge(connection_id=DISCONNECT_A_ID, **{other: "any"})
    mock_client.list_institutions.assert_not_called()


def _purge_account(
    *, item_id: str = _CHASE_ITEM, name: str = "Checking"
) -> SyncAccountScope:
    return SyncAccountScope.model_validate({
        "account_id": "account_a",
        "native_scope": {"kind": "plaid_item", "provider_item_id": item_id},
        "name": name,
    })


@pytest.mark.parametrize("change", ["generation", "account_id", "native_scope"])
def test_purge_confirmation_binding_detects_changed_grant(
    change: str, mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    service = _pull_both_items(mock_client, db, loader)
    connection = _chase_connection().model_copy(update={"accounts": [_purge_account()]})
    mock_client.list_institutions.return_value = [connection]
    confirmed = service.plan_purge(provider_item_id=_CHASE_ITEM)
    changed_account = _purge_account()
    if change == "generation":
        connection = connection.model_copy(update={"credential_generation": 2})
    else:
        changed_account = (
            changed_account.model_copy(update={"account_id": "new_account"})
            if change == "account_id"
            else _purge_account(item_id="another_native_scope")
        )
        connection = connection.model_copy(update={"accounts": [changed_account]})
    mock_client.list_institutions.return_value = [connection]

    def verify(live: sync_service.SyncPurgePlan) -> None:
        if live.binding != confirmed.binding:
            raise ValueError("confirmation mismatch")

    with pytest.raises(ValueError, match="confirmation mismatch"):
        service.purge_confirmed(provider_item_id=_CHASE_ITEM, verify=verify)
    mock_client.disconnect.assert_not_called()
    assert _raw_counts(db, _CHASE_ITEM)["plaid_transactions"] == 3


def test_purge_binding_ignores_account_order_and_labels(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    service = _pull_both_items(mock_client, db, loader)
    first = _purge_account()
    second = first.model_copy(update={"account_id": "account_b"})
    connection = _chase_connection().model_copy(update={"accounts": [first, second]})
    mock_client.list_institutions.return_value = [connection]
    confirmed = service.plan_purge(provider_item_id=_CHASE_ITEM)
    mock_client.list_institutions.return_value = [
        connection.model_copy(
            update={
                "accounts": [
                    second.model_copy(update={"name": "Savings renamed"}),
                    first,
                ],
            }
        )
    ]
    assert service.plan_purge(provider_item_id=_CHASE_ITEM).binding == confirmed.binding


def test_purge_connection_selector_passes_confirmed_grant_and_actual_remote_receipt(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    service = _pull_both_items(mock_client, db, loader)
    account = _purge_account()
    connection = _chase_connection().model_copy(
        update={"credential_generation": 3, "accounts": [account]}
    )
    mock_client.list_institutions.return_value = [connection]
    response = SyncDisconnectResponse(
        connection_id=UUID(connection.id),
        credential_generation=3,
        accounts=[account.model_copy(update={"name": "Broker label"})],
        remote_revocation="user_action_required",
    )
    mock_client.disconnect.return_value = response

    result = service.purge_confirmed(connection_id=connection.id, verify=lambda _: None)

    mock_client.disconnect.assert_called_once_with(
        connection.id, credential_generation=3, accounts=[account]
    )
    assert result.remote_result == response
    assert _raw_counts(db, _CHASE_ITEM)["plaid_transactions"] == 0


def test_orphan_purge_makes_no_remote_revocation_claim(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    service = _pull_both_items(mock_client, db, loader)
    mock_client.list_institutions.return_value = []
    result = service.purge_confirmed(
        provider_item_id=_CHASE_ITEM, verify=lambda _: None
    )
    assert result.remote_result is None
    mock_client.disconnect.assert_not_called()


def test_local_recovery_after_purge_preserves_job_and_audit_without_restoring_rows(
    mock_client: MagicMock, db: Database, loader: PlaidExtractor
) -> None:
    service = SyncService(client=mock_client, db=db, loader=loader)
    with patch.object(
        sync_service.SecurityResolver,
        "resolve_all",
        side_effect=RuntimeError("setup unavailable"),
    ):
        ingested = service.pull(refresh=False)
    assert ingested.setup_state == "failed"
    assert len(_accepted_link_rows(db, _CHASE_ITEM)) == 4
    mock_client.list_institutions.return_value = [_chase_connection()]
    service.purge_confirmed(provider_item_id=_CHASE_ITEM, verify=lambda _: None)
    reversed_links = db.execute(
        "SELECT link_id FROM app.account_links WHERE source_origin = ? AND status = 'reversed' ORDER BY link_id",
        [_CHASE_ITEM],
    ).fetchall()
    audit_ids = set(db.execute("SELECT audit_id FROM app.audit_log").fetchall())
    assert len(reversed_links) == 4
    assert audit_ids
    mock_client.reset_mock()

    recovered = service.recover(ingested.job_id, local_only=True, refresh=False)

    assert recovered.setup_state == "complete"
    assert recovered.ingestion_state == "committed"
    assert _raw_counts(db, _CHASE_ITEM) == {
        "plaid_accounts": 0,
        "plaid_transactions": 0,
        "plaid_balances": 0,
    }
    assert _accepted_link_rows(db, _CHASE_ITEM) == []
    assert (
        db.execute(
            "SELECT link_id FROM app.account_links WHERE source_origin = ? AND status = 'reversed' ORDER BY link_id",
            [_CHASE_ITEM],
        ).fetchall()
        == reversed_links
    )
    assert audit_ids <= set(db.execute("SELECT audit_id FROM app.audit_log").fetchall())
    assert (
        sync_service.SyncJobStateRepo(db).get(ingested.job_id, service.consumer_id)
        is not None
    )
    assert mock_client.mock_calls == []
