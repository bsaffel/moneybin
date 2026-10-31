"""Existing source deletion boundaries preserve reviewed evidence and Golden topology."""

from datetime import UTC, datetime
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from moneybin.connectors.sync_client import SyncClient
from moneybin.connectors.sync_models import ConnectedInstitution, SyncDisconnectResponse
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.extractors.plaid.extractor import PlaidExtractor
from moneybin.repositories.import_log_repo import ImportLogRepo
from moneybin.repositories.investment_event_members_repo import (
    InvestmentEventMembersRepo,
)
from moneybin.services.import_service import ImportService
from moneybin.services.investment_membership_service import InvestmentMembershipService
from moneybin.services.sync_service import SyncService
from tests.moneybin.test_investments.comparison_helpers import install_comparison_models
from tests.moneybin.test_investments.test_investment_membership_dependencies import (
    reviewed_profile,
)


def broker(db: Database) -> tuple[SyncService, MagicMock]:
    identity = uuid4()
    client = MagicMock(spec=SyncClient)
    client.list_institutions.return_value = [
        ConnectedInstitution(
            id=str(identity),
            provider_item_id="item_1",
            provider="plaid",
            status="active",
            created_at=datetime.now(UTC),
        )
    ]
    client.disconnect.return_value = SyncDisconnectResponse(
        connection_id=identity,
        credential_generation=1,
        accounts=[],
        remote_revocation="revoked",
    )
    return SyncService(
        client=client, db=db, loader=PlaidExtractor(db), consumer_id="test-consumer"
    ), client


def test_reviewed_source_purge_refuses_before_remote_disconnect(db: Database) -> None:
    reviewed_profile(db)
    service, client = broker(db)
    before = InvestmentEventMembersRepo(db).history()
    with pytest.raises(UserError, match="reviewed|Match|accepted"):
        service.purge_confirmed(
            provider_item_id="item_1", verify=lambda _: None, actor="test"
        )
    client.disconnect.assert_not_called()
    assert InvestmentEventMembersRepo(db).history() == before
    assert db.execute(
        "SELECT COUNT(*) FROM raw.plaid_investment_transactions"
    ).fetchone() == (1,)


def test_unreviewed_source_purge_retires_membership(db: Database) -> None:
    from tests.moneybin.test_stg_plaid_investments import (
        _link_security,  # pyright: ignore[reportPrivateUsage]  # synthetic identity fixture
        _publish_investment_fixture,  # pyright: ignore[reportPrivateUsage]  # production synthetic refresh
        _raw_investment_txn,  # pyright: ignore[reportPrivateUsage]  # synthetic source fixture
    )

    _raw_investment_txn(db)
    _link_security(db, "sec_1", "security")
    _publish_investment_fixture(db)
    before = InvestmentEventMembersRepo(db).active_members()
    service, client = broker(db)
    service.purge_confirmed(
        provider_item_id="item_1", verify=lambda _: None, actor="test"
    )
    client.disconnect.assert_called_once()
    assert InvestmentEventMembersRepo(db).active_members() == []
    history = InvestmentEventMembersRepo(db).history()
    assert {row["membership_id"] for row in before} <= {
        row["membership_id"] for row in history
    }
    assert all(row["retired_at"] is not None for row in history)
    assert db.execute(
        "SELECT COUNT(*) FROM core.fct_investment_transactions"
    ).fetchone() == (0,)


def test_manual_revert_retires_standalone_without_reactivating_history(
    comparison_db: Database,
) -> None:
    from tests.moneybin.test_stg_plaid_investments import _manual_investment_txn  # pyright: ignore[reportPrivateUsage]  # synthetic manual fixture

    log = ImportLogRepo(comparison_db)
    import_id = log.begin_import(
        source_file="synthetic",
        source_type="manual",
        source_origin="cli",
        account_names=[],
    )
    _manual_investment_txn(
        comparison_db, import_id=import_id, account_id="account", security_id="security"
    )
    log.close_import(import_id, status="complete", rows_imported=1)
    install_comparison_models(comparison_db)
    InvestmentMembershipService(comparison_db).reconcile(actor="test")
    before = InvestmentEventMembersRepo(comparison_db).active_members()
    result = ImportService(comparison_db).revert_confirmed(
        import_id, verify=lambda _: None, actor="test"
    )
    assert result["status"] == "reverted"
    assert InvestmentEventMembersRepo(comparison_db).active_members() == []
    assert {row["membership_id"] for row in before} <= {
        row["membership_id"]
        for row in InvestmentEventMembersRepo(comparison_db).history()
    }


def test_reviewed_manual_revert_preserves_raw_and_match(db: Database) -> None:
    reviewed_profile(db)
    log = ImportLogRepo(db)
    log.open_import(
        "test_import",
        source_file="synthetic",
        source_type="manual",
        source_origin="cli",
        account_names=[],
    )
    log.close_import("test_import", status="complete", rows_imported=1)
    before = InvestmentEventMembersRepo(db).history()
    with pytest.raises(UserError, match="reviewed|Match|accepted"):
        ImportService(db).revert_confirmed(
            "test_import", verify=lambda _: None, actor="test"
        )
    assert InvestmentEventMembersRepo(db).history() == before
    assert db.execute(
        "SELECT COUNT(*) FROM raw.manual_investment_transactions"
    ).fetchone() == (1,)
