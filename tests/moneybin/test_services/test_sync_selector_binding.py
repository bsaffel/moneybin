"""Returned jobs must remain within the user's requested retrieval scope."""

from types import SimpleNamespace
from typing import cast
from unittest.mock import patch
from uuid import UUID

import pytest

from moneybin.connectors.sync_errors import SyncAPIError
from moneybin.connectors.sync_models import PlaidItemScope
from moneybin.database import Database
from moneybin.extractors.simplefin.extractor import SimpleFINExtractor
from tests.moneybin.test_services.test_simplefin_sync_service import (
    _cash_plaid_envelope,  # pyright: ignore[reportPrivateUsage]  # shared synthetic broker
    _client,  # pyright: ignore[reportPrivateUsage]  # shared synthetic broker
    _envelope,  # pyright: ignore[reportPrivateUsage]  # shared synthetic broker
    _service,  # pyright: ignore[reportPrivateUsage]  # shared synthetic broker
)


@pytest.mark.parametrize(
    "mismatch",
    [
        "connection",
        "provider",
        "institution",
        "item_payload",
        "item_outcome",
        "item_provider",
        "extra_connection",
        "extra_provider",
        "investment_transaction",
        "investment_holding",
    ],
)
def test_pull_refuses_sources_outside_requested_scope_before_writes(
    db: Database, mismatch: str
) -> None:
    data = _cash_plaid_envelope()
    selected = data.sources[0]
    selectors: dict[str, str] = {}
    if mismatch in {"connection", "extra_connection"}:
        selectors["connection_id"] = str(
            UUID(int=999) if mismatch == "connection" else selected.connection_id
        )
    elif mismatch in {"provider", "extra_provider"}:
        selectors["provider"] = "simplefin" if mismatch == "provider" else "plaid"
    elif mismatch == "institution":
        selectors["institution"] = "Selected Synthetic Bank"
    else:
        selectors["provider_item_id"] = "plaid-item-1"
        if mismatch == "item_payload":
            selectors["provider_item_id"] = "other-item"
            selected.outcomes[0].native_scope = PlaidItemScope(
                kind="plaid_item", provider_item_id="other-item"
            )
        elif mismatch == "item_outcome":
            selected.outcomes[0].native_scope = PlaidItemScope(
                kind="plaid_item", provider_item_id="other-item"
            )
        elif mismatch in {"investment_transaction", "investment_holding"}:
            batch = selected.batches[0].model_copy(deep=True)
            batch.batch_id = UUID(int=998)
            if mismatch == "investment_transaction":
                batch.payload["investment_transactions"] = [
                    {
                        "investment_transaction_id": "synthetic-trade",
                        "account_id": "plaid-account-1",
                        "provider_item_id": "other-item",
                        "date": "2026-09-10",
                        "amount": "12.34",
                    }
                ]
            else:
                batch.payload["investment_holdings"] = [
                    {
                        "account_id": "plaid-account-1",
                        "provider_item_id": "other-item",
                        "security_id": "synthetic-security",
                    }
                ]
            selected.batches.append(batch)
        else:
            data.sources = _envelope().sources
    if mismatch in {"extra_connection", "extra_provider"}:
        data.sources.append(_envelope().sources[0])
    client = _client(data)
    client.list_institutions.return_value = [
        SimpleNamespace(
            id=str(UUID(int=999)), institution_name="Selected Synthetic Bank"
        )
    ]
    service = _service(db, client)

    with (
        patch.object(
            SimpleFINExtractor, "load", wraps=SimpleFINExtractor(db).load
        ) as simplefin_load,
        patch.object(service.loader, "load", wraps=service.loader.load) as plaid_load,
        patch.object(
            service.loader,
            "handle_removed_transactions",
            wraps=service.loader.handle_removed_transactions,
        ) as remove,
        pytest.raises(SyncAPIError, match="requested"),
    ):
        service.pull(
            connection_id=selectors.get("connection_id"),
            provider=selectors.get("provider"),
            provider_item_id=selectors.get("provider_item_id"),
            institution=selectors.get("institution"),
            refresh=False,
        )

    simplefin_load.assert_not_called()
    plaid_load.assert_not_called()
    remove.assert_not_called()
    client.ack.assert_not_called()
    for table in (
        "raw.simplefin_responses",
        "raw.plaid_transactions",
        "app.sync_job_state",
    ):
        assert db.execute(f"SELECT count(*) FROM {table}").fetchone() == (0,)  # noqa: S608  # fixed test table names


@pytest.mark.parametrize(
    "selector", ["connection_id", "provider", "provider_item_id", "institution"]
)
def test_pull_accepts_matching_requested_scope(db: Database, selector: str) -> None:
    data = _cash_plaid_envelope()
    client = _client(data)
    client.list_institutions.return_value = [
        SimpleNamespace(
            id=str(data.sources[0].connection_id), institution_name="Synthetic Bank"
        )
    ]
    value = {
        "connection_id": str(data.sources[0].connection_id),
        "provider": "plaid",
        "provider_item_id": "plaid-item-1",
        "institution": "Synthetic Bank",
    }[selector]

    result = _service(db, client).pull(
        connection_id=value if selector == "connection_id" else None,
        provider=value if selector == "provider" else None,
        provider_item_id=value if selector == "provider_item_id" else None,
        institution=value if selector == "institution" else None,
        refresh=False,
    )

    assert result.accounts_loaded == 1
    assert result.transactions_loaded == 1
    assert result.ack_state == "confirmed"
    assert result.complete


@pytest.mark.parametrize("native_record", [False, True])
def test_exact_item_removal_preserves_another_items_transaction(
    db: Database, native_record: bool
) -> None:
    data = _cash_plaid_envelope()
    data.sources[0].batches[0].payload["removed_transactions"] = [
        {"transaction_id": "other-item-transaction"}
        if native_record
        else "other-item-transaction"
    ]
    db.execute(
        """
        INSERT INTO raw.plaid_transactions
            (transaction_id, account_id, amount, transaction_date,
             source_file, source_origin)
        VALUES ('other-item-transaction', 'other-account', 1,
                DATE '2026-09-01', 'prior', 'other-item')
        """
    )

    result = _service(db, _client(data)).pull(
        provider_item_id="plaid-item-1", refresh=False
    )

    assert result.transactions_removed == 0
    assert result.ack_state == "confirmed"
    assert db.execute(
        """
        SELECT transaction_id, source_origin FROM raw.plaid_transactions
        ORDER BY transaction_id
        """
    ).fetchall() == [
        ("other-item-transaction", "other-item"),
        ("plaid-transaction-1", "plaid-item-1"),
    ]


def test_pull_refuses_unscoped_removals_before_any_ingestion(db: Database) -> None:
    data = _cash_plaid_envelope()
    batch = data.sources[0].batches[0].model_copy(deep=True)
    batch.batch_id = UUID(int=998)
    cast(dict[str, object], batch.payload["metadata"])["institutions"] = []
    for field in ("accounts", "transactions", "balances"):
        batch.payload[field] = []
    batch.payload["removed_transactions"] = ["unknown-transaction"]
    data.sources[0].batches.append(batch)
    data.sources.insert(0, _envelope().sources[0])
    client = _client(data)
    service = _service(db, client)

    with (
        patch.object(
            SimpleFINExtractor, "load", wraps=SimpleFINExtractor(db).load
        ) as simplefin_load,
        patch.object(service.loader, "load", wraps=service.loader.load) as plaid_load,
        pytest.raises(SyncAPIError, match="removal.*scope"),
    ):
        service.pull(refresh=False)

    simplefin_load.assert_not_called()
    plaid_load.assert_not_called()
    client.ack.assert_not_called()
    for table in (
        "raw.simplefin_responses",
        "raw.plaid_transactions",
        "app.sync_job_state",
    ):
        assert db.execute(f"SELECT count(*) FROM {table}").fetchone() == (0,)  # noqa: S608  # fixed test table names
