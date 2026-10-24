"""Existing sync journeys select broker grants across both providers."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from tests.moneybin.test_services.test_simplefin_sync_service import (
    _client,  # pyright: ignore[reportPrivateUsage]  # shared synthetic job fixture
    _envelope,  # pyright: ignore[reportPrivateUsage]  # shared synthetic job fixture
    _service,  # pyright: ignore[reportPrivateUsage]  # shared synthetic job fixture
)


def _connection(
    identifier: int = 100, *, provider: str = "simplefin"
) -> SimpleNamespace:
    return SimpleNamespace(
        id=str(UUID(int=identifier)),
        provider_item_id=None if provider == "simplefin" else f"item-{identifier}",
        institution_name="Synthetic Bank",
        provider=provider,
        status="active",
        credential_generation=1,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        last_sync=None,
        error_code=None,
        protocol_version="2.0.0-draft" if provider == "simplefin" else None,
        accounts=[],
        history=None,
        history_unavailable_reason=None,
    )


def test_ambiguous_name_offers_exact_connections_without_deletion(db: Database) -> None:
    client = _client(_envelope())
    client.list_institutions.return_value = [_connection(100), _connection(101)]
    with pytest.raises(UserError) as failure:
        _service(db, client).pull(institution="Synthetic Bank", refresh=False)
    message = str(failure.value)
    assert (
        "connection" in message
        and str(UUID(int=100)) in message
        and str(UUID(int=101)) in message
    )
    assert "remove the extra" not in message
    client.trigger_sync.assert_not_called()


def test_exact_disconnect_selects_the_broker_grant(db: Database) -> None:
    client = MagicMock()
    client.list_institutions.return_value = [_connection(100), _connection(101)]
    result = _service(db, client).plan_disconnect(connection_id=str(UUID(int=101)))
    assert result.id == str(UUID(int=101))
    client.disconnect.assert_not_called()


def test_link_replacement_uses_exact_grant_and_provider(db: Database) -> None:
    client = MagicMock()
    client.list_institutions.return_value = [_connection()]
    client.initiate_link.return_value = SimpleNamespace(link_type="widget_flow")
    result = _service(db, client).initiate_link(
        provider="simplefin", connection_id=str(UUID(int=100))
    )
    assert result.link_type == "widget_flow"
    assert client.initiate_link.call_args.kwargs["connection_id"] == str(UUID(int=100))
    assert client.initiate_link.call_args.kwargs["provider"] == "simplefin"


def test_invalid_link_provider_is_rejected_before_broker_access(db: Database) -> None:
    client = MagicMock()
    service = _service(db, client)
    with pytest.raises(UserError, match="plaid or simplefin"):
        service.initiate_link(provider="unrecognized-provider")
    client.list_institutions.assert_not_called()
    client.initiate_link.assert_not_called()
