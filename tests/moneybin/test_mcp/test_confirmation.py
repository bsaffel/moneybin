"""Tests for payload-bound destructive mutation confirmation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastmcp.server.elicitation import (
    AcceptedElicitation,
    CancelledElicitation,
    DeclinedElicitation,
)

from moneybin import error_codes
from moneybin.errors import UserError
from moneybin.mcp.confirmation import (
    ConfirmationBinding,
    ConfirmationBroker,
    confirm_exact_or_raise,
)

NOW = datetime(2026, 7, 18, 12, 0, tzinfo=UTC)


def _supports_elicitation(_context: object) -> bool:
    return True


def _does_not_support_elicitation(_context: object) -> bool:
    return False


def _make_binding(**updates: object) -> ConfirmationBinding:
    values: dict[str, object] = {
        "arguments": {
            "account": {"id": "acct_1", "include_archived": False},
            "mode": "delete",
        },
        "resolved_ids": ("acct_1", "txn_1"),
        "actor": "agent",
        "profile": "household",
        "authorization_context": "mcp-session:authorized",
        "operation_kind": "transactions_delete",
        "blast_radius": {"accounts": 1, "transactions": 4},
    }
    values.update(updates)
    return ConfirmationBinding.model_validate(values)


BINDING = _make_binding()


def test_token_is_bound_and_single_use() -> None:
    broker = ConfirmationBroker(ttl_seconds=300)
    token = broker.issue(BINDING, now=NOW)

    assert broker.consume(token, BINDING, now=NOW) == BINDING
    with pytest.raises(UserError, match="already used") as raised:
        broker.consume(token, BINDING, now=NOW)
    assert raised.value.code == error_codes.MUTATION_CONFIRMATION_REPLAYED


@pytest.mark.parametrize(
    ("field", "changed_value"),
    [
        ("arguments", {"account": {"id": "acct_9"}, "mode": "delete"}),
        ("resolved_ids", ("acct_9",)),
        ("actor", "human"),
        ("profile", "business"),
        ("authorization_context", "mcp-session:other"),
        ("operation_kind", "accounts_delete"),
        ("blast_radius", {"accounts": 2, "transactions": 4}),
    ],
)
def test_changed_binding_field_refuses_confirmation(
    field: str, changed_value: object
) -> None:
    broker = ConfirmationBroker(ttl_seconds=300)
    token = broker.issue(BINDING, now=NOW)
    changed = BINDING.model_copy(update={field: changed_value})

    with pytest.raises(UserError) as raised:
        broker.consume(token, changed, now=NOW)

    assert raised.value.code == error_codes.MUTATION_CONFIRMATION_MISMATCH


def test_mismatched_token_is_consumed() -> None:
    broker = ConfirmationBroker(ttl_seconds=300)
    token = broker.issue(BINDING, now=NOW)
    changed = BINDING.model_copy(update={"resolved_ids": ("acct_9",)})

    with pytest.raises(UserError):
        broker.consume(token, changed, now=NOW)
    with pytest.raises(UserError) as raised:
        broker.consume(token, BINDING, now=NOW)

    assert raised.value.code == error_codes.MUTATION_CONFIRMATION_REPLAYED


def test_expired_token_is_refused() -> None:
    broker = ConfirmationBroker(ttl_seconds=300)
    token = broker.issue(BINDING, now=NOW)

    with pytest.raises(UserError) as raised:
        broker.consume(token, BINDING, now=NOW + timedelta(seconds=301))

    assert raised.value.code == error_codes.MUTATION_CONFIRMATION_EXPIRED


def test_token_is_valid_at_exact_expiration_boundary() -> None:
    broker = ConfirmationBroker(ttl_seconds=300)
    token = broker.issue(BINDING, now=NOW)

    assert broker.consume(token, BINDING, now=NOW + timedelta(seconds=300)) == BINDING


@pytest.mark.parametrize("ttl_seconds", [29, 901])
def test_broker_rejects_ttl_outside_configured_range(ttl_seconds: int) -> None:
    with pytest.raises(ValueError, match="30 and 900"):
        ConfirmationBroker(ttl_seconds=ttl_seconds)


def test_canonical_binding_ignores_json_object_key_order() -> None:
    reordered = _make_binding(
        arguments={
            "mode": "delete",
            "account": {"include_archived": False, "id": "acct_1"},
        },
        blast_radius={"transactions": 4, "accounts": 1},
    )
    broker = ConfirmationBroker(ttl_seconds=300)
    token = broker.issue(BINDING, now=NOW)

    assert broker.consume(token, reordered, now=NOW) == reordered


@pytest.mark.asyncio
async def test_token_client_recomputes_binding_immediately_before_consumption(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = ConfirmationBroker(ttl_seconds=300)
    token = broker.issue(BINDING, now=NOW)
    recompute = MagicMock(return_value=BINDING)
    active_context = MagicMock(side_effect=AssertionError("context must not be read"))
    monkeypatch.setattr("moneybin.mcp.confirmation._utcnow", lambda: NOW)
    monkeypatch.setattr("moneybin.mcp.confirmation._active_context", active_context)

    await confirm_exact_or_raise(
        binding=BINDING,
        recompute=recompute,
        message="Delete four transactions?",
        confirmation_token=token,
        broker=broker,
    )

    recompute.assert_called_once_with()
    active_context.assert_not_called()


@pytest.mark.asyncio
async def test_token_client_refuses_recomputed_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = ConfirmationBroker(ttl_seconds=300)
    token = broker.issue(BINDING, now=NOW)
    changed = BINDING.model_copy(update={"resolved_ids": ("acct_9",)})
    monkeypatch.setattr("moneybin.mcp.confirmation._utcnow", lambda: NOW)

    with pytest.raises(UserError) as raised:
        await confirm_exact_or_raise(
            binding=BINDING,
            recompute=lambda: changed,
            message="Delete four transactions?",
            confirmation_token=token,
            broker=broker,
        )

    assert raised.value.code == error_codes.MUTATION_CONFIRMATION_MISMATCH


@pytest.mark.asyncio
async def test_accepted_elicitation_recomputes_and_verifies_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = MagicMock()
    ctx.elicit = AsyncMock(return_value=AcceptedElicitation(data=True))
    recompute = MagicMock(return_value=BINDING)
    monkeypatch.setattr("moneybin.mcp.confirmation._active_context", lambda: ctx)
    monkeypatch.setattr(
        "moneybin.mcp.confirmation.supports_elicitation", _supports_elicitation
    )

    await confirm_exact_or_raise(
        binding=BINDING,
        recompute=recompute,
        message="Delete four transactions?",
        confirmation_token=None,
        broker=ConfirmationBroker(ttl_seconds=300),
    )

    ctx.elicit.assert_awaited_once_with(
        "Delete four transactions?",
        response_type=bool,
        response_title="Confirm destructive operation",
        response_description=("Select true only after reviewing the exact operation."),
    )
    recompute.assert_called_once_with()


@pytest.mark.asyncio
async def test_accepted_false_elicitation_refuses_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = MagicMock()
    ctx.elicit = AsyncMock(return_value=AcceptedElicitation(data=False))
    recompute = MagicMock(return_value=BINDING)
    monkeypatch.setattr("moneybin.mcp.confirmation._active_context", lambda: ctx)
    monkeypatch.setattr(
        "moneybin.mcp.confirmation.supports_elicitation", _supports_elicitation
    )

    with pytest.raises(UserError) as raised:
        await confirm_exact_or_raise(
            binding=BINDING,
            recompute=recompute,
            message="Delete four transactions?",
            confirmation_token=None,
            broker=ConfirmationBroker(ttl_seconds=300),
        )

    assert raised.value.code == error_codes.MUTATION_CONFIRMATION_REQUIRED
    assert raised.value.details == {"reason": "declined"}
    recompute.assert_not_called()


@pytest.mark.asyncio
async def test_accepted_elicitation_refuses_recomputed_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = MagicMock()
    ctx.elicit = AsyncMock(return_value=AcceptedElicitation(data=True))
    changed = BINDING.model_copy(update={"blast_radius": {"transactions": 5}})
    monkeypatch.setattr("moneybin.mcp.confirmation._active_context", lambda: ctx)
    monkeypatch.setattr(
        "moneybin.mcp.confirmation.supports_elicitation", _supports_elicitation
    )

    with pytest.raises(UserError) as raised:
        await confirm_exact_or_raise(
            binding=BINDING,
            recompute=lambda: changed,
            message="Delete four transactions?",
            confirmation_token=None,
            broker=ConfirmationBroker(ttl_seconds=300),
        )

    assert raised.value.code == error_codes.MUTATION_CONFIRMATION_MISMATCH


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result",
    [DeclinedElicitation(), CancelledElicitation()],
)
async def test_declined_or_cancelled_elicitation_refuses_confirmation(
    result: DeclinedElicitation | CancelledElicitation,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = MagicMock()
    ctx.elicit = AsyncMock(return_value=result)
    recompute = MagicMock(return_value=BINDING)
    monkeypatch.setattr("moneybin.mcp.confirmation._active_context", lambda: ctx)
    monkeypatch.setattr(
        "moneybin.mcp.confirmation.supports_elicitation", _supports_elicitation
    )

    with pytest.raises(UserError) as raised:
        await confirm_exact_or_raise(
            binding=BINDING,
            recompute=recompute,
            message="Delete four transactions?",
            confirmation_token=None,
            broker=ConfirmationBroker(ttl_seconds=300),
        )

    assert raised.value.code == error_codes.MUTATION_CONFIRMATION_REQUIRED
    assert raised.value.details == {"reason": "declined"}
    recompute.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("has_context", [False, True])
async def test_degraded_client_gets_structured_opaque_token(
    has_context: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = ConfirmationBroker(ttl_seconds=300)
    ctx = MagicMock() if has_context else None
    monkeypatch.setattr("moneybin.mcp.confirmation._utcnow", lambda: NOW)
    monkeypatch.setattr("moneybin.mcp.confirmation._active_context", lambda: ctx)
    monkeypatch.setattr(
        "moneybin.mcp.confirmation.supports_elicitation",
        _does_not_support_elicitation,
    )

    with pytest.raises(UserError) as raised:
        await confirm_exact_or_raise(
            binding=BINDING,
            recompute=lambda: BINDING,
            message="Delete four transactions?",
            confirmation_token=None,
            broker=broker,
        )

    error = raised.value
    assert error.code == error_codes.MUTATION_CONFIRMATION_REQUIRED
    assert error.details is not None
    assert error.details["expires_in_seconds"] == 300
    assert error.details["operation_kind"] == "transactions_delete"
    assert error.details["blast_radius"] == {"accounts": 1, "transactions": 4}
    token = error.details["confirmation_token"]
    assert isinstance(token, str)
    assert token not in error.message
    assert broker.consume(token, BINDING, now=NOW) == BINDING
