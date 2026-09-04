"""Tests for payload-bound destructive mutation confirmation."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
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
    ConfirmationGrant,
    grant_confirmation_or_raise,
)
from moneybin.mcp.decorator import mcp_tool
from moneybin.privacy.sensitivity import Sensitivity
from moneybin.protocol.envelope import ResponseEnvelope, SummaryMeta

NOW = datetime(2026, 7, 18, 12, 0, tzinfo=UTC)


def _supports_elicitation(_context: object) -> bool:
    return True


def _does_not_support_elicitation(_context: object) -> bool:
    return False


class _RecordingBroker(ConfirmationBroker):
    """Real broker that also records every token it mints.

    Asserting only on the raised error would still pass if a token were minted
    and merely withheld from the response — the entry would sit live for the
    next call to redeem. This records the mint itself.
    """

    def __init__(self, *, ttl_seconds: int | None = None) -> None:
        super().__init__(ttl_seconds=ttl_seconds)
        self.issued: list[str] = []

    def issue(self, binding: ConfirmationBinding, *, now: datetime) -> str:
        """Issue through the real broker, recording the token."""
        token = super().issue(binding, now=now)
        self.issued.append(token)
        return token


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

    grant = broker.consume(token, now=NOW)
    grant.verify(BINDING)
    with pytest.raises(UserError, match="already used") as raised:
        broker.consume(token, now=NOW)
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
    grant = broker.consume(token, now=NOW)

    with pytest.raises(UserError) as raised:
        grant.verify(changed)

    assert raised.value.code == error_codes.MUTATION_CONFIRMATION_MISMATCH


def test_mismatched_token_is_consumed() -> None:
    broker = ConfirmationBroker(ttl_seconds=300)
    token = broker.issue(BINDING, now=NOW)
    changed = BINDING.model_copy(update={"resolved_ids": ("acct_9",)})
    grant = broker.consume(token, now=NOW)

    with pytest.raises(UserError):
        grant.verify(changed)
    with pytest.raises(UserError) as raised:
        broker.consume(token, now=NOW)

    assert raised.value.code == error_codes.MUTATION_CONFIRMATION_REPLAYED


def test_expired_token_is_refused() -> None:
    broker = ConfirmationBroker(ttl_seconds=300)
    token = broker.issue(BINDING, now=NOW)

    with pytest.raises(UserError) as raised:
        broker.consume(token, now=NOW + timedelta(seconds=301))

    assert raised.value.code == error_codes.MUTATION_CONFIRMATION_EXPIRED


def test_token_is_expired_at_exact_expiration_boundary() -> None:
    broker = ConfirmationBroker(ttl_seconds=300)
    token = broker.issue(BINDING, now=NOW)

    with pytest.raises(UserError) as raised:
        broker.consume(token, now=NOW + timedelta(seconds=300))

    assert raised.value.code == error_codes.MUTATION_CONFIRMATION_EXPIRED


def test_token_is_valid_immediately_before_expiration() -> None:
    broker = ConfirmationBroker(ttl_seconds=300)
    token = broker.issue(BINDING, now=NOW)

    grant = broker.consume(
        token,
        now=NOW + timedelta(seconds=299, microseconds=999_999),
    )
    grant.verify(BINDING)


def test_issuing_another_token_preserves_expired_classification() -> None:
    broker = ConfirmationBroker(ttl_seconds=300)
    expired_token = broker.issue(BINDING, now=NOW)
    later = NOW + timedelta(seconds=301)

    broker.issue(BINDING, now=later)
    with pytest.raises(UserError) as raised:
        broker.consume(expired_token, now=later)

    assert raised.value.code == error_codes.MUTATION_CONFIRMATION_EXPIRED


def test_expired_classification_tombstones_are_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("moneybin.mcp.confirmation._MAX_EXPIRED_TOKENS", 3)
    broker = ConfirmationBroker(ttl_seconds=300)
    tokens = [
        broker.issue(BINDING, now=NOW + timedelta(seconds=301 * offset))
        for offset in range(6)
    ]
    later = NOW + timedelta(seconds=301 * 5)

    with pytest.raises(UserError) as oldest:
        broker.consume(tokens[0], now=later)
    with pytest.raises(UserError) as recent:
        broker.consume(tokens[-2], now=later)

    assert oldest.value.code == error_codes.MUTATION_CONFIRMATION_REPLAYED
    assert recent.value.code == error_codes.MUTATION_CONFIRMATION_EXPIRED


def test_consuming_live_token_evicts_other_expired_abandoned_token() -> None:
    broker = ConfirmationBroker(ttl_seconds=300)
    expired_token = broker.issue(BINDING, now=NOW)
    live_token = broker.issue(BINDING, now=NOW + timedelta(seconds=299))
    later = NOW + timedelta(seconds=301)

    broker.consume(live_token, now=later).verify(BINDING)
    with pytest.raises(UserError) as raised:
        broker.consume(expired_token, now=later)
    with pytest.raises(UserError) as replayed:
        broker.consume(expired_token, now=later)

    assert raised.value.code == error_codes.MUTATION_CONFIRMATION_EXPIRED
    assert replayed.value.code == error_codes.MUTATION_CONFIRMATION_REPLAYED


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

    broker.consume(token, now=NOW).verify(reordered)


def test_blast_radius_rejects_negative_counts() -> None:
    with pytest.raises(ValueError):
        _make_binding(blast_radius={"transactions": -1})


@pytest.mark.asyncio
async def test_token_client_consumes_before_any_context_or_binding_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = ConfirmationBroker(ttl_seconds=300)
    token = broker.issue(BINDING, now=NOW)
    active_context = MagicMock(side_effect=AssertionError("context must not be read"))
    monkeypatch.setattr("moneybin.mcp.confirmation._utcnow", lambda: NOW)
    monkeypatch.setattr("moneybin.mcp.confirmation._active_context", active_context)

    grant = await grant_confirmation_or_raise(
        binding=None,
        message="Delete four transactions?",
        confirmation_token=token,
        broker=broker,
    )

    grant.verify(BINDING)
    active_context.assert_not_called()


@pytest.mark.asyncio
async def test_token_grant_refuses_live_binding_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = ConfirmationBroker(ttl_seconds=300)
    token = broker.issue(BINDING, now=NOW)
    changed = BINDING.model_copy(update={"resolved_ids": ("acct_9",)})
    monkeypatch.setattr("moneybin.mcp.confirmation._utcnow", lambda: NOW)
    grant = await grant_confirmation_or_raise(
        binding=None,
        message="Delete four transactions?",
        confirmation_token=token,
        broker=broker,
    )

    with pytest.raises(UserError) as raised:
        grant.verify(changed)

    assert raised.value.code == error_codes.MUTATION_CONFIRMATION_MISMATCH


@pytest.mark.asyncio
async def test_accepted_elicitation_returns_verifiable_digest_grant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = MagicMock()
    ctx.elicit = AsyncMock(return_value=AcceptedElicitation(data=True))
    monkeypatch.setattr("moneybin.mcp.confirmation._active_context", lambda: ctx)
    monkeypatch.setattr(
        "moneybin.mcp.confirmation.supports_elicitation", _supports_elicitation
    )

    grant = await grant_confirmation_or_raise(
        binding=BINDING,
        message="Delete four transactions?",
        confirmation_token=None,
        broker=ConfirmationBroker(ttl_seconds=300),
    )

    ctx.elicit.assert_awaited_once_with(
        "Delete four transactions?",
        response_type=bool,
        response_title="Confirm high-impact operation",
        response_description=("Select true only after reviewing the exact operation."),
    )
    assert isinstance(grant, ConfirmationGrant)
    grant.verify(BINDING)


@pytest.mark.asyncio
async def test_elicitation_binds_immutable_pre_await_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binding = _make_binding()
    original = _make_binding()

    async def mutate_during_elicitation(
        *_args: object, **_kwargs: object
    ) -> AcceptedElicitation[bool]:
        binding.blast_radius["transactions"] = 5
        return AcceptedElicitation(data=True)

    ctx = MagicMock()
    ctx.elicit = AsyncMock(side_effect=mutate_during_elicitation)
    monkeypatch.setattr("moneybin.mcp.confirmation._active_context", lambda: ctx)
    monkeypatch.setattr(
        "moneybin.mcp.confirmation.supports_elicitation", _supports_elicitation
    )

    grant = await grant_confirmation_or_raise(
        binding=binding,
        message="Delete four transactions?",
        confirmation_token=None,
        broker=ConfirmationBroker(ttl_seconds=300),
    )

    grant.verify(original)
    with pytest.raises(UserError) as raised:
        grant.verify(binding)
    assert raised.value.code == error_codes.MUTATION_CONFIRMATION_MISMATCH


@pytest.mark.asyncio
async def test_accepted_false_elicitation_refuses_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = MagicMock()
    ctx.elicit = AsyncMock(return_value=AcceptedElicitation(data=False))
    monkeypatch.setattr("moneybin.mcp.confirmation._active_context", lambda: ctx)
    monkeypatch.setattr(
        "moneybin.mcp.confirmation.supports_elicitation", _supports_elicitation
    )

    with pytest.raises(UserError) as raised:
        await grant_confirmation_or_raise(
            binding=BINDING,
            message="Delete four transactions?",
            confirmation_token=None,
            broker=ConfirmationBroker(ttl_seconds=300),
        )

    assert raised.value.code == error_codes.MUTATION_CONFIRMATION_DECLINED
    assert raised.value.details == {"reason": "declined"}


@pytest.mark.asyncio
async def test_accepted_elicitation_grant_refuses_changed_live_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = MagicMock()
    ctx.elicit = AsyncMock(return_value=AcceptedElicitation(data=True))
    changed = BINDING.model_copy(update={"blast_radius": {"transactions": 5}})
    monkeypatch.setattr("moneybin.mcp.confirmation._active_context", lambda: ctx)
    monkeypatch.setattr(
        "moneybin.mcp.confirmation.supports_elicitation", _supports_elicitation
    )

    grant = await grant_confirmation_or_raise(
        binding=BINDING,
        message="Delete four transactions?",
        confirmation_token=None,
        broker=ConfirmationBroker(ttl_seconds=300),
    )

    with pytest.raises(UserError) as raised:
        grant.verify(changed)
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
    monkeypatch.setattr("moneybin.mcp.confirmation._active_context", lambda: ctx)
    monkeypatch.setattr(
        "moneybin.mcp.confirmation.supports_elicitation", _supports_elicitation
    )

    with pytest.raises(UserError) as raised:
        await grant_confirmation_or_raise(
            binding=BINDING,
            message="Delete four transactions?",
            confirmation_token=None,
            broker=ConfirmationBroker(ttl_seconds=300),
        )

    assert raised.value.code == error_codes.MUTATION_CONFIRMATION_DECLINED
    assert raised.value.details == {"reason": "declined"}


@pytest.mark.asyncio
async def test_unanswered_elicitation_refuses_without_issuing_a_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dialog nobody answered must not degrade into a token.

    The token travels to the *caller*, so minting one after an unanswered
    prompt hands the calling agent a key to its own unanswered confirmation.
    A client that showed the dialog has a working way to ask again; only a
    client that never offered one needs the token path.
    """

    async def answers_too_late(
        *_args: object, **_kwargs: object
    ) -> AcceptedElicitation[bool]:
        await asyncio.sleep(1.0)
        return AcceptedElicitation(data=True)

    ctx = MagicMock()
    ctx.elicit = AsyncMock(side_effect=answers_too_late)
    monkeypatch.setattr("moneybin.mcp.confirmation._active_context", lambda: ctx)
    monkeypatch.setattr(
        "moneybin.mcp.confirmation.supports_elicitation", _supports_elicitation
    )
    monkeypatch.setattr(
        "moneybin.mcp.elicitation.elicitation_wait_seconds", lambda: 0.05
    )
    broker = _RecordingBroker(ttl_seconds=300)

    with pytest.raises(UserError) as raised:
        await grant_confirmation_or_raise(
            binding=BINDING,
            message="Merge the PDF-derived account into the OFX account?",
            confirmation_token=None,
            broker=broker,
        )

    assert raised.value.code == error_codes.MUTATION_CONFIRMATION_DECLINED
    assert raised.value.details == {"reason": "timeout"}
    assert broker.issued == []
    # `wait_for` already cancelled the elicitation, so no dialog is live to
    # answer. The hint has to order the retry first or it sends the user to a
    # dead prompt and burns a second wait window before the retry that would
    # have rendered a fresh one.
    hint = raised.value.hint or ""
    assert hint.index("again") < hint.index("answer")


@pytest.mark.asyncio
async def test_human_thinking_time_does_not_count_against_the_tool_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The cap bounds machine work; a human reading a prompt is not machine work."""
    monkeypatch.setattr("moneybin.mcp.decorator._get_timeout_seconds", lambda: 0.3)
    monkeypatch.setattr(
        "moneybin.mcp.decorator.interrupt_and_reset_database", MagicMock()
    )

    async def answers_after_thinking(
        *_args: object, **_kwargs: object
    ) -> AcceptedElicitation[bool]:
        await asyncio.sleep(0.5)  # deliberately longer than the 0.3s cap
        return AcceptedElicitation(data=True)

    ctx = MagicMock()
    ctx.elicit = AsyncMock(side_effect=answers_after_thinking)
    monkeypatch.setattr("moneybin.mcp.confirmation._active_context", lambda: ctx)
    monkeypatch.setattr(
        "moneybin.mcp.confirmation.supports_elicitation", _supports_elicitation
    )
    monkeypatch.setattr(
        "moneybin.mcp.elicitation.elicitation_wait_seconds", lambda: 5.0
    )

    @mcp_tool(dynamic_classification=True, maximum_sensitivity=Sensitivity.HIGH)
    async def merge_tool() -> ResponseEnvelope[Any]:
        await grant_confirmation_or_raise(
            binding=BINDING,
            message="Merge the PDF-derived account into the OFX account?",
            confirmation_token=None,
            broker=ConfirmationBroker(ttl_seconds=300),
        )
        return ResponseEnvelope(
            summary=SummaryMeta(total_count=0, returned_count=0), data=[]
        )

    result = await merge_tool()

    assert result.error is None, (
        f"cap fired while the human was reading: {result.error}"
    )


@pytest.mark.asyncio
async def test_the_cap_resumes_after_the_human_answers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Excluding the prompt must not leave the rest of the tool unbounded."""
    monkeypatch.setattr("moneybin.mcp.decorator._get_timeout_seconds", lambda: 0.3)
    monkeypatch.setattr(
        "moneybin.mcp.decorator.interrupt_and_reset_database", MagicMock()
    )

    async def answers_after_thinking(
        *_args: object, **_kwargs: object
    ) -> AcceptedElicitation[bool]:
        await asyncio.sleep(0.4)
        return AcceptedElicitation(data=True)

    ctx = MagicMock()
    ctx.elicit = AsyncMock(side_effect=answers_after_thinking)
    monkeypatch.setattr("moneybin.mcp.confirmation._active_context", lambda: ctx)
    monkeypatch.setattr(
        "moneybin.mcp.confirmation.supports_elicitation", _supports_elicitation
    )
    monkeypatch.setattr(
        "moneybin.mcp.elicitation.elicitation_wait_seconds", lambda: 5.0
    )

    @mcp_tool(dynamic_classification=True, maximum_sensitivity=Sensitivity.HIGH)
    async def merge_tool() -> ResponseEnvelope[Any]:
        await grant_confirmation_or_raise(
            binding=BINDING,
            message="Merge the PDF-derived account into the OFX account?",
            confirmation_token=None,
            broker=ConfirmationBroker(ttl_seconds=300),
        )
        await asyncio.sleep(0.5)  # machine work, past the 0.3s cap on its own
        return ResponseEnvelope(
            summary=SummaryMeta(total_count=0, returned_count=0), data=[]
        )

    result = await merge_tool()

    assert result.error is not None, "machine work after the prompt went unbounded"
    assert result.error.code == error_codes.INFRA_TIMED_OUT


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
        await grant_confirmation_or_raise(
            binding=BINDING,
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
    broker.consume(token, now=NOW).verify(BINDING)


@pytest.mark.asyncio
async def test_elicitation_only_mints_no_token_for_a_degraded_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An account merge must never hand the caller a key to its own merge.

    The ordinary path degrades to an opaque token when the client cannot
    prompt. For a merge that rewrites ledger history that degradation IS the
    hole: the token goes back to the calling agent, which can replay it on the
    next call and never reach a human. Refuse instead, and mint nothing --
    withholding a token from the response would still leave the entry live for
    the next call to redeem, which is why this asserts on the broker.
    """
    broker = _RecordingBroker(ttl_seconds=300)
    monkeypatch.setattr("moneybin.mcp.confirmation._utcnow", lambda: NOW)
    monkeypatch.setattr(
        "moneybin.mcp.confirmation._active_context", lambda: MagicMock()
    )
    monkeypatch.setattr(
        "moneybin.mcp.confirmation.supports_elicitation",
        _does_not_support_elicitation,
    )

    with pytest.raises(UserError) as raised:
        await grant_confirmation_or_raise(
            binding=BINDING,
            message="Merge two accounts?",
            confirmation_token=None,
            elicitation_only=True,
            broker=broker,
        )

    error = raised.value
    assert error.code == error_codes.MUTATION_CONFIRMATION_REQUIRED
    assert broker.issued == [], "a token was minted for an elicitation-only merge"
    assert "confirmation_token" not in (error.details or {})
    # The refusal is a dead end unless it names a route that actually works.
    assert "moneybin" in (error.hint or "")


@pytest.mark.asyncio
async def test_elicitation_only_refuses_a_supplied_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A token minted elsewhere must not buy its way past the merge prompt.

    Without this the fix is cosmetic: a caller holding any live token for the
    same binding could still skip the prompt entirely, which is exactly the
    replay this closes.
    """
    broker = _RecordingBroker(ttl_seconds=300)
    token = broker.issue(BINDING, now=NOW)
    monkeypatch.setattr("moneybin.mcp.confirmation._utcnow", lambda: NOW)

    with pytest.raises(UserError) as raised:
        await grant_confirmation_or_raise(
            binding=None,
            message="",
            confirmation_token=token,
            elicitation_only=True,
            broker=broker,
        )

    assert raised.value.code == error_codes.MUTATION_INVALID_INPUT
    # The token must survive unconsumed: refusing by burning it would let a
    # caller destroy a confirmation somebody else was about to use.
    broker.consume(token, now=NOW).verify(BINDING)


@pytest.mark.asyncio
async def test_elicitation_only_still_grants_on_an_accepted_prompt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The stricter path must still let a real human agreement through."""
    broker = _RecordingBroker(ttl_seconds=300)
    ctx = MagicMock()
    ctx.elicit = AsyncMock(return_value=AcceptedElicitation(data=True))
    monkeypatch.setattr("moneybin.mcp.confirmation._utcnow", lambda: NOW)
    monkeypatch.setattr("moneybin.mcp.confirmation._active_context", lambda: ctx)
    monkeypatch.setattr(
        "moneybin.mcp.confirmation.supports_elicitation", _supports_elicitation
    )

    grant = await grant_confirmation_or_raise(
        binding=BINDING,
        message="Merge two accounts?",
        confirmation_token=None,
        elicitation_only=True,
        broker=broker,
    )

    grant.verify(BINDING)
    assert broker.issued == []
