"""Tests for shared human-confirmation elicitation."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastmcp.server.elicitation import AcceptedElicitation

from moneybin import error_codes
from moneybin.errors import UserError
from moneybin.mcp.elicitation import confirm_or_raise


def _supports_elicitation(_context: object) -> bool:
    return True


@pytest.mark.asyncio
async def test_confirm_or_raise_requires_explicit_true(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = MagicMock()
    ctx.elicit = AsyncMock(return_value=AcceptedElicitation(data=False))
    monkeypatch.setattr("moneybin.mcp.elicitation.get_context", lambda: ctx)
    monkeypatch.setattr(
        "moneybin.mcp.elicitation.supports_elicitation",
        _supports_elicitation,
    )

    with pytest.raises(UserError) as raised:
        await confirm_or_raise(
            "Invert every amount?",
            subject="This sign inversion",
            unchanged="the import remains pending",
            cli_equivalent="moneybin import confirm statement.csv --confirm-sign",
            details={"file_path": "statement.csv"},
        )

    assert raised.value.code == error_codes.MUTATION_CONFIRMATION_REQUIRED
    assert raised.value.details == {
        "file_path": "statement.csv",
        "reason": "declined",
    }


@pytest.mark.asyncio
async def test_confirm_or_raise_reports_an_unanswered_prompt_as_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nobody answering is not the same as somebody declining."""

    async def answers_too_late(
        *_args: object, **_kwargs: object
    ) -> AcceptedElicitation[bool]:
        await asyncio.sleep(1.0)
        return AcceptedElicitation(data=True)

    ctx = MagicMock()
    ctx.elicit = AsyncMock(side_effect=answers_too_late)
    monkeypatch.setattr("moneybin.mcp.elicitation.get_context", lambda: ctx)
    monkeypatch.setattr(
        "moneybin.mcp.elicitation.supports_elicitation",
        _supports_elicitation,
    )
    monkeypatch.setattr(
        "moneybin.mcp.elicitation.elicitation_wait_seconds", lambda: 0.05
    )

    with pytest.raises(UserError) as raised:
        await confirm_or_raise(
            "Invert every amount?",
            subject="This sign inversion",
            unchanged="the import remains pending",
            cli_equivalent="moneybin import confirm statement.csv --confirm-sign",
            details={"file_path": "statement.csv"},
        )

    assert raised.value.code == error_codes.MUTATION_CONFIRMATION_REQUIRED
    assert raised.value.details == {
        "file_path": "statement.csv",
        "reason": "timeout",
    }
    # The refusal must still carry a way to finish the job.
    assert "moneybin import confirm" in (raised.value.hint or "")


@pytest.mark.asyncio
async def test_confirm_or_raise_uses_explicit_boolean_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = MagicMock()
    ctx.elicit = AsyncMock(return_value=AcceptedElicitation(data=True))
    monkeypatch.setattr("moneybin.mcp.elicitation.get_context", lambda: ctx)
    monkeypatch.setattr(
        "moneybin.mcp.elicitation.supports_elicitation",
        _supports_elicitation,
    )

    await confirm_or_raise(
        "Invert every amount?",
        subject="This sign inversion",
        unchanged="the import remains pending",
        cli_equivalent="moneybin import confirm statement.csv --confirm-sign",
        details={"file_path": "statement.csv"},
    )

    ctx.elicit.assert_awaited_once_with(
        "Invert every amount?",
        response_type=bool,
        response_title="Confirm inferred financial behavior",
        response_description=(
            "Select true only after reviewing the inference and affected data."
        ),
    )
