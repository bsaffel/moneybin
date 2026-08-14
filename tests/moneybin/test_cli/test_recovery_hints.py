"""The recovery routes the transfer-retirement surfaces print must be runnable.

Each message here fires when MoneyBin has just undone something the user
decided, or when a pass stopped partway — the one moment the published way back
has to work. Every test drives the producing code and resolves the commands it
emitted, so rewording is free and renaming a command is not.

Sibling guard on the same class of bug:
``test_transactions_review.py::test_pending_matches_hint_is_runnable``.
"""

from __future__ import annotations

import logging

import pytest

from moneybin.cli.commands.accounts.links import (
    _report_rematch,  # pyright: ignore[reportPrivateUsage]  # the unit that emits the hints
)
from moneybin.cli.utils import (
    RETIRED_BY_MATCH_STEP,
    RETIRED_BY_MERGE,
    RETIRED_BY_THIS_DECISION,
    warn_transfers_retired,
)
from moneybin.services.refresh import RefreshResult
from tests.cli_command_helpers import assert_published_commands_resolve


@pytest.mark.parametrize(
    "cause",
    [RETIRED_BY_THIS_DECISION, RETIRED_BY_MATCH_STEP, RETIRED_BY_MERGE],
    ids=["this-decision", "match-step", "merge"],
)
def test_retirement_warning_publishes_runnable_recovery(
    cause: str, caplog: pytest.LogCaptureFixture
) -> None:
    """All three triggers share one helper, so all three share its recovery route.

    Parametrized over the causes rather than the helper's single body because
    the reason the wording is shared is that the way back must not drift between
    surfaces; a future per-cause message would still be covered here.
    """
    with caplog.at_level(logging.WARNING, logger="moneybin.cli.utils"):
        warn_transfers_retired(2, cause=cause)

    assert caplog.messages, "the warning did not fire, so nothing was checked"
    assert_published_commands_resolve(caplog.messages[-1])


def test_retirement_warning_is_silent_on_zero(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Negative twin: nothing retired publishes nothing.

    Without it, a helper that warned unconditionally would satisfy every
    assertion above while telling the user a decision was undone on every
    ordinary accept.
    """
    with caplog.at_level(logging.WARNING, logger="moneybin.cli.utils"):
        warn_transfers_retired(0, cause=RETIRED_BY_THIS_DECISION)

    assert caplog.messages == []


def test_partial_rematch_report_publishes_runnable_recovery(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The crash branch: counts are incomplete and some merges already landed."""
    with caplog.at_level(logging.INFO, logger="moneybin.cli.commands.accounts.links"):
        _report_rematch(
            RefreshResult(applied=True, duration_seconds=0.0, matching_error="boom")
        )

    assert caplog.messages, "the partial-failure branch did not report"
    assert_published_commands_resolve("\n".join(caplog.messages))


def test_pending_transfer_hint_publishes_runnable_recovery(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The clean branch that raised transfers still owes a way to review them."""
    with caplog.at_level(logging.INFO, logger="moneybin.cli.commands.accounts.links"):
        _report_rematch(
            RefreshResult(
                applied=True, duration_seconds=0.0, matches_pending_transfers=3
            )
        )

    assert caplog.messages, "the pending-transfer branch did not report"
    assert_published_commands_resolve("\n".join(caplog.messages))
