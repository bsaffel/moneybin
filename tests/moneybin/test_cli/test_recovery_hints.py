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
from moneybin.cli.utils import warn_transfers_retired
from moneybin.matching.reconciliation import (
    RETIRED_SIDES_COLLAPSED,
    RETIRED_SIDES_OR_ACCOUNTS_COLLAPSED,
)
from moneybin.orchestration.refresh import RefreshResult
from tests.cli_command_helpers import assert_published_commands_resolve


@pytest.mark.parametrize(
    "cause",
    [RETIRED_SIDES_COLLAPSED, RETIRED_SIDES_OR_ACCOUNTS_COLLAPSED],
    ids=["sides", "sides-or-accounts"],
)
def test_retirement_warning_publishes_runnable_recovery(
    cause: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Every cause shares one helper, so every cause shares its recovery route.

    Parametrized over the causes rather than the helper's single body because
    the reason the wording is shared is that the way back must not drift between
    surfaces; a future per-cause message would still be covered here.
    """
    with caplog.at_level(logging.WARNING, logger="moneybin.cli.utils"):
        warn_transfers_retired(2, cause=cause)

    assert caplog.messages, "the warning did not fire, so nothing was checked"
    assert_published_commands_resolve(caplog.messages[-1])


def test_only_the_merge_clause_claims_two_accounts_collapsed() -> None:
    """The one distinction the two cause clauses exist to keep.

    Collapsing the old per-trigger constants dropped a causal claim the count
    could not support, but it must not also drop this: a merge is the only
    trigger that can fold two *accounts* into one, so a warning printed after a
    dedup accept may not offer that as an explanation. Asserted as a difference
    between the two constants rather than against a quoted sentence, so
    rewording either one stays free.
    """
    assert "accounts" in RETIRED_SIDES_OR_ACCOUNTS_COLLAPSED
    assert "accounts" not in RETIRED_SIDES_COLLAPSED
    assert RETIRED_SIDES_COLLAPSED in RETIRED_SIDES_OR_ACCOUNTS_COLLAPSED, (
        "the shared explanation drifted apart between the two clauses"
    )


def test_an_accept_path_retirement_points_at_the_rematch_it_owes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A reversal frees legs, and only the matcher can re-propose over them.

    `set_status` and `accept_all_pending` reconcile inside their own transaction
    and return without a Tier 4 pass, so a transfer the freed leg now allows
    stays unproposed until some unrelated refresh runs. `TransactionMatcher.run`
    closes that gap by re-running Tier 4 itself; the accept paths cannot, so they
    owe the user the follow-up in words.
    """
    with caplog.at_level(logging.WARNING, logger="moneybin.cli.utils"):
        warn_transfers_retired(2, cause=RETIRED_SIDES_COLLAPSED, rematch_follow_up=True)

    assert caplog.messages, "the warning did not fire, so nothing was checked"
    assert "matches run" in caplog.messages[-1]
    assert_published_commands_resolve(caplog.messages[-1])


def test_a_matcher_run_retirement_does_not_ask_for_another_run(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Negative twin: the matcher already ran Tier 4 after its own reconciliation.

    Without this, a helper that always appended the follow-up would send the
    user back through a pass that just completed — and the noise would teach
    them to ignore the sentence that matters on the accept paths.
    """
    with caplog.at_level(logging.WARNING, logger="moneybin.cli.utils"):
        warn_transfers_retired(2, cause=RETIRED_SIDES_COLLAPSED)

    assert caplog.messages, "the warning did not fire, so nothing was checked"
    assert "matches run" not in caplog.messages[-1]


def test_retirement_warning_is_silent_on_zero(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Negative twin: nothing retired publishes nothing.

    Without it, a helper that warned unconditionally would satisfy every
    assertion above while telling the user a decision was undone on every
    ordinary accept.
    """
    with caplog.at_level(logging.WARNING, logger="moneybin.cli.utils"):
        warn_transfers_retired(0, cause=RETIRED_SIDES_COLLAPSED)

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


def test_partial_rematch_report_names_the_decisions_that_landed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The crash branch has real counts now, and hedging past them hides merges.

    ``MatchRunError`` carries the committed counts and ``refresh`` copies them
    onto the result, so this branch knows exactly how many auto-merges and
    proposals are durable. Saying duplicates "may" have been merged spends that
    number on a hedge — and an auto-merge is what suppresses the duplicate side
    of a transaction in the ledger.
    """
    with caplog.at_level(logging.INFO, logger="moneybin.cli.commands.accounts.links"):
        _report_rematch(
            RefreshResult(
                applied=True,
                duration_seconds=0.0,
                matching_error="boom",
                matches_auto_merged=4,
                matches_pending_review=2,
            )
        )

    joined = "\n".join(caplog.messages)
    assert "4" in joined, f"the crash branch hid the merges that landed: {joined}"
    assert "2" in joined, f"the crash branch hid the proposals that landed: {joined}"
    assert_published_commands_resolve(joined)


def test_partial_rematch_report_claims_nothing_landed_when_nothing_did(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Negative twin: zero is now trustworthy, so it may be stated plainly.

    The engine leaves a run that committed nothing unwrapped, so zero counts on
    this branch mean the failure really did land before any write. A message that
    always hinted at possible merges would send the user hunting through the
    audit log for decisions that were never made.
    """
    with caplog.at_level(logging.INFO, logger="moneybin.cli.commands.accounts.links"):
        _report_rematch(
            RefreshResult(applied=True, duration_seconds=0.0, matching_error="boom")
        )

    joined = "\n".join(caplog.messages)
    assert "may already have been merged" not in joined


def test_mcp_partial_rematch_action_names_the_decisions_that_landed() -> None:
    """The MCP twin of the crash-branch hedge above.

    Codex reported only the CLI, but `rematch_actions` carries the same sentence
    off the same ``RefreshResult``. Fixing one surface would leave the agent —
    the caller least able to go read the audit log on a hunch — holding the
    vaguer of the two.
    """
    from moneybin.adapters.rematch_report import rematch_actions

    actions = rematch_actions(
        RefreshResult(
            applied=True,
            duration_seconds=0.0,
            matching_error="boom",
            matches_auto_merged=4,
        )
    )

    partial = next(a for a in actions if "stopped partway" in a)
    assert "4" in partial, f"the agent-facing action hid the merges: {partial}"


def test_mcp_partial_rematch_states_its_counts_once() -> None:
    """The crash branch owns the counts; the clean hints must not restate them.

    Both fire off the same fields, so an agent reading the unfiltered list gets
    "its remaining counts are incomplete" and, two lines later, a flat "the
    merge exposed 2 new duplicate proposal(s)" that reads like a finished pass.
    The CLI twin never had this — its branches are exclusive.
    """
    from moneybin.adapters.rematch_report import rematch_actions

    actions = rematch_actions(
        RefreshResult(
            applied=True,
            duration_seconds=0.0,
            matching_error="boom",
            matches_pending_review=2,
            matches_pending_transfers=1,
        )
    )

    assert sum("proposal(s)" in action for action in actions) == 1, actions
    assert sum("transfer(s)" in action for action in actions) == 1, actions
    assert "stopped partway" in " ".join(actions)


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
