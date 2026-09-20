"""Tests for the unified `transactions review` command."""

import json
import re
import shlex
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.cli.prompts import Choice
from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols
from moneybin.services.matching_service import PENDING_MATCHES_HINT

runner = CliRunner()


def test_review_help_lists_options() -> None:
    result = runner.invoke(app, ["transactions", "review", "--help"])
    assert result.exit_code == 0
    out = result.output
    assert "--type" in out
    assert "--status" in out
    assert "--confirm" in out
    assert "--reject" in out


def test_review_type_invalid() -> None:
    result = runner.invoke(
        app, ["transactions", "review", "--type", "bogus", "--status"]
    )
    assert result.exit_code != 0


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_bare_review_answers_its_own_help_text(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """`moneybin review` with no flags prints the counts its help advertises."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(app, ["review"])
    assert result.exit_code == 0
    out = result.output.lower()
    assert "not yet implemented" not in out
    assert "matches pending" in out


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_review_type_filter_without_status_prints_the_count(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """`--type matches` alone selects the count, not the unbuilt walk."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(app, ["review", "--type", "matches"])
    assert result.exit_code == 0
    out = result.output.lower()
    assert "not yet implemented" not in out
    assert "matches pending" in out


def test_interactive_is_the_only_stubbed_path() -> None:
    """The unbuilt walk is reachable only by asking for it by name."""
    result = runner.invoke(app, ["review", "--interactive"])
    assert result.exit_code == 0
    assert "not yet implemented" in result.output.lower()


@patch("moneybin.services.matching_service.MatchingService")
@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.cli.commands.transactions.review.get_terminal_policy")
@patch("moneybin.cli.commands.transactions.review.choose_required")
def test_missing_decision_queue_requires_an_explicit_choice(
    choose: MagicMock,
    _policy: MagicMock,
    mock_get_db: MagicMock,
    mock_service: MagicMock,
) -> None:
    """A decision never silently changes the first review queue."""
    choose.return_value = "matches"
    _policy.return_value = TerminalPolicy(
        output="text",
        interactive=False,
        page=False,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=False,
        ascii=True,
        width=80,
        height=24,
        symbols=TerminalSymbols(success="OK", attention="!", failure="X", action=">"),
        minus="-",
    )
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    from moneybin.services.matching_service import MatchDecisionOutcome

    mock_service.return_value.set_status.return_value = MatchDecisionOutcome(
        match_status="accepted", transfers_retired=0
    )

    result = runner.invoke(app, ["review", "--confirm", "tx_pending0001"])

    assert result.exit_code == 0, result.output
    assert choose.call_args.kwargs["flag"] == "--type"
    assert choose.call_args.kwargs["choices"] == (Choice("matches", "Matches"),)
    mock_service.return_value.set_status.assert_called_once_with(
        "tx_pending0001", status="accepted", actor="cli"
    )


@patch("moneybin.services.matching_service.MatchingService")
@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.cli.commands.transactions.review.choose_required")
def test_explicit_decision_queue_bypasses_the_prompt(
    choose: MagicMock, mock_get_db: MagicMock, mock_service: MagicMock
) -> None:
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    from moneybin.services.matching_service import MatchDecisionOutcome

    mock_service.return_value.set_status.return_value = MatchDecisionOutcome(
        match_status="accepted", transfers_retired=0
    )

    result = runner.invoke(
        app, ["review", "--type", "matches", "--confirm", "tx_pending0001"]
    )

    assert result.exit_code == 0, result.output
    choose.assert_not_called()
    mock_service.return_value.set_status.assert_called_once()


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.services.matching_service.MatchingService.set_status")
def test_pending_matches_hint_is_runnable(
    mock_set_status: MagicMock, mock_get_db: MagicMock
) -> None:
    """Run the command the hint prints; a usage error means we published a dead end.

    Extracts the invocation from `PENDING_MATCHES_HINT` rather than restating
    it, so editing the constant to drop `--type matches` fails here instead of
    in a user's terminal — which is how the flag went missing the first time.
    """
    mock_get_db.return_value.__enter__.return_value = MagicMock()

    invocation = re.search(r"'moneybin (review [^']+)'", PENDING_MATCHES_HINT)
    assert invocation, f"no `moneybin review` command found in {PENDING_MATCHES_HINT!r}"
    args = [
        arg.replace("<match-id>", "deadbeefcafe")
        for arg in shlex.split(invocation.group(1))
    ]

    result = runner.invoke(app, args)
    assert result.exit_code != 2, (
        f"the hint prints `moneybin {invocation.group(1)}`, which exits 2: "
        f"{result.output}"
    )
    mock_set_status.assert_called_once()


@pytest.mark.parametrize(
    "extra",
    [
        pytest.param(["--status", "--interactive"], id="status-and-interactive"),
        pytest.param(
            ["--status", "--type", "matches", "--confirm", "abc123"],
            id="status-and-confirm",
        ),
        pytest.param(
            ["--interactive", "--type", "matches", "--confirm-all"],
            id="interactive-and-confirm-all",
        ),
    ],
)
def test_review_modes_are_mutually_exclusive(extra: list[str]) -> None:
    """Two modes at once is a usage error, not a silent pick between them."""
    result = runner.invoke(app, ["review", *extra])
    assert result.exit_code == 2
    assert "pass only one" in result.output


def _bulk_outcome(*, accepted: int, reversed_: int, retired: int) -> MagicMock:
    outcome = MagicMock()
    outcome.accepted = accepted
    outcome.reversed_by_reconciliation = reversed_
    outcome.transfers_retired = retired
    outcome.accounting_stale = False
    return outcome


def _selection(*ids: str, limit: int = 50) -> MagicMock:
    selection = MagicMock()
    selection.ids = ids
    selection.limit = limit
    selection.items = tuple(
        MagicMock(
            match_id=match_id,
            match_type="dedup",
            source_transaction_id_a=f"{match_id}-a",
            source_transaction_id_b=f"{match_id}-b",
        )
        for match_id in ids
    )
    return selection


@patch("moneybin.services.matching_service.MatchingService")
@patch("moneybin.cli.commands.transactions.review.get_database")
def test_confirm_all_reports_only_the_rows_that_stayed_accepted(
    mock_get_db: MagicMock, mock_service: MagicMock
) -> None:
    """The bulk path owes the same effective count the single path reports.

    Three rows flipped, one reversed by the reconciliation the same call ran, so
    two stood. Printing the pre-reconciliation three calls a row accepted that
    committed as ``reversed``.
    """
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    selection = _selection("match-1", "match-2", "match-3")
    mock_service.return_value.preview_pending.return_value = selection
    mock_service.return_value.accept_previewed.return_value = _bulk_outcome(
        accepted=2, reversed_=1, retired=1
    )

    result = runner.invoke(app, ["review", "--type", "matches", "--confirm-all"])

    # Part of what the user asked for did not commit, so the exit code carries
    # it: --confirm-all is the surface most likely to be run unattended.
    assert result.exit_code == 1
    assert "Match decisions saved" in result.output
    assert "Accepted:     2" in result.output
    assert "Accepted 3" not in result.output
    assert "1 of them did not stand" in result.output


@patch("moneybin.services.matching_service.MatchingService")
@patch("moneybin.cli.commands.transactions.review.get_database")
def test_targeted_confirm_exits_non_zero_when_the_accept_was_reversed(
    mock_get_db: MagicMock, mock_service: MagicMock
) -> None:
    """The single-id path owes the same exit code as the bulk path above.

    `review --confirm <id>` and `matches set --status accepted` reach the same
    refusal through the same reconciliation, so a caller cannot be left to
    discover on one surface what the other reports in its status.
    """
    from moneybin.services.matching_service import MatchDecisionOutcome

    mock_get_db.return_value.__enter__.return_value = MagicMock()
    mock_service.return_value.set_status.return_value = MatchDecisionOutcome(
        match_status="reversed", transfers_retired=1
    )

    result = runner.invoke(
        app, ["review", "--type", "matches", "--confirm", "tx_stale00001"]
    )

    assert result.exit_code == 1
    assert "was not accepted" in result.output


@patch("moneybin.services.matching_service.MatchingService")
@patch("moneybin.cli.commands.transactions.review.get_database")
def test_targeted_confirm_exits_zero_when_the_accept_stood(
    mock_get_db: MagicMock, mock_service: MagicMock
) -> None:
    """Negative twin: an accept that committed keeps the success status."""
    from moneybin.services.matching_service import MatchDecisionOutcome

    mock_get_db.return_value.__enter__.return_value = MagicMock()
    mock_service.return_value.set_status.return_value = MatchDecisionOutcome(
        match_status="accepted", transfers_retired=0
    )

    result = runner.invoke(
        app, ["review", "--type", "matches", "--confirm", "tx_good000001"]
    )

    assert result.exit_code == 0
    assert "Match decision saved" in result.output
    assert "Decision: accepted" in result.output


@patch("moneybin.services.matching_service.MatchingService")
@patch("moneybin.cli.commands.transactions.review.get_database")
def test_a_refused_confirm_still_performs_the_reject_asked_for_beside_it(
    mock_get_db: MagicMock, mock_service: MagicMock
) -> None:
    """The non-zero exit must not cost the caller the other half of the call.

    `--confirm X --reject Y` is two decisions in one invocation. Raising as soon
    as the confirm is refused would silently drop the reject, so the caller
    reads exit 1, assumes neither landed, and re-runs — re-rejecting a match
    that was already rejected. The raise therefore sits after the reject block,
    and this test is what pins that ordering: move the raise up and the
    ``status="rejected"`` call below disappears while the exit code stays 1.
    """
    from moneybin.services.matching_service import MatchDecisionOutcome

    mock_get_db.return_value.__enter__.return_value = MagicMock()
    mock_service.return_value.set_status.return_value = MatchDecisionOutcome(
        match_status="reversed", transfers_retired=1
    )

    result = runner.invoke(
        app,
        [
            "review",
            "--type",
            "matches",
            "--confirm",
            "tx_stale00001",
            "--reject",
            "tx_other00002",
        ],
    )

    assert result.exit_code == 1
    rejected = [
        call
        for call in mock_service.return_value.set_status.call_args_list
        if call.kwargs.get("status") == "rejected"
    ]
    assert len(rejected) == 1
    assert rejected[0].args[0] == "tx_other00002"
    assert "Match decision saved" in result.output
    assert "Decision: rejected" in result.output


@patch("moneybin.services.matching_service.MatchingService")
@patch("moneybin.cli.commands.transactions.review.get_database")
def test_confirm_all_is_silent_about_reversals_when_none_happened(
    mock_get_db: MagicMock, mock_service: MagicMock
) -> None:
    """Negative twin: the ordinary bulk accept says nothing about reversals.

    Without it, a message printed unconditionally would satisfy the assertion
    above while telling every user that part of their bulk accept was refused.
    """
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    mock_service.return_value.preview_pending.return_value = _selection(
        "match-1", "match-2", "match-3"
    )
    mock_service.return_value.accept_previewed.return_value = _bulk_outcome(
        accepted=3, reversed_=0, retired=0
    )

    result = runner.invoke(app, ["review", "--type", "matches", "--confirm-all"])

    assert result.exit_code == 0
    assert "Match decisions saved" in result.output
    assert "Accepted:     3" in result.output
    assert "did not stand" not in result.output


@pytest.mark.parametrize(
    ("command", "expects_deprecation"),
    [
        pytest.param(["review"], False, id="top-level"),
        pytest.param(["transactions", "review"], True, id="deprecated-alias"),
    ],
)
@patch("moneybin.services.matching_service.MatchingService")
@patch("moneybin.cli.commands.transactions.review.get_database")
def test_confirm_all_previews_and_accepts_the_same_limited_selection(
    mock_get_db: MagicMock,
    mock_service: MagicMock,
    command: list[str],
    expects_deprecation: bool,
) -> None:
    """Both review entry points use the service-owned bounded selection."""
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    selection = _selection("match-1", limit=1)
    mock_service.return_value.preview_pending.return_value = selection
    mock_service.return_value.accept_previewed.return_value = _bulk_outcome(
        accepted=1, reversed_=0, retired=0
    )

    result = runner.invoke(
        app, [*command, "--type", "matches", "--confirm-all", "--limit", "1"]
    )

    assert result.exit_code == 0, result.output
    assert result.output.index("match-1") < result.output.index("Accepted:     1")
    if expects_deprecation:
        assert "deprecated" in result.output
    mock_service.return_value.preview_pending.assert_called_once_with(limit=1)
    mock_service.return_value.accept_previewed.assert_called_once_with(
        selection, actor="cli"
    )


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_review_status_flag(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """--status returns counts of both queues without entering interactive mode."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    # count_pending: 0 rows from app.match_decisions; count_uncategorized: 0 rows
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(app, ["transactions", "review", "--status"])
    assert result.exit_code == 0
    out = result.output.lower()
    assert "match" in out or "matches" in out
    assert "categori" in out


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_review_type_filter_matches(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """--type matches limits output to match queue count."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(
        app, ["transactions", "review", "--type", "matches", "--status"]
    )
    assert result.exit_code == 0
    out = result.output.lower()
    assert "match" in out
    assert "categori" not in out


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_review_type_filter_categorize(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """--type categorize limits output to categorize queue count."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(
        app, ["transactions", "review", "--type", "categorize", "--status"]
    )
    assert result.exit_code == 0
    out = result.output.lower()
    assert "categori" in out
    assert "match" not in out


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_review_type_filter_merchant_links(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """--type merchant-links limits output to the merchant-link queue count."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(
        app, ["transactions", "review", "--type", "merchant-links", "--status"]
    )
    assert result.exit_code == 0
    out = result.output.lower()
    assert "merchant-link" in out
    assert "matches pending" not in out
    assert "uncategorized" not in out


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_review_type_filter_security_links(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """--type security-links limits output to the security-link queue count."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(
        app, ["transactions", "review", "--type", "security-links", "--status"]
    )
    assert result.exit_code == 0
    out = result.output.lower()
    assert "security-link" in out
    assert "matches pending" not in out
    assert "uncategorized" not in out


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_review_status_json_type_filter_merchant_links(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """--type merchant-links --output json includes only the merchant_links key."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(
        app,
        [
            "transactions",
            "review",
            "--type",
            "merchant-links",
            "--status",
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)["data"]
    assert "merchant_links_pending" in payload
    assert "account_links_pending" not in payload
    assert "total" not in payload


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_review_status_json_output(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """--status --output json emits a structured envelope."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(
        app, ["transactions", "review", "--status", "--output", "json"]
    )
    assert result.exit_code == 0
    envelope = json.loads(result.stdout)
    payload = envelope["data"]
    assert payload == {
        "matches_pending": 0,
        "categorize_pending": 0,
        "account_links_pending": 0,
        "merchant_links_pending": 0,
        "security_links_pending": 0,
        "total": 0,
    }


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_review_status_json_type_filter(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """--type matches --output json includes only the matches key."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(
        app,
        ["transactions", "review", "--type", "matches", "--status", "--output", "json"],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)["data"]
    assert "matches_pending" in payload
    assert "categorize_pending" not in payload
    assert "total" not in payload
