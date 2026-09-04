"""Tests for `accounts links` CLI commands.

Mirrors test_matches.py for the transactions surface. CLI tests mock the
service layer and test argument parsing, exit codes, and output shape.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin import error_codes
from moneybin.cli.commands.accounts.links import (
    # Both are module-private on purpose — nothing outside the command should
    # build an approval or resolve a preview — so the tests that cover them
    # reach in rather than widening the surface to make them reachable.
    _ApprovedMerge,  # pyright: ignore[reportPrivateUsage]
    _drift_check,  # pyright: ignore[reportPrivateUsage]
    _merge_preview,  # pyright: ignore[reportPrivateUsage]
    app,
)
from moneybin.errors import UserError
from moneybin.orchestration.refresh import RefreshResult
from moneybin.protocol.write_contracts import AccountLinkDecisionRequest
from moneybin.services.account_links_service import AccountLinkAcceptImpact
from moneybin.services.account_resolution_types import UNNAMED_ACCOUNT_LABEL
from moneybin.services.identity_confirmation import identity_confirm_message
from moneybin.services.ledger_overlap import LedgerOverlap
from moneybin.services.review_decisions_service import (
    IdentityDecisionPlan,
    IdentityDecisionPlanItem,
)

runner = CliRunner()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clean_rematch() -> RefreshResult:
    """A post-merge pass that succeeded, for tests about something else.

    Never leave `AccountLinksService.set` returning a bare ``MagicMock`` here:
    its auto-created ``.error`` is a truthy Mock, so the command reads the
    rebuild as failed and exits 1, and ``transfers_retired`` renders as a repr
    inside the retirement warning.
    """
    return RefreshResult(applied=True, duration_seconds=0.0)


def _make_pending_group(
    *,
    provisional_id: str = "PROV1",
    provisional_name: str = "Provisional Account",
    decision_id: str = "dec001",
    candidate_id: str = "CAND001",
    candidate_name: str = "Candidate Account",
    signal: str = "institution_last4",
    overlap: LedgerOverlap | None = None,
    transactions: int = 346,
) -> MagicMock:
    """Build a mock PendingLinkGroup with sensible defaults."""
    candidate = MagicMock()
    candidate.decision_id = decision_id
    candidate.candidate_account_id = candidate_id
    candidate.candidate_display_name = candidate_name
    candidate.signal = signal
    candidate.overlap = overlap or LedgerOverlap(
        comparable=346,
        matched=345,
        window_start=date(2024, 5, 1),
        window_end=date(2026, 8, 2),
    )

    group = MagicMock()
    group.provisional_account_id = provisional_id
    group.provisional_display_name = provisional_name
    group.candidates = [candidate]
    group.transactions = transactions
    return group


def _merge_plan(
    *,
    provisional_id: str = "PROV1",
    candidate_id: str = "CAND001",
    decision_id: str = "dec001",
    transactions: tuple[str, ...] = ("t1",),
    changed: bool = True,
) -> IdentityDecisionPlan:
    """A one-item plan shaped exactly like ``_prepare_account`` builds for an accept.

    Built from the real dataclasses rather than a MagicMock so the prompt under
    test renders through the same ``blast_radius`` arithmetic the MCP binding
    uses — a mock would report whatever count the assertion asked for. The
    ``accounts`` pair mirrors the preparer, which counts both sides of a merge.
    """
    item = IdentityDecisionPlanItem(
        request=AccountLinkDecisionRequest(
            kind="account_link",
            decision_id=decision_id,
            decision="accept",
            target_id=candidate_id,
        ),
        changed=changed,
        status="accepted",
        source_id=provisional_id,
        target_id=candidate_id,
        group_key=("account", provisional_id),
        before_state=None,
        affected_ids={
            "accounts": (provisional_id, candidate_id),
            "merchants": (),
            "securities": (),
            "transactions": transactions,
            "lots": (),
            "price_marks": (),
        },
    )
    return IdentityDecisionPlan(items=(item,))


def _accept_impact(
    *,
    links: tuple[str, ...] = ("L1",),
    decisions: tuple[str, ...] = ("dec001",),
) -> AccountLinkAcceptImpact:
    """The impact the service recomputes inside its own write transaction.

    Counts are derived from the identity tuples rather than passed separately,
    so a fixture cannot describe a row set whose size disagrees with itself —
    which is precisely the same-count swap these tests exist to catch.
    """
    return AccountLinkAcceptImpact(
        provisional_account_id="PROV1",
        candidate_account_id="CAND001",
        blast_radius={
            "accounts": 2,
            "account_links": len(links),
            "account_link_decisions": len(decisions),
        },
        link_ids=links,
        decision_ids=decisions,
    )


def _approved_merge(
    *,
    transactions: tuple[str, ...] = ("t1",),
    links: tuple[str, ...] = ("L1",),
    decisions: tuple[str, ...] = ("dec001",),
) -> _ApprovedMerge:
    """What ``_merge_preview`` hands the prompt: everything the yes is bound to.

    Built from the real plan arithmetic on the sentence side so the rendered
    prompt and the drift comparison read the same numbers a live preview would
    produce, and from row identities on the other because that is what
    ``accept_impact`` returns.
    """
    return _ApprovedMerge(
        sentence=_merge_plan(transactions=transactions).blast_radius,
        links=links,
        decisions=decisions,
    )


def _previewed_merge(
    *,
    transactions: tuple[str, ...] = ("t1",),
    links: tuple[str, ...] = ("L1",),
    decisions: tuple[str, ...] = ("dec001",),
) -> tuple[_ApprovedMerge, str]:
    """What ``_merge_preview`` returns: the approval plus the text that earned it.

    The message is rendered by the real builder rather than stubbed, so a test
    asserting on what the operator read is asserting about the shipped sentence.
    """
    approved = _approved_merge(
        transactions=transactions, links=links, decisions=decisions
    )
    return approved, identity_confirm_message(
        approved.sentence, surface="cli", kinds=["account_link"]
    )


def _commit_running_verifier(
    *,
    links: tuple[str, ...] = ("L1",),
    decisions: tuple[str, ...] = ("dec001",),
) -> Callable[..., None]:
    """Stand in for ``AccountLinksService.set``, running the verifier it is given.

    The real service calls ``verify_accept`` inside its write transaction just
    before the first mutation, handing it the impact it recomputed there.
    Asserting the callback is present is half the point: a CLI that stopped
    passing one would otherwise still pass every drift assertion below by never
    checking anything. The row identities are parameters so a test can hand the
    verifier rows that moved since the prompt without touching the plan.
    """

    def _set(*_args: object, **kwargs: object) -> None:
        verify = kwargs.get("verify_accept")
        assert verify is not None, "the merge write must carry a verifier"
        assert callable(verify)
        verify(_accept_impact(links=links, decisions=decisions))

    return _set


# ---------------------------------------------------------------------------
# links pending
# ---------------------------------------------------------------------------


class TestLinksPending:
    """Tests for `accounts links pending`."""

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.pending")
    @patch("moneybin.services.account_links_service.AccountLinksService.count_pending")
    def test_pending_empty(
        self,
        mock_count: MagicMock,
        mock_pending: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """Empty queue exits 0 with no output."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_pending.return_value = []
        mock_count.return_value = 0

        result = runner.invoke(app, ["pending"])
        assert result.exit_code == 0

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.pending")
    @patch("moneybin.services.account_links_service.AccountLinksService.count_pending")
    def test_pending_shows_provisional_and_candidates(
        self,
        mock_count: MagicMock,
        mock_pending: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """Text output includes provisional account id and candidate decision ids."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        group = _make_pending_group(
            provisional_id="PROV1",
            decision_id="dec001",
            candidate_id="CAND001",
        )
        mock_pending.return_value = [group]
        mock_count.return_value = 1

        result = runner.invoke(app, ["pending"])
        assert result.exit_code == 0
        assert "PROV1" in result.output
        assert "dec001" in result.output

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.pending")
    @patch("moneybin.services.account_links_service.AccountLinksService.count_pending")
    def test_pending_leads_each_candidate_with_its_name_not_its_id(
        self,
        mock_count: MagicMock,
        mock_pending: MagicMock,
        mock_get_db: MagicMock,
        wide_terminal: None,
    ) -> None:
        """This table is where a reviewer decides which proposal to open.

        Two truncated hashes led each row and the name trailed four columns
        later, so the scan that picks a decision to act on was a scan of ids.
        """
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_pending.return_value = [_make_pending_group()]
        mock_count.return_value = 1

        result = runner.invoke(app, ["pending"])

        assert result.exit_code == 0
        row = next(ln for ln in result.output.splitlines() if "CAND001" in ln)
        assert row.index("Candidate Account") < row.index("CAND001"), row

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.pending")
    @patch("moneybin.services.account_links_service.AccountLinksService.count_pending")
    def test_pending_leads_each_group_with_the_provisional_name_not_its_id(
        self,
        mock_count: MagicMock,
        mock_pending: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """The group header names the account whose history is about to move."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_pending.return_value = [_make_pending_group()]
        mock_count.return_value = 1

        result = runner.invoke(app, ["pending"])

        assert result.exit_code == 0
        header = next(ln for ln in result.output.splitlines() if "PROV1" in ln)
        assert header.index("Provisional Account") < header.index("PROV1"), header

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.pending")
    @patch("moneybin.services.account_links_service.AccountLinksService.count_pending")
    def test_pending_states_the_evidence_and_the_magnitude(
        self,
        mock_count: MagicMock,
        mock_pending: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """Browsing has to answer "is this the same account" and "how much moves".

        The queue previously showed a confidence constant no input could move,
        so both questions cost a separate command. The candidate row now carries
        the measured overlap and the group header the size of the merge.
        """
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_pending.return_value = [_make_pending_group(transactions=346)]
        mock_count.return_value = 1

        result = runner.invoke(app, ["pending"])
        assert result.exit_code == 0
        assert "345 of 346" in result.output
        assert "346 transactions move" in result.output

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.pending")
    @patch("moneybin.services.account_links_service.AccountLinksService.count_pending")
    def test_pending_says_no_shared_period_rather_than_zero_of_zero(
        self,
        mock_count: MagicMock,
        mock_pending: MagicMock,
        mock_get_db: MagicMock,
        wide_terminal: None,
    ) -> None:
        """An unmeasurable probe must not render as evidence against the merge."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_pending.return_value = [
            _make_pending_group(
                overlap=LedgerOverlap(
                    comparable=0, matched=0, window_start=None, window_end=None
                )
            )
        ]
        mock_count.return_value = 1

        result = runner.invoke(app, ["pending"])
        assert result.exit_code == 0
        assert "0 of 0" not in result.output
        assert "no shared period" in result.output

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.pending")
    @patch("moneybin.services.account_links_service.AccountLinksService.count_pending")
    def test_pending_json_output_shape(
        self,
        mock_count: MagicMock,
        mock_pending: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """--output json emits groups[] with candidates[] and n_pending."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        group = _make_pending_group(
            provisional_id="PROV_J",
            decision_id="dec_j",
            candidate_id="CAND_J",
        )
        mock_pending.return_value = [group]
        mock_count.return_value = 1

        result = runner.invoke(app, ["pending", "--output", "json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        # Same envelope as MCP: summary + data + actions
        assert "data" in parsed
        assert "groups" in parsed["data"]
        groups = parsed["data"]["groups"]
        assert len(groups) == 1
        assert groups[0]["provisional_account_id"] == "PROV_J"
        assert len(groups[0]["candidates"]) == 1
        assert groups[0]["candidates"][0]["decision_id"] == "dec_j"
        assert "n_pending" in parsed["data"]
        # The JSON consumer gets the same two answers the table does: an agent
        # that had to re-derive them would be reading the ledger itself.
        assert groups[0]["transactions"] == 346
        assert groups[0]["candidates"][0]["overlap_matched"] == 345
        assert groups[0]["candidates"][0]["overlap_comparable"] == 346
        # The tolerance the ratio was measured at, so 345 of 346 is not read as
        # exact-date agreement it never claimed to be.
        assert groups[0]["candidates"][0]["overlap_window_days"] == 3
        assert "confidence" not in groups[0]["candidates"][0]

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.pending")
    @patch("moneybin.services.account_links_service.AccountLinksService.count_pending")
    def test_pending_json_no_ref_value(
        self,
        mock_count: MagicMock,
        mock_pending: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """JSON output never includes ref_value."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_pending.return_value = [_make_pending_group()]
        mock_count.return_value = 1

        result = runner.invoke(app, ["pending", "--output", "json"])
        assert result.exit_code == 0
        assert "ref_value" not in result.output


# ---------------------------------------------------------------------------
# links set
# ---------------------------------------------------------------------------


class TestLinksSet:
    """Tests for `accounts links set`."""

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.set")
    def test_set_into_calls_service_with_target(
        self,
        mock_set: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """--into <account_id> passes target_account_id to service.

        Carries --yes because a merge is gated: the flag is the non-interactive
        half of that gate, not an unrelated convenience.
        """
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_set.return_value = _clean_rematch()

        result = runner.invoke(app, ["set", "dec001", "--into", "CAND001", "--yes"])
        assert result.exit_code == 0
        # verify_accept=None: --yes displayed no radius, so there is nothing for
        # the write to hold itself to.
        mock_set.assert_called_once_with(
            "dec001",
            target_account_id="CAND001",
            decided_by="user",
            verify_accept=None,
        )

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.set")
    def test_set_into_reports_what_the_rematch_found(
        self,
        mock_set: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The merge re-matches, and a pass that auto-merged must say so.

        Silently collapsing rows the operator never reviewed is the failure the
        confirm gate exists to prevent — so the counts belong on the terminal,
        not only in the log file.
        """
        from moneybin.orchestration.refresh import RefreshResult

        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_set.return_value = RefreshResult(
            applied=True,
            duration_seconds=0.0,
            matches_auto_merged=2,
            matches_pending_review=5,
        )

        with caplog.at_level(logging.INFO):
            result = runner.invoke(app, ["set", "dec001", "--into", "CAND001", "--yes"])

        assert result.exit_code == 0
        assert "2" in caplog.text
        assert "5" in caplog.text
        assert "re-match" in caplog.text.lower()

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.set")
    def test_set_into_does_not_call_a_skipped_pass_clean(
        self,
        mock_set: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Nothing examined is not the same as nothing found.

        ``refresh()`` treats a missing or stale matching view as a precondition
        rather than a crash, so it returns zero counts with no error. Reading
        those zeros as a clean pass would tell the user their merge exposed no
        duplicates when the rows were never looked at.
        """
        from moneybin.orchestration.refresh import RefreshResult

        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_set.return_value = RefreshResult(
            applied=True,
            duration_seconds=0.0,
            matching_skipped=True,
        )

        with caplog.at_level(logging.INFO):
            result = runner.invoke(app, ["set", "dec001", "--into", "CAND001", "--yes"])

        assert result.exit_code == 0
        assert "no new duplicates found" not in caplog.text
        assert "could not run" in caplog.text

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.set")
    def test_set_into_reports_transfers_the_pass_raised(
        self,
        mock_set: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A full match pass raises Tier 4 candidates, and those are news too.

        Judging the run clean on the two dedup counters alone would print "no
        new duplicates found" over a merge that just queued transfers for
        review — the counters are the only place the user learns of them.
        """
        from moneybin.orchestration.refresh import RefreshResult

        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_set.return_value = RefreshResult(
            applied=True,
            duration_seconds=0.0,
            matches_pending_transfers=3,
        )

        with caplog.at_level(logging.INFO):
            result = runner.invoke(app, ["set", "dec001", "--into", "CAND001", "--yes"])

        assert result.exit_code == 0
        assert "no new duplicates found" not in caplog.text
        assert "3" in caplog.text
        assert "transfer" in caplog.text.lower()

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.set")
    def test_set_standalone_reports_no_rematch(
        self,
        mock_set: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A reject ran no match pass, so the output must not claim one."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_set.return_value = None

        with caplog.at_level(logging.INFO):
            result = runner.invoke(app, ["set", "dec001", "--standalone"])

        assert result.exit_code == 0
        assert "re-match" not in caplog.text.lower()

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.set")
    def test_set_into_warns_when_the_rebuild_after_the_merge_failed(
        self,
        mock_set: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Matching succeeded, SQLMesh did not — the counts alone would mislead.

        ``core.dim_accounts`` is ``kind FULL``, which is the whole reason
        ``transform`` follows ``match`` here. Without the apply it still lists
        both accounts, so reporting "2 auto-merged" and stopping describes a
        collapse the user cannot see anywhere in their ledger.
        """
        from moneybin.orchestration.refresh import RefreshResult

        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_set.return_value = RefreshResult(
            applied=False,
            duration_seconds=1.0,
            error="sqlmesh apply failed",
            matches_auto_merged=2,
            matches_pending_review=5,
        )

        with caplog.at_level(logging.INFO):
            result = runner.invoke(app, ["set", "dec001", "--into", "CAND001", "--yes"])

        # Exit 1, like `refresh` on the identical RefreshResult.error: the merge
        # committed but is invisible in core.dim_accounts until an apply lands,
        # and a script or agent gating on status must not read that as done.
        assert result.exit_code == 1
        # The match counts are real — decisions were written — so they stay.
        assert "2" in caplog.text
        assert [r for r in caplog.records if r.levelno == logging.WARNING], (
            "a failed rebuild must warn, not ride along under the match counts"
        )
        assert "refresh" in caplog.text.lower()

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.set")
    def test_set_into_warns_when_the_pass_retired_an_accepted_transfer(
        self,
        mock_set: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """An otherwise-clean pass that undid a decision the user made.

        Every other counter reports what the pass *found*. This one reports
        what it took away: a transfer the user had accepted, reversed because
        the merge invalidated it — its two sides turned out to be one
        transaction, or its two accounts one account. Riding along inside the
        ordinary "re-matched" line would bury it, so it warns and names the
        route back.
        """
        from moneybin.orchestration.refresh import RefreshResult

        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_set.return_value = RefreshResult(
            applied=True,
            duration_seconds=1.0,
            matches_auto_merged=1,
            transfers_retired=2,
        )

        with caplog.at_level(logging.INFO):
            result = runner.invoke(app, ["set", "dec001", "--into", "CAND001", "--yes"])

        assert result.exit_code == 0
        warnings = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any("2" in m and "transfer" in m for m in warnings), (
            "reversing a transfer the user accepted must warn, not ride along "
            "under the match counts"
        )
        assert "undo" in caplog.text.lower(), "the user must be told how to restore it"

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.set")
    def test_set_into_warns_about_both_failures_when_both_happened(
        self,
        mock_set: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Match and rebuild fail independently, so neither may mask the other.

        ``refresh()`` runs the transform step whether or not the match step
        raised, so one call can carry both errors. They also tell the user two
        different things: nothing was proposed, *and* the merge itself is not
        visible yet. Reporting only the first leaves the second silent.
        """
        from moneybin.orchestration.refresh import RefreshResult

        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_set.return_value = RefreshResult(
            applied=False,
            duration_seconds=1.0,
            error="sqlmesh apply failed",
            matching_error="matcher blew up",
        )

        with caplog.at_level(logging.INFO):
            result = runner.invoke(app, ["set", "dec001", "--into", "CAND001", "--yes"])

        assert result.exit_code == 1
        warnings = [
            r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING
        ]
        assert any("stopped partway" in m for m in warnings), (
            f"no warning covers the failed match: {warnings}"
        )
        # Collapsed before matching because the production message is wrapped
        # across source lines: a substring spanning the wrap point matches the
        # source and never the runtime string, which is how the previous
        # `"not\nreflected"` half of this assertion came to be permanently inert.
        collapsed = [" ".join(m.split()) for m in warnings]
        assert any("not reflected in your accounts" in m for m in collapsed), (
            f"no warning covers the failed rebuild: {warnings}"
        )

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.set")
    def test_set_standalone_calls_service_with_none(
        self,
        mock_set: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """--standalone passes target_account_id=None to service."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_set.return_value = _clean_rematch()

        result = runner.invoke(app, ["set", "dec001", "--standalone"])
        assert result.exit_code == 0
        mock_set.assert_called_once_with(
            "dec001",
            target_account_id=None,
            decided_by="user",
            verify_accept=None,
        )

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.cli.commands.accounts.links._merge_preview")
    @patch("moneybin.services.account_links_service.AccountLinksService.set")
    def test_set_into_declined_at_the_prompt_writes_nothing(
        self,
        mock_set: MagicMock,
        mock_preview: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """A merge the operator declines must not reach the service.

        `accounts links set --into` was the one accept path with no gate on any
        surface: MCP elicits, the import gate asks, and this command merged one
        account's whole history into another on a single unprompted invocation.
        """
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_preview.return_value = _previewed_merge()

        result = runner.invoke(app, ["set", "dec001", "--into", "CAND001"], input="n\n")

        # Exit 0: the project's code table reserves non-zero for runtime and
        # usage errors, and a declined prompt is the operator's choice carried
        # out, not a failure. Matches every sibling decline site.
        assert result.exit_code == 0
        mock_set.assert_not_called()

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.cli.commands.accounts.links._merge_preview")
    @patch("moneybin.services.account_links_service.AccountLinksService.set")
    def test_set_into_prompt_names_what_the_merge_moves(
        self,
        mock_set: MagicMock,
        mock_preview: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """The operator reads the same sentence the MCP elicitation shows.

        A bare "Are you sure?" would satisfy the gate above while telling the
        reader nothing about the size of what moves, which is the only fact that
        makes the answer more than a coin flip.
        """
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_set.return_value = _clean_rematch()
        mock_preview.return_value = _previewed_merge(transactions=("t1", "t2", "t3"))

        result = runner.invoke(app, ["set", "dec001", "--into", "CAND001"], input="y\n")

        assert result.exit_code == 0
        assert "3 transactions" in result.output
        assert "2 accounts" in result.output
        mock_set.assert_called_once()

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.cli.commands.accounts.links._merge_preview")
    @patch("moneybin.services.account_links_service.AccountLinksService.set")
    def test_set_standalone_is_not_gated(
        self,
        mock_set: MagicMock,
        mock_preview: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """Keeping an account standalone destroys nothing, so it never asks.

        Matches the MCP rule exactly — `IdentityDecisionPlan.destructive` is true
        only for a changed accept — so the gate cannot drift into confirming a
        rejection on one surface and not the other.
        """
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_set.return_value = _clean_rematch()

        result = runner.invoke(app, ["set", "dec001", "--standalone"])

        assert result.exit_code == 0
        mock_preview.assert_not_called()
        mock_set.assert_called_once()

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.cli.commands.accounts.links._plan_merge")
    @patch("moneybin.cli.commands.accounts.links._merge_preview")
    @patch("moneybin.services.account_links_service.AccountLinksService.set")
    def test_set_into_aborts_when_the_merge_grew_between_prompt_and_commit(
        self,
        mock_set: MagicMock,
        mock_preview: MagicMock,
        mock_plan: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """What the operator approved must still be what commits.

        The prompt reads on one connection and the write opens another, so a
        concurrent import can grow the merge while the operator is reading. The
        service re-derives the plan inside the write transaction and this
        verifier refuses a batch that no longer matches the sentence shown.
        """
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_preview.return_value = _previewed_merge(transactions=("t1",))
        mock_plan.return_value = _merge_plan(transactions=("t1", "t2", "t3"))
        mock_set.side_effect = _commit_running_verifier()

        result = runner.invoke(app, ["set", "dec001", "--into", "CAND001"], input="y\n")

        # The refusal reaches the operator through the error logger, not the
        # Click result stream, so assert on the record that actually carries it.
        assert result.exit_code != 0
        assert "changed while the confirmation was open" in caplog.text

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.cli.commands.accounts.links._plan_merge")
    @patch("moneybin.cli.commands.accounts.links._merge_preview")
    @patch("moneybin.services.account_links_service.AccountLinksService.set")
    def test_set_into_commits_when_the_merge_is_unchanged(
        self,
        mock_set: MagicMock,
        mock_preview: MagicMock,
        mock_plan: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """The guard must not refuse the ordinary case it wraps.

        Paired with the drift test above so neither passes for the other's
        reason: identical counts have to reach the service, or a verifier that
        refused everything would look correct.
        """
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_preview.return_value = _previewed_merge(transactions=("t1",))
        mock_plan.return_value = _merge_plan(transactions=("t1",))
        mock_set.side_effect = _commit_running_verifier()

        result = runner.invoke(app, ["set", "dec001", "--into", "CAND001"], input="y\n")

        assert result.exit_code == 0
        mock_set.assert_called_once()

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.cli.commands.accounts.links._plan_merge")
    @patch("moneybin.cli.commands.accounts.links._merge_preview")
    @patch("moneybin.services.account_links_service.AccountLinksService.set")
    def test_set_into_aborts_when_a_sibling_decision_arrived_between_prompt_and_commit(
        self,
        mock_set: MagicMock,
        mock_preview: MagicMock,
        mock_plan: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Drift the displayed sentence cannot express must still stop the write.

        The plan's radius counts accounts and transactions; the write also
        repoints every accepted link and auto-rejects every pending sibling
        decision. A concurrent `accounts links run` proposes one more candidate
        for the same provisional — no new account, no new transaction — so the
        sentence is word-for-word what the operator read while the commit now
        rejects a decision they never saw. Only the impact radius moves here,
        which is why comparing the plan alone cannot catch it.
        """
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_preview.return_value = _previewed_merge(decisions=("dec001",))
        mock_plan.return_value = _merge_plan(transactions=("t1",))
        mock_set.side_effect = _commit_running_verifier(decisions=("dec001", "dec002"))

        result = runner.invoke(app, ["set", "dec001", "--into", "CAND001"], input="y\n")

        assert result.exit_code != 0
        assert "changed while the confirmation was open" in caplog.text

    @patch("moneybin.cli.commands.accounts.links._plan_merge")
    def test_drift_refusal_carries_the_retriable_confirmation_code(
        self, mock_plan: MagicMock
    ) -> None:
        """A stale approval and an invalid decision are different failures.

        Both reach an agent through the same `--output json` envelope, where the
        code is the only machine-readable half — one is safely retriable and the
        other is not. Asserting the message text alone is exactly how the generic
        constraint-violation code sat here unnoticed, so this asserts the code.
        The plan is pinned unchanged so only the row comparison can refuse.
        """
        mock_plan.return_value = _merge_plan(transactions=("t1",))
        verify = _drift_check(MagicMock(), "dec001", _approved_merge(links=("L1",)))
        assert verify is not None

        with pytest.raises(UserError) as excinfo:
            verify(_accept_impact(links=("L1", "L2")))

        assert excinfo.value.code == error_codes.MUTATION_CONFIRMATION_MISMATCH

    @patch("moneybin.cli.commands.accounts.links._plan_merge")
    def test_drift_refusal_catches_a_swap_that_leaves_every_count_intact(
        self, mock_plan: MagicMock
    ) -> None:
        """One sibling resolved elsewhere while another arrives must still refuse.

        Every count is identical across the swap — same accounts, same
        transactions, one accepted link, two pending decisions — so a comparison
        made of counts cannot see it, and the write would auto-reject a decision
        that did not exist when the operator answered. Only comparing the row
        identities themselves catches it.
        """
        mock_plan.return_value = _merge_plan(transactions=("t1",))
        approved = _approved_merge(decisions=("dec001", "dec002"))
        verify = _drift_check(MagicMock(), "dec001", approved)
        assert verify is not None

        swapped = _accept_impact(decisions=("dec001", "dec003"))
        assert swapped.blast_radius["account_link_decisions"] == len(approved.decisions)

        with pytest.raises(UserError) as excinfo:
            verify(swapped)

        assert excinfo.value.code == error_codes.MUTATION_CONFIRMATION_MISMATCH

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.cli.commands.accounts.links._plan_merge")
    def test_merge_preview_asks_nothing_when_the_merge_moves_nothing(
        self,
        mock_plan: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """Re-running an already-settled decision has no sentence and no radius.

        `IdentityDecisionPlan.destructive` is false when the accept changes
        nothing, and `accept_impact` refuses that same decision as non-pending —
        so the destructive check has to come first or the legitimate re-run
        raises instead of passing through. Returning None is what lets the
        command skip the prompt and commit with no radius to hold itself to.
        """
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_plan.return_value = _merge_plan(changed=False)

        assert _merge_preview("dec001", "CAND001") is None

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.cli.commands.accounts.links._merge_preview")
    @patch("moneybin.services.account_links_service.AccountLinksService.set")
    def test_set_into_commits_unprompted_when_the_merge_moves_nothing(
        self,
        mock_set: MagicMock,
        mock_preview: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """A preview that moves nothing reaches the service without asking.

        No stdin is supplied on purpose: a prompt appearing here would read EOF
        and abort, so the passing exit code is itself the evidence that nothing
        was asked. `verify_accept` is None because there is no approved radius —
        inventing one would refuse a write on a comparison never shown.
        """
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_set.return_value = _clean_rematch()
        mock_preview.return_value = None

        result = runner.invoke(app, ["set", "dec001", "--into", "CAND001"])

        assert result.exit_code == 0
        assert "Merge these accounts?" not in result.output
        mock_set.assert_called_once_with(
            "dec001",
            target_account_id="CAND001",
            decided_by="user",
            verify_accept=None,
        )

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch(
        "moneybin.services.review_decisions_service.ReviewDecisionsService.plan_identity"
    )
    @patch("moneybin.services.account_links_service.AccountLinksService.set")
    def test_set_into_names_why_the_preflight_refused(
        self,
        mock_set: MagicMock,
        mock_plan_identity: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A mistyped id keeps naming itself once the gate reads it first.

        ``plan_identity`` batches per-decision reasons into ``details["errors"]``,
        which the MCP envelope carries and the CLI's text mode never prints. One
        request can only fail for one reason, so surfacing it beats the generic
        batch message the operator would otherwise get — worse than the ungated
        command gave, and only on the path that asks.
        """
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_plan_identity.side_effect = UserError(
            "Identity decision preflight failed.",
            code=error_codes.MUTATION_INVALID_INPUT,
            details={
                "errors": [
                    {
                        "reason": "No account-link decision found for id 'typo'.",
                        "code": error_codes.MUTATION_NOT_FOUND,
                    }
                ]
            },
        )

        result = runner.invoke(app, ["set", "typo", "--into", "CAND001"], input="y\n")

        assert result.exit_code != 0
        assert "No account-link decision found for id 'typo'." in caplog.text
        mock_set.assert_not_called()

    def test_preflight_reason_keeps_the_specific_code(self) -> None:
        """The unwrapped code is the decision's own, not the batch's.

        ``--output json`` puts it in the envelope an agent branches on, so
        collapsing a missing id to the batch's ``invalid_input`` would cost the
        one distinction worth having on a typo.
        """
        from moneybin.cli.commands.accounts.links import (
            _preflight_reason,  # pyright: ignore[reportPrivateUsage]  # the unwrapping is the unit under test
        )

        unwrapped = _preflight_reason(
            UserError(
                "Identity decision preflight failed.",
                code=error_codes.MUTATION_INVALID_INPUT,
                details={
                    "errors": [
                        {"reason": "gone", "code": error_codes.MUTATION_NOT_FOUND}
                    ]
                },
            )
        )

        assert unwrapped.code == error_codes.MUTATION_NOT_FOUND

    def test_preflight_reason_refuses_an_undeclared_code(self) -> None:
        """An unrecognized upstream code degrades instead of reaching the wire.

        Every wire code is proven declared by a scan of source literals, which
        cannot see through a dict. The lookup is what keeps that proof true if
        the preflight ever grows a code this command has not been taught.
        """
        from moneybin.cli.commands.accounts.links import (
            _preflight_reason,  # pyright: ignore[reportPrivateUsage]  # the unwrapping is the unit under test
        )

        unwrapped = _preflight_reason(
            UserError(
                "Identity decision preflight failed.",
                code=error_codes.MUTATION_INVALID_INPUT,
                details={
                    "errors": [{"reason": "novel", "code": "invented_at_runtime"}]
                },
            )
        )

        assert unwrapped.message == "novel"
        assert unwrapped.code == error_codes.MUTATION_INVALID_INPUT

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.cli.commands.accounts.links._merge_preview")
    @patch("moneybin.services.account_links_service.AccountLinksService.set")
    def test_set_into_declined_says_nothing_was_merged(
        self,
        mock_set: MagicMock,
        mock_preview: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """A decline states the outcome instead of Click's bare ``Aborted!``.

        Every other confirm-then-decline site in the CLI prints its own line
        (`transactions notes`, `sync`, `db kill`); a merge decision is the last
        place to leave the reader guessing whether anything moved.
        """
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_preview.return_value = _previewed_merge()

        result = runner.invoke(app, ["set", "dec001", "--into", "CAND001"], input="n\n")

        assert "nothing was merged" in result.output.lower()
        mock_set.assert_not_called()

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.cli.commands.accounts.links._merge_preview")
    @patch("moneybin.services.account_links_service.AccountLinksService.set")
    def test_set_into_with_yes_never_reaches_the_prompt(
        self,
        mock_set: MagicMock,
        mock_preview: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """--yes is the whole gate, preflight included.

        Asserting the preview never runs is what proves the flag short-circuits
        rather than answering a prompt that still cost a read.
        """
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_set.return_value = _clean_rematch()

        result = runner.invoke(app, ["set", "dec001", "--into", "CAND001", "--yes"])

        assert result.exit_code == 0
        mock_preview.assert_not_called()
        mock_set.assert_called_once()

    def test_set_requires_into_or_standalone(self) -> None:
        """Invoking set without --into or --standalone exits 2."""
        result = runner.invoke(app, ["set", "dec001"])
        assert result.exit_code == 2

    def test_set_rejects_both_flags(self) -> None:
        """--into and --standalone are mutually exclusive → exit 2."""
        result = runner.invoke(
            app, ["set", "dec001", "--into", "CAND001", "--standalone"]
        )
        assert result.exit_code == 2


# ---------------------------------------------------------------------------
# links history
# ---------------------------------------------------------------------------


class TestLinksRun:
    """Tests for `accounts links run`."""

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.run")
    def test_run_exits_0(self, mock_run: MagicMock, mock_get_db: MagicMock) -> None:
        """Run exits 0 and prints the new-proposal count."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_run.return_value = 3

        result = runner.invoke(app, ["run"])
        assert result.exit_code == 0
        assert "3" in result.output

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.run")
    def test_run_mentions_pending_command(
        self, mock_run: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """Run output hints the user toward `accounts links pending`."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_run.return_value = 2

        result = runner.invoke(app, ["run"])
        assert result.exit_code == 0
        assert "pending" in result.output.lower()

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.run")
    def test_run_zero_proposals_exits_0(
        self, mock_run: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """Run with 0 new proposals still exits 0."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_run.return_value = 0

        result = runner.invoke(app, ["run"])
        assert result.exit_code == 0

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.run")
    def test_run_json_output_shape(
        self, mock_run: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """--output json returns an envelope with new_proposals in data."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_run.return_value = 7

        result = runner.invoke(app, ["run", "--output", "json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert "data" in parsed
        assert parsed["data"]["new_proposals"] == 7

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.propose_pair")
    @patch("moneybin.services.account_links_service.AccountLinksService.run")
    def test_run_with_a_named_pair_proposes_only_that_pair(
        self, mock_run: MagicMock, mock_propose: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """Two ids ask for one proposal, not a sweep of every account."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_propose.return_value = "dec_manual01"

        result = runner.invoke(app, ["run", "acct_aaa00000", "acct_bbb00000"])

        assert result.exit_code == 0
        mock_propose.assert_called_once_with("acct_aaa00000", "acct_bbb00000")
        mock_run.assert_not_called()

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.propose_pair")
    @patch("moneybin.services.account_links_service.AccountLinksService.run")
    def test_run_with_one_id_refuses_rather_than_sweeping(
        self, mock_run: MagicMock, mock_propose: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """A single id is ambiguous, and the safe reading is not the sweep.

        Silently backfilling every account because the second id was forgotten
        would write proposals the caller never asked for.
        """
        mock_get_db.return_value.__enter__.return_value = MagicMock()

        result = runner.invoke(app, ["run", "acct_aaa00000"])

        assert result.exit_code == 2
        mock_run.assert_not_called()
        mock_propose.assert_not_called()

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.propose_pair")
    def test_run_json_with_a_named_pair_carries_the_decision_id(
        self, mock_propose: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """The caller can decide the proposal it just made without re-querying."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_propose.return_value = "dec_manual01"

        result = runner.invoke(
            app, ["run", "acct_aaa00000", "acct_bbb00000", "--output", "json"]
        )

        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["data"]["new_proposals"] == 1
        assert parsed["data"]["decision_id"] == "dec_manual01"

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.run")
    def test_run_json_without_a_pair_names_no_decision(
        self, mock_run: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """A sweep writes many proposals or none, so no single id identifies it."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_run.return_value = 4

        result = runner.invoke(app, ["run", "--output", "json"])

        assert result.exit_code == 0
        assert json.loads(result.output)["data"]["decision_id"] is None


class TestLinksHistory:
    """Tests for `accounts links history`."""

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.history")
    def test_history_names_the_accounts_rather_than_only_their_ids(
        self, mock_history: MagicMock, mock_get_db: MagicMock, wide_terminal: None
    ) -> None:
        """Reading back what a merge did should not require an id lookup.

        The table rendered `Provisional` and `Candidate` as two truncated
        hashes, so the record of an irreversible decision was unreadable to the
        person who made it.
        """
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_history.return_value = [
            {
                "decision_id": "dh001",
                "provisional_account_id": "PROV_H",
                "candidate_account_id": "CAND_H",
                "provisional_display_name": "Provisional One",
                "candidate_display_name": "Candidate Alpha",
                "status": "accepted",
                "decided_by": "user",
                "decided_at": "2025-06-01T10:00:00",
                "confidence_score": 0.85,
                "match_signals": {"signal": "name"},
            }
        ]

        result = runner.invoke(app, ["history"])

        assert result.exit_code == 0
        assert "Provisional One" in result.output
        assert "Candidate Alpha" in result.output

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.history")
    def test_history_columns_stay_aligned_for_real_display_names(
        self, mock_history: MagicMock, mock_get_db: MagicMock, wide_terminal: None
    ) -> None:
        """Two resolved names plus an arrow must still leave the row aligned.

        ``dim_accounts`` builds a display name as institution + subtype + the
        masked last four, so a merge of two of them runs past a column sized for
        one. The hand-built table this replaced sized that column by hand:
        overflowing pushed every later column out of line on the rows that had
        names at all, leaving the table aligned only where it had fallen back
        to ids. `render_rows` sizes each column to its widest value instead, so
        the guard is that this command keeps using it rather than reacquiring a
        guessed width.
        """
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_history.return_value = [
            {
                "decision_id": "dh002",
                "provisional_account_id": "PROV_W",
                "candidate_account_id": "CAND_W",
                "provisional_display_name": "Example Bank checking \u20260000",
                "candidate_display_name": "Example Bank savings \u20261111",
                "status": "accepted",
                "decided_by": "user",
                "decided_at": "2025-06-01T10:00:00",
                "confidence_score": 0.85,
                "match_signals": {"signal": "name"},
            }
        ]

        result = runner.invoke(app, ["history"])

        assert result.exit_code == 0
        lines = result.output.splitlines()
        header = next(line for line in lines if "merged" in line)
        row = next(line for line in lines if "Example Bank checking" in line)
        assert row.index("accepted") == header.index("status")

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.history")
    def test_history_says_unnamed_rather_than_printing_an_id(
        self, mock_history: MagicMock, mock_get_db: MagicMock, wide_terminal: None
    ) -> None:
        """A frozen "" is a decision, not a gap — it must not become an id.

        The freeze writes "" when the only name it could find was raw-derived
        and it declined to record it. Falling back to a truncated account_id
        put an id back in the name column, which is the defect this surface
        exists to fix, and a 12-character prefix cannot even be used as a
        handle.

        Asserted against ``UNNAMED_ACCOUNT_LABEL`` rather than a literal so
        this row and the one ``core.dim_accounts`` produces for an account it
        could not name cannot drift into two spellings of the same answer.
        """
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_history.return_value = [
            {
                "decision_id": "dh003",
                "provisional_account_id": "471166339912",
                "candidate_account_id": "CAND_U",
                "provisional_display_name": "",
                "candidate_display_name": "Candidate Alpha",
                "status": "rejected",
                "decided_by": "user",
                "decided_at": "2025-06-01T10:00:00",
                "confidence_score": 0.85,
                "match_signals": {"signal": "name"},
            }
        ]

        result = runner.invoke(app, ["history"])

        assert result.exit_code == 0
        assert "471166339912" not in result.output
        assert UNNAMED_ACCOUNT_LABEL in result.output
        assert "Candidate Alpha" in result.output

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.history")
    def test_history_empty(
        self, mock_history: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """Empty history exits 0."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_history.return_value = []

        result = runner.invoke(app, ["history"])
        assert result.exit_code == 0

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.history")
    def test_history_json_output(
        self, mock_history: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """--output json returns an envelope with decisions[]."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_history.return_value = [
            {
                "decision_id": "dh001",
                "provisional_account_id": "PROV_H",
                "candidate_account_id": "CAND_H",
                "status": "accepted",
                "decided_by": "user",
                "decided_at": "2025-06-01T10:00:00",
                "confidence_score": 0.85,
                "match_signals": {"signal": "name"},
            }
        ]

        result = runner.invoke(app, ["history", "--output", "json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert "data" in parsed
        assert "decisions" in parsed["data"]
        decisions = parsed["data"]["decisions"]
        assert len(decisions) == 1
        assert decisions[0]["decision_id"] == "dh001"
        assert decisions[0]["signal"] == "name"
        # The service row above still carries confidence_score — it stays an
        # audit column on app.account_link_decisions. The public envelope is
        # what dropped it, so the row reaching the renderer is exactly the
        # fixture that would catch it leaking back through.
        assert "confidence" not in decisions[0]
        assert "confidence_score" not in decisions[0]

    @patch("moneybin.cli.commands.accounts.links.get_database")
    @patch("moneybin.services.account_links_service.AccountLinksService.history")
    def test_history_limit_option(
        self, mock_history: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """--limit is forwarded to the service."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_history.return_value = []

        runner.invoke(app, ["history", "--limit", "10"])
        mock_history.assert_called_once_with(limit=10)


def test_links_run_arity_error_is_logged_like_its_siblings(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A half-named pair reaches the console marker and the durable log.

    The sibling usage errors in this command group go through
    ``logger.error("❌ ...")``, which both marks the line as an error on the
    console and writes it to ``cli_YYYY-MM-DD.log``. A bare ``typer.echo(...,
    err=True)`` does neither, so the one usage error an agent is most likely to
    hit would have been the only one leaving no trace. The check runs before
    the database opens, so this exercises no writer lock.
    """
    with caplog.at_level(logging.ERROR):
        result = runner.invoke(app, ["run", "ACC001"])

    assert result.exit_code == 2
    assert "❌" in caplog.text
    assert "ambiguous" in caplog.text
