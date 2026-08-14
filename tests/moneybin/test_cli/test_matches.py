"""Tests for matches CLI commands."""

import json
import logging
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.transactions.matches import app

runner = CliRunner()


def _warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    """The WARNING-level records only — the surface's disclosure channel.

    A failed matcher run emits three kinds of record: the disclosures
    (WARNING), the frame chain (ERROR), and the classified ``❌`` line
    (ERROR). Filtering by level keeps a disclosure assertion pointed at the
    disclosure whatever the failure path logs around it.
    """
    return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]


class TestMatchesRun:
    """Tests for the matches run command."""

    @patch("moneybin.cli.commands.transactions.matches.get_database")
    @patch("moneybin.services.matching_service.MatchingService.run")
    def test_run_succeeds(
        self,
        mock_run: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        from moneybin.matching.engine import MatchResult

        mock_get_db.return_value = MagicMock()
        mock_run.return_value = MatchResult(auto_merged=3, pending_review=1)

        result = runner.invoke(app, ["run", "--skip-transform"])
        assert result.exit_code == 0
        mock_run.assert_called_once_with(auto_accept_transfers=False, actor="cli")

    @patch("moneybin.cli.commands.transactions.matches.get_database")
    @patch("moneybin.services.matching_service.MatchingService.run")
    def test_run_discloses_transfers_it_retired(
        self,
        mock_run: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A run that reverses accepted transfers may not report only "no matches".

        The reconciliation fires inside ``run()`` regardless of what the tiers
        find, so the two counts are independent: this fixture retires two
        transfers while finding nothing, which is exactly the case where the
        summary line alone tells the user their ledger is unchanged.

        Asserted through ``caplog`` rather than ``result.output`` because the CLI
        logger writes to stderr and CliRunner leaves that out of ``output`` — an
        output-based assertion here reads empty and proves nothing either way.
        """
        from moneybin.matching.engine import MatchResult

        mock_get_db.return_value = MagicMock()
        mock_run.return_value = MatchResult(transfers_retired=2)

        with caplog.at_level(logging.WARNING, logger="moneybin.cli.utils"):
            result = runner.invoke(app, ["run", "--skip-transform"])

        assert result.exit_code == 0
        assert caplog.messages, "the run never disclosed the retirement"
        assert "2" in caplog.messages[-1]

    @patch("moneybin.cli.commands.transactions.matches.get_database")
    @patch("moneybin.services.matching_service.MatchingService.run")
    def test_run_is_silent_when_nothing_was_retired(
        self,
        mock_run: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Negative twin: an ordinary run must not imply a decision was undone."""
        from moneybin.matching.engine import MatchResult

        mock_get_db.return_value = MagicMock()
        mock_run.return_value = MatchResult(auto_merged=3, transfers_retired=0)

        with caplog.at_level(logging.WARNING, logger="moneybin.cli.utils"):
            result = runner.invoke(app, ["run", "--skip-transform"])

        assert result.exit_code == 0
        assert caplog.messages == []

    @patch("moneybin.cli.commands.transactions.matches.get_database")
    @patch("moneybin.services.matching_service.MatchingService.run")
    def test_run_discloses_a_retirement_that_outlived_a_crash(
        self,
        mock_run: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A failed run still owes the user the reversals it already committed.

        The reconciliation commits as it goes, so a later tier crash leaves them
        behind — which is why ``MatchRunError`` carries the count at all. Only
        ``refresh()`` was reading it; this command let the exception through and
        took the sole record of a destructive, user-visible change with it.
        """
        from moneybin.matching.engine import MatchResult, MatchRunError

        mock_get_db.return_value = MagicMock()
        mock_run.side_effect = MatchRunError(
            RuntimeError("transfer tier failed"),
            partial=MatchResult(transfers_retired=2),
        )

        with caplog.at_level(logging.WARNING, logger="moneybin.cli.utils"):
            result = runner.invoke(app, ["run", "--skip-transform"])

        assert result.exit_code != 0, "a crashed run must not report success"
        # The disclosure is the WARNING; the ❌ line and the frame-chain log
        # that follow it are the failure's own presentation, so a positional
        # `messages[-1]` would read one of those instead.
        warnings = _warnings(caplog)
        assert warnings, "the crash swallowed the retirement disclosure"
        assert "2" in warnings[-1]

    @patch("moneybin.cli.commands.transactions.matches.get_database")
    @patch("moneybin.services.matching_service.MatchingService.run")
    def test_run_discloses_merges_that_outlived_a_crash(
        self,
        mock_run: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A crash before any retirement still leaves committed merges behind.

        Keying the disclosure on ``transfers_retired`` alone goes silent on
        exactly this run: a dedup tier persisted four merges — which suppress the
        duplicate side of four transactions — and then died before the
        reconciliation could reverse anything. Nothing was undone, so the
        retirement warning is correctly quiet; the merges still happened.
        """
        from moneybin.matching.engine import MatchResult, MatchRunError

        mock_get_db.return_value = MagicMock()
        mock_run.side_effect = MatchRunError(
            RuntimeError("tier 3 failed"),
            partial=MatchResult(auto_merged=4, transfers_retired=0),
        )

        with caplog.at_level(logging.WARNING, logger="moneybin.cli.utils"):
            result = runner.invoke(app, ["run", "--skip-transform"])

        assert result.exit_code != 0, "a crashed run must not report success"
        warnings = _warnings(caplog)
        assert warnings, "the crash swallowed the committed merges"
        assert "4" in warnings[-1]

    @patch("moneybin.cli.commands.transactions.matches.get_database")
    @patch("moneybin.services.matching_service.MatchingService.run")
    def test_run_stays_quiet_when_a_crash_committed_nothing(
        self,
        mock_run: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Negative twin: a crash with an empty ledger claims no committed work.

        The engine only wraps a run that wrote something, but the surface must
        not invent a disclosure from the wrapper's mere presence either — a
        warning naming zero merges would send the user hunting for state that is
        not there.
        """
        from moneybin.matching.engine import MatchResult, MatchRunError

        mock_get_db.return_value = MagicMock()
        mock_run.side_effect = MatchRunError(
            RuntimeError("tier 3 failed"), partial=MatchResult()
        )

        with caplog.at_level(logging.WARNING, logger="moneybin.cli.utils"):
            result = runner.invoke(app, ["run", "--skip-transform"])

        assert result.exit_code != 0
        assert _warnings(caplog) == []

    @patch("moneybin.cli.commands.transactions.matches.get_database")
    @patch("moneybin.services.matching_service.MatchingService.run")
    def test_run_fails_through_the_cli_error_path_not_a_traceback(
        self,
        mock_run: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The re-raise above must land on the ``❌`` + ``Exit(1)`` presentation.

        ``MatchRunError``'s own message *is* ``str(cause)``, and everything
        downstream of the tiers is DuckDB and repository work — so an
        unclassified re-raise puts binder text and file paths on the terminal
        as an unhandled traceback. The MCP twin was hardened against exactly
        this in this PR; the CLI twin is what this asserts.

        The exit code cannot tell the two apart: CliRunner reports 1 for a
        clean ``typer.Exit(1)`` and for a crash alike, which is why the three
        sibling tests above stayed green while the crash was raw.
        """
        from moneybin.matching.engine import MatchResult, MatchRunError

        mock_get_db.return_value = MagicMock()
        mock_run.side_effect = MatchRunError(
            RuntimeError("Binder Error: no column 'amt' in /Users/x/moneybin.duckdb"),
            partial=MatchResult(auto_merged=4),
        )

        with caplog.at_level(logging.WARNING):
            result = runner.invoke(app, ["run", "--skip-transform"])

        assert not isinstance(result.exception, MatchRunError), (
            "the matcher error reached the terminal as an unhandled traceback"
        )
        assert result.exit_code == 1
        errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
        assert any(m.startswith("❌") for m in errors), errors
        assert not any("Binder Error" in m for m in errors), errors


class TestMatchesBackfill:
    """Tests for the matches backfill command."""

    @patch("moneybin.cli.commands.transactions.matches.get_database")
    @patch("moneybin.services.matching_service.MatchingService.run")
    def test_backfill_discloses_transfers_it_retired(
        self,
        mock_run: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The backfill's own twin of the run gap above.

        Separate test rather than a parametrize over both commands: they build
        their summaries independently, so one can regain the disclosure while the
        other loses it, and a shared case would report that as green.
        """
        from moneybin.matching.engine import MatchResult

        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db
        mock_db.execute.return_value.fetchone.return_value = (0,)
        mock_run.return_value = MatchResult(transfers_retired=2)

        with caplog.at_level(logging.WARNING, logger="moneybin.cli.utils"):
            result = runner.invoke(app, ["backfill", "--skip-transform"])

        assert result.exit_code == 0
        assert caplog.messages, "the backfill never disclosed the retirement"
        assert "2" in caplog.messages[-1]

    @patch("moneybin.cli.commands.transactions.matches.get_database")
    @patch("moneybin.services.matching_service.MatchingService.run")
    def test_backfill_discloses_a_retirement_that_outlived_a_crash(
        self,
        mock_run: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The backfill's own twin of the crash gap above.

        Separate test for the same reason the success pair is separate: the two
        commands catch independently, so one can regain the disclosure while the
        other loses it.
        """
        from moneybin.matching.engine import MatchResult, MatchRunError

        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db
        mock_db.execute.return_value.fetchone.return_value = (0,)
        mock_run.side_effect = MatchRunError(
            RuntimeError("transfer tier failed"),
            partial=MatchResult(transfers_retired=2),
        )

        with caplog.at_level(logging.WARNING, logger="moneybin.cli.utils"):
            result = runner.invoke(app, ["backfill", "--skip-transform"])

        assert result.exit_code != 0, "a crashed backfill must not report success"
        warnings = _warnings(caplog)
        assert warnings, "the crash swallowed the retirement disclosure"
        assert "2" in warnings[-1]

    @patch("moneybin.cli.commands.transactions.matches.get_database")
    @patch("moneybin.services.matching_service.MatchingService.run")
    def test_backfill_discloses_merges_that_outlived_a_crash(
        self,
        mock_run: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The backfill's own twin of the committed-merges gap above."""
        from moneybin.matching.engine import MatchResult, MatchRunError

        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db
        mock_db.execute.return_value.fetchone.return_value = (0,)
        mock_run.side_effect = MatchRunError(
            RuntimeError("tier 3 failed"),
            partial=MatchResult(auto_merged=4, transfers_retired=0),
        )

        with caplog.at_level(logging.WARNING, logger="moneybin.cli.utils"):
            result = runner.invoke(app, ["backfill", "--skip-transform"])

        assert result.exit_code != 0, "a crashed backfill must not report success"
        warnings = _warnings(caplog)
        assert warnings, "the crash swallowed the committed merges"
        assert "4" in warnings[-1]

    @patch("moneybin.cli.commands.transactions.matches.get_database")
    @patch("moneybin.services.matching_service.MatchingService.run")
    def test_backfill_fails_through_the_cli_error_path_not_a_traceback(
        self,
        mock_run: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The backfill's own twin of the raw-crash gap above.

        Separate test for the reason the pairs above are separate: the two
        commands re-raise independently, so one can regain the clean
        presentation while the other keeps leaking.
        """
        from moneybin.matching.engine import MatchResult, MatchRunError

        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db
        mock_db.execute.return_value.fetchone.return_value = (0,)
        mock_run.side_effect = MatchRunError(
            RuntimeError("Binder Error: no column 'amt' in /Users/x/moneybin.duckdb"),
            partial=MatchResult(auto_merged=4),
        )

        with caplog.at_level(logging.WARNING):
            result = runner.invoke(app, ["backfill", "--skip-transform"])

        assert not isinstance(result.exception, MatchRunError), (
            "the matcher error reached the terminal as an unhandled traceback"
        )
        assert result.exit_code == 1
        errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
        assert any(m.startswith("❌") for m in errors), errors
        assert not any("Binder Error" in m for m in errors), errors


class TestMatchesHistory:
    """Tests for the matches history command."""

    @patch("moneybin.cli.commands.transactions.matches.get_database")
    @patch("moneybin.services.matching_service.MatchingService.get_log")
    def test_history_empty(self, mock_log: MagicMock, mock_get_db: MagicMock) -> None:
        mock_get_db.return_value = MagicMock()
        mock_log.return_value = []
        result = runner.invoke(app, ["history"])
        assert result.exit_code == 0


class TestMatchesPending:
    """Tests for the matches pending command (grouped pending display)."""

    @patch("moneybin.cli.commands.transactions.matches.get_database")
    @patch("moneybin.services.matching_service.MatchingService.get_pending")
    def test_pending_empty(
        self, mock_pending: MagicMock, mock_get_db: MagicMock
    ) -> None:
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_pending.return_value = []
        result = runner.invoke(app, ["pending"])
        assert result.exit_code == 0

    @patch("moneybin.cli.commands.transactions.matches.get_database")
    @patch("moneybin.services.matching_service.MatchingService.get_pending")
    def test_pending_groups_by_component_key(
        self, mock_pending: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """Text output shows one header per component_key group."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_pending.return_value = [
            {
                "match_id": "m_ab",
                "match_type": "dedup",
                "match_tier": "3",
                "confidence_score": 0.95,
                "source_type_a": "csv",
                "source_transaction_id_a": "t1",
                "source_type_b": "ofx",
                "source_transaction_id_b": "t2",
                "match_status": "pending",
                "component_key": "csv|t1",
                "account_id": "acc1",
            },
            {
                "match_id": "m_bc",
                "match_type": "dedup",
                "match_tier": "3",
                "confidence_score": 0.92,
                "source_type_a": "ofx",
                "source_transaction_id_a": "t2",
                "source_type_b": "tiller",
                "source_transaction_id_b": "t3",
                "match_status": "pending",
                "component_key": "csv|t1",
                "account_id": "acc1",
            },
        ]
        result = runner.invoke(app, ["pending"])
        assert result.exit_code == 0
        # One component header appears; both match IDs are in the output
        assert "component csv|t1" in result.output
        assert "m_ab" in result.output
        assert "m_bc" in result.output

    @patch("moneybin.cli.commands.transactions.matches.get_database")
    @patch("moneybin.services.matching_service.MatchingService.get_pending")
    def test_pending_json_output_includes_component_key(
        self, mock_pending: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """--output json returns rows with component_key present."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_pending.return_value = [
            {
                "match_id": "m_ab",
                "match_type": "dedup",
                "match_tier": "3",
                "confidence_score": 0.95,
                "source_type_a": "csv",
                "source_transaction_id_a": "t1",
                "source_type_b": "ofx",
                "source_transaction_id_b": "t2",
                "match_status": "pending",
                "component_key": "csv|t1",
                "account_id": "acc1",
            }
        ]
        result = runner.invoke(app, ["pending", "--output", "json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["matches"][0]["component_key"] == "csv|t1"


class TestMatchesSet:
    """The confirmation must name the status that committed, not the requested one.

    Accepting a match runs the transfer reconciliation, which walks every
    accepted transfer including the row this call just wrote. When that row
    loses the earliest-decided-first tiebreak it is reversed inside the same
    transaction, so the requested status is the one thing that cannot be trusted
    to describe the outcome.
    """

    @patch("moneybin.cli.commands.transactions.matches.get_database")
    @patch("moneybin.services.matching_service.MatchingService.set_status")
    def test_set_confirms_an_accept_that_committed(
        self,
        mock_set_status: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        from moneybin.services.matching_service import MatchDecisionOutcome

        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_set_status.return_value = MatchDecisionOutcome(
            match_status="accepted", transfers_retired=0
        )

        with caplog.at_level(logging.INFO):
            result = runner.invoke(app, ["set", "dd_100000001", "--status", "accepted"])

        assert result.exit_code == 0
        assert any("✅" in m for m in caplog.messages)

    @patch("moneybin.cli.commands.transactions.matches.get_database")
    @patch("moneybin.services.matching_service.MatchingService.set_status")
    def test_set_does_not_claim_success_when_the_accept_was_reversed(
        self,
        mock_set_status: MagicMock,
        mock_get_db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Twin of the case above, decided against the caller.

        A ✅ here tells the user their request landed when the committed row says
        the opposite, and the generic retirement warning that follows names a
        count rather than this decision — so nothing on screen contradicts it.
        """
        from moneybin.services.matching_service import MatchDecisionOutcome

        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_set_status.return_value = MatchDecisionOutcome(
            match_status="reversed", transfers_retired=1
        )

        with caplog.at_level(logging.INFO):
            result = runner.invoke(
                app, ["set", "tx_stale00001", "--status", "accepted"]
            )

        assert result.exit_code == 0
        assert not any("✅" in m for m in caplog.messages)
        assert any("reversed" in m for m in caplog.messages)


class TestMatchesUndo:
    """Tests for the matches undo command."""

    @patch("moneybin.cli.commands.transactions.matches.get_database")
    @patch("moneybin.services.matching_service.MatchingService.undo")
    def test_undo_calls_service(
        self, mock_undo: MagicMock, mock_get_db: MagicMock
    ) -> None:
        mock_get_db.return_value = MagicMock()
        result = runner.invoke(app, ["undo", "abc123", "--yes"])
        assert result.exit_code == 0
        mock_undo.assert_called_once()
