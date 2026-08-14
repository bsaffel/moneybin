"""Tests for matches CLI commands."""

import json
import logging
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.transactions.matches import app

runner = CliRunner()


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
