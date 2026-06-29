"""Tests for `merchants links` CLI commands.

Mirrors test_accounts_links.py for the merchants surface. CLI tests mock the
service layer and test argument parsing, exit codes, and output shape.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from moneybin.cli.commands.merchants.links import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pending_group(
    *,
    ref_value: str = "ENT001",
    source_type: str = "plaid",
    provider_name: str | None = "Starbucks",
    decision_id: str = "dec001",
    candidate_id: str = "merch001aa",
    candidate_name: str = "Starbucks Inc.",
    confidence: float = 0.90,
) -> MagicMock:
    """Build a mock PendingMerchantLinkGroup with sensible defaults."""
    candidate = MagicMock()
    candidate.decision_id = decision_id
    candidate.candidate_merchant_id = candidate_id
    candidate.candidate_canonical_name = candidate_name
    candidate.confidence = confidence

    group = MagicMock()
    group.ref_value = ref_value
    group.source_type = source_type
    group.provider_merchant_name = provider_name
    group.candidates = [candidate]
    return group


# ---------------------------------------------------------------------------
# links pending
# ---------------------------------------------------------------------------


class TestMerchantLinksPending:
    """Tests for `merchants links pending`."""

    @patch("moneybin.cli.commands.merchants.links.get_database")
    @patch("moneybin.services.merchant_links_service.MerchantLinksService.pending")
    @patch(
        "moneybin.services.merchant_links_service.MerchantLinksService.count_pending"
    )
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

    @patch("moneybin.cli.commands.merchants.links.get_database")
    @patch("moneybin.services.merchant_links_service.MerchantLinksService.pending")
    @patch(
        "moneybin.services.merchant_links_service.MerchantLinksService.count_pending"
    )
    def test_pending_shows_entity_and_candidates(
        self,
        mock_count: MagicMock,
        mock_pending: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """Text output includes provider entity id and candidate decision ids."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        group = _make_pending_group(
            ref_value="ENT001",
            decision_id="dec001",
            candidate_id="merch001aa",
        )
        mock_pending.return_value = [group]
        mock_count.return_value = 1

        result = runner.invoke(app, ["pending"])
        assert result.exit_code == 0
        assert "ENT001" in result.output
        assert "dec001" in result.output

    @patch("moneybin.cli.commands.merchants.links.get_database")
    @patch("moneybin.services.merchant_links_service.MerchantLinksService.pending")
    @patch(
        "moneybin.services.merchant_links_service.MerchantLinksService.count_pending"
    )
    def test_pending_json_output_shape(
        self,
        mock_count: MagicMock,
        mock_pending: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """--output json emits groups[] with candidates[] and n_pending."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        group = _make_pending_group(
            ref_value="ENT_J",
            decision_id="dec_j",
            candidate_id="merch_j",
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
        assert groups[0]["ref_value"] == "ENT_J"
        assert len(groups[0]["candidates"]) == 1
        assert groups[0]["candidates"][0]["decision_id"] == "dec_j"
        assert "n_pending" in parsed["data"]


# ---------------------------------------------------------------------------
# links set
# ---------------------------------------------------------------------------


class TestMerchantLinksSet:
    """Tests for `merchants links set`."""

    @patch("moneybin.cli.commands.merchants.links.get_database")
    @patch("moneybin.services.merchant_links_service.MerchantLinksService.set")
    def test_set_into_calls_service_with_target(
        self,
        mock_set: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """--into <merchant_id> passes target_merchant_id to service."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()

        result = runner.invoke(app, ["set", "dec001", "--into", "merch001aa"])
        assert result.exit_code == 0
        mock_set.assert_called_once_with(
            "dec001", target_merchant_id="merch001aa", decided_by="user"
        )

    @patch("moneybin.cli.commands.merchants.links.get_database")
    @patch("moneybin.services.merchant_links_service.MerchantLinksService.set")
    def test_set_new_calls_service_with_none(
        self,
        mock_set: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        """--new passes target_merchant_id=None to service."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()

        result = runner.invoke(app, ["set", "dec001", "--new"])
        assert result.exit_code == 0
        mock_set.assert_called_once_with(
            "dec001", target_merchant_id=None, decided_by="user"
        )

    def test_set_requires_into_or_new(self) -> None:
        """Invoking set without --into or --new exits 2."""
        result = runner.invoke(app, ["set", "dec001"])
        assert result.exit_code == 2

    def test_set_rejects_both_flags(self) -> None:
        """--into and --new are mutually exclusive → exit 2."""
        result = runner.invoke(app, ["set", "dec001", "--into", "merch001aa", "--new"])
        assert result.exit_code == 2


# ---------------------------------------------------------------------------
# links history
# ---------------------------------------------------------------------------


class TestMerchantLinksHistory:
    """Tests for `merchants links history`."""

    @patch("moneybin.cli.commands.merchants.links.get_database")
    @patch("moneybin.services.merchant_links_service.MerchantLinksService.history")
    def test_history_empty(
        self, mock_history: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """Empty history exits 0."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_history.return_value = []

        result = runner.invoke(app, ["history"])
        assert result.exit_code == 0

    @patch("moneybin.cli.commands.merchants.links.get_database")
    @patch("moneybin.services.merchant_links_service.MerchantLinksService.history")
    def test_history_json_output(
        self, mock_history: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """--output json returns an envelope with decisions[]."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_history.return_value = [
            {
                "decision_id": "dh001",
                "ref_value": "ENT_H",
                "ref_kind": "merchant_entity_id",
                "source_type": "plaid",
                "provider_merchant_name": "Coffee Co",
                "candidate_merchant_id": "merch001aa",
                "status": "accepted",
                "decided_by": "user",
                "decided_at": "2025-06-01T10:00:00",
                "confidence_score": 0.85,
                "match_signals": {"signal": "merchant_entity_id"},
                "match_reason": None,
                "reversed_at": None,
                "reversed_by": None,
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
        assert decisions[0]["signal"] == "merchant_entity_id"

    @patch("moneybin.cli.commands.merchants.links.get_database")
    @patch("moneybin.services.merchant_links_service.MerchantLinksService.history")
    def test_history_limit_option(
        self, mock_history: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """--limit is forwarded to the service."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_history.return_value = []

        runner.invoke(app, ["history", "--limit", "10"])
        mock_history.assert_called_once_with(limit=10)


# ---------------------------------------------------------------------------
# links run
# ---------------------------------------------------------------------------


class TestMerchantLinksRun:
    """Tests for `merchants links run`."""

    @patch("moneybin.cli.commands.merchants.links.get_database")
    @patch("moneybin.services.merchant_links_service.MerchantLinksService.run")
    def test_run_exits_0(self, mock_run: MagicMock, mock_get_db: MagicMock) -> None:
        """Run exits 0 and prints the new-proposal count."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_run.return_value = 3

        result = runner.invoke(app, ["run"])
        assert result.exit_code == 0
        assert "3" in result.output

    @patch("moneybin.cli.commands.merchants.links.get_database")
    @patch("moneybin.services.merchant_links_service.MerchantLinksService.run")
    def test_run_mentions_pending_command(
        self, mock_run: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """Run output hints the user toward `merchants links pending`."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_run.return_value = 2

        result = runner.invoke(app, ["run"])
        assert result.exit_code == 0
        assert "pending" in result.output.lower()

    @patch("moneybin.cli.commands.merchants.links.get_database")
    @patch("moneybin.services.merchant_links_service.MerchantLinksService.run")
    def test_run_zero_proposals_exits_0(
        self, mock_run: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """Run with 0 new proposals still exits 0."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_run.return_value = 0

        result = runner.invoke(app, ["run"])
        assert result.exit_code == 0

    @patch("moneybin.cli.commands.merchants.links.get_database")
    @patch("moneybin.services.merchant_links_service.MerchantLinksService.run")
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
