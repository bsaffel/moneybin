# ruff: noqa: S101,S106
"""Tests for the smart-import confirmation flow in the CLI.

Tests the --confirm / --mapping flags on `import files` and the new
`import confirm` subcommand. Business logic (resolve_or_confirm) is
tested in the service layer tests; these tests verify CLI wiring only:
argument parsing, exit codes, error messages, and JSON envelope shape.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.import_cmd import app
from moneybin.extractors.confidence import Confidence
from moneybin.services.account_resolution_types import (
    AccountCandidate,
    AccountProposal,
    AccountProposalDict,
)
from moneybin.services.import_confirmation import (
    ConfirmationRequired,
    ImportConfirmationRequiredError,
    ProposedMapping,
    SignConventionProposal,
)
from moneybin.services.import_service import BridgeApplyResult, ImportResult

runner = CliRunner()


def _account_proposal_dict(source_account_key: str) -> AccountProposalDict:
    """One account proposal dict via the real serializer (guarantees the shape)."""
    return AccountProposal(
        source_account_key=source_account_key,
        proposed_account_id="prov12345678",
        is_new=True,
        candidates=(
            AccountCandidate(
                account_id="cand87654321",
                display_name="Checking",
                confidence=0.5,
                signal="name",
            ),
        ),
    ).to_dict()


def _make_import_result(**kwargs: Any) -> ImportResult:
    """Factory for ImportResult with sensible defaults."""
    defaults: dict[str, Any] = {
        "file_path": "test.csv",
        "file_type": "csv",
        "accounts": 1,
        "transactions": 5,
        "import_id": "abc123",
    }
    defaults.update(kwargs)
    return ImportResult(**defaults)


def _make_confirmation_error(
    *,
    tier: str = "medium",
    score: float = 0.55,
    flagged: tuple[str, ...] = ("description",),
    missing_required: tuple[str, ...] = (),
    field_mapping: dict[str, str] | None = None,
    unmapped: tuple[str, ...] = ("Notes",),
) -> ImportConfirmationRequiredError:
    """Build an ImportConfirmationRequiredError with testable defaults."""
    if field_mapping is None:
        field_mapping = {"date": "Date", "amount": "Amount", "description": "Memo"}
    proposed = ProposedMapping(
        field_mapping=field_mapping,
        sample_values={
            "date": ["2025-01-01", "2025-01-02"],
            "amount": ["-50.00", "100.00"],
            "description": ["Coffee", "Paycheck"],
        },
        unmapped_columns=unmapped,
    )
    confidence = Confidence(
        score=score,
        tier=tier,  # type: ignore[arg-type]
        flagged=flagged,
        missing_required=missing_required,
    )
    outcome = ConfirmationRequired(
        channel="tabular",
        confidence=confidence,
        proposed=proposed,
        reason="unknown_layout",
        samples=dict(proposed.sample_values),
    )
    return ImportConfirmationRequiredError(outcome)


def _make_sign_confirmation_error() -> ImportConfirmationRequiredError:
    """Build a PDF sign-convention ImportConfirmationRequiredError."""
    outcome = ConfirmationRequired(
        channel="pdf",
        confidence=Confidence(
            score=1.0,
            tier="high",
            flagged=(),
            missing_required=(),
        ),
        proposed=SignConventionProposal(
            sign_convention="negative_is_income",
            evidence=("Minimum Payment Due", "New Balance"),
            sample_rows=[
                {
                    "description": "COFFEE SHOP",
                    "as_printed": "12.50",
                    "as_recorded": "-12.50",
                }
            ],
        ),
        reason="sign_convention",
        error_message=(
            "This looks like a credit-card statement "
            "(matched: Minimum Payment Due, New Balance). Charges will be "
            "recorded as expenses and payments as credits."
        ),
    )
    return ImportConfirmationRequiredError(outcome)


def test_tabular_sign_recovery_preserves_mapping_overrides() -> None:
    """The sign-confirmation retry repeats the mapping the user already chose."""
    from moneybin.cli.commands.import_cmd import (
        _sign_recovery_commands,  # type: ignore[reportPrivateUsage]  # testing CLI recovery helper
    )

    actions = _sign_recovery_commands(  # type: ignore[reportPrivateUsage]  # testing CLI recovery helper
        "card.csv",
        channel="tabular",
        mapping={"description": "Memo"},
    )

    assert "--mapping description=Memo" in actions[0]


class TestImportFilesConfirmFlow:
    """Verify --confirm / --mapping flags on `import files`."""

    @pytest.fixture
    def mock_db(self, mocker: Any) -> MagicMock:
        return mocker.patch(
            "moneybin.database.get_database",
            return_value=MagicMock(),
        )

    @pytest.fixture
    def mock_import_file(self, mocker: Any) -> MagicMock:
        return mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            return_value=_make_import_result(),
        )

    @pytest.fixture
    def mock_import_file_raises_confirm(self, mocker: Any) -> MagicMock:
        """import_file raises ImportConfirmationRequiredError on first call."""
        return mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            side_effect=_make_confirmation_error(),
        )

    def test_confirm_flag_passes_confirm_true_to_service(
        self,
        mock_db: MagicMock,
        mock_import_file: MagicMock,
        tmp_path: Path,
    ) -> None:
        """--confirm passes confirm=True to service; import proceeds."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        result = runner.invoke(app, ["files", str(csv_file), "--confirm"])

        assert result.exit_code == 0
        call_kwargs = mock_import_file.call_args.kwargs
        assert call_kwargs["confirm"] is True

    def test_no_confirm_flag_passes_confirm_false(
        self,
        mock_db: MagicMock,
        mock_import_file: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Without --confirm, confirm=False is the default."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        result = runner.invoke(app, ["files", str(csv_file), "--account-name", "Chase"])

        assert result.exit_code == 0
        call_kwargs = mock_import_file.call_args.kwargs
        assert call_kwargs["confirm"] is False

    def test_mapping_flag_passed_as_overrides(
        self,
        mock_db: MagicMock,
        mock_import_file: MagicMock,
        tmp_path: Path,
    ) -> None:
        """--mapping description=Memo is forwarded as overrides to service."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        result = runner.invoke(
            app,
            ["files", str(csv_file), "--mapping", "description=Memo"],
        )

        assert result.exit_code == 0
        call_kwargs = mock_import_file.call_args.kwargs
        assert call_kwargs["overrides"] == {"description": "Memo"}

    def test_mapping_flag_multiple_overrides(
        self,
        mock_db: MagicMock,
        mock_import_file: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Multiple --mapping flags combine into a single override dict."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        result = runner.invoke(
            app,
            [
                "files",
                str(csv_file),
                "--mapping",
                "description=Memo",
                "--mapping",
                "date=Date",
            ],
        )

        assert result.exit_code == 0
        call_kwargs = mock_import_file.call_args.kwargs
        assert call_kwargs["overrides"] == {"description": "Memo", "date": "Date"}

    def test_existing_override_flag_still_works(
        self,
        mock_db: MagicMock,
        mock_import_file: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Back-compat: --override field=column still functions as partial-merge."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        result = runner.invoke(
            app,
            ["files", str(csv_file), "--override", "description=Memo"],
        )

        assert result.exit_code == 0
        call_kwargs = mock_import_file.call_args.kwargs
        assert call_kwargs["overrides"] == {"description": "Memo"}

    def test_account_confirmation_envelope_carries_proposals_on_files(
        self,
        mock_db: MagicMock,
        mocker: Any,
        tmp_path: Path,
    ) -> None:
        """`import files` surfaces account_proposals + the binding hint on the gate."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")
        outcome = ConfirmationRequired(
            channel="tabular",
            confidence=Confidence(
                score=1.0, tier="high", flagged=(), missing_required=()
            ),
            proposed=ProposedMapping(
                field_mapping={"description": "Memo"},
                sample_values={},
                unmapped_columns=(),
            ),
            reason="account_confirmation",
            account_proposals=[_account_proposal_dict("checking")],
        )
        mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            side_effect=ImportConfirmationRequiredError(outcome),
        )

        result = runner.invoke(
            app,
            ["files", str(csv_file), "--account-name", "Checking", "--output", "json"],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        data = payload["data"]
        assert data["status"] == "confirmation_required"
        assert data["reason"] == "account_confirmation"
        assert data["account_proposals"][0]["source_account_key"] == "checking"
        assert any("--account-binding" in a for a in payload["actions"])
        # Mapping/accept hints are gated out for account_confirmation (noise —
        # the layout is settled and --accept without a binding loops the gate).
        assert not any("--mapping" in a for a in payload["actions"])

    def test_account_confirmation_tty_renders_proposals(
        self,
        mock_db: MagicMock,
        mocker: Any,
        tmp_path: Path,
    ) -> None:
        """Interactive TTY prompt shows source keys + candidates to bind."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")
        outcome = ConfirmationRequired(
            channel="tabular",
            confidence=Confidence(
                score=1.0, tier="high", flagged=(), missing_required=()
            ),
            proposed=ProposedMapping(
                field_mapping={"description": "Memo"},
                sample_values={},
                unmapped_columns=(),
            ),
            reason="account_confirmation",
            account_proposals=[_account_proposal_dict("checking")],
        )
        mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            side_effect=ImportConfirmationRequiredError(outcome),
        )
        # Force the interactive (TTY) branch — patch the module's sys.
        mock_sys = mocker.patch("moneybin.cli.commands.import_cmd.sys")
        mock_sys.stdout.isatty.return_value = True

        result = runner.invoke(
            app, ["files", str(csv_file), "--account-name", "Checking"]
        )

        assert "Account binding required" in result.output
        assert "checking" in result.output  # the source key
        assert "cand87654321" in result.output  # the candidate account id
        assert "--account-binding" in result.output

    def test_sign_convention_tty_shows_evidence_and_sign_recovery(
        self,
        mock_db: MagicMock,
        mocker: Any,
        tmp_path: Path,
    ) -> None:
        """Interactive prompt renders a card sign flip honestly.

        It must show the matched disclosures and the printed-vs-recorded rows,
        and name the --confirm / --sign recovery — never "Validation failed"
        (this is a proposal) or --mapping (a dead-end loop for a PDF).
        """
        pdf_file = tmp_path / "statement.pdf"
        pdf_file.write_bytes(b"%PDF-1.4 fake\n")
        mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            side_effect=_make_sign_confirmation_error(),
        )
        mock_sys = mocker.patch("moneybin.cli.commands.import_cmd.sys")
        mock_sys.stdout.isatty.return_value = True

        result = runner.invoke(app, ["files", str(pdf_file)])

        # Evidence + printed-vs-recorded rows are visible.
        assert "Minimum Payment Due" in result.output
        assert "12.50" in result.output
        assert "-12.50" in result.output
        # Honest recovery, no mislabeling.
        assert "--confirm" in result.output
        assert "--sign negative_is_expense" in result.output
        assert "Validation failed" not in result.output
        assert "--mapping" not in result.output

    def test_sign_convention_json_envelope_carries_evidence_and_recovery(
        self,
        mock_db: MagicMock,
        mocker: Any,
        tmp_path: Path,
    ) -> None:
        """--output json surfaces the sign proposal + honest recovery actions."""
        pdf_file = tmp_path / "statement.pdf"
        pdf_file.write_bytes(b"%PDF-1.4 fake\n")
        mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            side_effect=_make_sign_confirmation_error(),
        )

        result = runner.invoke(app, ["files", str(pdf_file), "--output", "json"])

        assert result.exit_code == 0
        payload = json.loads(result.output)
        data = payload["data"]
        assert data["reason"] == "sign_convention"
        assert data["sign_convention"] == "negative_is_income"
        assert data["sign_evidence"] == ["Minimum Payment Due", "New Balance"]
        assert data["sign_sample_rows"][0]["as_recorded"] == "-12.50"
        actions = payload["actions"]
        assert any("--confirm" in a for a in actions)
        assert any("--sign negative_is_expense" in a for a in actions)
        # No tabular mapping/validation/preview language for a sign flip.
        assert not any("Validation failed" in a for a in actions)
        assert not any("--mapping" in a for a in actions)
        assert not any("import preview" in a for a in actions)

    def test_unknown_layout_non_tty_emits_json_envelope(
        self,
        mock_db: MagicMock,
        mock_import_file_raises_confirm: MagicMock,
        tmp_path: Path,
    ) -> None:
        """When --output json and service raises, emit confirmation_required envelope; exit 0."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        result = runner.invoke(
            app,
            ["files", str(csv_file), "--output", "json"],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        # Top-level envelope shape must match MCP (build_envelope output).
        assert payload["status"] == "ok"
        assert "summary" in payload
        assert "data" in payload
        assert "actions" in payload
        assert payload["data"]["status"] == "confirmation_required"
        assert payload["data"]["channel"] == "tabular"
        assert "proposed_mapping" in payload["data"]
        assert "samples" in payload["data"]

    def test_unknown_layout_json_envelope_includes_tier(
        self,
        mock_db: MagicMock,
        mock_import_file_raises_confirm: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Confirmation envelope includes tier and score fields."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        result = runner.invoke(
            app,
            ["files", str(csv_file), "--output", "json"],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        # Top-level envelope shape must match MCP (build_envelope output).
        assert payload["status"] == "ok"
        assert "summary" in payload
        assert "data" in payload
        assert "actions" in payload
        data = payload["data"]
        assert "tier" in data
        assert "score" in data
        assert "reason" in data
        assert "flagged" in data
        assert "missing_required" in data
        assert "unmapped_columns" in data

    def test_low_tier_includes_missing_required_in_envelope(
        self,
        mock_db: MagicMock,
        mocker: Any,
        tmp_path: Path,
    ) -> None:
        """Low-tier confirmation envelope includes missing_required fields."""
        mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            side_effect=_make_confirmation_error(
                tier="low",
                score=0.2,
                missing_required=("amount", "date"),
            ),
        )
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Narration,Notes\nCoffee,memo\n")

        result = runner.invoke(
            app,
            ["files", str(csv_file), "--output", "json"],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        # Top-level envelope shape must match MCP (build_envelope output).
        assert payload["status"] == "ok"
        assert "summary" in payload
        assert "data" in payload
        assert "actions" in payload
        data = payload["data"]
        assert data["status"] == "confirmation_required"
        assert (
            "amount" in data["missing_required"] or "date" in data["missing_required"]
        )

    def test_confirm_flag_actor_kind_human(
        self,
        mock_db: MagicMock,
        mock_import_file: MagicMock,
        tmp_path: Path,
    ) -> None:
        """CLI always uses actor_kind='human'."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        result = runner.invoke(app, ["files", str(csv_file), "--confirm"])

        assert result.exit_code == 0
        call_kwargs = mock_import_file.call_args.kwargs
        assert call_kwargs.get("actor_kind") == "human"

    def test_single_path_uses_import_file_not_batch(
        self,
        mock_db: MagicMock,
        mocker: Any,
        tmp_path: Path,
    ) -> None:
        """Single-path + --confirm uses import_file directly (not import_files)."""
        mock_file = mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            return_value=_make_import_result(),
        )
        mock_batch = mocker.patch(
            "moneybin.services.import_service.ImportService.import_files",
        )

        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        runner.invoke(app, ["files", str(csv_file), "--confirm"])

        mock_file.assert_called_once()
        mock_batch.assert_not_called()


class TestImportConfirmCommand:
    """Verify the `moneybin import confirm` subcommand."""

    @pytest.fixture
    def mock_db(self, mocker: Any) -> MagicMock:
        return mocker.patch(
            "moneybin.database.get_database",
            return_value=MagicMock(),
        )

    @pytest.fixture
    def mock_import_file(self, mocker: Any) -> MagicMock:
        return mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            return_value=_make_import_result(),
        )

    def test_confirm_with_accept_loads(
        self,
        mock_db: MagicMock,
        mock_import_file: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Import confirm <file> --accept calls service with confirm=True."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        result = runner.invoke(app, ["confirm", str(csv_file), "--accept"])

        assert result.exit_code == 0
        call_kwargs = mock_import_file.call_args.kwargs
        assert call_kwargs["confirm"] is True
        assert call_kwargs.get("actor_kind") == "human"

    def test_confirm_with_mapping_loads(
        self,
        mock_db: MagicMock,
        mock_import_file: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Import confirm <file> --mapping description=Memo calls service with override."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        result = runner.invoke(
            app, ["confirm", str(csv_file), "--mapping", "description=Memo"]
        )

        assert result.exit_code == 0
        call_kwargs = mock_import_file.call_args.kwargs
        assert call_kwargs["overrides"] == {"description": "Memo"}

    def test_confirm_sign_is_distinct_from_mapping_accept(
        self,
        mock_db: MagicMock,
        mock_import_file: MagicMock,
        tmp_path: Path,
    ) -> None:
        """--confirm-sign, not --accept, carries the tabular sign decision."""
        csv_file = tmp_path / "card.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        result = runner.invoke(
            app, ["confirm", str(csv_file), "--accept", "--confirm-sign"]
        )

        assert result.exit_code == 0
        call_kwargs = mock_import_file.call_args.kwargs
        assert call_kwargs["confirm"] is True
        assert call_kwargs["human_sign_confirmation"] is True

    def test_bridge_response_requires_explicit_confirm(self, tmp_path: Path) -> None:
        """A JSON bridge recipe cannot load until the terminal user confirms it."""
        pdf_file = tmp_path / "statement.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n")
        response_file = tmp_path / "response.json"
        response_file.write_text('{"recipe": {}, "rows": []}')

        result = runner.invoke(
            app,
            ["confirm", str(pdf_file), "--bridge-response", str(response_file)],
        )

        assert result.exit_code != 0
        assert "requires --confirm" in result.output

    def test_confirmed_bridge_response_loads(
        self, mock_db: MagicMock, mocker: Any, tmp_path: Path
    ) -> None:
        """The CLI sends an explicit human confirmation to the shared service."""
        pdf_file = tmp_path / "statement.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n")
        response_file = tmp_path / "response.json"
        response_file.write_text('{"recipe": {}, "rows": []}')
        apply = mocker.patch(
            "moneybin.services.import_service.ImportService.apply_pdf_bridge_response",
            return_value=BridgeApplyResult(
                outcome="applied",
                import_id="bridge123",
                rows_loaded=2,
                format_name="chase_abc123",
                expected_row_count=2,
                actual_row_count=2,
                rows_diverged=False,
            ),
        )
        mocker.patch(
            "moneybin.services.inbox_service.InboxService.for_active_profile_no_db"
        )

        result = runner.invoke(
            app,
            [
                "confirm",
                str(pdf_file),
                "--bridge-response",
                str(response_file),
                "--confirm",
                "--output",
                "json",
            ],
        )

        assert result.exit_code == 0
        assert apply.call_args.kwargs["confirm"] is True
        assert apply.call_args.args[1] == {
            "recipe": {},
            "rows": [],
        }
        assert json.loads(result.output)["data"]["status"] == "applied"

    def test_requires_accept_or_mapping(
        self,
        tmp_path: Path,
    ) -> None:
        """No --accept and no --mapping exits non-zero with actionable error."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        result = runner.invoke(app, ["confirm", str(csv_file)])

        assert result.exit_code != 0

    def test_file_not_found_exits_with_error(self, tmp_path: Path) -> None:
        """Confirm exits 1 when the file does not exist."""
        result = runner.invoke(
            app, ["confirm", str(tmp_path / "missing.csv"), "--accept"]
        )
        assert result.exit_code == 1

    def test_confirm_no_save_format(
        self,
        mock_db: MagicMock,
        mock_import_file: MagicMock,
        tmp_path: Path,
    ) -> None:
        """--no-save-format passes save_format=False to service."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        result = runner.invoke(
            app, ["confirm", str(csv_file), "--accept", "--no-save-format"]
        )

        assert result.exit_code == 0
        call_kwargs = mock_import_file.call_args.kwargs
        assert call_kwargs["save_format"] is False

    def test_confirm_json_output(
        self,
        mock_db: MagicMock,
        mock_import_file: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Import confirm --accept --output json emits a JSON envelope."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        result = runner.invoke(
            app, ["confirm", str(csv_file), "--accept", "--output", "json"]
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        # Top-level envelope shape must match MCP (build_envelope output).
        assert payload["status"] == "ok"
        assert "summary" in payload
        assert "data" in payload
        assert "actions" in payload
        data = payload["data"]
        assert "rows_loaded" in data
        assert "import_id" in data
        # Success branch MUST carry the same `data.status` discriminant the
        # confirmation_required branch uses; scripted propose→review→confirm
        # loops branch on `data.status`, not on exit code.
        assert data["status"] == "imported"

    def test_confirm_accept_and_mapping_together(
        self,
        mock_db: MagicMock,
        mock_import_file: MagicMock,
        tmp_path: Path,
    ) -> None:
        """--accept + --mapping: mapping takes precedence via overrides."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        result = runner.invoke(
            app,
            ["confirm", str(csv_file), "--accept", "--mapping", "description=Memo"],
        )

        assert result.exit_code == 0
        call_kwargs = mock_import_file.call_args.kwargs
        assert call_kwargs["overrides"] == {"description": "Memo"}
        assert call_kwargs["confirm"] is True

    def test_account_binding_parsed_and_forwarded(
        self,
        mock_db: MagicMock,
        mock_import_file: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Repeatable --account-binding folds into an account_bindings map."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        result = runner.invoke(
            app,
            [
                "confirm",
                str(csv_file),
                "--accept",
                "--account-binding",
                "wf-checking=acct123456",
                "--account-binding",
                "wf-savings=new",
            ],
        )

        assert result.exit_code == 0
        call_kwargs = mock_import_file.call_args.kwargs
        assert call_kwargs["account_bindings"] == {
            "wf-checking": "acct123456",
            "wf-savings": "new",
        }

    def test_account_meta_parsed_into_nested_map(
        self,
        mock_db: MagicMock,
        mock_import_file: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Repeatable --account-meta source_key:field=value nests per source key."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        result = runner.invoke(
            app,
            [
                "confirm",
                str(csv_file),
                "--accept",
                "--account-binding",
                "wf-checking=new",
                "--account-meta",
                "wf-checking:display_name=WF Checking",
                "--account-meta",
                "wf-checking:last_four=4267",
            ],
        )

        assert result.exit_code == 0
        call_kwargs = mock_import_file.call_args.kwargs
        assert call_kwargs["account_metadata"] == {
            "wf-checking": {"display_name": "WF Checking", "last_four": "4267"}
        }

    def test_account_meta_invalid_format_exits(
        self,
        mock_db: MagicMock,
        mock_import_file: MagicMock,
        tmp_path: Path,
    ) -> None:
        """--account-meta without the source_key:field=value shape exits non-zero."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        result = runner.invoke(
            app,
            ["confirm", str(csv_file), "--accept", "--account-meta", "no-colon=here"],
        )

        assert result.exit_code != 0

    def test_account_confirmation_envelope_carries_proposals(
        self,
        mock_db: MagicMock,
        mocker: Any,
        tmp_path: Path,
    ) -> None:
        """An account_confirmation surfaces account_proposals in the JSON envelope."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")
        outcome = ConfirmationRequired(
            channel="tabular",
            confidence=Confidence(
                score=1.0, tier="high", flagged=(), missing_required=()
            ),
            proposed=ProposedMapping(
                field_mapping={"description": "Memo"},
                sample_values={},
                unmapped_columns=(),
            ),
            reason="account_confirmation",
            account_proposals=[_account_proposal_dict("wf-checking")],
        )
        mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            side_effect=ImportConfirmationRequiredError(outcome),
        )

        result = runner.invoke(
            app, ["confirm", str(csv_file), "--accept", "--output", "json"]
        )

        payload = json.loads(result.output)
        data = payload["data"]
        assert data["status"] == "confirmation_required"
        assert data["reason"] == "account_confirmation"
        assert data["account_proposals"][0]["source_account_key"] == "wf-checking"
        assert any("--account-binding" in a for a in payload["actions"])
        # Mapping/accept hints gated out for account_confirmation.
        assert not any("--mapping" in a for a in payload["actions"])

    def test_account_confirmation_tty_renders_proposals(
        self,
        mock_db: MagicMock,
        mocker: Any,
        tmp_path: Path,
    ) -> None:
        """`import confirm` TTY error path shows proposals + binding hint, not --mapping."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")
        outcome = ConfirmationRequired(
            channel="tabular",
            confidence=Confidence(
                score=1.0, tier="high", flagged=(), missing_required=()
            ),
            proposed=ProposedMapping(
                field_mapping={"description": "Memo"},
                sample_values={},
                unmapped_columns=(),
            ),
            reason="account_confirmation",
            account_proposals=[_account_proposal_dict("wf-checking")],
        )
        mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            side_effect=ImportConfirmationRequiredError(outcome),
        )
        mock_sys = mocker.patch("moneybin.cli.commands.import_cmd.sys")
        mock_sys.stdout.isatty.return_value = True

        result = runner.invoke(app, ["confirm", str(csv_file), "--accept"])

        assert result.exit_code == 1
        # The proposals (source key + candidate) render so the user sees what to
        # bind. The --account-binding hint itself is a logger.info line (real
        # stderr, not captured here under the sys mock).
        assert "Account binding required" in result.output
        assert "wf-checking" in result.output
        assert "cand87654321" in result.output
