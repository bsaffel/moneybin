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
from moneybin.services.import_confirmation import (
    ConfirmationRequired,
    ImportConfirmationRequiredError,
    ProposedMapping,
)
from moneybin.services.import_service import ImportResult

runner = CliRunner()


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
            account_proposals=[
                {
                    "source_account_key": "wf-checking",
                    "proposed_account_id": "prov12345678",
                    "is_new": True,
                    "candidates": [
                        {
                            "account_id": "cand87654321",
                            "display_name": "WF Checking",
                            "confidence": 0.5,
                            "signal": "institution_last4",
                        }
                    ],
                }
            ],
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
