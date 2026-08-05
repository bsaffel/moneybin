# ruff: noqa: S101,S106
"""Tests for the smart-import confirmation flow in the CLI.

Tests the --confirm / --mapping flags on `import files` and the new
`import confirm` subcommand. Business logic (resolve_or_confirm) is
tested in the service layer tests; these tests verify CLI wiring only:
argument parsing, exit codes, error messages, and JSON envelope shape.
"""

from __future__ import annotations

import json
import shlex
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
    Channel,
    ConfirmationRequired,
    ImportConfirmationRequiredError,
    ProposedMapping,
    SignConventionProposal,
)
from moneybin.services.import_service import (
    BridgeApplyResult,
    CreatedAccount,
    ImportResult,
)

runner = CliRunner()

_MINTED = (CreatedAccount(account_id="acct00000001", display_name="WF Checking"),)


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
    ).to_dict(proposal_ref="@0")


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
    reason: str = "unknown_layout",
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
        reason=reason,  # type: ignore[arg-type]  # test fixture accepts every reason
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


def test_tabular_sign_recoveries_preserve_confirmation_inputs() -> None:
    """Both sign choices losslessly serialize every public confirmation input."""
    from moneybin.cli.commands.import_cmd import (
        _sign_recovery_commands,  # type: ignore[reportPrivateUsage]  # testing CLI recovery helper
    )

    actions = _sign_recovery_commands(  # type: ignore[reportPrivateUsage]  # testing CLI recovery helper
        "Owner's card.csv",
        channel="tabular",
        accept=False,
        mapping={"description": "Merchant Name"},
        save_format=False,
        account_id="acct explicit",
        account_name="Owner's Card",
        account_bindings={"settled key": "acct existing", "minted card": "new"},
        account_metadata={
            "minted card": {
                "display_name": "Owner's Card",
                "last_four": "4267",
            }
        },
    )

    assert len(actions) == 2
    approve_command = actions[0].split(": ", 1)[1]
    native_command = actions[1].split(": ", 1)[1]
    approve_tokens = shlex.split(approve_command)
    native_tokens = shlex.split(native_command)

    for command, tokens in (
        (approve_command, approve_tokens),
        (native_command, native_tokens),
    ):
        assert tokens[:4] == ["moneybin", "import", "confirm", "Owner's card.csv"]
        assert "--accept" not in tokens
        assert "--no-save-format" in tokens
        assert tokens[tokens.index("--account-id") + 1] == "acct explicit"
        assert tokens[tokens.index("--account-name") + 1] == "Owner's Card"
        assert tokens[tokens.index("--mapping") + 1] == "description=Merchant Name"
        assert {
            tokens[i + 1] for i, arg in enumerate(tokens) if arg == "--account-binding"
        } == {"settled key=acct existing", "minted card=new"}
        assert {
            tokens[i + 1] for i, arg in enumerate(tokens) if arg == "--account-meta"
        } == {
            "minted card:display_name=Owner's Card",
            "minted card:last_four=4267",
        }
        assert "human_sign_confirmation" not in command

    assert "--confirm-sign" in approve_tokens
    assert "--sign" not in approve_tokens
    assert "--confirm-sign" not in native_tokens
    assert native_tokens[native_tokens.index("--sign") + 1] == "negative_is_expense"


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

    def test_a_created_account_is_named_in_the_json_envelope(
        self,
        mock_db: MagicMock,
        mocker: Any,
        tmp_path: Path,
    ) -> None:
        """A mint no longer stops the import, so the envelope has to name it.

        Structured, not prose: an agent that just created an account needs to
        answer "which one, and was that right?" without parsing a log line.
        """
        mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            return_value=_make_import_result(accounts_created=_MINTED),
        )
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        result = runner.invoke(
            app, ["files", str(csv_file), "--confirm", "--output", "json"]
        )

        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert payload["data"]["files"][0]["accounts_created"] == [
            {"account_id": "acct00000001", "display_name": "WF Checking"}
        ]

    def test_a_created_account_is_named_in_text_output(
        self,
        mock_db: MagicMock,
        mocker: Any,
        tmp_path: Path,
    ) -> None:
        """The human running the bare command sees the account by name.

        The JSON branch alone would leave the default invocation — the one a
        person actually types — silent about an account that just appeared.
        """
        mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            return_value=_make_import_result(accounts_created=_MINTED),
        )
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        result = runner.invoke(app, ["files", str(csv_file), "--confirm"])

        assert result.exit_code == 0
        assert "WF Checking" in result.output

    def test_an_import_that_created_nothing_says_nothing(
        self,
        mock_db: MagicMock,
        mock_import_file: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Re-importing into a known account is the common case; keep it quiet.

        A line that prints on every import stops carrying the signal that
        something new appeared.
        """
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        result = runner.invoke(
            app, ["files", str(csv_file), "--confirm", "--output", "json"]
        )

        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert "accounts_created" not in payload["data"]["files"][0]

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

    def test_ofx_account_confirmation_names_a_command_that_runs(
        self,
        mock_db: MagicMock,
        mocker: Any,
        tmp_path: Path,
    ) -> None:
        """An OFX gate names `import confirm`, the same command every gate names.

        The premise this test used to carry — that `import confirm` would
        bounce an OFX with a usage error — is not true of the CLI. It takes a
        file path, not a staged preview id (that is the MCP tool), and
        `moneybin import confirm <file>.ofx --accept` imports. `--accept`
        ratifies nothing on a channel with no mapping, but it satisfies the
        command's require-an-action guard, which is exactly the form
        `InboxService` has always emitted for an account gate on any channel.

        Naming `import files` instead had a cost beyond the second vocabulary:
        only `import confirm` calls `archive_confirmed_file`, so answering a
        pending inbox file with `import files` left it in `pending/` to be
        offered again forever.
        """
        ofx_file = tmp_path / "statement.ofx"
        ofx_file.write_text("OFXHEADER:100\n")
        outcome = ConfirmationRequired(
            channel="ofx",
            confidence=Confidence(
                score=1.0, tier="high", flagged=(), missing_required=()
            ),
            proposed=ProposedMapping(
                field_mapping={}, sample_values={}, unmapped_columns=()
            ),
            reason="account_confirmation",
            account_proposals=[_account_proposal_dict("1111")],
        )
        mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            side_effect=ImportConfirmationRequiredError(outcome),
        )

        result = runner.invoke(app, ["files", str(ofx_file), "--output", "json"])

        assert result.exit_code == 0
        payload = json.loads(result.output)
        recovery = next(a for a in payload["actions"] if "--account-binding" in a)
        assert "moneybin import confirm" in recovery
        assert "import files" not in recovery
        assert "1111=<account_id|new>" in recovery
        # Required by the command's own guard, and what InboxService emits.
        assert "--accept" in recovery

    @pytest.mark.parametrize("suffix", [".ofx", ".pdf", ".csv"])
    def test_the_account_recovery_it_prints_actually_parses(
        self,
        mock_db: MagicMock,
        mocker: Any,
        tmp_path: Path,
        suffix: str,
    ) -> None:
        """Run the printed command back through the CLI, on every channel.

        The test this sits beside asserted which command was named and never
        ran it, so its claim that the other one "would bounce with a usage
        error" went unchecked for as long as it was false. Re-invoking is what
        makes the assertion about the command rather than about the string.
        """
        source = tmp_path / f"statement{suffix}"
        source.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")
        channels: dict[str, Channel] = {
            ".ofx": "ofx",
            ".pdf": "pdf",
            ".csv": "tabular",
        }
        channel = channels[suffix]
        outcome = ConfirmationRequired(
            channel=channel,
            confidence=Confidence(
                score=1.0, tier="high", flagged=(), missing_required=()
            ),
            proposed=ProposedMapping(
                field_mapping={}, sample_values={}, unmapped_columns=()
            ),
            reason="account_confirmation",
            account_proposals=[_account_proposal_dict("1111")],
        )
        gated = mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            side_effect=ImportConfirmationRequiredError(outcome),
        )
        payload = json.loads(
            runner.invoke(app, ["files", str(source), "--output", "json"]).output
        )
        recovery = next(a for a in payload["actions"] if "--account-binding" in a)

        # Strip the surrounding prose, then fill the placeholder as a user would.
        quoted = recovery[recovery.index("moneybin") : recovery.index("` ")]
        argv = [a.replace("<account_id|new>", "new") for a in shlex.split(quoted)[1:]]
        assert argv[0] == "import"
        gated.side_effect = None
        gated.return_value = _make_import_result()
        mocker.patch(
            "moneybin.services.inbox_service.InboxService.for_active_profile_no_db"
        )

        rerun = runner.invoke(app, argv[1:])

        assert rerun.exit_code == 0, f"{channel}: {rerun.output}"
        assert gated.call_args.kwargs["account_bindings"] == {"1111": "new"}

    def test_the_account_recovery_carries_the_institution_it_was_given(
        self,
        mock_db: MagicMock,
        mocker: Any,
        tmp_path: Path,
    ) -> None:
        """An --institution the user had to supply survives into the printed recovery.

        `resolve_institution` raises before the account gate, so the only way to
        reach that gate on such a file is `import files x.ofx --institution
        chase`. The recovery printed there dropped the flag, so pasting it hit
        the institution error again — a command MoneyBin printed that does not
        run, which is the defect the unified recovery was meant to end.

        Re-invokes rather than asserting on the string alone: the flag has to
        reach the service, not just appear in the text.
        """
        ofx_file = tmp_path / "statement.ofx"
        ofx_file.write_text("OFXHEADER:100\n")
        outcome = ConfirmationRequired(
            channel="ofx",
            confidence=Confidence(
                score=1.0, tier="high", flagged=(), missing_required=()
            ),
            proposed=ProposedMapping(
                field_mapping={}, sample_values={}, unmapped_columns=()
            ),
            reason="account_confirmation",
            account_proposals=[_account_proposal_dict("1111")],
        )
        gated = mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            side_effect=ImportConfirmationRequiredError(outcome),
        )
        payload = json.loads(
            runner.invoke(
                app,
                ["files", str(ofx_file), "--institution", "chase", "--output", "json"],
            ).output
        )
        recovery = next(a for a in payload["actions"] if "--account-binding" in a)

        assert "--institution chase" in recovery

        quoted = recovery[recovery.index("moneybin") : recovery.index("` ")]
        argv = [a.replace("<account_id|new>", "new") for a in shlex.split(quoted)[1:]]
        gated.side_effect = None
        gated.return_value = _make_import_result()
        mocker.patch(
            "moneybin.services.inbox_service.InboxService.for_active_profile_no_db"
        )

        rerun = runner.invoke(app, argv[1:])

        assert rerun.exit_code == 0, rerun.output
        assert gated.call_args.kwargs["institution"] == "chase"
        assert gated.call_args.kwargs["account_bindings"] == {"1111": "new"}

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

    def test_consumed_header_actions_do_not_prescribe_another_mapping(
        self,
        mock_db: MagicMock,
        mocker: Any,
        tmp_path: Path,
    ) -> None:
        """No mapping restores a row already read as column names.

        The reason was wired into the MCP action builders and not the CLI, so
        both catch blocks fell into the generic branch and told the user to
        retry with another mapping — which returns the same refusal.
        """
        csv_file = tmp_path / "headerless.csv"
        csv_file.write_text("2026-01-05,-4.50,Coffee\n")
        mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            side_effect=_make_confirmation_error(reason="header_row_consumed"),
        )

        result = runner.invoke(app, ["files", str(csv_file), "--output", "json"])

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["data"]["reason"] == "header_row_consumed"
        actions = payload["actions"]
        assert any("Add a header row" in a for a in actions), actions
        assert not any("--mapping <field>=<column>" in a for a in actions), actions
        assert not any("--confirm to accept" in a for a in actions), actions

    def test_unreadable_date_json_actions_name_both_recoveries(
        self,
        mock_db: MagicMock,
        mocker: Any,
        tmp_path: Path,
    ) -> None:
        """An unreadable date has two causes, and the actions must name both.

        `--date-format` alone is a dead end when a status column claimed the
        date alias: map_columns re-runs detection against whatever an override
        names, so a column correction recovers that file while a format aimed
        at the wrong column is refused again. `--mapping` alone is a dead end
        when the mapped column is right and its format simply has no candidate.
        The generic `--confirm` accept hint helps in neither case — it re-hits
        this gate, which fires ahead of resolve_or_confirm whatever signal it
        carries.
        """
        csv_file = tmp_path / "compact.csv"
        csv_file.write_text("Date,Amount,Memo\n20260105,-50.00,Coffee\n")
        mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            side_effect=_make_confirmation_error(reason="unreadable_date"),
        )

        result = runner.invoke(app, ["files", str(csv_file), "--output", "json"])

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["data"]["reason"] == "unreadable_date"
        actions = payload["actions"]
        recovery = [a for a in actions if "--date-format" in a]
        assert recovery, actions
        # One action carries both, so neither cause is left without a fix.
        assert any("--mapping transaction_date=" in a for a in recovery), recovery
        assert not any("--confirm to accept" in a for a in actions)

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

    def test_confirm_sign_flag_is_forwarded_with_confirm(
        self,
        mock_db: MagicMock,
        mock_import_file: MagicMock,
        tmp_path: Path,
    ) -> None:
        """`import files --confirm --confirm-sign` reaches the service unchanged."""
        csv_file = tmp_path / "card.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        result = runner.invoke(
            app,
            ["files", str(csv_file), "--confirm", "--confirm-sign"],
        )

        assert result.exit_code == 0
        call_kwargs = mock_import_file.call_args.kwargs
        assert call_kwargs["confirm"] is True
        assert call_kwargs["human_sign_confirmation"] is True

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

    def test_confirm_names_the_account_it_created(
        self,
        mock_db: MagicMock,
        mocker: Any,
        tmp_path: Path,
    ) -> None:
        """Answering the gate is the moment an account is most likely to be new.

        `import confirm` is where a caller lands after binding an identity, so
        it owes the same report as `import files` — on both output formats.
        """
        mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            return_value=_make_import_result(accounts_created=_MINTED),
        )
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        text = runner.invoke(app, ["confirm", str(csv_file), "--accept"])
        assert text.exit_code == 0
        assert "WF Checking" in text.output

        as_json = runner.invoke(
            app, ["confirm", str(csv_file), "--accept", "--output", "json"]
        )
        assert as_json.exit_code == 0
        payload = json.loads(as_json.stdout)
        assert payload["data"]["accounts_created"] == [
            {"account_id": "acct00000001", "display_name": "WF Checking"}
        ]

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

    def test_generated_sign_recoveries_parse_to_lossless_service_arguments(
        self,
        mock_db: MagicMock,
        mock_import_file: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Both generated human choices execute through the real confirm parser."""
        from moneybin.cli.commands.import_cmd import (
            _sign_recovery_commands,  # type: ignore[reportPrivateUsage]  # testing CLI recovery helper
        )

        csv_file = tmp_path / "Owner's card.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,50.00,Coffee\n")
        actions = _sign_recovery_commands(  # type: ignore[reportPrivateUsage]  # testing CLI recovery helper
            str(csv_file),
            channel="tabular",
            accept=False,
            mapping={"description": "Merchant Name"},
            save_format=False,
            account_id="acct explicit",
            account_name="Owner's Card",
            account_bindings={"settled key": "acct existing", "minted card": "new"},
            account_metadata={
                "minted card": {
                    "display_name": "Owner's Card",
                    "last_four": "4267",
                }
            },
        )

        for index, expected_sign in (
            (0, None),
            (1, "negative_is_expense"),
        ):
            command = actions[index].split(": ", 1)[1]
            tokens = shlex.split(command)
            result = runner.invoke(app, tokens[2:])

            assert result.exit_code == 0, result.output
            call_kwargs = mock_import_file.call_args.kwargs
            assert call_kwargs["confirm"] is False
            assert call_kwargs["overrides"] == {"description": "Merchant Name"}
            assert call_kwargs["save_format"] is False
            assert call_kwargs["account_id"] == "acct explicit"
            assert call_kwargs["account_name"] == "Owner's Card"
            assert call_kwargs["account_bindings"] == {
                "settled key": "acct existing",
                "minted card": "new",
            }
            assert call_kwargs["account_metadata"] == {
                "minted card": {
                    "display_name": "Owner's Card",
                    "last_four": "4267",
                }
            }
            assert call_kwargs.get("sign") == expected_sign
            if index == 0:
                assert call_kwargs["human_sign_confirmation"] is True
            else:
                assert "human_sign_confirmation" not in call_kwargs
            mock_import_file.reset_mock()

    def test_confirm_sign_and_explicit_sign_are_mutually_exclusive(
        self,
        mock_db: MagicMock,
        mock_import_file: MagicMock,
        tmp_path: Path,
    ) -> None:
        """One invocation cannot approve and override the sign simultaneously."""
        csv_file = tmp_path / "card.csv"
        csv_file.write_text("Date,Amount\n2025-01-01,50.00\n")

        result = runner.invoke(
            app,
            [
                "confirm",
                str(csv_file),
                "--accept",
                "--confirm-sign",
                "--sign",
                "negative_is_expense",
            ],
        )

        assert result.exit_code != 0
        assert "alternate sign decisions" in result.output
        mock_import_file.assert_not_called()

    def test_bridge_response_rejects_tabular_sign_override(
        self,
        tmp_path: Path,
    ) -> None:
        """The tabular sign option cannot be silently ignored by PDF bridge apply."""
        pdf_file = tmp_path / "statement.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n")
        response_file = tmp_path / "response.json"
        response_file.write_text('{"recipe": {}, "rows": []}')

        result = runner.invoke(
            app,
            [
                "confirm",
                str(pdf_file),
                "--bridge-response",
                str(response_file),
                "--confirm",
                "--sign",
                "negative_is_expense",
            ],
        )

        assert result.exit_code != 0
        assert "--bridge-response cannot be combined" in result.output
        assert "--sign" in result.output

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

    def test_bridge_response_can_answer_the_account_gate_it_raises(
        self, mock_db: MagicMock, mocker: Any, tmp_path: Path
    ) -> None:
        """The bridge raises the account gate, so it must accept the answer.

        `apply_pdf_bridge_response` gates on account identity like every other
        channel and takes `account_bindings` to resolve it — the MCP path
        forwards them. The CLI rejected the flag outright, so the one surface
        where a human answers this had no way to: the same command re-raised
        the same gate however it was re-run.
        """
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
                "--account-binding",
                "@0=new",
            ],
        )

        assert result.exit_code == 0, result.output
        assert apply.call_args.kwargs["account_bindings"] == {"@0": "new"}

    def test_bridge_response_still_refuses_the_tabular_account_flags(
        self, tmp_path: Path
    ) -> None:
        """--account-name and --account-meta stay refused; only bindings opened.

        A PDF's account identity comes from the statement, not from a column,
        and neither flag reaches the bridge service. Refusing them is the
        contract; refusing --account-binding alongside them was the bug.
        """
        pdf_file = tmp_path / "statement.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n")
        response_file = tmp_path / "response.json"
        response_file.write_text('{"recipe": {}, "rows": []}')

        for flag, value in (("--account-name", "Chase"), ("--account-meta", "k:a=b")):
            result = runner.invoke(
                app,
                [
                    "confirm",
                    str(pdf_file),
                    "--bridge-response",
                    str(response_file),
                    "--confirm",
                    flag,
                    value,
                ],
            )
            assert result.exit_code != 0, flag
            # …and the refusal advertises the one account flag that IS accepted,
            # so a caller who reached for the wrong one is pointed at it.
            assert "--account-binding only" in result.output, flag

    def test_institution_override_reaches_the_service(
        self, mock_db: MagicMock, mocker: Any, tmp_path: Path
    ) -> None:
        """`import confirm` carries --institution, because the gate it answers can need it.

        `resolve_institution` raises before the account gate, so an OFX whose
        issuer is underivable from <FID>, <ORG>, and the filename reaches the
        account confirmation only on a re-run that already carries
        --institution. Without this option the command that answers the gate
        cannot also satisfy the check that precedes it.
        """
        ofx_file = tmp_path / "statement.ofx"
        ofx_file.write_text("OFXHEADER:100\n")
        imported = mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            return_value=_make_import_result(),
        )
        mocker.patch(
            "moneybin.services.inbox_service.InboxService.for_active_profile_no_db"
        )

        result = runner.invoke(
            app, ["confirm", str(ofx_file), "--accept", "--institution", "chase"]
        )

        assert result.exit_code == 0, result.output
        assert imported.call_args.kwargs["institution"] == "chase"

    def test_bridge_response_refuses_the_institution_override(
        self, tmp_path: Path
    ) -> None:
        """--institution is refused with --bridge-response rather than dropped.

        `apply_pdf_bridge_response` has no institution parameter — the recipe
        carries the format. Accepting the flag here would silently ignore it,
        which is the failure mode this whole option exists to close.

        Asserts the refusal sentence, not the exit code: with the guard removed
        this command still exits non-zero, on the unrelated missing-database
        error, so an exit-code check passes either way and pins nothing.
        """
        pdf_file = tmp_path / "statement.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n")
        response_file = tmp_path / "response.json"
        response_file.write_text('{"recipe": {}, "rows": []}')

        result = runner.invoke(
            app,
            [
                "confirm",
                str(pdf_file),
                "--bridge-response",
                str(response_file),
                "--confirm",
                "--institution",
                "chase",
            ],
        )

        assert result.exit_code != 0
        assert "--account-name, --account-meta, or --institution" in result.output

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

    def test_account_binding_accepts_a_positional_proposal_ref(
        self,
        mock_db: MagicMock,
        mock_import_file: MagicMock,
        tmp_path: Path,
    ) -> None:
        """``@0`` reaches the service intact — the CLI must not eat the sigil.

        The gate surfaces a proposal_ref because source_account_key masks on
        the MCP side; the CLI answers with the same referent so both surfaces
        take one vocabulary. Click treats a leading ``@`` as ordinary text,
        but that is a dependency worth pinning: an argument-file convention
        here would silently swallow the ref.
        """
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Memo\n2025-01-01,-50.00,Coffee\n")

        result = runner.invoke(
            app,
            ["confirm", str(csv_file), "--accept", "--account-binding", "@0=new"],
        )

        assert result.exit_code == 0
        assert mock_import_file.call_args.kwargs["account_bindings"] == {"@0": "new"}

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

    def test_account_recovery_after_sign_preserves_confirmation_inputs(
        self,
        mock_db: MagicMock,
        mocker: Any,
        tmp_path: Path,
    ) -> None:
        """A later account gate retains the sign, mapping, and binding choices."""
        csv_file = tmp_path / "card.csv"
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
            account_proposals=[_account_proposal_dict("card-abc")],
        )
        mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            side_effect=ImportConfirmationRequiredError(outcome),
        )

        result = runner.invoke(
            app,
            [
                "confirm",
                str(csv_file),
                "--accept",
                "--confirm-sign",
                "--mapping",
                "description=Memo",
                "--account-binding",
                "settled=acct-123",
                "--account-binding",
                "minted=new",
                "--account-meta",
                "minted:display_name=Travel Card",
                "--account-meta",
                "minted:last_four=4267",
                "--account-id",
                "acct-explicit",
                "--account-name",
                "Card Account",
                "--no-save-format",
                "--output",
                "json",
            ],
        )

        assert result.exit_code == 0
        actions = json.loads(result.output)["actions"]
        recovery = next(action for action in actions if action.startswith("Re-run `"))
        command = recovery.split("`", 2)[1]
        tokens = shlex.split(command)
        assert "--accept" in tokens
        assert "--confirm-sign" in tokens
        assert "--no-save-format" in tokens
        assert tokens[tokens.index("--account-id") + 1] == "acct-explicit"
        assert tokens[tokens.index("--account-name") + 1] == "Card Account"
        assert tokens[tokens.index("--mapping") + 1] == "description=Memo"
        assert {
            tokens[i + 1] for i, arg in enumerate(tokens) if arg == "--account-binding"
        } == {
            "settled=acct-123",
            "minted=new",
            "card-abc=<account_id|new>",
        }
        assert {
            tokens[i + 1] for i, arg in enumerate(tokens) if arg == "--account-meta"
        } == {
            "minted:display_name=Travel Card",
            "minted:last_four=4267",
        }
        assert "human_sign_confirmation" not in command

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
        # The ref is what --account-binding's help tells the user to type, so
        # the gate that demands a binding has to be where they read it.
        assert "@0" in result.output


class TestSignRecoveryDirection:
    """A sign confirmation must name the two conventions actually on offer.

    The PDF guidance was written for the first-contact card inference, where the
    proposal is always `negative_is_income` and the alternative is always
    `negative_is_expense` — so "is this a credit card?" is both answerable and
    correct. Self-heal introduced the opposite direction: a re-derivation can
    propose `negative_is_expense` for a format the user previously ratified as a
    card. Rendered with the card framing, `--confirm` then does the *opposite* of
    what the text says, and no printed command lets the user keep the convention
    they had.
    """

    def _pdf_actions(self, **kwargs: object) -> list[str]:
        from moneybin.cli.commands.import_cmd import (
            _sign_recovery_commands,  # type: ignore[reportPrivateUsage]  # testing CLI recovery helper
        )

        return _sign_recovery_commands(  # type: ignore[reportPrivateUsage]  # testing CLI recovery helper
            "statement.pdf",
            channel="pdf",
            **kwargs,  # type: ignore[arg-type]
        )

    def test_first_contact_card_proposal_keeps_the_card_framing(self) -> None:
        """No prior convention → the card question is accurate; don't regress it."""
        actions = self._pdf_actions(proposed_sign="negative_is_income")

        assert any("IS a credit card" in a for a in actions)
        assert any("--confirm" in a for a in actions)
        assert any("--sign negative_is_expense" in a for a in actions)

    def test_an_un_inverting_repair_offers_a_way_back_to_the_old_convention(
        self,
    ) -> None:
        """The direction self-heal added: card → as-printed.

        `--confirm` here ratifies `negative_is_expense`, so card framing would be
        actively wrong, and `--sign negative_is_expense` (the old "not a card"
        escape hatch) is now identical to `--confirm` — leaving the user with two
        commands that do the same thing and none that keeps the card convention.
        """
        actions = self._pdf_actions(
            proposed_sign="negative_is_expense",
            prior_sign="negative_is_income",
        )

        joined = "\n".join(actions)
        # The escape hatch must exist and must name the OLD convention.
        assert "--sign negative_is_income" in joined
        # And the card framing must be gone — it describes the wrong direction.
        assert "IS a credit card" not in joined
        assert "records charges as expenses" not in joined.split("--confirm")[0]

    def test_a_re_inverting_repair_offers_a_way_back_too(self) -> None:
        """The mirror case: as-printed → card. Both directions or neither."""
        actions = self._pdf_actions(
            proposed_sign="negative_is_income",
            prior_sign="negative_is_expense",
        )

        assert "--sign negative_is_expense" in "\n".join(actions)
