"""Tests for import-tool helpers, including the file-path security boundary."""

from __future__ import annotations

import shlex
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest import MonkeyPatch

from moneybin.errors import UserError
from moneybin.extractors.confidence import Confidence
from moneybin.mcp.tools.import_tools import (
    _bridge_confirm_action,  # pyright: ignore[reportPrivateUsage]
    _validate_file_path,  # pyright: ignore[reportPrivateUsage]
    import_confirm,
    import_files,
    import_preview,
)
from moneybin.services.import_confirmation import (
    BridgePayload,
    Channel,
    ConfirmationRequired,
    ImportConfirmationRequiredError,
    ProposedMapping,
)
from tests.moneybin.pdf_statement_fixtures import write_card_statement_pdf


def test_valid_path_within_home_returns_resolved_path(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    """Paths inside the user's home directory resolve and are returned."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    target = tmp_path / "statements" / "bank.csv"
    target.parent.mkdir(parents=True)
    target.touch()

    assert _validate_file_path(str(target)) == target


def test_path_outside_home_raises_user_error(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    """Absolute paths outside the home directory are rejected."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "home")

    with pytest.raises(UserError) as excinfo:
        _validate_file_path("/etc/passwd")

    assert excinfo.value.code == "invalid_file_path"


def test_symlink_escaping_home_raises_user_error(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    """Symlinks inside home that resolve outside home are rejected."""
    home = tmp_path / "home"
    home.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    target = outside / "secret.csv"
    target.touch()
    link = home / "link.csv"
    link.symlink_to(target)

    monkeypatch.setattr(Path, "home", lambda: home)

    with pytest.raises(UserError) as excinfo:
        _validate_file_path(str(link))

    assert excinfo.value.code == "invalid_file_path"


def test_bridge_confirm_action_quotes_path_with_apostrophe() -> None:
    """Embed a single-quote path via ``repr``, not raw interpolation.

    Keeps the suggested ``import_confirm`` call in the action hint a
    syntactically valid string literal even when the path contains a quote.
    """
    path = "/home/alice/O'Brien/statement.pdf"

    hint = _bridge_confirm_action(path, payload_ref="bridge_payload")

    # repr-quoted form: file_path="/home/alice/O'Brien/statement.pdf" — valid.
    assert f"file_path={path!r}" in hint
    # The buggy form file_path='/home/alice/O'Brien/...' is an unterminated
    # literal and must not appear.
    assert f"file_path='{path}'" not in hint


# ---------------------------------------------------------------------------
# Helpers shared by the new test classes
# ---------------------------------------------------------------------------


def _make_confidence(
    score: float = 0.4,
    tier: str = "medium",
    flagged: tuple[str, ...] = (),
    missing_required: tuple[str, ...] = (),
) -> Confidence:
    return Confidence(
        score=score,
        tier=tier,  # type: ignore[arg-type]
        flagged=flagged,
        missing_required=missing_required,
    )


def _make_confirmation_error(
    *,
    tier: str = "medium",
    score: float = 0.4,
    field_mapping: dict[str, str] | None = None,
    flagged: tuple[str, ...] = (),
    missing_required: tuple[str, ...] = (),
    reason: str = "unknown_layout",
    account_proposals: list[dict[str, object]] | None = None,
) -> ImportConfirmationRequiredError:
    proposed = ProposedMapping(
        field_mapping=field_mapping or {"transaction_date": "Date", "amount": "Amount"},
        sample_values={"Date": ["2024-01-01"], "Amount": ["-50.00"]},
        unmapped_columns=("Notes",),
    )
    outcome = ConfirmationRequired(
        channel="tabular",
        confidence=_make_confidence(
            score=score, tier=tier, flagged=flagged, missing_required=missing_required
        ),
        proposed=proposed,
        reason=reason,  # type: ignore[arg-type]
        samples={"Date": ["2024-01-01"], "Amount": ["-50.00"]},
        account_proposals=account_proposals or [],  # type: ignore[arg-type]
    )
    return ImportConfirmationRequiredError(outcome)


@contextmanager
def _fake_database(**_kw: object):  # type: ignore[misc]
    yield MagicMock()


# ---------------------------------------------------------------------------
# TestImportFilesConfirmationRequired
# ---------------------------------------------------------------------------


class TestImportFilesConfirmationRequired:
    """import_files emits confirmation_required envelope on unknown layouts."""

    async def test_unknown_layout_returns_confirmation_required_envelope(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """import_files returns confirmation_required when service raises the error."""
        csv_file = tmp_path / "statements" / "unknown.csv"
        csv_file.parent.mkdir(parents=True)
        csv_file.touch()

        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database",
            _fake_database,
        )

        error = _make_confirmation_error(tier="medium", score=0.4)
        mock_service = MagicMock()
        mock_service.import_file.side_effect = error

        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            result = await import_files(paths=[str(csv_file)])

        # Uniform shape: single-file confirmation_required is one entry in
        # data.files[] (mirrors the multi-file path) — callers branch on
        # data.files[i].status, not on payload shape.
        data = result.data
        from moneybin.privacy.payloads.imports import ImportFilesPayload

        assert isinstance(data, ImportFilesPayload)
        assert len(data.files) == 1
        row = data.files[0]
        assert row.status == "confirmation_required"
        payload = row.confirmation_payload
        assert payload is not None
        assert payload["channel"] == "tabular"
        assert payload["tier"] == "medium"
        assert "score" in payload
        # Envelope summary.sensitivity must reflect that the response carries
        # sample rows + proposed mapping (per moneybin-mcp.md). Pure-success
        # batches stay at "low"; any pending file bumps the batch to "medium".
        assert result.summary.sensitivity == "medium"

    async def test_actions_list_includes_import_confirm_hint(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """actions[] includes a concrete import_confirm invocation hint."""
        csv_file = tmp_path / "statements" / "unknown.csv"
        csv_file.parent.mkdir(parents=True)
        csv_file.touch()

        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database",
            _fake_database,
        )

        error = _make_confirmation_error()
        mock_service = MagicMock()
        mock_service.import_file.side_effect = error

        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            result = await import_files(paths=[str(csv_file)])

        joined = " ".join(result.actions or [])
        assert "import_confirm" in joined
        assert "accept=True" in joined

    async def test_account_confirmation_action_binds_every_account(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """Bind every account in one import_confirm call.

        A bare account_confirmation file's action ratifies the mapping AND binds
        every account — not a looping accept-only hint (no binding) nor an
        irrelevant mapping-override hint.
        """
        csv_file = tmp_path / "statements" / "bare.csv"
        csv_file.parent.mkdir(parents=True)
        csv_file.touch()

        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database",
            _fake_database,
        )

        error = _make_confirmation_error(
            tier="high",
            score=1.0,
            reason="account_confirmation",
            account_proposals=[{"source_account_key": "bare-abc123", "candidates": []}],
        )
        mock_service = MagicMock()
        mock_service.import_file.side_effect = error

        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            result = await import_files(paths=[str(csv_file)])

        joined = " ".join(result.actions or [])
        assert "account_bindings={'bare-abc123': '<account_id|new>'}" in joined
        assert "accept=True" in joined
        # The account-confirmation reason must not emit the mapping-only paths
        # (accept-as-is with no binding loops; a mapping override is irrelevant).
        assert "to accept the proposed mapping as-is" not in joined
        assert "mapping={'<dest_field>'" not in joined

    async def test_low_tier_envelope_includes_missing_required(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """Envelope data includes missing_required when detector flags them."""
        csv_file = tmp_path / "statements" / "low.csv"
        csv_file.parent.mkdir(parents=True)
        csv_file.touch()

        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database",
            _fake_database,
        )

        error = _make_confirmation_error(
            tier="low",
            score=0.15,
            missing_required=("description",),
        )
        mock_service = MagicMock()
        mock_service.import_file.side_effect = error

        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            result = await import_files(paths=[str(csv_file)])

        from moneybin.privacy.payloads.imports import ImportFilesPayload

        data = result.data
        assert isinstance(data, ImportFilesPayload)
        payload = data.files[0].confirmation_payload
        assert payload is not None
        assert "description" in payload["missing_required"]  # type: ignore[operator]

    async def test_actor_kind_agent_passed_to_service(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """import_files passes actor_kind='agent' to ImportService.import_file."""
        csv_file = tmp_path / "statements" / "test.csv"
        csv_file.parent.mkdir(parents=True)
        csv_file.touch()

        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database",
            _fake_database,
        )

        mock_service = MagicMock()
        # Simulate a successful import (no confirmation required) to see the call kwargs
        from moneybin.services.import_service import ImportResult

        mock_service.import_file.return_value = ImportResult(
            file_path=str(csv_file),
            file_type="tabular",
            transactions=5,
            import_id="abc123",
        )

        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            await import_files(paths=[str(csv_file)])

        mock_service.import_file.assert_called_once()
        _args, kwargs = mock_service.import_file.call_args
        assert kwargs.get("actor_kind") == "agent"

    async def test_sign_override_replay_reaches_the_agent(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """A replayed `--sign` override must be visible on the MCP surface too.

        The agent is the user's only narrator here: the saved override disarms the
        credit-card detector for this format, so the row carries the fact and
        actions[] tells the agent to say so. MCP has no `sign` parameter — the hint
        must point at the CLI flag that can change it.
        """
        pdf = tmp_path / "statements" / "card.pdf"
        pdf.parent.mkdir(parents=True)
        pdf.touch()

        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database",
            _fake_database,
        )

        from moneybin.services.import_service import ImportResult

        mock_service = MagicMock()
        mock_service.import_file.return_value = ImportResult(
            file_path=str(pdf),
            file_type="pdf",
            transactions=2,
            import_id="abc123",
            sign_override_replayed=True,
        )

        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            result = await import_files(paths=[str(pdf)])

        from moneybin.privacy.payloads.imports import ImportFilesPayload

        data = result.data
        assert isinstance(data, ImportFilesPayload)
        assert data.files[0].sign_override_replayed is True
        joined = " ".join(result.actions or [])
        assert "saved" in joined and "--sign" in joined


# ---------------------------------------------------------------------------
# TestImportConfirmTool
# ---------------------------------------------------------------------------


class TestImportConfirmTool:
    """import_confirm tool: accept, override, validation, actor_kind."""

    def test_allows_human_confirmation_timeout(self) -> None:
        """Human sign elicitation gets the established 180-second decision window."""
        assert import_confirm._mcp_timeout_seconds == 180.0  # type: ignore[attr-defined]

    async def test_requires_accept_or_mapping(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """Calling with neither accept=True nor mapping returns an error envelope."""
        csv_file = tmp_path / "statements" / "test.csv"
        csv_file.parent.mkdir(parents=True)
        csv_file.touch()

        monkeypatch.setattr(Path, "home", lambda: tmp_path)

        # The @mcp_tool decorator converts UserError to an error envelope.
        result = await import_confirm(file_path=str(csv_file))
        assert result.error is not None
        assert result.error.code == "confirm_requires_signal"

    async def test_accept_loads_data(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """accept=True calls import_file with confirm=True and returns imported status."""
        csv_file = tmp_path / "statements" / "test.csv"
        csv_file.parent.mkdir(parents=True)
        csv_file.touch()

        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database",
            _fake_database,
        )

        from moneybin.services.import_service import ImportResult

        mock_service = MagicMock()
        mock_service.import_file.return_value = ImportResult(
            file_path=str(csv_file),
            file_type="tabular",
            transactions=10,
            import_id="abc-123",
        )

        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            with patch(
                "moneybin.extractors.tabular.format_detector.detect_format",
                side_effect=ValueError("preview unavailable"),
            ):
                result = await import_confirm(file_path=str(csv_file), accept=True)

        assert result.data.rows_loaded == 10
        assert result.data.import_id == "abc-123"

    async def test_tabular_sign_confirmation_elicitation_is_human_gated(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """An agent's mapping accept pauses for a separate human sign decision."""
        from moneybin.services.import_confirmation import SignConventionProposal
        from moneybin.services.import_service import ImportResult

        csv_file = tmp_path / "statements" / "card.csv"
        csv_file.parent.mkdir(parents=True)
        csv_file.touch()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database", _fake_database
        )
        sign_error = ImportConfirmationRequiredError(
            ConfirmationRequired(
                channel="tabular",
                confidence=_make_confidence(score=1.0, tier="high"),
                proposed=SignConventionProposal(
                    sign_convention="negative_is_income",
                    evidence=("a column header contains the word 'credit'",),
                    sample_rows=[],
                ),
                reason="sign_convention",
            )
        )
        mock_service = MagicMock()
        mock_service.import_file.side_effect = [
            sign_error,
            ImportResult(
                file_path=str(csv_file),
                file_type="tabular",
                transactions=2,
                import_id="sign-123",
            ),
        ]
        confirm = AsyncMock()
        with (
            patch(
                "moneybin.services.import_service.ImportService",
                return_value=mock_service,
            ),
            patch("moneybin.mcp.elicitation.confirm_or_raise", confirm),
            patch("moneybin.services.inbox_service.InboxService"),
            patch(
                "moneybin.extractors.tabular.format_detector.detect_format",
                side_effect=ValueError("preview unavailable"),
            ),
        ):
            result = await import_confirm(file_path=str(csv_file), accept=True)

        assert result.data.import_id == "sign-123"
        confirm.assert_awaited_once()
        assert confirm.await_args is not None
        assert "Sample rows" not in confirm.await_args.args[0]
        assert (
            mock_service.import_file.call_args_list[1].kwargs["human_sign_confirmation"]
            is True
        )

    async def test_tabular_sign_no_elicitation_cli_fallback_is_lossless(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """The terminal fallback reproduces every public confirmation input."""
        from moneybin.services.import_confirmation import SignConventionProposal

        csv_file = tmp_path / "statements" / "Owner's card.csv"
        csv_file.parent.mkdir(parents=True)
        csv_file.touch()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database", _fake_database
        )
        sign_error = ImportConfirmationRequiredError(
            ConfirmationRequired(
                channel="tabular",
                confidence=_make_confidence(score=1.0, tier="high"),
                proposed=SignConventionProposal(
                    sign_convention="negative_is_income",
                    evidence=("Debit",),
                    sample_rows=[],
                ),
                reason="sign_convention",
            )
        )
        mock_service = MagicMock()
        mock_service.import_file.side_effect = sign_error
        mapping = {"description": "Merchant Name"}
        bindings = {"minted card": "new", "settled": "acct existing"}
        metadata = {
            "minted card": {
                "display_name": "Owner's Card",
                "last_four": "4267",
            }
        }

        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            result = await import_confirm(
                file_path=str(csv_file),
                mapping=mapping,
                save_format=False,
                account_id="acct explicit",
                account_name="Owner's Card",
                account_bindings=bindings,
                account_metadata=metadata,
            )

        assert result.error is not None
        assert result.error.code == "mutation_confirmation_required"
        assert result.error.hint is not None
        command = result.error.hint.split("`", 2)[1]
        tokens = shlex.split(command)
        assert tokens[:4] == ["moneybin", "import", "confirm", str(csv_file)]
        assert "--accept" not in tokens
        assert "--confirm-sign" in tokens
        assert "--no-save-format" in tokens
        assert tokens[tokens.index("--account-id") + 1] == "acct explicit"
        assert tokens[tokens.index("--account-name") + 1] == "Owner's Card"
        assert tokens[tokens.index("--mapping") + 1] == "description=Merchant Name"
        binding_values = {
            tokens[i + 1] for i, arg in enumerate(tokens) if arg == "--account-binding"
        }
        assert binding_values == {
            "minted card=new",
            "settled=acct existing",
        }
        metadata_values = {
            tokens[i + 1] for i, arg in enumerate(tokens) if arg == "--account-meta"
        }
        assert metadata_values == {
            "minted card:display_name=Owner's Card",
            "minted card:last_four=4267",
        }
        assert "human_sign_confirmation" not in result.error.hint
        assert mock_service.import_file.call_count == 1

    async def test_tabular_sign_retry_returns_account_confirmation_envelope(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """Account recovery preserves inputs and discloses repeated sign elicitation."""
        from moneybin.services.import_confirmation import SignConventionProposal

        csv_file = tmp_path / "statements" / "card.csv"
        csv_file.parent.mkdir(parents=True)
        csv_file.touch()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database", _fake_database
        )
        sign_error = ImportConfirmationRequiredError(
            ConfirmationRequired(
                channel="tabular",
                confidence=_make_confidence(score=1.0, tier="high"),
                proposed=SignConventionProposal(
                    sign_convention="negative_is_income",
                    evidence=("a column header contains the word 'credit'",),
                    sample_rows=[],
                ),
                reason="sign_convention",
            )
        )
        account_error = _make_confirmation_error(
            tier="high",
            score=1.0,
            reason="account_confirmation",
            account_proposals=[{"source_account_key": "card-abc", "candidates": []}],
        )
        mock_service = MagicMock()
        mock_service.import_file.side_effect = [sign_error, account_error]
        with (
            patch(
                "moneybin.services.import_service.ImportService",
                return_value=mock_service,
            ),
            patch("moneybin.mcp.elicitation.confirm_or_raise", AsyncMock()),
            patch("moneybin.services.inbox_service.InboxService"),
            patch(
                "moneybin.extractors.tabular.format_detector.detect_format",
                side_effect=ValueError("preview unavailable"),
            ),
        ):
            result = await import_confirm(
                file_path=str(csv_file),
                mapping={"description": "Memo"},
                save_format=False,
                account_id="acct-explicit",
                account_name="Card Account",
                account_bindings={"settled": "acct-123", "minted": "new"},
                account_metadata={
                    "minted": {
                        "display_name": "Travel Card",
                        "last_four": "4267",
                    }
                },
            )

        data = result.data
        assert isinstance(data, dict)
        assert data["status"] == "confirmation_required"
        assert data["reason"] == "account_confirmation"
        actions = " ".join(result.actions or [])
        assert "accept=True" not in actions
        assert "mapping={'description': 'Memo'}" in actions
        assert "save_format=False" in actions
        assert "account_id='acct-explicit'" in actions
        assert "account_name='Card Account'" in actions
        assert "'settled': 'acct-123'" in actions
        assert "'minted': 'new'" in actions
        assert "'card-abc': '<account_id|new>'" in actions
        assert "'display_name': 'Travel Card'" in actions
        assert "'last_four': '4267'" in actions
        assert "not persisted across MCP calls" in actions
        assert "ask the human to confirm the sign inversion again" in actions
        assert "human_sign_confirmation" not in actions

    async def test_mapping_override_passes_overrides_to_service(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """mapping= is forwarded as overrides= to import_file."""
        csv_file = tmp_path / "statements" / "test.csv"
        csv_file.parent.mkdir(parents=True)
        csv_file.touch()

        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database",
            _fake_database,
        )

        from moneybin.services.import_service import ImportResult

        mock_service = MagicMock()
        mock_service.import_file.return_value = ImportResult(
            file_path=str(csv_file),
            file_type="tabular",
            transactions=5,
            import_id="xyz-456",
        )
        override = {"description": "Memo"}

        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            with patch(
                "moneybin.extractors.tabular.format_detector.detect_format",
                side_effect=ValueError("preview unavailable"),
            ):
                await import_confirm(file_path=str(csv_file), mapping=override)

        _args, kwargs = mock_service.import_file.call_args
        assert kwargs.get("overrides") == override

    async def test_account_confirmation_reprompt_carries_proposals_and_binding(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """Account_confirmation re-prompt from import_confirm carries proposals.

        Bare-file path: import_files returns unknown_layout, the agent calls
        import_confirm(accept=True), and the bare-account gate fires. The
        re-prompt envelope must carry account_proposals in data AND an
        account-binding action — not a looping accept/mapping-only hint.
        """
        csv_file = tmp_path / "statements" / "bare.csv"
        csv_file.parent.mkdir(parents=True)
        csv_file.touch()

        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database",
            _fake_database,
        )

        error = _make_confirmation_error(
            tier="high",
            score=1.0,
            reason="account_confirmation",
            account_proposals=[{"source_account_key": "bare-abc123", "candidates": []}],
        )
        mock_service = MagicMock()
        mock_service.import_file.side_effect = error

        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            with patch(
                "moneybin.extractors.tabular.format_detector.detect_format",
                side_effect=ValueError("preview unavailable"),
            ):
                result = await import_confirm(file_path=str(csv_file), accept=True)

        data = result.data
        assert isinstance(data, dict)
        assert data["status"] == "confirmation_required"
        assert data["reason"] == "account_confirmation"
        proposals = data["account_proposals"]
        assert isinstance(proposals, list) and proposals
        assert proposals[0]["source_account_key"] == "bare-abc123"
        joined = " ".join(result.actions or [])
        assert "account_bindings={'bare-abc123': '<account_id|new>'}" in joined
        assert "accept=True" in joined
        # No standalone accept-as-is / mapping-override hint for this reason.
        assert "to accept the proposed mapping as-is" not in joined
        assert "mapping={'<dest_field>'" not in joined

    async def test_passes_actor_kind_agent_to_service(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """import_confirm always passes actor_kind='agent' to ImportService."""
        csv_file = tmp_path / "statements" / "test.csv"
        csv_file.parent.mkdir(parents=True)
        csv_file.touch()

        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database",
            _fake_database,
        )

        from moneybin.services.import_service import ImportResult

        mock_service = MagicMock()
        mock_service.import_file.return_value = ImportResult(
            file_path=str(csv_file),
            file_type="tabular",
            transactions=3,
            import_id="qrs-789",
        )

        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            with patch(
                "moneybin.extractors.tabular.format_detector.detect_format",
                side_effect=ValueError("preview unavailable"),
            ):
                await import_confirm(file_path=str(csv_file), accept=True)

        _args, kwargs = mock_service.import_file.call_args
        assert kwargs.get("actor_kind") == "agent"

    async def test_account_bindings_forwarded_to_service(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """import_confirm threads account_bindings to ImportService.import_file."""
        csv_file = tmp_path / "statements" / "test.csv"
        csv_file.parent.mkdir(parents=True)
        csv_file.touch()

        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database",
            _fake_database,
        )

        from moneybin.services.import_service import ImportResult

        mock_service = MagicMock()
        mock_service.import_file.return_value = ImportResult(
            file_path=str(csv_file),
            file_type="tabular",
            transactions=2,
            import_id="bnd-001",
        )
        bindings = {"wf-checking": "acct123456", "wf-savings": "new"}

        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            with patch(
                "moneybin.extractors.tabular.format_detector.detect_format",
                side_effect=ValueError("preview unavailable"),
            ):
                await import_confirm(
                    file_path=str(csv_file),
                    accept=True,
                    account_bindings=bindings,
                )

        _args, kwargs = mock_service.import_file.call_args
        assert kwargs.get("account_bindings") == bindings

    async def test_account_metadata_forwarded_to_service(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """import_confirm threads account_metadata to ImportService.import_file."""
        csv_file = tmp_path / "statements" / "test.csv"
        csv_file.parent.mkdir(parents=True)
        csv_file.touch()

        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database",
            _fake_database,
        )

        from moneybin.services.import_service import ImportResult

        mock_service = MagicMock()
        mock_service.import_file.return_value = ImportResult(
            file_path=str(csv_file),
            file_type="tabular",
            transactions=1,
            import_id="mta-001",
        )
        metadata = {"wf-checking": {"display_name": "WF Checking", "last_four": "4267"}}

        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            with patch(
                "moneybin.extractors.tabular.format_detector.detect_format",
                side_effect=ValueError("preview unavailable"),
            ):
                await import_confirm(
                    file_path=str(csv_file),
                    accept=True,
                    account_bindings={"wf-checking": "new"},
                    account_metadata=metadata,
                )

        _args, kwargs = mock_service.import_file.call_args
        assert kwargs.get("account_metadata") == metadata

    async def test_save_format_default_true(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """save_format defaults to True and is forwarded to import_file."""
        csv_file = tmp_path / "statements" / "test.csv"
        csv_file.parent.mkdir(parents=True)
        csv_file.touch()

        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database",
            _fake_database,
        )

        from moneybin.services.import_service import ImportResult

        mock_service = MagicMock()
        mock_service.import_file.return_value = ImportResult(
            file_path=str(csv_file),
            file_type="tabular",
            transactions=2,
            import_id="save-001",
        )

        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            with patch(
                "moneybin.extractors.tabular.format_detector.detect_format",
                side_effect=ValueError("preview unavailable"),
            ):
                await import_confirm(file_path=str(csv_file), accept=True)

        _args, kwargs = mock_service.import_file.call_args
        assert kwargs.get("save_format") is True

    async def test_import_revert_hint_in_actions(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """actions[] includes an import_revert hint referencing the import_id."""
        csv_file = tmp_path / "statements" / "test.csv"
        csv_file.parent.mkdir(parents=True)
        csv_file.touch()

        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database",
            _fake_database,
        )

        from moneybin.services.import_service import ImportResult

        mock_service = MagicMock()
        mock_service.import_file.return_value = ImportResult(
            file_path=str(csv_file),
            file_type="tabular",
            transactions=1,
            import_id="revert-001",
        )

        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            with patch(
                "moneybin.extractors.tabular.format_detector.detect_format",
                side_effect=ValueError("preview unavailable"),
            ):
                result = await import_confirm(file_path=str(csv_file), accept=True)

        joined = " ".join(result.actions)
        assert "import_revert" in joined
        assert "revert-001" in joined


# ---------------------------------------------------------------------------
# PDF bridge wire-in: import_files escalation, import_preview, import_confirm
# ---------------------------------------------------------------------------


def _bridge_error(reason: str = "unknown_layout") -> ImportConfirmationRequiredError:
    """A confirmation error whose proposal is a PDF BridgePayload."""
    payload = BridgePayload(
        payload={
            "transparency_notice": "Proceeding surfaces this PDF to the agent.",
            "source_file": "chase.pdf",
            "document_text": "Chase Bank\nDate Description Amount\n...",
            "tables_preview": [{"page": 1, "header": ["Date"], "rows": [["05/01"]]}],
            "fingerprint": {"issuer": "chase", "headers": ["Date"], "page_bucket": "1"},
            "request_kind": "propose_recipe",
            "saved_recipe_for_re_derive": None,
        }
    )
    outcome = ConfirmationRequired(
        channel="pdf",
        confidence=_make_confidence(score=0.4, tier="low"),
        proposed=payload,
        reason=reason,  # type: ignore[arg-type]
    )
    return ImportConfirmationRequiredError(outcome)


def _sign_error() -> ImportConfirmationRequiredError:
    """A confirmation error whose proposal is a PDF SignConventionProposal."""
    from moneybin.services.import_confirmation import SignConventionProposal

    outcome = ConfirmationRequired(
        channel="pdf",
        confidence=_make_confidence(score=0.75, tier="medium"),
        proposed=SignConventionProposal(
            sign_convention="negative_is_income",
            evidence=("minimum payment", "credit limit"),
            sample_rows=[
                {
                    "description": "COFFEE SHOP",
                    "as_printed": "150.00",
                    "as_recorded": "-150.00",
                }
            ],
        ),
        reason="sign_convention",
        error_message="This looks like a credit-card statement.",
    )
    return ImportConfirmationRequiredError(outcome)


class TestImportFilesPdfBridge:
    """import_files surfaces the bridge payload when a PDF escalates."""

    async def test_pdf_escalation_returns_bridge_payload(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        pdf = tmp_path / "statements" / "chase.pdf"
        pdf.parent.mkdir(parents=True)
        pdf.touch()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database", _fake_database
        )

        mock_service = MagicMock()
        mock_service.import_file.side_effect = _bridge_error()
        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            result = await import_files(paths=[str(pdf)])

        from moneybin.privacy.payloads.imports import ImportFilesPayload

        data = result.data
        assert isinstance(data, ImportFilesPayload)
        row = data.files[0]
        assert row.status == "confirmation_required"
        payload = row.confirmation_payload
        assert payload is not None
        assert payload["channel"] == "pdf"
        bridge = payload["bridge_payload"]
        assert isinstance(bridge, dict)
        assert bridge["request_kind"] == "propose_recipe"
        assert "transparency_notice" in bridge

    async def test_pdf_escalation_action_points_at_bridge_response(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        pdf = tmp_path / "statements" / "chase.pdf"
        pdf.parent.mkdir(parents=True)
        pdf.touch()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database", _fake_database
        )

        mock_service = MagicMock()
        mock_service.import_file.side_effect = _bridge_error()
        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            result = await import_files(paths=[str(pdf)])

        assert any("bridge_response" in a for a in result.actions)
        # The tabular accept/mapping hint must NOT appear for a PDF bridge.
        assert not any("mapping={" in a for a in result.actions)


class TestImportFilesPdfSign:
    """import_files surfaces the credit-card sign confirmation with CLI recovery."""

    async def test_card_statement_action_directs_to_cli_not_import_confirm(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """A card through the import path gets the honest terminal recovery.

        The service raises a sign_convention confirmation (exactly what a real
        card statement produces — see test_import_pdf_transactions). MCP cannot
        ratify a sign inversion in place yet, so the actions[] must point at the
        CLI, not at the broken Task 6 hints (`import_confirm(sign=...)` /
        `import_confirm(accept=True)`) that cannot ratify a sign flip.
        """
        pdf = tmp_path / "statements" / "chase_card.pdf"
        pdf.parent.mkdir(parents=True)
        pdf.touch()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database", _fake_database
        )

        mock_service = MagicMock()
        mock_service.import_file.side_effect = _sign_error()
        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            result = await import_files(paths=[str(pdf)])

        from moneybin.privacy.payloads.imports import ImportFilesPayload

        data = result.data
        assert isinstance(data, ImportFilesPayload)
        row = data.files[0]
        assert row.status == "confirmation_required"
        payload = row.confirmation_payload
        assert payload is not None
        assert payload["reason"] == "sign_convention"
        assert payload["sign_convention"] == "negative_is_income"

        actions = " ".join(result.actions)
        # The in-MCP human gate leads; the terminal CLI stays as the fallback,
        # both branches named.
        assert "confirm_pdf_sign=True" in actions
        assert "moneybin import files" in actions
        assert "--confirm" in actions
        assert "--sign negative_is_expense" in actions
        # Dead ends must stay gone: import_confirm has no sign= parameter, and a
        # sign confirmation is NOT a bridge, so neither hint may leak in.
        assert "sign='negative_is_expense'" not in actions
        assert "bridge_response" not in actions
        # A sign proposal is not a validation failure — no misleading prefix.
        assert "Validation failed" not in actions


class TestImportPreviewTabular:
    """import_preview surfaces header/reconciliation transparency for tabular files.

    Regression coverage for the silent-header-eating AX gap: the preview
    envelope gave no signal that a row had been consumed as a header, or that
    row counts didn't reconcile.
    """

    async def test_preview_surfaces_header_and_reconciliation_fields(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        csv_file = tmp_path / "statements" / "basic.csv"
        csv_file.parent.mkdir(parents=True)
        csv_file.write_text("Date,Amount,Description\n2026-01-01,42.50,Coffee\n")
        monkeypatch.setattr(Path, "home", lambda: tmp_path)

        result = await import_preview(file_path=str(csv_file))

        from moneybin.privacy.payloads.imports import ImportPreviewPayload

        data = result.data
        assert isinstance(data, ImportPreviewPayload)
        assert data.has_header is True
        assert data.skip_rows == 0
        assert data.rows_in_file == 2  # 1 header + 1 data row
        assert data.header_row_looks_like_data is False

    async def test_preview_flags_headerless_csv(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        csv_file = tmp_path / "statements" / "wf.csv"
        csv_file.parent.mkdir(parents=True)
        csv_file.write_text(
            '"04/16/2026","150.00","*","","RECURRING TRANSFER FROM ACME"\n'
            '"04/15/2026","-150.00","*","","RECURRING TRANSFER TO ACME"\n'
        )
        monkeypatch.setattr(Path, "home", lambda: tmp_path)

        result = await import_preview(file_path=str(csv_file))

        from moneybin.privacy.payloads.imports import ImportPreviewPayload

        data = result.data
        assert isinstance(data, ImportPreviewPayload)
        assert data.has_header is False
        assert data.skip_rows == 0
        assert data.rows_in_file == 2


class TestImportPreviewPdf:
    """import_preview dispatches .pdf to the deterministic rung / bridge."""

    async def test_pdf_deterministic_preview(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        from moneybin.services.import_service import PdfPreviewResult

        pdf = tmp_path / "statements" / "chase.pdf"
        pdf.parent.mkdir(parents=True)
        pdf.touch()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database", _fake_database
        )

        mock_service = MagicMock()
        mock_service.pdf_preview.return_value = PdfPreviewResult(
            file_path=str(pdf),
            deterministic=True,
            decision_reason="passed",
            confidence=0.95,
            row_count=12,
            fingerprint={"issuer": "chase"},
        )
        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            result = await import_preview(file_path=str(pdf))

        data = result.data
        assert isinstance(data, dict)
        assert data["status"] == "preview"
        assert data["deterministic"] is True
        assert data["row_count"] == 12
        assert result.summary.sensitivity == "medium"

    async def test_pdf_bridge_escalation_returns_payload(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        pdf = tmp_path / "statements" / "chase.pdf"
        pdf.parent.mkdir(parents=True)
        pdf.touch()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database", _fake_database
        )

        mock_service = MagicMock()
        mock_service.pdf_preview.side_effect = _bridge_error()
        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            result = await import_preview(file_path=str(pdf))

        data = result.data
        assert isinstance(data, dict)
        assert data["status"] == "confirmation_required"
        assert data["channel"] == "pdf"
        assert data["bridge_payload"]["request_kind"] == "propose_recipe"

    async def test_pdf_sign_confirmation_returns_proposal(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """A card statement surfaces the inversion, not a RuntimeError.

        The handler used to reject any non-BridgePayload proposal outright, so the
        moment pdf_preview could raise a sign confirmation, every credit-card
        statement previewed as a server error instead of the confirm.
        """
        pdf = tmp_path / "statements" / "chase_card.pdf"
        pdf.parent.mkdir(parents=True)
        pdf.touch()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database", _fake_database
        )

        mock_service = MagicMock()
        mock_service.pdf_preview.side_effect = _sign_error()
        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            result = await import_preview(file_path=str(pdf))

        data = result.data
        assert isinstance(data, dict)
        assert data["status"] == "confirmation_required"
        assert data["channel"] == "pdf"
        assert data["reason"] == "sign_convention"
        assert data["sign_convention"] == "negative_is_income"
        assert data["sign_evidence"] == ["minimum payment", "credit limit"]
        assert data["sign_sample_rows"][0]["as_printed"] == "150.00"
        assert data["sign_sample_rows"][0]["as_recorded"] == "-150.00"

        # The agent is told both branches, and pointed at the in-MCP human gate
        # first — with the terminal as the fallback.
        actions = " ".join(result.actions)
        assert "confirm_pdf_sign=True" in actions
        assert "moneybin import files" in actions
        assert "--confirm" in actions
        assert "--sign negative_is_expense" in actions
        # Still a dead end: import_confirm has no sign= parameter.
        assert "sign='negative_is_expense'" not in actions


class TestImportConfirmBridge:
    """import_confirm(bridge_response=...) applies a PDF bridge response."""

    def _patch(self, monkeypatch: MonkeyPatch, tmp_path: Path) -> Path:
        pdf = tmp_path / "statements" / "chase.pdf"
        pdf.parent.mkdir(parents=True)
        pdf.touch()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database", _fake_database
        )
        return pdf

    async def test_applied(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        from moneybin.services.import_service import BridgeApplyResult

        pdf = self._patch(monkeypatch, tmp_path)
        mock_service = MagicMock()
        mock_service.apply_pdf_bridge_response.return_value = BridgeApplyResult(
            outcome="applied",
            import_id="imp123",
            rows_loaded=12,
            format_name="chase_abc123",
            expected_row_count=12,
            actual_row_count=12,
            rows_diverged=False,
        )
        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            result = await import_confirm(
                file_path=str(pdf),
                bridge_response={"recipe": {}, "rows": []},
            )

        data = result.data
        assert isinstance(data, dict)
        assert data["status"] == "applied"
        assert data["import_id"] == "imp123"
        assert data["rows_loaded"] == 12
        assert any("import_revert" in a for a in result.actions)

    async def test_applied_archives_pending_file(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """A successful bridge confirm archives the PDF out of pending/.

        Mirrors the tabular confirm path: a bridge-confirmed inbox PDF must
        complete the inbox lifecycle rather than lingering in pending/ after a
        successful load.
        """
        from moneybin.services.import_service import BridgeApplyResult

        pdf = self._patch(monkeypatch, tmp_path)
        mock_service = MagicMock()
        mock_service.apply_pdf_bridge_response.return_value = BridgeApplyResult(
            outcome="applied",
            import_id="imp123",
            rows_loaded=12,
            format_name="chase_abc123",
            expected_row_count=12,
            actual_row_count=12,
            rows_diverged=False,
        )
        mock_inbox_cls = MagicMock()
        with (
            patch(
                "moneybin.services.import_service.ImportService",
                return_value=mock_service,
            ),
            patch(
                "moneybin.services.inbox_service.InboxService",
                mock_inbox_cls,
            ),
        ):
            await import_confirm(
                file_path=str(pdf),
                bridge_response={"recipe": {}, "rows": []},
            )

        archive = mock_inbox_cls.for_active_profile_no_db.return_value
        archive.archive_confirmed_file.assert_called_once()

    async def test_invalid_does_not_archive(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """An invalid reconciliation loaded nothing, so nothing is archived."""
        from moneybin.services.import_service import BridgeApplyResult

        pdf = self._patch(monkeypatch, tmp_path)
        mock_service = MagicMock()
        mock_service.apply_pdf_bridge_response.return_value = BridgeApplyResult(
            outcome="invalid",
            import_id=None,
            rows_loaded=0,
            format_name=None,
            expected_row_count=10,
            actual_row_count=8,
            rows_diverged=True,
            reject_reason="reconciliation_failed",
        )
        mock_inbox_cls = MagicMock()
        with (
            patch(
                "moneybin.services.import_service.ImportService",
                return_value=mock_service,
            ),
            patch(
                "moneybin.services.inbox_service.InboxService",
                mock_inbox_cls,
            ),
        ):
            await import_confirm(
                file_path=str(pdf),
                bridge_response={"recipe": {}, "rows": []},
            )

        archive = mock_inbox_cls.for_active_profile_no_db.return_value
        archive.archive_confirmed_file.assert_not_called()

    async def test_invalid_reconciliation(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        from moneybin.services.import_service import BridgeApplyResult

        pdf = self._patch(monkeypatch, tmp_path)
        mock_service = MagicMock()
        mock_service.apply_pdf_bridge_response.return_value = BridgeApplyResult(
            outcome="invalid",
            import_id=None,
            rows_loaded=0,
            format_name=None,
            expected_row_count=10,
            actual_row_count=8,
            rows_diverged=True,
            reject_reason="reconciliation_failed",
        )
        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            result = await import_confirm(
                file_path=str(pdf),
                bridge_response={"recipe": {}, "rows": []},
            )

        data = result.data
        assert isinstance(data, dict)
        assert data["status"] == "invalid"
        assert data["reject_reason"] == "reconciliation_failed"
        assert "import_id" not in data  # nothing loaded

    async def test_divergence_surfaced_in_actions(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        from moneybin.services.import_service import BridgeApplyResult

        pdf = self._patch(monkeypatch, tmp_path)
        mock_service = MagicMock()
        mock_service.apply_pdf_bridge_response.return_value = BridgeApplyResult(
            outcome="applied",
            import_id="imp123",
            rows_loaded=11,
            format_name="chase_abc123",
            expected_row_count=12,
            actual_row_count=11,
            rows_diverged=True,
        )
        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            result = await import_confirm(
                file_path=str(pdf),
                bridge_response={"recipe": {}, "rows": []},
            )

        assert any("12" in a and "11" in a for a in result.actions)

    async def test_conflict_with_accept_returns_error_envelope(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        # UserError raised inside the tool is caught by @mcp_tool and surfaced
        # as result.error (the decorator never lets it propagate).
        pdf = self._patch(monkeypatch, tmp_path)
        result = await import_confirm(
            file_path=str(pdf),
            accept=True,
            bridge_response={"recipe": {}, "rows": []},
        )
        assert result.error is not None
        assert result.error.code == "confirm_channel_conflict"

    async def test_malformed_response_maps_to_user_error(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        from moneybin.extractors.pdf.bridge import BridgeResponseError

        pdf = self._patch(monkeypatch, tmp_path)
        mock_service = MagicMock()
        # parse_bridge_response raises BridgeResponseError on a bad shape.
        mock_service.apply_pdf_bridge_response.side_effect = BridgeResponseError(
            "bridge response missing 'recipe' key"
        )
        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            result = await import_confirm(
                file_path=str(pdf),
                bridge_response={"rows": []},
            )
        assert result.error is not None
        assert result.error.code == "bridge_response_invalid"

    async def test_inverted_bridge_recipe_requires_human_elicitation(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """A human approval, not the agent, is required before the bridge loads."""
        from moneybin.services.import_service import BridgeApplyResult

        pdf = self._patch(monkeypatch, tmp_path)
        mock_service = MagicMock()
        mock_service.apply_pdf_bridge_response.side_effect = [
            _sign_error(),
            BridgeApplyResult(
                outcome="applied",
                import_id="imp123",
                rows_loaded=2,
                format_name="chase_abc123",
                expected_row_count=2,
                actual_row_count=2,
                rows_diverged=False,
            ),
        ]
        confirm = AsyncMock()
        mock_inbox_cls = MagicMock()
        with (
            patch(
                "moneybin.services.import_service.ImportService",
                return_value=mock_service,
            ),
            patch("moneybin.mcp.elicitation.confirm_or_raise", confirm),
            patch("moneybin.services.inbox_service.InboxService", mock_inbox_cls),
        ):
            result = await import_confirm(
                file_path=str(pdf),
                bridge_response={"recipe": {}, "rows": []},
            )

        data = result.data
        assert isinstance(data, dict)
        assert data["status"] == "applied"
        confirm.assert_awaited_once()
        assert confirm.await_args is not None
        prompt = confirm.await_args.args[0]
        assert "minimum payment" in prompt
        assert "COFFEE SHOP" in prompt
        assert (
            mock_service.apply_pdf_bridge_response.call_args_list[1].kwargs["confirm"]
            is True
        )

    async def test_inverted_bridge_recipe_cannot_load_without_elicitation(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """An unsupported MCP client leaves the PDF unchanged."""
        pdf = self._patch(monkeypatch, tmp_path)
        mock_service = MagicMock()
        mock_service.apply_pdf_bridge_response.side_effect = _sign_error()
        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            result = await import_confirm(
                file_path=str(pdf),
                bridge_response={"recipe": {}, "rows": []},
            )

        assert result.error is not None
        assert result.error.code == "mutation_confirmation_required"
        assert mock_service.apply_pdf_bridge_response.call_count == 1

    async def test_non_parse_value_error_not_labeled_bridge_invalid(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        # A plain ValueError raised after parsing (e.g. malformed PDF in
        # extract, or the load) must NOT be mislabeled bridge_response_invalid —
        # the narrowed catch lets it propagate to the generic error boundary.
        pdf = self._patch(monkeypatch, tmp_path)
        mock_service = MagicMock()
        mock_service.apply_pdf_bridge_response.side_effect = ValueError(
            "could not extract text from PDF"
        )
        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            result = await import_confirm(
                file_path=str(pdf),
                bridge_response={"recipe": {}, "rows": []},
            )
        # classify_user_error maps every ValueError to a non-None error
        # envelope, so an error IS surfaced — assert that (no unreachable
        # is-None arm that would hide a classification regression). The one
        # outcome we forbid is the misleading bridge_response_invalid.
        assert result.error is not None
        assert result.error.code != "bridge_response_invalid"

    async def test_account_name_with_bridge_raises(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        # PDF rows resolve the account from the statement; account_name is a
        # tabular-only signal. Passing it with bridge_response must error loudly
        # rather than being silently dropped (the bridge path takes account_id).
        pdf = self._patch(monkeypatch, tmp_path)
        result = await import_confirm(
            file_path=str(pdf),
            bridge_response={"recipe": {}, "rows": []},
            account_name="Chase Checking",
        )
        assert result.error is not None
        assert result.error.code == "pdf_account_signal_unsupported"

    async def test_invalid_path_precedes_account_name_guard(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        # A bad path must surface as invalid_file_path even when account_name is
        # also (mis)used with bridge_response — path validation runs first, so
        # the path error isn't masked by the account_name guard.
        monkeypatch.setattr(Path, "home", lambda: tmp_path / "home")
        result = await import_confirm(
            file_path="/etc/passwd",
            bridge_response={"recipe": {}, "rows": []},
            account_name="Chase Checking",
        )
        assert result.error is not None
        assert result.error.code == "invalid_file_path"

    async def test_pdf_with_accept_rejected_not_looped(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        # A PDF confirmed via the tabular channel (accept=True, no
        # bridge_response) must be rejected with channel guidance — NOT run
        # through the tabular import path, which would re-raise the bridge
        # escalation that this tool's catch can't serialize, looping the agent.
        pdf = self._patch(monkeypatch, tmp_path)
        result = await import_confirm(file_path=str(pdf), accept=True)
        assert result.error is not None
        assert result.error.code == "confirm_channel_conflict"

    async def test_card_sign_confirm_directs_to_the_sign_channel_and_loads_nothing(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """accept=True on a real card statement refuses and names the right channel.

        accept= is the tabular column-mapping signal and never ratifies a PDF. The
        refusal must route the caller to confirm_pdf_sign=True (which elicits the
        human), never crash with a TypeError, and never run the import path (no
        inverted rows land). Uses the committed card fixture so the file on disk
        is genuinely a credit-card statement.
        """
        pdf = write_card_statement_pdf(tmp_path)
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database", _fake_database
        )

        mock_service = MagicMock()
        with patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ):
            result = await import_confirm(file_path=str(pdf), accept=True)

        # A clean UserError envelope — not a crash / TypeError.
        assert result.error is not None
        assert result.error.code == "confirm_channel_conflict"
        message = result.error.message
        # The live in-MCP channel, plus the terminal fallback, both branches named.
        assert "confirm_pdf_sign=True" in message
        assert "moneybin import files" in message
        assert "--confirm" in message
        assert "--sign negative_is_expense" in message
        # Refused before any import ran — nothing loaded, inverted or otherwise.
        mock_service.import_file.assert_not_called()
        mock_service.pdf_preview.assert_not_called()


class TestImportConfirmPdfSign:
    """import_confirm(confirm_pdf_sign=True) ratifies a deterministic PDF inversion.

    The same credit-card inversion already elicits a human on the bridge and
    tabular channels. These tests pin the third channel to that one gate: a
    human decides, an agent never self-accepts, and a decline loads nothing.
    """

    def _card_pdf(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> Path:
        pdf = write_card_statement_pdf(tmp_path)
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database", _fake_database
        )
        return pdf

    def _sign_error(self) -> ImportConfirmationRequiredError:
        from moneybin.services.import_confirmation import SignConventionProposal

        return ImportConfirmationRequiredError(
            ConfirmationRequired(
                channel="pdf",
                confidence=_make_confidence(score=1.0, tier="high"),
                proposed=SignConventionProposal(
                    sign_convention="negative_is_income",
                    evidence=("Minimum Payment Due",),
                    sample_rows=[{"printed": "39.83", "recorded": "-39.83"}],
                ),
                reason="sign_convention",
                error_message="This looks like a credit-card statement.",
            )
        )

    async def test_confirm_pdf_sign_elicits_human_then_imports(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """The human ratifies once; the retry carries that ratification down."""
        from moneybin.services.import_service import ImportResult

        pdf = self._card_pdf(tmp_path, monkeypatch)
        mock_service = MagicMock()
        mock_service.pdf_preview.side_effect = self._sign_error()
        mock_service.import_file.return_value = ImportResult(
            file_path=str(pdf),
            file_type="pdf",
            transactions=24,
            import_id="pdf-sign-1",
        )
        confirm = AsyncMock()
        with (
            patch(
                "moneybin.services.import_service.ImportService",
                return_value=mock_service,
            ),
            patch("moneybin.mcp.elicitation.confirm_or_raise", confirm),
            patch("moneybin.services.inbox_service.InboxService"),
        ):
            result = await import_confirm(file_path=str(pdf), confirm_pdf_sign=True)

        assert result.error is None
        assert result.data.rows_loaded == 24
        assert result.data.import_id == "pdf-sign-1"
        confirm.assert_awaited_once()
        # The proposal is surfaced by the non-mutating probe, so the ONLY write
        # happens after the human approves — and it carries their ratification.
        mock_service.pdf_preview.assert_called_once()
        mock_service.import_file.assert_called_once()
        assert mock_service.import_file.call_args.kwargs["confirm"] is True

    async def test_confirm_pdf_sign_declined_imports_nothing(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """A refused (or unavailable) elicitation never inverts the ledger."""
        pdf = self._card_pdf(tmp_path, monkeypatch)
        mock_service = MagicMock()
        mock_service.pdf_preview.side_effect = self._sign_error()
        declined = AsyncMock(
            side_effect=UserError("declined", code="mutation_confirmation_required")
        )
        with (
            patch(
                "moneybin.services.import_service.ImportService",
                return_value=mock_service,
            ),
            patch("moneybin.mcp.elicitation.confirm_or_raise", declined),
        ):
            result = await import_confirm(file_path=str(pdf), confirm_pdf_sign=True)

        assert result.error is not None
        assert result.error.code == "mutation_confirmation_required"
        # A refusal writes NOTHING at all — the proposal came from the probe,
        # so no import attempt ran even to surface it.
        mock_service.import_file.assert_not_called()

    async def test_confirm_pdf_sign_prompt_names_the_statement_not_a_bridge(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """A deterministic PDF has no bridge recipe; the prompt must not claim one."""
        from moneybin.services.import_service import ImportResult

        pdf = self._card_pdf(tmp_path, monkeypatch)
        mock_service = MagicMock()
        mock_service.pdf_preview.side_effect = self._sign_error()
        mock_service.import_file.return_value = ImportResult(
            file_path=str(pdf), file_type="pdf", transactions=1, import_id="x"
        )
        confirm = AsyncMock()
        with (
            patch(
                "moneybin.services.import_service.ImportService",
                return_value=mock_service,
            ),
            patch("moneybin.mcp.elicitation.confirm_or_raise", confirm),
            patch("moneybin.services.inbox_service.InboxService"),
        ):
            await import_confirm(file_path=str(pdf), confirm_pdf_sign=True)

        assert confirm.await_args is not None
        message = confirm.await_args.args[0]
        assert "bridge" not in message.lower()
        assert "PDF statement" in message
        # The concrete flip the human is ratifying, not just an abstract claim.
        assert "39.83" in message

    async def test_confirm_pdf_sign_rejected_alongside_bridge_response(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """The two PDF channels are mutually exclusive, like accept/mapping."""
        pdf = self._card_pdf(tmp_path, monkeypatch)
        result = await import_confirm(
            file_path=str(pdf),
            bridge_response={"recipe": {}, "rows": []},
            confirm_pdf_sign=True,
        )
        assert result.error is not None
        assert result.error.code == "confirm_channel_conflict"

    async def test_confirm_pdf_sign_rejected_on_tabular_file(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """Tabular files ratify their inversion through accept=, not confirm_pdf_sign=."""
        csv_file = tmp_path / "statements" / "card.csv"
        csv_file.parent.mkdir(parents=True)
        csv_file.touch()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)

        result = await import_confirm(file_path=str(csv_file), confirm_pdf_sign=True)
        assert result.error is not None
        assert result.error.code == "confirm_channel_conflict"


def test_pdf_sign_actions_lead_with_the_mcp_confirm_path() -> None:
    """The agent's first hint is the in-MCP human gate, not the terminal."""
    from moneybin.mcp.tools.import_tools import (
        _sign_confirm_actions,  # pyright: ignore[reportPrivateUsage]
    )

    actions = _sign_confirm_actions(
        "/home/a/card.pdf", "looks like a card", channel="pdf"
    )

    assert "confirm_pdf_sign=True" in actions[1]
    # The terminal override for "it is NOT a card" survives as the escape hatch.
    assert any("--sign negative_is_expense" in a for a in actions)


class TestImportConfirmPdfSignBridgeEscalation:
    """confirm_pdf_sign on a PDF that turns out to need the bridge, not a sign decision."""

    async def test_bridge_escalation_returns_payload_without_blank_action(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """The bridge request carries no error_message — no empty action may leak."""
        pdf = write_card_statement_pdf(tmp_path)
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database", _fake_database
        )
        bridge_error = ImportConfirmationRequiredError(
            ConfirmationRequired(
                channel="pdf",
                confidence=_make_confidence(score=0.3, tier="low"),
                proposed=BridgePayload(
                    payload={"document_text": "…", "transparency_notice": "…"}
                ),
                reason="unknown_layout",
            )
        )
        mock_service = MagicMock()
        mock_service.pdf_preview.side_effect = bridge_error
        confirm = AsyncMock()
        with (
            patch(
                "moneybin.services.import_service.ImportService",
                return_value=mock_service,
            ),
            patch("moneybin.mcp.elicitation.confirm_or_raise", confirm),
        ):
            result = await import_confirm(file_path=str(pdf), confirm_pdf_sign=True)

        data = result.data
        assert isinstance(data, dict)
        assert data["status"] == "confirmation_required"
        assert data["bridge_payload"] is not None
        # A bridge request is not a sign decision — the human is never asked.
        confirm.assert_not_awaited()
        # Every hint must be substantive; a blank string is not a next step.
        assert all(action.strip() for action in result.actions)
        assert any("bridge_response" in action for action in result.actions)


async def test_confirm_pdf_sign_rejects_account_name_instead_of_dropping_it(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    """account_name is a tabular signal the PDF sign channel cannot honor.

    `_import_pdf` takes no account_name — accepting one and forwarding only
    account_id would silently bind the rows to a filename-derived account
    instead of the one the caller named. The bridge channel rejects it for
    exactly this reason; this channel must too.
    """
    pdf = write_card_statement_pdf(tmp_path)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr("moneybin.mcp.tools.import_tools.get_database", _fake_database)

    mock_service = MagicMock()
    with patch(
        "moneybin.services.import_service.ImportService", return_value=mock_service
    ):
        result = await import_confirm(
            file_path=str(pdf), confirm_pdf_sign=True, account_name="Chase Freedom"
        )

    assert result.error is not None
    assert result.error.code == "pdf_account_signal_unsupported"
    assert "account_id" in result.error.message
    # Refused before any import ran — no rows bound to the wrong account.
    mock_service.import_file.assert_not_called()


@pytest.mark.parametrize(
    "signal",
    [
        {"account_name": "Chase Freedom"},
        {"account_bindings": {"stmt-4387": "acct-123"}},
        {"account_metadata": {"stmt-4387": {"display_name": "Freedom"}}},
    ],
    ids=["account_name", "account_bindings", "account_metadata"],
)
@pytest.mark.parametrize(
    "channel",
    [
        {"confirm_pdf_sign": True},
        {"bridge_response": {"recipe": {}, "rows": []}},
    ],
    ids=["sign", "bridge"],
)
async def test_pdf_channels_reject_every_tabular_account_signal(
    channel: dict[str, object],
    signal: dict[str, object],
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Neither PDF channel may silently discard an account-selection signal.

    `_import_pdf` and `apply_pdf_bridge_response` both take only `account_id`.
    Any other account signal cannot be honored, so it must be refused rather
    than dropped — a drop binds the rows to a statement- or filename-derived
    account while the caller believes they chose one. The full matrix is pinned
    here so a fourth signal can't be added on one channel and forgotten on the
    other.
    """
    pdf = write_card_statement_pdf(tmp_path)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr("moneybin.mcp.tools.import_tools.get_database", _fake_database)

    mock_service = MagicMock()
    with patch(
        "moneybin.services.import_service.ImportService", return_value=mock_service
    ):
        result = await import_confirm(file_path=str(pdf), **channel, **signal)  # pyright: ignore[reportArgumentType]

    assert result.error is not None
    assert result.error.code == "pdf_account_signal_unsupported"
    # The message must name the offending parameter and the supported one.
    assert next(iter(signal)) in result.error.message
    assert "account_id" in result.error.message
    # Refused before any import ran — nothing bound to the wrong account.
    mock_service.import_file.assert_not_called()
    mock_service.apply_pdf_bridge_response.assert_not_called()


async def test_confirm_pdf_sign_without_a_pending_proposal_imports_nothing(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    """confirm_pdf_sign asserts a sign proposal exists; a false premise must not import.

    The caller is answering a question MoneyBin asked. If no sign gate actually
    fires for this PDF — a stale proposal, a replaced file, the wrong path —
    then running the import anyway silently does something the caller never
    requested (loading an ordinary statement, or writing seed rows) and returns
    success without a human ever being asked. Verify the premise read-only
    first.
    """
    from moneybin.services.import_service import PdfPreviewResult

    pdf = write_card_statement_pdf(tmp_path)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr("moneybin.mcp.tools.import_tools.get_database", _fake_database)

    mock_service = MagicMock()
    # The probe reports a clean deterministic PDF — no sign proposal pending.
    mock_service.pdf_preview.return_value = PdfPreviewResult(
        file_path=str(pdf),
        deterministic=True,
        decision_reason="passed",
        confidence=1.0,
        row_count=24,
    )
    confirm = AsyncMock()
    archive = MagicMock()
    with (
        patch(
            "moneybin.services.import_service.ImportService",
            return_value=mock_service,
        ),
        patch("moneybin.mcp.elicitation.confirm_or_raise", confirm),
        patch("moneybin.services.inbox_service.InboxService", archive),
    ):
        result = await import_confirm(file_path=str(pdf), confirm_pdf_sign=True)

    assert result.error is not None
    assert result.error.code == "sign_confirmation_not_pending"
    # Nothing was written and nobody was asked — the premise failed first.
    mock_service.import_file.assert_not_called()
    confirm.assert_not_awaited()
    archive.for_active_profile_no_db.assert_not_called()


class TestConfirmationBindsToTheApprovedBytes:
    """A sign approval must not transfer to content the human never saw.

    The prompt stays open as long as the person takes to answer (the tool
    allows 180s) and the retry re-reads the path. If the file is replaced in
    that window, a different card statement would get its inversion
    pre-ratified — every amount reversed in a document nobody reviewed. Each
    test replaces the file from inside the elicitation callback, which is
    exactly when the real race would land.
    """

    def _sign_error(self, channel: Channel) -> ImportConfirmationRequiredError:
        from moneybin.services.import_confirmation import SignConventionProposal

        return ImportConfirmationRequiredError(
            ConfirmationRequired(
                channel=channel,
                confidence=_make_confidence(score=1.0, tier="high"),
                proposed=SignConventionProposal(
                    sign_convention="negative_is_income",
                    evidence=("Minimum Payment Due",),
                    sample_rows=[],
                ),
                reason="sign_convention",
            )
        )

    async def test_pdf_sign_channel_refuses_a_swapped_file(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        pdf = write_card_statement_pdf(tmp_path)
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database", _fake_database
        )
        mock_service = MagicMock()
        mock_service.pdf_preview.side_effect = self._sign_error("pdf")

        async def _swap_file_while_prompt_is_open(*_a: object, **_k: object) -> None:
            pdf.write_bytes(b"%PDF-1.4 a completely different statement")

        with (
            patch(
                "moneybin.services.import_service.ImportService",
                return_value=mock_service,
            ),
            patch(
                "moneybin.mcp.elicitation.confirm_or_raise",
                AsyncMock(side_effect=_swap_file_while_prompt_is_open),
            ),
            patch("moneybin.services.inbox_service.InboxService"),
        ):
            result = await import_confirm(file_path=str(pdf), confirm_pdf_sign=True)

        assert result.error is not None
        assert result.error.code == "file_changed_during_confirmation"
        # The approval never reached the replacement.
        mock_service.import_file.assert_not_called()

    async def test_tabular_channel_refuses_a_swapped_file(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        csv_file = tmp_path / "statements" / "card.csv"
        csv_file.parent.mkdir(parents=True)
        csv_file.write_text("date,amount\n2026-01-01,10.00\n")
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database", _fake_database
        )
        mock_service = MagicMock()
        mock_service.import_file.side_effect = self._sign_error("tabular")

        async def _swap_file_while_prompt_is_open(*_a: object, **_k: object) -> None:
            csv_file.write_text("date,amount\n2026-02-02,-999.00\n")

        with (
            patch(
                "moneybin.services.import_service.ImportService",
                return_value=mock_service,
            ),
            patch(
                "moneybin.mcp.elicitation.confirm_or_raise",
                AsyncMock(side_effect=_swap_file_while_prompt_is_open),
            ),
            patch("moneybin.services.inbox_service.InboxService"),
            patch(
                "moneybin.extractors.tabular.format_detector.detect_format",
                side_effect=ValueError("preview unavailable"),
            ),
        ):
            result = await import_confirm(file_path=str(csv_file), accept=True)

        assert result.error is not None
        assert result.error.code == "file_changed_during_confirmation"
        # Only the gating attempt ran — the ratified retry never did.
        assert mock_service.import_file.call_count == 1

    async def test_bridge_channel_refuses_a_swapped_file(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        pdf = tmp_path / "statements" / "chase.pdf"
        pdf.parent.mkdir(parents=True)
        pdf.write_bytes(b"%PDF-1.4 original")
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_tools.get_database", _fake_database
        )
        mock_service = MagicMock()
        mock_service.apply_pdf_bridge_response.side_effect = self._sign_error("pdf")

        async def _swap_file_while_prompt_is_open(*_a: object, **_k: object) -> None:
            pdf.write_bytes(b"%PDF-1.4 a completely different statement")

        with (
            patch(
                "moneybin.services.import_service.ImportService",
                return_value=mock_service,
            ),
            patch(
                "moneybin.mcp.elicitation.confirm_or_raise",
                AsyncMock(side_effect=_swap_file_while_prompt_is_open),
            ),
            patch("moneybin.services.inbox_service.InboxService"),
        ):
            result = await import_confirm(
                file_path=str(pdf), bridge_response={"recipe": {}, "rows": []}
            )

        assert result.error is not None
        assert result.error.code == "file_changed_during_confirmation"
        # Only the gating attempt ran — the ratified retry never did.
        assert mock_service.apply_pdf_bridge_response.call_count == 1


@pytest.mark.parametrize(
    "tabular_signal",
    [{"accept": True}, {"mapping": {"amount": "Amount"}}],
    ids=["accept", "mapping"],
)
async def test_confirm_pdf_sign_rejects_tabular_mapping_signals(
    tabular_signal: dict[str, object], tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    """The sign channel takes no column mapping, and says so instead of guessing.

    Sibling coverage to `test_confirm_pdf_sign_rejected_alongside_bridge_response`:
    each channel selector must refuse the others' signals rather than silently
    picking one. A caller who learned the CLI's `--accept --confirm-sign`
    pairing will try exactly this combination.
    """
    pdf = write_card_statement_pdf(tmp_path)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr("moneybin.mcp.tools.import_tools.get_database", _fake_database)

    mock_service = MagicMock()
    with patch(
        "moneybin.services.import_service.ImportService", return_value=mock_service
    ):
        result = await import_confirm(
            file_path=str(pdf),
            confirm_pdf_sign=True,
            **tabular_signal,  # pyright: ignore[reportArgumentType]
        )

    assert result.error is not None
    assert result.error.code == "confirm_channel_conflict"
    # Names both the offending signal class and the channel that owns it.
    assert "mapping" in result.error.message
    # Refused before any probe or import ran.
    mock_service.pdf_preview.assert_not_called()
    mock_service.import_file.assert_not_called()
