"""Tests for import-tool helpers, including the file-path security boundary."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

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
    ConfirmationRequired,
    ImportConfirmationRequiredError,
    ProposedMapping,
)


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


# ---------------------------------------------------------------------------
# TestImportConfirmTool
# ---------------------------------------------------------------------------


class TestImportConfirmTool:
    """import_confirm tool: accept, override, validation, actor_kind."""

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
        assert result.error.code == "bridge_account_name_unsupported"

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
