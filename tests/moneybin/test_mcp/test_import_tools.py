"""Tests for import-tool helpers, including the file-path security boundary."""

from __future__ import annotations

import asyncio
import hashlib
import shlex
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest import MonkeyPatch

import moneybin.mcp.tools.import_tools as import_tools_module
from moneybin.errors import RecoveryAction, UserError
from moneybin.extractors.confidence import Confidence
from moneybin.mcp.tools.import_tools import (
    _bridge_confirm_action,  # pyright: ignore[reportPrivateUsage]
    _validate_file_path,  # pyright: ignore[reportPrivateUsage]
    import_confirm,
    import_confirm_coarse,
    import_files,
    import_files_coarse,
    import_formats,
    import_preview,
    import_preview_coarse,
    import_revert_coarse,
    import_status,
    import_status_coarse,
)
from moneybin.metrics.observations import MetricObservations
from moneybin.services.import_confirmation import (
    BridgePayload,
    Channel,
    ConfirmationRequired,
    ImportConfirmationRequiredError,
    ProposedMapping,
)
from moneybin.services.import_service import ReviewedTabularPlan
from tests.moneybin.pdf_statement_fixtures import write_card_statement_pdf
from tests.moneybin.test_mcp.schema_assertions import isolated_server


async def test_import_workflow_registrar_preserves_seven_trust_boundaries() -> None:
    registrar = import_tools_module.register_import_workflow_tools
    mcp = isolated_server(registrar)

    tools = await mcp._list_tools()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    names = {tool.name for tool in tools}

    assert names == {
        "import_files",
        "import_preview",
        "import_confirm",
        "import_status",
        "import_revert",
        "import_inbox_sync",
        "import_labels_set",
    }
    assert "import_formats" not in names
    assert "import_inbox_pending" not in names
    revert = next(tool for tool in tools if tool.name == "import_revert")
    assert "saved format" in (revert.description or "").lower()
    assert "confirmation" in (revert.description or "").lower()
    assert "system_audit_undo" in (revert.description or "")
    assert "confirmation_token" in revert.parameters["properties"]
    confirm = next(tool for tool in tools if tool.name == "import_confirm")
    assert "confirmation_token" in confirm.parameters["properties"]


def test_import_human_confirmation_tools_allow_decision_window() -> None:
    """Every public import tool that can elicit allows the established timeout."""
    assert import_confirm_coarse._mcp_timeout_seconds == 180.0  # type: ignore[attr-defined]
    assert import_revert_coarse._mcp_timeout_seconds == 180.0  # type: ignore[attr-defined]


async def test_import_files_coarse_preserves_warning_actions(
    monkeypatch: MonkeyPatch,
) -> None:
    from moneybin.privacy.payloads.imports import ImportFilesPayload, ImportPerFileRow
    from moneybin.protocol.envelope import build_envelope

    response = build_envelope(
        data=ImportFilesPayload(
            imported_count=1,
            failed_count=0,
            total_count=1,
            transforms_applied=False,
            transforms_duration_seconds=None,
            transforms_error="refresh failed",
            files=[
                ImportPerFileRow(
                    path="/Users/example/statement.csv",
                    status="complete",
                    source_type="csv",
                    rows_loaded=1,
                    import_id="imp_warning",
                    error=None,
                )
            ],
        ),
        actions=[
            "Refresh failed after import — call refresh_run to retry",
            "Use import_status(sections=['formats']) to inspect saved layouts",
        ],
    )
    monkeypatch.setattr(
        import_tools_module,
        "import_files",
        MagicMock(return_value=response),
    )

    result = await import_files_coarse(paths=["/Users/example/statement.csv"])

    assert "Refresh failed after import — call refresh_run to retry" in result.actions
    assert any(
        "import_status(sections=['formats'])" in action for action in result.actions
    )


async def test_import_revert_coarse_preserves_error_recovery(
    monkeypatch: MonkeyPatch,
) -> None:
    from moneybin.protocol.envelope import build_error_envelope

    recovery = RecoveryAction(
        tool="system_audit_undo",
        arguments={"operation_id": "op_recovery"},
        rationale="Restore the accepted import state.",
        confidence="certain",
        idempotent=False,
    )
    response = build_error_envelope(
        error=UserError(
            "Accepted state blocks revert.",
            code="revert_accepted",
            recovery_actions=[recovery],
        ),
        actions=["Inspect the accepted decision before retrying."],
    )
    monkeypatch.setattr(
        import_tools_module,
        "import_revert",
        MagicMock(return_value=response),
    )

    result = await import_revert_coarse(import_id="imp_accepted")

    assert result.error is not None
    assert result.actions == ["Inspect the accepted decision before retrying."]
    assert result.recovery_actions == [recovery]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"operation": "revert_import"},
        {
            "operation": "revert_import",
            "import_id": "import_1",
            "format_name": "saved_format",
        },
        {"operation": "delete_saved_format"},
        {
            "operation": "delete_saved_format",
            "import_id": "import_1",
            "format_name": "saved_format",
        },
    ],
)
async def test_import_revert_strictly_discriminates_destructive_targets(
    kwargs: dict[str, str],
) -> None:
    result = (await import_revert_coarse(**kwargs)).to_dict()  # type: ignore[arg-type]

    assert result["error"]["code"] == "import_revert_invalid_target"
    assert "exactly" in result["error"]["message"]


async def test_import_revert_deletes_a_saved_format_with_audit(
    mcp_db: Path,
) -> None:
    from moneybin.database import get_database
    from moneybin.extractors.tabular.formats import (
        TabularFormat,
        load_formats_from_db,
        save_format_to_db,
    )

    saved = TabularFormat(
        name="parity_saved",
        institution_name="Parity Bank",
        header_signature=["Date", "Amount"],
        field_mapping={"transaction_date": "Date", "amount": "Amount"},
        sign_convention="negative_is_expense",
        date_format="%m/%d/%Y",
    )
    with get_database(read_only=False) as db:
        save_format_to_db(db, saved, actor="test")

    required = await import_revert_coarse(
        operation="delete_saved_format",
        format_name="parity_saved",
    )
    assert required.error is not None
    assert required.error.code == "mutation_confirmation_required"
    assert required.error.details is not None
    assert required.error.details["operation_kind"] == "saved_format_delete"
    assert required.error.details["blast_radius"] == {"saved_formats": 1}

    result = await import_revert_coarse(
        operation="delete_saved_format",
        format_name="parity_saved",
        confirmation_token=str(required.error.details["confirmation_token"]),
    )

    assert result.error is None
    assert result.data.format_name == "parity_saved"
    assert result.data.status == "deleted"
    assert result.data.operation_id.startswith("op_")
    recovery = cast(list[RecoveryAction], result.recovery_actions or [])
    assert [action.tool for action in recovery] == [
        "system_audit",
        "system_audit_undo",
    ]
    with get_database(read_only=True) as db:
        assert "parity_saved" not in load_formats_from_db(db)
        audit = db.execute(
            """
            SELECT action, actor, operation_id
            FROM app.audit_log
            WHERE action = 'tabular_format.delete'
            ORDER BY occurred_at DESC
            LIMIT 1
            """
        ).fetchone()
    assert audit == ("tabular_format.delete", "mcp", result.data.operation_id)

    from moneybin.mcp.tools.system import system_audit_undo

    undo = await system_audit_undo(result.data.operation_id)
    assert undo.error is None
    with get_database(read_only=True) as db:
        assert "parity_saved" in load_formats_from_db(db)


async def test_import_revert_rejects_stale_saved_format_confirmation(
    mcp_db: Path,
) -> None:
    from moneybin.database import get_database
    from moneybin.extractors.tabular.formats import (
        TabularFormat,
        load_formats_from_db,
        save_format_to_db,
    )

    original = TabularFormat(
        name="stale_saved",
        institution_name="Before",
        header_signature=["Date", "Amount"],
        field_mapping={"transaction_date": "Date", "amount": "Amount"},
        sign_convention="negative_is_expense",
        date_format="%m/%d/%Y",
    )
    with get_database(read_only=False) as db:
        save_format_to_db(db, original, actor="test")
    required = await import_revert_coarse(
        operation="delete_saved_format",
        format_name="stale_saved",
    )
    assert required.error is not None
    assert required.error.details is not None

    changed = original.model_copy(update={"institution_name": "After"})
    with get_database(read_only=False) as db:
        save_format_to_db(db, changed, actor="test")

    result = await import_revert_coarse(
        operation="delete_saved_format",
        format_name="stale_saved",
        confirmation_token=str(required.error.details["confirmation_token"]),
    )

    assert result.error is not None
    assert result.error.code == "mutation_confirmation_mismatch"
    with get_database(read_only=True) as db:
        assert "stale_saved" in load_formats_from_db(db)


async def test_import_revert_confirmation_is_bound_to_one_format(
    mcp_db: Path,
) -> None:
    from moneybin.database import get_database
    from moneybin.extractors.tabular.formats import (
        TabularFormat,
        load_formats_from_db,
        save_format_to_db,
    )

    for name in ("bound_a", "bound_b"):
        with get_database(read_only=False) as db:
            save_format_to_db(
                db,
                TabularFormat(
                    name=name,
                    institution_name="Parity",
                    header_signature=["Date", "Amount"],
                    field_mapping={
                        "transaction_date": "Date",
                        "amount": "Amount",
                    },
                    sign_convention="negative_is_expense",
                    date_format="%m/%d/%Y",
                ),
                actor="test",
            )
    required = await import_revert_coarse(
        operation="delete_saved_format",
        format_name="bound_a",
    )
    assert required.error is not None
    assert required.error.details is not None

    result = await import_revert_coarse(
        operation="delete_saved_format",
        format_name="bound_b",
        confirmation_token=str(required.error.details["confirmation_token"]),
    )

    assert result.error is not None
    assert result.error.code == "mutation_confirmation_mismatch"
    with get_database(read_only=True) as db:
        saved = load_formats_from_db(db)
    assert {"bound_a", "bound_b"} <= set(saved)


async def test_import_revert_refuses_builtin_format_deletion(mcp_db: Path) -> None:
    result = (
        await import_revert_coarse(
            operation="delete_saved_format",
            format_name="tiller",
        )
    ).to_dict()

    assert result["error"]["code"] == "saved_format_builtin_immutable"


def _issue_coarse_preview(
    path: Path,
    *,
    channel: str,
    data: dict[str, object],
) -> str:
    from moneybin.database import get_database
    from moneybin.mcp.tools.import_tools import (
        _file_identity,  # pyright: ignore[reportPrivateUsage]
    )
    from moneybin.repositories.import_previews_repo import ImportPreviewsRepo

    sha256, size = _file_identity(path)  # pyright: ignore[reportPrivateUsage]
    now = datetime.now(UTC)
    with get_database(read_only=False) as db:
        return ImportPreviewsRepo(db).issue(
            file_path=str(path),
            file_sha256=sha256,
            file_size_bytes=size,
            channel=channel,  # type: ignore[arg-type]
            source_bytes=path.read_bytes(),
            snapshot={"data": data, "actions": [], "sensitivity": "medium"},
            issued_at=now,
            expires_at=now + timedelta(minutes=5),
            actor="mcp",
        )


def _valid_bridge_response() -> dict[str, object]:
    """Return a valid recipe shape that reaches PDF extraction."""
    return {
        "recipe": {
            "row_region": {
                "start_anchor": "Transactions",
                "end_anchor": "Total:",
            },
            "row_split": r"\s{2,}",
            "fields": [
                {
                    "name": "Date",
                    "pattern": r"\d{2}/\d{2}/\d{4}",
                    "cast": "date",
                    "date_format": "%m/%d/%Y",
                },
                {"name": "Description", "pattern": r".+", "cast": "str"},
                {
                    "name": "Amount",
                    "pattern": r"-?\$?[\d,]+\.\d{2}",
                    "cast": "decimal",
                },
            ],
            "sign_convention": "negative_is_expense",
            "routing": "transactions",
        },
        "rows": [
            {
                "Date": "01/15/2024",
                "Description": "Coffee",
                "Amount": "-4.50",
            }
        ],
    }


def _coarse_sign_error(
    channel: Channel,
    *,
    evidence: tuple[str, ...] = ("minimum payment", "credit limit"),
) -> ImportConfirmationRequiredError:
    """Return a sign proposal suitable for coarse confirm replay tests."""
    from moneybin.services.import_confirmation import SignConventionProposal

    return ImportConfirmationRequiredError(
        ConfirmationRequired(
            channel=channel,
            confidence=_make_confidence(score=0.75, tier="medium"),
            proposed=SignConventionProposal(
                sign_convention="negative_is_income",
                evidence=evidence,
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
    )


def _track_completed_rollbacks(
    monkeypatch: MonkeyPatch,
    events: list[str],
) -> None:
    """Append a marker only after the real database rollback returns."""
    from moneybin.database import Database

    real_rollback = Database.rollback

    def observed_rollback(db: Database) -> None:
        real_rollback(db)
        events.append("rollback")

    monkeypatch.setattr(Database, "rollback", observed_rollback)


def _event_counter(events: list[str], event: str) -> MagicMock:
    """Return a counter mock that records each increment in event order."""
    metric = MagicMock()
    metric.labels.return_value.inc.side_effect = lambda: events.append(event)
    return metric


async def test_import_preview_binds_parse_hash_and_storage_to_one_byte_read(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    csv = tmp_path / "statement.csv"
    original = b"Date,Description,Amount\n2026-07-01,Reviewed Coffee,-4.50\n"
    replacement = b"Date,Description,Amount\n2026-07-01,Unreviewed Wire,-9000.00\n"
    csv.write_bytes(original)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)

    from moneybin.extractors.tabular import column_mapper

    real_map = column_mapper.map_columns

    def swap_after_parse(*args: object, **kwargs: object) -> object:
        result = real_map(*args, **kwargs)  # type: ignore[arg-type]
        csv.write_bytes(replacement)
        return result

    monkeypatch.setattr(column_mapper, "map_columns", swap_after_parse)

    response = await import_preview_coarse(file_path=str(csv))

    from moneybin.database import get_database
    from moneybin.repositories.import_previews_repo import ImportPreviewsRepo

    preview_id = response.data.preview_id
    with get_database(read_only=True) as db:
        repo = ImportPreviewsRepo(db)
        row = repo.get(preview_id)
        assert row is not None
        assert row["file_sha256"] == hashlib.sha256(original).hexdigest()
        assert row["file_size_bytes"] == len(original)
        assert repo.get_source_bytes(preview_id) == original


@pytest.mark.parametrize(
    ("suffix", "limit_field"),
    [
        (".csv", "text_size_limit_mb"),
        (".xlsx", "binary_size_limit_mb"),
    ],
)
async def test_import_preview_rejects_oversized_tabular_before_full_read(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    suffix: str,
    limit_field: str,
) -> None:
    """The configured limit runs before preview materializes the whole file."""
    source = tmp_path / f"oversized{suffix}"
    source.write_bytes(b"x")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)

    from moneybin.config import get_settings

    settings = get_settings()
    limited_tabular = settings.providers.tabular.model_copy(update={limit_field: 0})
    limited_providers = settings.providers.model_copy(
        update={"tabular": limited_tabular}
    )
    limited_settings = settings.model_copy(update={"providers": limited_providers})
    monkeypatch.setattr(
        "moneybin.config.get_settings",
        lambda: limited_settings,
    )
    with patch.object(Path, "open", autospec=True) as full_open:
        response = await import_preview_coarse(file_path=str(source))

    assert response.error is not None
    assert response.error.code == "preview_error"
    full_open.assert_not_called()


async def test_import_preview_rejects_file_growth_during_bounded_capture(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A post-preflight growth race reads at most expected size plus one."""
    source = tmp_path / "growing.csv"
    source_bytes = b"12345"
    source.write_bytes(source_bytes)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)

    from moneybin.extractors.tabular.format_detector import FormatInfo

    def stale_format(_path: Path) -> FormatInfo:
        return FormatInfo(file_type="csv", file_size=4)

    monkeypatch.setattr(
        "moneybin.extractors.tabular.format_detector.detect_format",
        stale_format,
    )
    read_sizes: list[int | None] = []

    class TrackedBytesIO(BytesIO):
        def read(self, size: int | None = -1) -> bytes:
            read_sizes.append(size)
            return super().read(size)

    with patch.object(
        Path,
        "open",
        autospec=True,
        return_value=TrackedBytesIO(source_bytes),
    ) as bounded_open:
        response = await import_preview_coarse(file_path=str(source))

    assert response.error is not None
    assert response.error.code == "IMPORT_PREVIEW_CHANGED"
    bounded_open.assert_called_once()
    assert read_sizes == [5]


async def test_import_confirm_loads_stored_bytes_after_post_hash_swap(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    csv = tmp_path / "statement.csv"
    original = b"Date,Description,Amount\n2026-07-01,Reviewed Coffee,-4.50\n"
    replacement = b"Date,Description,Amount\n2026-07-01,Unreviewed Wire,-9000.00\n"
    csv.write_bytes(original)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    preview = await import_preview_coarse(file_path=str(csv))

    real_identity = import_tools_module._file_identity  # pyright: ignore[reportPrivateUsage]

    def swap_after_hash(path: Path) -> tuple[str, int]:
        identity = real_identity(path)
        path.write_bytes(replacement)
        return identity

    monkeypatch.setattr(import_tools_module, "_file_identity", swap_after_hash)
    captured: dict[str, object] = {}

    from moneybin.services.import_service import ImportResult

    def capture_import(_service: object, path: Path, **kwargs: object) -> ImportResult:
        captured["live_bytes"] = path.read_bytes()
        captured.update(kwargs)
        return ImportResult(
            file_path=str(path),
            file_type="tabular",
            transactions=1,
            import_id="imp_snapshot",
            field_mapping={
                "transaction_date": "Date",
                "description": "Description",
                "amount": "Amount",
            },
        )

    monkeypatch.setattr(
        "moneybin.services.import_service.ImportService.import_file",
        capture_import,
    )

    response = await import_confirm_coarse(
        preview_id=preview.data.preview_id,
        account_name="Checking",
    )

    assert response.data.import_id == "imp_snapshot"
    assert captured["live_bytes"] == replacement
    assert captured["source_bytes"] == original
    plan = captured["reviewed_plan"]
    assert isinstance(plan, ReviewedTabularPlan)
    assert plan.field_mapping == {
        "transaction_date": "Date",
        "description": "Description",
        "amount": "Amount",
    }
    assert plan.delimiter == ","
    assert plan.encoding == "utf-8"
    assert plan.date_format == "%Y-%m-%d"
    assert plan.sign_convention == "negative_is_expense"
    assert plan.skip_rows == 0
    assert plan.has_header is True


async def test_import_confirm_ignores_format_created_after_preview(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    csv = tmp_path / "statement.csv"
    csv.write_text("Date,Description,Amount\n2026-07-01,Reviewed Coffee,-4.50\n")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    preview = await import_preview_coarse(file_path=str(csv))

    from moneybin.database import get_database
    from moneybin.extractors.tabular.formats import TabularFormat, save_format_to_db

    conflicting = TabularFormat(
        name="late_conflict",
        institution_name="Wrong Bank",
        file_type="csv",
        delimiter=",",
        encoding="utf-8",
        header_signature=["Date", "Description", "Amount"],
        field_mapping={
            "transaction_date": "Date",
            "description": "Amount",
            "amount": "Description",
        },
        sign_convention="negative_is_income",
        date_format="%m/%d/%Y",
    )
    with get_database(read_only=False) as db:
        save_format_to_db(db, conflicting, actor="test")

    confirmed = await import_confirm_coarse(
        preview_id=preview.data.preview_id,
        account_name="Checking",
        save_format=False,
    )

    assert confirmed.data.status == "complete"
    assert confirmed.data.merged_mapping == {
        "transaction_date": "Date",
        "description": "Description",
        "amount": "Amount",
    }


async def test_import_preview_pdf_bridge_keeps_recipe_inputs_usable(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pdf = tmp_path / "statement.pdf"
    pdf.write_bytes(b"%PDF bridge fixture")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    service = MagicMock()
    service.pdf_preview.side_effect = _bridge_error()

    with patch(
        "moneybin.services.import_service.ImportService",
        return_value=service,
    ):
        response = await import_preview_coarse(file_path=str(pdf))

    from moneybin.privacy.payloads.imports import ImportPdfBridgePreviewPayload

    assert isinstance(response.data, ImportPdfBridgePreviewPayload)
    assert response.summary.sensitivity == "medium"
    assert "description" in (response.classes_returned or [])
    assert response.data.bridge_payload.document_text == (
        "Chase Bank\nDate Description Amount\n05/01 COFFEE SHOP -12.34"
    )
    assert response.data.bridge_payload.tables_preview[0].rows == [
        ["05/01", "COFFEE SHOP", "-12.34"]
    ]
    assert any(
        f"preview_id='{response.data.preview_id}'" in action
        for action in response.actions
    )


@pytest.mark.parametrize("deterministic", [True, False])
async def test_import_preview_pdf_direct_modes_are_typed_and_route_to_import_files(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    deterministic: bool,
) -> None:
    from moneybin.services.import_service import PdfPreviewResult

    pdf = tmp_path / "statement.pdf"
    pdf.write_bytes(b"%PDF direct fixture")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    service = MagicMock()
    service.pdf_preview.return_value = PdfPreviewResult(
        file_path=str(pdf),
        deterministic=deterministic,
        decision_reason="passed" if deterministic else "no_transaction_table",
        confidence=0.95 if deterministic else 0.2,
        row_count=2 if deterministic else 0,
        fingerprint={"issuer": "example"},
    )

    with patch(
        "moneybin.services.import_service.ImportService",
        return_value=service,
    ):
        response = await import_preview_coarse(file_path=str(pdf))

    assert response.data.kind in {"pdf_deterministic", "pdf_seed"}
    assert any("import_files" in action for action in response.actions)
    assert not any("import_confirm(" in action for action in response.actions)
    assert response.classes_returned


async def test_import_preview_pdf_sign_mode_routes_to_human_confirm(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pdf = tmp_path / "statement.pdf"
    pdf.write_bytes(b"%PDF sign fixture")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    service = MagicMock()
    service.pdf_preview.side_effect = _sign_error()

    with patch(
        "moneybin.services.import_service.ImportService",
        return_value=service,
    ):
        response = await import_preview_coarse(file_path=str(pdf))

    assert response.data.kind == "pdf_sign"
    assert any(
        f"import_confirm(preview_id='{response.data.preview_id}')" in action
        for action in response.actions
    )
    assert "txn_amount" in (response.classes_returned or [])


async def test_import_confirm_coarse_elicits_pdf_sign_then_imports_snapshot(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    from moneybin.privacy.payloads.imports import ImportPdfSignAppliedPayload
    from moneybin.services.import_service import ImportResult

    pdf = tmp_path / "statement.pdf"
    source_bytes = b"%PDF sign fixture"
    pdf.write_bytes(source_bytes)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    preview_service = MagicMock()
    preview_service.pdf_preview.side_effect = _sign_error()
    with patch(
        "moneybin.services.import_service.ImportService",
        return_value=preview_service,
    ):
        preview = await import_preview_coarse(file_path=str(pdf))

    apply = MagicMock(
        side_effect=[
            _sign_error(),
            _sign_error(),
            ImportResult(
                file_path=str(pdf),
                file_type="pdf",
                transactions=2,
                import_id="imp_pdf_sign",
            ),
        ]
    )
    monkeypatch.setattr(
        "moneybin.services.import_service.ImportService.import_file",
        apply,
    )
    confirm = AsyncMock(return_value=MagicMock())
    monkeypatch.setattr(
        "moneybin.mcp.tools.import_tools.grant_confirmation_or_raise",
        confirm,
    )
    threaded: list[str] = []
    real_to_thread = asyncio.to_thread

    async def observed_to_thread(
        func: object,
        /,
        *args: object,
        **kwargs: object,
    ) -> object:
        threaded.append(func.__name__)  # type: ignore[attr-defined]
        return await real_to_thread(func, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(import_tools_module.asyncio, "to_thread", observed_to_thread)

    response = await import_confirm_coarse(preview_id=preview.data.preview_id)

    assert response.error is None
    assert isinstance(response.data, ImportPdfSignAppliedPayload)
    assert response.data.kind == "pdf_sign_applied"
    assert response.data.import_id == "imp_pdf_sign"
    confirm.assert_awaited_once()
    assert apply.call_count == 3
    assert apply.call_args_list[0].kwargs["confirm"] is False
    assert apply.call_args_list[1].kwargs["confirm"] is False
    assert apply.call_args_list[2].kwargs["confirm"] is True
    assert apply.call_args_list[2].kwargs["source_bytes"] == source_bytes
    assert [name for name in threaded if name.startswith("_")] == [
        "_load_import_confirm_preview",
        "_run_import_confirm_attempt",
        "_run_import_confirm_attempt",
    ]


async def test_import_confirm_coarse_refuses_changed_pdf_sign_proposal(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pdf = tmp_path / "statement.pdf"
    pdf.write_bytes(b"%PDF sign fixture")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    preview_id = _issue_coarse_preview(
        pdf,
        channel="pdf",
        data={
            "status": "confirmation_required",
            "channel": "pdf",
            "reason": "sign_convention",
        },
    )
    apply = MagicMock(
        side_effect=[
            _coarse_sign_error("pdf"),
            _coarse_sign_error("pdf", evidence=("account type changed",)),
        ]
    )
    monkeypatch.setattr(
        "moneybin.services.import_service.ImportService.import_file",
        apply,
    )
    confirm = AsyncMock(return_value=MagicMock())
    monkeypatch.setattr(
        "moneybin.mcp.tools.import_tools.grant_confirmation_or_raise",
        confirm,
    )

    response = await import_confirm_coarse(preview_id=preview_id)

    assert response.error is not None
    assert response.error.code == "IMPORT_SIGN_PROPOSAL_CHANGED"
    confirm.assert_awaited_once()
    assert [call.kwargs["confirm"] for call in apply.call_args_list] == [False, False]
    from moneybin.database import get_database
    from moneybin.repositories.import_previews_repo import ImportPreviewsRepo

    with get_database(read_only=True) as db:
        assert ImportPreviewsRepo(db).get(preview_id)["consumed_at"] is None  # type: ignore[index]


async def test_import_confirm_coarse_refuses_disappeared_pdf_sign_proposal(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    from moneybin.services.import_service import ImportResult

    pdf = tmp_path / "statement.pdf"
    pdf.write_bytes(b"%PDF sign fixture")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    preview_id = _issue_coarse_preview(
        pdf,
        channel="pdf",
        data={
            "status": "confirmation_required",
            "channel": "pdf",
            "reason": "sign_convention",
        },
    )
    apply = MagicMock(
        side_effect=[
            _coarse_sign_error("pdf"),
            ImportResult(
                file_path=str(pdf),
                file_type="pdf",
                transactions=1,
                import_id="must_rollback",
            ),
        ]
    )
    monkeypatch.setattr(
        "moneybin.services.import_service.ImportService.import_file",
        apply,
    )
    monkeypatch.setattr(
        "moneybin.mcp.tools.import_tools.grant_confirmation_or_raise",
        AsyncMock(return_value=MagicMock()),
    )

    response = await import_confirm_coarse(preview_id=preview_id)

    assert response.error is not None
    assert response.error.code == "IMPORT_SIGN_PROPOSAL_CHANGED"
    assert [call.kwargs["confirm"] for call in apply.call_args_list] == [False, False]
    from moneybin.database import get_database
    from moneybin.repositories.import_previews_repo import ImportPreviewsRepo

    with get_database(read_only=True) as db:
        preview = ImportPreviewsRepo(db).get(preview_id)
        assert preview is not None
        assert preview["consumed_at"] is None
        assert preview["import_id"] is None


@pytest.mark.parametrize("channel", ["pdf", "tabular", "bridge"])
@pytest.mark.parametrize("proposal_state", ["changed", "disappeared"])
async def test_import_confirm_sign_revalidation_rolls_back_all_raw_rows(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    channel: str,
    proposal_state: str,
) -> None:
    from moneybin.loaders import import_log
    from moneybin.services.import_service import ImportResult, ImportService

    source = tmp_path / ("statement.csv" if channel == "tabular" else "statement.pdf")
    if channel == "tabular":
        source.write_text("Date,Description,Amount\n2026-07-01,Coffee,4.50\n")
    else:
        source.write_bytes(b"%PDF sign fixture")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    if channel == "tabular":
        preview_id = (
            await import_preview_coarse(file_path=str(source))
        ).data.preview_id
    else:
        preview_id = _issue_coarse_preview(
            source,
            channel="pdf",
            data=(
                {
                    "status": "confirmation_required",
                    "channel": "pdf",
                    "reason": "low_confidence",
                    "bridge_payload": {},
                }
                if channel == "bridge"
                else {
                    "status": "confirmation_required",
                    "channel": "pdf",
                    "reason": "sign_convention",
                }
            ),
        )
    proposal_channel: Channel = "tabular" if channel == "tabular" else "pdf"
    calls = 0

    def apply(service: ImportService, *_args: object, **_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise _coarse_sign_error(proposal_channel)
        import_id = import_log.begin_import(
            service._db,  # pyright: ignore[reportPrivateUsage]
            source_file=str(source),
            source_type="pdf",
            source_origin="rollback_probe",
            account_names=["rollback_probe"],
        )
        service._db.execute(  # pyright: ignore[reportPrivateUsage]
            "INSERT INTO raw.pdf_seeds "
            "(alias, row_hash, data, source_file, page, import_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                "rollback_probe",
                f"probe_{channel}_{proposal_state}",
                "{}",
                str(source),
                1,
                import_id,
            ],
        )
        if proposal_state == "changed":
            raise _coarse_sign_error(
                proposal_channel,
                evidence=("proposal changed after approval",),
            )
        if channel == "bridge":
            return SimpleNamespace(
                outcome="applied",
                import_id=import_id,
                rows_loaded=1,
                format_name="must_rollback",
                expected_row_count=1,
                actual_row_count=1,
                rows_diverged=False,
                reject_reason=None,
            )
        return ImportResult(
            file_path=str(source),
            file_type="pdf" if channel == "pdf" else "tabular",
            transactions=1,
            import_id=import_id,
        )

    method = "apply_pdf_bridge_response" if channel == "bridge" else "import_file"
    monkeypatch.setattr(
        f"moneybin.services.import_service.ImportService.{method}",
        apply,
    )
    monkeypatch.setattr(
        "moneybin.mcp.tools.import_tools.grant_confirmation_or_raise",
        AsyncMock(return_value=MagicMock()),
    )

    response = await import_confirm_coarse(
        preview_id=preview_id,
        bridge_response=(
            {"recipe": {"version": 1}, "rows": [{}]} if channel == "bridge" else None
        ),
        account_name="Credit card" if channel == "tabular" else None,
    )

    assert response.error is not None
    assert response.error.code == "IMPORT_SIGN_PROPOSAL_CHANGED"
    assert calls == 2
    from moneybin.database import get_database
    from moneybin.repositories.import_previews_repo import ImportPreviewsRepo

    with get_database(read_only=True) as db:
        import_log_count = db.execute("SELECT COUNT(*) FROM raw.import_log").fetchone()
        seed_count = db.execute("SELECT COUNT(*) FROM raw.pdf_seeds").fetchone()
        assert import_log_count is not None and import_log_count[0] == 0
        assert seed_count is not None and seed_count[0] == 0
        repo = ImportPreviewsRepo(db)
        preview = repo.get(preview_id)
        assert preview is not None and preview["consumed_at"] is None
        assert repo.get_source_bytes(preview_id) == source.read_bytes()


async def test_import_confirm_coarse_revalidates_tabular_sign_inside_write_attempt(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    from moneybin.services.import_service import ImportResult

    csv = tmp_path / "statement.csv"
    csv.write_text("Date,Description,Amount\n2026-07-01,Coffee,4.50\n")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    preview = await import_preview_coarse(file_path=str(csv))
    proposal = _coarse_sign_error("tabular")
    apply = MagicMock(
        side_effect=[
            proposal,
            proposal,
            ImportResult(
                file_path=str(csv),
                file_type="tabular",
                transactions=1,
                import_id="imp_tabular_sign",
            ),
        ]
    )
    monkeypatch.setattr(
        "moneybin.services.import_service.ImportService.import_file",
        apply,
    )
    monkeypatch.setattr(
        "moneybin.mcp.tools.import_tools.grant_confirmation_or_raise",
        AsyncMock(return_value=MagicMock()),
    )

    response = await import_confirm_coarse(
        preview_id=preview.data.preview_id,
        account_name="Credit card",
    )

    assert response.error is None
    assert [
        call.kwargs["human_sign_confirmation"] for call in apply.call_args_list
    ] == [False, False, True]


async def test_import_confirm_coarse_revalidates_bridge_sign_inside_write_attempt(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pdf = tmp_path / "statement.pdf"
    pdf.write_bytes(b"%PDF bridge fixture")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    preview_id = _issue_coarse_preview(
        pdf,
        channel="pdf",
        data={
            "status": "confirmation_required",
            "channel": "pdf",
            "reason": "low_confidence",
            "bridge_payload": {},
        },
    )
    proposal = _coarse_sign_error("pdf")
    applied = SimpleNamespace(
        outcome="applied",
        import_id="imp_bridge_sign",
        rows_loaded=1,
        format_name="bridge_recipe",
        expected_row_count=1,
        actual_row_count=1,
        rows_diverged=False,
        reject_reason=None,
    )
    apply = MagicMock(side_effect=[proposal, proposal, applied])
    monkeypatch.setattr(
        "moneybin.services.import_service.ImportService.apply_pdf_bridge_response",
        apply,
    )
    monkeypatch.setattr(
        "moneybin.mcp.tools.import_tools.grant_confirmation_or_raise",
        AsyncMock(return_value=MagicMock()),
    )

    response = await import_confirm_coarse(
        preview_id=preview_id,
        bridge_response={"recipe": {"version": 1}, "rows": [{}]},
    )

    assert response.error is None
    assert [call.kwargs["confirm"] for call in apply.call_args_list] == [
        False,
        False,
        True,
    ]


async def test_import_confirm_duration_excludes_human_confirmation_wait(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    from moneybin.services.import_service import ImportResult

    pdf = tmp_path / "statement.pdf"
    pdf.write_bytes(b"%PDF sign fixture")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    preview_id = _issue_coarse_preview(
        pdf,
        channel="pdf",
        data={
            "status": "confirmation_required",
            "channel": "pdf",
            "reason": "sign_convention",
        },
    )
    starts: list[float] = []

    def attempt(**kwargs: object) -> object:
        starts.append(cast(float, kwargs["started"]))
        if len(starts) == 1:
            raise _coarse_sign_error("pdf")
        return (
            None,
            ImportResult(
                file_path=str(pdf),
                file_type="pdf",
                transactions=1,
                import_id="imp_duration",
            ),
            "imp_duration",
        )

    monkeypatch.setattr(import_tools_module, "_run_import_confirm_attempt", attempt)
    monkeypatch.setattr(
        "moneybin.mcp.tools.import_tools.grant_confirmation_or_raise",
        AsyncMock(return_value=MagicMock()),
    )
    monkeypatch.setattr(
        import_tools_module,
        "time",
        SimpleNamespace(monotonic=MagicMock(side_effect=[10.0, 1000.0])),
    )

    response = await import_confirm_coarse(preview_id=preview_id)

    assert response.error is None
    assert starts == [10.0, 1000.0]


async def test_import_confirm_bridge_sign_degraded_client_retries_with_opaque_token(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pdf = tmp_path / "statement.pdf"
    pdf.write_bytes(b"%PDF bridge fixture")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    preview_id = _issue_coarse_preview(
        pdf,
        channel="pdf",
        data={
            "status": "confirmation_required",
            "channel": "pdf",
            "reason": "low_confidence",
            "bridge_payload": {},
        },
    )
    proposal = _coarse_sign_error("pdf")
    applied = SimpleNamespace(
        outcome="applied",
        import_id="imp_bridge_token",
        rows_loaded=1,
        format_name="bridge_recipe",
        expected_row_count=1,
        actual_row_count=1,
        rows_diverged=False,
        reject_reason=None,
    )
    apply = MagicMock(side_effect=[proposal, proposal, proposal, applied])
    monkeypatch.setattr(
        "moneybin.services.import_service.ImportService.apply_pdf_bridge_response",
        apply,
    )
    monkeypatch.setattr(
        "moneybin.mcp.confirmation._active_context",
        MagicMock(return_value=None),
    )
    monkeypatch.setattr(
        "moneybin.mcp.confirmation.supports_elicitation",
        MagicMock(return_value=False),
    )
    bridge_response = {"recipe": {"version": 1}, "rows": [{}]}

    required = await import_confirm_coarse(
        preview_id=preview_id,
        bridge_response=bridge_response,
    )

    assert required.error is not None
    assert required.error.details is not None
    token = str(required.error.details["confirmation_token"])
    assert "bridge_response" not in required.error.details
    confirmed = await import_confirm_coarse(
        preview_id=preview_id,
        bridge_response=bridge_response,
        confirmation_token=token,
    )

    assert confirmed.error is None
    assert confirmed.data.import_id == "imp_bridge_token"
    assert [call.kwargs["confirm"] for call in apply.call_args_list] == [
        False,
        False,
        False,
        True,
    ]
    replayed = await import_confirm_coarse(
        preview_id=preview_id,
        bridge_response=bridge_response,
        confirmation_token=token,
    )
    assert replayed.error is not None
    assert replayed.error.code == "mutation_confirmation_replayed"


async def test_import_confirm_token_reconstruction_does_not_double_count_sign_proposal(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    from moneybin.metrics.registry import PDF_SIGN_GATE_TOTAL

    pdf = write_card_statement_pdf(tmp_path)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    preview_id = _issue_coarse_preview(
        pdf,
        channel="pdf",
        data={
            "status": "confirmation_required",
            "channel": "pdf",
            "reason": "sign_convention",
        },
    )
    monkeypatch.setattr(
        "moneybin.mcp.confirmation._active_context",
        MagicMock(return_value=None),
    )
    proposed = PDF_SIGN_GATE_TOTAL.labels(outcome="proposed")
    confirmed_metric = PDF_SIGN_GATE_TOTAL.labels(outcome="confirmed")
    proposed_before = proposed._value.get()  # type: ignore[attr-defined]  # testing prometheus internals
    confirmed_before = confirmed_metric._value.get()  # type: ignore[attr-defined]  # testing prometheus internals

    required = await import_confirm_coarse(preview_id=preview_id)

    assert required.error is not None
    assert required.error.details is not None
    assert proposed._value.get() == proposed_before + 1  # type: ignore[attr-defined]  # testing prometheus internals
    confirmed = await import_confirm_coarse(
        preview_id=preview_id,
        confirmation_token=str(required.error.details["confirmation_token"]),
    )

    assert confirmed.error is None
    assert proposed._value.get() == proposed_before + 1  # type: ignore[attr-defined]  # testing prometheus internals
    assert confirmed_metric._value.get() == confirmed_before + 1  # type: ignore[attr-defined]  # testing prometheus internals


async def test_import_confirm_bridge_token_without_reconstructed_proposal_is_mismatch(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    from moneybin import error_codes

    pdf = tmp_path / "statement.pdf"
    pdf.write_bytes(b"%PDF bridge fixture")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    preview_id = _issue_coarse_preview(
        pdf,
        channel="pdf",
        data={
            "status": "confirmation_required",
            "channel": "pdf",
            "reason": "low_confidence",
            "bridge_payload": {},
        },
    )
    proposal = _coarse_sign_error("pdf")
    invalid = SimpleNamespace(
        outcome="invalid",
        import_id=None,
        rows_loaded=0,
        format_name=None,
        expected_row_count=1,
        actual_row_count=0,
        rows_diverged=True,
        reject_reason="reconciliation_failed",
    )
    apply = MagicMock(side_effect=[proposal, invalid])
    monkeypatch.setattr(
        "moneybin.services.import_service.ImportService.apply_pdf_bridge_response",
        apply,
    )
    monkeypatch.setattr(
        "moneybin.mcp.confirmation._active_context",
        MagicMock(return_value=None),
    )
    monkeypatch.setattr(
        "moneybin.mcp.confirmation.supports_elicitation",
        MagicMock(return_value=False),
    )
    bridge_response = {"recipe": {"version": 1}, "rows": [{}]}
    required = await import_confirm_coarse(
        preview_id=preview_id,
        bridge_response=bridge_response,
    )
    assert required.error is not None
    assert required.error.details is not None

    retried = await import_confirm_coarse(
        preview_id=preview_id,
        bridge_response=bridge_response,
        confirmation_token=str(required.error.details["confirmation_token"]),
    )

    assert retried.error is not None
    assert retried.error.code == error_codes.MUTATION_CONFIRMATION_MISMATCH
    assert [call.kwargs["confirm"] for call in apply.call_args_list] == [False, False]


async def test_import_confirm_bridge_token_is_bound_to_exact_response(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    from moneybin import error_codes

    pdf = tmp_path / "statement.pdf"
    pdf.write_bytes(b"%PDF bridge fixture")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    preview_id = _issue_coarse_preview(
        pdf,
        channel="pdf",
        data={
            "status": "confirmation_required",
            "channel": "pdf",
            "reason": "low_confidence",
            "bridge_payload": {},
        },
    )
    proposal = _coarse_sign_error("pdf")
    apply = MagicMock(side_effect=[proposal, proposal, proposal])
    monkeypatch.setattr(
        "moneybin.services.import_service.ImportService.apply_pdf_bridge_response",
        apply,
    )
    monkeypatch.setattr(
        "moneybin.mcp.confirmation._active_context",
        MagicMock(return_value=None),
    )
    monkeypatch.setattr(
        "moneybin.mcp.confirmation.supports_elicitation",
        MagicMock(return_value=False),
    )
    original_response = {"recipe": {"version": 1}, "rows": [{}]}
    required = await import_confirm_coarse(
        preview_id=preview_id,
        bridge_response=original_response,
    )
    assert required.error is not None
    assert required.error.details is not None

    changed = await import_confirm_coarse(
        preview_id=preview_id,
        bridge_response={"recipe": {"version": 2}, "rows": [{}]},
        confirmation_token=str(required.error.details["confirmation_token"]),
    )

    assert changed.error is not None
    assert changed.error.code == error_codes.MUTATION_CONFIRMATION_MISMATCH
    assert all(call.kwargs["confirm"] is False for call in apply.call_args_list)
    from moneybin.database import get_database

    with get_database(read_only=True) as db:
        import_log_count = db.execute("SELECT COUNT(*) FROM raw.import_log").fetchone()
        assert import_log_count is not None and import_log_count[0] == 0


async def test_import_confirm_coarse_preserves_sign_warning(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    from moneybin.services.import_service import ImportResult

    csv = tmp_path / "statement.csv"
    csv.write_text("Date,Description,Amount\n2026-07-01,Coffee,-4.50\n")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    preview = await import_preview_coarse(file_path=str(csv))
    monkeypatch.setattr(
        "moneybin.services.import_service.ImportService.import_file",
        MagicMock(
            return_value=ImportResult(
                file_path=str(csv),
                file_type="tabular",
                transactions=1,
                import_id="imp_sign_warning",
                sign_correction_suggested=True,
            )
        ),
    )

    response = await import_confirm_coarse(
        preview_id=preview.data.preview_id,
        account_name="Checking",
    )

    assert any(
        "Sign convention may be inverted" in action for action in response.actions
    )


def test_import_workflow_registrar_uses_public_privacy_actor_names() -> None:
    registered: list[tuple[str, str | None]] = []

    def capture(
        _mcp: object,
        _callback: object,
        name: str,
        _description: str,
        *,
        privacy_actor: str | None = None,
        **_kwargs: object,
    ) -> None:
        registered.append((name, privacy_actor))

    with patch.object(import_tools_module, "register", capture):
        import_tools_module.register_import_workflow_tools(MagicMock())

    assert registered
    assert registered == [(name, name) for name, _ in registered]


async def test_import_confirm_coarse_applies_pdf_bridge_by_preview_id(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pdf = tmp_path / "statement.pdf"
    pdf.write_bytes(b"%PDF bridge fixture")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    preview_id = _issue_coarse_preview(
        pdf,
        channel="pdf",
        data={
            "status": "confirmation_required",
            "channel": "pdf",
            "reason": "low_confidence",
            "bridge_payload": {"layout_fingerprint": {"issuer": "Example"}},
        },
    )
    applied = SimpleNamespace(
        outcome="applied",
        import_id="imp_pdf",
        rows_loaded=2,
        format_name="example_pdf",
        expected_row_count=2,
        actual_row_count=2,
        rows_diverged=False,
        reject_reason=None,
    )
    apply = MagicMock(return_value=applied)
    monkeypatch.setattr(
        "moneybin.services.import_service.ImportService.apply_pdf_bridge_response",
        apply,
    )

    response = await import_confirm_coarse(
        preview_id=preview_id,
        bridge_response={"recipe": {"version": 1}, "rows": [{}, {}]},
    )

    assert response.data.import_id == "imp_pdf"
    apply.assert_called_once()
    assert apply.call_args.kwargs["in_outer_txn"] is True
    from moneybin.database import get_database
    from moneybin.repositories.import_previews_repo import ImportPreviewsRepo

    with get_database(read_only=True) as db:
        assert ImportPreviewsRepo(db).get(preview_id)["import_id"] == "imp_pdf"  # type: ignore[index]


async def test_import_confirm_coarse_keeps_pdf_preview_live_when_bridge_is_invalid(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pdf = tmp_path / "statement.pdf"
    pdf.write_bytes(b"%PDF bridge fixture")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    preview_id = _issue_coarse_preview(
        pdf,
        channel="pdf",
        data={
            "status": "confirmation_required",
            "channel": "pdf",
            "reason": "low_confidence",
            "bridge_payload": {},
        },
    )
    invalid = SimpleNamespace(
        outcome="invalid",
        import_id=None,
        rows_loaded=0,
        format_name=None,
        expected_row_count=1,
        actual_row_count=0,
        rows_diverged=True,
        reject_reason="reconciliation_failed",
    )
    monkeypatch.setattr(
        "moneybin.services.import_service.ImportService.apply_pdf_bridge_response",
        MagicMock(return_value=invalid),
    )

    response = await import_confirm_coarse(
        preview_id=preview_id,
        bridge_response={"recipe": {"version": 1}, "rows": [{}]},
    )

    assert response.data.status == "invalid"
    from moneybin.database import get_database
    from moneybin.repositories.import_previews_repo import ImportPreviewsRepo

    with get_database(read_only=True) as db:
        assert ImportPreviewsRepo(db).get(preview_id)["consumed_at"] is None  # type: ignore[index]


async def test_import_confirm_invalid_bridge_flushes_observation_once_after_rollback(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pdf = tmp_path / "statement.pdf"
    pdf.write_bytes(b"%PDF bridge fixture")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    preview_id = _issue_coarse_preview(
        pdf,
        channel="pdf",
        data={
            "status": "confirmation_required",
            "channel": "pdf",
            "reason": "low_confidence",
            "bridge_payload": {},
        },
    )
    metric = MagicMock()

    def invalid_with_observation(
        _service: object,
        _path: Path,
        _response: dict[str, object],
        **kwargs: object,
    ) -> object:
        observations = kwargs["observations"]
        assert isinstance(observations, MetricObservations)
        observations.counter(
            metric,
            labels={"outcome": "invalid"},
            disposition="rollback",
        )
        return SimpleNamespace(
            outcome="invalid",
            import_id=None,
            rows_loaded=0,
            format_name=None,
            expected_row_count=1,
            actual_row_count=0,
            rows_diverged=True,
            reject_reason="reconciliation_failed",
        )

    monkeypatch.setattr(
        "moneybin.services.import_service.ImportService.apply_pdf_bridge_response",
        invalid_with_observation,
    )

    response = await import_confirm_coarse(
        preview_id=preview_id,
        bridge_response={"recipe": {"version": 1}, "rows": [{}]},
    )

    assert response.data.status == "invalid"
    metric.labels.assert_called_once_with(outcome="invalid")
    metric.labels.return_value.inc.assert_called_once_with()


async def test_import_confirm_late_failure_discards_success_observations(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    csv = tmp_path / "statement.csv"
    csv.write_text("Date,Description,Amount\n2026-07-01,Reviewed Coffee,-4.50\n")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    preview = await import_preview_coarse(file_path=str(csv))
    metric = MagicMock()

    from moneybin.services.import_service import ImportResult

    def observed_import(
        _service: object,
        path: Path,
        **kwargs: object,
    ) -> ImportResult:
        observations = kwargs["observations"]
        assert isinstance(observations, MetricObservations)
        observations.counter(metric, labels={"outcome": "accepted"})
        return ImportResult(
            file_path=str(path),
            file_type="tabular",
            transactions=1,
            import_id="imp_late_failure",
            field_mapping={
                "transaction_date": "Date",
                "description": "Description",
                "amount": "Amount",
            },
        )

    monkeypatch.setattr(
        "moneybin.services.import_service.ImportService.import_file",
        observed_import,
    )
    monkeypatch.setattr(
        "moneybin.repositories.import_previews_repo.ImportPreviewsRepo.record_result",
        MagicMock(side_effect=RuntimeError("late failure")),
    )

    with pytest.raises(RuntimeError, match="late failure"):
        await import_confirm_coarse(
            preview_id=preview.data.preview_id,
            account_name="Checking",
        )

    metric.labels.assert_not_called()


async def test_import_confirm_success_flushes_observations_exactly_once(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    csv = tmp_path / "statement.csv"
    csv.write_text("Date,Description,Amount\n2026-07-01,Reviewed Coffee,-4.50\n")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    preview = await import_preview_coarse(file_path=str(csv))
    counter = MagicMock()
    histogram = MagicMock()

    from moneybin.services.import_service import ImportResult

    def observed_import(
        _service: object,
        path: Path,
        **kwargs: object,
    ) -> ImportResult:
        observations = kwargs["observations"]
        assert isinstance(observations, MetricObservations)
        observations.counter(counter, labels={"outcome": "accepted"})
        observations.observe(histogram, 0.25, labels={"source_type": "tabular"})
        return ImportResult(
            file_path=str(path),
            file_type="tabular",
            transactions=1,
            import_id="imp_success",
            field_mapping={
                "transaction_date": "Date",
                "description": "Description",
                "amount": "Amount",
            },
        )

    monkeypatch.setattr(
        "moneybin.services.import_service.ImportService.import_file",
        observed_import,
    )

    response = await import_confirm_coarse(
        preview_id=preview.data.preview_id,
        account_name="Checking",
    )

    assert response.data.status == "complete"
    counter.labels.assert_called_once_with(outcome="accepted")
    counter.labels.return_value.inc.assert_called_once_with()
    histogram.labels.assert_called_once_with(source_type="tabular")
    histogram.labels.return_value.observe.assert_called_once_with(0.25)


async def test_import_confirm_malformed_bridge_observes_failure_after_rollback(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pdf = tmp_path / "statement.pdf"
    pdf.write_bytes(b"%PDF malformed bridge fixture")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    preview_id = _issue_coarse_preview(
        pdf,
        channel="pdf",
        data={
            "status": "confirmation_required",
            "channel": "pdf",
            "reason": "low_confidence",
            "bridge_payload": {},
        },
    )
    events: list[str] = []
    failure = _event_counter(events, "failure")
    success = MagicMock()

    from moneybin.metrics import registry

    _track_completed_rollbacks(monkeypatch, events)
    monkeypatch.setattr(registry, "PDF_BRIDGE_EGRESS_TOTAL", failure)
    monkeypatch.setattr(registry, "IMPORT_RECORDS_TOTAL", success)

    response = await import_confirm_coarse(
        preview_id=preview_id,
        bridge_response={"rows": []},
    )

    assert response.error is not None
    assert response.error.code == "infra_invalid_input"
    assert events == ["rollback", "failure"]
    failure.labels.assert_called_once_with(outcome="invalid")
    success.labels.assert_not_called()


async def test_import_confirm_bridge_extraction_failure_observed_after_rollback(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    from moneybin.extractors.pdf.ir import PdfDocument

    pdf = tmp_path / "statement.pdf"
    pdf.write_bytes(b"%PDF extraction failure fixture")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    preview_id = _issue_coarse_preview(
        pdf,
        channel="pdf",
        data={
            "status": "confirmation_required",
            "channel": "pdf",
            "reason": "low_confidence",
            "bridge_payload": {},
        },
    )
    events: list[str] = []
    failure = _event_counter(events, "failure")
    success = MagicMock()

    from moneybin.metrics import registry

    class FailingExtractor:
        def extract(
            self,
            _path: Path,
            *,
            source_bytes: bytes | None = None,
        ) -> PdfDocument:
            del source_bytes
            raise ValueError("could not extract text from PDF")

    _track_completed_rollbacks(monkeypatch, events)
    monkeypatch.setattr(
        "moneybin.extractors.pdf.extractor.PDFExtractor",
        FailingExtractor,
    )
    monkeypatch.setattr(registry, "PDF_IMPORT_TOTAL", failure)
    monkeypatch.setattr(registry, "IMPORT_RECORDS_TOTAL", success)

    response = await import_confirm_coarse(
        preview_id=preview_id,
        bridge_response=_valid_bridge_response(),
    )

    assert response.error is not None
    assert response.error.code == "infra_invalid_input"
    assert events == ["rollback", "failure"]
    failure.labels.assert_called_once_with(outcome="failed", rung="bridge")
    success.labels.assert_not_called()


async def test_import_confirm_tabular_transform_failures_observed_after_rollback(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    csv = tmp_path / "statement.csv"
    csv.write_text("Date,Description,Amount\n2026-07-01,Coffee,-4.50\n")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    preview = await import_preview_coarse(file_path=str(csv))
    events: list[str] = []
    batch_failure = _event_counter(events, "batch_failure")
    transform_failure = _event_counter(events, "transform_failure")
    success = MagicMock()

    from moneybin.metrics import registry

    def fail_transform(**_kwargs: object) -> object:
        raise ValueError("bad transform")

    _track_completed_rollbacks(monkeypatch, events)
    monkeypatch.setattr(
        "moneybin.extractors.tabular.transforms.transform_dataframe",
        fail_transform,
    )
    monkeypatch.setattr(
        "moneybin.extractors.tabular.extractor.TABULAR_IMPORT_BATCHES",
        batch_failure,
    )
    monkeypatch.setattr(
        "moneybin.services.import_service.IMPORT_ERRORS_TOTAL",
        transform_failure,
    )
    monkeypatch.setattr(registry, "IMPORT_RECORDS_TOTAL", success)

    response = await import_confirm_coarse(
        preview_id=preview.data.preview_id,
        account_name="Checking",
    )

    assert response.error is not None
    assert response.error.code == "infra_invalid_input"
    assert events == ["rollback", "batch_failure", "transform_failure"]
    batch_failure.labels.assert_called_once_with(status="failed")
    transform_failure.labels.assert_called_once_with(
        source_type="csv",
        error_type="transform",
    )
    success.labels.assert_not_called()


async def test_import_confirm_coarse_rejects_pdf_without_confirmation_gate(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pdf = tmp_path / "statement.pdf"
    pdf.write_bytes(b"%PDF fixture")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    preview_id = _issue_coarse_preview(
        pdf,
        channel="pdf",
        data={
            "status": "preview",
            "channel": "pdf",
            "deterministic": True,
        },
    )

    response = await import_confirm_coarse(preview_id=preview_id)

    assert response.error is not None
    assert response.error.code == "IMPORT_PREVIEW_DIRECT_IMPORT_REQUIRED"


async def test_import_preview_coarse_keeps_ofx_on_direct_import_surface(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    ofx = tmp_path / "statement.ofx"
    ofx.write_text("OFXHEADER:100\n<OFX>")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)

    response = await import_preview_coarse(file_path=str(ofx))

    assert response.error is not None
    assert response.error.code == "IMPORT_PREVIEW_DIRECT_IMPORT_REQUIRED"


async def test_import_preview_confirm_status_coarse_workflow(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    csv = tmp_path / "statement.csv"
    csv.write_text(
        "Date,Description,Amount\n2026-07-01,Coffee,-4.50\n2026-07-02,Deposit,100.00\n"
    )
    monkeypatch.setattr(Path, "home", lambda: tmp_path)

    preview = await import_preview_coarse(file_path=str(csv))
    assert any(
        f"preview_id='{preview.data.preview_id}'" in action
        for action in preview.actions
    )
    confirmed = await import_confirm_coarse(
        preview_id=preview.data.preview_id,
        account_name="Checking",
    )
    status = await import_status_coarse(
        sections=["imports"],
        import_id=confirmed.data.import_id,
    )

    assert confirmed.data.status == "complete"
    assert status.data.sections[0].records[0]["status"] == "complete"


async def test_import_status_coarse_defaults_to_all_sections(
    mcp_db: object,
) -> None:
    response = await import_status_coarse()

    assert [section.kind for section in response.data.sections] == [
        "imports",
        "formats",
        "inbox",
    ]


async def test_import_status_coarse_normalizes_selected_section_order(
    mcp_db: object,
) -> None:
    response = await import_status_coarse(
        sections=["inbox", "imports", "formats"],
    )

    assert [section.kind for section in response.data.sections] == [
        "imports",
        "formats",
        "inbox",
    ]


async def test_import_status_coarse_preserves_legacy_section_data(
    mcp_db: object,
) -> None:
    legacy_imports = import_status(limit=100)
    legacy_formats = import_formats()
    from moneybin.mcp.tools.import_inbox import import_inbox_pending

    legacy_inbox = import_inbox_pending()

    response = await import_status_coarse()
    imports, formats, inbox = response.data.sections

    assert imports.records == legacy_imports.data.records
    assert formats.formats == legacy_formats.data.formats
    assert formats.pdf_formats == legacy_formats.data.pdf_formats
    assert inbox.would_process == legacy_inbox.data.would_process
    assert inbox.ignored == legacy_inbox.data.ignored


@pytest.mark.parametrize(
    ("sections", "import_id", "limit", "cursor", "code"),
    [
        ([], None, 100, None, "IMPORT_SECTIONS_REQUIRED"),
        (
            ["imports", "imports"],
            None,
            100,
            None,
            "IMPORT_SECTIONS_DUPLICATE",
        ),
        (
            ["imports", "formats"],
            "imp_1",
            100,
            None,
            "IMPORT_ID_NOT_ALLOWED",
        ),
        (None, "imp_1", 100, None, "IMPORT_ID_NOT_ALLOWED"),
        (
            ["formats"],
            None,
            10,
            None,
            "IMPORT_PAGINATION_NOT_ALLOWED",
        ),
        (
            ["inbox"],
            None,
            100,
            "opaque",
            "IMPORT_PAGINATION_NOT_ALLOWED",
        ),
    ],
)
async def test_import_status_coarse_rejects_incompatible_arguments(
    sections: list[str] | None,
    import_id: str | None,
    limit: int,
    cursor: str | None,
    code: str,
) -> None:
    response = await import_status_coarse(  # type: ignore[arg-type]
        sections=sections,
        import_id=import_id,
        limit=limit,
        cursor=cursor,
    )

    assert response.error is not None
    assert response.error.code == code


async def test_import_status_coarse_paginates_exactly_with_total_order(
    mcp_db: object,
) -> None:
    from moneybin.database import get_database

    with get_database(read_only=False) as db:
        for import_id in ("imp_a", "imp_b", "imp_c"):
            db.execute(
                """
                INSERT INTO raw.import_log (
                    import_id, source_file, source_type, source_origin,
                    account_names, status, started_at
                ) VALUES (?, ?, 'csv', 'test', '[]', 'complete', ?)
                """,
                [
                    import_id,
                    f"/home/test/imports/{import_id}.csv",
                    "2099-01-01 00:00:00",
                ],
            )

    first = await import_status_coarse(sections=["imports"], limit=2)
    first_section = first.data.sections[0]
    assert [row["import_id"] for row in first_section.records] == [
        "imp_c",
        "imp_b",
    ]
    assert first.summary.total_count == 3
    assert first.summary.returned_count == 2
    assert first.summary.has_more is True
    assert first.next_cursor is not None
    assert any(
        "sections=['imports']" in action
        and "limit=2" in action
        and first.next_cursor in action
        for action in first.actions
    )

    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO raw.import_log (
                import_id, source_file, source_type, source_origin,
                account_names, status, started_at
            ) VALUES (
                'imp_new', '/tmp/new.csv', 'csv', 'test', '[]', 'complete',
                '2100-01-01 00:00:00'
            )
            """
        )

    second = await import_status_coarse(
        sections=["imports"],
        limit=2,
        cursor=first.next_cursor,
    )
    assert [row["import_id"] for row in second.data.sections[0].records] == ["imp_a"]
    assert second.summary.total_count == 3
    assert second.summary.returned_count == 1
    assert second.summary.has_more is False

    wrong_filter = await import_status_coarse(
        sections=["imports", "formats"],
        limit=2,
        cursor=first.next_cursor,
    )
    assert wrong_filter.error is not None
    assert wrong_filter.error.code == "IMPORT_CURSOR_INVALID"


async def test_import_status_mixed_cursor_carries_full_initial_total(
    mcp_db: object,
) -> None:
    from moneybin.database import get_database
    from moneybin.mcp.pagination import decode_keyset_cursor

    with get_database(read_only=False) as db:
        for import_id in ("imp_a", "imp_b"):
            db.execute(
                """
                INSERT INTO raw.import_log (
                    import_id, source_file, source_type, source_origin,
                    account_names, status, started_at
                ) VALUES (
                    ?, ?, 'csv', 'test', '[]', 'complete',
                    '2099-01-01 00:00:00'
                )
                """,
                [import_id, f"/home/test/imports/{import_id}.csv"],
            )

    first = await import_status_coarse(
        sections=["imports", "formats"],
        limit=1,
    )
    assert first.next_cursor is not None
    position = decode_keyset_cursor(
        first.next_cursor,
        namespace="import_status.imports",
        scope={"import_id": None, "sections": ["imports", "formats"]},
    )

    assert first.summary.total_count > 2
    assert position.total == first.summary.total_count
    assert position.snapshot[2] == 2
    assert position.after[2] == 2


async def test_import_status_coarse_allows_multiple_canonical_prepends(
    mcp_db: object,
) -> None:
    from moneybin.database import get_database

    with get_database(read_only=False) as db:
        for import_id, started_at in (
            ("imp_a", "2099-01-01 00:00:00"),
            ("imp_b", "2099-02-01 00:00:00"),
            ("imp_c", "2099-03-01 00:00:00"),
        ):
            db.execute(
                """
                INSERT INTO raw.import_log (
                    import_id, source_file, source_type, source_origin,
                    account_names, status, started_at
                ) VALUES (?, ?, 'csv', 'test', '[]', 'complete', ?)
                """,
                [import_id, f"/home/test/imports/{import_id}.csv", started_at],
            )

    first = await import_status_coarse(sections=["imports"], limit=1)
    assert [row["import_id"] for row in first.data.sections[0].records] == ["imp_c"]
    assert first.next_cursor is not None

    with get_database(read_only=False) as db:
        for import_id, started_at in (
            ("imp_new_1", "2100-01-01 00:00:00"),
            ("imp_new_2", "2100-02-01 00:00:00"),
        ):
            db.execute(
                """
                INSERT INTO raw.import_log (
                    import_id, source_file, source_type, source_origin,
                    account_names, status, started_at
                ) VALUES (?, ?, 'csv', 'test', '[]', 'complete', ?)
                """,
                [import_id, f"/home/test/imports/{import_id}.csv", started_at],
            )

    second = await import_status_coarse(
        sections=["imports"],
        limit=1,
        cursor=first.next_cursor,
    )
    assert [row["import_id"] for row in second.data.sections[0].records] == ["imp_b"]
    assert second.next_cursor is not None

    third = await import_status_coarse(
        sections=["imports"],
        limit=1,
        cursor=second.next_cursor,
    )
    assert [row["import_id"] for row in third.data.sections[0].records] == ["imp_a"]
    assert third.next_cursor is None


async def test_import_status_coarse_allows_prepends_tied_with_snapshot_head(
    mcp_db: object,
) -> None:
    from moneybin.database import get_database

    with get_database(read_only=False) as db:
        for import_id in ("imp_a", "imp_b", "imp_c"):
            db.execute(
                """
                INSERT INTO raw.import_log (
                    import_id, source_file, source_type, source_origin,
                    account_names, status, started_at
                ) VALUES (?, ?, 'csv', 'test', '[]', 'complete', ?)
                """,
                [
                    import_id,
                    f"/home/test/imports/{import_id}.csv",
                    "2099-01-01 00:00:00",
                ],
            )

    first = await import_status_coarse(sections=["imports"], limit=1)
    assert [row["import_id"] for row in first.data.sections[0].records] == ["imp_c"]
    assert first.next_cursor is not None

    with get_database(read_only=False) as db:
        for import_id in ("imp_y", "imp_z"):
            db.execute(
                """
                INSERT INTO raw.import_log (
                    import_id, source_file, source_type, source_origin,
                    account_names, status, started_at
                ) VALUES (?, ?, 'csv', 'test', '[]', 'complete', ?)
                """,
                [
                    import_id,
                    f"/home/test/imports/{import_id}.csv",
                    "2099-01-01 00:00:00",
                ],
            )

    second = await import_status_coarse(
        sections=["imports"],
        limit=2,
        cursor=first.next_cursor,
    )
    assert [row["import_id"] for row in second.data.sections[0].records] == [
        "imp_b",
        "imp_a",
    ]
    assert second.next_cursor is None


async def test_import_status_coarse_survives_removal_and_prepend_without_skipping(
    mcp_db: object,
) -> None:
    from moneybin.database import get_database

    with get_database(read_only=False) as db:
        for import_id, started_at in (
            ("imp_a", "2099-01-01 00:00:00"),
            ("imp_b", "2099-02-01 00:00:00"),
            ("imp_c", "2099-03-01 00:00:00"),
        ):
            db.execute(
                """
                INSERT INTO raw.import_log (
                    import_id, source_file, source_type, source_origin,
                    account_names, status, started_at
                ) VALUES (?, ?, 'csv', 'test', '[]', 'complete', ?)
                """,
                [import_id, f"/home/test/imports/{import_id}.csv", started_at],
            )

    first = await import_status_coarse(sections=["imports"], limit=1)
    assert first.next_cursor is not None

    with get_database(read_only=False) as db:
        db.execute("DELETE FROM raw.import_log WHERE import_id = 'imp_c'")
        db.execute(
            """
            INSERT INTO raw.import_log (
                import_id, source_file, source_type, source_origin,
                account_names, status, started_at
            ) VALUES (
                'imp_new', '/tmp/imp_new.csv', 'csv', 'test', '[]', 'complete',
                '2100-01-01 00:00:00'
            )
            """
        )

    response = await import_status_coarse(
        sections=["imports"],
        limit=1,
        cursor=first.next_cursor,
    )
    assert [row["import_id"] for row in response.data.sections[0].records] == ["imp_b"]
    assert response.summary.total_count == 3
    assert response.next_cursor is not None


async def test_import_status_coarse_survives_unserved_row_removal_without_duplication(
    mcp_db: object,
) -> None:
    from moneybin.database import get_database

    with get_database(read_only=False) as db:
        for import_id in ("imp_a", "imp_b", "imp_c"):
            db.execute(
                """
                INSERT INTO raw.import_log (
                    import_id, source_file, source_type, source_origin,
                    account_names, status, started_at
                ) VALUES (?, ?, 'csv', 'test', '[]', 'complete', ?)
                """,
                [
                    import_id,
                    f"/home/test/imports/{import_id}.csv",
                    "2099-01-01 00:00:00",
                ],
            )

    first = await import_status_coarse(sections=["imports"], limit=1)
    assert first.next_cursor is not None

    with get_database(read_only=False) as db:
        db.execute("DELETE FROM raw.import_log WHERE import_id = 'imp_b'")

    response = await import_status_coarse(
        sections=["imports"],
        limit=2,
        cursor=first.next_cursor,
    )
    assert [row["import_id"] for row in response.data.sections[0].records] == ["imp_a"]
    assert response.summary.total_count == 3
    assert response.next_cursor is None


async def test_import_status_coarse_rejects_invalid_key_types_before_data_access(
    mcp_db: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.mcp.pagination import encode_keyset_cursor

    invalid_cursor = encode_keyset_cursor(
        namespace="import_status.imports",
        scope={"import_id": None, "sections": ["imports"]},
        snapshot=(123, "imp_b"),
        after=(123, "imp_b"),
        total=2,
    )
    accessed = False

    def fail_if_accessed(*args: object, **kwargs: object) -> object:
        nonlocal accessed
        accessed = True
        raise AssertionError("data access must follow cursor validation")

    monkeypatch.setattr(
        "moneybin.loaders.import_log.get_import_history_page",
        fail_if_accessed,
    )

    response = await import_status_coarse(
        sections=["imports"], limit=1, cursor=invalid_cursor
    )
    assert response.error is not None
    assert response.error.code == "IMPORT_CURSOR_INVALID"
    assert accessed is False


async def test_import_status_coarse_import_id_returns_one_exact_record(
    mcp_db: object,
) -> None:
    from moneybin.database import get_database

    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO raw.import_log (
                import_id, source_file, source_type, source_origin,
                account_names, status
            ) VALUES ('imp_exact', '/tmp/exact.csv', 'csv', 'test', '[]', 'complete')
            """
        )

    response = await import_status_coarse(
        sections=["imports"],
        import_id="imp_exact",
    )

    assert response.summary.total_count == 1
    assert response.summary.returned_count == 1
    assert response.data.sections[0].records[0]["import_id"] == "imp_exact"


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
            result = import_files(paths=[str(csv_file)])

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
        assert payload.get("channel") == "tabular"
        assert payload.get("tier") == "medium"
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
            result = import_files(paths=[str(csv_file)])

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
            result = import_files(paths=[str(csv_file)])

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
            result = import_files(paths=[str(csv_file)])

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
            import_files(paths=[str(csv_file)])

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
            result = import_files(paths=[str(pdf)])

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

    async def test_requires_accept_or_mapping(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """Calling with neither accept=True nor mapping returns an error envelope."""
        csv_file = tmp_path / "statements" / "test.csv"
        csv_file.parent.mkdir(parents=True)
        csv_file.touch()

        monkeypatch.setattr(Path, "home", lambda: tmp_path)

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
            "document_text": (
                "Chase Bank\nDate Description Amount\n05/01 COFFEE SHOP -12.34"
            ),
            "tables_preview": [
                {
                    "page": 1,
                    "header": ["Date", "Description", "Amount"],
                    "rows": [["05/01", "COFFEE SHOP", "-12.34"]],
                }
            ],
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
            result = import_files(paths=[str(pdf)])

        from moneybin.privacy.payloads.imports import ImportFilesPayload

        data = result.data
        assert isinstance(data, ImportFilesPayload)
        row = data.files[0]
        assert row.status == "confirmation_required"
        payload = row.confirmation_payload
        assert payload is not None
        assert payload.get("channel") == "pdf"
        bridge = payload.get("bridge_payload")
        assert isinstance(bridge, dict)
        assert bridge.get("request_kind") == "propose_recipe"
        assert "transparency_notice" in bridge
        assert bridge.get("document_text") == (
            "Chase Bank\nDate Description Amount\n05/01 COFFEE SHOP -12.34"
        )
        assert bridge.get("tables_preview") == [
            {
                "page": 1,
                "header": ["Date", "Description", "Amount"],
                "rows": [["05/01", "COFFEE SHOP", "-12.34"]],
            }
        ]

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
            result = import_files(paths=[str(pdf)])

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
            result = import_files(paths=[str(pdf)])

        from moneybin.privacy.payloads.imports import ImportFilesPayload

        data = result.data
        assert isinstance(data, ImportFilesPayload)
        row = data.files[0]
        assert row.status == "confirmation_required"
        payload = row.confirmation_payload
        assert payload is not None
        assert payload.get("reason") == "sign_convention"
        assert payload.get("sign_convention") == "negative_is_income"

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

        result = import_preview(file_path=str(csv_file))

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

        result = import_preview(file_path=str(csv_file))

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
            result = import_preview(file_path=str(pdf))

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
            result = import_preview(file_path=str(pdf))

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
            result = import_preview(file_path=str(pdf))

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

    def _un_inverting_sign_error(self) -> ImportConfirmationRequiredError:
        """A self-heal repair proposing the OPPOSITE of the card inference.

        Mirrors what `_attempt_self_heal` hands the gate when a previously
        ratified `negative_is_income` recipe re-derives as `negative_is_expense`
        (exercised end-to-end by `test_a_repair_that_un_inverts_the_ledger_is_gated`).
        """
        from moneybin.services.import_confirmation import SignConventionProposal

        return ImportConfirmationRequiredError(
            ConfirmationRequired(
                channel="pdf",
                confidence=_make_confidence(score=1.0, tier="high"),
                proposed=SignConventionProposal(
                    sign_convention="negative_is_expense",
                    prior_sign_convention="negative_is_income",
                    evidence=("Minimum Payment Due",),
                    sample_rows=[{"printed": "39.83", "recorded": "39.83"}],
                ),
                reason="sign_convention",
                error_message="The saved layout was re-derived.",
            )
        )

    async def test_the_elicitation_prompt_describes_the_flip_that_will_apply(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """The prompt a human approves must match what approving does.

        This is the last thing shown before a ledger-wide sign change is
        applied, so a wrong direction here is the most expensive place for one:
        the human either approves a flip they were told was its opposite, or
        rejects a correct repair. The card wording is right for a first-contact
        inference and backwards for a repair that *un*-inverts.
        """
        from moneybin.services.import_service import ImportResult

        pdf = self._card_pdf(tmp_path, monkeypatch)
        mock_service = MagicMock()
        mock_service.pdf_preview.side_effect = self._un_inverting_sign_error()
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
        # Approving here accepts the as-printed convention, so the prompt must
        # not tell the human it identifies a credit card and reverses amounts.
        assert "credit card" not in message.lower()
        assert "charges become negative expenses" not in message
        # Both conventions must be named, so the direction is unambiguous.
        assert "negative_is_income" in message
        assert "negative_is_expense" in message

    async def test_the_elicitation_prompt_keeps_card_framing_on_first_contact(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """The common case must not regress into convention jargon.

        With no prior convention the proposal is always `negative_is_income`,
        and "is this a credit card?" is the question the human can actually
        answer.
        """
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
        assert "credit card" in confirm.await_args.args[0].lower()

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
