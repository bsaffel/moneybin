"""Tests for import-tool helpers, including the file-path security boundary."""

from __future__ import annotations

import base64
import json
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from pytest import MonkeyPatch

import moneybin.mcp.tools.import_tools as import_tools_module
from moneybin.errors import UserError
from moneybin.extractors.confidence import Confidence
from moneybin.mcp.tools.import_tools import (
    _bridge_confirm_action,  # pyright: ignore[reportPrivateUsage]
    _validate_file_path,  # pyright: ignore[reportPrivateUsage]
    import_confirm,
    import_confirm_coarse,
    import_files,
    import_formats,
    import_preview,
    import_preview_coarse,
    import_status,
    import_status_coarse,
)
from moneybin.services.import_confirmation import (
    BridgePayload,
    ConfirmationRequired,
    ImportConfirmationRequiredError,
    ProposedMapping,
)
from tests.moneybin.pdf_statement_fixtures import write_card_statement_pdf
from tests.moneybin.test_mcp.schema_assertions import isolated_server


async def test_import_workflow_registrar_preserves_seven_trust_boundaries() -> None:
    registrar = import_tools_module.register_import_workflow_tools
    mcp = isolated_server(registrar)

    names = {tool.name for tool in await mcp._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]

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
            snapshot={"data": data, "actions": [], "sensitivity": "medium"},
            issued_at=now,
            expires_at=now + timedelta(minutes=5),
            actor="mcp",
        )


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

    assert response.data["import_id"] == "imp_pdf"
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

    assert response.data["status"] == "invalid"
    from moneybin.database import get_database
    from moneybin.repositories.import_previews_repo import ImportPreviewsRepo

    with get_database(read_only=True) as db:
        assert ImportPreviewsRepo(db).get(preview_id)["consumed_at"] is None  # type: ignore[index]


@pytest.mark.parametrize(
    ("preview_data", "code"),
    [
        (
            {
                "status": "preview",
                "channel": "pdf",
                "deterministic": True,
            },
            "IMPORT_PREVIEW_DIRECT_IMPORT_REQUIRED",
        ),
        (
            {
                "status": "confirmation_required",
                "channel": "pdf",
                "reason": "sign_convention",
            },
            "IMPORT_PREVIEW_SIGN_CONFIRMATION_CLI_REQUIRED",
        ),
    ],
)
async def test_import_confirm_coarse_preserves_non_bridge_pdf_divisions(
    mcp_db: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    preview_data: dict[str, object],
    code: str,
) -> None:
    pdf = tmp_path / "statement.pdf"
    pdf.write_bytes(b"%PDF fixture")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    preview_id = _issue_coarse_preview(pdf, channel="pdf", data=preview_data)

    response = await import_confirm_coarse(preview_id=preview_id)

    assert response.error is not None
    assert response.error.code == code


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
    confirmed = await import_confirm_coarse(
        preview_id=preview.data["preview_id"],
        account_name="Checking",
    )
    status = await import_status_coarse(
        sections=["imports"],
        import_id=confirmed.data["import_id"],
    )

    assert confirmed.data["status"] == "complete"
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
    legacy_imports = await import_status(limit=100)
    legacy_formats = await import_formats()
    from moneybin.mcp.tools.import_inbox import import_inbox_pending

    legacy_inbox = await import_inbox_pending()

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


async def test_import_status_coarse_rejects_delete_plus_prepend(
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
        db.execute("DELETE FROM raw.import_log WHERE import_id = 'imp_a'")
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
    assert response.error is not None
    assert response.error.code == "IMPORT_CURSOR_INVALID"


async def test_import_status_coarse_rejects_snapshot_anchor_deletion(
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
        db.execute("DELETE FROM raw.import_log WHERE import_id = 'imp_c'")
        db.execute(
            """
            INSERT INTO raw.import_log (
                import_id, source_file, source_type, source_origin,
                account_names, status, started_at
            ) VALUES (
                'imp_z', '/tmp/imp_z.csv', 'csv', 'test', '[]', 'complete',
                '2099-01-01 00:00:00'
            )
            """
        )

    response = await import_status_coarse(
        sections=["imports"],
        limit=1,
        cursor=first.next_cursor,
    )
    assert response.error is not None
    assert response.error.code == "IMPORT_CURSOR_INVALID"


async def test_import_status_coarse_rejects_original_order_mutation(
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
        db.execute(
            """
            UPDATE raw.import_log
            SET started_at = '2098-01-01 00:00:00'
            WHERE import_id = 'imp_b'
            """
        )

    response = await import_status_coarse(
        sections=["imports"],
        limit=1,
        cursor=first.next_cursor,
    )
    assert response.error is not None
    assert response.error.code == "IMPORT_CURSOR_INVALID"


async def test_import_status_coarse_rejects_invalid_snapshot_head(
    mcp_db: object,
) -> None:
    from moneybin.database import get_database

    with get_database(read_only=False) as db:
        for import_id in ("imp_a", "imp_b"):
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
    decoded = json.loads(base64.urlsafe_b64decode(first.next_cursor))
    decoded["snapshot"]["head"][0] = "not-a-timestamp"
    invalid_cursor = base64.urlsafe_b64encode(
        json.dumps(decoded, sort_keys=True, separators=(",", ":")).encode()
    ).decode()

    response = await import_status_coarse(
        sections=["imports"],
        limit=1,
        cursor=invalid_cursor,
    )
    assert response.error is not None
    assert response.error.code == "IMPORT_CURSOR_INVALID"


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
        # Corrected recovery: the terminal CLI command, both branches named.
        assert "moneybin import files" in actions
        assert "--confirm" in actions
        assert "--sign negative_is_expense" in actions
        # The broken hints must be gone — and a sign confirmation is NOT a bridge,
        # so the bridge_response hint must not leak in either.
        assert "import_confirm(" not in actions
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

        # The agent is told both branches — and told to resolve them in a
        # terminal, because MCP cannot ratify a sign inversion in place yet.
        actions = " ".join(result.actions)
        assert "moneybin import files" in actions
        assert "--confirm" in actions
        assert "--sign negative_is_expense" in actions
        # The broken Task 6 hints must be gone: import_confirm has no sign= param
        # and rejects accept= for a .pdf, so advertising either is a dead end.
        assert "import_confirm(" not in actions
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

    async def test_card_sign_confirm_directs_to_cli_and_loads_nothing(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """accept=True on a real card statement refuses with terminal recovery.

        import_confirm cannot ratify a sign inversion in place (elicitation is the
        planned path). A card statement confirmed via accept= must therefore get
        the honest CLI recovery, never crash with a TypeError, and never run the
        import path (no inverted rows land). Uses the committed card fixture so the
        file on disk is genuinely a credit-card statement.
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
        # Honest terminal recovery, both branches named.
        assert "moneybin import files" in message
        assert "--confirm" in message
        assert "--sign negative_is_expense" in message
        # Honest that MCP cannot ratify the inversion in place yet.
        assert "in place" in message
        # Refused before any import ran — nothing loaded, inverted or otherwise.
        mock_service.import_file.assert_not_called()
        mock_service.pdf_preview.assert_not_called()
