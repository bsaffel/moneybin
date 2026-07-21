"""Tests for the shared export orchestration service."""

from __future__ import annotations

from dataclasses import asdict, replace
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from moneybin.database import Database
from moneybin.exports.models import (
    ExportDestination,
    ExportReceipt,
    ExportRequest,
)
from moneybin.exports.service import ExportService
from moneybin.metrics import registry as metrics_registry


def _destination(kind: str) -> ExportDestination:
    if kind == "local":
        return ExportDestination(
            destination_id="local-1",
            name="archive",
            kind="local",
            local_path=Path.cwd() / "moneybin-export-test",
            spreadsheet_id=None,
            managed_tab_prefix=None,
        )
    return ExportDestination(
        destination_id="sheets-1",
        name="dashboard",
        kind="sheets",
        local_path=None,
        spreadsheet_id="spreadsheet-1",
        managed_tab_prefix="MB",
    )


def _request(
    *,
    subject_kind: str = "bundle",
    destination_kind: str = "local",
    format: str | None = None,
    report_id: str | None = None,
    report_parameters: dict[str, Any] | None = None,
    compress_zip: bool = False,
) -> ExportRequest:
    return ExportRequest(
        subject_kind=cast(Any, subject_kind),
        report_id=report_id,
        report_parameters=report_parameters or {},
        destination=_destination(destination_kind),
        format=cast(
            Any,
            format or ("sheets" if destination_kind == "sheets" else "csv"),
        ),
        redaction_mode="redacted",
        compress_zip=compress_zip,
    )


def _receipt(destination: ExportDestination) -> ExportReceipt:
    return ExportReceipt(
        subject={"kind": "bundle"},
        redaction_mode="redacted",
        destination=destination,
        artifact_path=None,
        compressed_artifact_path=None,
        sheets_identity=None,
        row_counts={"accounts": 1},
        checksums={"accounts": "abc"},
        recovery_actions=(),
    )


def _histogram_count(metric: Any) -> float:
    return cast(
        float,
        next(
            sample.value
            for family in metric.collect()
            for sample in family.samples
            if sample.name.endswith("_count")
        ),
    )


def test_export_metrics_are_registered_with_bounded_labels() -> None:
    runs = getattr(metrics_registry, "EXPORT_RUNS_TOTAL", None)
    duration = getattr(metrics_registry, "EXPORT_DURATION_SECONDS", None)

    assert runs is not None
    assert duration is not None
    assert runs._labelnames == (  # type: ignore[reportPrivateUsage]
        "subject_kind",
        "format",
        "destination_kind",
        "redaction_mode",
        "outcome",
    )
    assert duration._labelnames == (  # type: ignore[reportPrivateUsage]
        "subject_kind",
        "format",
        "destination_kind",
        "redaction_mode",
    )


def test_run_records_failed_duration_with_fixed_invalid_label_values(
    db: Database,
) -> None:
    labels = {
        "subject_kind": "bundle",
        "format": "invalid",
        "destination_kind": "local",
        "redaction_mode": "redacted",
    }
    run_metric = metrics_registry.EXPORT_RUNS_TOTAL.labels(
        **labels,
        outcome="failed",
    )
    duration_metric = metrics_registry.EXPORT_DURATION_SECONDS.labels(**labels)
    runs_before = run_metric._value.get()  # type: ignore[reportPrivateUsage]
    duration_count_before = _histogram_count(duration_metric)
    service = ExportService(db)

    with pytest.raises(ValueError):
        service.run(_request(format="user-chosen-private-format"), actor="test")

    assert run_metric._value.get() == runs_before + 1  # type: ignore[reportPrivateUsage]
    duration_count_after = _histogram_count(duration_metric)
    assert duration_count_after == duration_count_before + 1


@patch("moneybin.config.get_settings")
@patch("moneybin.exports.local.LocalExportPublisher")
def test_run_records_success_outcome(
    publisher_type: MagicMock,
    get_settings: MagicMock,
    db: Database,
) -> None:
    labels = {
        "subject_kind": "bundle",
        "format": "csv",
        "destination_kind": "local",
        "redaction_mode": "redacted",
    }
    metric = metrics_registry.EXPORT_RUNS_TOTAL.labels(**labels, outcome="success")
    before = metric._value.get()  # type: ignore[reportPrivateUsage]
    get_settings.return_value.profile = "personal"
    destination = _destination("local")
    publisher_type.return_value.publish.return_value = _receipt(destination)
    service = ExportService(db)

    with patch.object(service, "prepare_bundle", return_value=MagicMock()):
        service.run(_request(), actor="test")

    assert metric._value.get() == before + 1  # type: ignore[reportPrivateUsage]


@patch("moneybin.config.get_settings")
@patch("moneybin.exports.local.LocalExportPublisher")
def test_run_prepares_and_publishes_one_local_bundle(
    publisher_type: MagicMock,
    get_settings: MagicMock,
    db: Database,
) -> None:
    get_settings.return_value.profile = "personal"
    destination = _destination("local")
    publisher = publisher_type.return_value
    publisher.publish.return_value = _receipt(destination)
    service = ExportService(db)
    snapshot = MagicMock()

    with patch.object(service, "prepare_bundle", return_value=snapshot) as prepare:
        receipt = service.run(_request(), actor="cli")

    prepare.assert_called_once_with(profile="personal", redaction_mode="redacted")
    publisher_type.assert_called_once_with(
        destination.local_path,
        destination_name="archive",
    )
    publisher.publish.assert_called_once_with(
        snapshot,
        format="csv",
        compress_zip=False,
    )
    assert receipt.destination == destination
    assert receipt.redaction_mode == "redacted"


@patch("moneybin.config.get_settings")
def test_run_prepares_and_publishes_one_sheets_report(
    get_settings: MagicMock,
    db: Database,
) -> None:
    get_settings.return_value.profile = "personal"
    get_settings.return_value.mcp.max_rows = 321
    destination = _destination("sheets")
    publisher = MagicMock()
    publisher.publish.return_value = _receipt(destination)
    service = ExportService(db, sheets_publisher=publisher)
    snapshot = MagicMock()

    with patch.object(service, "prepare_report", return_value=snapshot) as prepare:
        receipt = service.run(
            _request(
                subject_kind="report",
                destination_kind="sheets",
                report_id="core:spending",
                report_parameters={"months": 3},
            ),
            actor="mcp",
        )

    prepare.assert_called_once_with(
        profile="personal",
        report_id="core:spending",
        report_parameters={"months": 3},
        max_rows=321,
        redaction_mode="redacted",
    )
    publisher.publish.assert_called_once_with(snapshot, destination)
    assert receipt.destination == destination
    assert receipt.redaction_mode == "redacted"


@pytest.mark.parametrize(
    "export_request",
    [
        _request(report_id="core:spending"),
        _request(report_parameters={"months": 3}),
        _request(subject_kind="report"),
        _request(destination_kind="sheets", format="csv"),
        _request(destination_kind="sheets", compress_zip=True),
        _request(destination_kind="local", format="sheets"),
        _request(destination_kind="local", format="xlsx", compress_zip=True),
        replace(
            _request(),
            destination=replace(_destination("local"), name="   "),
        ),
        replace(
            _request(destination_kind="sheets"),
            destination=replace(
                _destination("sheets"),
                managed_tab_prefix="",
            ),
        ),
        replace(
            _request(destination_kind="sheets"),
            destination=replace(
                _destination("sheets"),
                managed_tab_prefix="bad*prefix",
            ),
        ),
    ],
)
@patch("moneybin.config.get_settings")
@patch("moneybin.exports.local.LocalExportPublisher")
def test_run_rejects_impossible_combinations_before_preparing_or_writing(
    publisher_type: MagicMock,
    get_settings: MagicMock,
    export_request: ExportRequest,
    db: Database,
) -> None:
    get_settings.return_value.profile = "personal"
    sheets_publisher = MagicMock()
    service = ExportService(db, sheets_publisher=sheets_publisher)

    with (
        patch.object(service, "prepare_bundle") as prepare_bundle,
        patch.object(service, "prepare_report") as prepare_report,
        pytest.raises(ValueError),
    ):
        service.run(export_request, actor="test")

    prepare_bundle.assert_not_called()
    prepare_report.assert_not_called()
    publisher_type.assert_not_called()
    sheets_publisher.publish.assert_not_called()


def test_status_projects_destination_readiness_without_target_identifiers(
    db: Database,
) -> None:
    from moneybin.connectors.gsheet.testing.fake_oauth_client import TestOAuthClient

    db.execute(
        """
        INSERT INTO app.export_destinations (
            destination_id, name, kind, local_path, spreadsheet_id,
            managed_tab_prefix, created_at, updated_at
        ) VALUES
            ('local-1', 'archive', 'local', '/private/export/path', NULL, NULL,
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            ('sheets-1', 'dashboard', 'sheets', NULL, 'private-sheet-id', 'MB',
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            ('sheets-2', 'broken-dashboard', 'sheets', NULL,
             'other-private-sheet-id', '',
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """
    )
    db.execute(
        """
        INSERT INTO app.gsheet_connections (
            connection_id, spreadsheet_id, sheet_gid, sheet_name, workbook_name,
            adapter, column_mapping, header_signature, sign_convention, skip_rows,
            status
        ) VALUES (
            'inbound-1', 'private-sheet-id', 0, 'Private source', 'Private book',
            'transactions', '{}', '[]', 'negative_is_expense', 0, 'healthy'
        )
        """
    )

    result = ExportService(db).status(
        sheets_authorization=TestOAuthClient(
            authorized=True,
            write_authorized=False,
        )
    )

    assert [item.name for item in result.destinations] == [
        "local:exports",
        "archive",
        "broken-dashboard",
        "dashboard",
    ]
    assert [item.kind for item in result.destinations] == [
        "local",
        "local",
        "sheets",
        "sheets",
    ]
    assert result.destinations[0].ready is True
    assert result.destinations[0].write_capable is True
    assert result.destinations[3].ready is False
    assert result.destinations[3].write_capable is False
    assert result.destinations[2].reasons == (
        "invalid_managed_tab_prefix",
        "sheets_write_authorization_required",
    )
    assert result.destinations[3].reasons == (
        "inbound_connection_collision",
        "sheets_write_authorization_required",
    )
    serialized = asdict(result)
    assert "private-sheet-id" not in str(serialized)
    assert "other-private-sheet-id" not in str(serialized)
    assert "/private/export/path" not in str(serialized)
