"""Tests for the shared export orchestration service."""

from __future__ import annotations

from collections.abc import Callable, Generator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any, cast
from unittest.mock import ANY, MagicMock, patch

import pytest

from moneybin.database import Database, DatabaseLockError
from moneybin.errors import UserError
from moneybin.exports.models import (
    ExportCommand,
    ExportDestination,
    ExportReceipt,
    ExportRequest,
)
from moneybin.exports.service import ExportService
from moneybin.exports.workbook_roles import WorkbookRolePermit
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
        format="csv" if destination.kind == "local" else "sheets",
        redaction_mode="redacted",
        destination=destination,
        artifact_path=None,
        compressed_artifact_path=None,
        sheets_identity=None,
        row_counts={"accounts": 1},
        output_classes={"accounts": {"account_id": "record_id"}},
        checksums={"accounts": "abc"},
        recovery_actions=(),
    )


def _command(*, destination_kind: str = "local") -> ExportCommand:
    return ExportCommand(
        subject_kind="bundle",
        report_id=None,
        report_parameters={},
        destination_reference=(
            "sheets:dashboard" if destination_kind == "sheets" else "local:archive"
        ),
        format="sheets" if destination_kind == "sheets" else "csv",
        redaction_mode="redacted",
        compress_zip=False,
    )


def _command_from_request(request: ExportRequest) -> ExportCommand:
    return ExportCommand(
        subject_kind=request.subject_kind,
        report_id=request.report_id,
        report_parameters=request.report_parameters,
        destination_reference=f"{request.destination.kind}:{request.destination.name}",
        format=request.format,
        redaction_mode=request.redaction_mode,
        compress_zip=request.compress_zip,
    )


@contextmanager
def _database_context(db: Database) -> Generator[Database]:
    yield db


def _database_factory(db: Database) -> Callable[..., AbstractContextManager[Database]]:
    """Hand each ``get_database()`` call its own one-shot context.

    ``run`` opens the read-only snapshot lease and then, after publication, a
    separate short write lease to record the receipt. A single shared
    ``_GeneratorContextManager`` cannot be entered twice.
    """

    def _open(*_args: object, **_kwargs: object) -> AbstractContextManager[Database]:
        return _database_context(db)

    return _open


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
    receipt_failures = getattr(metrics_registry, "EXPORT_RECEIPT_FAILURES_TOTAL", None)
    assert receipt_failures is not None
    # `reason` carries the exception *type* name, never its message: a lock or
    # attach failure's message embeds the database path, and an unbounded
    # label value is a cardinality bomb as well as a disclosure.
    assert receipt_failures._labelnames == (  # type: ignore[reportPrivateUsage]
        "destination_kind",
        "reason",
    )


@patch("moneybin.exports.local.LocalExportPublisher")
@patch("moneybin.database.get_database")
def test_run_releases_read_only_snapshot_before_local_publication(
    get_database: MagicMock,
    publisher_type: MagicMock,
    db: Database,
) -> None:
    """Rendering and filesystem publication happen after DuckDB is closed."""
    active = False
    context = get_database.return_value

    def enter() -> Database:
        nonlocal active
        active = True
        return db

    def exit(*_args: object) -> None:
        nonlocal active
        active = False

    context.__enter__.side_effect = enter
    context.__exit__.side_effect = exit
    destination = _destination("local")
    snapshot = MagicMock()
    receipt = _receipt(destination)

    def publish(*_args: object, **_kwargs: object) -> ExportReceipt:
        assert active is False
        return receipt

    publisher_type.return_value.publish.side_effect = publish
    with (
        patch.object(ExportService, "resolve_destination", return_value=destination),
        patch.object(ExportService, "prepare_bundle", return_value=snapshot),
        patch(
            "moneybin.repositories.export_destinations_repo."
            "ExportDestinationsRepo.assert_current_for_publication"
        ),
    ):
        result = ExportService.run(_command(), actor="test")

    # Two distinct leases, in this order: the read-only snapshot lease (closed
    # before publication — `publish` above asserts `active is False`), then a
    # separate write lease for the receipt. Asserting the sequence rather than
    # a single call keeps the real property — no writer lock is ever held
    # across filesystem I/O — falsifiable if the receipt write moves inside.
    # The write lease keeps the default lock wait: what stops it outliving the
    # caller is the publication barrier around it, not a shortened wait — see
    # test_run_skips_the_receipt_write_when_the_request_already_ended.
    assert [opened.kwargs for opened in get_database.call_args_list] == [
        {"read_only": True},
        {"read_only": False},
    ]
    assert result == receipt


@patch("moneybin.exports.local.LocalExportPublisher")
@patch("moneybin.database.get_database")
def test_run_records_a_discoverable_receipt_in_the_audit_log(
    get_database: MagicMock,
    publisher_type: MagicMock,
    db: Database,
) -> None:
    """A completed export leaves a receipt a later turn or session can find.

    `export_run`'s description promises "recovery uses the returned artifact or
    Sheets receipt", but the receipt was returned exactly once and persisted
    nowhere, so no query path to it existed afterwards (testing.md X.6).
    """
    from moneybin.services.audit_service import AuditService

    context = get_database.return_value
    context.__enter__.return_value = db
    context.__exit__.return_value = None

    destination = _destination("local")
    receipt = replace(
        _receipt(destination),
        export_id="exp-abc123",
        artifact_path=Path("/exports/archive/bundle.csv"),
        row_counts={"accounts": 3, "transactions": 42},
        checksums={"accounts": "sha-a", "transactions": "sha-b"},
    )
    publisher_type.return_value.publish.return_value = receipt

    with (
        patch.object(ExportService, "resolve_destination", return_value=destination),
        patch.object(ExportService, "prepare_bundle", return_value=MagicMock()),
        patch(
            "moneybin.repositories.export_destinations_repo."
            "ExportDestinationsRepo.assert_current_for_publication"
        ),
    ):
        ExportService.run(_command(), actor="mcp:export_run")

    events = AuditService(db).list_events(action_pattern="export.run")

    assert len(events) == 1
    event = events[0]
    assert event.actor == "mcp:export_run"
    assert event.target_id == "exp-abc123"
    recorded = event.context_json or {}
    assert recorded["artifact_name"] == "bundle.csv"
    # The name a destination is known by is mutable; its id is not. Both are
    # recorded so the row still identifies the target after a rename.
    assert recorded["destination_id"] == "local-1"
    # export.md R9 forbids persisting full local paths: a real export
    # directory is ~/Documents/MoneyBin/<profile>/exports and embeds the OS
    # username. Asserted across the whole context, not just the one key, so
    # the guard survives a future field carrying the directory back in.
    assert "/exports/archive" not in str(recorded)
    assert recorded["row_counts"] == {"accounts": 3, "transactions": 42}
    assert recorded["checksums"] == {"accounts": "sha-a", "transactions": "sha-b"}


@patch("moneybin.exports.local.LocalExportPublisher")
@patch("moneybin.database.get_database")
def test_run_records_the_report_subject_without_its_parameter_values(
    get_database: MagicMock,
    publisher_type: MagicMock,
    db: Database,
) -> None:
    """A report receipt says *which report* ran, and nothing about its binding.

    ``app.audit_log`` is durable and re-served by ``system_audit`` long after
    the one-time artifact is gone, so no parameter value — and no derivative of
    one — belongs in it. An earlier revision hashed the binding to tell two runs
    apart; it was removed rather than keyed. For a redacted export the hash
    covered already-masked values, so two bindings sharing a mask collapsed to
    one fingerprint; for an unredacted one it was an unkeyed digest of a
    low-entropy binding, which is a verifier for guessing it. What distinguishes
    two runs instead is already here and stronger: ``export_id`` is unique per
    run, and ``checksums`` differ whenever the exported content does.
    """
    from moneybin.services.audit_service import AuditService

    get_database.side_effect = _database_factory(db)
    destination = _destination("local")
    receipt = replace(
        _receipt(destination),
        export_id="exp-report",
        subject={
            "kind": "report",
            "report_id": "spend_by_category",
            "parameters": {"start": "2026-01-01", "account_id": "acct-77"},
        },
    )
    publisher_type.return_value.publish.return_value = receipt

    with (
        patch.object(ExportService, "resolve_destination", return_value=destination),
        patch.object(ExportService, "prepare_bundle", return_value=MagicMock()),
        patch(
            "moneybin.repositories.export_destinations_repo."
            "ExportDestinationsRepo.assert_current_for_publication"
        ),
    ):
        ExportService.run(_command(), actor="mcp:export_run")

    recorded = AuditService(db).list_events(action_pattern="export.run")[0].context_json
    assert recorded is not None
    assert recorded["subject_kind"] == "report"
    assert recorded["report_id"] == "spend_by_category"
    assert "acct-77" not in str(recorded)
    assert "2026-01-01" not in str(recorded)
    # No derivative of the binding either. Asserted by key rather than by
    # scanning for a digest, so re-adding the field fails here even when its
    # hash happens to contain no substring the checks above would catch.
    assert "parameters_fingerprint" not in recorded


@patch("moneybin.exports.local.LocalExportPublisher")
@patch("moneybin.database.get_database")
def test_export_receipt_refuses_undo_with_a_legible_target(
    get_database: MagicMock,
    publisher_type: MagicMock,
    db: Database,
) -> None:
    """Refusing to undo a published export must say *what* it refuses.

    A published artifact can never be withdrawn, so the refusal itself is
    correct and load-bearing. But ``UndoService`` builds that message from the
    row's ``(target_schema, target_table)``, and a null pair renders as the
    bare ``"."`` — ``_row_targets`` coalesces each null to ``""``. Every other
    outside-``app.*`` audit row (``import_service``'s ``("raw", "pdf_seeds")``)
    keeps a real pair precisely so this message stays readable.
    """
    from moneybin.services.audit_service import AuditService
    from moneybin.services.undo_service import UndoService

    get_database.side_effect = _database_factory(db)
    destination = _destination("local")
    receipt = replace(_receipt(destination), export_id="exp-undo")
    publisher_type.return_value.publish.return_value = receipt

    with (
        patch.object(ExportService, "resolve_destination", return_value=destination),
        patch.object(ExportService, "prepare_bundle", return_value=MagicMock()),
        patch(
            "moneybin.repositories.export_destinations_repo."
            "ExportDestinationsRepo.assert_current_for_publication"
        ),
    ):
        ExportService.run(_command(), actor="mcp:export_run")

    event = AuditService(db).list_events(action_pattern="export.run")[0]
    # Drive the real refusal rather than asserting on the stored tuple: the
    # garbling happens in UndoService's message construction, so a test that
    # only read the row back would pass with the bug in place.
    with pytest.raises(UserError) as refusal:
        UndoService(db).undo(event.operation_id, actor="mcp:system_audit_undo")

    assert "export.run" in str(refusal.value)
    # The exact garbled rendering a null pair produces, pinned so a revert to
    # `(None, None, ...)` fails here rather than only weakening the message.
    assert "touched .," not in str(refusal.value)


@patch("moneybin.exports.local.LocalExportPublisher")
@patch("moneybin.database.get_database")
def test_run_skips_the_receipt_write_when_the_request_already_ended(
    get_database: MagicMock,
    publisher_type: MagicMock,
    db: Database,
) -> None:
    """A receipt must never commit after its caller was told the tool timed out.

    The write opens *after* publication, so unlike every other write in a tool
    body it cannot rely on ``tool_timeout_seconds >= the write-lock wait`` to
    prove it stopped queuing in time — publication's own duration is unbounded.
    The publication barrier is what bounds it: entering an already-cancelled
    request raises instead of writing, and entering a live one makes the
    timeout handler wait rather than return while the write is still running.
    """
    from moneybin.services.audit_service import AuditService
    from moneybin.services.request_lifetime import (
        RequestLifetime,
        request_lifetime_scope,
    )

    # A factory, not one shared context: a single _GeneratorContextManager
    # cannot be entered twice, so reusing one would make this pass on the
    # re-entry error instead of on the barrier.
    get_database.side_effect = _database_factory(db)
    lifetime = RequestLifetime()

    destination = _destination("local")
    receipt = replace(_receipt(destination), export_id="exp-cancelled")

    def publish(*_args: object, **_kwargs: object) -> ExportReceipt:
        # The window this guards: the artifact is on disk, and the tool
        # deadline fires before the receipt write opens. Cancelling here is
        # what the decorator's timeout handler does.
        lifetime.cancel()
        return receipt

    publisher_type.return_value.publish.side_effect = publish

    with (
        request_lifetime_scope(lifetime),
        patch.object(ExportService, "resolve_destination", return_value=destination),
        patch.object(ExportService, "prepare_bundle", return_value=MagicMock()),
        patch(
            "moneybin.repositories.export_destinations_repo."
            "ExportDestinationsRepo.assert_current_for_publication"
        ),
    ):
        result = ExportService.run(_command(), actor="mcp:export_run")

    assert result == receipt
    assert AuditService(db).list_events(action_pattern="export.run") == []


@patch("moneybin.exports.local.LocalExportPublisher")
@patch("moneybin.database.get_database")
def test_run_binds_the_receipt_write_to_an_explicit_publication_lifetime(
    get_database: MagicMock,
    publisher_type: MagicMock,
    db: Database,
) -> None:
    """An explicit ``publication_lifetime`` must bound the receipt write too.

    ``run`` resolves ``publication_lifetime or current_request_lifetime()`` once
    and hands that to both publish steps. A receipt write that re-derived the
    ambient lifetime instead would silently get ``None`` here — no ambient scope
    is installed — and a no-op barrier, so the write this test cancels would
    commit anyway. The parameter exists for callers that own their own
    lifetime; the barrier is worth nothing to them if only two of the three
    steps honour it.
    """
    from moneybin.services.audit_service import AuditService
    from moneybin.services.request_lifetime import RequestLifetime

    get_database.side_effect = _database_factory(db)
    lifetime = RequestLifetime()

    destination = _destination("local")
    receipt = replace(_receipt(destination), export_id="exp-explicit-lifetime")

    def publish(*_args: object, **_kwargs: object) -> ExportReceipt:
        lifetime.cancel()
        return receipt

    publisher_type.return_value.publish.side_effect = publish

    # Deliberately no request_lifetime_scope: the ambient lifetime stays None so
    # only the explicit argument can carry the cancellation through.
    with (
        patch.object(ExportService, "resolve_destination", return_value=destination),
        patch.object(ExportService, "prepare_bundle", return_value=MagicMock()),
        patch(
            "moneybin.repositories.export_destinations_repo."
            "ExportDestinationsRepo.assert_current_for_publication"
        ),
    ):
        result = ExportService.run(
            _command(), actor="mcp:export_run", publication_lifetime=lifetime
        )

    assert result == receipt
    assert AuditService(db).list_events(action_pattern="export.run") == []


@patch("moneybin.exports.sheets.SheetsExportPublisher")
@patch("moneybin.database.get_database")
def test_run_records_a_sheets_receipt_with_its_workbook_identity(
    get_database: MagicMock,
    publisher_type: MagicMock,
    db: Database,
) -> None:
    """Sheets is the destination the audit row is the *only* recovery path for.

    A local export leaves an artifact on disk to find; a Sheets export leaves
    nothing outside the workbook, which is why the missing receipt was worth
    fixing at all. The write path has no kind-specific branching, so this
    asserts the end state rather than a separate mechanism: the row carries the
    workbook identity and no local-artifact names.
    """
    from moneybin.services.audit_service import AuditService

    get_database.side_effect = _database_factory(db)

    destination = _destination("sheets")
    receipt = replace(
        _receipt(destination),
        export_id="exp-sheets-1",
        sheets_identity="spreadsheet-1/MB_bundle",
    )
    publisher_type.return_value.publish.return_value = receipt

    with (
        patch.object(ExportService, "resolve_destination", return_value=destination),
        patch.object(ExportService, "prepare_bundle", return_value=MagicMock()),
        patch(
            "moneybin.repositories.export_destinations_repo."
            "ExportDestinationsRepo.assert_current_for_publication"
        ),
    ):
        ExportService.run(_command(destination_kind="sheets"), actor="mcp:export_run")

    events = AuditService(db).list_events(action_pattern="export.run")

    assert len(events) == 1
    recorded = events[0].context_json or {}
    assert events[0].target_id == "exp-sheets-1"
    assert recorded["destination_kind"] == "sheets"
    assert recorded["sheets_identity"] == "spreadsheet-1/MB_bundle"
    assert recorded["artifact_name"] is None
    assert recorded["compressed_artifact_name"] is None


@patch("moneybin.exports.local.LocalExportPublisher")
@patch("moneybin.database.get_database")
def test_run_returns_the_receipt_when_the_audit_write_cannot_open(
    get_database: MagicMock,
    publisher_type: MagicMock,
    db: Database,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A failed receipt write must not turn a published export into an error.

    The receipt write opens its own connection *after* the artifact is already
    on disk (or in Sheets), and takes a single non-blocking attempt — so any
    concurrent holder at that instant raises, making this reachable rather
    than theoretical. Propagating it would report failure for an irreversible
    success and lose the caller's only copy of the receipt, inviting a re-run
    that publishes a second artifact. Fail loudly in the log, not in the
    return value.

    What reaches the log is bounded too: a lock or attach failure carries the
    database path in its message, and ``SanitizedLogFormatter`` masks amounts
    and account numbers, not filesystem paths.
    """
    failures = metrics_registry.EXPORT_RECEIPT_FAILURES_TOTAL.labels(
        destination_kind="local", reason="DatabaseLockError"
    )
    before = failures._value.get()  # type: ignore[reportPrivateUsage]
    read_lease = _database_context(db)
    locked = DatabaseLockError("/Users/someone/Documents/MoneyBin/main.duckdb held")
    get_database.side_effect = [read_lease, locked]

    destination = _destination("local")
    receipt = replace(_receipt(destination), export_id="exp-locked")
    publisher_type.return_value.publish.return_value = receipt

    with (
        patch.object(ExportService, "resolve_destination", return_value=destination),
        patch.object(ExportService, "prepare_bundle", return_value=MagicMock()),
        patch(
            "moneybin.repositories.export_destinations_repo."
            "ExportDestinationsRepo.assert_current_for_publication"
        ),
        caplog.at_level("ERROR"),
    ):
        result = ExportService.run(_command(), actor="mcp:export_run")

    assert result == receipt
    assert "exp-locked" in caplog.text
    assert "DatabaseLockError" in caplog.text
    # The type and origin are useful; the message is not worth the path it
    # carries. Asserting the absence keeps a later switch back to
    # logger.exception (which appends both message and traceback) failing.
    assert "/Users/someone" not in caplog.text
    # A swallowed failure must leave a countable trace. Without this the only
    # way to learn how often receipts silently fail to record is log-scraping,
    # and the export itself still reports outcome="success" — correctly, since
    # the artifact really was published.
    assert failures._value.get() == before + 1  # type: ignore[reportPrivateUsage]


@patch("moneybin.config.get_settings")
@patch("moneybin.database.get_database")
def test_run_rechecks_sheets_role_then_closes_database_before_network(
    get_database: MagicMock,
    get_settings: MagicMock,
    db: Database,
) -> None:
    """The active role permit outlives the snapshot DB but not publication."""
    from moneybin.repositories.export_destinations_repo import ExportDestinationsRepo

    ExportDestinationsRepo(db).set_sheets(
        name="dashboard",
        spreadsheet_id="spreadsheet-1",
        managed_tab_prefix="MB",
        actor="test",
    )
    destination = ExportService(db).resolve_destination("sheets:dashboard")
    active = False
    context = get_database.return_value

    def enter() -> Database:
        nonlocal active
        active = True
        return db

    def exit(*_args: object) -> None:
        nonlocal active
        active = False

    context.__enter__.side_effect = enter
    context.__exit__.side_effect = exit
    get_settings.return_value.profile = "personal"
    publisher = MagicMock()
    publisher.publish.return_value = _receipt(destination)
    permit: WorkbookRolePermit | None = None

    def publish(*_args: object, **kwargs: object) -> ExportReceipt:
        nonlocal permit
        assert active is False
        permit = cast(WorkbookRolePermit, kwargs["role_permit"])
        permit.assert_for("spreadsheet-1")
        return _receipt(destination)

    publisher.publish.side_effect = publish
    with patch.object(ExportService, "prepare_bundle", return_value=MagicMock()):
        result = ExportService.run(
            _command(destination_kind="sheets"),
            actor="test",
            sheets_publisher=publisher,
        )

    assert result.destination == destination
    assert permit is not None
    with pytest.raises(RuntimeError, match="no longer active"):
        permit.assert_for("spreadsheet-1")


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
    request = _request(format="user-chosen-private-format")
    with (
        patch(
            "moneybin.database.get_database",
            side_effect=_database_factory(db),
        ),
        patch.object(
            ExportService,
            "resolve_destination",
            return_value=request.destination,
        ),
        pytest.raises(ValueError),
    ):
        ExportService.run(_command_from_request(request), actor="test")

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
    request = _request()
    with (
        patch(
            "moneybin.database.get_database",
            side_effect=_database_factory(db),
        ),
        patch.object(
            ExportService,
            "resolve_destination",
            return_value=destination,
        ),
        patch.object(ExportService, "prepare_bundle", return_value=MagicMock()),
        patch(
            "moneybin.repositories.export_destinations_repo."
            "ExportDestinationsRepo.assert_current_for_publication"
        ),
    ):
        ExportService.run(_command_from_request(request), actor="test")

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
    snapshot = MagicMock()

    request = _request()
    with (
        patch(
            "moneybin.database.get_database",
            side_effect=_database_factory(db),
        ),
        patch.object(
            ExportService,
            "resolve_destination",
            return_value=destination,
        ),
        patch.object(
            ExportService,
            "prepare_bundle",
            return_value=snapshot,
        ) as prepare,
        patch(
            "moneybin.repositories.export_destinations_repo."
            "ExportDestinationsRepo.assert_current_for_publication"
        ),
    ):
        receipt = ExportService.run(_command_from_request(request), actor="cli")

    prepare.assert_called_once_with(profile="personal", redaction_mode="redacted")
    publisher_type.assert_called_once_with(
        destination.local_path,
        destination_name="archive",
    )
    publisher.publish.assert_called_once_with(
        snapshot,
        format="csv",
        compress_zip=False,
        publication_lifetime=None,
    )
    assert receipt.destination == destination
    assert receipt.redaction_mode == "redacted"


@patch("moneybin.config.get_settings")
def test_run_prepares_and_publishes_one_sheets_report(
    get_settings: MagicMock,
    db: Database,
) -> None:
    get_settings.return_value.profile = "personal"
    destination = _destination("sheets")
    publisher = MagicMock()
    publisher.publish.return_value = _receipt(destination)
    snapshot = MagicMock()
    request = _request(
        subject_kind="report",
        destination_kind="sheets",
        report_id="core:spending",
        report_parameters={"months": 3},
    )

    with (
        patch(
            "moneybin.database.get_database",
            side_effect=_database_factory(db),
        ),
        patch.object(
            ExportService,
            "resolve_destination",
            return_value=destination,
        ),
        patch(
            "moneybin.repositories.export_destinations_repo."
            "ExportDestinationsRepo.assert_current_for_publication"
        ),
        patch.object(
            ExportService,
            "prepare_report",
            return_value=snapshot,
        ) as prepare,
    ):
        receipt = ExportService.run(
            _command_from_request(request),
            actor="mcp",
            sheets_publisher=publisher,
        )

    prepare.assert_called_once_with(
        profile="personal",
        report_id="core:spending",
        report_parameters={"months": 3},
        redaction_mode="redacted",
    )
    publisher.publish.assert_called_once_with(
        snapshot,
        destination,
        role_permit=ANY,
        publication_lifetime=None,
    )
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
    with (
        patch(
            "moneybin.database.get_database",
            side_effect=_database_factory(db),
        ),
        patch.object(
            ExportService,
            "resolve_destination",
            return_value=export_request.destination,
        ),
        patch.object(ExportService, "prepare_bundle") as prepare_bundle,
        patch.object(ExportService, "prepare_report") as prepare_report,
        pytest.raises(ValueError),
    ):
        ExportService.run(
            _command_from_request(export_request),
            actor="test",
            sheets_publisher=sheets_publisher,
        )

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


def test_status_marks_default_file_destination_not_writable(
    db: Database, tmp_path: Path
) -> None:
    """The derived local destination is not ready when its path is a file."""
    local_file = tmp_path / "exports-file"
    local_file.write_text("not a directory")

    with patch("moneybin.config.get_settings") as get_settings:
        get_settings.return_value.profile_exports_dir = local_file
        status = ExportService(db).status()

    assert status.destinations[0].reasons == ("local_path_not_directory",)
    assert status.destinations[0].write_capable is False


def test_status_marks_local_destination_without_writable_parent_not_ready(
    db: Database, tmp_path: Path
) -> None:
    """A configured path reports unavailable before publishing tries to create it."""
    parent = tmp_path / "parent"
    parent.mkdir()
    destination_path = parent / "exports"
    db.execute(
        """
        INSERT INTO app.export_destinations (
            destination_id, name, kind, local_path, spreadsheet_id,
            managed_tab_prefix, created_at, updated_at
        ) VALUES (
            'local-not-writable', 'archive', 'local', ?, NULL, NULL,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        )
        """,
        [str(destination_path)],
    )

    with patch("moneybin.exports.service.os.access", create=True, return_value=False):
        status = ExportService(db).status()

    archive = next(item for item in status.destinations if item.name == "archive")
    assert archive.reasons == ("local_path_not_writable",)
    assert archive.write_capable is False


@patch("moneybin.config.get_settings")
@patch("moneybin.exports.local.LocalExportPublisher")
def test_run_rejects_file_local_destination_before_preparing_or_writing(
    publisher_type: MagicMock,
    get_settings: MagicMock,
    db: Database,
    tmp_path: Path,
) -> None:
    """The delivery gate shares the local file-path readiness validation."""
    local_file = tmp_path / "exports-file"
    local_file.write_text("not a directory")
    request = replace(
        _request(),
        destination=replace(_destination("local"), local_path=local_file),
    )
    get_settings.return_value.profile = "personal"

    with (
        patch(
            "moneybin.database.get_database",
            side_effect=_database_factory(db),
        ),
        patch.object(
            ExportService,
            "resolve_destination",
            return_value=request.destination,
        ),
        patch.object(ExportService, "prepare_bundle") as prepare_bundle,
        pytest.raises(ValueError, match="local_path_not_directory"),
    ):
        ExportService(db).run(_command_from_request(request), actor="test")

    prepare_bundle.assert_not_called()
    publisher_type.assert_not_called()


@pytest.mark.parametrize(
    "reference",
    ["local:exports", "local: EXPORTS ", "local:ｅｘｐｏｒｔｓ"],
)
def test_resolve_destination_normalizes_the_builtin_exports_name(
    db: Database,
    reference: str,
) -> None:
    destination = ExportService(db).resolve_destination(reference)

    assert destination.name == "local:exports"
    assert destination.kind == "local"


@pytest.mark.parametrize(
    "reference",
    ["local:", "local:   ", "local:archive:monthly", "sheets:   "],
)
def test_resolve_destination_rejects_unaddressable_names(
    db: Database,
    reference: str,
) -> None:
    with pytest.raises(UserError) as exc_info:
        ExportService(db).resolve_destination(reference)

    assert exc_info.value.code == "mutation_invalid_input"
