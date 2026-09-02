"""CLI coverage for the tier-spanning catalog/runner and lifecycle subcommands.

Business logic lives in ``UserReportsService`` and is tested against a real
database in ``tests/moneybin/test_reports/test_user_reports.py``. These tests own
what only the CLI decides: flag grammar, exit codes, prompts, and the arguments
that reach the service.
"""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import typer
from click.testing import Result
from typer.testing import CliRunner

from moneybin import error_codes
from moneybin.cli.commands.reports import user_reports
from moneybin.cli.main import app
from moneybin.cli.output import CLI_MAX_ROWS
from moneybin.cli.report_params import parse_parameter_value
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.reports._framework.contract import DefaultColumns, ReportQuery
from moneybin.reports._framework.execute import ReportResult
from moneybin.reports._framework.explain import ColumnProvenance, ReportExplanation
from moneybin.reports._framework.introspect import build_spec
from moneybin.repositories.user_reports_repo import UNSET
from moneybin.services.user_reports_service import ReclassifyOutcome, SaveOutcome
from moneybin.tables import TableRef
from tests.database_mocks import no_profile_database
from tests.moneybin.test_reports._metadata import TEST_SEMANTICS, output_columns

runner = CliRunner()

_ROW: dict[str, Any] = {
    "report_id": "user:rab12cd34ef56",
    "name": "my_accounts",
    # `reclassify` reads this before it prompts and hands it back to the service,
    # so the approval cannot land on a revision the user never saw.
    "class_fingerprint": "fp-at-prompt-time",
}


def _service(**attributes: Any) -> MagicMock:
    service = MagicMock()
    service.resolve.return_value = _ROW
    # A real class, not a child mock: `reclassify` interpolates this into the
    # confirmation prompt, and `<MagicMock id=...>` there would be a passing test
    # describing an unreadable question.
    service.derived_class.return_value = DataClass.TXN_AMOUNT
    for name, value in attributes.items():
        getattr(service, name).return_value = value
    return service


def _patch_service(service: MagicMock) -> Any:
    return patch(
        "moneybin.services.user_reports_service.UserReportsService",
        return_value=service,
    )


def _patch_database() -> Any:
    """Patch the name the command module bound, not the definition site.

    ``user_reports`` imports ``get_database`` at module level, so a patch on
    ``moneybin.database`` would never be seen — and the command would fail on a
    real missing profile, which every exit-code assertion here would read as the
    failure it was looking for.
    """
    return patch(
        "moneybin.cli.commands.reports.user_reports.get_database",
        return_value=no_profile_database(),
    )


def _patch_catalog_database() -> Any:
    """Patch the connection ``open_report_catalog`` opens, in its own module.

    ``reports list`` reaches the database through the catalog helper rather than
    directly, and that helper *degrades* to the packaged tiers when no database
    exists — so leaving it unpatched would let a list test pass through the
    degradation branch and prove nothing about the flag it is checking.
    """
    return patch(
        "moneybin.reports._framework.catalog.get_database",
        return_value=no_profile_database(),
    )


def _catalog_entry(
    report_id: str, *, name: str = "a_report", archived: bool = False
) -> MagicMock:
    """One listing row, shaped like ``ReportCatalogEntry`` for the text render."""
    entry = MagicMock(
        report_id=report_id,
        tier="user" if report_id.startswith("user:") else "builtin",
        archived=archived,
        parameter_classes={},
        description="A report.",
    )
    # `name` is reserved by the MagicMock constructor — passing it above would
    # name the mock and leave `entry.name` a child mock, so the render would
    # print a repr and the assertion would pass on nothing.
    entry.name = name
    # The JSON path serializes each entry; a mock's `model_dump` would hand
    # `json.dumps` an unserializable child mock.
    entry.model_dump.return_value = {"report_id": report_id, "name": name}
    return entry


def _save_outcome(**overrides: Any) -> SaveOutcome:
    fields: dict[str, Any] = {
        "report_id": _ROW["report_id"],
        "name": _ROW["name"],
    }
    return SaveOutcome(**(fields | overrides))


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


def test_list_rejects_an_unknown_tier() -> None:
    result = runner.invoke(app, ["reports", "list", "--tier", "packaged"])

    assert result.exit_code == 2
    assert "builtin, extension, or user" in result.output


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (["reports", "list"], False),
        (["reports", "list", "--include-archived"], True),
    ],
    ids=["default", "widened"],
)
def test_list_asks_for_the_view_its_flag_names(argv: list[str], expected: bool) -> None:
    """``--include-archived`` widens the listing, matching ``accounts list``.

    Only the payload takes the view now — the envelope's sensitivity is read off
    the rows the payload produced, so the two cannot name different row sets. See
    ``test_list_reports_the_sensitivity_of_the_rows_it_returned``. Which rows each
    view actually contains is proved against a real database in
    ``test_reports/test_user_reports.py``.
    """
    with (
        _patch_catalog_database(),
        patch(
            "moneybin.reports._framework.catalog.get_report_catalog",
            return_value=MagicMock(),
        ),
        patch(
            "moneybin.reports._framework.catalog.catalog_to_payload",
            return_value=MagicMock(reports=[]),
        ) as to_payload,
    ):
        result = runner.invoke(app, argv)

    assert result.exit_code == 0, result.output
    assert to_payload.call_args.kwargs == {"include_archived": expected}


def test_list_reports_the_sensitivity_of_the_rows_it_returned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--tier builtin`` returns reviewed metadata only, so the envelope is LOW.

    The tier was read off the whole catalog before ``--tier`` narrowed it, so a
    listing carrying nothing but repo-authored built-in names reported MEDIUM and
    ``user_note`` — an envelope describing rows the response did not contain. It
    is not a leak; it is the audit record naming a class that was never returned,
    which is the signal a reviewer would use to find one.

    The mock catalog is populated on purpose: a regression that reads the catalog
    again then fails on the wrong *tier* rather than on iterating a bare mock.
    """
    captured: dict[str, object] = {}
    monkeypatch.setattr("moneybin.cli.output.write_privacy_event", captured.update)
    catalog = MagicMock()
    catalog.list.return_value = [
        MagicMock(report_id="core:networth"),
        MagicMock(report_id="user:rab12cd34ef56"),
    ]
    entries = [
        _catalog_entry("core:networth"),
        _catalog_entry("user:rab12cd34ef56", name="monthly_spend"),
    ]
    with (
        _patch_catalog_database(),
        patch(
            "moneybin.reports._framework.catalog.get_report_catalog",
            return_value=catalog,
        ),
        patch(
            "moneybin.reports._framework.catalog.catalog_to_payload",
            return_value=MagicMock(reports=entries),
        ),
    ):
        result = runner.invoke(
            app, ["reports", "list", "--tier", "builtin", "--output", "json"]
        )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["summary"]["sensitivity"] == "low"
    assert captured["classes_returned"] == ["aggregate"]


def test_list_marks_an_archived_row_it_was_asked_to_include() -> None:
    """A widened listing that cannot say which rows it widened to include is a trap.

    The caller sees a runnable report with nothing stating the user had put it
    away. The rendered table must carry the distinction, not just the JSON.
    """
    entries = [
        _catalog_entry("core:networth", archived=False),
        _catalog_entry("user:put_away", archived=True),
    ]
    with (
        _patch_catalog_database(),
        patch(
            "moneybin.reports._framework.catalog.get_report_catalog",
            return_value=MagicMock(),
        ),
        patch(
            "moneybin.reports._framework.catalog.catalog_to_payload",
            return_value=MagicMock(reports=entries),
        ),
        patch(
            "moneybin.reports._framework.catalog.catalog_sensitivity",
            return_value="medium",
        ),
    ):
        result = runner.invoke(app, ["reports", "list", "--include-archived"])

    assert result.exit_code == 0, result.output
    archived_line = next(
        line for line in result.output.splitlines() if "put_away" in line
    )
    assert "archived" in archived_line
    active_line = next(
        line for line in result.output.splitlines() if "networth" in line
    )
    assert "archived" not in active_line


def test_list_renders_the_handle_and_not_only_the_opaque_id() -> None:
    """A saved report's id is minted; its name is the string its owner typed.

    ``resolve()`` takes either, but only one is memorable — and for the user tier
    the id is ``user:r`` plus twelve hex characters. A listing showing that alone
    is a dead end: the report is runnable and its handle is unknowable.
    """
    entries = [_catalog_entry("user:rab12cd34ef56", name="monthly_spend")]
    with (
        _patch_catalog_database(),
        patch(
            "moneybin.reports._framework.catalog.get_report_catalog",
            return_value=MagicMock(),
        ),
        patch(
            "moneybin.reports._framework.catalog.catalog_to_payload",
            return_value=MagicMock(reports=entries),
        ),
        patch(
            "moneybin.reports._framework.catalog.catalog_sensitivity",
            return_value="medium",
        ),
    ):
        result = runner.invoke(app, ["reports", "list"])

    assert result.exit_code == 0, result.output
    # Not a substring of the id, so the assertion cannot pass on the id alone.
    assert "monthly_spend" in result.output


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def test_run_coerces_a_parameter_to_its_declared_type_before_executing() -> None:
    """R5's binder: ``--param top=5`` must reach the report as an int, not "5"."""
    catalog = MagicMock()
    catalog.execute.return_value = ReportResult(
        records=[{"value": 1}],
        columns=["value"],
        output_classes={"value": DataClass.AGGREGATE},
        tier=Tier.LOW,
        total_count=1,
        truncated=False,
    )
    with (
        _patch_database(),
        patch(
            "moneybin.reports._framework.catalog.get_report_catalog",
            return_value=catalog,
        ),
        patch(
            "moneybin.cli.report_params.parse_report_parameters",
            return_value={"top": 5},
        ) as parse,
    ):
        result = runner.invoke(
            app,
            ["reports", "run", "core:merchants", "--param", "top=5", "--limit", "3"],
        )

    assert result.exit_code == 0, result.output
    assert parse.call_args.args[1:] == ("core:merchants", ["top=5"])
    assert catalog.execute.call_args.kwargs["parameters"] == {"top": 5}
    assert catalog.execute.call_args.kwargs["limit"] == 3


def test_run_help_states_the_default_row_cap_it_actually_applies() -> None:
    """The help said "unbounded" while the code substituted a cap.

    A user reading "Default: unbounded" and receiving 1,000,000 of 4,000,000
    rows has an incomplete financial answer and a documented reason to believe
    it is complete. Derived from the constant, so the two cannot drift.
    """
    result = runner.invoke(app, ["reports", "run", "--help"])

    assert result.exit_code == 0, result.output
    assert "unbounded" not in result.output
    assert f"{CLI_MAX_ROWS:,}" in " ".join(result.output.split())


def test_run_rejects_a_limit_below_one() -> None:
    """Parity with the ``reports`` MCP tool, which validates ``ge=1``.

    Passed through, ``--limit 0`` slices to zero rows and then reports
    ``truncated: true`` against a ``total_count`` of 1 — an empty result claiming
    to have been cut short, from the surface with no schema to refuse it.
    """
    result = runner.invoke(app, ["reports", "run", "core:merchants", "--limit", "0"])

    assert result.exit_code == 2
    assert "at least 1" in result.output


# ---------------------------------------------------------------------------
# run: default columns, --wide, and the terminal fit (requirements 6-10)
# ---------------------------------------------------------------------------

_RUN_CLASSES = {
    "year_month": DataClass.TXN_DATE,
    "account_name": DataClass.USER_NOTE,
    "category": DataClass.CATEGORY,
    "currency_code": DataClass.CURRENCY,
    "net": DataClass.TXN_AMOUNT,
    "txn_count": DataClass.AGGREGATE,
}
_RUN_COLUMN_NAMES = tuple(_RUN_CLASSES)


def _run_runner(db: Database) -> ReportQuery:
    """Grouped rollup.

    Args:
        db: Open read-only database connection.
    """
    return ReportQuery("SELECT 1", [])


def _run_spec(default_columns: DefaultColumns | None):  # noqa: ANN202 — test helper
    return build_spec(
        _run_runner,
        report_id="test:run",
        name="run_report",
        view=TableRef("reports", "test_run"),
        classes=_RUN_CLASSES,
        parameter_classes={},
        columns=output_columns(_RUN_CLASSES),
        semantics=TEST_SEMANTICS,
        default_columns=default_columns,
    )


def _invoke_run(
    *args: str,
    default_columns: DefaultColumns | None,
    columns_env: str = "200",
) -> Result:
    """Run ``reports run`` against a real spec rather than a MagicMock one.

    A MagicMock catalog answers ``spec.default_columns`` with a callable child
    mock, so the declaration is neither ``None`` nor a tuple: the resolution
    takes the callable branch, iterates an auto-created empty return, and fails
    open to the whole projection. The command then renders every column and
    fits nothing — passing whatever this path does, because it never runs.
    """
    catalog = MagicMock()
    catalog.resolve.return_value = _run_spec(default_columns)
    catalog.execute.return_value = ReportResult(
        records=[dict.fromkeys(_RUN_COLUMN_NAMES, "value")],
        columns=list(_RUN_COLUMN_NAMES),
        output_classes=dict(_RUN_CLASSES),
        tier=Tier.LOW,
        total_count=1,
        truncated=False,
    )
    with (
        _patch_database(),
        patch(
            "moneybin.reports._framework.catalog.get_report_catalog",
            return_value=catalog,
        ),
        patch(
            "moneybin.cli.report_params.parse_report_parameters",
            return_value={},
        ),
    ):
        return runner.invoke(
            app,
            ["reports", "run", "test:run", *args],
            env={"COLUMNS": columns_env},
        )


def test_run_renders_only_the_reports_default_columns() -> None:
    """Requirement 7: one report renders the same whichever command ran it.

    `reports run` reaches every tier, built-ins included, so a report narrowed
    by its generated command and widened by `run` would answer the same
    question two ways.
    """
    result = _invoke_run(default_columns=("year_month", "net"))

    assert result.exit_code == 0, result.output
    assert "year_month" in result.output
    assert "txn_count" not in result.output


def test_run_frames_the_columns_it_omitted() -> None:
    """Requirement 10: the omission is disclosed on this path too."""
    result = _invoke_run(default_columns=("year_month", "net"))

    assert "2 of 6 columns shown — --wide for all" in result.output


def test_wide_restores_the_full_projection_on_run() -> None:
    """Requirement 7's escape hatch, and nothing left to disclose."""
    result = _invoke_run("--wide", default_columns=("year_month", "net"))

    assert result.exit_code == 0, result.output
    assert "txn_count" in result.output
    assert "columns shown" not in result.output


def test_run_fits_a_report_that_declares_no_columns_to_the_terminal() -> None:
    """Requirement 6 for the saved reports this command exists to run.

    A user's report names no default set — their own SELECT is the projection —
    so the renderer measures it against the real terminal instead of capping
    it. Every column here fits at 200 and cannot at 40.
    """
    narrow = _invoke_run(default_columns=None, columns_env="40")

    assert narrow.exit_code == 0, narrow.output
    assert "…" in narrow.output
    assert "columns shown — --wide for all" in narrow.output


def test_run_does_not_fit_a_report_that_declares_no_columns_under_wide() -> None:
    """`--wide` asks for the whole projection, not for a fitted one."""
    result = _invoke_run("--wide", default_columns=None, columns_env="40")

    assert result.exit_code == 0, result.output
    assert "columns shown" not in result.output


def test_run_leaves_a_declared_set_unfitted() -> None:
    """A declared set is a curated answer, not a width budget.

    Re-deciding it from the terminal would discard the author's judgement, so
    the narrowing a report asked for is the only one it gets — the framing line
    still counts two, not one, in a terminal too narrow for both.
    """
    result = _invoke_run(default_columns=("year_month", "net"), columns_env="20")

    assert result.exit_code == 0, result.output
    assert "2 of 6 columns shown — --wide for all" in result.output


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------


def test_create_requires_exactly_one_query_source() -> None:
    bare = runner.invoke(app, ["reports", "create", "spend"])
    both = runner.invoke(
        app,
        ["reports", "create", "spend", "--sql", "SELECT 1", "--sql-file", "q.sql"],
    )

    assert bare.exit_code == 2
    assert both.exit_code == 2
    assert "--sql or --sql-file" in bare.output


def test_create_reads_the_query_from_a_file(tmp_path: Path) -> None:
    query = tmp_path / "spend.sql"
    query.write_text("SELECT account_id FROM core.dim_accounts\n")
    service = _service(create=_save_outcome())

    with _patch_database(), _patch_service(service):
        result = runner.invoke(
            app, ["reports", "create", "my_accounts", "--sql-file", str(query)]
        )

    assert result.exit_code == 0, result.output
    assert service.create.call_args.kwargs["query_sql"] == (
        "SELECT account_id FROM core.dim_accounts\n"
    )


def test_create_routes_a_missing_sql_file_through_the_json_error_boundary(
    tmp_path: Path,
) -> None:
    """A file that cannot be read owes ``--output json`` an error envelope.

    The read ran *before* ``handle_cli_errors``, so the OSError became a Typer
    usage error: plain text on a JSON invocation, and exit 2 where ``cli.md``
    reserves 2 for usage errors and puts a failed read at 1. Nothing had to be
    written to classify it — ``classify_user_error`` already answers
    ``FileNotFoundError``; the read just never reached the handler that asks.
    """
    missing = tmp_path / "absent.sql"

    with _patch_database(), _patch_service(_service(create=_save_outcome())):
        result = runner.invoke(
            app,
            [
                "reports",
                "create",
                "spend",
                "--sql-file",
                str(missing),
                "--output",
                "json",
            ],
        )

    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert payload["error"]["code"] == error_codes.INFRA_FILE_NOT_FOUND


def test_create_routes_an_undecodable_sql_file_through_the_json_error_boundary(
    tmp_path: Path,
) -> None:
    """Non-UTF-8 bytes escaped as a traceback, which no envelope contract survives.

    ``UnicodeDecodeError`` is a ``ValueError``, so it fell through the ``except
    OSError`` that caught the sibling case entirely — an unclassified crash rather
    than the wrong exit code. Asserted separately from the missing-file test
    because one ``except`` clause covering both is exactly what was wrong.
    """
    undecodable = tmp_path / "spend.sql"
    undecodable.write_bytes(b"SELECT \xff\xfe FROM core.dim_accounts")

    with _patch_database(), _patch_service(_service(create=_save_outcome())):
        result = runner.invoke(
            app,
            [
                "reports",
                "create",
                "spend",
                "--sql-file",
                str(undecodable),
                "--output",
                "json",
            ],
        )

    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert payload["error"]["code"] == error_codes.INFRA_INVALID_INPUT


def test_set_routes_a_missing_sql_file_through_the_json_error_boundary(
    tmp_path: Path,
) -> None:
    """``set`` reads the same file through the same helper, one command later.

    Both call sites sat outside the handler, so fixing only ``create`` would leave
    the identical defect in the command that replaces a saved report's query.
    """
    missing = tmp_path / "absent.sql"

    with _patch_database(), _patch_service(_service(update=_save_outcome())):
        result = runner.invoke(
            app,
            [
                "reports",
                "set",
                "my_accounts",
                "--sql-file",
                str(missing),
                "--output",
                "json",
            ],
        )

    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)
    assert payload["error"]["code"] == error_codes.INFRA_FILE_NOT_FOUND


def test_create_declares_a_typed_parameter_with_a_default() -> None:
    service = _service(create=_save_outcome())

    with _patch_database(), _patch_service(service):
        result = runner.invoke(
            app,
            [
                "reports",
                "create",
                "top_spend",
                "--sql",
                "SELECT account_id FROM core.dim_accounts LIMIT $top",
                "--param",
                "top:int=5",
            ],
        )

    assert result.exit_code == 0, result.output
    (declared,) = service.create.call_args.kwargs["params"]
    assert declared.name == "top"
    assert declared.annotation is int
    assert declared.default == 5
    assert declared.required is False


def test_create_reports_unresolved_columns_as_a_warning_not_a_failure() -> None:
    service = _service(create=_save_outcome(unresolved_columns=("mystery",)))

    with _patch_database(), _patch_service(service):
        result = runner.invoke(
            app, ["reports", "create", "opaque", "--sql", "SELECT 1 AS mystery"]
        )

    assert result.exit_code == 0, result.output
    assert "mystery" in result.output


def test_create_names_the_floored_columns_and_the_scan_that_covers_them() -> None:
    """A floored column returns values in the clear, so the note is the only tell.

    The line has to say more than the column name: the author's question is
    whether the column is protected, and "scanned per value, with these gaps" is
    a different answer from ``unresolved_columns``' "masked whole".
    """
    service = _service(create=_save_outcome(floored_columns=("raw_note",)))

    with _patch_database(), _patch_service(service):
        result = runner.invoke(
            app,
            [
                "reports",
                "create",
                "peek",
                "--sql",
                "SELECT note AS raw_note FROM prep.stg_tabular__transactions",
            ],
        )

    assert result.exit_code == 0, result.output
    assert "raw_note" in result.output
    assert "DECIMAL" in result.output


@pytest.mark.parametrize(
    ("field", "alias"),
    [
        ("unresolved_columns", "amazon_spend"),
        ("floored_columns", "costco_run"),
        ("cleared_downgrades", "wholefoods_total"),
    ],
)
def test_a_save_note_keeps_the_column_alias_out_of_the_durable_log(
    field: str, alias: str
) -> None:
    """The alias reaches the terminal; the log keeps an id and a count.

    A saved report's output alias is user-authored text — ``amazon_spend`` is as
    plausible a merchant name as a column one, and ``SanitizedLogFormatter``
    cannot recognize either. These notes are console presentation, so the detail
    belongs in an echoed line and the durable record keeps only what identifies
    the event.

    The logger is replaced rather than read through ``caplog`` for a reason that
    is itself the bug: the note is *only* a log call today, so the record and the
    console line are the same string. Substituting the sink separates them — the
    alias must survive in the output with logging silenced, which no assertion
    over a shared string can require.

    Parametrized over both notes rather than only the reviewed one: they are the
    same defect twice, four lines apart, so fixing one and leaving the other is
    how a second pattern for the same job survives beside the first.
    """
    service = _service(create=_save_outcome(**{field: (alias,)}))

    with (
        _patch_database(),
        _patch_service(service),
        patch.object(user_reports, "logger") as log,
    ):
        result = runner.invoke(
            app,
            ["reports", "create", "opaque", "--sql", f"SELECT 1 AS {alias}"],
        )

    assert result.exit_code == 0, result.output
    # The user still learns which column the note is about.
    assert alias in result.output
    logged = [str(call.args[0]) for call in log.warning.call_args_list]
    assert logged, "expected a durable record identifying the event"
    assert not any(alias in message for message in logged)
    assert any(_ROW["report_id"] in message for message in logged)


def test_create_says_nothing_extra_when_every_column_resolved() -> None:
    """R3's other half: silence everywhere the inference was not uncertain.

    A confirm or note that fires on the clean path is the failure mode
    ``design-principles.md`` names — it trains the reader to skip the one that
    matters. Asserting on the marker rather than on exact prose keeps this from
    breaking on a wording change while still failing on a spurious note.
    """
    service = _service(create=_save_outcome())

    with _patch_database(), _patch_service(service):
        result = runner.invoke(
            app,
            ["reports", "create", "my_accounts", "--sql", "SELECT 1 AS n"],
        )

    assert result.exit_code == 0, result.output
    assert "⚠️" not in result.output


def test_create_rejects_an_unsupported_parameter_type() -> None:
    with _patch_database(), _patch_service(_service(create=_save_outcome())):
        result = runner.invoke(
            app,
            [
                "reports",
                "create",
                "odd",
                "--sql",
                "SELECT 1 AS n",
                "--param",
                "when:timestamp",
            ],
        )

    assert result.exit_code == 1
    assert "timestamp" in result.output


# ---------------------------------------------------------------------------
# set
# ---------------------------------------------------------------------------


def test_set_rejects_opposite_lifecycle_flags() -> None:
    result = runner.invoke(
        app, ["reports", "set", "my_accounts", "--archive", "--restore"]
    )

    assert result.exit_code == 2
    assert "opposites" in result.output


def test_set_rejects_a_request_that_changes_nothing() -> None:
    result = runner.invoke(app, ["reports", "set", "my_accounts"])

    assert result.exit_code == 2
    assert "nothing to change" in result.output


def test_set_sends_only_the_supplied_fields_to_the_service() -> None:
    """An omitted flag must reach the service as UNSET, not as ``None``.

    ``description`` is nullable, so ``None`` is a value a caller may legitimately
    write — conflating the two would clear a description on every rename.
    """
    service = _service(update=_save_outcome())

    with _patch_database(), _patch_service(service):
        result = runner.invoke(
            app, ["reports", "set", "my_accounts", "--name", "accounts"]
        )

    assert result.exit_code == 0, result.output
    kwargs = service.update.call_args.kwargs
    assert kwargs["name"] == "accounts"
    assert kwargs["description"] is UNSET
    assert kwargs["query_sql"] is UNSET
    assert kwargs["params"] is UNSET
    assert kwargs["is_active"] is UNSET


def test_set_can_clear_every_parameter_declaration() -> None:
    """Dropping the last placeholder from a saved query must be expressible.

    An omitted ``--param`` means "leave the declarations alone" (``UNSET``), and
    every occurrence of the repeatable option requires a value — so there was no
    spelling for the empty list. Meanwhile ``derive.py::_refuse_unused_parameters``
    rejects a declaration the new SQL no longer interpolates, which made an
    otherwise-valid update unreachable from the CLI: the report could neither keep
    its stale declarations nor shed them. ``--clear-params`` follows the
    ``--clear-FIELD`` convention ``accounts set`` already established.
    """
    service = _service(update=_save_outcome())

    with _patch_database(), _patch_service(service):
        result = runner.invoke(
            app,
            [
                "reports",
                "set",
                "my_accounts",
                "--sql",
                "SELECT 1 AS n",
                "--clear-params",
            ],
        )

    assert result.exit_code == 0, result.output
    assert service.update.call_args.kwargs["params"] == []


def test_set_refuses_to_clear_and_declare_parameters_at_once() -> None:
    """The two flags express opposite intents; silently preferring one hides it.

    Asserts the refusal's own wording, not the flag name: an unrecognized option
    also exits 2 and echoes the name it did not recognize, so a name-only
    assertion passes just as well when the flag does not exist at all.
    """
    with _patch_database(), _patch_service(_service(update=_save_outcome())):
        result = runner.invoke(
            app,
            [
                "reports",
                "set",
                "my_accounts",
                "--clear-params",
                "--param",
                "top:int=5",
            ],
        )

    assert result.exit_code == 2
    assert "opposites" in result.output


def test_set_archives_by_clearing_is_active() -> None:
    service = _service(update=_save_outcome())

    with _patch_database(), _patch_service(service):
        result = runner.invoke(app, ["reports", "set", "my_accounts", "--archive"])

    assert result.exit_code == 0, result.output
    assert service.update.call_args.kwargs["is_active"] is False


def test_set_sends_an_empty_query_to_the_service_instead_of_dropping_it() -> None:
    """``--sql ""`` is a supplied value, and an invalid one — not an omission.

    Tested for truthiness, an empty string reached the service as ``UNSET``: the
    rename in the same invocation applied and the SQL change vanished, where
    ``create --sql ""`` is correctly refused. Silently dropping one field of a
    multi-field update is the failure mode ``UNSET`` exists to prevent.
    """
    service = _service(update=_save_outcome())

    with _patch_database(), _patch_service(service):
        runner.invoke(
            app, ["reports", "set", "my_accounts", "--name", "accounts", "--sql", ""]
        )

    assert service.update.call_args.kwargs["query_sql"] == ""


def test_set_help_attributes_downgrade_clearing_to_the_sql_alone() -> None:
    """The prose is the only place this rule is stated before the user acts.

    ``--param`` re-derives but deliberately keeps an approved downgrade — the
    behavioural half of this pair is
    ``test_user_reports.py::test_a_params_only_update_keeps_an_approved_downgrade``.
    Help that credits a parameter change with clearing it invites the opposite
    belief about a masking floor, and this one is only ever read *before* the
    mutation, so nothing downstream corrects it.
    """
    result = runner.invoke(app, ["reports", "set", "--help"])

    assert result.exit_code == 0, result.output
    flat = _flatten(result.output)
    assert "re-derives the privacy contract" in flat
    assert "only new SQL drops an approved classification downgrade" in flat


def test_set_warns_when_a_query_change_cleared_an_approved_downgrade() -> None:
    service = _service(update=_save_outcome(cleared_downgrades=("spend",)))

    with _patch_database(), _patch_service(service):
        result = runner.invoke(
            app, ["reports", "set", "my_accounts", "--sql", "SELECT 1 AS spend"]
        )

    assert result.exit_code == 0, result.output
    assert "spend" in result.output
    assert "reclassify" in result.output


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


def test_delete_aborts_when_the_prompt_is_declined() -> None:
    """Declining is the requested outcome, so it exits 0 and deletes nothing.

    ``cli.md`` reserves exit 1 for "operation ran and failed". This asserted 1,
    which pinned the defect in place: every sibling destructive confirm
    (``transactions notes``, ``transactions splits``, ``import``, ``export``,
    ``db``, ``gsheet``) exits 0 on decline, so a script testing ``$?`` could not
    distinguish "the user said no" from "the delete errored" on this command
    alone.
    """
    service = _service()

    with _patch_database(), _patch_service(service):
        result = runner.invoke(app, ["reports", "delete", "my_accounts"], input="n\n")

    assert result.exit_code == 0
    service.delete.assert_not_called()


def test_delete_proceeds_without_a_prompt_under_yes() -> None:
    service = _service()

    with _patch_database(), _patch_service(service):
        result = runner.invoke(app, ["reports", "delete", "my_accounts", "--yes"])

    assert result.exit_code == 0, result.output
    service.delete.assert_called_once_with(_ROW["report_id"], actor="cli")


def test_delete_reports_an_unaskable_confirmation_through_the_envelope() -> None:
    """A closed stdin is not a decline — and it must not skip the envelope.

    ``typer.confirm`` raises ``click.Abort`` on EOF, which is what a piped or
    non-TTY invocation without ``--yes`` produces. ``classify_user_error`` does not
    recognize ``Abort``, so letting it escape spent the whole interaction on a bare
    ``Aborted.``: no error code, and no JSON for a caller that asked for JSON.
    """
    service = _service()

    with _patch_database(), _patch_service(service):
        result = runner.invoke(
            app, ["reports", "delete", "my_accounts", "--output", "json"], input=""
        )

    assert result.exit_code != 0
    # `result.stdout`, not `result.output`: the prompt itself goes to stderr, and
    # the combined stream would put that text in front of the JSON.
    payload = json.loads(result.stdout)
    assert payload["error"]["code"] == "mutation_confirmation_required"
    service.delete.assert_not_called()


# ---------------------------------------------------------------------------
# reclassify
# ---------------------------------------------------------------------------


def test_reclassify_rejects_an_unknown_privacy_class() -> None:
    result = runner.invoke(
        app,
        [
            "reports",
            "reclassify",
            "my_accounts",
            "--column",
            "spend",
            "--to",
            "harmless",
            "--reason",
            "It is fine.",
        ],
    )

    assert result.exit_code == 2
    assert "unknown privacy class" in result.output


@pytest.mark.parametrize(
    ("answer", "confirmed"),
    [("y\n", True), ("n\n", False)],
)
def test_reclassify_passes_the_prompt_answer_through_to_the_service(
    answer: str, confirmed: bool
) -> None:
    """The CLI never decides the downgrade — it relays what the human said.

    Both directions matter: the service refuses an unconfirmed downgrade, so a
    CLI that quietly turned "n" into ``True`` would defeat the only gate on the
    one path that durably lowers a masking floor.
    """
    service = _service(
        reclassify=ReclassifyOutcome(
            report_id=_ROW["report_id"],
            column="spend",
            from_class=DataClass.TXN_AMOUNT,
            to_class=DataClass.AGGREGATE,
        )
    )

    with _patch_database(), _patch_service(service):
        runner.invoke(
            app,
            [
                "reports",
                "reclassify",
                "my_accounts",
                "--column",
                "spend",
                "--to",
                "aggregate",
                "--reason",
                "A single total reveals no transaction amount.",
            ],
            input=answer,
        )

    assert service.reclassify.call_args.kwargs["confirmed"] is confirmed
    assert service.reclassify.call_args.kwargs["confirmed_via"] == "prompt"
    # The revision the prompt described. Read from the row resolved *before* the
    # prompt: the write connection re-resolves, so a fingerprint fetched after
    # the answer would pin the row to itself and guard nothing.
    assert (
        service.reclassify.call_args.kwargs["expected_fingerprint"]
        == _ROW["class_fingerprint"]
    )


def test_reclassify_asks_about_the_class_derivation_produces_now() -> None:
    """The prompt has to name the floor being waived, not just the target.

    The stored fingerprint only moves on a *write*, so an upstream taxonomy change
    that raised this column's class leaves it matching — and the old prompt named
    only ``aggregate``, so a human approving what they read as
    ``txn_amount → aggregate`` could be approving ``routing_number → aggregate``.
    The class is derived fresh at the moment the fingerprint is read, shown, and
    handed back as ``expected_from_class`` so the service refuses if it moved
    again between the question and the write.
    """
    service = _service(
        derived_class=DataClass.ROUTING_NUMBER,
        reclassify=ReclassifyOutcome(
            report_id=_ROW["report_id"],
            column="spend",
            from_class=DataClass.ROUTING_NUMBER,
            to_class=DataClass.AGGREGATE,
        ),
    )

    with _patch_database(), _patch_service(service):
        result = runner.invoke(
            app,
            [
                "reports",
                "reclassify",
                "my_accounts",
                "--column",
                "spend",
                "--to",
                "aggregate",
                "--reason",
                "A single total reveals no transaction amount.",
            ],
            input="y\n",
        )

    assert result.exit_code == 0, result.output
    # The question, not the receipt: the success line reports `from_class` too, so
    # a bare `"routing_number" in output` would pass with the prompt unchanged.
    assert "from routing_number to aggregate" in _flatten(result.output)
    assert service.derived_class.call_args.kwargs["column"] == "spend"
    # Read from the row resolved before the prompt, for the same reason the
    # fingerprint is: a class derived after the answer would agree with itself.
    assert service.derived_class.call_args.args[0] is _ROW
    assert (
        service.reclassify.call_args.kwargs["expected_from_class"]
        is DataClass.ROUTING_NUMBER
    )


def test_reclassify_rejects_a_blank_reason_before_it_asks_anyone() -> None:
    """A request that cannot be stored must not spend a human confirmation.

    The service refuses a blank reason outright — it is the only durable record of
    why a masking floor was lowered. Discovering that *after* the prompt would ask
    someone to approve a downgrade that was never going to land.
    """
    service = _service()

    with _patch_database(), _patch_service(service):
        result = runner.invoke(
            app,
            [
                "reports",
                "reclassify",
                "my_accounts",
                "--column",
                "spend",
                "--to",
                "aggregate",
                "--reason",
                "   ",
            ],
            input="y\n",
        )

    assert result.exit_code == 2
    assert "reason" in _flatten(result.output).lower()
    assert "Permanently lower" not in result.output
    assert service.reclassify.call_count == 0


def test_reclassify_reports_that_it_could_not_ask_rather_than_aborting() -> None:
    """A piped invocation with no ``--yes`` has nobody to ask.

    ``typer.confirm`` aborts on EOF, and an escaping abort spends the whole
    interaction on the word "Aborted!" — no error code, no envelope, nothing
    naming what was refused. ``None`` routes it through the service's ordinary
    refusal instead, which is also what separates "could not ask" from "a human
    said no" in the counter.
    """
    service = _service(
        reclassify=ReclassifyOutcome(
            report_id=_ROW["report_id"],
            column="spend",
            from_class=DataClass.TXN_AMOUNT,
            to_class=DataClass.AGGREGATE,
        )
    )

    with _patch_database(), _patch_service(service):
        result = runner.invoke(
            app,
            [
                "reports",
                "reclassify",
                "my_accounts",
                "--column",
                "spend",
                "--to",
                "aggregate",
                "--reason",
                "A single total reveals no transaction amount.",
            ],
            input="",
        )

    assert service.reclassify.call_args.kwargs["confirmed"] is None
    # Still the prompt path: it was attempted and found nobody, which is not the
    # same claim as "a flag stood in for the human."
    assert service.reclassify.call_args.kwargs["confirmed_via"] == "prompt"
    assert "Aborted" not in result.output


def test_reclassify_routes_a_decline_through_the_service_not_an_early_exit() -> None:
    """The decline reaches the service, unlike ``delete``'s exit-0 cancel.

    Pinned as intent because the divergence looks like an oversight: every
    sibling destructive confirm short-circuits on decline and exits 0. This one
    must not, because the service owns the ``declined`` vs ``no_elicitation``
    split, and it can only count them if it is handed the answer. A short-circuit
    here would leave the one counter that distinguishes "users refuse downgrades"
    from "our prompt never reached a human" reading zero forever, so the refusal
    keeps the structured envelope and exit 1 that the unaskable case returns.
    """
    service = _service(
        reclassify=ReclassifyOutcome(
            report_id=_ROW["report_id"],
            column="spend",
            from_class=DataClass.TXN_AMOUNT,
            to_class=DataClass.AGGREGATE,
        )
    )
    service.reclassify.side_effect = UserError(
        "A classification downgrade needs explicit confirmation.",
        code=error_codes.REPORT_CLASS_CONFIRM_REQUIRED,
    )

    with _patch_database(), _patch_service(service):
        result = runner.invoke(
            app,
            [
                "reports",
                "reclassify",
                "my_accounts",
                "--column",
                "spend",
                "--to",
                "aggregate",
                "--reason",
                "A single total reveals no transaction amount.",
            ],
            input="n\n",
        )

    assert service.reclassify.call_count == 1
    assert service.reclassify.call_args.kwargs["confirmed"] is False
    assert result.exit_code == 1


def test_reclassify_treats_yes_as_the_confirmation() -> None:
    """``--yes`` confirms, and says so — the flag is never recorded as a prompt.

    The audit row is the only place a reviewer can see whether a human approved
    a permanent masking downgrade or an assistant supplied the flag on their
    behalf, so the CLI has to name which one it did.
    """
    service = _service(
        reclassify=ReclassifyOutcome(
            report_id=_ROW["report_id"],
            column="spend",
            from_class=DataClass.TXN_AMOUNT,
            to_class=DataClass.AGGREGATE,
        )
    )

    with _patch_database(), _patch_service(service):
        result = runner.invoke(
            app,
            [
                "reports",
                "reclassify",
                "my_accounts",
                "--column",
                "spend",
                "--to",
                "aggregate",
                "--reason",
                "A single total reveals no transaction amount.",
                "--yes",
            ],
        )

    assert result.exit_code == 0, result.output
    assert service.reclassify.call_args.kwargs["confirmed"] is True
    assert service.reclassify.call_args.kwargs["confirmed_via"] == "flag"
    assert "txn_amount" in result.output


def test_reclassify_help_states_the_confirmation_is_a_human_decision() -> None:
    """An agent reads ``--help``, never the design rule that forbids self-accept."""
    result = runner.invoke(app, ["reports", "reclassify", "--help"])

    assert result.exit_code == 0, result.output
    assert "human decision" in _flatten(result.output)


def _flatten(output: str) -> str:
    """Join Typer's wrapped help text so a phrase can be matched across lines."""
    return " ".join(output.split())


def _explanation(**overrides: Any) -> ReportExplanation:
    fields: dict[str, Any] = {
        "report_id": _ROW["report_id"],
        "name": _ROW["name"],
        "description": "Accounts and their routing numbers.",
        "tier": "user",
        "sql": "SELECT account_id FROM core.dim_accounts WHERE routing_number = $rn",
        "sql_template": (
            "SELECT account_id FROM core.dim_accounts WHERE routing_number = $rn"
        ),
        "sql_unavailable": None,
        "withheld_parameters": ("rn",),
        "sql_suppressed_by": (),
        "columns": (
            ColumnProvenance(
                column="account_id",
                data_class=DataClass.RECORD_ID,
                origin="upstream",
                upstream="core.dim_accounts.account_id",
            ),
        ),
        "lineage": ("core.dim_accounts",),
        "class_fingerprint": "abc123",
        "drift_detected": False,
        "drift_reason": None,
        "updated_at": "2026-07-26 00:00:00",
        "graduation": "eligible",
        "graduation_blockers": (),
    }
    return ReportExplanation(**(fields | overrides))


def _patch_explain(explanation: ReportExplanation) -> Any:
    return patch(
        "moneybin.reports._framework.explain.explain_spec",
        return_value=explanation,
    )


def test_explain_binds_parameters_and_renders_the_class_map() -> None:
    with (
        _patch_database(),
        patch(
            "moneybin.reports._framework.catalog.get_report_catalog",
            return_value=MagicMock(),
        ),
        patch(
            "moneybin.cli.report_params.coerce_report_parameters",
            return_value={"rn": "021000021"},
        ),
        _patch_explain(_explanation()) as explain,
    ):
        result = runner.invoke(
            app,
            ["reports", "explain", "my_accounts", "--param", "rn=021000021"],
        )

    assert result.exit_code == 0, result.output
    assert explain.call_args.kwargs["parameters"] == {"rn": "021000021"}
    flat = _flatten(result.output)
    assert "core.dim_accounts.account_id" in flat
    assert "record_id" in flat
    assert "eligible" in flat
    # The withheld parameter is named, and its value never printed.
    assert "021000021" not in result.output
    assert "rn" in flat


def test_explain_reports_a_suppressed_executed_form_with_the_fix() -> None:
    """An agent must be told which ``--param`` would produce the executed form."""
    explanation = _explanation(sql=None, sql_suppressed_by=("acct",))
    with (
        _patch_database(),
        patch(
            "moneybin.reports._framework.catalog.get_report_catalog",
            return_value=MagicMock(),
        ),
        patch("moneybin.cli.report_params.coerce_report_parameters", return_value={}),
        _patch_explain(explanation),
    ):
        result = runner.invoke(app, ["reports", "explain", "my_accounts"])

    assert result.exit_code == 0, result.output
    flat = _flatten(result.output)
    assert "No executed form" in flat
    assert "acct" in flat


def test_explain_echoes_the_drift_reason_and_keeps_it_out_of_the_log() -> None:
    """The drift reason names the columns that moved, so it is user-authored text.

    Same shape as the save notes: routed through ``logger.warning`` it reached the
    log file with the aliases and — because diagnostic logging is off the console
    — never reached the reader who ran ``reports explain`` at all. The terminal
    gets the reason; the record gets the report id.
    """
    explanation = _explanation(
        drift_detected=True,
        drift_reason="stale_classification: amazon_spend moved upward",
    )
    with (
        _patch_database(),
        patch(
            "moneybin.reports._framework.catalog.get_report_catalog",
            return_value=MagicMock(),
        ),
        patch("moneybin.cli.report_params.coerce_report_parameters", return_value={}),
        _patch_explain(explanation),
        patch.object(user_reports, "logger") as log,
    ):
        result = runner.invoke(app, ["reports", "explain", "my_accounts"])

    assert result.exit_code == 0, result.output
    # The substantive half: with logging substituted, the reason must still reach
    # the reader. Before the fix the reason was *only* a log call, so this fails
    # on the defect rather than passing vacuously alongside the guard below.
    assert "amazon_spend" in _flatten(result.output)
    # The guard. An empty record set satisfies it, and that is the intended state
    # here — drift is logged in counts at its detection site, not re-logged by the
    # renderer — so this fails only on a record that carries the alias back.
    logged = [str(call.args[0]) for call in log.warning.call_args_list]
    assert not any("amazon_spend" in message for message in logged)


def test_explain_json_carries_the_provenance_and_freshness() -> None:
    explanation = _explanation(drift_detected=True, drift_reason="stale_classification")
    with (
        _patch_database(),
        patch(
            "moneybin.reports._framework.catalog.get_report_catalog",
            return_value=MagicMock(),
        ),
        patch("moneybin.cli.report_params.coerce_report_parameters", return_value={}),
        _patch_explain(explanation),
    ):
        result = runner.invoke(
            app, ["reports", "explain", "my_accounts", "--output", "json"]
        )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)["data"]
    assert payload["columns"][0]["upstream"] == "core.dim_accounts.account_id"
    assert payload["class_fingerprint"] == "abc123"
    assert payload["drift_detected"] is True
    assert payload["graduation"] == "eligible"
    assert payload["withheld_parameters"] == ["rn"]


def test_explain_resolves_its_handle_through_the_tier_spanning_catalog() -> None:
    """Archiving hides a report from the catalog; it must not hide the evidence.

    ``explain`` needs no archived flag: the catalog it resolves against always
    spans archived rows, so the promise is a property of the resolver rather than
    of each caller remembering to widen it. Archive-then-explain runs against a
    real database in ``test_reports/test_explain.py``.
    """
    catalog = MagicMock()
    with (
        _patch_database(),
        patch(
            "moneybin.reports._framework.catalog.get_report_catalog",
            return_value=catalog,
        ),
        patch("moneybin.cli.report_params.coerce_report_parameters", return_value={}),
        _patch_explain(_explanation()),
    ):
        result = runner.invoke(app, ["reports", "explain", "my_accounts"])

    assert result.exit_code == 0, result.output
    assert catalog.resolve.call_args.args == ("my_accounts",)


def test_explain_json_envelope_reports_a_saved_report_as_medium() -> None:
    """A saved report's SQL and name are user-authored text, not repo prose."""
    with (
        _patch_database(),
        patch(
            "moneybin.reports._framework.catalog.get_report_catalog",
            return_value=MagicMock(),
        ),
        patch("moneybin.cli.report_params.coerce_report_parameters", return_value={}),
        _patch_explain(_explanation()),
    ):
        saved = runner.invoke(
            app, ["reports", "explain", "my_accounts", "--output", "json"]
        )
    with (
        _patch_database(),
        patch(
            "moneybin.reports._framework.catalog.get_report_catalog",
            return_value=MagicMock(),
        ),
        patch("moneybin.cli.report_params.coerce_report_parameters", return_value={}),
        _patch_explain(_explanation(tier="builtin")),
    ):
        built_in = runner.invoke(
            app, ["reports", "explain", "core:spending", "--output", "json"]
        )

    assert json.loads(saved.output)["summary"]["sensitivity"] == "medium"
    assert json.loads(built_in.output)["summary"]["sensitivity"] == "low"


def test_declared_parameter_defaults_to_text() -> None:
    from moneybin.cli.report_params import parse_parameter_declaration

    declared = parse_parameter_declaration("month")

    assert declared.annotation is str
    assert declared.required is True
    assert declared.data_class is DataClass.UNRESOLVED


def test_declared_parameter_rejects_an_empty_name() -> None:
    from moneybin.cli.report_params import parse_parameter_declaration

    with pytest.raises(Exception, match="names no parameter"):
        parse_parameter_declaration(":int")


def test_declared_parameter_coerces_its_default_to_the_declared_type() -> None:
    from moneybin.cli.report_params import parse_parameter_declaration

    declared = parse_parameter_declaration("as_of:date=2026-07-01")

    assert declared.default.isoformat() == "2026-07-01"


# ---------------------------------------------------------------------------
# What the privacy audit trail records, and when the writer lock is held
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("argv", "patches"),
    [
        (["reports", "create", "r", "--sql", "SELECT 1"], "create"),
        (["reports", "set", "r", "--description", "d"], "update"),
        (["reports", "delete", "r", "--yes"], "delete"),
        (
            [
                "reports",
                "reclassify",
                "r",
                "--column",
                "spend",
                "--to",
                "aggregate",
                "--reason",
                "monthly total",
                "--yes",
            ],
            "reclassify",
        ),
    ],
    ids=["create", "set", "delete", "reclassify"],
)
def test_a_lifecycle_response_records_the_classes_it_returned(
    argv: list[str], patches: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A bare-dict payload declares nothing, so the audit event recorded ``[]``.

    Every one of these responses names a report by its minted id and by the
    user-authored name the text render echoes. ``render_report_result`` in the
    same feature passes its classes explicitly and says why; these four did not.
    """
    captured: dict[str, object] = {}
    monkeypatch.setattr("moneybin.cli.output.write_privacy_event", captured.update)
    outcomes: dict[str, Any] = {
        "create": _save_outcome(),
        "update": _save_outcome(),
        "delete": MagicMock(),
        "reclassify": ReclassifyOutcome(
            report_id=_ROW["report_id"],
            column="spend",
            from_class=DataClass.TXN_AMOUNT,
            to_class=DataClass.AGGREGATE,
        ),
    }
    with _patch_database(), _patch_service(_service(**{patches: outcomes[patches]})):
        result = runner.invoke(app, [*argv, "--output", "json"])

    assert result.exit_code == 0, result.output
    assert captured["classes_returned"] == ["record_id", "user_note"]


def test_the_catalog_listing_records_the_classes_it_returned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A listing that includes a saved report returns user-authored names."""
    captured: dict[str, object] = {}
    monkeypatch.setattr("moneybin.cli.output.write_privacy_event", captured.update)
    with (
        _patch_catalog_database(),
        patch(
            "moneybin.reports._framework.catalog.get_report_catalog",
            return_value=MagicMock(),
        ),
        patch(
            "moneybin.reports._framework.catalog.catalog_to_payload",
            return_value=MagicMock(reports=[]),
        ),
        patch(
            "moneybin.reports._framework.catalog.catalog_sensitivity",
            return_value="medium",
        ),
    ):
        result = runner.invoke(app, ["reports", "list", "--output", "json"])

    assert result.exit_code == 0, result.output
    assert captured["classes_returned"] == ["aggregate", "user_note"]


def test_the_explanation_records_the_classes_it_returned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``explain`` publishes a saved report's SQL verbatim — user-authored text."""
    captured: dict[str, object] = {}
    monkeypatch.setattr("moneybin.cli.output.write_privacy_event", captured.update)
    with (
        _patch_database(),
        patch(
            "moneybin.reports._framework.catalog.get_report_catalog",
            return_value=MagicMock(),
        ),
        patch("moneybin.cli.report_params.coerce_report_parameters", return_value={}),
        _patch_explain(_explanation()),
    ):
        result = runner.invoke(
            app, ["reports", "explain", "my_accounts", "--output", "json"]
        )

    assert result.exit_code == 0, result.output
    assert captured["classes_returned"] == ["aggregate", "user_note"]


@pytest.mark.parametrize(
    "argv",
    [
        ["reports", "delete", "r"],
        [
            "reports",
            "reclassify",
            "r",
            "--column",
            "spend",
            "--to",
            "aggregate",
            "--reason",
            "monthly total",
        ],
    ],
    ids=["delete", "reclassify"],
)
def test_a_confirm_prompt_is_never_held_open_over_the_writer_lock(
    argv: list[str],
) -> None:
    """A write connection holds the exclusive DuckDB writer lock for its lifetime.

    Prompting inside one blocks every other writer — another CLI invocation, the
    MCP server, a scheduled refresh — for an unbounded human wait. Every sibling
    confirm in the codebase prompts *above* its write connection; these two did
    not. Declining is what makes the assertion possible: no write happens, so the
    only connection the command may have opened by prompt time is read-only.
    """
    opened: list[bool] = []
    at_prompt: list[list[bool]] = []

    def _record(*, read_only: bool, **_: Any) -> MagicMock:
        opened.append(read_only)
        return no_profile_database()

    def _decline(*_args: Any, **_kwargs: Any) -> bool:
        at_prompt.append(list(opened))
        return False

    with (
        patch(
            "moneybin.cli.commands.reports.user_reports.get_database",
            side_effect=_record,
        ),
        patch("typer.confirm", side_effect=_decline),
        _patch_service(_service()),
    ):
        runner.invoke(app, argv)

    assert at_prompt, "the command never prompted"
    assert all(read_only for read_only in at_prompt[0]), (
        "a write connection was already open when the prompt appeared"
    )


@pytest.mark.parametrize(
    ("raw", "annotation", "expected"),
    [
        ("2026-01-01", date, date(2026, 1, 1)),
        ("10.50", Decimal, Decimal("10.50")),
        ("true", bool, True),
        ("false", bool, False),
        ("1.5", float, 1.5),
        ("5", int, 5),
        # `_annotation_accepts_container`: JSON syntax is read as JSON, not as a
        # string, so a list parameter is expressible from one `--param`.
        ("[1, 2]", list[int], [1, 2]),
        # `_annotation_accepts_none`: the literal `null` binds None only where the
        # declaration admits it — the pair below proves the "only" half.
        ("null", int | None, None),
    ],
    ids=[
        "date",
        "decimal",
        "bool-true",
        "bool-false",
        "float",
        "int",
        "json-list",
        "null-into-optional",
    ],
)
def test_the_param_binder_coerces_each_declared_type(
    raw: str, annotation: object, expected: object
) -> None:
    """The CLI's own coercion, unmocked.

    Every other test of this path patches ``coerce_report_parameters`` out, which
    proves the wiring and nothing about the coercion. ``--param since=2026-01-01``
    reaching a runner as the string it was typed as is a boundary bug the report's
    own body would report as its internals failing.
    """
    assert parse_parameter_value(raw, annotation) == expected


@pytest.mark.parametrize(
    ("raw", "annotation"),
    [
        ("2026-13-01", date),
        # A locale-style decimal is refused rather than read as 1050 or 10.
        ("10,50", Decimal),
        ("not-a-float", float),
        ("[1,", list[int]),
        ("null", int),
    ],
    ids=[
        "impossible-date",
        "locale-decimal",
        "non-numeric-float",
        "truncated-json",
        "null-into-non-optional",
    ],
)
def test_the_param_binder_refuses_a_value_it_cannot_coerce(
    raw: str, annotation: object
) -> None:
    """A malformed value is a usage error, named at the flag that carried it.

    The refusal matters as much as the coercion: silently reading ``10,50`` as
    ``10`` would filter a money column by a number the user did not type. Typer
    renders ``BadParameter`` as exit code 2 with the ``--param`` hint.
    """
    with pytest.raises(typer.BadParameter):
        parse_parameter_value(raw, annotation)
