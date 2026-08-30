"""Tests for the dynamic Typer CLI registrar."""

from __future__ import annotations

import inspect
import json
from collections.abc import Mapping
from dataclasses import replace
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import typer
from click.testing import Result
from typer.testing import CliRunner

from moneybin import error_codes
from moneybin.cli.output import OutputFormat
from moneybin.cli.render import Money
from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.reports._framework.cli_register import (
    build_cli_command,
    money_columns,
    register_report_cli,
    visible_columns,
)
from moneybin.reports._framework.contract import (
    DefaultColumns,
    OutputColumn,
    ReportQuery,
)
from moneybin.reports._framework.execute import ReportResult, inspection_hint
from moneybin.reports._framework.introspect import (
    _RESERVED_CLI_PARAMS,  # pyright: ignore[reportPrivateUsage]  # the guard's subject
    build_spec,
)
from moneybin.repositories.profile_settings_repo import ProfileSettingsRepo
from moneybin.tables import TableRef
from tests.database_mocks import no_profile_database
from tests.moneybin.test_reports._metadata import TEST_SEMANTICS, output_columns

_VIEW = TableRef("reports", "test_summary")
_CLASSES = {"account_id": DataClass.ACCOUNT_IDENTIFIER}
_runner_cli = CliRunner()


def _runner(db: Database, *, top: int = 25) -> ReportQuery:
    """Per-account summary.

    Args:
        db: Open read-only database connection.
        top: Maximum rows to return.
    """
    return ReportQuery("SELECT 1", [])


def _spec():  # noqa: ANN202 — test helper
    return build_spec(
        _runner,
        report_id="test:balance_drift",
        name="balance_drift",
        view=_VIEW,
        classes=_CLASSES,
        parameter_classes={"top": DataClass.AGGREGATE},
        columns=output_columns(_CLASSES),
        semantics=TEST_SEMANTICS,
    )


def _multi_command_app():  # noqa: ANN202 — test helper
    """A Typer app with the report command plus a sibling.

    Typer collapses a single-command app (the subcommand name becomes
    unnecessary); a second command keeps it in multi-command mode so the
    report is invoked as ``<app> balance-drift ...`` like in production.
    """
    app = typer.Typer()
    register_report_cli(_spec(), app)
    app.command("noop")(lambda: None)
    return app


def _result() -> ReportResult:
    return ReportResult(
        records=[{"account_id": "****2222", "txn_count": 2}],
        columns=["account_id", "txn_count"],
        output_classes={
            "account_id": DataClass.ACCOUNT_IDENTIFIER,
            "txn_count": DataClass.AGGREGATE,
        },
        tier=Tier.CRITICAL,
        total_count=1,
        truncated=False,
    )


def _windowed_runner(
    db: Database, *, from_month: str | None = None, to_month: str | None = None
) -> ReportQuery:
    """Windowed summary.

    Args:
        db: Open read-only database connection.
        from_month: Inclusive start month (YYYY-MM).
        to_month: Inclusive end month (YYYY-MM).
    """
    return ReportQuery("SELECT 1", [])


def _windowed_app():  # noqa: ANN202 — test helper
    app = typer.Typer()
    spec = build_spec(
        _windowed_runner,
        report_id="test:windowed",
        name="windowed",
        view=_VIEW,
        classes=_CLASSES,
        parameter_classes={
            "from_month": DataClass.TXN_DATE,
            "to_month": DataClass.TXN_DATE,
        },
        columns=output_columns(_CLASSES),
        semantics=TEST_SEMANTICS,
    )
    register_report_cli(spec, app)
    app.command("noop")(lambda: None)
    return app


def test_cli_command_accepts_hyphenated_window_flags() -> None:
    # The underscore→hyphen flag derivation (from_month → --from-month) is the
    # most prominent breaking change in the report-framework migration. Assert
    # the derived flags parse and forward end-to-end through the injected
    # __signature__ — not just that the Python param name exists.
    app = _windowed_app()
    captured: dict[str, object] = {}

    def _fake_execute(
        db: object,
        *,
        report_id: str,
        parameters: dict[str, object],
        limit: int,
        display_currency: str | None,
        home_currency: str | None,
    ) -> ReportResult:
        captured.update(parameters)
        return _result()

    with (
        patch(
            "moneybin.reports._framework.cli_register.get_database",
            return_value=no_profile_database(),
        ),
        patch("moneybin.reports._framework.catalog.get_report_catalog") as mock_catalog,
    ):
        mock_catalog.return_value.execute.side_effect = _fake_execute
        result = _runner_cli.invoke(
            app,
            [
                "windowed",
                "--from-month",
                "2024-01",
                "--to-month",
                "2024-06",
                "--output",
                "json",
            ],
        )
    assert result.exit_code == 0, result.output
    assert captured == {"from_month": "2024-01", "to_month": "2024-06"}


def test_build_cli_command_signature_has_params_and_output() -> None:
    cmd = build_cli_command(_spec())
    sig = inspect.signature(cmd)
    assert "top" in sig.parameters
    assert "output" in sig.parameters
    assert sig.parameters["output"].annotation is OutputFormat


def _paramless_runner(db: Database) -> ReportQuery:
    """Summary with no parameters.

    Args:
        db: Open read-only database connection.
    """
    return ReportQuery("SELECT 1", [])


def test_reserved_cli_params_match_what_the_registrar_injects() -> None:
    """The two halves of one coupling, held together mechanically.

    ``_cli_signature`` injects shared options beside the runner's own params;
    ``_RESERVED_CLI_PARAMS`` refuses a runner param that would collide with one.
    Adding an option to the first without the second does not fail here — it
    fails at *import* of the reports command group, for whoever next writes a
    report that happens to use the name, and it takes the whole group down.

    Derived from a paramless spec rather than a literal list so a future shared
    option is covered the moment it is injected.
    """
    spec = build_spec(
        _paramless_runner,
        report_id="test:paramless",
        name="paramless",
        view=_VIEW,
        classes=_CLASSES,
        parameter_classes={},
        columns=output_columns(_CLASSES),
        semantics=TEST_SEMANTICS,
    )
    injected = set(inspect.signature(build_cli_command(spec)).parameters)

    assert injected == _RESERVED_CLI_PARAMS


def test_cli_command_forwards_display_currency_to_the_catalog() -> None:
    """``--display-currency`` is a framework option, not a per-report parameter.

    It reaches ``ReportCatalog.execute`` as its own argument — never through
    ``parameters``, which is the runner's own contract and would reject it.
    """
    app = _multi_command_app()
    with (
        patch(
            "moneybin.reports._framework.cli_register.get_database",
            return_value=no_profile_database(),
        ),
        patch("moneybin.reports._framework.catalog.get_report_catalog") as mock_catalog,
    ):
        mock_catalog.return_value.execute.return_value = _result()
        result = _runner_cli.invoke(
            app, ["balance-drift", "--display-currency", "EUR", "--output", "json"]
        )

    assert result.exit_code == 0, result.output
    call = mock_catalog.return_value.execute.call_args
    assert call.kwargs["display_currency"] == "EUR"
    assert "display_currency" not in call.kwargs["parameters"]


def test_cli_command_hands_the_catalog_the_profile_home_currency(
    saved_db: Database,
) -> None:
    """Requirement 9's default only exists if every surface passes it along.

    Read through the real ``ProfileSettingsRepo`` rather than a patched value:
    what this guards is a surface that reaches the catalog without the profile's
    currency, and a stubbed resolver would report success for a surface that
    never called one.
    """
    ProfileSettingsRepo(saved_db).set_home_currency("EUR", actor="test")
    app = _multi_command_app()
    context = MagicMock()
    context.__enter__.return_value = saved_db
    with (
        patch(
            "moneybin.reports._framework.cli_register.get_database",
            return_value=context,
        ),
        patch("moneybin.reports._framework.catalog.get_report_catalog") as mock_catalog,
    ):
        mock_catalog.return_value.execute.return_value = _result()
        result = _runner_cli.invoke(app, ["balance-drift", "--top", "5"])

    assert result.exit_code == 0, result.output
    assert mock_catalog.return_value.execute.call_args.kwargs["home_currency"] == "EUR"


def test_register_report_cli_adds_named_command() -> None:
    app = typer.Typer()
    register_report_cli(_spec(), app)
    names = {c.name for c in app.registered_commands}
    assert "balance-drift" in names  # cli_name = name with hyphens


def test_the_text_path_says_why_a_drifted_report_is_masked() -> None:
    """A ``*****`` with no reason is the failure mode R4 exists to prevent.

    The catalog sets ``degraded`` and ``degraded_reason`` and masks the affected
    columns, but ``render_or_json`` renders no envelope metadata on the text path —
    so ``reports run`` printed the masked table alone while JSON and MCP callers
    received the reason. Silent masking trains the reader to accept ``*****`` as
    normal, which is exactly what makes the honest case unreadable.
    """
    app = _multi_command_app()
    drifted = replace(
        _result(),
        degraded=True,
        degraded_reason="stale_classification: account_id moved upward",
    )
    with (
        patch(
            "moneybin.reports._framework.cli_register.get_database",
            return_value=no_profile_database(),
        ),
        patch("moneybin.reports._framework.catalog.get_report_catalog") as mock_catalog,
    ):
        mock_catalog.return_value.execute.return_value = drifted
        result = _runner_cli.invoke(app, ["balance-drift", "--top", "5"])

    assert result.exit_code == 0, result.output
    assert "stale_classification" in result.output


def test_the_text_path_says_when_rows_were_cut() -> None:
    """An incomplete financial answer that looks complete is the worse failure.

    ``truncated`` reaches JSON and MCP callers through the envelope, and the
    text path renders no envelope metadata — so a capped run printed a table
    that reads as the whole result. Same reason the drift note above exists.

    The note states what was shown and that more exists, never a total: a
    truncated execution sets ``total_count`` to the ``limit + 1`` it probed, so
    printing it would report one row missing where millions are. The fixture
    carries a ``total_count`` that is *not* the probe value precisely so a
    regression that starts printing it again is visible here.
    """
    app = _multi_command_app()
    cut = replace(_result(), truncated=True, total_count=4200)
    with (
        patch(
            "moneybin.reports._framework.cli_register.get_database",
            return_value=no_profile_database(),
        ),
        patch("moneybin.reports._framework.catalog.get_report_catalog") as mock_catalog,
    ):
        mock_catalog.return_value.execute.return_value = cut
        result = _runner_cli.invoke(app, ["balance-drift", "--top", "5"])

    assert result.exit_code == 0, result.output
    assert "first 1 rows" in result.output
    assert "more exist" in result.output
    assert "--limit" in result.output
    # The probe total never reaches the user as a count of what was cut.
    assert "4,200" not in result.output
    assert "4200" not in result.output


def test_the_text_path_prints_the_hint_the_masked_output_earned() -> None:
    """The hint names a CLI command, and the CLI was the one surface that hid it.

    ``redact_catalog_execution`` appends ``inspection_hint`` to ``actions`` whenever
    a column masks, and ``actions`` rides the envelope to JSON and MCP callers — so
    the two surfaces that cannot run ``moneybin reports explain`` were the ones told
    to, while the terminal printed ``*****`` alone. Third instance of the asymmetry
    the two notes above fix, and the sharpest: the hint's own docstring says it
    names a CLI command deliberately.

    Built from ``inspection_hint`` rather than a copied literal so a reworded hint
    keeps this honest instead of pinning prose the renderer no longer emits.
    """
    app = _multi_command_app()
    hint = inspection_hint("test:balance_drift", ("account_id",))
    masked = replace(_result(), actions=[hint])
    with (
        patch(
            "moneybin.reports._framework.cli_register.get_database",
            return_value=no_profile_database(),
        ),
        patch("moneybin.reports._framework.catalog.get_report_catalog") as mock_catalog,
    ):
        mock_catalog.return_value.execute.return_value = masked
        result = _runner_cli.invoke(app, ["balance-drift", "--top", "5"])

    assert result.exit_code == 0, result.output
    assert hint in result.output


def test_the_text_path_adds_no_hint_when_the_report_offered_none() -> None:
    """The hint's other half: a 💡 beside every table is a 💡 nobody reads.

    Separate from the drift-silence test below rather than folded into it: a
    fixture that trips both markers would stay green with either renderer removed.
    """
    app = _multi_command_app()
    with (
        patch(
            "moneybin.reports._framework.cli_register.get_database",
            return_value=no_profile_database(),
        ),
        patch("moneybin.reports._framework.catalog.get_report_catalog") as mock_catalog,
    ):
        mock_catalog.return_value.execute.return_value = _result()
        result = _runner_cli.invoke(app, ["balance-drift", "--top", "5"])

    assert result.exit_code == 0, result.output
    assert "💡" not in result.output


def test_the_text_path_sends_every_note_to_stderr() -> None:
    """Diagnostics take fd 2 so a redirected report carries only its rows.

    Asserted against ``stdout``/``stderr`` separately because ``result.output``
    structurally cannot see it: Click 8.2 turned that into a mix of both streams
    in write order, so an assertion against it passes whichever stream the note
    took. Every other note test here reads ``.output``, which is why the routing
    needs its own case rather than a stricter assertion in one of them.
    """
    app = _multi_command_app()
    noted = replace(
        _result(),
        truncated=True,
        actions=["Run moneybin reports explain test:balance_drift"],
        degraded=True,
        degraded_reason="no stored EUR->USD rates at all",
    )
    with (
        patch(
            "moneybin.reports._framework.cli_register.get_database",
            return_value=no_profile_database(),
        ),
        patch("moneybin.reports._framework.catalog.get_report_catalog") as mock_catalog,
    ):
        mock_catalog.return_value.execute.return_value = noted
        result = _runner_cli.invoke(app, ["balance-drift", "--top", "5"])

    assert result.exit_code == 0, result.output
    assert "no stored EUR->USD rates at all" in result.stderr
    assert "more exist" in result.stderr
    assert "💡" in result.stderr
    # The other half: routing the notes away must not take the answer with them.
    assert "****2222" in result.stdout
    assert "⚠️" not in result.stdout
    assert "💡" not in result.stdout


def test_the_text_path_stays_silent_when_no_drift_occurred() -> None:
    """R4's other half: the note must not fire on the clean path.

    A warning printed beside every ordinary table is a warning nobody reads, so
    the marker's absence here is what gives it meaning above.
    """
    app = _multi_command_app()
    with (
        patch(
            "moneybin.reports._framework.cli_register.get_database",
            return_value=no_profile_database(),
        ),
        patch("moneybin.reports._framework.catalog.get_report_catalog") as mock_catalog,
    ):
        mock_catalog.return_value.execute.return_value = _result()
        result = _runner_cli.invoke(app, ["balance-drift", "--top", "5"])

    assert result.exit_code == 0, result.output
    assert "⚠️" not in result.output


def test_cli_command_json_output_emits_envelope() -> None:
    app = _multi_command_app()
    with (
        patch(
            "moneybin.reports._framework.cli_register.get_database",
            return_value=no_profile_database(),
        ),
        patch("moneybin.reports._framework.catalog.get_report_catalog") as mock_catalog,
    ):
        mock_catalog.return_value.execute.return_value = _result()
        result = _runner_cli.invoke(
            app, ["balance-drift", "--top", "5", "--output", "json"]
        )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["data"] == [{"account_id": "****2222", "txn_count": 2}]
    assert payload["summary"]["sensitivity"] == "critical"


def test_cli_command_passes_classes_returned_to_audit() -> None:
    # Bare-list payload + lineage-derived classes: classes_returned must reach
    # render_or_json so the privacy.log audit event records the real data
    # classes instead of an empty set (the `sql query` contract).
    app = _multi_command_app()
    with (
        patch(
            "moneybin.reports._framework.cli_register.get_database",
            return_value=no_profile_database(),
        ),
        patch("moneybin.reports._framework.catalog.get_report_catalog") as mock_catalog,
        patch("moneybin.reports._framework.cli_register.render_or_json") as mock_render,
    ):
        mock_catalog.return_value.execute.return_value = _result()
        result = _runner_cli.invoke(
            app, ["balance-drift", "--top", "5", "--output", "json"]
        )
    assert result.exit_code == 0, result.output
    assert mock_render.call_args.kwargs["classes_returned"] == [
        "account_identifier",
        "aggregate",
    ]


def test_cli_command_value_error_emits_json_error_envelope() -> None:
    # A runner ValueError (bad enum value) under --output json must flow through
    # the shared classified-error path (handle_cli_errors → INFRA_INVALID_INPUT)
    # and emit a JSON error envelope — NOT a plain-text typer.BadParameter that
    # bypasses the envelope and exits 2, breaking the JSON contract for agents.
    app = _multi_command_app()
    with (
        patch(
            "moneybin.reports._framework.cli_register.get_database",
            return_value=no_profile_database(),
        ),
        patch("moneybin.reports._framework.catalog.get_report_catalog") as mock_catalog,
    ):
        mock_catalog.return_value.execute.side_effect = ValueError(
            "Unknown status: bogus"
        )
        result = _runner_cli.invoke(app, ["balance-drift", "--output", "json"])
    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert payload["error"]["code"] == error_codes.INFRA_INVALID_INPUT
    assert "Unknown status: bogus" in payload["error"]["message"]


def test_cli_command_executes_stable_report_id_through_catalog() -> None:
    app = _multi_command_app()
    database_context = no_profile_database()
    database = database_context.__enter__.return_value
    with (
        patch(
            "moneybin.reports._framework.cli_register.get_database",
            return_value=database_context,
        ),
        patch("moneybin.reports._framework.catalog.get_report_catalog") as mock_catalog,
    ):
        mock_catalog.return_value.execute.return_value = _result()
        result = _runner_cli.invoke(
            app, ["balance-drift", "--top", "5", "--output", "json"]
        )

    assert result.exit_code == 0, result.output
    mock_catalog.return_value.execute.assert_called_once_with(
        database,
        report_id="test:balance_drift",
        parameters={"top": 5},
        limit=1_000_000,
        display_currency=None,
        home_currency=None,
    )


def _money_runner(db: Database) -> ReportQuery:
    """Amounts in two kinds.

    Args:
        db: Open read-only database connection.
    """
    return ReportQuery("SELECT 1", [])


_MONEY_COLUMNS = (
    OutputColumn("net", "A signed flow.", DataClass.TXN_AMOUNT, money_kind="flow"),
    OutputColumn(
        "spend",
        "A positive absolute outflow.",
        DataClass.TXN_AMOUNT,
        money_kind="magnitude",
    ),
    OutputColumn("txn_count", "How many rows.", DataClass.AGGREGATE),
)


def _money_spec():  # noqa: ANN202 — test helper
    return build_spec(
        _money_runner,
        report_id="test:money",
        name="money",
        view=_VIEW,
        classes={
            "net": DataClass.TXN_AMOUNT,
            "spend": DataClass.TXN_AMOUNT,
            "txn_count": DataClass.AGGREGATE,
        },
        parameter_classes={},
        columns=_MONEY_COLUMNS,
        semantics=TEST_SEMANTICS,
    )


def _money_app():  # noqa: ANN202 — test helper
    app = typer.Typer()
    register_report_cli(_money_spec(), app)
    app.command("noop")(lambda: None)
    return app


def _money_result() -> ReportResult:
    return ReportResult(
        records=[
            {
                "net": Decimal("-1234.5"),
                "spend": Decimal("1234.5"),
                "txn_count": 2,
            }
        ],
        columns=["net", "spend", "txn_count"],
        output_classes={
            "net": DataClass.TXN_AMOUNT,
            "spend": DataClass.TXN_AMOUNT,
            "txn_count": DataClass.AGGREGATE,
        },
        tier=Tier.MEDIUM,
        total_count=1,
        truncated=False,
    )


def test_a_reports_declared_money_kind_reaches_its_rendered_table() -> None:
    """The generated command is the only path carrying requirement 12.

    Every built-in report but ``networth`` renders through this command, and
    the kinds they declare are inert unless ``money_columns`` is wired into
    ``render_report_result``. Asserting the rendered string rather than the
    dict is what makes that wiring load-bearing: a ``money_columns`` returning
    ``{}`` leaves every column reaching the table through ``str()``, which no
    other test in the tree would notice.
    """
    with (
        patch(
            "moneybin.reports._framework.cli_register.get_database",
            return_value=no_profile_database(),
        ),
        patch("moneybin.reports._framework.catalog.get_report_catalog") as mock_catalog,
    ):
        mock_catalog.return_value.execute.return_value = _money_result()
        result = _runner_cli.invoke(_money_app(), ["money"], env={"COLUMNS": "200"})

    assert result.exit_code == 0, result.output
    # `flow` is signed with U+2212, separated, and carried to two places —
    # `str(Decimal("-1234.5"))` is "-1234.5" on all three counts.
    assert "\u22121,234.50" in result.output
    # `magnitude` is the same number unsigned: its direction is the column's,
    # not the value's, so a `+` here would read as income.
    assert "+1,234.50" not in result.output
    assert "1,234.50" in result.output


def test_every_declared_money_column_survives_spec_registration() -> None:
    """A declaration that never reaches `money_columns` is decoration.

    The kinds are declared on `OutputColumn` in the report definitions, but
    `build_spec` rebuilds the spec from the runner's signature, so a field it
    failed to carry would drop every declaration silently.
    """
    declared = money_columns(_money_spec())

    assert declared == {
        "net": Money("flow"),
        "spend": Money("magnitude"),
    }, "txn_count declares no kind and must not appear"


def _noisy_result() -> ReportResult:
    """A result carrying both a next-step hint and a fidelity warning."""
    return ReportResult(
        records=[
            {"net": Decimal("-1234.5"), "spend": Decimal("1234.5"), "txn_count": 2}
        ],
        columns=["net", "spend", "txn_count"],
        output_classes={
            "net": DataClass.TXN_AMOUNT,
            "spend": DataClass.TXN_AMOUNT,
            "txn_count": DataClass.AGGREGATE,
        },
        tier=Tier.MEDIUM,
        total_count=1,
        truncated=True,
        actions=["Run 'moneybin reports explain test:money' for the SQL"],
    )


def _invoke_money(*args: str) -> Result:
    """Run the generated command over a result that emits both note kinds."""
    with (
        patch(
            "moneybin.reports._framework.cli_register.get_database",
            return_value=no_profile_database(),
        ),
        patch("moneybin.reports._framework.catalog.get_report_catalog") as mock_catalog,
    ):
        mock_catalog.return_value.execute.return_value = _noisy_result()
        return _runner_cli.invoke(
            _money_app(), ["money", *args], env={"COLUMNS": "200"}
        )


def test_quiet_suppresses_a_reports_next_step_hint() -> None:
    """Requirement 4: a `render_note` status line is what `-q` silences.

    The generated command advertises `--quiet` in its own signature, so a hint
    that survives it is the flag not working rather than a design choice. The
    non-quiet half is asserted beside it because a hint that never renders at
    all would satisfy the suppression assertion on its own.
    """
    loud = _invoke_money()
    quiet = _invoke_money("--quiet")

    assert loud.exit_code == 0, loud.output
    assert quiet.exit_code == 0, quiet.output
    assert "reports explain test:money" in loud.output
    assert "reports explain test:money" not in quiet.output


def test_quiet_does_not_suppress_the_truncation_warning() -> None:
    """`-q` drops the chatter, never the statement that rows are missing.

    `moneybin reports <x> -q > out.txt` would otherwise capture a capped table
    that reads as the whole answer — the silent truncation requirement 10
    forbids, arriving through the quiet flag instead of through the stream
    split. Nothing about the rows themselves looks unusual, which is what makes
    it worse than a masked cell.
    """
    quiet = _invoke_money("--quiet")

    assert quiet.exit_code == 0, quiet.output
    assert "more exist" in quiet.output
    # The rows are data and are never suppressed either (requirement 5).
    assert "−1,234.50" in quiet.output


# --- Default columns and --wide (requirements 6-10) ---

_WIDE_CLASSES = {
    "year_month": DataClass.TXN_DATE,
    "account_id": DataClass.RECORD_ID,
    "account_name": DataClass.USER_NOTE,
    "category": DataClass.CATEGORY,
    "currency_code": DataClass.CURRENCY,
    "net": DataClass.TXN_AMOUNT,
    "txn_count": DataClass.AGGREGATE,
}
_WIDE_COLUMN_NAMES = tuple(_WIDE_CLASSES)


def _wide_runner(db: Database, *, by: str = "account-and-category") -> ReportQuery:
    """Grouped rollup.

    Args:
        db: Open read-only database connection.
        by: Grouping dimension.
    """
    return ReportQuery("SELECT 1", [])


def _wide_spec(  # noqa: ANN202 — test helper
    default_columns: DefaultColumns | None = None,
):
    return build_spec(
        _wide_runner,
        report_id="test:wide",
        name="wide",
        view=_VIEW,
        classes=_WIDE_CLASSES,
        parameter_classes={"by": DataClass.TXN_TYPE},
        columns=output_columns(_WIDE_CLASSES),
        semantics=TEST_SEMANTICS,
        default_columns=default_columns,
    )


def test_wide_renders_every_column_the_result_carries() -> None:
    """Requirement 7: `--wide` is the escape hatch back to the full projection."""
    visible = visible_columns(
        _wide_spec(("year_month", "net")),
        _WIDE_COLUMN_NAMES,
        parameters={},
        wide=True,
    )

    assert visible == _WIDE_COLUMN_NAMES


def test_a_static_default_set_narrows_the_result_to_its_own_columns() -> None:
    """Requirement 6: the report declares what a text reader sees first."""
    visible = visible_columns(
        _wide_spec(("year_month", "category", "net")),
        _WIDE_COLUMN_NAMES,
        parameters={},
        wide=False,
    )

    assert visible == ("year_month", "category", "net")


def test_the_declared_order_wins_over_the_projections() -> None:
    """The declaration is a display decision, so it orders the table.

    An author putting the identifying column first must not have to reorder the
    SQL projection — which `--wide`, `--output json`, and every MCP caller also
    read — to do it.
    """
    visible = visible_columns(
        _wide_spec(("net", "year_month")),
        _WIDE_COLUMN_NAMES,
        parameters={},
        wide=False,
    )

    assert visible == ("net", "year_month")


def test_a_parameter_aware_default_set_reads_the_effective_parameters() -> None:
    """Requirement 6: `cash_flow` selects its columns from `by`.

    Any single tuple would name a field one grouping does not return or drop a
    dimension another one does, so the declaration is a callable of the
    parameters the report was actually run with.
    """

    def _by_grouping(parameters: Mapping[str, Any]) -> tuple[str, ...]:
        if parameters.get("by") == "account":
            return ("year_month", "account_name")
        return ("year_month", "category")

    spec = _wide_spec(_by_grouping)

    assert visible_columns(
        spec, _WIDE_COLUMN_NAMES, parameters={"by": "account"}, wide=False
    ) == ("year_month", "account_name")
    assert visible_columns(
        spec, _WIDE_COLUMN_NAMES, parameters={"by": "category"}, wide=False
    ) == ("year_month", "category")


def test_a_parameter_aware_default_survives_an_omitted_parameter() -> None:
    """The callable takes the mapping, so an unpassed option is not a crash.

    An MCP caller supplies only the parameters it wants; a callable taking
    keyword arguments would raise `TypeError` on the rest and take down a
    report that runs fine on every other surface.
    """

    def _by_grouping(parameters: Mapping[str, Any]) -> tuple[str, ...]:
        if parameters.get("by") == "account":
            return ("year_month",)
        return ("category",)

    spec = _wide_spec(_by_grouping)

    assert visible_columns(spec, _WIDE_COLUMN_NAMES, parameters={}, wide=False) == (
        "category",
    )


def test_a_default_column_absent_from_this_projection_is_dropped() -> None:
    """A grouping that returns no `category` must not render an empty column.

    The result's own columns are the authority on what exists; the declaration
    only chooses among them.
    """
    visible = visible_columns(
        _wide_spec(("year_month", "category", "net")),
        ("year_month", "net", "txn_count"),
        parameters={},
        wide=False,
    )

    assert visible == ("year_month", "net")


def test_an_undeclared_report_falls_back_to_its_first_six_columns() -> None:
    """Requirement 6: an extension that declares nothing still gets a cap.

    Six is a fixed count rather than a computed fit — `OutputColumn` carries no
    display width, so "the columns that fit 80" is not answerable without
    measuring runtime values, which would make an extension's column set vary
    with its data.
    """
    visible = visible_columns(
        _wide_spec(), _WIDE_COLUMN_NAMES, parameters={}, wide=False
    )

    assert visible == _WIDE_COLUMN_NAMES[:6]
    assert "txn_count" not in visible


def test_a_default_set_matching_nothing_renders_the_whole_result() -> None:
    """Failing open beats rendering a table with no columns.

    Requirement 6 makes an unresolvable default set a spec violation, caught by
    the width contract test rather than at the user's terminal. If one reaches
    a real run anyway, the whole result is the honest fallback — an empty table
    under `0 of 7 columns shown` reads as a report that returned nothing.
    """

    def _unresolvable(parameters: Mapping[str, Any]) -> tuple[str, ...]:
        return ("nonexistent",)

    visible = visible_columns(
        _wide_spec(_unresolvable),
        _WIDE_COLUMN_NAMES,
        parameters={},
        wide=False,
    )

    assert visible == _WIDE_COLUMN_NAMES


def _invoke_wide(*args: str) -> Result:
    """Run a generated command whose default set is narrower than its result."""
    app = typer.Typer()
    register_report_cli(_wide_spec(("year_month", "net")), app)
    app.command("noop")(lambda: None)
    result = ReportResult(
        records=[dict.fromkeys(_WIDE_COLUMN_NAMES, "x")],
        columns=list(_WIDE_COLUMN_NAMES),
        output_classes=dict(_WIDE_CLASSES),
        tier=Tier.MEDIUM,
        total_count=1,
        truncated=False,
    )
    with (
        patch(
            "moneybin.reports._framework.cli_register.get_database",
            return_value=no_profile_database(),
        ),
        patch("moneybin.reports._framework.catalog.get_report_catalog") as mock_catalog,
    ):
        mock_catalog.return_value.execute.return_value = result
        return _runner_cli.invoke(app, ["wide", *args], env={"COLUMNS": "200"})


def test_the_generated_command_renders_only_its_default_columns() -> None:
    """Requirement 6, end to end through the Typer command."""
    result = _invoke_wide()

    assert result.exit_code == 0, result.output
    assert "year_month" in result.output
    assert "txn_count" not in result.output


def test_the_generated_command_frames_the_columns_it_omitted() -> None:
    """Requirement 10: the omission is disclosed, never silent."""
    result = _invoke_wide()

    assert "2 of 7 columns shown — --wide for all" in result.output


def test_wide_restores_the_full_projection_on_the_generated_command() -> None:
    """Requirement 7, and the framing line has nothing left to disclose."""
    result = _invoke_wide("--wide")

    assert result.exit_code == 0, result.output
    assert "txn_count" in result.output
    assert "columns shown" not in result.output


def test_json_output_is_unaffected_by_the_default_column_set() -> None:
    """Requirement 8: the column policy is a text-rendering decision only.

    An agent reading `--output json` gets the full projection whether or not a
    human running the same report would see every column, because the two
    surfaces answer different questions and `--json-fields` is the JSON
    caller's own filter.
    """
    result = _invoke_wide("--output", "json")

    assert result.exit_code == 0, result.output
    assert "txn_count" in json.loads(result.output)["data"][0]


def test_json_output_is_unaffected_by_wide() -> None:
    """Requirement 8: `--wide` widens the table, and nothing else."""
    result = _invoke_wide("--wide", "--output", "json")

    assert result.exit_code == 0, result.output
    assert "columns shown" not in result.output
