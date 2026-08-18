"""Tests for the dynamic Typer CLI registrar."""

from __future__ import annotations

import inspect
import json
from dataclasses import replace
from unittest.mock import MagicMock, patch

import typer
from typer.testing import CliRunner

from moneybin import error_codes
from moneybin.cli.output import OutputFormat
from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.reports._framework.cli_register import (
    build_cli_command,
    register_report_cli,
)
from moneybin.reports._framework.contract import ReportQuery
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
