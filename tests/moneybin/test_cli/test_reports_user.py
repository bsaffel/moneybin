"""CLI coverage for the tier-spanning catalog/runner and lifecycle subcommands.

Business logic lives in ``UserReportsService`` and is tested against a real
database in ``tests/moneybin/test_reports/test_user_reports.py``. These tests own
what only the CLI decides: flag grammar, exit codes, prompts, and the arguments
that reach the service.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.reports._framework.execute import ReportResult
from moneybin.repositories.user_reports_repo import UNSET
from moneybin.services.user_reports_service import ReclassifyOutcome, SaveOutcome

runner = CliRunner()

_ROW: dict[str, Any] = {"report_id": "user:rab12cd34ef56", "name": "my_accounts"}


def _database() -> MagicMock:
    """A ``get_database`` stand-in whose context manager yields a mock."""
    context = MagicMock()
    context.__enter__.return_value = MagicMock()
    return context


def _service(**attributes: Any) -> MagicMock:
    service = MagicMock()
    service.resolve.return_value = _ROW
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
        return_value=_database(),
    )


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


def test_list_asks_the_catalog_for_archived_reports_when_widened() -> None:
    catalog = MagicMock()
    with (
        _patch_database(),
        patch(
            "moneybin.reports._framework.catalog.get_report_catalog",
            return_value=catalog,
        ) as get_catalog,
        patch(
            "moneybin.reports._framework.catalog.catalog_to_payload",
            return_value=MagicMock(reports=[]),
        ),
        patch(
            "moneybin.reports._framework.catalog.catalog_sensitivity",
            return_value="low",
        ),
    ):
        result = runner.invoke(app, ["reports", "list", "--archived"])

    assert result.exit_code == 0, result.output
    assert get_catalog.call_args.kwargs == {"include_archived": True}


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


def test_set_archives_by_clearing_is_active() -> None:
    service = _service(update=_save_outcome())

    with _patch_database(), _patch_service(service):
        result = runner.invoke(app, ["reports", "set", "my_accounts", "--archive"])

    assert result.exit_code == 0, result.output
    assert service.update.call_args.kwargs["is_active"] is False


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
    service = _service()

    with _patch_database(), _patch_service(service):
        result = runner.invoke(app, ["reports", "delete", "my_accounts"], input="n\n")

    assert result.exit_code == 1
    service.delete.assert_not_called()


def test_delete_proceeds_without_a_prompt_under_yes() -> None:
    service = _service()

    with _patch_database(), _patch_service(service):
        result = runner.invoke(app, ["reports", "delete", "my_accounts", "--yes"])

    assert result.exit_code == 0, result.output
    service.delete.assert_called_once_with(_ROW["report_id"], actor="cli")


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


def test_reclassify_treats_yes_as_the_confirmation() -> None:
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
    assert "txn_amount" in result.output


def test_reclassify_help_states_the_confirmation_is_a_human_decision() -> None:
    """An agent reads ``--help``, never the design rule that forbids self-accept."""
    result = runner.invoke(app, ["reports", "reclassify", "--help"])

    assert result.exit_code == 0, result.output
    assert "human decision" in _flatten(result.output)


def _flatten(output: str) -> str:
    """Join Typer's wrapped help text so a phrase can be matched across lines."""
    return " ".join(output.split())


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
