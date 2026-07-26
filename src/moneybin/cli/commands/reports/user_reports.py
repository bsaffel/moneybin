"""Catalog, runner, and lifecycle subcommands spanning all three report tiers.

R5 of ``docs/specs/reports-dynamic.md``. ``list`` and ``run`` are the CLI twins
of the shipped ``reports`` MCP catalog/runner and serve every tier;
``create`` / ``set`` / ``delete`` / ``reclassify`` are the lifecycle capability,
which owns only the user tier because a built-in is a file in the repo.

These are CLI-only by design: no MCP identity is named for a lifecycle verb.
Each is a thin wrapper over :class:`UserReportsService`.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import handle_cli_errors, render_rich_table
from moneybin.database import get_database
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope

logger = logging.getLogger(__name__)

# The CLI is an operator/agent surface, so the framing cap is effectively off;
# a report's own LIMIT parameters are what bound its result.
_CLI_MAX_ROWS = 1_000_000

_PARAM_BIND_HELP = (
    "Parameter value as key=value; repeat for multiple values. "
    "Values are coerced to the report's declared type."
)
_PARAM_DECLARE_HELP = (
    "Parameter declaration as name[:type][=default]; repeat for multiple. "
    "Types: str (default), int, float, bool, date, decimal. "
    "A parameter is required unless it declares a default."
)


def reports_list(
    archived: bool = typer.Option(
        False,
        "--archived",
        help="Show archived reports instead of the active catalog.",
    ),
    tier: str | None = typer.Option(
        None,
        "--tier",
        help="Show one tier only: builtin, extension, or user.",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """List every registered report — built-in, extension, and saved."""
    from moneybin.reports._framework.catalog import (
        catalog_sensitivity,
        catalog_to_payload,
        get_report_catalog,
    )

    if tier is not None and tier not in ("builtin", "extension", "user"):
        raise typer.BadParameter(
            "tier must be builtin, extension, or user", param_hint="--tier"
        )

    with handle_cli_errors(cli_actor="reports_list"):
        with get_database(read_only=True) as db:
            catalog = get_report_catalog(db, include_archived=archived)
            payload = catalog_to_payload(catalog)
            sensitivity = catalog_sensitivity(catalog)

    entries = [
        entry
        for entry in payload.reports
        # `--archived` is an archived-only view rather than a widened one: the
        # catalog entry carries no archived flag, so a combined listing could not
        # say which rows were archived without adding one to a payload every
        # tier shares.
        if (tier is None or entry.tier == tier)
        and (entry.tier == "user" or not archived)
    ]

    def _render_text(_: ResponseEnvelope[Any]) -> None:
        if not entries:
            if not quiet:
                logger.info("No reports match.")
            return
        render_rich_table(
            ["report_id", "tier", "parameters", "description"],
            [
                (
                    entry.report_id,
                    entry.tier,
                    ", ".join(sorted(entry.parameter_classes)) or "-",
                    entry.description,
                )
                for entry in entries
            ],
        )

    render_or_json(
        build_envelope(
            data=[entry.model_dump(mode="json") for entry in entries],
            sensitivity=sensitivity,
            total_count=len(entries),
        ),
        output,
        render_fn=_render_text,
        cli_actor="reports_list",
    )


def reports_run(
    handle: str = typer.Argument(..., help="Report ID or name, any tier."),
    param: list[str] | None = typer.Option(None, "--param", help=_PARAM_BIND_HELP),
    limit: int | None = typer.Option(
        None, "--limit", help="Maximum rows to return. Default: unbounded."
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001  # result rows are data, never suppressed
) -> None:
    """Run one registered report by ID or name."""
    from moneybin.cli.report_params import parse_report_parameters
    from moneybin.reports._framework.catalog import get_report_catalog
    from moneybin.reports._framework.cli_register import render_report_result

    with handle_cli_errors(cli_actor="reports_run"):
        with get_database(read_only=True) as db:
            catalog = get_report_catalog(db)
            parameters = parse_report_parameters(catalog, handle, param)
            result = catalog.execute(
                db,
                report_id=handle,
                parameters=parameters,
                limit=_CLI_MAX_ROWS if limit is None else limit,
            )
    render_report_result(result, output, cli_actor="reports_run")


def reports_create(
    name: str = typer.Argument(..., help="Report name: lowercase slug, unique."),
    sql: str | None = typer.Option(
        None, "--sql", help="The report's read-only SELECT."
    ),
    sql_file: Path | None = typer.Option(
        None, "--sql-file", help="Read the SELECT from a file instead of --sql."
    ),
    description: str | None = typer.Option(
        None, "--description", help="What the report answers."
    ),
    param: list[str] | None = typer.Option(None, "--param", help=_PARAM_DECLARE_HELP),
    output: OutputFormat = output_option,
) -> None:
    """Save a query as a durable report.

    Classification is derived from the SQL and stored; you never declare it.
    """
    from moneybin.cli.report_params import parse_parameter_declaration
    from moneybin.services.user_reports_service import UserReportsService

    query_sql = _query_sql(sql, sql_file)

    # Declaration parsing sits *inside* the handler: an unsupported type raises
    # UserError, and outside it that reaches the user as a bare traceback with no
    # message and no JSON envelope.
    with handle_cli_errors(cli_actor="reports_create"):
        params = [parse_parameter_declaration(raw) for raw in param or []]
        with get_database(read_only=False) as db:
            outcome = UserReportsService(db).create(
                name=name,
                query_sql=query_sql,
                description=description,
                params=params,
                actor="cli",
            )

    _render_save(outcome, output, cli_actor="reports_create", verb="Saved")


def reports_set(
    handle: str = typer.Argument(..., help="Report ID or name of a saved report."),
    name: str | None = typer.Option(None, "--name", help="Rename the report."),
    description: str | None = typer.Option(
        None, "--description", help="Replace the description."
    ),
    sql: str | None = typer.Option(None, "--sql", help="Replace the SELECT."),
    sql_file: Path | None = typer.Option(
        None, "--sql-file", help="Read the replacement SELECT from a file."
    ),
    param: list[str] | None = typer.Option(
        None,
        "--param",
        help=f"Replace the whole declared parameter list. {_PARAM_DECLARE_HELP}",
    ),
    archive: bool = typer.Option(
        False, "--archive", help="Hide the report from the default catalog."
    ),
    restore: bool = typer.Option(
        False, "--restore", help="Return an archived report to the catalog."
    ),
    output: OutputFormat = output_option,
) -> None:
    """Update one saved report: rename, re-describe, re-query, archive, restore.

    Changing the SQL or the parameters re-derives the privacy contract and drops
    any approved classification downgrade — an approval covers one column of one
    query, not the name it was filed under.
    """
    from moneybin.cli.report_params import parse_parameter_declaration
    from moneybin.repositories.user_reports_repo import UNSET
    from moneybin.services.user_reports_service import UserReportsService

    if archive and restore:
        raise typer.BadParameter(
            "--archive and --restore are opposites", param_hint="--archive"
        )
    query_sql = _query_sql(sql, sql_file) if (sql or sql_file) else None
    fields: dict[str, Any] = {
        "name": UNSET if name is None else name,
        "description": UNSET if description is None else description,
        "query_sql": UNSET if query_sql is None else query_sql,
        "params": UNSET if param is None else param,
        "is_active": False if archive else (True if restore else UNSET),
    }
    if all(value is UNSET for value in fields.values()):
        raise typer.BadParameter("nothing to change", param_hint="--name")

    # Declaration parsing sits inside the handler so an unsupported type reaches
    # the user as a message rather than a traceback.
    with handle_cli_errors(cli_actor="reports_set"):
        if param is not None:
            fields["params"] = [parse_parameter_declaration(raw) for raw in param]
        with get_database(read_only=False) as db:
            outcome = UserReportsService(db).update(handle, actor="cli", **fields)

    _render_save(outcome, output, cli_actor="reports_set", verb="Updated")


def reports_delete(
    handle: str = typer.Argument(..., help="Report ID or name of a saved report."),
    yes: bool = typer.Option(
        False, "--yes", "-y", help="Delete without the confirmation prompt."
    ),
    output: OutputFormat = output_option,
) -> None:
    """Delete one saved report permanently.

    The audit log keeps the full prior row, so `moneybin system audit undo`
    restores it. To hide a report without deleting it, use `--archive` on
    `reports set`.
    """
    from moneybin.services.user_reports_service import UserReportsService

    with handle_cli_errors(cli_actor="reports_delete"):
        with get_database(read_only=False) as db:
            service = UserReportsService(db)
            row = service.resolve(handle)
            report_id = str(row["report_id"])
            if not yes and not typer.confirm(
                f"Delete saved report {row['name']} ({report_id})?", err=True
            ):
                typer.echo("Delete cancelled.", err=True)
                raise typer.Exit(1)
            service.delete(report_id, actor="cli")

    def _render_text(_: ResponseEnvelope[Any]) -> None:
        typer.echo(f"✅ Deleted {row['name']} ({report_id})")

    render_or_json(
        build_envelope(
            data={"report_id": report_id, "state": "absent"},
            sensitivity="low",
        ),
        output,
        render_fn=_render_text,
        cli_actor="reports_delete",
    )


def reports_reclassify(
    handle: str = typer.Argument(..., help="Report ID or name of a saved report."),
    column: str = typer.Option(..., "--column", help="Output column to downgrade."),
    to: str = typer.Option(..., "--to", help="The lower privacy class to apply."),
    reason: str = typer.Option(
        ..., "--reason", help="Why this column reveals less than its derived class."
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help=(
            "Confirm the downgrade without the prompt. This is a human decision: "
            "an assistant driving this command must not supply it unasked."
        ),
    ),
    output: OutputFormat = output_option,
) -> None:
    """Lower one column's masking floor, permanently, for this report.

    Derivation over-classifies computed columns — a z-score derives as an amount
    and masks. This is the escape hatch, and the only path that durably lowers
    what is masked, so it requires explicit confirmation and is audited. The
    downgrade must drop the sensitivity tier: a same-tier weakening (whole
    masking to partial) is refused whatever the reason.
    """
    from moneybin.privacy.taxonomy import DataClass
    from moneybin.services.user_reports_service import UserReportsService

    try:
        to_class = DataClass(to)
    except ValueError as e:
        raise typer.BadParameter(
            f"unknown privacy class {to!r}; one of: "
            f"{', '.join(sorted(item.value for item in DataClass))}",
            param_hint="--to",
        ) from e

    with handle_cli_errors(cli_actor="reports_reclassify"):
        with get_database(read_only=False) as db:
            service = UserReportsService(db)
            row = service.resolve(handle)
            confirmed = yes or typer.confirm(
                f"Permanently lower masking of {column!r} to {to_class.value} for "
                f"{row['name']}, on every future run?",
                err=True,
            )
            outcome = service.reclassify(
                str(row["report_id"]),
                column=column,
                to_class=to_class,
                reason=reason,
                confirmed=confirmed,
                actor="cli",
            )

    def _render_text(_: ResponseEnvelope[Any]) -> None:
        typer.echo(
            f"✅ {outcome.column} on {row['name']}: "
            f"{outcome.from_class.value} → {outcome.to_class.value}"
        )

    render_or_json(
        build_envelope(
            data={
                "report_id": outcome.report_id,
                "column": outcome.column,
                "from": outcome.from_class.value,
                "to": outcome.to_class.value,
            },
            sensitivity="low",
        ),
        output,
        render_fn=_render_text,
        cli_actor="reports_reclassify",
    )


def _query_sql(sql: str | None, sql_file: Path | None) -> str:
    """Read the report's SELECT from exactly one of the two sources."""
    if (sql is None) == (sql_file is None):
        raise typer.BadParameter(
            "supply the query with either --sql or --sql-file", param_hint="--sql"
        )
    if sql is not None:
        return sql
    assert sql_file is not None  # noqa: S101  # narrowed by the exclusivity check
    try:
        return sql_file.read_text()
    except OSError as e:
        raise typer.BadParameter(
            f"could not read {sql_file}", param_hint="--sql-file"
        ) from e


def _render_save(
    outcome: Any, output: OutputFormat, *, cli_actor: str, verb: str
) -> None:
    """Render one save outcome, including R3's non-blocking notes."""

    def _render_text(_: ResponseEnvelope[Any]) -> None:
        typer.echo(f"✅ {verb} {outcome.name} ({outcome.report_id})")
        if outcome.unresolved_columns:
            logger.warning(
                f"⚠️  Masked — no upstream class could be derived for "
                f"{', '.join(outcome.unresolved_columns)}. "
                "Project the underlying column directly to resolve it."
            )
        if outcome.cleared_downgrades:
            logger.warning(
                f"⚠️  Cleared the approved downgrade for "
                f"{', '.join(outcome.cleared_downgrades)}; the query changed. "
                "Re-apply with `moneybin reports reclassify`."
            )

    render_or_json(
        build_envelope(
            data={
                "report_id": outcome.report_id,
                "name": outcome.name,
                "unresolved_columns": list(outcome.unresolved_columns),
                "cleared_downgrades": list(outcome.cleared_downgrades),
            },
            # `name` is user-authored text, classed USER_NOTE — the same tier the
            # stored column carries, not the AGGREGATE a built-in's name is.
            sensitivity="medium",
        ),
        output,
        render_fn=_render_text,
        cli_actor=cli_actor,
    )
