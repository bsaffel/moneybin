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
from typing import Any, Literal

import click
import typer

from moneybin import error_codes
from moneybin.cli.output import (
    CLI_MAX_ROWS,
    OutputFormat,
    display_currency_option,
    output_option,
    quiet_option,
    render_or_json,
    wide_option,
)
from moneybin.cli.render import render_rows
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.privacy.taxonomy import DataClass
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope

logger = logging.getLogger(__name__)

_PARAM_BIND_HELP = (
    "Parameter value as key=value; repeat for multiple values. "
    "Values are coerced to the report's declared type."
)
_PARAM_DECLARE_HELP = (
    "Parameter declaration as name[:type][=default]; repeat for multiple. "
    "Types: str (default), int, float, bool, date, decimal. "
    "A parameter is required unless it declares a default."
)

# Every lifecycle response names one report by its minted id and its user-authored
# name. Declared once because the four of them return the same two things.
_LIFECYCLE_CLASSES = [DataClass.RECORD_ID.value, DataClass.USER_NOTE.value]


def reports_list(
    include_archived: bool = typer.Option(
        False,
        "--include-archived",
        help="Include archived saved reports, marked as archived in the listing.",
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
        catalog_classes_returned,
        catalog_sensitivity,
        catalog_to_payload,
        open_report_catalog,
    )

    if tier is not None and tier not in ("builtin", "extension", "user"):
        raise typer.BadParameter(
            "tier must be builtin, extension, or user", param_hint="--tier"
        )

    with handle_cli_errors(cli_actor="reports_list"):
        # Widen-and-mark, the same shape as `accounts list --include-archived`:
        # one answer to "show me the hidden ones" across the CLI rather than one
        # per command group. The catalog always spans archived rows so they stay
        # runnable; this decides only what a *listing* shows.
        with open_report_catalog() as (catalog, _):
            payload = catalog_to_payload(catalog, include_archived=include_archived)

    entries = [entry for entry in payload.reports if tier is None or entry.tier == tier]
    # After `--tier`, not before: the envelope has to describe the rows it
    # carries, and `--tier builtin` drops every user-authored name.
    sensitivity = catalog_sensitivity(entries)

    def _render_text(_: ResponseEnvelope[Any]) -> None:
        if not entries:
            if not quiet:
                logger.info("No reports match.")
            return
        render_rows(
            # `name` leads: it is the handle `run`, `explain`, and `export` take,
            # and the only one a user typed. `report_id` stays because it is what
            # survives a rename and what breaks a cross-tier name collision.
            ["name", "report_id", "tier", "parameters", "description"],
            [
                (
                    entry.name,
                    entry.report_id,
                    # The tier column, not a fifth column: archived is a state of
                    # the user tier, and only a widened listing ever shows one.
                    f"{entry.tier} [archived]" if entry.archived else entry.tier,
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
            classes_returned=catalog_classes_returned(sensitivity),
        ),
        output,
        render_fn=_render_text,
        cli_actor="reports_list",
        # A bare-list payload carries no annotations, so the audit event would
        # record no classes at all for a response holding user-authored names.
        classes_returned=catalog_classes_returned(sensitivity),
    )


def reports_run(
    handle: str = typer.Argument(..., help="Report ID or name, any tier."),
    param: list[str] | None = typer.Option(None, "--param", help=_PARAM_BIND_HELP),
    limit: int | None = typer.Option(
        None,
        "--limit",
        # Built from the constant, not restated: a cap the help contradicts is
        # how a truncated financial answer comes to read as a complete one.
        help=f"Maximum rows to return. Default: {CLI_MAX_ROWS:,}.",
    ),
    display_currency: str | None = display_currency_option,
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    wide: bool = wide_option,
) -> None:
    """Run one registered report by ID or name."""
    from moneybin.cli.report_params import parse_report_parameters
    from moneybin.reports._framework.catalog import (
        get_report_catalog,
        profile_home_currency,
    )
    from moneybin.reports._framework.cli_register import (
        column_view,
        money_columns,
        render_report_result,
    )

    # Parity with the `reports` MCP tool, which validates `ge=1`. `--limit 0`
    # otherwise slices to zero rows and reports `truncated: true` with a
    # total_count of 1 — an empty result that claims to have been cut short.
    if limit is not None and limit < 1:
        raise typer.BadParameter("must be at least 1", param_hint="--limit")

    with handle_cli_errors(cli_actor="reports_run"):
        with get_database(read_only=True) as db:
            catalog = get_report_catalog(db)
            parameters = parse_report_parameters(catalog, handle, param)
            # Resolved here rather than inside the renderer: the catalog needs
            # an open database to build a user-tier spec, and this is the only
            # scope that has one. `run` reaches built-ins too, so without it a
            # report would render its amounts one way through `reports spending`
            # and another through `reports run spending`.
            spec = catalog.resolve(handle)
            money = money_columns(spec)
            result = catalog.execute(
                db,
                report_id=handle,
                parameters=parameters,
                limit=CLI_MAX_ROWS if limit is None else limit,
                display_currency=display_currency,
                home_currency=profile_home_currency(db),
            )
            # Resolved inside the database scope for the same reason `money` is:
            # a user-tier spec is built from a row, and this is the only scope
            # holding the connection that builds it.
            view = column_view(spec, result.columns, parameters=parameters, wide=wide)
    render_report_result(
        result,
        output,
        cli_actor="reports_run",
        money=money,
        quiet=quiet,
        columns=view.columns,
        fit=view.fit,
    )


def reports_explain(
    handle: str = typer.Argument(..., help="Report ID or name, any tier."),
    param: list[str] | None = typer.Option(None, "--param", help=_PARAM_BIND_HELP),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001  # the evidence IS the output
) -> None:
    """Show a report's query, class map, lineage, freshness, and portability.

    Runs nothing. The query is returned in two forms: the executed form with
    parameters rendered as literals, and the stored template with placeholders
    intact. A parameter classed above the lowest tier keeps its placeholder in
    the executed form — rendering is not execution, so it never passes through
    the redaction the report's own rows do.
    """
    from moneybin.cli.report_params import coerce_report_parameters
    from moneybin.reports._framework.catalog import get_report_catalog
    from moneybin.reports._framework.explain import explain_spec

    with handle_cli_errors(cli_actor="reports_explain"):
        with get_database(read_only=True) as db:
            # Resolved here and handed to `explain_spec` rather than calling
            # `explain_report`, which would build the catalog a second time —
            # one build derives a spec per saved row.
            catalog = get_report_catalog(db)
            report = catalog.resolve(handle)
            parameters = coerce_report_parameters(report, param)
            explanation = explain_spec(db, report, parameters=parameters)

    def _render_text(_: ResponseEnvelope[Any]) -> None:
        typer.echo(f"{explanation.report_id}  ({explanation.tier})")
        if explanation.description:
            typer.echo(explanation.description)
        render_rows(
            ["column", "class", "origin", "upstream"],
            [
                (
                    column.column,
                    column.data_class.value,
                    column.origin,
                    column.upstream or "-",
                )
                for column in explanation.columns
            ],
        )
        for label, value in (
            ("Reads", ", ".join(explanation.lineage) or "-"),
            ("Graduation", explanation.graduation),
            ("Updated", explanation.updated_at or "-"),
            ("Fingerprint", explanation.class_fingerprint or "-"),
        ):
            typer.echo(f"{label}: {value}")
        for blocker in explanation.graduation_blockers:
            typer.echo(f"  ⚠️  {blocker}")
        # Echoed, not logged. The reason names the columns that moved, and a saved
        # report's aliases are user-authored. No safe record is lost by dropping
        # the log call: `_reresolved` already logs the drift where it is detected,
        # in counts, and this path reaches it through `spec_from_row`.
        if explanation.drift_reason:
            typer.echo(f"  ⚠️  {explanation.drift_reason}")
        if explanation.sql_unavailable:
            typer.echo(f"SQL: {explanation.sql_unavailable}")
        if explanation.withheld_parameters:
            typer.echo(
                "Withheld from the rendered SQL (classed above the lowest tier): "
                f"{', '.join(explanation.withheld_parameters)}"
            )
        if explanation.sql_suppressed_by:
            typer.echo(
                "No executed form — supply a value for "
                f"{', '.join(explanation.sql_suppressed_by)} with --param"
            )
        for label, form in (
            ("SQL", explanation.sql),
            ("Template", explanation.sql_template),
        ):
            if form is not None:
                typer.echo(f"\n{label}:\n{form}")

    render_or_json(
        build_envelope(
            data={
                "report_id": explanation.report_id,
                "name": explanation.name,
                "tier": explanation.tier,
                "sql": explanation.sql,
                "sql_template": explanation.sql_template,
                "sql_unavailable": explanation.sql_unavailable,
                "withheld_parameters": list(explanation.withheld_parameters),
                "sql_suppressed_by": list(explanation.sql_suppressed_by),
                "columns": [
                    {
                        "column": column.column,
                        "data_class": column.data_class.value,
                        "origin": column.origin,
                        "upstream": column.upstream,
                    }
                    for column in explanation.columns
                ],
                "lineage": list(explanation.lineage),
                "class_fingerprint": explanation.class_fingerprint,
                "drift_detected": explanation.drift_detected,
                "drift_reason": explanation.drift_reason,
                "updated_at": explanation.updated_at,
                "graduation": explanation.graduation,
                "graduation_blockers": list(explanation.graduation_blockers),
            },
            sensitivity=explanation.sensitivity,
            classes_returned=_explanation_classes(explanation.sensitivity),
        ),
        output,
        render_fn=_render_text,
        cli_actor="reports_explain",
        # The name, description, and verbatim SQL of a *saved* report are
        # user-authored text; a bare-dict payload declares nothing on its own.
        classes_returned=_explanation_classes(explanation.sensitivity),
    )


def _explanation_classes(sensitivity: Literal["low", "medium"]) -> list[str]:
    """The classes an explanation's own prose carries, by the tier it reports."""
    from moneybin.reports._framework.catalog import catalog_classes_returned

    return catalog_classes_returned(sensitivity)


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

    # Reading the file and parsing the declarations both sit *inside* the handler:
    # each raises for a reason the user caused, and outside it that reaches them as
    # a bare traceback or a usage error with no JSON envelope.
    with handle_cli_errors(cli_actor="reports_create"):
        query_sql = _query_sql(sql, sql_file)
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
    clear_params: bool = typer.Option(
        False,
        "--clear-params",
        help="Drop every declared parameter (for SQL with no placeholders left).",
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

    Changing the SQL or the parameters re-derives the privacy contract; only new
    SQL drops an approved classification downgrade. An approval covers one column
    of one query, so a rewrite voids it while a re-declared parameter leaves both
    unchanged — and an approval whose column now derives a different class stops
    applying on its own.
    """
    from moneybin.cli.report_params import parse_parameter_declaration
    from moneybin.repositories.user_reports_repo import UNSET
    from moneybin.services.user_reports_service import UserReportsService

    if archive and restore:
        raise typer.BadParameter(
            "--archive and --restore are opposites", param_hint="--archive"
        )
    if clear_params and param:
        raise typer.BadParameter(
            "--clear-params and --param are opposites", param_hint="--clear-params"
        )
    # Reading the file and parsing the declarations both sit inside the handler so
    # a failure the user caused reaches them as a message — or a JSON envelope —
    # rather than a traceback or a usage error. The two flag conflicts above stay
    # outside it: those really are usage errors, and exit 2 is theirs.
    with handle_cli_errors(cli_actor="reports_set"):
        # `is not None`, not truthiness: `--sql ""` must reach the service and be
        # refused as an invalid query, not be silently dropped from the update.
        query_sql = (
            _query_sql(sql, sql_file)
            if (sql is not None or sql_file is not None)
            else None
        )
        fields: dict[str, Any] = {
            "name": UNSET if name is None else name,
            "description": UNSET if description is None else description,
            "query_sql": UNSET if query_sql is None else query_sql,
            # `--clear-params` is the only way to say "no declarations": every
            # `--param` occurrence requires a value, so an omitted option can only
            # mean UNSET, and derivation refuses a declaration the new SQL no
            # longer interpolates — leaving an otherwise-valid update unspellable.
            "params": [] if clear_params else (UNSET if param is None else param),
            "is_active": False if archive else (True if restore else UNSET),
        }
        if all(value is UNSET for value in fields.values()):
            raise typer.BadParameter("nothing to change", param_hint="--name")
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
        # Resolve on a read-only connection and prompt with it closed. A write
        # connection holds the exclusive DuckDB writer lock for its whole
        # lifetime, so confirming inside one blocks every other writer for an
        # unbounded human wait — every sibling confirm in the CLI prompts first.
        with get_database(read_only=True) as db:
            row = UserReportsService(db).resolve(handle)
        report_id = str(row["report_id"])
        if not yes and not _confirm_delete(row["name"], report_id):
            typer.echo("Delete cancelled.", err=True)
            # Exit 0: declining a confirmation is the requested outcome, not a
            # failure. `cli.md` reserves 1 for "operation ran and failed", and
            # every sibling destructive confirm exits 0 on decline, so a script
            # testing `$?` would read a decline here as an error nowhere else.
            raise typer.Exit(0)
        with get_database(read_only=False) as db:
            # By `report_id`, and the service re-resolves it: the row may have
            # been deleted while the prompt was open, and a stale name must not
            # be what decides which report goes.
            UserReportsService(db).delete(report_id, actor="cli")

    def _render_text(_: ResponseEnvelope[Any]) -> None:
        typer.echo(f"✅ Deleted {row['name']} ({report_id})")

    render_or_json(
        build_envelope(
            data={"report_id": report_id, "state": "absent"},
            sensitivity="medium",
            classes_returned=_LIFECYCLE_CLASSES,
        ),
        output,
        render_fn=_render_text,
        cli_actor="reports_delete",
        # `report_id` is an opaque minted id; the report's name reaches the text
        # render, and a bare-dict payload declares neither on its own.
        classes_returned=_LIFECYCLE_CLASSES,
    )


def _confirm_delete(name: object, report_id: str) -> bool:
    """Ask before a permanent delete, failing loudly when nobody can be asked.

    ``typer.confirm`` raises ``click.Abort`` on EOF, which is what a piped or
    non-TTY invocation without ``--yes`` produces. ``classify_user_error`` does
    not recognize ``Abort``, so letting it escape spends the interaction on a bare
    ``Aborted.`` — no error code, and no JSON envelope for a caller that asked for
    one. Raised as a ``UserError`` instead, the same way
    :func:`_prompt_for_downgrade` routes its own unaskable case.

    PATTERN: confirm-abort-envelope — the target shape for every CLI confirm.
    The other 29 `typer.confirm` call sites (31 total across 18 modules; find them
    with `grep -rn "typer.confirm" src/moneybin/cli/commands/`) still let `Abort`
    escape, so a piped invocation without `--yes` gets a bare `Aborted.` there.
    Each is a mechanical change, but 18 modules of unrelated commands do not
    belong in this milestone's diff, so the migration is filed instead.
    """
    try:
        return typer.confirm(f"Delete saved report {name} ({report_id})?", err=True)
    except click.Abort as e:
        raise UserError(
            "Deleting a saved report needs explicit confirmation.",
            code=error_codes.MUTATION_CONFIRMATION_REQUIRED,
            hint="This surface had no way to ask. Re-run with --yes to confirm.",
        ) from e


def _prompt_for_downgrade(
    name: object, column: str, from_class: DataClass, to_class: DataClass
) -> bool | None:
    """Ask the human, or report that this surface could not ask.

    ``typer.confirm`` aborts on EOF, which is what a piped or non-TTY invocation
    without ``--yes`` produces. Letting that abort escape spends the interaction
    on the word "Aborted!": no code, no envelope, no statement of what was
    refused or why. Returning ``None`` routes it through the same refusal every
    other caller gets — and counts it as a surface that could not ask rather
    than as a human who said no.

    ``from_class`` is the class derivation produces *now*, not the stored one.
    Naming only the target class left the answer ambiguous exactly when it
    mattered: an upstream reclassification writes nothing, so the stored
    fingerprint still matches, and a downgrade read as ``txn_amount → aggregate``
    could be ``routing_number → aggregate``.
    """
    try:
        return typer.confirm(
            f"Permanently lower masking of {column!r} from {from_class.value} to "
            f"{to_class.value} for {name}, on every future run?",
            err=True,
        )
    except click.Abort:
        return None


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
    from moneybin.services.user_reports_service import UserReportsService

    try:
        to_class = DataClass(to)
    except ValueError as e:
        raise typer.BadParameter(
            f"unknown privacy class {to!r}; one of: "
            f"{', '.join(sorted(item.value for item in DataClass))}",
            param_hint="--to",
        ) from e
    if not reason.strip():
        # The service refuses this too — that is where the invariant lives. Here
        # it is a usage error so it lands before the prompt: asking a human to
        # approve a downgrade that cannot be stored spends the one interaction
        # this command gets.
        raise typer.BadParameter(
            "a reason is required; it is the only record of why this column "
            "reveals less than its derived class",
            param_hint="--reason",
        )

    with handle_cli_errors(cli_actor="reports_reclassify"):
        # Same ordering as `delete`, for the same reason: the writer lock must not
        # be held across an interactive prompt.
        with get_database(read_only=True) as db:
            service = UserReportsService(db)
            row = service.resolve(handle)
            # Derived in the same read as the fingerprint, because that is the
            # revision the prompt is about to describe. A plain read never
            # refreshes the stored fingerprint, so it cannot report an upstream
            # reclassification — only a fresh derivation can.
            from_class = service.derived_class(row, column=column)
        confirmed = (
            True
            if yes
            else _prompt_for_downgrade(row["name"], column, from_class, to_class)
        )
        # Deliberately *not* short-circuited on `confirmed is False`, unlike
        # `delete`'s exit-0 decline: the service is what distinguishes a human
        # declining (`declined`) from a surface that could not ask
        # (`no_elicitation`), and it can only count them if it sees the answer.
        # Exiting 0 here would skip that increment, leaving the one metric that
        # separates "users refuse downgrades" from "our prompt never reached a
        # human" permanently reading zero. A refused downgrade is a recorded
        # privacy gate rather than a cancelled mutation, so it keeps the error
        # envelope and exit 1 that every other refusal on this path returns.
        with get_database(read_only=False) as db:
            outcome = UserReportsService(db).reclassify(
                str(row["report_id"]),
                column=column,
                to_class=to_class,
                reason=reason,
                confirmed=confirmed,
                # `actor="cli"` is the same either way, so the audit row would
                # otherwise be identical whether Brandon answered the prompt or
                # an assistant passed `--yes` on his behalf.
                confirmed_via="flag" if yes else "prompt",
                # The revision read *above*, before the prompt — the whole point
                # is that the write connection re-resolves, so re-reading it here
                # would pin the row to itself and guard nothing.
                expected_fingerprint=str(row["class_fingerprint"]),
                # And the class the prompt actually named, for the drift the
                # fingerprint cannot see.
                expected_from_class=from_class,
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
            sensitivity="medium",
            classes_returned=_LIFECYCLE_CLASSES,
        ),
        output,
        render_fn=_render_text,
        cli_actor="reports_reclassify",
        classes_returned=_LIFECYCLE_CLASSES,
    )


def _query_sql(sql: str | None, sql_file: Path | None) -> str:
    """Read the report's SELECT from exactly one of the two sources.

    Call this *inside* the caller's ``handle_cli_errors`` block. Which of the two
    sources was supplied is a usage question, and ``BadParameter``'s exit 2 is
    right for it; whether the named file could be read is not — `cli.md` puts a
    failed read at exit 1, and `--output json` is owed an error envelope for one.

    The read raises rather than translating, because the shared classifier already
    has better answers than a local ``BadParameter`` did: ``FileNotFoundError``
    names the missing path, ``PermissionError`` carries the platform's
    Full-Disk-Access advice, and ``UnicodeDecodeError`` — a ``ValueError``, so
    never caught by the ``except OSError`` that used to sit here — classifies as
    invalid input instead of escaping as a traceback.
    """
    if (sql is None) == (sql_file is None):
        raise typer.BadParameter(
            "supply the query with either --sql or --sql-file", param_hint="--sql"
        )
    if sql is not None:
        return sql
    assert sql_file is not None  # noqa: S101  # narrowed by the exclusivity check
    return sql_file.read_text()


def _render_save(
    outcome: Any, output: OutputFormat, *, cli_actor: str, verb: str
) -> None:
    """Render one save outcome, including R3's non-blocking notes."""

    def _render_text(_: ResponseEnvelope[Any]) -> None:
        typer.echo(f"✅ {verb} {outcome.name} ({outcome.report_id})")
        if outcome.unresolved_columns:
            typer.echo(
                f"⚠️  Masked — no upstream class could be derived for "
                f"{', '.join(outcome.unresolved_columns)}. "
                "Project the underlying column directly to resolve it."
            )
        if outcome.floored_columns:
            typer.echo(
                f"⚠️  No declared class for "
                f"{', '.join(outcome.floored_columns)}. "
                "Each value is scanned at run time and masked only when it is "
                "shaped like an SSN or holds a run of 8 or more digits; a "
                "shorter run, a separator-formatted number, and any DECIMAL or "
                "FLOAT pass through. `moneybin reports explain` names every "
                "column's class."
            )
        if outcome.cleared_downgrades:
            typer.echo(
                f"⚠️  Cleared the approved downgrade for "
                f"{', '.join(outcome.cleared_downgrades)}; the query changed. "
                "Re-apply with `moneybin reports reclassify`."
            )

    # Echoed above, counted here. A saved report's output alias is user-authored
    # text — `amazon_spend` is as plausible a merchant name as a column one, and
    # `SanitizedLogFormatter` recognizes neither — so the terminal gets the names
    # and the durable record gets the identity and the shape. Logged outside
    # `_render_text` so `--output json` records the event too.
    if (
        outcome.unresolved_columns
        or outcome.floored_columns
        or outcome.cleared_downgrades
    ):
        logger.warning(
            f"user_report.{cli_actor} notes report_id={outcome.report_id} "
            f"unresolved_columns={len(outcome.unresolved_columns)} "
            f"floored_columns={len(outcome.floored_columns)} "
            f"cleared_downgrades={len(outcome.cleared_downgrades)}"
        )

    render_or_json(
        build_envelope(
            data={
                "report_id": outcome.report_id,
                "name": outcome.name,
                "unresolved_columns": list(outcome.unresolved_columns),
                "floored_columns": list(outcome.floored_columns),
                "cleared_downgrades": list(outcome.cleared_downgrades),
            },
            # `name` is user-authored text, classed USER_NOTE — the same tier the
            # stored column carries, not the AGGREGATE a built-in's name is.
            sensitivity="medium",
            classes_returned=_LIFECYCLE_CLASSES,
        ),
        output,
        render_fn=_render_text,
        cli_actor=cli_actor,
        classes_returned=_LIFECYCLE_CLASSES,
    )
