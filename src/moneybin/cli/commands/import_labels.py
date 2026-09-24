"""Import labels subgroup: add, remove, list.

Thin wrappers over ``ImportService.{add_labels,remove_labels,list_labels,
list_distinct_labels}``. Labels are slug-flavored markers attached to a single
``app.imports`` row.
"""

from __future__ import annotations

import typer

from moneybin.cli.output import (
    OutputFormat,
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.render import build_rows, build_summary, compose_human_result
from moneybin.cli.utils import abort_cli_error, get_terminal_policy, handle_cli_errors
from moneybin.database import get_database
from moneybin.protocol.envelope import build_envelope

app = typer.Typer(
    help="Manage labels on import_log rows",
    no_args_is_help=True,
)


@app.command("add")
def import_labels_add(
    import_id: str = typer.Argument(..., help="Import ID"),
    labels: list[str] = typer.Argument(..., help="One or more labels"),
    output: OutputFormat = output_option,
) -> None:
    """Add one or more labels to an import."""
    from moneybin.services.import_service import ImportService

    try:
        with handle_cli_errors():
            with get_database(read_only=False) as db:
                updated = ImportService(db).add_labels(import_id, labels, actor="cli")
    except ValueError as e:
        abort_cli_error(e, output=output, exit_code=1, cli_actor="import_labels_add")

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data={"import_id": import_id, "labels": updated}, sensitivity="low"
            ),
            output,
            cli_actor="import_labels_add",
        )
        return
    emit_human_result(
        compose_human_result([
            build_summary(
                [
                    ("Import", import_id),
                    ("Labels", ", ".join(updated) if updated else "None"),
                ],
                title="Labels added",
            )
        ]),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )


@app.command("remove")
def import_labels_remove(
    import_id: str = typer.Argument(..., help="Import ID"),
    labels: list[str] = typer.Argument(..., help="One or more labels"),
    output: OutputFormat = output_option,
) -> None:
    """Remove one or more labels from an import."""
    from moneybin.services.import_service import ImportService

    try:
        with handle_cli_errors():
            with get_database(read_only=False) as db:
                updated = ImportService(db).remove_labels(
                    import_id, labels, actor="cli"
                )
    except ValueError as e:
        abort_cli_error(e, output=output, exit_code=1, cli_actor="import_labels_remove")

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data={"import_id": import_id, "labels": updated}, sensitivity="low"
            ),
            output,
            cli_actor="import_labels_remove",
        )
        return
    emit_human_result(
        compose_human_result([
            build_summary(
                [
                    ("Import", import_id),
                    ("Labels", ", ".join(updated) if updated else "None"),
                ],
                title="Labels removed",
            )
        ]),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )


@app.command("list")
def import_labels_list(
    import_id: str | None = typer.Option(
        None, "--import-id", help="Filter to one import (omit for distinct counts)"
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # the requested label inventory remains visible
    no_pager: bool = no_pager_option,
) -> None:
    """List labels for one import, or all distinct labels with usage counts."""
    from moneybin.services.import_service import ImportService

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            svc = ImportService(db)
            if import_id is not None:
                labels = svc.list_labels(import_id)
                if output == OutputFormat.JSON:
                    render_or_json(
                        build_envelope(
                            data={"import_id": import_id, "labels": labels},
                            sensitivity="low",
                        ),
                        output,
                        cli_actor="import_labels_list",
                    )
                    return
                parts: list[object] = [
                    build_summary(
                        [("Import", import_id), ("Labels", str(len(labels)))],
                        title="Import labels",
                    )
                ]
                if labels:
                    parts.append(build_rows(["Label"], [(label,) for label in labels]))
                else:
                    parts.append(
                        build_summary(
                            [
                                (
                                    "Result",
                                    "No labels on this import. Next: moneybin import labels add "
                                    f"{import_id} <label>",
                                )
                            ],
                            title="No labels",
                        )
                    )
                emit_human_result(
                    compose_human_result(parts),
                    policy=get_terminal_policy(no_pager=no_pager),
                    finite_read=True,
                    no_pager=no_pager,
                )
                return

            rows = svc.list_distinct_labels()

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data=[{"label": label, "usage_count": n} for label, n in rows],
                sensitivity="low",
            ),
            output,
            cli_actor="import_labels_list",
        )
        return
    parts = [
        build_summary(
            [("Distinct labels", str(len(rows)))],
            title="Import labels",
        )
    ]
    if rows:
        parts.append(build_rows(["Label", "Imports"], rows, numeric=["Imports"]))
    else:
        parts.append(
            build_summary(
                [
                    (
                        "Result",
                        "No labels are in use. Next: moneybin import labels add <import-id> <label>",
                    )
                ],
                title="No labels",
            )
        )
    emit_human_result(
        compose_human_result(parts),
        policy=get_terminal_policy(no_pager=no_pager),
        finite_read=True,
        no_pager=no_pager,
    )
