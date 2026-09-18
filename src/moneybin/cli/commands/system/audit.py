"""System audit subgroup: list, show, undo, history, get.

Thin wrappers over ``AuditService`` (list/show) and ``UndoService`` (undo,
history, get) — the CLI peer of the ``system_audit_*`` MCP tools.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Sequence

import click
import typer

from moneybin import error_codes
from moneybin.cli.output import (
    OutputFormat,
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.render import build_rows, build_summary, compose_human_result
from moneybin.cli.utils import get_terminal_policy, handle_cli_errors
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.protocol.envelope import build_envelope
from moneybin.services.audit_service import AuditEvent
from moneybin.services.undo_service import OperationDetail

app = typer.Typer(
    help="Inspect the unified audit log",
    no_args_is_help=True,
)


def _value(value: object) -> str:
    """Render returned audit values fully without Python's dict representation."""
    return json.dumps(value, indent=2, sort_keys=True, default=str)


def _filter_scope(**filters: object) -> str:
    """Name the filters that shaped an otherwise empty or bounded answer."""
    active = [f"{name}={value}" for name, value in filters.items() if value is not None]
    return ", ".join(active) if active else "all audit events"


def _audit_detail(events: Sequence[AuditEvent], *, title: str) -> object:
    """Build full audit images as a single pageable answer."""
    lines = [title]
    for event in events:
        lines.extend([
            "",
            f"Audit ID: {event.audit_id}",
            f"Occurred: {event.occurred_at}",
            f"Actor: {event.actor}",
            f"Action: {event.action}",
            (
                "Target: "
                f"{event.target_schema}.{event.target_table} "
                f"({event.target_id or '-'})"
            ),
            f"Before: {_value(event.before_value)}",
            f"After: {_value(event.after_value)}",
            f"Context: {_value(event.context_json)}",
        ])
    return "\n".join(lines)


def _undo_confirmation(detail: OperationDetail, *, operation_id: str) -> bool:
    """Ask for one displayed operation selection, or report an unaskable request."""
    try:
        return typer.confirm(
            f"Reverse {detail.reversible_source_event_count} selected source "
            f"audit event(s) across {', '.join(detail.source_tables) or 'no'} "
            "source table(s) "
            f"for operation {operation_id}?",
            err=True,
        )
    except click.Abort as exc:
        raise UserError(
            "Undo needs explicit confirmation.",
            code=error_codes.MUTATION_CONFIRMATION_REQUIRED,
            hint=f"Review with 'moneybin system audit get {operation_id}', then re-run with --yes.",
        ) from exc


@app.command("list")
def system_audit_list(
    actor: str | None = typer.Option(None, "--actor", help="Filter by actor"),
    action: str | None = typer.Option(
        None, "--action", help="Filter by action LIKE pattern (e.g., 'tag.%')"
    ),
    target_table: str | None = typer.Option(
        None, "--target-table", help="Filter by target table"
    ),
    target_id: str | None = typer.Option(
        None, "--target-id", help="Filter by target_id"
    ),
    from_ts: str | None = typer.Option(
        None, "--from", help="Filter occurred_at >= timestamp"
    ),
    to_ts: str | None = typer.Option(
        None, "--to", help="Filter occurred_at <= timestamp"
    ),
    limit: int = typer.Option(100, "--limit", help="Max events to return"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
) -> None:
    """List audit events with filters."""
    from moneybin.services.audit_service import AuditService

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            events = AuditService(db).list_events(
                actor=actor,
                action_pattern=action,
                target_table=target_table,
                target_id=target_id,
                from_ts=from_ts,
                to_ts=to_ts,
                limit=limit,
            )

    envelope = build_envelope(
        # AuditEvent.before_value / after_value carry TXN_AMOUNT (HIGH); the
        # payload is a bare list[dict] so render_or_json can't derive the tier
        # from a typed class. Declare HIGH explicitly to keep audit rows
        # correctly classified.
        data=[e.to_dict() for e in events],
        sensitivity="high",
    )
    if output == OutputFormat.JSON:
        render_or_json(envelope, output, cli_actor="system_audit_list")
        return
    scope = _filter_scope(
        actor=actor,
        action=action,
        target_table=target_table,
        target_id=target_id,
        from_ts=from_ts,
        to_ts=to_ts,
    )
    parts: list[object] = [
        build_summary(
            [("Scope", scope), ("Returned", f"{len(events):,} of up to {limit:,}")],
            title="Audit events",
        )
    ]
    if events:
        parts.append(
            build_rows(
                ["audit id", "occurred", "actor", "action", "target"],
                [
                    (
                        event.audit_id,
                        event.occurred_at,
                        event.actor,
                        event.action,
                        f"{event.target_schema}.{event.target_table}:{event.target_id or '-'}",
                    )
                    for event in events
                ],
                terminal=get_terminal_policy(no_pager=no_pager),
            )
        )
    else:
        parts.append(build_summary([("Result", "No audit events match this scope.")]))
    emit_human_result(
        compose_human_result(parts),
        policy=get_terminal_policy(no_pager=no_pager),
        finite_read=True,
        no_pager=no_pager,
    )


@app.command("show")
def system_audit_show(
    audit_id: str = typer.Argument(..., help="Audit event ID"),
    output: OutputFormat = output_option,
    no_pager: bool = no_pager_option,
) -> None:
    """Show one audit event plus any chained children (parent_audit_id matches)."""
    from moneybin.services.audit_service import AuditService

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            events = AuditService(db).chain_for(audit_id)
        if not events:
            raise UserError(
                f"No audit event found with id {audit_id!r}.",
                code=error_codes.AUDIT_IDENTIFIER_NOT_FOUND,
            )

    envelope = build_envelope(
        # AuditEvent.before_value / after_value carry TXN_AMOUNT (HIGH); the
        # payload is a bare list[dict] so render_or_json can't derive the tier
        # from a typed class. Declare HIGH explicitly to keep audit rows
        # correctly classified.
        data=[e.to_dict() for e in events],
        sensitivity="high",
    )
    if output == OutputFormat.JSON:
        render_or_json(envelope, output, cli_actor="system_audit_show")
        return
    emit_human_result(
        _audit_detail(events, title=f"Audit chain · {audit_id}"),
        policy=get_terminal_policy(no_pager=no_pager),
        finite_read=True,
        no_pager=no_pager,
    )


@app.command("undo")
def system_audit_undo(
    operation_id: str = typer.Argument(..., help="Operation id to reverse"),
    yes: bool = typer.Option(
        False, "--yes", "-y", help="Confirm the displayed undo scope"
    ),
    output: OutputFormat = output_option,
) -> None:
    """Reverse every app.* mutation in one operation as a unit (keyed on operation_id).

    Refuses (exit 1) when a later operation modified the same rows
    (``undo_cascade_blocked`` — undo those first), when the operation was already
    undone, or when it touched a table outside the undoable app.* surface. The
    returned ``undo_operation_id`` is itself undoable.
    """
    from moneybin.services.undo_service import UndoService

    with handle_cli_errors(cli_actor="system_audit_undo"):
        with get_database(read_only=True) as db:
            detail = UndoService(db).get(operation_id)
        expected_audit_ids = tuple(event.audit_id for event in detail.events)
        if not yes:
            if output == OutputFormat.JSON:
                raise UserError(
                    "Undo needs explicit confirmation.",
                    code=error_codes.MUTATION_CONFIRMATION_REQUIRED,
                    hint=f"Review with 'moneybin system audit get {operation_id}', then re-run with --yes.",
                )
            policy = get_terminal_policy()
            if not policy.interactive:
                raise UserError(
                    "Undo needs explicit confirmation.",
                    code=error_codes.MUTATION_CONFIRMATION_REQUIRED,
                    hint=f"Review with 'moneybin system audit get {operation_id}', then re-run with --yes.",
                )
            if not detail.can_undo:
                raise UserError(
                    detail.undo_refusal_message
                    or f"Operation {operation_id!r} is not undoable.",
                    code=detail.undo_refusal_code or error_codes.RECOVERY_NO_PATH,
                    recovery_actions=detail.undo_recovery_actions,
                )
            emit_human_result(
                compose_human_result([
                    build_summary(
                        [
                            ("Operation", operation_id),
                            ("Undoable", "yes"),
                            (
                                "Selected source events",
                                str(detail.reversible_source_event_count),
                            ),
                            (
                                "Source tables",
                                ", ".join(detail.source_tables) or "none",
                            ),
                            (
                                "Effect",
                                "Restore the displayed source-state images. "
                                "A legacy manual-investment source event can create "
                                "linked inverse rows; the receipt reports the actual "
                                "inverse rows and tables.",
                            ),
                        ],
                        title="Undo preview",
                    ),
                    _audit_detail(detail.events, title="Selected source audit events"),
                ]),
                policy=policy,
                finite_read=False,
                receipt=True,
            )
            if not _undo_confirmation(detail, operation_id=operation_id):
                typer.echo("Undo cancelled. No changes were made.")
                return
        with get_database(read_only=False) as db:
            result = UndoService(db).undo(
                operation_id,
                actor="cli",
                expected_audit_ids=expected_audit_ids,
            )

    def _render_text(_: object) -> None:
        tables = ", ".join(result.tables) if result.tables else "no"
        typer.echo(f"Reversed operation {result.undone_operation_id}")
        typer.echo(f"Rows reversed: {result.reversed_row_count}")
        typer.echo(f"Tables: {tables}")
        typer.echo(
            f"Undo this undo: moneybin system audit undo {result.undo_operation_id}"
        )

    render_or_json(
        build_envelope(data=dataclasses.asdict(result), sensitivity="low"),
        output,
        render_fn=_render_text,
        cli_actor="system_audit_undo",
    )


@app.command("history")
def system_audit_history(
    domain: str | None = typer.Option(
        None, "--domain", help="Filter to an action family (e.g. 'tag')"
    ),
    since: str | None = typer.Option(
        None, "--since", help="Filter occurred_at >= timestamp"
    ),
    actor: str | None = typer.Option(None, "--actor", help="Filter by actor"),
    limit: int = typer.Option(50, "--limit", help="Max operations to return"),
    include_undone: bool = typer.Option(
        False, "--include-undone", help="Include the undo operations themselves"
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
) -> None:
    """List recent audited operations, newest first, with undoability.

    The pull surface for reversing a change when no error preceded the regret.
    """
    from moneybin.services.undo_service import UndoService

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            operations = UndoService(db).history(
                domain=domain,
                since=since,
                actor=actor,
                limit=limit,
                include_undone=include_undone,
            )

    envelope = build_envelope(
        data=[dataclasses.asdict(o) for o in operations], sensitivity="low"
    )
    if output == OutputFormat.JSON:
        render_or_json(envelope, output, cli_actor="system_audit_history")
        return
    scope = _filter_scope(domain=domain, since=since, actor=actor)
    parts = [
        build_summary(
            [("Scope", scope), ("Returned", f"{len(operations):,} of up to {limit:,}")],
            title="Audit operations",
        )
    ]
    if operations:
        parts.append(
            build_rows(
                ["operation id", "occurred", "actor", "actions", "rows", "undo"],
                [
                    (
                        operation.operation_id,
                        operation.occurred_at,
                        operation.actor,
                        ", ".join(operation.actions),
                        operation.row_count,
                        (
                            "undoable"
                            if operation.can_undo
                            else "blocked by "
                            + ", ".join(operation.undo_blocked_by or [])
                        ),
                    )
                    for operation in operations
                ],
                numeric=["rows"],
                terminal=get_terminal_policy(no_pager=no_pager),
            )
        )
    else:
        parts.append(build_summary([("Result", "No operations match this scope.")]))
    emit_human_result(
        compose_human_result(parts),
        policy=get_terminal_policy(no_pager=no_pager),
        finite_read=True,
        no_pager=no_pager,
    )


@app.command("get")
def system_audit_get(
    operation_id: str = typer.Argument(..., help="Operation id to inspect"),
    output: OutputFormat = output_option,
    no_pager: bool = no_pager_option,
) -> None:
    """Show full before/after for every row of one operation before undoing it."""
    from moneybin.services.undo_service import UndoService

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            detail = UndoService(db).get(operation_id)

    envelope = build_envelope(
        # before/after carry TXN_AMOUNT (HIGH); bare-dict payload can't derive
        # the tier, so declare it explicitly (matches `system audit show`).
        data={
            "operation_id": detail.operation_id,
            "events": [e.to_dict() for e in detail.events],
            "can_undo": detail.can_undo,
            "undo_blocked_by": detail.undo_blocked_by,
        },
        sensitivity="high",
    )
    if output == OutputFormat.JSON:
        render_or_json(envelope, output, cli_actor="system_audit_get")
        return
    state = "Undoable" if detail.can_undo else "Not undoable"
    if detail.undo_blocked_by:
        state += f"; blocked by {', '.join(detail.undo_blocked_by)}"
    emit_human_result(
        compose_human_result([
            build_summary(
                [("Operation", detail.operation_id), ("State", state)],
                title="Audit operation",
            ),
            _audit_detail(detail.events, title="Returned events"),
        ]),
        policy=get_terminal_policy(no_pager=no_pager),
        finite_read=True,
        no_pager=no_pager,
    )
