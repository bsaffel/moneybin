"""System audit subgroup: list, show, undo, history, get.

Thin wrappers over ``AuditService`` (list/show) and ``UndoService`` (undo,
history, get) — the CLI peer of the ``system_audit_*`` MCP tools.
"""

from __future__ import annotations

import dataclasses
import logging

import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.protocol.envelope import build_envelope

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Inspect the unified audit log",
    no_args_is_help=True,
)


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

    def _render_text(_: object) -> None:
        if not events:
            if not quiet:
                logger.info("No audit events match.")
            return
        for e in events:
            target = e.target_id or "-"
            typer.echo(
                f"  [{e.audit_id}] {e.occurred_at} {e.actor} {e.action} {target}"
            )

    render_or_json(
        # AuditEvent.before_value / after_value carry TXN_AMOUNT (HIGH); the
        # payload is a bare list[dict] so render_or_json can't derive the tier
        # from a typed class. Declare HIGH explicitly to keep audit rows
        # correctly classified.
        build_envelope(data=[e.to_dict() for e in events], sensitivity="high"),
        output,
        render_fn=_render_text,
        cli_actor="system_audit_list",
    )


@app.command("show")
def system_audit_show(
    audit_id: str = typer.Argument(..., help="Audit event ID"),
    output: OutputFormat = output_option,
) -> None:
    """Show one audit event plus any chained children (parent_audit_id matches)."""
    from moneybin.services.audit_service import AuditService

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            events = AuditService(db).chain_for(audit_id)

    if not events:
        raise LookupError(f"audit_id={audit_id} not found")

    def _render_text(_: object) -> None:
        for e in events:
            marker = "  " if e.parent_audit_id else ""
            typer.echo(f"{marker}[{e.audit_id}] {e.occurred_at} {e.actor} {e.action}")
            typer.echo(
                f"{marker}  target: {e.target_schema}.{e.target_table} id={e.target_id}"
            )
            if e.before_value is not None:
                typer.echo(f"{marker}  before: {e.before_value}")
            if e.after_value is not None:
                typer.echo(f"{marker}  after:  {e.after_value}")
            if e.context_json is not None:
                typer.echo(f"{marker}  context: {e.context_json}")

    render_or_json(
        # AuditEvent.before_value / after_value carry TXN_AMOUNT (HIGH); the
        # payload is a bare list[dict] so render_or_json can't derive the tier
        # from a typed class. Declare HIGH explicitly to keep audit rows
        # correctly classified.
        build_envelope(data=[e.to_dict() for e in events], sensitivity="high"),
        output,
        render_fn=_render_text,
        cli_actor="system_audit_show",
    )


@app.command("undo")
def system_audit_undo(
    operation_id: str = typer.Argument(..., help="Operation id to reverse"),
    output: OutputFormat = output_option,
) -> None:
    """Reverse every app.* mutation in one operation as a unit (keyed on operation_id).

    Refuses (exit 1) when a later operation modified the same rows
    (``undo_cascade_blocked`` — undo those first), when the operation was already
    undone, or when it touched a table outside the undoable app.* surface. The
    returned ``undo_operation_id`` is itself undoable.
    """
    from moneybin.services.undo_service import UndoService

    with handle_cli_errors():
        with get_database() as db:
            result = UndoService(db).undo(operation_id, actor="cli")

    def _render_text(_: object) -> None:
        tables = ", ".join(result.tables) if result.tables else "no"
        typer.echo(
            f"✅ Reversed operation {result.undone_operation_id} "
            f"({result.reversed_row_count} row(s) across {tables} table(s))."
        )
        typer.echo(
            f"💡 Undo this undo: moneybin system audit undo {result.undo_operation_id}"
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

    def _render_text(_: object) -> None:
        if not operations:
            if not quiet:
                logger.info("No operations match.")
            return
        for o in operations:
            mark = "✅" if o.can_undo else "🔒"
            blocked = (
                f" blocked-by={','.join(o.undo_blocked_by)}"
                if o.undo_blocked_by
                else ""
            )
            typer.echo(
                f"{mark} [{o.operation_id}] {o.occurred_at} {o.actor} "
                f"{','.join(o.actions)} ({o.row_count} row(s)){blocked}"
            )

    render_or_json(
        build_envelope(
            data=[dataclasses.asdict(o) for o in operations], sensitivity="low"
        ),
        output,
        render_fn=_render_text,
        cli_actor="system_audit_history",
    )


@app.command("get")
def system_audit_get(
    operation_id: str = typer.Argument(..., help="Operation id to inspect"),
    output: OutputFormat = output_option,
) -> None:
    """Show full before/after for every row of one operation before undoing it."""
    from moneybin.services.undo_service import UndoService

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            detail = UndoService(db).get(operation_id)

    def _render_text(_: object) -> None:
        mark = "✅ undoable" if detail.can_undo else "🔒 not undoable"
        blocked = (
            f" (blocked by {', '.join(detail.undo_blocked_by)})"
            if detail.undo_blocked_by
            else ""
        )
        typer.echo(f"Operation {detail.operation_id} — {mark}{blocked}")
        for e in detail.events:
            typer.echo(
                f"  [{e.audit_id}] {e.action} "
                f"{e.target_schema}.{e.target_table} id={e.target_id}"
            )
            if e.before_value is not None:
                typer.echo(f"    before: {e.before_value}")
            if e.after_value is not None:
                typer.echo(f"    after:  {e.after_value}")

    render_or_json(
        # before/after carry TXN_AMOUNT (HIGH); bare-dict payload can't derive
        # the tier, so declare it explicitly (matches `system audit show`).
        build_envelope(
            data={
                "operation_id": detail.operation_id,
                "events": [e.to_dict() for e in detail.events],
                "can_undo": detail.can_undo,
                "undo_blocked_by": detail.undo_blocked_by,
            },
            sensitivity="high",
        ),
        output,
        render_fn=_render_text,
        cli_actor="system_audit_get",
    )
