"""System audit subgroup: list, show.

Thin wrappers over ``AuditService.list_events`` and ``chain_for``.
"""

from __future__ import annotations

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
        build_envelope(data=[e.to_dict() for e in events], sensitivity="low"),
        output,
        render_fn=_render_text,
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
        build_envelope(data=[e.to_dict() for e in events], sensitivity="low"),
        output,
        render_fn=_render_text,
    )
