"""Convenience wrapper: list audit events targeted at one transaction.

Equivalent to ``moneybin system audit list --target-id <txn_id>`` but reads
better when investigating a specific record.
"""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import emit_json, handle_cli_errors
from moneybin.services.audit_service import AuditEvent

logger = logging.getLogger(__name__)


def _event_to_dict(e: AuditEvent) -> dict[str, object]:
    return {
        "audit_id": e.audit_id,
        "occurred_at": e.occurred_at,
        "actor": e.actor,
        "action": e.action,
        "target_schema": e.target_schema,
        "target_table": e.target_table,
        "target_id": e.target_id,
        "before_value": e.before_value,
        "after_value": e.after_value,
        "parent_audit_id": e.parent_audit_id,
        "context_json": e.context_json,
    }


def transactions_audit(
    transaction_id: str = typer.Argument(..., help="Transaction ID"),
    limit: int = typer.Option(100, "--limit", help="Max events to return"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """List audit events for one transaction."""
    from moneybin.services.audit_service import AuditService

    with handle_cli_errors() as db:
        events = AuditService(db).list_events(target_id=transaction_id, limit=limit)

    if output == OutputFormat.JSON:
        emit_json("audit_events", [_event_to_dict(e) for e in events])
        return
    if not events:
        if not quiet:
            logger.info(f"No audit events for {transaction_id}")
        return
    for e in events:
        typer.echo(f"  [{e.audit_id}] {e.occurred_at} {e.actor} {e.action}")
