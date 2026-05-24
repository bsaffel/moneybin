"""List every audit event relating to one transaction.

Matches both transaction-level mutations and the transaction's child entities
(notes, tags, splits) — whose audit rows are keyed by their own PK (row-grain
``target_id``) but carry the ``transaction_id`` in their captured row image.
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


def transactions_audit(
    transaction_id: str = typer.Argument(..., help="Transaction ID"),
    limit: int = typer.Option(100, "--limit", help="Max events to return"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """List audit events for one transaction."""
    from moneybin.services.audit_service import AuditService

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            events = AuditService(db).events_for_transaction(
                transaction_id, limit=limit
            )

    def _render_text(_: object) -> None:
        if not events:
            if not quiet:
                logger.info(f"No audit events for {transaction_id}")
            return
        for e in events:
            typer.echo(f"  [{e.audit_id}] {e.occurred_at} {e.actor} {e.action}")

    render_or_json(
        # AuditEvent.before_value / after_value carry TXN_AMOUNT (HIGH); the
        # payload is a bare list[dict] so render_or_json can't derive the tier
        # from a typed class. Declare HIGH explicitly to keep audit rows
        # correctly classified — mirrors the system audit command.
        build_envelope(data=[e.to_dict() for e in events], sensitivity="high"),
        output,
        render_fn=_render_text,
        cli_actor="transactions_audit",
    )
