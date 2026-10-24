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
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.render import build_rows, build_summary, compose_human_result
from moneybin.cli.utils import get_terminal_policy, handle_cli_errors
from moneybin.database import get_database
from moneybin.protocol.envelope import build_envelope

logger = logging.getLogger(__name__)


def transactions_audit(
    transaction_id: str = typer.Argument(..., help="Transaction ID"),
    limit: int = typer.Option(100, "--limit", help="Max events to return"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
) -> None:
    """List audit events for one transaction."""
    from moneybin.services.audit_service import AuditService

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            events = AuditService(db).events_for_transaction(
                transaction_id, limit=limit
            )

    envelope = build_envelope(
        # AuditEvent.before_value / after_value carry TXN_AMOUNT (HIGH); the
        # payload is a bare list[dict] so render_or_json can't derive the tier
        # from a typed class. Declare HIGH explicitly to keep audit rows
        # correctly classified — mirrors the system audit command.
        data=[e.to_dict() for e in events],
        sensitivity="high",
    )
    if output == OutputFormat.JSON:
        render_or_json(envelope, output, cli_actor="transactions_audit")
        return
    policy = get_terminal_policy(no_pager=no_pager)
    parts: list[object] = [
        build_summary(
            [("Transaction", transaction_id), ("Maximum events", str(limit))],
            title="Transaction audit",
        )
    ]
    if events:
        parts.append(
            build_rows(
                ["audit id", "occurred", "actor", "action"],
                [
                    (event.audit_id, event.occurred_at, event.actor, event.action)
                    for event in events
                ],
                terminal=policy,
            )
        )
    else:
        parts.append(
            build_summary([("Result", "No audit events for this transaction.")])
        )
    emit_human_result(
        compose_human_result(
            parts,
            disclosures=(() if events else ("Next: moneybin transactions --help",)),
        ),
        policy=policy,
        finite_read=True,
        no_pager=no_pager,
    )
