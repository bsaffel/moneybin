"""`moneybin privacy log` — recent consent + privacy log events."""

from __future__ import annotations

import typer

from moneybin.cli.output import OutputFormat, output_option, render_or_json
from moneybin.cli.utils import handle_cli_errors
from moneybin.privacy.log import read_privacy_events
from moneybin.privacy.payloads.consent import PrivacyLogPayload, PrivacyLogRow
from moneybin.protocol.envelope import build_envelope


def privacy_log(
    last: int = typer.Option(50, "--last", help="Max events to show"),
    actor: str | None = typer.Option(None, "--actor", help="Filter by actor"),
    output: OutputFormat = output_option,
) -> None:
    """Show recent privacy log events (consent grants/revokes + tool calls)."""
    filters: dict[str, object] = {}
    if actor:
        filters["actor"] = actor
    with handle_cli_errors():
        events = read_privacy_events(filters, max_rows=last)
    payload = PrivacyLogPayload(
        events=[
            PrivacyLogRow(
                ts=str(e.get("ts", "")),
                action=str(e.get("action", "")),
                actor=str(e.get("actor", "")),
                feature_category=str(e.get("feature_category", "")),
                backend=str(e.get("backend", "")),
            )
            for e in events
        ]
    )
    if output == OutputFormat.JSON:
        render_or_json(build_envelope(data=payload), output, cli_actor="privacy_log")
        return
    if not payload.events:
        typer.echo("No privacy log events.")
        return
    for e in payload.events:
        typer.echo(f"{e.ts} | {e.action} | {e.actor} | {e.feature_category}")
