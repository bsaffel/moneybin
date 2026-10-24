"""`moneybin privacy log` — recent consent + privacy log events."""

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
from moneybin.cli.utils import get_terminal_policy, handle_cli_errors
from moneybin.privacy.log import MAX_LOG_ROWS, read_privacy_events
from moneybin.privacy.payloads.consent import PrivacyLogPayload, PrivacyLogRow
from moneybin.protocol.envelope import build_envelope


def privacy_log(
    last: int = typer.Option(50, "--last", help="Max events to show (capped at 1000)"),
    actor: str | None = typer.Option(None, "--actor", help="Filter by actor"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
) -> None:
    """Show recent privacy log events (consent grants/revokes + tool calls)."""
    filters: dict[str, object] = {}
    if actor is not None:
        filters["actor"] = actor
    with handle_cli_errors():
        events = read_privacy_events(filters, max_rows=last)
    payload = PrivacyLogPayload(events=[PrivacyLogRow.from_event(e) for e in events])
    if output == OutputFormat.JSON:
        render_or_json(build_envelope(data=payload), output, cli_actor="privacy_log")
        return
    limit = min(max(last, 0), MAX_LOG_ROWS)
    scope = f"actor={actor}" if actor is not None else "all actors"
    heading = f"Showing last {limit} event{'s' if limit != 1 else ''} ({scope}; total matching events unknown)"
    if not payload.events:
        emit_human_result(
            compose_human_result([
                build_summary(
                    [("Privacy log", f"No events matched {scope}.")], title=heading
                )
            ]),
            policy=get_terminal_policy(no_pager=no_pager),
            finite_read=True,
            no_pager=no_pager,
        )
        return
    rows: list[tuple[str, str, str, str]] = []
    for e in payload.events:
        if e.action == "tool_call":
            classes = ",".join(e.classes_returned or []) or "(none)"
            row_count = e.row_count if e.row_count is not None else "(n/a)"
            detail = f"sensitivity={e.sensitivity or '(n/a)'} classes={classes} rows={row_count}"
        else:
            mode = f" | {e.consent_mode}" if e.consent_mode else ""
            detail = f"{e.feature_category or ''} | {e.backend or ''}{mode}"
        rows.append((e.ts, e.action, e.actor, detail))
    emit_human_result(
        compose_human_result([
            build_summary([("Returned", str(len(payload.events)))], title=heading),
            build_rows(
                ["When", "Action", "Actor", "Details"],
                rows,
                # `When`, `Action`, and `Actor` are each one unbroken token
                # (a timestamp, `consent.grant`, `cli.privacy_log`) with no
                # space to fold on; `Details` is the one column built from
                # `key=value` pairs joined by spaces, so it is the column
                # that should give way in a narrow terminal.
                nowrap=("When", "Action", "Actor"),
            ),
        ]),
        policy=get_terminal_policy(no_pager=no_pager),
        finite_read=True,
        no_pager=no_pager,
    )
