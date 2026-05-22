"""`moneybin privacy status` — show active consent + configured backend."""

from __future__ import annotations

import typer

from moneybin.cli.output import OutputFormat, output_option, render_or_json
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.privacy.payloads.consent import ConsentGrantRow, PrivacyStatusPayload
from moneybin.protocol.envelope import build_envelope
from moneybin.services.consent_service import ConsentService


def privacy_status(output: OutputFormat = output_option) -> None:
    """Show active AI consent grants, the configured backend, and consent policy."""
    with handle_cli_errors():
        with get_database(read_only=True) as db:
            status = ConsentService(db).status()
    payload = PrivacyStatusPayload(
        default_backend=status.default_backend or "(none)",
        consent_policy=status.consent_policy,
        active_grants=[
            ConsentGrantRow(
                feature_category=g.feature_category,
                backend=g.backend,
                consent_mode=g.consent_mode.value,
                granted_at=str(g.granted_at),
            )
            for g in status.active_grants
        ],
    )
    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data=payload,
                actions=[
                    "Use `moneybin privacy grant <category>` to add consent",
                    "Use `moneybin privacy log` to see the consent history",
                ],
            ),
            output,
            cli_actor="privacy_status",
        )
        return
    typer.echo(
        f"Backend: {payload.default_backend}    Policy: {payload.consent_policy}"
    )
    if not payload.active_grants:
        typer.echo("No active consent grants.")
        return
    for g in payload.active_grants:
        typer.echo(
            f"  {g.feature_category}\t{g.backend}\t{g.consent_mode}\t{g.granted_at}"
        )
