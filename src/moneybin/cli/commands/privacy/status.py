"""`moneybin privacy status` — show active consent + configured backend."""

from __future__ import annotations

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
from moneybin.privacy.payloads.consent import ConsentGrantRow, PrivacyStatusPayload
from moneybin.protocol.envelope import build_envelope
from moneybin.services.consent_service import ConsentService


def privacy_status(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
) -> None:
    """Show active AI consent grants, the configured backend, and consent policy."""
    with handle_cli_errors():
        with get_database(read_only=True) as db:
            status = ConsentService(db).status()
    payload = PrivacyStatusPayload(
        default_backend=status.default_backend,
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
    parts: list[object] = [
        build_summary(
            [
                ("Backend", payload.default_backend or "No default backend configured"),
                ("Consent policy", payload.consent_policy),
            ],
            title="Privacy status",
        )
    ]
    if not payload.active_grants:
        parts.append(
            build_summary([
                (
                    "Active consent grants",
                    "None. Grant one with `moneybin privacy grant <category>`.",
                )
            ])
        )
    else:
        parts.append(
            build_rows(
                ["Category", "Backend", "Mode", "Granted"],
                [
                    (g.feature_category, g.backend, g.consent_mode, g.granted_at)
                    for g in payload.active_grants
                ],
            )
        )
    emit_human_result(
        compose_human_result(parts),
        policy=get_terminal_policy(no_pager=no_pager),
        finite_read=True,
        no_pager=no_pager,
    )
