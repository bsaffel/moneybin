"""`moneybin privacy revoke-all` — revoke every active consent grant."""

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
from moneybin.privacy.payloads.consent import ConsentRevokeAllPayload
from moneybin.protocol.envelope import build_envelope
from moneybin.services.consent_service import ConsentService

from ._presentation import emit_receipt, require_confirmation

logger = logging.getLogger(__name__)


def privacy_revoke_all(
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Revoke ALL active AI consent grants."""
    with handle_cli_errors(
        cli_actor="privacy_revoke_all", payload_type=ConsentRevokeAllPayload
    ):
        with get_database(read_only=True) as db:
            selected_grants = tuple(ConsentService(db).status().active_grants)
        selected_grant_ids = {grant.grant_id for grant in selected_grants}
        selected_details = (
            "\n".join(
                f"  {grant.feature_category} | {grant.backend} | {grant.consent_mode.value}"
                for grant in selected_grants
            )
            or "  No active consent grants"
        )
        confirmed = require_confirmation(
            yes=yes,
            output=output,
            prompt=(
                f"Revoke {len(selected_grants)} active consent grant(s)?\n"
                f"{selected_details}\nProceed?"
            ),
        )
        if not confirmed:
            emit_receipt(
                "Consent revoke cancelled",
                [("Outcome", "No consent grants were revoked")],
            )
            raise typer.Exit(0)
        with get_database(read_only=False) as db:
            count = ConsentService(db).revoke_all(
                actor="cli.privacy_revoke_all",
                expected_grant_ids=selected_grant_ids,
            )
    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(data=ConsentRevokeAllPayload(revoked_count=count)),
            output,
            cli_actor="privacy_revoke_all",
        )
        return
    emit_receipt(
        "Consent grants revoked" if count else "Consent unchanged",
        [
            (
                "Outcome",
                f"Revoked {count} consent grant(s)"
                if count
                else "No active consent grants; no change made",
            )
        ],
    )
