"""`import preview` consent before a PDF's content leaves (pdf-disclosure-consent.md §3).

Order: a stale flag is refused, then a remembered grant, the flag, the
terminal prompt, else the content-free result. A person's approval is audited
after the release attempt. Neither the flag (carried by an agent) nor the
prompt (offered only to an unidentified host) remembers; `privacy grant
--recipient` is the CLI's one way to remember.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Literal

import typer

from moneybin import error_codes
from moneybin.adapters.pdf_disclosure_adapters import (
    consent_request,
    display_label,
    file_names,
    pre_grant_text,
    remembered_notice,
)
from moneybin.cli.output import OutputFormat, emit_human_result, render_or_json
from moneybin.cli.render import build_summary, compose_human_result, render_rows
from moneybin.cli.utils import (
    generated_cli_command,
    get_terminal_policy,
    terminal_safe_text,
)
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.privacy.classified_envelope import classify
from moneybin.privacy.disclosure import AIHost, disclosure_request_id, risk_statement
from moneybin.privacy.payloads.imports import (
    ImportAIAssistanceRequiredPayload,
    ImportPdfBridgePreviewPayload,
)
from moneybin.protocol.envelope import build_envelope
from moneybin.services.import_confirmation import (
    AIAssistanceRequired,
    ConfirmationRequired,
    DisclosureApproval,
    DisclosureConsent,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CLIConsent:
    """A verified consent awaiting release; ``remember_prompt`` grants after it."""

    consent: DisclosureConsent
    pending: tuple[AIAssistanceRequired, ...]
    remember_prompt: str | None


def _record(
    host: AIHost,
    pending: Sequence[AIAssistanceRequired],
    *,
    outcome: Literal["approved", "declined", "expired"],
    approved_via: DisclosureApproval,
    remembered: bool = False,
    grant_id: str | None = None,
    refusal_code: str | None = None,
    binding_id: str | None = None,
    released: bool = False,
) -> None:
    from moneybin.services.import_service import ImportService

    with get_database(read_only=False) as db:
        ImportService(db).record_disclosure_decision(
            host=host,
            outcome=outcome,
            approved_via=approved_via,
            remembered=remembered,
            grant_id=grant_id,
            pending=pending,
            refusal_code=refusal_code,
            binding_id=binding_id,
            released=released,
        )


def _receipt(title: str, pairs: Sequence[tuple[str, str]]) -> None:
    emit_human_result(
        compose_human_result((build_summary(pairs, title=title),)),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )


def _safe(text: str) -> str:
    """Strip terminal control sequences from untrusted PDF text; keep tabs."""
    return "\t".join(terminal_safe_text(part) for part in text.split("\t"))


def obtain_cli_disclosure_consent(
    source: Path,
    proposal: AIAssistanceRequired,
    host: AIHost,
    *,
    output: OutputFormat,
    allow_ai_disclosure: str | None,
) -> CLIConsent | None:
    """Return a verified consent, or emit the content-free result and return None."""
    from moneybin.services.consent_service import ConsentService

    pending = (replace(proposal, file_path=str(source)),)
    request_id = disclosure_request_id(file_sha256s=[proposal.file_sha256], host=host)
    statement = risk_statement(host, file_names=file_names(pending))
    one_time = frozenset({proposal.file_sha256})

    if allow_ai_disclosure is not None and allow_ai_disclosure != request_id:
        _record(
            host,
            pending,
            outcome="expired",
            approved_via="cli_flag",
            refusal_code=error_codes.IMPORT_PDF_DISCLOSURE_REQUEST_MISMATCH,
            binding_id=request_id,
        )
        current = generated_cli_command("import", "preview", source, "--output", "json")
        raise UserError(
            "This --allow-ai-disclosure id does not match this file, AI app or "
            "current terms; nothing was sent.",
            code=error_codes.IMPORT_PDF_DISCLOSURE_REQUEST_MISMATCH,
            hint=(
                f"Run `{current}` for the current request id and show the person "
                "its risk statement."
            ),
        )

    grant = None
    if host.recipient_key is not None:
        with get_database(read_only=True) as db:
            grant = ConsentService(db).active_host_grant(host=host)
    if grant is not None:
        consent = DisclosureConsent(
            host=host,
            approved_via="remembered",
            grant_id=grant.grant_id,
            binding_id=None,
            remembered=True,
            file_sha256s=None,
        )
        return CLIConsent(consent, pending, remember_prompt=None)

    if allow_ai_disclosure is not None:
        consent = DisclosureConsent(
            host=host,
            approved_via="cli_flag",
            grant_id=None,
            binding_id=request_id,
            remembered=False,
            file_sha256s=one_time,
        )
        # The agent carries the flag: one-time only, never a remembered grant.
        return CLIConsent(consent, pending, remember_prompt=None)

    # An identified host means an agent shell; an agent with a pty could answer
    # the prompt itself, so it gets the receipt and asks the person in chat.
    if (
        output == OutputFormat.TEXT
        and not host.identified
        and get_terminal_policy().interactive
    ):
        typer.echo(_safe(statement), err=True)
        if not typer.confirm(
            f"Send this PDF's contents to {display_label(host)}?",
            default=False,
            err=True,
        ):
            _record(
                host,
                pending,
                outcome="declined",
                approved_via="cli_prompt",
                binding_id=request_id,
            )
            _receipt(
                "PDF not sent",
                [
                    ("File", source.name),
                    ("Result", "Nothing from the document was sent"),
                ],
            )
            return None
        # Unidentified host: one-time only, nothing to key a remembered grant on.
        consent = DisclosureConsent(
            host=host,
            approved_via="cli_prompt",
            grant_id=None,
            binding_id=request_id,
            remembered=False,
            file_sha256s=one_time,
        )
        return CLIConsent(consent, pending, remember_prompt=None)

    _emit_ai_assistance_required(
        source,
        pending,
        host,
        statement=statement,
        request_id=request_id,
        output=output,
    )
    return None


def settle_cli_disclosure(obtained: CLIConsent, *, released: bool) -> str | None:
    """After the release attempt: audit the approval, remember only if released.

    A remembered grant that released nothing involved no decision: no row.
    """
    from moneybin.services.consent_service import ConsentService

    consent = obtained.consent
    if not released and consent.approved_via == "remembered":
        return None
    grant_id = consent.grant_id if released else None
    if released and obtained.remember_prompt is not None:
        with get_database(read_only=False) as db:
            grant, _created = ConsentService(db).grant_host_consent(
                host=consent.host,
                approved_via=consent.approved_via,
                actor="cli",
                grant_prompt=obtained.remember_prompt,
            )
        grant_id = grant.grant_id
    _record(
        consent.host,
        obtained.pending,
        outcome="approved",
        approved_via=consent.approved_via,
        remembered=grant_id is not None,
        grant_id=grant_id,
        binding_id=consent.binding_id,
        released=released,
    )
    if consent.approved_via == "remembered" and grant_id is not None:
        return remembered_notice(consent.host, grant_id)
    return None


def settle_cli_unreleased(obtained: CLIConsent) -> None:
    """Audit an approval whose release failed; never mask the release error."""
    try:
        settle_cli_disclosure(obtained, released=False)
    except Exception as exc:  # the release error is the one to surface
        logger.error(
            f"Could not audit an unreleased PDF disclosure approval: "
            f"{type(exc).__name__}"
        )


def _emit_ai_assistance_required(
    source: Path,
    pending: Sequence[AIAssistanceRequired],
    host: AIHost,
    *,
    statement: str,
    request_id: str,
    output: OutputFormat,
) -> None:
    """The content-free result: receipt, request id and the consent text."""
    pre_grant = pre_grant_text(host)
    allow = generated_cli_command(
        "import",
        "preview",
        source,
        "--allow-ai-disclosure",
        request_id,
        "--output",
        "json",
    )
    if output == OutputFormat.JSON:
        request = consent_request(
            host=host,
            pending=pending,
            statement=statement,
            token=None,
            expires_at=None,
            refusal_reason=None,
        )
        del request["confirmation_token"]
        request["request_id"] = request_id
        classification = classify(ImportAIAssistanceRequiredPayload)
        render_or_json(
            build_envelope(
                sensitivity=classification.sensitivity,
                data={
                    "kind": "pdf_ai_assistance",
                    "status": "ai_assistance_required",
                    "channel": "pdf",
                    "consent_request": request,
                },
                actions=[
                    "MoneyBin could not parse this PDF locally and needs AI help. "
                    "Nothing from the document has been sent. Show the person "
                    "data.consent_request.risk_statement verbatim and ask whether "
                    f"to send the document to {display_label(host)}.",
                    f"Only on a clear yes, run `{allow}`.",
                    *([pre_grant] if pre_grant else []),
                    "On no, stop: nothing is sent, and `moneybin import files` "
                    "can still store the file as a queryable seed.",
                ],
            ),
            output,
            cli_actor="import_preview",
            classes_returned=classification.classes_returned,
        )
        return
    host_label = display_label(host) + ("" if host.identified else " (unidentified)")
    _receipt(
        "PDF preview needs AI assistance",
        [
            ("File", source.name),
            ("Why AI help is needed", pending[0].decision_reason),
            ("AI host", host_label),
            ("Request id", request_id),
            ("Result", "Nothing from the document was sent"),
        ],
    )
    typer.echo(_safe(statement))
    # The command on its own line so a narrow receipt never wraps it.
    typer.echo(
        f"{get_terminal_policy().symbols.action} Next, only if the person agrees "
        f"to the text above: {_safe(allow)}"
    )
    if pre_grant:
        typer.echo(_safe(pre_grant))


def emit_released_bridge(
    source: Path,
    outcome: ConfirmationRequired,
    payload: dict[str, Any],
    output: OutputFormat,
    notice: str | None,
) -> None:
    """Print the released bridge request: same data as MCP, or text to hand on."""
    confirm = generated_cli_command(
        "import",
        "confirm",
        source,
        "--bridge-response",
        f"{source.name}.json",
        "--confirm",
    )
    if output == OutputFormat.JSON:
        classification = classify(ImportPdfBridgePreviewPayload)
        render_or_json(
            build_envelope(
                sensitivity=classification.sensitivity,
                data={
                    "status": "confirmation_required",
                    "channel": outcome.channel,
                    "file": source.name,
                    "tier": outcome.confidence.tier,
                    "score": outcome.confidence.score,
                    "reason": outcome.reason,
                    "bridge_payload": payload,
                },
                actions=[
                    *([notice] if notice else []),
                    "Read data.bridge_payload (only the account numbers, routing "
                    "numbers and SSNs MoneyBin identified are redacted), write "
                    "{'recipe': ..., 'rows': [...]} to a JSON file, then run "
                    f"`{confirm}` to reconcile and load.",
                ],
            ),
            output,
            cli_actor="import_preview",
            classes_returned=classification.classes_returned,
        )
        return
    _receipt(
        "PDF released for AI-assisted extraction",
        [("File", source.name), ("Reason", outcome.reason)],
    )
    typer.echo(_safe(str(payload.get("document_text", ""))))
    for table in payload.get("tables_preview", []):
        header = [_safe(str(cell or "")) for cell in table.get("header", [])]
        rows = [
            [_safe(str(cell or "")) for cell in row] for row in table.get("rows", [])
        ]
        width = max([len(header), *(len(r) for r in rows)], default=0)
        if not rows or width == 0:
            continue
        columns = header + [""] * (width - len(header))
        typer.echo(f"Table on page {table.get('page')}")
        render_rows(columns, [r + [""] * (width - len(r)) for r in rows])
    typer.echo(f"{get_terminal_policy().symbols.action} Next: {_safe(confirm)}")
