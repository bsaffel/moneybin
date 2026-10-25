"""PDF-disclosure consent pieces shared by the MCP and CLI adapters.

The consent request shape, the remembered-consent notice, and the ledger and
audit writes each surface makes around a decision (pdf-disclosure-consent.md).
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from pathlib import PurePath
from typing import Any, Literal

from moneybin.database import get_database
from moneybin.privacy.disclosure import AIHost, disclosure_request_id
from moneybin.services.import_confirmation import (
    AIAssistanceRequired,
    DisclosureApproval,
)
from moneybin.temporal import format_utc

WHAT_IS_SENT: tuple[str, ...] = (
    "the statement's document text",
    "the first rows of each table it contains",
    "only the account numbers, routing numbers and SSNs MoneyBin identified are "
    "redacted; everything else is sent as-is",
)


def display_label(host: AIHost) -> str:
    """The name shown to the person for ``host``."""
    return host.display_name or host.name or "your AI app"


def file_names(pending: Sequence[AIAssistanceRequired]) -> list[str]:
    """Distinct base names of the pending files, in order."""
    return list(dict.fromkeys(PurePath(p.file_path).name for p in pending))


def record_decision(
    host: AIHost,
    pending: Sequence[AIAssistanceRequired],
    *,
    outcome: Literal["approved", "declined", "timeout", "expired"],
    approved_via: DisclosureApproval,
    remembered: bool,
    grant_id: str | None,
    refusal_code: str | None = None,
) -> None:
    """Audit one consent decision covering ``pending``."""
    from moneybin.services.import_service import ImportService

    with get_database(read_only=False) as db:
        ImportService(db).record_disclosure_decision(
            host=host,
            outcome=outcome,
            approved_via=approved_via,
            remembered=remembered,
            grant_id=grant_id,
            file_sha256s=[p.file_sha256 for p in pending],
            decision_reason=",".join(sorted({p.decision_reason for p in pending})),
            refusal_code=refusal_code,
        )


def remember_host(
    host: AIHost, approved_via: DisclosureApproval, *, actor: str, prompt: str
) -> str:
    """Write the remembered ``ai_host`` grant; ``prompt`` is the text shown."""
    from moneybin.services.consent_service import ConsentService

    with get_database(read_only=False) as db:
        grant, _created = ConsentService(db).grant_host_consent(
            host=host, approved_via=approved_via, actor=actor, grant_prompt=prompt
        )
    return grant.grant_id


def remembered_grant_id(host: AIHost) -> str | None:
    """The active remembered grant for ``host``, if any."""
    from moneybin.services.consent_service import ConsentService

    if host.recipient_key is None:
        return None
    with get_database(read_only=True) as db:
        grant = ConsentService(db).active_host_grant(host=host)
    return grant.grant_id if grant is not None else None


def remembered_notice(host: AIHost, grant_id: str) -> str:
    """The visible notice that content left under a remembered consent."""
    return (
        f"This PDF's content was sent to {display_label(host)} under remembered "
        f"consent {grant_id}; nothing asked the person this time. Tell them, and "
        "that `moneybin privacy revoke smart-import-parsing --recipient "
        f"{host.name}` (run in a terminal) stops it."
    )


def consent_request(
    *,
    host: AIHost,
    pending: Sequence[AIAssistanceRequired],
    statement: str,
    token: str | None,
    expires_at: datetime | None,
    refusal_reason: str | None,
) -> dict[str, Any]:
    """The §1 consent request; carries no document content."""
    terms = host.terms
    return {
        "binding_id": disclosure_request_id(
            file_sha256s=[p.file_sha256 for p in pending], host=host
        ),
        "confirmation_token": token,
        "expires_at": format_utc(expires_at) if expires_at is not None else None,
        "risk_statement": statement,
        "recipient": {
            "host_name": host.display_name or host.name,
            "host_version": host.version,
            "identified": host.identified,
            "source": host.source,
        },
        "vendor_terms": (
            {
                "vendor": terms.vendor,
                "retention": terms.retention,
                "training_use": terms.training_use,
                "policy_url": terms.policy_url,
                "plan_caveat": terms.plan_caveat,
                "terms_version": terms.terms_version,
            }
            if terms is not None
            else None
        ),
        "vendor_terms_unavailable_reason": (
            None
            if terms is not None
            else "MoneyBin has no data-terms summary for this AI app"
        ),
        "remember_available": host.identified,
        "what_is_sent": list(WHAT_IS_SENT),
        "files": [
            {
                "file": p.file_path,
                "file_sha256": p.file_sha256,
                "decision_reason": p.decision_reason,
                "request_kind": p.request_kind,
            }
            for p in pending
        ],
        "refusal_reason": refusal_reason,
    }
