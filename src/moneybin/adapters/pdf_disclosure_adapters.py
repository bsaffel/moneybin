"""PDF-disclosure consent text and request shape shared by MCP and CLI.

Pure: no I/O (pdf-disclosure-consent.md §1, §3).
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from pathlib import PurePath
from typing import Any

from moneybin.privacy.disclosure import AIHost, disclosure_request_id
from moneybin.services.import_confirmation import AIAssistanceRequired
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


def remember_text(host: AIHost, *, lead: str) -> str | None:
    """What remembering does for an identified host (R20); None when it cannot."""
    if not host.identified:
        return None
    return (
        f"{lead} lets MoneyBin send future PDFs to {display_label(host)} without "
        "asking; `moneybin privacy revoke smart-import-parsing --recipient "
        f"{host.name}` stops it."
    )


def pre_grant_text(host: AIHost) -> str | None:
    """How the person, not the agent, stops being asked; None when unidentified.

    An agent-carried approval is one-time only (spec section 3), so this is the
    one way an agent can tell the person to make consent stick.
    """
    if not host.identified:
        return None
    return (
        f"To stop being asked about {display_label(host)}, the person can run "
        f"`moneybin privacy grant smart-import-parsing --recipient {host.name}` "
        "in their own terminal; an approval passed back through the agent covers only this request."
    )


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
