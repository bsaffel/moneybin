"""MCP consent before a PDF's content reaches the AI host (pdf-disclosure-consent.md).

The service stays synchronous and only acts on a verified ``DisclosureConsent``;
this module obtains one: a remembered ``ai_host`` grant, an elicitation prompt,
or a relayed single-use token. Without one it raises ``DisclosureConsentPendingError``
carrying the content-free consent request.
"""

from __future__ import annotations

import asyncio
import hashlib
import threading
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal

from fastmcp.server.dependencies import get_context
from fastmcp.server.elicitation import AcceptedElicitation

from moneybin import error_codes
from moneybin.adapters.pdf_disclosure_adapters import (
    consent_request,
    display_label,
    file_names,
    remember_text,
    remembered_notice,
)
from moneybin.config import get_settings
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp.confirmation import (
    ConfirmationBinding,
    ConfirmationBroker,
    confirmation_broker,
)
from moneybin.mcp.elicitation import elicit_bounded, supports_elicitation
from moneybin.privacy.disclosure import (
    REDACTION_POLICY_VERSION,
    AIHost,
    disclosure_request_id,
    host_from_client_info,
    risk_statement,
    terms_version_for,
)
from moneybin.services.import_confirmation import (
    AIAssistanceRequired,
    DisclosureApproval,
    DisclosureConsent,
)

if TYPE_CHECKING:
    from fastmcp.server.context import Context

_SITE = "pdf_disclosure"
_HOST_CATEGORY = "smart-import-parsing"
_MAX_ISSUED = 1024


@dataclass
class DisclosureAnswer:
    """Elicited answer when the AI host is identified and remember is offered."""

    accept: bool
    remember: bool = False


@dataclass
class _OneTimeAnswer:
    """Elicited answer for an unidentified host: no remember option."""

    accept: bool


@dataclass(frozen=True)
class _IssuedToken:
    issued_at: datetime
    statement: str


class DisclosureConsentPendingError(Exception):
    """No consent on this call; carries the content-free consent request."""

    def __init__(self, consent_request: dict[str, Any], actions: list[str]) -> None:
        """Hold the §1 consent request and the agent's next steps."""
        super().__init__("ai_assistance_required")
        self.consent_request = consent_request
        self.actions = actions


@dataclass(frozen=True)
class ObtainedConsent:
    """A verified consent awaiting release; settle it only after the release.

    ``remember_prompt`` is the exact text shown, stored as the grant's prompt.
    """

    consent: DisclosureConsent
    notice: str | None
    pending: tuple[AIAssistanceRequired, ...]
    remember_prompt: str | None


# Issue time and the exact statement shown, keyed by token digest; the broker
# stays generic. Revocation after issue invalidates a token (spec section 2).
_issued: dict[bytes, _IssuedToken] = {}
_issued_lock = threading.Lock()


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _token_digest(token: str) -> bytes:
    return hashlib.sha256(token.encode()).digest()


def _active_context() -> Context | None:
    try:
        return get_context()
    except RuntimeError:
        return None


def current_ai_host(ctx: Context | None) -> AIHost:
    """The AI host as reported at initialize; any missing piece is unidentified."""
    if ctx is None:
        return host_from_client_info(None, None, transport="stdio")
    try:
        params = ctx.session.client_params
    except (RuntimeError, AttributeError):
        params = None
    info = getattr(params, "clientInfo", None)
    name = getattr(info, "name", None)
    version = getattr(info, "version", None)
    transport = getattr(ctx, "transport", None)
    if transport == "stdio":
        return host_from_client_info(name, version, transport="stdio")
    if transport in ("sse", "streamable-http"):
        return host_from_client_info(name, version, transport="http")
    # Unknown run mode: show the reported name, never remember for it.
    host = host_from_client_info(name, version, transport="stdio")
    return replace(host, identified=False)


def disclosure_binding(
    *,
    file_sha256s: Sequence[str],
    request_kind: str,
    host: AIHost,
    profile: str,
) -> ConfirmationBinding:
    """Bind a relay token to the files, recipient, terms and redaction policy."""
    return ConfirmationBinding(
        arguments={
            "request_kind": request_kind,
            "recipient": host.recipient_key or "unidentified",
            "terms_version": terms_version_for(host),
            "redaction_policy": REDACTION_POLICY_VERSION,
        },
        resolved_ids=tuple(sorted(set(file_sha256s))),
        actor="mcp",
        profile=profile,
        authorization_context=host.transport,
        operation_kind="pdf_disclosure",
        blast_radius={"files": len(file_sha256s)},
    )


def _request_kind(pending: Sequence[AIAssistanceRequired]) -> str:
    kinds = sorted({p.request_kind for p in pending})
    return kinds[0] if len(kinds) == 1 else "+".join(kinds)


def _record(
    host: AIHost,
    pending: Sequence[AIAssistanceRequired],
    *,
    outcome: Literal["approved", "declined", "timeout", "expired"],
    approved_via: DisclosureApproval,
    remembered: bool,
    grant_id: str | None,
    refusal_code: str | None = None,
    binding_id: str | None = None,
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
        )


def _remember(host: AIHost, approved_via: DisclosureApproval, prompt: str) -> str:
    from moneybin.services.consent_service import ConsentService

    with get_database(read_only=False) as db:
        grant, _created = ConsentService(db).grant_host_consent(
            host=host, approved_via=approved_via, actor="mcp", grant_prompt=prompt
        )
    return grant.grant_id


def _remembered_grant_id(host: AIHost) -> str | None:
    from moneybin.services.consent_service import ConsentService

    if host.recipient_key is None:
        return None
    with get_database(read_only=True) as db:
        grant = ConsentService(db).active_host_grant(host=host)
    return grant.grant_id if grant is not None else None


def _revoked_since(host: AIHost, since: datetime) -> bool:
    from moneybin.services.consent_service import ConsentService

    key = host.recipient_key
    if key is None:
        return False
    with get_database(read_only=True) as db:
        grants = ConsentService(db).list_grants(include_revoked=True)
    for g in grants:
        if (
            g.recipient_kind != "ai_host"
            or g.feature_category != _HOST_CATEGORY
            or g.backend != key
            or g.revoked_at is None
        ):
            continue
        # A naive instant cannot be ordered against the issue time: fail closed.
        if g.revoked_at.utcoffset() is None or g.revoked_at >= since:
            return True
    return False


def _elicitation_message(host: AIHost, statement: str) -> str:
    if host.identified:
        tail = (
            f" Set remember to true to send future PDFs to {display_label(host)} "
            "without asking."
        )
    else:
        tail = (
            " MoneyBin cannot identify this AI app, so this consent covers this "
            "request only."
        )
    return f"{statement}{tail} Set accept to true only if you agree."


def _retry_call(tool: str, pending: Sequence[AIAssistanceRequired], extra: str) -> str:
    paths = list(dict.fromkeys(p.file_path for p in pending))
    if tool == "import_preview":
        return f"import_preview(file_path={paths[0]!r}{extra})"
    return f"import_files(paths={paths!r}{extra})"


def _relay_actions(
    host: AIHost, pending: Sequence[AIAssistanceRequired], token: str, tool: str
) -> list[str]:
    label = display_label(host)
    call = _retry_call(tool, pending, f", confirmation_token={token!r}")
    remember = _relay_remember_text(host)
    remember_hint = (
        f' If they also want that, show them "{remember}" verbatim and add '
        "remember_disclosure=True."
        if remember is not None
        else " Remembering is unavailable: MoneyBin cannot identify this AI app."
    )
    return [
        "MoneyBin could not parse this PDF locally and needs AI help. Nothing "
        "from the document has been sent. Show the person "
        "data.consent_request.risk_statement verbatim and ask whether to send "
        f"the document to {label}.",
        f"Only on a clear yes, call {call} before it expires.{remember_hint}",
        "On no, stop: nothing is sent, and the file can still be imported as a "
        "queryable seed from a terminal with `moneybin import files`.",
    ]


def _relay_remember_text(host: AIHost) -> str | None:
    return remember_text(host, lead="Remembering this consent")


def _refusal_actions(
    reason: str, pending: Sequence[AIAssistanceRequired], tool: str
) -> list[str]:
    what = "declined" if reason == "declined" else "did not answer"
    return [
        f"The person {what} the consent prompt; nothing from the document was "
        "sent. Do not retry unless they ask.",
        f"To ask again, call {_retry_call(tool, pending, '')}.",
    ]


def _token_on_eliciting_client() -> UserError:
    return UserError(
        "confirmation_token is not accepted here: this client can show the "
        "consent prompt, so the person answers it directly.",
        code=error_codes.MUTATION_INVALID_INPUT,
        hint="Repeat the call without confirmation_token.",
    )


def _revoked_error() -> UserError:
    return UserError(
        "Consent for this AI app was revoked after this token was issued.",
        code=error_codes.MUTATION_CONFIRMATION_EXPIRED,
        hint="Repeat the call without confirmation_token to ask again.",
    )


def _remember_issued(token: str, entry: _IssuedToken) -> None:
    with _issued_lock:
        _issued[_token_digest(token)] = entry
        while len(_issued) > _MAX_ISSUED:
            del _issued[next(iter(_issued))]


def _pop_issued(token: str) -> _IssuedToken | None:
    with _issued_lock:
        return _issued.pop(_token_digest(token), None)


async def obtain_disclosure_consent(
    *,
    pending: Sequence[AIAssistanceRequired],
    confirmation_token: str | None,
    remember: bool,
    tool: Literal["import_preview", "import_files"],
    broker: ConfirmationBroker = confirmation_broker,
) -> ObtainedConsent:
    """Return a verified consent or raise ``DisclosureConsentPendingError``.

    Order: remembered grant, elicitation, relay token, else issue a token.
    Refusals are audited here; an approval is remembered and audited by
    ``settle_disclosure_consent`` only after the release succeeds.
    """
    if not pending:
        raise ValueError("obtain_disclosure_consent needs at least one file")
    ctx = _active_context()
    host = current_ai_host(ctx)
    statement = risk_statement(host, file_names=file_names(pending))

    grant_id = await asyncio.to_thread(_remembered_grant_id, host)
    if grant_id is not None:
        return ObtainedConsent(
            DisclosureConsent(
                host=host,
                approved_via="remembered",
                grant_id=grant_id,
                binding_id=None,
                remembered=True,
                file_sha256s=None,
            ),
            notice=remembered_notice(host, grant_id),
            pending=tuple(pending),
            remember_prompt=None,
        )

    binding_id = disclosure_request_id(
        file_sha256s=[p.file_sha256 for p in pending], host=host
    )
    if ctx is not None and supports_elicitation(ctx):
        if confirmation_token is not None:
            # Refuse before consuming: the prompt is the stronger path.
            raise _token_on_eliciting_client()
        return await _elicit(ctx, host, pending, statement, binding_id, tool)

    if confirmation_token is not None:
        return await _redeem(
            host, pending, confirmation_token, remember, binding_id, broker
        )

    binding = disclosure_binding(
        file_sha256s=[p.file_sha256 for p in pending],
        request_kind=_request_kind(pending),
        host=host,
        profile=get_settings().profile,
    )
    now = _utcnow()
    token = broker.issue(binding, now=now)
    _remember_issued(token, _IssuedToken(issued_at=now, statement=statement))
    raise DisclosureConsentPendingError(
        consent_request(
            host=host,
            pending=pending,
            statement=statement,
            token=token,
            expires_at=now + timedelta(seconds=broker.ttl_seconds),
            refusal_reason=None,
        ),
        _relay_actions(host, pending, token, tool),
    )


async def _elicit(
    ctx: Context,
    host: AIHost,
    pending: Sequence[AIAssistanceRequired],
    statement: str,
    binding_id: str,
    tool: Literal["import_preview", "import_files"],
) -> ObtainedConsent:
    message = _elicitation_message(host, statement)
    response_type: type[DisclosureAnswer] | type[_OneTimeAnswer] = (
        DisclosureAnswer if host.identified else _OneTimeAnswer
    )
    result = await elicit_bounded(
        ctx.elicit(message, response_type=response_type),
        site=_SITE,
    )
    accepted = isinstance(result, AcceptedElicitation) and (
        getattr(result.data, "accept", False) is True
    )
    if not accepted:
        reason: Literal["declined", "timeout"] = (
            "timeout" if result is None else "declined"
        )
        await asyncio.to_thread(
            _record,
            host,
            pending,
            outcome=reason,
            approved_via="prompt",
            remembered=False,
            grant_id=None,
            binding_id=binding_id,
        )
        raise DisclosureConsentPendingError(
            consent_request(
                host=host,
                pending=pending,
                statement=statement,
                token=None,
                expires_at=None,
                refusal_reason=reason,
            ),
            _refusal_actions(reason, pending, tool),
        )
    data = result.data if isinstance(result, AcceptedElicitation) else None
    wants_remember = host.identified and getattr(data, "remember", False) is True
    return _one_time(
        host,
        pending,
        "prompt",
        binding_id,
        remember_prompt=message if wants_remember else None,
    )


def _one_time(
    host: AIHost,
    pending: Sequence[AIAssistanceRequired],
    approved_via: DisclosureApproval,
    binding_id: str,
    *,
    remember_prompt: str | None,
) -> ObtainedConsent:
    return ObtainedConsent(
        DisclosureConsent(
            host=host,
            approved_via=approved_via,
            grant_id=None,
            binding_id=binding_id,
            remembered=False,
            file_sha256s=frozenset(p.file_sha256 for p in pending),
        ),
        notice=None,
        pending=tuple(pending),
        remember_prompt=remember_prompt,
    )


def settle_disclosure_consent(obtained: ObtainedConsent) -> None:
    """After a successful release: remember if asked, then audit the approval."""
    consent = obtained.consent
    grant_id = consent.grant_id
    if obtained.remember_prompt is not None:
        grant_id = _remember(
            consent.host, consent.approved_via, obtained.remember_prompt
        )
    _record(
        consent.host,
        obtained.pending,
        outcome="approved",
        approved_via=consent.approved_via,
        remembered=grant_id is not None,
        grant_id=grant_id,
        binding_id=consent.binding_id,
    )


async def _redeem(
    host: AIHost,
    pending: Sequence[AIAssistanceRequired],
    token: str,
    remember: bool,
    binding_id: str,
    broker: ConfirmationBroker,
) -> ObtainedConsent:
    issued = _pop_issued(token)
    try:
        grant = broker.consume(token, now=_utcnow())
        grant.verify(
            disclosure_binding(
                file_sha256s=[p.file_sha256 for p in pending],
                request_kind=_request_kind(pending),
                host=host,
                profile=get_settings().profile,
            )
        )
        if issued is None:
            raise UserError(
                "Confirmation token has expired.",
                code=error_codes.MUTATION_CONFIRMATION_EXPIRED,
            )
        if await asyncio.to_thread(_revoked_since, host, issued.issued_at):
            raise _revoked_error()
    except UserError as refused:
        await asyncio.to_thread(
            _record,
            host,
            pending,
            outcome="expired",
            approved_via="agent_relay",
            remembered=False,
            grant_id=None,
            refusal_code=refused.code,
            binding_id=binding_id,
        )
        raise
    # The stored prompt is what the agent was told to show, remembering included.
    shown = _relay_remember_text(host) if remember else None
    return _one_time(
        host,
        pending,
        "agent_relay",
        binding_id,
        remember_prompt=f"{issued.statement} {shown}" if shown else None,
    )
