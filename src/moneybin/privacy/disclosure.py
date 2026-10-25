"""AI host identity, vendor terms, and the one shared PDF-disclosure risk statement."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

REDACTION_POLICY_VERSION = "1"

Transport = Literal["stdio", "http", "cli"]


@dataclass(frozen=True)
class VendorTerms:
    """Release-shipped summary of a vendor's data terms; states the plan range."""

    vendor: str
    retention: str
    training_use: str
    policy_url: str
    plan_caveat: str
    terms_version: str


@dataclass(frozen=True)
class AIHost:
    """The AI app on the receiving end, as it reports itself."""

    name: str | None
    version: str | None
    identified: bool
    source: Literal["reported_by_client", "cli_environment", "none"]
    transport: Transport
    display_name: str | None
    terms: VendorTerms | None

    @property
    def recipient_key(self) -> str | None:
        """Transport class plus normalized name; None when unidentified."""
        if not self.identified or self.name is None:
            return None
        transport_class = "remote" if self.transport == "http" else "local"
        return f"{transport_class}:{self.name}"


@dataclass(frozen=True)
class _HostEntry:
    display_name: str
    client_names: tuple[str, ...]  # verified MCP initialize clientInfo.name values
    env_markers: tuple[str, ...]  # (VAR, value) pairs rendered as "VAR=value"
    terms: VendorTerms


_PLAN_CAVEAT = (
    "Terms differ for consumer, business and API plans; MoneyBin cannot see yours."
)

_ANTHROPIC_TERMS = VendorTerms(
    vendor="Anthropic",
    retention=(
        "API data is kept about 30 days for trust and safety; consumer plans "
        "can keep conversations longer"
    ),
    training_use=(
        "API data is not used for training; consumer plans may use "
        "conversations for training depending on your settings"
    ),
    policy_url="https://www.anthropic.com/legal/privacy",
    plan_caveat=_PLAN_CAVEAT,
    terms_version="anthropic-2026-10",
)

_CURSOR_TERMS = VendorTerms(
    vendor="varies by model",
    retention="depends on the model selected in Cursor",
    training_use="depends on the model selected in Cursor and your Cursor privacy settings",
    policy_url="https://cursor.com/privacy",
    plan_caveat=_PLAN_CAVEAT,
    terms_version="cursor-2026-10",
)

# Keys are normalized host ids. Sources: Claude Desktop reports "claude-ai" in
# initialize (MCP discussion #1231); Cursor reports "cursor-vscode" (Cursor
# forum thread 77248); Claude Code sets CLAUDECODE=1 in spawned shells (Claude
# Code env-vars docs). Hosts not verified from a primary source are omitted.
_HOSTS: dict[str, _HostEntry] = {
    "claude-ai": _HostEntry("Claude", ("claude-ai",), (), _ANTHROPIC_TERMS),
    "claude-code": _HostEntry("Claude Code", (), ("CLAUDECODE=1",), _ANTHROPIC_TERMS),
    "cursor": _HostEntry("Cursor", ("cursor-vscode",), (), _CURSOR_TERMS),
}


_MAX_REPORTED_NAME = 64


def _normalize(name: str) -> str:
    return name.strip().lower()


def _printable_capped(name: str) -> str:
    """Self-reported names reach consent text: printable characters only, capped."""
    return "".join(c for c in name if c.isprintable())[:_MAX_REPORTED_NAME]


def _entry_for_client_name(name: str) -> tuple[str, _HostEntry] | None:
    for host_id, entry in _HOSTS.items():
        if name in entry.client_names:
            return host_id, entry
    return None


def host_from_client_info(
    name: str | None,
    version: str | None,
    *,
    transport: Literal["stdio", "http"],
) -> AIHost:
    """Identify the host from MCP initialize clientInfo; unknown names stay unidentified."""
    if not name or not name.strip():
        return AIHost(None, version, False, "none", transport, None, None)
    normalized = _normalize(name)
    match = _entry_for_client_name(normalized)
    if match is None:
        return AIHost(
            _printable_capped(normalized),
            version,
            False,
            "reported_by_client",
            transport,
            None,
            None,
        )
    host_id, entry = match
    return AIHost(
        host_id,
        version,
        True,
        "reported_by_client",
        transport,
        entry.display_name,
        entry.terms,
    )


def host_from_environment(env: Mapping[str, str]) -> AIHost:
    """Identify the CLI-invoking agent host from known environment markers."""
    for host_id, entry in _HOSTS.items():
        for marker in entry.env_markers:
            var, _, value = marker.partition("=")
            if env.get(var) == value:
                return AIHost(
                    host_id,
                    None,
                    True,
                    "cli_environment",
                    "cli",
                    entry.display_name,
                    entry.terms,
                )
    return AIHost(None, None, False, "none", "cli", None, None)


def host_for_recipient_name(name: str) -> AIHost | None:
    """The CLI-transport host for a known host name, or None if unknown."""
    host_id = _normalize(name)
    entry = _HOSTS.get(host_id)
    if entry is None:
        return None
    return AIHost(
        host_id, None, True, "cli_environment", "cli", entry.display_name, entry.terms
    )


def known_host_names() -> tuple[str, ...]:
    """Normalized ids of the hosts in the release table."""
    return tuple(_HOSTS)


def terms_version_for(host: AIHost) -> str:
    """The host's vendor terms version, or "none"."""
    return host.terms.terms_version if host.terms else "none"


def risk_statement(host: AIHost, *, file_names: Sequence[str]) -> str:
    """The one consent text shared by every surface (spec section 10)."""
    files = ", ".join(file_names) or "your PDF statements"
    label = host.display_name or host.name or "your AI app"
    if host.terms is None:
        terms = (
            f"MoneyBin has no summary of {label}'s data terms; "
            "check its privacy policy before continuing."
        )
    elif host.terms.vendor == "varies by model":
        terms = (
            f"What happens to it depends on the model selected in {label}. "
            f"Policy: {host.terms.policy_url}. {host.terms.plan_caveat}"
        )
    else:
        t = host.terms
        terms = (
            f"{t.vendor} terms: {t.retention}. {t.training_use}. "
            f"Policy: {t.policy_url}. {t.plan_caveat}"
        )
    return (
        f"To read {files}, MoneyBin will send its statement text and table rows "
        f"to {label} (as reported by your AI app). Only the account numbers, "
        "routing numbers and SSNs MoneyBin identified are redacted; everything "
        "else, including any it did not identify, is sent as-is. "
        f"{terms} "
        "Once sent, MoneyBin cannot pull it back. "
        "This covers what MoneyBin sends. If you already shared this file with "
        "your AI app directly, it already has the contents. "
        "To revoke a remembered consent, run `moneybin privacy revoke`."
    )


def disclosure_request_id(*, file_sha256s: Sequence[str], host: AIHost) -> str:
    """Deterministic 16-hex id binding files, recipient, terms and redaction policy."""
    payload = "|".join((
        "|".join(sorted(file_sha256s)),
        host.recipient_key or "unidentified",
        terms_version_for(host),
        REDACTION_POLICY_VERSION,
    ))
    return hashlib.sha256(payload.encode()).hexdigest()[:16]
