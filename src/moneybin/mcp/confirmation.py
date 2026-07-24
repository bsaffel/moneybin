"""Payload-bound confirmation for high-impact MCP operations."""

from __future__ import annotations

import hashlib
import json
import secrets
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from fastmcp.server.dependencies import get_context
from fastmcp.server.elicitation import AcceptedElicitation
from pydantic import BaseModel, ConfigDict, JsonValue, NonNegativeInt

from moneybin import error_codes
from moneybin.config import (
    DEFAULT_CONFIRMATION_TTL_SECONDS,
    MAX_CONFIRMATION_TTL_SECONDS,
    MIN_CONFIRMATION_TTL_SECONDS,
    get_settings,
)
from moneybin.errors import UserError
from moneybin.mcp.elicitation import supports_elicitation

if TYPE_CHECKING:
    from fastmcp.server.context import Context

_MAX_EXPIRED_TOKENS = 1024


class ConfirmationBinding(BaseModel):
    """Canonical description of the exact mutation a user approves."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    arguments: dict[str, JsonValue]
    resolved_ids: tuple[str, ...]
    actor: str
    profile: str
    authorization_context: str
    operation_kind: str
    blast_radius: dict[str, NonNegativeInt]

    def canonical_bytes(self) -> bytes:
        """Return deterministic JSON bytes for confirmation binding."""
        payload = self.model_dump(mode="json")
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


@dataclass(frozen=True)
class _Entry:
    digest: bytes
    expires_at: datetime


@dataclass(frozen=True)
class ConfirmationGrant:
    """Immutable proof that one exact binding received confirmation."""

    _digest: bytes

    def verify(self, binding: ConfirmationBinding) -> None:
        """Raise unless ``binding`` exactly matches the confirmed digest."""
        _require_digest(self._digest, binding)


def _binding_digest(binding: ConfirmationBinding) -> bytes:
    return hashlib.sha256(binding.canonical_bytes()).digest()


def _expired() -> UserError:
    return UserError(
        "Confirmation token has expired.",
        code=error_codes.MUTATION_CONFIRMATION_EXPIRED,
    )


def _replayed_or_unknown() -> UserError:
    return UserError(
        "Confirmation token is unknown or already used.",
        code=error_codes.MUTATION_CONFIRMATION_REPLAYED,
    )


def _mismatch() -> UserError:
    return UserError(
        "Confirmation no longer matches the requested mutation.",
        code=error_codes.MUTATION_CONFIRMATION_MISMATCH,
    )


def _confirmation_declined() -> UserError:
    return UserError(
        "The high-impact operation was not confirmed.",
        code=error_codes.MUTATION_CONFIRMATION_DECLINED,
        details={"reason": "declined"},
    )


def _require_digest(expected: bytes, binding: ConfirmationBinding) -> None:
    actual = _binding_digest(binding)
    if not secrets.compare_digest(expected, actual):
        raise _mismatch()


class ConfirmationBroker:
    """Issue and consume process-local, opaque, single-use confirmation tokens."""

    def __init__(self, *, ttl_seconds: int | None = None) -> None:
        """Initialize an empty broker with the configured confirmation TTL."""
        if ttl_seconds is not None and not (
            MIN_CONFIRMATION_TTL_SECONDS <= ttl_seconds <= MAX_CONFIRMATION_TTL_SECONDS
        ):
            raise ValueError(
                "Confirmation TTL must be between "
                f"{MIN_CONFIRMATION_TTL_SECONDS} and "
                f"{MAX_CONFIRMATION_TTL_SECONDS} seconds inclusive."
            )
        self._ttl_seconds = ttl_seconds
        self._lock = threading.Lock()
        self._entries: dict[str, _Entry] = {}
        self._expired_tokens: dict[str, None] = {}

    @property
    def ttl_seconds(self) -> int:
        """Return the configured token lifetime."""
        if self._ttl_seconds is not None:
            return self._ttl_seconds
        try:
            return get_settings().mcp.confirmation_ttl_seconds
        except RuntimeError:
            return DEFAULT_CONFIRMATION_TTL_SECONDS

    def issue(self, binding: ConfirmationBinding, *, now: datetime) -> str:
        """Issue a process-local opaque token for exactly ``binding``."""
        ttl_seconds = self.ttl_seconds
        entry = _Entry(
            digest=_binding_digest(binding),
            expires_at=now + timedelta(seconds=ttl_seconds),
        )
        with self._lock:
            self._evict_expired_locked(now)
            token = secrets.token_urlsafe(32)
            while token in self._entries or token in self._expired_tokens:
                token = secrets.token_urlsafe(32)
            self._entries[token] = entry
        return token

    def consume(
        self,
        token: str,
        *,
        now: datetime,
    ) -> ConfirmationGrant:
        """Consume ``token`` once and return its live immutable digest grant."""
        with self._lock:
            entry = self._entries.pop(token, None)
            was_evicted = token in self._expired_tokens
            if was_evicted:
                del self._expired_tokens[token]
            self._evict_expired_locked(now)
        if entry is None:
            if was_evicted:
                raise _expired()
            raise _replayed_or_unknown()
        if now >= entry.expires_at:
            raise _expired()
        return ConfirmationGrant(entry.digest)

    def _evict_expired_locked(self, now: datetime) -> None:
        """Remove abandoned expired entries while the broker lock is held."""
        expired_tokens = [
            token for token, entry in self._entries.items() if now >= entry.expires_at
        ]
        for token in expired_tokens:
            del self._entries[token]
            self._expired_tokens[token] = None
        while len(self._expired_tokens) > _MAX_EXPIRED_TOKENS:
            oldest_token = next(iter(self._expired_tokens))
            del self._expired_tokens[oldest_token]


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _active_context() -> Context | None:
    try:
        return get_context()
    except RuntimeError:
        return None


def _confirmation_required(
    token: str,
    binding: ConfirmationBinding,
    *,
    expires_in_seconds: int,
) -> UserError:
    return UserError(
        "This high-impact operation needs explicit confirmation.",
        code=error_codes.MUTATION_CONFIRMATION_REQUIRED,
        details={
            "confirmation_token": token,
            "expires_in_seconds": expires_in_seconds,
            "operation_kind": binding.operation_kind,
            "blast_radius": binding.blast_radius,
        },
    )


confirmation_broker = ConfirmationBroker()


async def grant_confirmation_or_raise(
    *,
    binding: ConfirmationBinding | None,
    message: str,
    confirmation_token: str | None,
    broker: ConfirmationBroker = confirmation_broker,
) -> ConfirmationGrant:
    """Return a digest grant through elicitation or one consumed opaque token."""
    if confirmation_token is not None:
        return broker.consume(confirmation_token, now=_utcnow())
    if binding is None:
        raise ValueError("binding is required when issuing or eliciting confirmation")

    ctx = _active_context()
    if ctx is not None and supports_elicitation(ctx):
        expected_digest = _binding_digest(binding)
        result = await ctx.elicit(
            message,
            response_type=bool,
            response_title="Confirm high-impact operation",
            response_description=(
                "Select true only after reviewing the exact operation."
            ),
        )
        if isinstance(result, AcceptedElicitation) and result.data is True:
            return ConfirmationGrant(expected_digest)
        raise _confirmation_declined()

    token = broker.issue(binding, now=_utcnow())
    raise _confirmation_required(
        token,
        binding,
        expires_in_seconds=broker.ttl_seconds,
    )
