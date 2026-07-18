"""Payload-bound confirmation for destructive MCP mutations."""

from __future__ import annotations

import hashlib
import json
import secrets
import threading
from collections.abc import Callable
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
        "The destructive mutation was not confirmed.",
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
            token = secrets.token_urlsafe(32)
            while token in self._entries:
                token = secrets.token_urlsafe(32)
            self._entries[token] = entry
        return token

    def consume(
        self,
        token: str,
        binding: ConfirmationBinding,
        *,
        now: datetime,
    ) -> ConfirmationBinding:
        """Consume ``token`` once if it is live and bound to ``binding``."""
        with self._lock:
            entry = self._entries.pop(token, None)
        if entry is None:
            raise _replayed_or_unknown()
        if now >= entry.expires_at:
            raise _expired()
        _require_digest(entry.digest, binding)
        return binding


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
        "This destructive mutation needs explicit confirmation.",
        code=error_codes.MUTATION_CONFIRMATION_REQUIRED,
        details={
            "confirmation_token": token,
            "expires_in_seconds": expires_in_seconds,
            "operation_kind": binding.operation_kind,
            "blast_radius": binding.blast_radius,
        },
    )


confirmation_broker = ConfirmationBroker()


async def confirm_exact_or_raise(
    *,
    binding: ConfirmationBinding,
    recompute: Callable[[], ConfirmationBinding],
    message: str,
    confirmation_token: str | None,
    broker: ConfirmationBroker = confirmation_broker,
) -> None:
    """Confirm the exact mutation through elicitation or one opaque token."""
    if confirmation_token is not None:
        broker.consume(confirmation_token, recompute(), now=_utcnow())
        return

    ctx = _active_context()
    if ctx is not None and supports_elicitation(ctx):
        expected_digest = _binding_digest(binding)
        result = await ctx.elicit(
            message,
            response_type=bool,
            response_title="Confirm destructive operation",
            response_description=(
                "Select true only after reviewing the exact operation."
            ),
        )
        if isinstance(result, AcceptedElicitation) and result.data is True:
            _require_digest(expected_digest, recompute())
            return
        raise _confirmation_declined()

    token = broker.issue(binding, now=_utcnow())
    raise _confirmation_required(
        token,
        binding,
        expires_in_seconds=broker.ttl_seconds,
    )
