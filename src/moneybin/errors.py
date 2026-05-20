"""Cross-cutting user-facing error classification.

Translates internal exceptions into structured ``UserError`` values that CLI
and MCP surfaces deliver via their own conventions:

- CLI: ``handle_cli_errors`` logs the message and exits with code 1.
- MCP: ``mcp_tool`` decorator catches UserError and returns an error envelope.

Unrecognized exceptions return ``None`` from ``classify_user_error`` so they
propagate as 500-equivalent failures — programmer errors must not be silently
translated into user-facing messages.
"""

from __future__ import annotations

from decimal import InvalidOperation
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from moneybin.connectors.sync_errors import SyncError
from moneybin.database import (
    DatabaseKeyError,
    DatabaseLockError,
    DatabaseNotInitializedError,
    SchemaDriftError,
    database_key_error_hint,
)


class RecoveryAction(BaseModel):
    """One structured action an agent can execute to fix a failure.

    Carried in the optional `recovery_actions` field on both UserError
    and ResponseEnvelope. Agents read the field, pick the highest-confidence
    action they're authorized to run, and invoke `tool(**arguments)`.

    Semantics:
    - tool: an MCP tool name (e.g. "system_audit_undo"). For CLI parity,
      the same string maps to a CLI command via the surface registry.
    - arguments: pre-filled arguments the agent can execute directly. No
      placeholder strings; if a value isn't known at error-construction
      time, the action belongs as `confidence="suggested"` with the
      missing argument named in rationale.
    - rationale: short prose explaining WHY this action fixes the failure.
      One sentence. Agent surfaces this to the user when confirming.
    - confidence: "certain" = this will fix it; "suggested" = the agent
      should weigh other context and may need user input.
    - idempotent: True if running the action twice is safe — agents can
      retry on transient failures without confirming.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    tool: str = Field(..., min_length=1)
    arguments: dict[str, Any] = Field(default_factory=dict)
    rationale: str = Field(..., min_length=1)
    confidence: Literal["certain", "suggested"]
    idempotent: bool


class UserError(Exception):
    """A classified, user-facing error that can be raised and caught.

    Carries a sanitized message safe to show end users, a stable code for
    programmatic handling, an optional hint pointing at recovery steps, and
    optional structured recovery actions an agent can execute to fix the failure.

    Can be raised directly in tool code::

        raise UserError("Category not found", code="NOT_FOUND")

    The ``mcp_tool`` decorator catches this and converts it to an error
    ``ResponseEnvelope`` automatically.
    """

    def __init__(
        self,
        message: str,
        *,
        code: str,
        hint: str | None = None,
        details: dict[str, Any] | None = None,
        recovery_actions: list[RecoveryAction] | None = None,
    ) -> None:
        """Initialize with a user-safe message and optional metadata."""
        super().__init__(message)
        self.message = message
        self.code = code
        self.hint = hint
        self.details = details
        self.recovery_actions = recovery_actions

    def to_dict(self) -> dict[str, Any]:
        """Convert to a plain dict for envelope serialization."""
        d: dict[str, Any] = {"message": self.message, "code": self.code}
        if self.hint is not None:
            d["hint"] = self.hint
        if self.details is not None:
            d["details"] = self.details
        return d


def classify_user_error(exc: BaseException) -> UserError | None:
    """Map a known exception to a ``UserError``, or ``None`` if unexpected.

    Returning ``None`` for unrecognized exceptions is intentional: callers
    should re-raise so programmer errors surface as failures rather than
    being translated into user-facing messages.
    """
    if isinstance(exc, UserError):
        return exc
    if isinstance(exc, DatabaseNotInitializedError):
        return UserError(
            "Database not found. Run 'moneybin db init' to initialize it first.",
            code="database_not_initialized",
        )
    if isinstance(exc, DatabaseLockError):
        return UserError(
            str(exc),
            code="database_locked",
            hint="💡 Run 'moneybin db ps' for details or wait and retry",
        )
    if isinstance(exc, DatabaseKeyError):
        return UserError(
            str(exc),
            code="wrong_key",
            hint=database_key_error_hint(),
        )
    if isinstance(exc, SchemaDriftError):
        return UserError(
            str(exc),
            code="schema_drift",
            hint="💡 Run 'moneybin transform apply' to rebuild stale models",
        )
    if isinstance(exc, FileNotFoundError):
        # Drop the "[Errno 2]" prefix that str(FileNotFoundError) includes —
        # end users don't need the errno number.
        msg = f"{exc.strerror}: {exc.filename}" if exc.filename else str(exc)
        return UserError(msg, code="file_not_found")
    if isinstance(exc, OSError) and not isinstance(exc, TimeoutError):
        msg = f"{exc.strerror}: {exc.filename}" if exc.filename else str(exc)
        return UserError(msg, code="io_error")
    if isinstance(exc, ValueError):
        return UserError(str(exc), code="invalid_input")
    if isinstance(exc, InvalidOperation):
        return UserError(f"invalid decimal value: {exc}", code="invalid_input")
    if isinstance(exc, LookupError) and not isinstance(exc, (KeyError, IndexError)):
        return UserError(str(exc), code="not_found")
    if isinstance(exc, SyncError):
        return UserError(str(exc), code="sync_error")
    return None
