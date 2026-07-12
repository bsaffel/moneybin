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

from moneybin import error_codes
from moneybin.connectors.sync_errors import SyncError
from moneybin.database import (
    DatabaseCryptoError,
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

        from moneybin import error_codes
        raise UserError("Category not found", code=error_codes.MUTATION_NOT_FOUND)

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
        if self.recovery_actions is not None:
            # Coerce plain dicts defensively (mirrors ResponseEnvelope.to_dict):
            # callers SHOULD pass RecoveryAction instances, but a dict slipping
            # in (e.g., from deserialized JSON) would otherwise AttributeError.
            d["recovery_actions"] = [
                ra if isinstance(ra, dict) else ra.model_dump()
                for ra in self.recovery_actions
            ]
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
        suggest_create = False
        try:  # deferred imports avoid an errors<->services import cycle
            from moneybin.config import get_current_profile
            from moneybin.services.profile_service import ProfileService

            profiles = ProfileService()
            profile = get_current_profile(auto_resolve=False)
            # Registration, not the directory, decides the verb. `profile create`
            # completes an unregistered directory in place (config, database, and
            # inbox), so it is the right advice whether or not one is already there.
            # A *registered* profile has finished setup and is only missing its
            # database — that is `db init`'s job, and `profile create` would refuse.
            suggest_create = not profiles.is_registered(profile)
        except Exception:  # noqa: BLE001 — fall back to the db-init message
            suggest_create = False
        if suggest_create:
            message = (
                "Profile not set up. Run 'moneybin profile create <name> "
                "--init-inbox' to create the profile (config, database, and inbox)."
            )
        else:
            message = (
                "Database not found. Run 'moneybin db init' to initialize it first."
            )
        return UserError(
            message,
            code=error_codes.INFRA_DATABASE_NOT_INITIALIZED,
        )
    if isinstance(exc, DatabaseLockError):
        return UserError(
            str(exc),
            code=error_codes.INFRA_DATABASE_LOCKED,
            hint="💡 Run 'moneybin db ps' for details or wait and retry",
            recovery_actions=[
                RecoveryAction(
                    tool="system_status",
                    # No arguments: system_status takes none, and its
                    # database_connections block (always present) names the
                    # holder. A section filter would return a subset of an
                    # already-cheap payload.
                    rationale=(
                        "Inspect the database_connections block of system_status "
                        "to identify the process holding the database, then decide "
                        "whether to wait, retry, or surface to the user."
                    ),
                    # "suggested", not "certain": system_status diagnoses the
                    # contention but does not resolve it — the agent still has to
                    # choose wait/retry/surface from what it learns.
                    confidence="suggested",
                    idempotent=True,
                ),
            ],
        )
    if isinstance(exc, DatabaseKeyError):
        return UserError(
            str(exc),
            code=error_codes.INFRA_WRONG_KEY,
            hint=database_key_error_hint(),
        )
    if isinstance(exc, DatabaseCryptoError):
        # The exception already carries a crafted, actionable message (which
        # extension is missing and why the first write needs network). Preserve
        # it and add the one-line recovery hint the other Database*Errors carry.
        return UserError(
            str(exc),
            code=error_codes.INFRA_CRYPTO_UNAVAILABLE,
            hint=(
                "💡 Run one write while online so DuckDB can fetch its crypto "
                "extension from extensions.duckdb.org, then retry offline."
            ),
        )
    if isinstance(exc, SchemaDriftError):
        return UserError(
            str(exc),
            code=error_codes.INFRA_SCHEMA_DRIFT,
            hint="💡 Run 'moneybin transform apply' to rebuild stale models",
        )
    if isinstance(exc, FileNotFoundError):
        # Drop the "[Errno 2]" prefix that str(FileNotFoundError) includes —
        # end users don't need the errno number.
        msg = f"{exc.strerror}: {exc.filename}" if exc.filename else str(exc)
        return UserError(msg, code=error_codes.INFRA_FILE_NOT_FOUND)
    if isinstance(exc, OSError) and not isinstance(exc, TimeoutError):
        msg = f"{exc.strerror}: {exc.filename}" if exc.filename else str(exc)
        return UserError(msg, code=error_codes.INFRA_IO_ERROR)
    if isinstance(exc, ValueError):
        # Generic ValueError fires on read paths too (date/enum/decimal parsing
        # in reports, query filters, etc.) — INFRA_INVALID_INPUT is prefix-
        # neutral about write-vs-read, parallel to INFRA_NOT_FOUND. Write
        # callers that mean "the entity-shape you wrote is invalid" should
        # raise UserError(code=MUTATION_INVALID_INPUT) directly at the site.
        return UserError(str(exc), code=error_codes.INFRA_INVALID_INPUT)
    if isinstance(exc, InvalidOperation):
        return UserError(
            f"invalid decimal value: {exc}", code=error_codes.INFRA_INVALID_INPUT
        )
    if isinstance(exc, LookupError) and not isinstance(exc, (KeyError, IndexError)):
        # Generic LookupError fires on read paths (account/category/note lookups)
        # — INFRA_NOT_FOUND is prefix-neutral about write-vs-read context, unlike
        # MUTATION_NOT_FOUND which would mis-signal "this was a write attempt"
        # to agents branching on the code's prefix.
        return UserError(str(exc), code=error_codes.INFRA_NOT_FOUND)
    if isinstance(exc, SyncError):
        return UserError(str(exc), code=error_codes.SYNC_ERROR)
    return None
