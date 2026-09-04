"""Cross-cutting user-facing error classification.

Translates internal exceptions into structured ``UserError`` values that CLI
and MCP surfaces deliver via their own conventions:

- CLI: ``handle_cli_errors`` logs the message and exits with code 1.
- MCP: ``mcp_tool`` decorator catches UserError and returns an error envelope.

Unrecognized exceptions return ``None`` from ``classify_user_error`` so they
propagate as 500-equivalent failures — programmer errors must not be silently
translated into user-facing messages.

This module is a leaf: importing it pulls in nothing from ``moneybin`` but
``error_codes``, so any layer may raise a ``UserError`` without minting an
import cycle — which is what lets ``connectors.feed_errors`` and the price and
rate error modules subclass it outright. The domain families
``classify_user_error`` recognizes are imported inside the function, on the
failure path.
"""

from __future__ import annotations

import platform
import traceback
from decimal import InvalidOperation
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from moneybin import error_codes


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


class ErrorDetail(BaseModel):
    """The wire representation of a failure, carried on ResponseEnvelope.

    Distinct from `UserError`, which is the *raiseable* exception. Holding a
    live exception in a wire-bound dataclass forces every serializer to know
    how to flatten it; pydantic, for one, cannot generate a schema for an
    Exception subclass at all. This model is the plain-data projection the
    envelope carries instead.

    `recovery_actions` deliberately does NOT live here: the envelope's
    top-level field is the single canonical wire location for those.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    message: str
    code: str
    hint: str | None = None
    details: dict[str, Any] | None = None

    @classmethod
    def from_user_error(cls, error: UserError) -> ErrorDetail:
        """Project a raised UserError onto its wire shape."""
        return cls(
            message=error.message,
            code=error.code,
            hint=error.hint,
            details=error.details,
        )


# macOS gates these three directories behind a TCC grant. Verified by probe
# 2026-07-18: a POSIX mode denial (`chmod 000`) raises errno 13 EACCES
# "Permission denied" at `open()`, while a TCC denial raises errno 1 EPERM
# "Operation not permitted" at `Path.exists()` — `pathlib` only swallows
# ENOENT/ENOTDIR/EBADF/ELOOP, so EPERM propagates before any open.
_MACOS_PROTECTED_ROOTS = ("Documents", "Desktop", "Downloads")
_EPERM = 1
_EACCES = 13


def permission_advice(
    errno: int, platform: str, path: Path | None
) -> tuple[str, dict[str, Any]]:
    """Return (hint, details) for a permission failure.

    Performs no platform detection of its own: `platform` is the caller's
    `platform.system()` value, so the policy stays testable on any host.
    (Path resolution does touch the filesystem — only to canonicalize the
    path handed in, never to read the target.)

    `path` is None when the exception carried no filename. That stays
    generic rather than falling back to the working directory, which is not
    the path that failed.

    The macOS Full-Disk-Access advice fires ONLY on the conjunction of
    EPERM + Darwin + a protected root. EPERM alone is not proof of TCC —
    immutable (`uchg`) flags and sandbox denials produce it too — and sending
    someone to System Settings for one of those is a confident wrong answer,
    which is worse than an honest vague one.
    """
    details: dict[str, Any] = {"errno": errno, "platform": platform}

    if errno == _EACCES:
        return (
            "Check file ownership and permissions "
            "(e.g., chmod 644, or chown to your user).",
            details,
        )

    # The errno check is half the conjunction, not a formality: any errno that
    # is neither EACCES nor EPERM must fall through to the generic hint rather
    # than reach the protected-root test below.
    if errno == _EPERM:
        protected_root = _protected_root_for(platform, path)
        if protected_root is not None:
            details["protected_root"] = protected_root
            return (
                f"macOS blocks access to {protected_root} until you grant "
                "permission. Open System Settings → Privacy & Security → Full "
                "Disk Access, enable the app running MoneyBin (e.g. your "
                "terminal), restart it, then retry.",
                details,
            )

    return (
        "Something outside the file's own permissions is blocking access "
        "(e.g. a security policy, a sandbox, or an immutable flag).",
        details,
    )


def _protected_root_for(platform: str, path: Path | None) -> str | None:
    """Return the `~/<root>` label if `path` sits under a TCC-gated directory."""
    if platform != "Darwin" or path is None:
        return None
    try:
        # Non-strict `resolve()` is load-bearing, not laziness: under a live TCC
        # denial the OS refuses to stat the path, and non-strict `realpath`
        # answers with the joined path instead of raising — which is the only
        # reason the root comparison below still recognizes the denial.
        # `strict=True` would raise here on exactly the failure this function
        # exists to classify, silently downgrading every real TCC block to the
        # generic hint while every test (whose paths do resolve) stayed green.
        resolved = path.expanduser().resolve()
    except OSError:
        # An unresolvable path simply isn't classifiable — stay generic rather
        # than guess, per the conjunction rule above.
        return None
    home = Path.home()
    for root in _MACOS_PROTECTED_ROOTS:
        if resolved.is_relative_to(home / root):
            return f"~/{root}"
    return None


def exception_origin(exc: BaseException) -> str:
    """Where an exception was raised, without any of what it said.

    ``logger.exception`` writes the whole traceback, whose last line is
    ``<Type>: <str(exc)>`` — and an exception message can carry an amount, a
    description, or a SQL fragment. ``SanitizedLogFormatter`` is not a backstop
    for that: its money pattern requires a literal ``$``, so a bare
    ``-2412.55`` passes through unmasked. AGENTS.md forbids financial data in
    logs with no local-log carve-out, so the diagnosable part — the frame
    chain — is extracted and the message is dropped.

    Returns innermost-last, e.g. ``"tools/system.py:412 in doctor"``.
    """
    frames = traceback.extract_tb(exc.__traceback__)
    if not frames:
        return "origin unavailable"
    return " -> ".join(
        f"{Path(frame.filename).name}:{frame.lineno} in {frame.name}"
        for frame in frames[-3:]
    )


def classify_user_error(exc: BaseException) -> UserError | None:
    """Map a known exception to a ``UserError``, or ``None`` if unexpected.

    Returning ``None`` for unrecognized exceptions is intentional: callers
    should re-raise so programmer errors surface as failures rather than
    being translated into user-facing messages.
    """
    if isinstance(exc, UserError):
        return exc

    # Deferred so this module stays a leaf of the import graph. A module-level
    # import would put `moneybin.database` (and everything it loads) behind
    # every `from moneybin.errors import UserError`, so nothing `database.py`
    # imports could raise a UserError without minting a cycle — which is what
    # forced the service layer into deferred imports back the other way.
    # Classification runs on the failure path, where a one-time module load
    # costs nothing and the module that raised is already loaded.
    # Guarded by tests/moneybin/test_architecture/test_errors_is_import_light.py.
    from moneybin.connectors.sync_errors import (  # noqa: PLC0415 — keeps errors.py import-light
        SyncError,
    )
    from moneybin.database import (  # noqa: PLC0415 — keeps errors.py import-light
        DatabaseCryptoError,
        DatabaseKeyError,
        DatabaseLockError,
        DatabaseNotInitializedError,
        SchemaDriftError,
        database_key_error_hint,
    )
    from moneybin.secrets import (  # noqa: PLC0415 — keeps errors.py import-light
        SecretNotFoundError,
        SecretStorageUnavailableError,
        SecretUnavailableError,
    )

    if isinstance(exc, DatabaseNotInitializedError):
        # Registration, not the directory, decides the verb. `profile create`
        # completes an unregistered directory in place (config, database, and
        # inbox), so it is the right advice whether or not one is already there.
        # A *registered* profile has finished setup and is only missing its
        # database — that is `db init`'s job, and `profile create` would refuse.
        # `get_database` answers this while it still holds resolved settings; an
        # unanswered `None` takes the `db init` message, the verb that never
        # refuses.
        if exc.profile_registered is False:
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
    if isinstance(exc, SecretUnavailableError):
        # Above its SecretNotFoundError base on purpose: the keychain reported
        # the read as denied rather than missing, and "unlock it" is a different
        # remedy from "store the secret".
        return UserError(
            str(exc),
            code=error_codes.INFRA_PERMISSION_DENIED,
            hint=(
                "💡 Unlock the OS keychain, or grant the process running MoneyBin "
                "access to it, then retry."
            ),
        )
    if isinstance(exc, SecretNotFoundError):
        return UserError(
            str(exc),
            code=error_codes.INFRA_SETUP_REQUIRED,
            hint=(
                "💡 The message names the missing secret — store it with the "
                "command that owns it (e.g. 'moneybin db unlock' for the "
                "database encryption key)."
            ),
        )
    if isinstance(exc, SecretStorageUnavailableError):
        return UserError(
            str(exc),
            code=error_codes.INFRA_SETUP_REQUIRED,
            hint=(
                "💡 No OS keyring backend is available to store secrets. "
                "Configure one (macOS Keychain, GNOME Keyring, KWallet) and retry."
            ),
        )
    if isinstance(exc, FileNotFoundError):
        # Drop the "[Errno 2]" prefix that str(FileNotFoundError) includes —
        # end users don't need the errno number.
        msg = f"{exc.strerror}: {exc.filename}" if exc.filename else str(exc)
        return UserError(msg, code=error_codes.INFRA_FILE_NOT_FOUND)
    if isinstance(exc, PermissionError):
        # Above the generic OSError branch on purpose: PermissionError is an
        # OSError subclass and would otherwise be swallowed into
        # INFRA_IO_ERROR with an unactionable "Operation not permitted".
        # None, not cwd: cwd is not the path that failed, and guessing from it
        # could aim the Full-Disk-Access advice at an unrelated EPERM.
        target = Path(exc.filename) if exc.filename else None
        hint, details = permission_advice(exc.errno or 0, platform.system(), target)
        msg = f"{exc.strerror}: {exc.filename}" if exc.filename else str(exc)
        return UserError(
            msg,
            code=error_codes.INFRA_PERMISSION_DENIED,
            # Decorated here, not in `permission_advice`: the advice text also
            # feeds the inbox `.error.yml` `suggestion` field, which is
            # structured data and carries no display affordances.
            hint=f"💡 {hint}",
            details=details,
        )
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
    if _is_match_run_error(exc):
        # Registered centrally rather than wrapped at each matcher command:
        # `MatchRunError.__init__` passes `str(cause)` to `Exception`, so the
        # carrier's own message IS the raw failure — DuckDB binder text, file
        # paths, row values. Left unclassified it propagates unchanged (see the
        # docstring), which on the CLI means an unhandled traceback carrying all
        # of that. The counts are the disclosable part and each surface already
        # reports them off `exc.partial` before re-raising; this branch supplies
        # only the failure's presentation. `partway through` holds for every
        # instance: the engine wraps a run solely once it has durable writes.
        return UserError(
            "Matching failed partway through — the decisions it had already "
            "committed are durable; the cause is in the local log",
            code=error_codes.REFRESH_MATCH_FAILED,
            hint=(
                "💡 Review what landed with 'moneybin transactions matches "
                "pending', then rerun 'moneybin transactions matches run'"
            ),
        )
    return None


def _is_match_run_error(exc: BaseException) -> bool:
    """True if ``exc`` is the matcher's partial-run carrier.

    Imported inside the call for the same reason as the block in
    ``classify_user_error``: ``moneybin.matching.engine`` pulls in the config,
    database and metrics layers, and this module stays a leaf. There is no
    cycle to break here — this marker used to claim one, but
    ``matching.engine`` imports no repository and reaches this module by no
    module-level path, so hoisting it would cost import weight, not correctness.

    Kept in its own function rather than joining that block so only an exception
    that matched no branch above pays the import.
    """
    from moneybin.matching.engine import (  # noqa: PLC0415 — keeps errors.py import-light
        MatchRunError,
    )

    return isinstance(exc, MatchRunError)
