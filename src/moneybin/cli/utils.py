"""Shared helpers for CLI commands."""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING

import typer

from moneybin.cli.output import OutputFormat, emit_json_error
from moneybin.config import set_current_profile
from moneybin.errors import classify_user_error
from moneybin.observability import setup_observability
from moneybin.services.mutation_context import operation
from moneybin.utils.user_config import ensure_default_profile, get_default_profile

if TYPE_CHECKING:
    from moneybin.database import Database
    from moneybin.matching.engine import MatchResult
    from moneybin.services.refresh_outcome import RefreshStepOutcome

logger = logging.getLogger(__name__)

# Profile-resolution provenance: kept in the log file, kept off the console.
# Named so `_CONSOLE_SUPPRESSED_PREFIXES` can target it without silencing the
# rest of `moneybin.cli`, which is ordinary user-facing output.
_profile_source_logger = logger.getChild("profile_source")


def _error_audit_classification(payload_type: type | None) -> tuple[str, list[str]]:
    """Audit (sensitivity, classes) for a JSON-mode error row.

    Conservative default: without the failed command's payload type we can't
    derive its tier, so default ``"high"`` rather than under-report a
    CRITICAL command's failure as ``"low"`` in ``privacy.log.jsonl`` — the
    failure paths auditors care most about. When ``payload_type`` IS supplied
    (wired by the command), derive the exact tier + classes the success path
    would have logged. A misclassified payload (``PrivacyContractError``) also
    falls back to the conservative ``"high"`` rather than breaking the error
    path.
    """
    if payload_type is None:
        return "high", []
    from moneybin.cli.output import derive_log_sensitivity  # noqa: PLC0415
    from moneybin.privacy.introspection import (  # noqa: PLC0415
        PrivacyContractError,
        extract_data_classes,
    )

    try:
        sensitivity = derive_log_sensitivity(payload_type, "high")
        classes = [c.value for c in sorted(extract_data_classes(payload_type))]
    except PrivacyContractError:
        return "high", []
    return sensitivity, classes


@contextmanager
def handle_cli_errors(
    *, cli_actor: str | None = None, payload_type: type | None = None
) -> Generator[None, None, None]:
    """Cross-cutting CLI error handler.

    Catches classified user-facing exceptions (DatabaseKeyError,
    DatabaseLockError, DatabaseNotInitializedError, etc.) and exits with
    code 1. When the active output format is JSON (set via ``output_option``
    callback), emits a structured error envelope to stdout; otherwise logs
    the message with the standard ❌ prefix and prints any hint straight to
    stderr (never through the logger — see the text-mode branch below).
    Unrecognized exceptions propagate unchanged.

    On JSON-mode failure, writes a ``privacy.log.jsonl`` audit row mirroring
    the success-path entry render_or_json writes — keeps failed/blocked CLI
    invocations in the same audit trail. ``cli_actor`` names the command
    (e.g. ``"accounts_get"``) when known; defaults to ``"unknown"`` so call
    sites can adopt incrementally without changing observed behavior on
    text-mode paths. ``payload_type`` is the command's success-path payload
    type; when supplied the audit row's sensitivity + classes are derived
    from it, otherwise the row defaults to the conservative ``"high"`` tier so
    a CRITICAL command's failure is never under-reported (see
    ``_error_audit_classification``).

    Does NOT open or yield a Database — commands acquire their own
    connections with ``get_database(read_only=...)``.

    Binds one ``operation()`` for the command body so every audit row written
    during this invocation shares one ``operation_id`` (REC-PR1) — the CLI
    half of the surface seam, mirroring the MCP tool decorator.

    """
    with operation():
        try:
            yield
        except typer.Exit:
            # Commands raise typer.Exit for their own early-exit paths
            # (mutually exclusive flags, user-cancelled prompts). Don't run
            # those through the user-error classifier.
            raise
        except Exception as e:
            user_error = classify_user_error(e)
            if user_error is None:
                raise
            if _flags.output == OutputFormat.JSON:
                # JSON-mode errors bypass logger.error intentionally: stdout
                # stays machine-readable for agents and the structured envelope
                # carries the full error context.
                emit_json_error(user_error)
                # Mirror the MCP decorator's error-path audit emission so
                # JSON-mode failures appear in privacy.log.jsonl alongside
                # success rows.
                from moneybin.privacy.log import (  # noqa: PLC0415 — defer import
                    build_tool_call_event,
                    write_privacy_event,
                )

                sensitivity, classes_returned = _error_audit_classification(
                    payload_type
                )
                write_privacy_event(
                    build_tool_call_event(
                        actor=f"cli.{cli_actor or 'unknown'}",
                        sensitivity=sensitivity,
                        classes_returned=classes_returned,
                        row_count=0,
                    )
                )
            else:
                logger.error(f"❌ {user_error.message}")
                if user_error.hint:
                    # NOT logger.info: the root logger runs at INFO and the
                    # file handler is unfiltered (`_ConsoleNoiseFilter` only
                    # guards the console handler — see its docstring), so a
                    # logged hint persists to the durable cli_YYYY-MM-DD.log.
                    # `hint` is not always MoneyBin-authored: `sql_query`'s
                    # hint threads the head of a DuckDB binder/catalog
                    # message, which can carry text the caller typed into
                    # the query. `message` above stays on the logger — it IS
                    # a fixed MoneyBin string. This mirrors the "Secrets in
                    # Error Output" pattern (cli.md): text that must reach
                    # the console but never the log file goes straight to
                    # stderr via typer.echo, bypassing the logging pipeline
                    # entirely.
                    typer.echo(user_error.hint, err=True)
            raise typer.Exit(1) from e


def warn_transfers_retired(
    count: int, *, cause: str, rematch_follow_up: bool = False
) -> None:
    """Warn that ``count`` standing transfers the user had accepted were reversed.

    One helper rather than a line per surface because the thing being reported
    is the same everywhere and its recovery route must not drift: every path
    that folds a duplicate can reach the reconciliation, and a user who reads
    the way back on one surface should find it on the next. ``cause`` names what
    collapsed, which is the only part that differs. The reversal itself is
    always this call's doing, so the sentence may claim that much; what it may
    not claim is that this call caused the invalidation. Silent on zero, so the
    warning keeps its meaning.

    ``rematch_follow_up`` is for the accept paths only — see the branch below.
    """
    if not count:
        return
    follow_up = (
        # Only the accept paths. They reconcile inside their own transaction and
        # return, so a transfer the freed legs now allow stays unproposed until
        # some later pass; `TransactionMatcher.run` re-runs Tier 4 itself and
        # would be sending the user back through a pass that just finished.
        "; then run 'moneybin transactions matches run' — the reversal freed "
        "transaction legs a new transfer may now pair"
        if rematch_follow_up
        else ""
    )
    logger.warning(
        f"⚠️  Retired {count} previously accepted transfer(s) — {cause}; "
        "inspect with 'moneybin system audit list' and restore with "
        f"'moneybin system audit undo <operation-id>' if that was wrong{follow_up}"
    )


def warn_refresh_steps(outcome: RefreshStepOutcome | None) -> None:
    """Warn about every best-effort refresh step that failed or came up short.

    One helper for the same reason as :func:`warn_transfers_retired` above, and
    a sharper one: ``refresh(steps=None)`` runs four best-effort steps, so every
    command that closes with a refresh — import, sync pull, inbox drain, sheet
    pull — silently does the same work on the user's behalf. Each of these
    outcomes carries a *different* remedy: re-run the step, wait out an outage,
    record the rate by hand, accept a short span. A surface that prints some of
    them teaches a remedy that does not exist for the ones it drops.

    Silent when every step did what it planned, so a warning keeps its meaning.
    Pairs are named rather than counted: which pair is missing decides whether
    the gap matters at all, and a currency pair discloses no amount.
    """
    if outcome is None:
        return
    if outcome.matching_error is not None:
        logger.warning(f"⚠️  Matching step failed: {outcome.matching_error}")
    if outcome.categorization_error is not None:
        logger.warning(f"⚠️  Categorization step failed: {outcome.categorization_error}")
    for domain in outcome.identity_errors:
        logger.warning(f"⚠️  {domain.title()} identity backfill failed")
    if outcome.rate_backfill_error is not None:
        # Ahead of the three pair warnings below, and never instead of them: a
        # crash names no pair, so those lines stay silent and this is the only
        # signal the step failed at all.
        logger.warning(
            f"⚠️  Exchange rate backfill failed: {outcome.rate_backfill_error}"
        )
    if outcome.rate_pairs_failed:
        logger.warning(
            f"⚠️  Exchange rates unavailable for {', '.join(outcome.rate_pairs_failed)}"
        )
    if outcome.rate_pairs_unsupported:
        # Separate line from the one above because the remedy is different, and
        # the remedy is the whole reason to print it: retrying never fills this.
        logger.warning(
            f"⚠️  No exchange rate series is published for "
            f"{', '.join(outcome.rate_pairs_unsupported)}. "
            "Record these rates yourself with `moneybin fx set`."
        )
    if outcome.rate_pairs_discarded:
        # Hedged, unlike the two above: this pair may have stored most of its
        # span and lost a day at one end, so it says coverage may be short
        # rather than that the pair is missing. Worded around the shortfall
        # rather than around a dropped rate, because the causes that bound the
        # answer's span — a series starting after the window or stopping before
        # it — never sent one to drop.
        logger.warning(
            f"⚠️  Exchange rate coverage is short for "
            f"{', '.join(outcome.rate_pairs_discarded)}. "
            "Conversion may be incomplete on those dates."
        )


def warn_match_decisions_committed(partial: MatchResult) -> None:
    """Warn that a failed run had already written these decisions.

    Sibling of `warn_transfers_retired`, and one helper for the same reason: a
    tier persists one decision per pair with no transaction around the loop, so
    every command that can reach the tiers can end up here, and the route back to
    the decisions must not drift between them. Retirements are reported
    separately because only they undo something the user decided; these are
    merges and proposals the run made and kept.

    Silent when the run found nothing, so the warning keeps its meaning — a
    failure that committed no decision has none to point at.
    """
    if not partial.has_matches:
        return
    logger.warning(
        f"⚠️  Committed {partial.summary().lower()} before matching failed — "
        "those decisions are durable; review them with "
        "'moneybin transactions matches pending'"
    )


def parse_cli_date(value: str, flag: str) -> date:
    """Parse an ISO date, exiting 2 on a usage error rather than raising.

    ``flag`` names the argument in the message, so one helper can speak for a
    positional (``DATE``) and an option (``--since``) alike.
    """
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()  # noqa: DTZ007  # a calendar date, not an instant
    except ValueError:
        typer.echo(
            f"error: {flag} must be an ISO date (YYYY-MM-DD), got {value!r}", err=True
        )
        raise typer.Exit(2) from None


def parse_cli_decimal(value: str, flag: str) -> Decimal:
    """Parse a number as an exact, finite Decimal — never through float.

    The finite check is not belt-and-braces: ``Decimal`` parses ``"NaN"`` and
    ``"Infinity"`` as ordinary literals, so without it they survive as numbers
    and fail far downstream — NaN raises ``InvalidOperation`` on the first
    comparison, infinity fails inside DuckDB — reporting an internal error for
    what is a typo.
    """
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        typer.echo(f"error: {flag} must be a decimal number, got {value!r}", err=True)
        raise typer.Exit(2) from None
    if not parsed.is_finite():
        typer.echo(f"error: {flag} must be a finite number, got {value!r}", err=True)
        raise typer.Exit(2) from None
    return parsed


def emit_json(key: str, payload: object) -> None:
    """Emit a single-key JSON envelope to stdout.

    Uses ``PayloadEncoder`` so typed dataclass / Pydantic payloads serialize
    to dicts, not ``str(...)`` reprs. ``default=str`` would silently override
    the encoder's dataclass handling — keep them mutually exclusive.
    """
    from moneybin.protocol.envelope import (  # noqa: PLC0415 — defer import
        PayloadEncoder,
    )

    typer.echo(json.dumps({key: payload}, indent=2, cls=PayloadEncoder))


def render_rich_table(cols: list[str], rows: list[tuple[object, ...]]) -> None:
    """Render ``rows`` as a Rich table to stdout, with headers ``cols``."""
    from rich.console import Console  # noqa: PLC0415 — defer heavy import
    from rich.table import Table  # noqa: PLC0415 — defer heavy import

    # markup=False because every cell here is data, and much of it is
    # user-authored — a report description, a merchant name, a transaction
    # description. Rich reads `[...]` as a style tag, so a default console drops
    # "spend [excluding rent]" down to "spend " and lets stored text steer the
    # terminal's styling. No caller passes style tags in a cell.
    console = Console(markup=False)
    table = Table(*cols)
    for row in rows:
        table.add_row(*[str(v) if v is not None else "" for v in row])
    console.print(table)


@contextmanager
def sqlmesh_command(
    label: str, *, success: str | None = None
) -> Generator[Database, None, None]:
    """Wrap a SQLMesh-fronted command with consistent ⚙️/✅/❌ logging.

    Opens its own write connection, yields it, and handles both classified
    user errors and SQLMesh's broad untyped exceptions. Binds one
    ``operation()`` for the body so transform commands routed through here
    (not ``handle_cli_errors``) still share one ``operation_id`` per call —
    the same CLI seam REC-PR1 establishes. ``label`` is named to avoid
    shadowing the imported ``operation`` context manager.

    Args:
        label: Verb-noun describing the action (e.g. ``"Seed materialization"``).
            Used in the leading ``⚙️ {label}…`` and trailing
            ``❌ {label} failed`` lines, so it reaches the user verbatim and
            names the action in their vocabulary, never a dependency (req 17).
            The message guard cannot catch a violation here — the string lives
            at the call site, which is neither ``logger.*`` nor ``typer.echo``.
        success: Custom success message after ``✅ ``. Defaults to
            ``f"{label} completed"``.
    """
    from moneybin.database import get_database  # noqa: PLC0415 — defer heavy import

    logger.info(f"⚙️  {label}...")
    try:
        with (
            operation(),
            get_database(read_only=False, operation_type="transform_apply") as db,
        ):
            yield db
        logger.info(f"✅ {success or f'{label} completed'}")
    except typer.Exit:
        raise
    except Exception as e:  # noqa: BLE001
        user_error = classify_user_error(e)
        if user_error is not None:
            logger.error(f"❌ {user_error.message}")
            if user_error.hint:
                # See handle_cli_errors above: never logger.info — the file
                # handler has no level filter, so a logged hint would persist
                # to the durable log. Same fix, same reason, kept in sync so
                # this path doesn't quietly reacquire the retired pattern.
                typer.echo(user_error.hint, err=True)
        else:
            logger.error(f"❌ {label} failed: {e}")
        raise typer.Exit(1) from e


@dataclass
class _CLIFlags:
    """Flags stashed by ``main_callback`` for later lazy resolution."""

    profile: str | None = None
    verbose: bool = False
    output: OutputFormat = OutputFormat.TEXT


_flags = _CLIFlags()


def stash_cli_flags(profile: str | None, verbose: bool) -> None:
    """Record top-level CLI flags for the lazy profile resolver."""
    _flags.profile = profile
    _flags.verbose = verbose


def get_verbose_flag() -> bool:
    """Return whether --verbose was passed on the top-level CLI."""
    return _flags.verbose


def set_output_flag(value: OutputFormat) -> OutputFormat:
    """Record the active output format; called by the output_option callback."""
    _flags.output = value
    return value


def resolve_profile() -> None:
    """Resolve the active profile and finish CLI setup.

    Invoked lazily via the resolver registered with ``config.py`` the first
    time a command needs settings or the active profile name. Performs the
    full chain — flag → ``MONEYBIN_PROFILE`` env → ``config.yaml`` →
    first-run wizard — then calls ``set_current_profile`` and re-initializes
    observability with profile-specific log files.

    Skipping this work in ``main_callback`` keeps the parent inert so
    leaf-level usage errors (``moneybin logs`` with no stream) and ``--help``
    surface cleanly without spinning up the wizard, log files, or profile
    directories.
    """
    profile_name = _flags.profile
    source: str | None = None
    if profile_name is not None:
        source = "--profile flag"
    elif env_profile := os.environ.get("MONEYBIN_PROFILE"):
        profile_name = env_profile
        source = "MONEYBIN_PROFILE env var"
    else:
        # Which of the two things `ensure_default_profile` does is decided by
        # whether config.yaml already names an active profile — the same check
        # it makes first. Reading it here is what lets the banner name the
        # source that actually resolved instead of listing both (req 19).
        configured = get_default_profile()
        try:
            profile_name = ensure_default_profile()
        except KeyboardInterrupt:
            raise typer.Abort() from None
        source = "config.yaml" if configured else "the first-run wizard"

    try:
        set_current_profile(profile_name)
    except ValueError as e:
        raise typer.BadParameter(str(e)) from e

    from moneybin.config import get_base_dir
    from moneybin.utils.user_config import normalize_profile_name

    normalized = normalize_profile_name(profile_name)
    profile_dir = get_base_dir() / "profiles" / normalized
    if not profile_dir.exists():
        logger.error(f"❌ Profile '{normalized}' does not exist")
        logger.info("💡 Run 'moneybin profile list' to see available profiles")
        logger.info(f"💡 Run 'moneybin profile create {normalized}' to create it")
        raise typer.Exit(1)

    setup_observability(stream="cli", verbose=_flags.verbose, profile=profile_name)
    logger.info(f"Using profile: {profile_name}")
    if source:
        # INFO, on a child logger the console denylist covers: which of
        # --profile, MONEYBIN_PROFILE, or config.yaml chose this profile is
        # trivia on every command but the only evidence when an unexpected one
        # is selected. `logger.debug` would drop it from the log file too —
        # the root logger sits at INFO and never emits DEBUG records.
        _profile_source_logger.info(f"Profile resolved from {source}")
