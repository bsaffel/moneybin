"""Shared helpers for CLI commands."""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass

import typer

from moneybin.cli.output import OutputFormat, emit_json_error
from moneybin.config import set_current_profile
from moneybin.database import Database, get_database
from moneybin.errors import classify_user_error
from moneybin.observability import setup_observability
from moneybin.utils.user_config import ensure_default_profile

logger = logging.getLogger(__name__)


@contextmanager
def handle_cli_errors(
    output: OutputFormat = OutputFormat.TEXT,
) -> Generator[Database, None, None]:
    """Get the active database with cross-cutting CLI error handling.

    Yields a ``Database`` and catches user-facing exceptions raised either
    during database open or inside the ``with`` block. When ``output`` is
    ``OutputFormat.JSON``, classified errors are emitted as a structured JSON
    envelope to stdout. Otherwise they are logged with the standard ``❌``
    prefix. Unrecognized exceptions propagate unchanged.
    """
    try:
        db = get_database()
        yield db
    except typer.Exit:
        # Commands raise typer.Exit for their own early-exit paths
        # (mutually exclusive flags, user-cancelled prompts). Don't run
        # those through the user-error classifier.
        raise
    except Exception as e:
        user_error = classify_user_error(e)
        if user_error is None:
            raise
        if output == OutputFormat.JSON:
            # JSON-mode errors bypass logger.error intentionally: stdout stays
            # machine-readable for agents and the structured envelope carries
            # the full error context. The agent's transcript is the audit trail.
            emit_json_error(user_error)
        else:
            logger.error(f"❌ {user_error.message}")
            if user_error.hint:
                logger.info(user_error.hint)
        raise typer.Exit(1) from e


def emit_json(key: str, payload: object) -> None:
    """Emit a single-key JSON envelope to stdout."""
    typer.echo(json.dumps({key: payload}, indent=2, default=str))


def render_rich_table(cols: list[str], rows: list[tuple[object, ...]]) -> None:
    """Render ``rows`` as a Rich table to stdout, with headers ``cols``."""
    from rich.console import Console  # noqa: PLC0415 — defer heavy import
    from rich.table import Table  # noqa: PLC0415 — defer heavy import

    console = Console()
    table = Table(*cols)
    for row in rows:
        table.add_row(*[str(v) if v is not None else "" for v in row])
    console.print(table)


@contextmanager
def sqlmesh_command(
    operation: str, *, success: str | None = None
) -> Generator[Database, None, None]:
    """Wrap a SQLMesh-fronted command with consistent ⚙️/✅/❌ logging.

    SQLMesh raises broad untyped exceptions that bypass ``classify_user_error``.
    This wrapper layers on top of ``handle_cli_errors`` so user errors
    (DatabaseKeyError, etc.) still classify correctly while SQLMesh's broad
    failures get a uniform ``❌ {operation} failed: {e}`` line and exit 1.
    Yields the active ``Database`` for commands that need it.

    Args:
        operation: Verb-noun describing the action (e.g. ``"SQLMesh plan"``).
            Used in the leading ``⚙️ {operation}…`` and trailing
            ``❌ {operation} failed`` lines.
        success: Custom success message after ``✅ ``. Defaults to
            ``f"{operation} completed"``.
    """
    logger.info(f"⚙️  {operation}...")
    try:
        # TODO: forward output param when sqlmesh_command callers gain --output json support
        with handle_cli_errors() as db:
            yield db
        logger.info(f"✅ {success or f'{operation} completed'}")
    except typer.Exit:
        raise
    except Exception as e:  # noqa: BLE001 — SQLMesh raises broad exceptions
        logger.error(f"❌ {operation} failed: {e}")
        raise typer.Exit(1) from e


@dataclass
class _CLIFlags:
    """Flags stashed by ``main_callback`` for later lazy resolution."""

    profile: str | None = None
    verbose: bool = False


_flags = _CLIFlags()


def stash_cli_flags(profile: str | None, verbose: bool) -> None:
    """Record top-level CLI flags for the lazy profile resolver."""
    _flags.profile = profile
    _flags.verbose = verbose


def get_verbose_flag() -> bool:
    """Return whether --verbose was passed on the top-level CLI."""
    return _flags.verbose


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
        try:
            profile_name = ensure_default_profile()
        except KeyboardInterrupt:
            raise typer.Abort() from None
        source = "config.yaml or first-run wizard"

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
    if source:
        logger.info(f"Using profile: {profile_name} (from {source})")
    else:
        logger.info(f"Using profile: {profile_name}")
