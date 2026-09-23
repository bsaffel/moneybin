"""Database management commands for MoneyBin CLI.

This module provides commands for creating, exploring, backing up, and
managing the encryption lifecycle of the MoneyBin DuckDB database.
"""

import json
import logging
import os
import shutil
import subprocess  # noqa: S404  # subprocess used with static args for DuckDB CLI invocation and lsof/ps process inspection
import sys
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal, cast

import typer

from moneybin.cli.output import (
    OutputFormat,
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.progress import operation_progress
from moneybin.cli.render import (
    build_rows,
    build_summary,
    compose_human_result,
    render_rows,
)
from moneybin.cli.utils import (
    emit_cli_commentary,
    emit_cli_outcome,
    format_cli_attention,
    format_cli_failure,
    get_terminal_policy,
)
from moneybin.progress import ProgressEvent
from moneybin.protocol.envelope import build_envelope

from .stubs import _not_implemented

app = typer.Typer(help="Database management commands", no_args_is_help=True)
key_app = typer.Typer(
    help="Manage the encryption key for the active profile's database",
    no_args_is_help=True,
)
app.add_typer(key_app, name="key")
logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from moneybin.cli.terminal import TerminalPolicy


# Shown in --help and emitted to stderr on every invocation of the three
# direct-DB commands (shell, ui, query). These commands bypass the MCP
# privacy middleware entirely — account numbers and other CRITICAL-tier
# fields are NOT masked. The agent-mediated path (`moneybin sql query`)
# applies SQL lineage + CRITICAL masking before returning results.
def _direct_db_banner(policy: "TerminalPolicy | None" = None) -> str:
    """Return the direct-DB disclosure using the active terminal policy."""
    return format_cli_attention(
        "Direct DB access - no privacy middleware applies.\n"
        "   Account numbers and sensitive fields are NOT masked here.\n"
        "   For agent-mediated access with privacy enforcement, use:\n"
        '     moneybin sql query "<your SQL>"',
        policy=policy,
    )


_CLI_ENCRYPTION_KEY_ENV_VAR = "MONEYBIN_DATABASE__ENCRYPTION_KEY"


def _require_interactive_prompt(*, action: str, guidance: str) -> None:
    """Refuse prompts when redirected streams cannot express a real choice."""
    if get_terminal_policy().interactive:
        return
    typer.echo(f"{action} requires an interactive terminal. {guidance}", err=True)
    raise typer.Exit(1)


@contextmanager
def _load_encryption_key() -> Generator[str, None, None]:
    """Load the active profile's encryption key; delete the local ref on exit.

    Exits with code 1 when the key is not in the keychain, with a hint that
    distinguishes fresh-install (db file absent → suggest ``db init``) from
    locked (db file present → suggest ``db unlock``).

    The ``finally`` block removes this generator's own local name binding.
    Python cannot zero-fill string memory, and the caller's ``as key:``
    binding still holds the same string object until their with-block exits —
    but ``del`` here prevents this frame from extending the object's lifetime
    beyond the yield.
    """
    from moneybin.database import database_key_error_hint
    from moneybin.secrets import (
        SecretNotFoundError,
        SecretStore,
        SecretUnavailableError,
    )

    store = SecretStore()
    try:
        key = store.get_key("DATABASE__ENCRYPTION_KEY")
    except SecretUnavailableError:
        logger.error(
            format_cli_failure(
                f"OS keychain denied access to the key. {database_key_error_hint()}"
            )
        )
        raise typer.Exit(1) from None
    except SecretNotFoundError:
        logger.error(format_cli_failure(f"Key not found. {database_key_error_hint()}"))
        raise typer.Exit(1) from None
    try:
        yield key
    finally:
        del key


def _format_bytes(num_bytes: int) -> str:
    """Format a byte count as a human-readable string (B / KB / MB)."""
    if num_bytes < 1024:
        return f"{num_bytes} B"
    if num_bytes < 1024 * 1024:
        return f"{num_bytes / 1024:.1f} KB"
    return f"{num_bytes / (1024 * 1024):.1f} MB"


def _check_duckdb_cli() -> str | None:
    """Check if DuckDB CLI is available and return its path.

    Returns:
        str | None: Path to DuckDB CLI executable, or None if not found.
    """
    return shutil.which("duckdb")


def _create_init_script(db_path: Path) -> Path:
    """Create a temporary SQL init script for DuckDB CLI with encrypted attach.

    The script installs+loads httpfs, attaches the encrypted database, and
    sets USE. Created with 0600 permissions. Caller is responsible for
    cleanup.

    Args:
        db_path: Path to the encrypted DuckDB database file.

    Returns:
        Path to the temporary init script.
    """
    from moneybin.database import escape_sql_literal

    safe_path = escape_sql_literal(str(db_path))
    attach_sql = (
        f"ATTACH '{safe_path}' AS \"moneybin\" "
        f"(TYPE DUCKDB, ENCRYPTION_KEY getenv('{_CLI_ENCRYPTION_KEY_ENV_VAR}'))"
    )

    # Write temp script with restrictive permissions
    fd, script_path = tempfile.mkstemp(suffix=".sql", prefix="moneybin_init_")
    try:
        with os.fdopen(fd, "w") as f:
            # DuckDB CLI 1.5.3+ no longer auto-installs httpfs on LOAD; the
            # explicit INSTALL is required for first run on a fresh CLI cache.
            # Subsequent runs are no-ops.
            f.write("INSTALL httpfs;\n")
            f.write("LOAD httpfs;\n")
            f.write(f"{attach_sql};\n")
            f.write("USE moneybin;\n")
        if sys.platform != "win32":
            os.chmod(script_path, 0o600)
    except OSError:
        os.unlink(script_path)
        raise

    return Path(script_path)


def _duckdb_cli_environment() -> dict[str, str]:
    """Build the DuckDB CLI environment with its encryption key scoped to the child."""
    from moneybin.secrets import SecretStore

    child_environment = os.environ.copy()
    child_environment[_CLI_ENCRYPTION_KEY_ENV_VAR] = SecretStore().get_key(
        "DATABASE__ENCRYPTION_KEY"
    )
    return child_environment


@app.command("init")
def db_init(
    database: Path | None = typer.Option(
        None,
        "--database",
        "-d",
        help="Path to DuckDB database file (default: profile config)",
    ),
    passphrase: bool = typer.Option(
        False,
        "--passphrase",
        help="Use passphrase-based key derivation instead of auto-generated key",
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Skip confirmation prompts",
    ),
) -> None:
    """Create a new encrypted database with all schemas initialized.

    By default, generates a random 256-bit encryption key and stores it
    in the OS keychain (auto-key mode). Use --passphrase for passphrase-
    based key derivation via Argon2id.
    """
    from moneybin.config import get_settings
    from moneybin.database import init_db
    from moneybin.secrets import SecretStore

    settings = get_settings()
    db_path = database or settings.database.path

    if db_path.exists() and not yes:
        _require_interactive_prompt(
            action="Database reinitialization confirmation",
            guidance="Use --yes only after reviewing the database path.",
        )
        overwrite = typer.confirm(
            f"Database already exists at {db_path}. Reinitialize?"
        )
        if not overwrite:
            raise typer.Exit(0)

    pp: str | None = None
    if passphrase:
        _require_interactive_prompt(
            action="Passphrase entry",
            guidance="MoneyBin never reads passphrases from redirected input.",
        )
        pp = typer.prompt("Enter passphrase", hide_input=True)
        pp_confirm = typer.prompt("Confirm passphrase", hide_input=True)
        if pp != pp_confirm:
            logger.error(format_cli_failure("Passphrases do not match"))
            raise typer.Exit(1)

    db_cfg = settings.database
    try:
        init_db(
            db_path,
            passphrase=pp,
            secret_store=SecretStore(),
            argon2_time_cost=db_cfg.argon2_time_cost,
            argon2_memory_cost=db_cfg.argon2_memory_cost,
            argon2_parallelism=db_cfg.argon2_parallelism,
            argon2_hash_len=db_cfg.argon2_hash_len,
        )
    except Exception as e:  # duckdb raises untyped errors on key/file issues
        logger.error(format_cli_failure(f"Failed to initialize database: {e}"))
        if db_path.exists():
            typer.echo(
                "An existing database may be encrypted with a different key. "
                "Delete the file or restore the original key, then retry.",
                err=True,
            )
        raise typer.Exit(1) from e
    typer.echo(f"Encrypted database created: {db_path}")


def _run_duckdb_cli(
    db_path: Path,
    *,
    extra_args: list[str] | None = None,
    start_msg: str = "Opening DuckDB interactive shell...",
    hint_msg: str = "   Type .help for commands, .quit to exit",
    error_noun: str = "DuckDB shell",
    exit_msg: str = "DuckDB shell cancelled",
) -> None:
    """Run DuckDB CLI with encrypted database attached.

    Handles the common preamble shared by shell, ui, and query commands:
    db existence check, CLI availability check, init script lifecycle,
    and subprocess error handling.

    Args:
        db_path: Path to the encrypted DuckDB database file.
        extra_args: Additional CLI arguments (e.g. ["-ui"], ["-csv", "-c", sql]).
        start_msg: Log message before launching.
        hint_msg: Secondary log hint (empty string to skip).
        error_noun: Noun for error messages (e.g. "DuckDB shell", "DuckDB UI").
        exit_msg: Log message on KeyboardInterrupt.
    """
    if not db_path.exists():
        logger.error(format_cli_failure(f"Database file not found: {db_path}"))
        typer.echo("Run 'moneybin db init' to create the database first", err=True)
        raise typer.Exit(1)

    duckdb_path = _check_duckdb_cli()
    if duckdb_path is None:
        logger.error(format_cli_failure("DuckDB CLI not found in PATH"))
        typer.echo("Install from: https://duckdb.org/docs/installation/", err=True)
        raise typer.Exit(1)

    from moneybin.database import database_key_error_hint
    from moneybin.secrets import SecretNotFoundError, SecretUnavailableError

    init_script = _create_init_script(db_path)

    try:
        try:
            child_environment = _duckdb_cli_environment()
        except SecretUnavailableError:
            logger.error(
                format_cli_failure(
                    "OS keychain denied access to the key. "
                    f"{database_key_error_hint(db_path)}"
                )
            )
            raise typer.Exit(1) from None
        except SecretNotFoundError:
            logger.error(
                format_cli_failure(f"Key not found. {database_key_error_hint(db_path)}")
            )
            raise typer.Exit(1) from None

        if start_msg:
            emit_cli_commentary(start_msg)
        if hint_msg:
            emit_cli_commentary(hint_msg)
        cmd = [duckdb_path, "-init", str(init_script)]
        if extra_args:
            cmd.extend(extra_args)
        subprocess.run(  # noqa: S603  # cmd built from static args and validated flags
            cmd, check=True, env=child_environment
        )
    except subprocess.CalledProcessError as e:
        logger.error(format_cli_failure(f"{error_noun} failed: {e}"))
        raise typer.Exit(1) from e
    except KeyboardInterrupt:
        if exit_msg:
            emit_cli_outcome(exit_msg)
        raise typer.Exit(130) from None
    finally:
        init_script.unlink(missing_ok=True)


@app.command("shell")
def db_shell(
    database: Path | None = typer.Option(
        None,
        "--database",
        "-d",
        help="Path to DuckDB database file (default: profile config)",
    ),
) -> None:
    """Open an interactive DuckDB SQL shell with encrypted database attached.

    WARNING: Direct DB access — no privacy middleware applies.
    Account numbers and sensitive fields are NOT masked here.
    For agent-mediated access with privacy enforcement, use:
    moneybin sql query "<your SQL>"
    """
    from moneybin.config import get_settings

    settings = get_settings()
    typer.echo(_direct_db_banner(get_terminal_policy(settings=settings.cli)), err=True)
    _run_duckdb_cli(database or settings.database.path)


@app.command("ui")
def db_ui(
    database: Path | None = typer.Option(
        None,
        "--database",
        "-d",
        help="Path to DuckDB database file (default: profile config)",
    ),
) -> None:
    """Open DuckDB web UI with encrypted database auto-attached.

    WARNING: Direct DB access — no privacy middleware applies.
    Account numbers and sensitive fields are NOT masked here.
    For agent-mediated access with privacy enforcement, use:
    moneybin sql query "<your SQL>"
    """
    from moneybin.config import get_settings

    settings = get_settings()
    typer.echo(_direct_db_banner(get_terminal_policy(settings=settings.cli)), err=True)
    _run_duckdb_cli(
        database or settings.database.path,
        extra_args=["-ui"],
        start_msg="Opening DuckDB web UI...",
        hint_msg="   Press Ctrl+C to stop the server",
        error_noun="DuckDB UI",
        exit_msg="DuckDB UI cancelled",
    )


@app.command("query")
def db_query(
    sql: str = typer.Argument(..., help="SQL query to execute"),
    database: Path | None = typer.Option(
        None,
        "--database",
        "-d",
        help="Path to DuckDB database file (default: profile config)",
    ),
    output: Annotated[
        Literal["text", "json", "csv", "markdown", "box"],
        typer.Option(
            "-o",
            "--output",
            help="Output format: text, json, csv, markdown, or box",
        ),
    ] = "text",
    quiet: Annotated[  # query has no informational chatter to gate
        bool,
        typer.Option("-q", "--quiet", help="Suppress informational output"),
    ] = False,
) -> None:
    """Execute a SQL query against the encrypted DuckDB database.

    WARNING: Direct DB access — no privacy middleware applies.
    Account numbers and sensitive fields are NOT masked here.
    For agent-mediated access with privacy enforcement, use:
    moneybin sql query "<your SQL>"
    """
    from moneybin.config import get_settings

    settings = get_settings()
    typer.echo(_direct_db_banner(get_terminal_policy(settings=settings.cli)), err=True)
    output_flag = {
        "text": "-table",
        "json": "-json",
        "csv": "-csv",
        "markdown": "-markdown",
        "box": "-box",
    }[output]
    extra_args: list[str] = [output_flag, "-c", sql]

    _run_duckdb_cli(
        database or settings.database.path,
        extra_args=extra_args,
        start_msg="",
        hint_msg="",
        error_noun="Query",
        exit_msg="",
    )


@app.command("info")
def db_info(
    database: Path | None = typer.Option(
        None,
        "--database",
        "-d",
        help="Path to DuckDB database file (default: profile config)",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # db info has no info-only chatter; only data lines
    no_pager: bool = no_pager_option,
) -> None:
    """Display database metadata: file size, tables, encryption status, versions."""
    from moneybin.config import get_settings
    from moneybin.database import Database
    from moneybin.secrets import SecretNotFoundError, SecretStore

    settings = get_settings()
    db_path = database or settings.database.path

    if not db_path.exists():
        logger.error(format_cli_failure(f"Database file not found: {db_path}"))
        raise typer.Exit(1)

    payload: dict[str, object] = {
        "database": str(db_path),
        "file_size_bytes": db_path.stat().st_size,
        "encryption": "AES-256-GCM",
        "key_mode": settings.database.encryption_key_mode,
    }

    # Check lock state
    store = SecretStore()
    try:
        store.get_key("DATABASE__ENCRYPTION_KEY")
        payload["lock_state"] = "unlocked"
    except SecretNotFoundError:
        payload["lock_state"] = "locked"
        if output == OutputFormat.JSON:
            # Not `render_or_json`: this is operator-territory lifecycle
            # metadata about the database FILE — its path, size, encryption
            # state, table row counts — not ledger rows. Judged by disclosure
            # rather than by column class (security.md), and it must keep
            # answering while the database is locked, which is the one state
            # the typed-payload path cannot read anything in.
            typer.echo(json.dumps(payload, indent=2, default=str))
            return
        policy = get_terminal_policy(no_pager=no_pager)
        emit_human_result(
            build_summary([
                ("Database", str(payload["database"])),
                ("File size", _format_bytes(cast(int, payload["file_size_bytes"]))),
                ("Encryption", "AES-256-GCM (always on)"),
                ("Key mode", str(payload["key_mode"])),
                ("Lock state", "locked (no key in keychain or env)"),
                ("Tables", "unavailable while locked"),
            ]),
            policy=policy,
            finite_read=True,
            no_pager=no_pager,
        )
        return

    # Open database to get table info
    try:
        with Database(db_path, secret_store=store, read_only=True) as db:
            tables = db.execute("""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                  AND table_schema NOT IN ('sqlmesh')
                ORDER BY table_schema, table_name
            """).fetchall()

            from sqlglot import exp

            table_rows: list[dict[str, object]] = []
            if tables:
                # One round trip instead of N — UNION ALL over per-table counts
                # so DuckDB plans them together.
                count_selects: list[str] = []
                for schema, table in tables:
                    safe_schema = exp.to_identifier(schema, quoted=True).sql("duckdb")  # type: ignore[reportUnknownMemberType]  # sqlglot has no stubs
                    safe_table = exp.to_identifier(table, quoted=True).sql("duckdb")  # type: ignore[reportUnknownMemberType]  # sqlglot has no stubs
                    # Double single-quotes per SQL standard so identifiers like
                    # `o'clock` survive embedding as string literal labels.
                    label_schema = schema.replace("'", "''")
                    label_table = table.replace("'", "''")
                    sql = (
                        f"SELECT '{label_schema}' AS schema, '{label_table}' AS \"table\", "  # noqa: S608  # sqlglot-quoted FROM identifiers; labels are quote-escaped
                        f"COUNT(*) AS rows FROM {safe_schema}.{safe_table}"
                    )
                    count_selects.append(sql)
                union_sql = " UNION ALL ".join(count_selects)
                count_rows = db.execute(
                    union_sql
                ).fetchall()  # sqlglot-quoted catalog identifiers and information_schema-sourced names
                table_rows = [
                    {"schema": s, "table": t, "rows": c} for s, t, c in count_rows
                ]

            payload["tables"] = table_rows

            version = db.sql("SELECT version()").fetchone()
            if version:
                payload["duckdb_version"] = version[0]

            if output == OutputFormat.JSON:
                typer.echo(json.dumps(payload, indent=2, default=str))
                return

            pairs = [
                ("Database", str(payload["database"])),
                ("File size", _format_bytes(cast(int, payload["file_size_bytes"]))),
                ("Encryption", "AES-256-GCM (always on)"),
                ("Key mode", str(payload["key_mode"])),
                ("Lock state", "unlocked"),
                ("Tables", str(len(table_rows))),
            ]
            if "duckdb_version" in payload:
                pairs.append(("DuckDB version", str(payload["duckdb_version"])))
            policy = get_terminal_policy(no_pager=no_pager)
            parts: list[object] = [build_summary(pairs)]
            if table_rows:
                parts.append(
                    build_rows(
                        ["schema", "table", "rows"],
                        [
                            (row["schema"], row["table"], row["rows"])
                            for row in table_rows
                        ],
                        terminal=policy,
                    )
                )
            else:
                parts.append(build_summary([("Tables", "No user tables found.")]))
            emit_human_result(
                compose_human_result(parts),
                policy=policy,
                finite_read=True,
                no_pager=no_pager,
            )
    except (
        Exception
    ) as e:  # duckdb raises untyped errors on connection/encryption failure
        logger.error(format_cli_failure(f"Could not open database: {e}"))
        raise typer.Exit(1) from e


@app.command("backup")
def db_backup(
    output: Path | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Output path for backup (default: data/<profile>/backups/)",
    ),
) -> None:
    """Create a timestamped backup of the encrypted database file."""
    from datetime import datetime

    from moneybin.config import get_settings

    settings = get_settings()
    db_path = settings.database.path

    if not db_path.exists():
        logger.error(format_cli_failure(f"Database file not found: {db_path}"))
        raise typer.Exit(1)

    if output:
        backup_path = output
    else:
        backup_dir = settings.database.backup_path or db_path.parent / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        backup_path = backup_dir / f"moneybin_{timestamp}.duckdb"

    try:
        with operation_progress(get_terminal_policy()) as report:
            report(ProgressEvent("Copying database backup"))
            shutil.copy2(str(db_path), str(backup_path))
            report(ProgressEvent("Securing database backup"))
            # Set restrictive permissions
            if sys.platform != "win32":
                try:
                    backup_path.chmod(0o600)
                except OSError:
                    pass
    except KeyboardInterrupt:
        if backup_path.exists():
            typer.echo(
                f"Backup interrupted; {backup_path} may be incomplete and was not confirmed."
            )
        else:
            typer.echo("Backup interrupted before a backup file was confirmed.")
        raise typer.Exit(130) from None

    typer.echo(
        f"Backup created: {backup_path} ({_format_bytes(backup_path.stat().st_size)})"
    )


@app.command("restore")
def db_restore(
    from_path: Path | None = typer.Option(
        None,
        "--from",
        help="Path to backup file to restore from",
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
    latest: bool = typer.Option(
        False,
        "--latest",
        help="Auto-select the most recent backup (non-interactive)",
    ),
) -> None:
    """Restore database from a backup file."""
    from datetime import datetime

    from moneybin.config import get_settings
    from moneybin.database import Database, DatabaseKeyError
    from moneybin.secrets import SecretStore

    settings = get_settings()
    db_path = settings.database.path
    backup_dir = settings.database.backup_path or db_path.parent / "backups"

    if from_path is None:
        if not backup_dir.exists():
            logger.error(format_cli_failure(f"No backup directory found: {backup_dir}"))
            raise typer.Exit(1)

        backups: list[Path] = sorted(backup_dir.glob("*.duckdb"), reverse=True)
        if not backups:
            logger.error(format_cli_failure(f"No backups found in {backup_dir}"))
            raise typer.Exit(1)

        if latest:
            from_path = backups[0]
        else:
            _require_interactive_prompt(
                action="Backup selection",
                guidance="Use --from or --latest in noninteractive runs.",
            )
            typer.echo("Available backups:")
            for i, b in enumerate(backups, 1):
                typer.echo(f"  {i}. {b.name} ({_format_bytes(b.stat().st_size)})")

            choice = typer.prompt("Select backup number", type=int)
            if choice < 1 or choice > len(backups):
                logger.error(format_cli_failure("Invalid selection"))
                raise typer.Exit(1)
            from_path = backups[choice - 1]

    # from_path is guaranteed non-None here (either provided or selected above)
    selected_path: Path = from_path  # type: ignore[assignment]  # Pyright can't narrow across Typer Option | None

    if not selected_path.exists():
        logger.error(format_cli_failure(f"Backup file not found: {selected_path}"))
        raise typer.Exit(1)

    if not yes:
        _require_interactive_prompt(
            action="Database restore confirmation",
            guidance="Use --yes only after reviewing the selected backup.",
        )
        confirm = typer.confirm(
            f"Restore from {selected_path.name}? Current database will be backed up first."
        )
        if not confirm:
            raise typer.Exit(0)

    # Auto-backup current database
    auto_backup: Path | None = None
    replacement_started = False
    database_replaced = False
    try:
        with operation_progress(get_terminal_policy()) as report:
            if db_path.exists():
                backup_dir.mkdir(parents=True, exist_ok=True)
                timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
                auto_backup = backup_dir / f"moneybin_{timestamp}_pre_restore.duckdb"
                report(ProgressEvent("Saving pre-restore backup"))
                shutil.copy2(str(db_path), str(auto_backup))

            replacement_started = True
            report(ProgressEvent("Replacing database from backup"))
            shutil.copy2(str(selected_path), str(db_path))
            database_replaced = True
            if sys.platform != "win32":
                try:
                    db_path.chmod(0o600)
                except OSError:
                    pass

            report(ProgressEvent("Validating restored database"))
            store = SecretStore()
            with Database(db_path, secret_store=store, read_only=True):
                pass
        typer.echo(f"Database restored from {selected_path.name}")
    except KeyboardInterrupt:
        saved_state = (
            f"Pre-restore backup saved at {auto_backup}. "
            if auto_backup is not None and auto_backup.exists()
            else "No pre-restore backup was confirmed. "
        )
        replacement_state = (
            "Database replacement completed before interruption. "
            if database_replaced
            else (
                "Database replacement may be incomplete. "
                if replacement_started
                else "Database replacement did not begin. "
            )
        )
        typer.echo(f"Restore interrupted. {saved_state}{replacement_state}")
        raise typer.Exit(130) from None
    except DatabaseKeyError:
        saved_state = (
            f"Pre-restore backup saved at {auto_backup}. "
            if auto_backup is not None
            else "No pre-restore backup was needed because no database existed. "
        )
        typer.echo(
            f"Database was replaced from {selected_path}. {saved_state}"
            "Validation could not open it with the current key; it may predate a key "
            "rotation. Set MONEYBIN_DATABASE__ENCRYPTION_KEY to the original key and "
            "run 'moneybin db key rotate' to re-encrypt."
        )
        raise typer.Exit(1) from None
    except (
        Exception
    ):  # duckdb raises untyped errors on bad ENCRYPTION_KEY at ATTACH time
        saved_state = (
            f"Pre-restore backup saved at {auto_backup}. "
            if auto_backup is not None
            else "No pre-restore backup was needed because no database existed. "
        )
        if database_replaced:
            typer.echo(
                f"Database was replaced from {selected_path}. {saved_state}"
                "Validation could not open it; the backup may be corrupted."
            )
        elif replacement_started:
            typer.echo(
                f"Restore failed. {saved_state}Database replacement may be incomplete; "
                "validation did not run."
            )
        else:
            typer.echo(
                f"Restore failed. {saved_state}Database replacement did not begin."
            )
        raise typer.Exit(1) from None


@app.command("lock")
def db_lock() -> None:
    """Clear the cached encryption key from OS keychain."""
    from moneybin.secrets import SecretNotFoundError, SecretStore

    store = SecretStore()
    try:
        store.delete_key("DATABASE__ENCRYPTION_KEY")
        from moneybin.database import (  # defer to avoid cold-start cost
            invalidate_encryption_key_cache,
        )

        invalidate_encryption_key_cache()
        typer.echo("Database locked — key cleared from keychain")
    except SecretNotFoundError:
        typer.echo("Database is already locked (no key in keychain)")
    except Exception as e:  # keyring backends may raise non-specific errors
        logger.error(format_cli_failure(f"Failed to lock: {e}"))
        raise typer.Exit(1) from e


@app.command("unlock")
def db_unlock() -> None:
    """Derive key from passphrase and cache in OS keychain."""
    import base64
    import binascii

    from moneybin.config import get_settings
    from moneybin.database import SALT_NAME, Database, derive_key_from_passphrase
    from moneybin.secrets import SecretNotFoundError, SecretStore

    settings = get_settings()
    store = SecretStore()

    # Retrieve the stored salt. Deliberately not split into a separate
    # SecretUnavailableError branch like the other two call sites: even a
    # confirmed denial doesn't resolve the ambiguity here, since the salt
    # could equally be absent because auto-key mode (not --passphrase) was
    # used — the hedge below covers both regardless of which was raised.
    try:
        salt_b64 = store.get_key(SALT_NAME)
    except SecretNotFoundError:
        if not settings.database.path.exists():
            logger.error(
                format_cli_failure(
                    "No passphrase salt found - no database exists yet. "
                    "Run 'moneybin db init --passphrase' to create one."
                )
            )
        else:
            # The database exists but the salt couldn't be read — genuinely
            # ambiguous (see #419): could mean --passphrase mode was never
            # used, or that this environment (sandboxed, headless, or CI)
            # denies keychain access outright, which looks identical.
            logger.error(
                format_cli_failure(
                    "No passphrase salt found in keychain or env var, but "
                    "the database already exists. This can mean it wasn't "
                    "created with --passphrase mode, or that this environment "
                    "denies keychain access. If you know the raw encryption "
                    "key, set MONEYBIN_DATABASE__ENCRYPTION_KEY to open the "
                    "database directly, bypassing the salt entirely."
                )
            )
        raise typer.Exit(1) from None

    try:
        salt = base64.b64decode(salt_b64)
    except binascii.Error as e:
        logger.error(
            format_cli_failure(
                f"Stored passphrase salt is corrupted: {e}. "
                "Run 'moneybin db init --passphrase' to reinitialize."
            )
        )
        raise typer.Exit(1) from e
    _require_interactive_prompt(
        action="Passphrase entry",
        guidance="MoneyBin never reads passphrases from redirected input.",
    )
    pp = typer.prompt("Enter passphrase", hide_input=True)

    # Re-derive key using same params and stored salt via shared helper.
    db_cfg = settings.database
    encryption_key = derive_key_from_passphrase(
        pp,
        salt,
        time_cost=db_cfg.argon2_time_cost,
        memory_cost=db_cfg.argon2_memory_cost,
        parallelism=db_cfg.argon2_parallelism,
        hash_len=db_cfg.argon2_hash_len,
    )

    store.set_key("DATABASE__ENCRYPTION_KEY", encryption_key)

    if not settings.database.path.exists():
        store.delete_key("DATABASE__ENCRYPTION_KEY")
        logger.error(
            format_cli_failure(f"Database file not found: {settings.database.path}")
        )
        typer.echo(
            "Run 'moneybin db init --passphrase' to create a new database.", err=True
        )
        raise typer.Exit(1)
    try:
        with Database(settings.database.path, secret_store=store, read_only=True):
            pass
        from moneybin.database import (  # defer to avoid cold-start cost
            invalidate_encryption_key_cache,
        )

        invalidate_encryption_key_cache()
        typer.echo("Database unlocked")
    except (
        Exception
    ):  # duckdb raises untyped errors on bad ENCRYPTION_KEY at ATTACH time
        cleanup_confirmed = True
        try:
            store.delete_key("DATABASE__ENCRYPTION_KEY")
        except Exception:  # keyring backends may raise beyond SecretNotFoundError
            cleanup_confirmed = False
            logger.debug(
                "Could not remove key from keychain during unlock failure",
                exc_info=True,
            )
        if cleanup_confirmed:
            typer.echo(
                "Wrong passphrase; the derived key was removed from the keychain"
            )
        else:
            typer.echo(
                "Wrong passphrase; MoneyBin could not confirm removal of the derived "
                "key from the keychain. Run 'moneybin db lock' before retrying."
            )
        raise typer.Exit(1) from None


@key_app.command("show")
def db_key_show(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # security warning is unconditional
) -> None:
    """Print the database encryption key."""
    with _load_encryption_key() as key:
        # Security warning is unconditional — the key provides full database
        # access, so the warning must reach stderr regardless of -q/--quiet or
        # --output json. Hoisted above all branches so neither path can suppress
        # it.
        logger.warning(
            format_cli_attention(
                "Security warning: this key provides full access to your "
                "database. Do not share it or store it in plain text."
            )
        )

        if output == OutputFormat.JSON:
            render_or_json(
                build_envelope(data={"key": key}, sensitivity="high"),
                output,
                cli_actor="db_key_show",
            )
            return

        typer.echo(key)


@key_app.command("rotate")
def db_key_rotate(
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Re-encrypt the database with a new key."""
    import secrets as secrets_mod

    import duckdb as duckdb_mod

    from moneybin.config import get_settings
    from moneybin.secrets import SecretStore

    settings = get_settings()
    db_path = settings.database.path
    store = SecretStore()

    if not db_path.exists():
        logger.error(format_cli_failure(f"Database file not found: {db_path}"))
        raise typer.Exit(1)

    if not yes:
        logger.warning(
            format_cli_attention(
                "Existing backups will remain encrypted with the old key."
            )
        )
        _require_interactive_prompt(
            action="Key rotation confirmation",
            guidance="Use --yes only after reviewing the backup warning.",
        )
        confirm = typer.confirm("Proceed with key rotation?")
        if not confirm:
            raise typer.Exit(0)

    new_key = secrets_mod.token_hex(32)

    from moneybin.database import build_attach_sql, scrub_key_material

    rotated_path = db_path.with_suffix(".rotated.duckdb")
    old_backup = db_path.with_suffix(".old.duckdb")
    original_move_started = False
    original_archived = False
    replacement_move_started = False
    database_rotated = False
    keychain_updated = False
    try:
        with operation_progress(get_terminal_policy()) as report:
            with _load_encryption_key() as old_key:
                # Direct duckdb.connect() required here: COPY FROM DATABASE needs two
                # simultaneous open connections; the Database class wraps a single one.
                conn = duckdb_mod.connect()
                try:
                    report(ProgressEvent("Copying encrypted database"))
                    conn.execute("LOAD httpfs;")
                    conn.execute(build_attach_sql(db_path, old_key, alias="old_db"))
                    conn.execute(
                        build_attach_sql(rotated_path, new_key, alias="new_db")
                    )
                    conn.execute("COPY FROM DATABASE old_db TO new_db")
                except KeyboardInterrupt:
                    rotated_path.unlink(missing_ok=True)
                    raise
                except (
                    Exception
                ) as e:  # duckdb raises untyped errors on ATTACH/COPY failure
                    scrub_key_material(e, old_key, new_key)
                    logger.error(format_cli_failure(f"Key rotation failed: {e}"))
                    rotated_path.unlink(missing_ok=True)
                    raise typer.Exit(1) from e
                finally:
                    conn.close()

            report(ProgressEvent("Swapping encrypted database files"))
            original_move_started = True
            shutil.move(str(db_path), str(old_backup))
            original_archived = True
            replacement_move_started = True
            shutil.move(str(rotated_path), str(db_path))
            database_rotated = True
            if sys.platform != "win32":
                try:
                    db_path.chmod(0o600)
                except OSError:
                    pass

            report(ProgressEvent("Updating encryption keychain entry"))
            store.set_key("DATABASE__ENCRYPTION_KEY", new_key)
            keychain_updated = True
    except Exception as e:  # keyring backends may raise non-specific errors
        if not database_rotated:
            if original_archived:
                typer.echo(
                    "Key rotation could not replace the database. The original database "
                    f"was archived at {old_backup}; replacement state is unknown. Inspect "
                    "both files before retrying.",
                    err=True,
                )
                raise typer.Exit(1) from e
            if original_move_started or replacement_move_started:
                typer.echo(
                    "Key rotation failed during the database swap; file state is "
                    "unknown. Inspect the database and retained backup before retrying.",
                    err=True,
                )
                raise typer.Exit(1) from e
            raise
        # The DB file now holds new_key but the keychain still has old_key.
        # old_backup is intact — recovery is possible.
        # Print the new key to stderr directly (not via logger) so it does
        # not appear in log files or get processed by SanitizedLogFormatter.
        typer.echo(
            "Database re-encrypted, but the keychain update failed. Keep the old "
            "database backup until access is restored.",
            err=True,
        )
        typer.echo("Recovery: set the following env var to regain access:", err=True)
        typer.echo(f"  MONEYBIN_DATABASE__ENCRYPTION_KEY={new_key}", err=True)
        typer.echo(f"  (old database backup: {old_backup})", err=True)
        raise typer.Exit(1) from e
    except KeyboardInterrupt:
        if database_rotated:
            if keychain_updated:
                typer.echo(
                    "Key rotation interrupted after the keychain update completed. "
                    "Keep the old database backup until access is verified.",
                    err=True,
                )
            else:
                typer.echo(
                    "Key rotation interrupted after the database swap began; the keychain "
                    "update is unconfirmed. Keep the old database backup until access is restored.",
                    err=True,
                )
                typer.echo(
                    "Recovery: set the following env var to regain access:", err=True
                )
                typer.echo(f"  MONEYBIN_DATABASE__ENCRYPTION_KEY={new_key}", err=True)
                typer.echo(f"  (old database backup: {old_backup})", err=True)
        elif original_archived:
            typer.echo(
                "Key rotation interrupted after the original database was archived. The "
                f"original backup is retained at {old_backup}; the replacement is "
                "unconfirmed. The original backup requires the old key. Inspect the "
                "original backup, rotated candidate "
                f"({rotated_path}), and configured database path ({db_path}) before "
                "retrying.",
                err=True,
            )
            typer.echo(
                "The new key can only access a confirmed rotated candidate; it "
                "cannot access the original backup.",
                err=True,
            )
            typer.echo(
                "Recovery: after confirming the configured database path holds the "
                "rotated candidate, set the following env var to regain access:",
                err=True,
            )
            typer.echo(f"  MONEYBIN_DATABASE__ENCRYPTION_KEY={new_key}", err=True)
        elif original_move_started or replacement_move_started:
            typer.echo(
                "Key rotation interrupted during the database swap; file state is "
                "unknown. Inspect the database and retained backup before retrying.",
                err=True,
            )
        else:
            typer.echo(
                "Key rotation interrupted before the database swap; the rotated candidate "
                "may be incomplete.",
                err=True,
            )
        raise typer.Exit(130) from None
    old_backup.unlink(missing_ok=True)

    # Rotation changes the encryption key, so the in-process cache is stale.
    # Clear it so subsequent Database() calls fetch the new key from the keychain.
    from moneybin.database import (  # defer to avoid cold-start cost
        invalidate_encryption_key_cache,
    )

    invalidate_encryption_key_cache()

    typer.echo("Database re-encrypted with new key")
    typer.echo("Existing backups are still encrypted with the old key")


@key_app.command("export", hidden=True)
def db_key_export(
    out: Annotated[
        Path | None,
        typer.Option(
            "--out",
            "-o",
            help="Path to write the exported key envelope",
        ),
    ] = None,
) -> None:
    """Export the encryption key to an encrypted envelope (not yet implemented)."""
    del out
    # Exits 1, unlike the stubs that exit 0: this command shipped that way and
    # the milestone preserves exit codes. The message is shared so both stub
    # families read identically.
    _not_implemented("encryption key export")
    raise typer.Exit(1)


@key_app.command("import", hidden=True)
def db_key_import(
    envelope: Annotated[
        Path,
        typer.Argument(help="Path to the encrypted key envelope to import"),
    ],
) -> None:
    """Import an encryption key from an envelope (not yet implemented)."""
    del envelope
    _not_implemented("encryption key import")
    raise typer.Exit(1)


@key_app.command("verify", hidden=True)
def db_key_verify() -> None:
    """Verify the encryption key matches the database (not yet implemented)."""
    _not_implemented("encryption key verification")
    raise typer.Exit(1)


def _find_db_processes(db_path: Path) -> list[dict[str, str | int]]:
    """Find processes that have the DuckDB file open, excluding the current process.

    Args:
        db_path: Path to the DuckDB database file.

    Returns:
        List of dicts with keys: pid (int), command (str), cmdline (str).
    """
    from moneybin.utils.db_processes import find_blocking_processes

    return find_blocking_processes(db_path)


def _capture_process_snapshot(
    processes: list[dict[str, str | int]],
) -> dict[int, object] | None:
    """Capture psutil identities for a selected process roll.

    The shared lsof/ps dictionaries remain the public discovery contract. This
    private snapshot is only the `db kill` guard against PID reuse between the
    preview and SIGTERM.
    """
    import psutil

    snapshot: dict[int, object] = {}
    for process_row in processes:
        pid = int(process_row["pid"])
        try:
            process = psutil.Process(pid)
            # Force psutil to retain the creation-time identity before showing
            # the selection. Its signal methods recheck that identity later.
            process.create_time()
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            return None
        snapshot[pid] = process
    return snapshot


def _same_process_selection(
    preview: dict[int, object], current: dict[int, object]
) -> bool:
    """Return whether both scans name the same psutil process instances."""
    return preview.keys() == current.keys() and all(
        preview[pid] == current[pid] for pid in preview
    )


def _render_db_processes(db_path: Path, processes: list[dict[str, str | int]]) -> None:
    """Render the roll of processes holding `db_path` open (requirement 1).

    One renderer for both `ps` and `kill`'s preamble, which printed the same
    three columns from two copies of the same format string.
    """
    typer.echo(f"Processes holding {db_path} open:\n")
    render_rows(
        ["pid", "command", "args"],
        [(proc["pid"], proc["command"], proc["cmdline"]) for proc in processes],
        numeric=("pid",),
    )


@app.command("ps")
def db_ps(
    database: Path | None = typer.Option(
        None, "--database", "-d", help="Path to DuckDB database file"
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
) -> None:
    """Show processes holding the MoneyBin database file open."""
    from moneybin.config import get_settings

    db_path = database or get_settings().database.path

    if output == OutputFormat.JSON:
        # Same exemption as `db info` above: PIDs and a file path describe the
        # operator's own machine, not their ledger.
        processes: list[dict[str, str | int]] = (
            _find_db_processes(db_path) if db_path.exists() else []
        )
        typer.echo(
            json.dumps(
                {"database": str(db_path), "processes": processes},
                indent=2,
                default=str,
            )
        )
        return

    if not db_path.exists():
        policy = get_terminal_policy(no_pager=no_pager)
        emit_human_result(
            build_summary([
                ("Processes", f"Database file does not exist yet: {db_path}")
            ]),
            policy=policy,
            finite_read=True,
            no_pager=no_pager,
        )
        return
    procs = _find_db_processes(db_path)
    policy = get_terminal_policy(no_pager=no_pager)
    if not procs:
        emit_human_result(
            build_summary([
                ("Processes", f"No other processes have {db_path.name} open")
            ]),
            policy=policy,
            finite_read=True,
            no_pager=no_pager,
        )
        return
    emit_human_result(
        compose_human_result([
            build_summary([("Database", str(db_path))]),
            build_rows(
                ["pid", "command", "args"],
                [(proc["pid"], proc["command"], proc["cmdline"]) for proc in procs],
                numeric=("pid",),
                terminal=policy,
            ),
        ]),
        policy=policy,
        finite_read=True,
        no_pager=no_pager,
    )


@app.command("kill")
def db_kill(
    database: Path | None = typer.Option(
        None, "--database", "-d", help="Path to DuckDB database file"
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
) -> None:
    """Kill processes holding the MoneyBin database file open."""
    import psutil

    from moneybin.config import get_settings

    db_path = database or get_settings().database.path
    if not db_path.exists():
        typer.echo(f"Database file does not exist yet: {db_path}")
        return
    processes = _find_db_processes(db_path)
    if not processes:
        typer.echo(f"No other processes have {db_path.name} open")
        return
    preview_snapshot = _capture_process_snapshot(processes)
    if preview_snapshot is None:
        typer.echo(
            "Could not verify the selected process identities; no SIGTERM was sent."
        )
        raise typer.Exit(1)
    _render_db_processes(db_path, processes)
    typer.echo()

    count = len(processes)
    noun = "process" if count == 1 else "processes"
    if not yes:
        _require_interactive_prompt(
            action="Process signal confirmation",
            guidance="Use --yes only after reviewing the displayed process selection.",
        )
        if not typer.confirm(f"Send SIGTERM to {count} {noun}?"):
            raise typer.Exit(0)

    current_processes = _find_db_processes(db_path)
    current_snapshot = _capture_process_snapshot(current_processes)
    if current_snapshot is None or not _same_process_selection(
        preview_snapshot, current_snapshot
    ):
        typer.echo(
            "Process scope changed after the preview; no SIGTERM was sent. Rerun "
            "'moneybin db kill' to review the current selection."
        )
        if current_processes:
            _render_db_processes(db_path, current_processes)
        raise typer.Exit(1)

    sent = 0
    exited_or_reused = 0
    denied = 0
    for proc in processes:
        pid = int(proc["pid"])
        process = preview_snapshot[pid]
        try:
            if not process.is_running():  # type: ignore[union-attr]  # private psutil snapshot
                exited_or_reused += 1
                typer.echo(f"PID {pid} already exited or was reused; no signal sent")
                continue
            process.terminate()  # type: ignore[union-attr]  # SIGTERM guarded by psutil identity
            typer.echo(f"Sent SIGTERM to PID {pid} ({proc['command']})")
            sent += 1
        except psutil.NoSuchProcess:
            exited_or_reused += 1
            typer.echo(f"PID {pid} already exited or was reused; no signal sent")
        except psutil.AccessDenied:
            denied += 1
            typer.echo(f"No permission to signal PID {pid} ({proc['command']})")
    if sent:
        sent_noun = "process" if sent == 1 else "processes"
        typer.echo(f"Sent SIGTERM to {sent} {sent_noun}")
    if exited_or_reused:
        typer.echo(f"{exited_or_reused} already exited or was reused; no signal sent")
    if denied:
        typer.echo(f"{denied} permission denied")
    if exited_or_reused or denied:
        raise typer.Exit(1)
