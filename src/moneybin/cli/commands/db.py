"""Database management commands for MoneyBin CLI.

This module provides commands for creating, exploring, backing up, and
managing the encryption lifecycle of the MoneyBin DuckDB database.
"""

import json
import logging
import os
import shutil
import signal
import subprocess  # noqa: S404 — subprocess used with static args for DuckDB CLI invocation and lsof/ps process inspection
import sys
import tempfile
from pathlib import Path
from typing import Annotated, Literal, cast

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import emit_json

app = typer.Typer(help="Database management commands", no_args_is_help=True)
key_app = typer.Typer(
    help="Manage the encryption key for the active profile's database",
    no_args_is_help=True,
)
app.add_typer(key_app, name="key")
logger = logging.getLogger(__name__)


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

    The script loads httpfs, attaches the encrypted database, and sets USE.
    Created with 0600 permissions. Caller is responsible for cleanup.

    Args:
        db_path: Path to the encrypted DuckDB database file.

    Returns:
        Path to the temporary init script.
    """
    from moneybin.database import build_attach_sql
    from moneybin.secrets import SecretStore

    store = SecretStore()
    encryption_key = store.get_key("DATABASE__ENCRYPTION_KEY")

    # Write temp script with restrictive permissions
    fd, script_path = tempfile.mkstemp(suffix=".sql", prefix="moneybin_init_")
    try:
        with os.fdopen(fd, "w") as f:
            f.write("LOAD httpfs;\n")
            f.write(f"{build_attach_sql(db_path, encryption_key)};\n")
            f.write("USE moneybin;\n")
        if sys.platform != "win32":
            os.chmod(script_path, 0o600)
    except OSError:
        os.unlink(script_path)
        raise

    return Path(script_path)


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
        overwrite = typer.confirm(
            f"Database already exists at {db_path}. Reinitialize?"
        )
        if not overwrite:
            raise typer.Exit(0)

    pp: str | None = None
    if passphrase:
        pp = typer.prompt("Enter passphrase", hide_input=True)
        pp_confirm = typer.prompt("Confirm passphrase", hide_input=True)
        if pp != pp_confirm:
            logger.error("❌ Passphrases do not match")
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
    except Exception as e:  # noqa: BLE001 — duckdb raises untyped errors on key/file issues
        logger.error(f"❌ Failed to initialize database: {e}")
        if db_path.exists():
            logger.info(
                "💡 An existing database may be encrypted with a different key. "
                "Delete the file or restore the original key, then retry."
            )
        raise typer.Exit(1) from e
    logger.info(f"✅ Encrypted database created: {db_path}")


def _run_duckdb_cli(
    db_path: Path,
    *,
    extra_args: list[str] | None = None,
    start_msg: str = "🦆 Opening DuckDB interactive shell...",
    hint_msg: str = "   Type .help for commands, .quit to exit",
    error_noun: str = "DuckDB shell",
    exit_msg: str = "✅ DuckDB shell closed",
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
        logger.error(f"❌ Database file not found: {db_path}")
        logger.info("💡 Run 'moneybin db init' to create the database first")
        raise typer.Exit(1)

    duckdb_path = _check_duckdb_cli()
    if duckdb_path is None:
        logger.error("❌ DuckDB CLI not found in PATH")
        logger.info("💡 Install from: https://duckdb.org/docs/installation/")
        raise typer.Exit(1)

    from moneybin.secrets import SecretNotFoundError

    try:
        init_script = _create_init_script(db_path)
    except SecretNotFoundError:
        logger.error("❌ Database is locked — run 'moneybin db unlock' first")
        raise typer.Exit(1) from None

    try:
        if start_msg:
            logger.info(start_msg)
        if hint_msg:
            logger.info(hint_msg)
        cmd = [duckdb_path, "-init", str(init_script)]
        if extra_args:
            cmd.extend(extra_args)
        subprocess.run(cmd, check=True)  # noqa: S603 — cmd built from static args and validated flags
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ {error_noun} failed: {e}")
        raise typer.Exit(1) from e
    except KeyboardInterrupt:
        if exit_msg:
            logger.info(f"\n{exit_msg}")
        sys.exit(0)
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
    """Open an interactive DuckDB SQL shell with encrypted database attached."""
    from moneybin.config import get_settings

    _run_duckdb_cli(database or get_settings().database.path)


@app.command("ui")
def db_ui(
    database: Path | None = typer.Option(
        None,
        "--database",
        "-d",
        help="Path to DuckDB database file (default: profile config)",
    ),
) -> None:
    """Open DuckDB web UI with encrypted database auto-attached."""
    from moneybin.config import get_settings

    _run_duckdb_cli(
        database or get_settings().database.path,
        extra_args=["-ui"],
        start_msg="🚀 Opening DuckDB web UI...",
        hint_msg="   Press Ctrl+C to stop the server",
        error_noun="DuckDB UI",
        exit_msg="✅ DuckDB UI stopped",
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
    quiet: Annotated[  # noqa: ARG001 — query has no informational chatter to gate
        bool,
        typer.Option("-q", "--quiet", help="Suppress informational output"),
    ] = False,
) -> None:
    """Execute a SQL query against the encrypted DuckDB database."""
    from moneybin.config import get_settings

    output_flag = {
        "text": "-table",
        "json": "-json",
        "csv": "-csv",
        "markdown": "-markdown",
        "box": "-box",
    }[output]
    extra_args: list[str] = [output_flag, "-c", sql]

    _run_duckdb_cli(
        database or get_settings().database.path,
        extra_args=extra_args,
        start_msg="",
        hint_msg="",
        error_noun="Query",
        exit_msg="",
    )


def _render_db_info_header(payload: dict[str, object]) -> None:
    logger.info(f"Database: {payload['database']}")
    logger.info(f"  File size: {_format_bytes(cast(int, payload['file_size_bytes']))}")
    logger.info("  Encryption: AES-256-GCM (always on)")
    logger.info(f"  Key mode: {payload['key_mode']}")


@app.command("info")
def db_info(
    database: Path | None = typer.Option(
        None,
        "--database",
        "-d",
        help="Path to DuckDB database file (default: profile config)",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — db info has no info-only chatter; only data lines
) -> None:
    """Display database metadata: file size, tables, encryption status, versions."""
    from moneybin.config import get_settings
    from moneybin.database import Database
    from moneybin.secrets import SecretNotFoundError, SecretStore

    settings = get_settings()
    db_path = database or settings.database.path

    if not db_path.exists():
        logger.error(f"❌ Database file not found: {db_path}")
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
            typer.echo(json.dumps(payload, indent=2, default=str))
            return
        _render_db_info_header(payload)
        logger.info("  Lock state: locked (no key in keychain or env)")
        return

    # Open database to get table info
    try:
        with Database(db_path, secret_store=store) as db:
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
                        f"SELECT '{label_schema}' AS schema, '{label_table}' AS \"table\", "  # noqa: S608 — sqlglot-quoted FROM identifiers; labels are quote-escaped
                        f"COUNT(*) AS rows FROM {safe_schema}.{safe_table}"
                    )
                    count_selects.append(sql)
                union_sql = " UNION ALL ".join(count_selects)
                count_rows = db.execute(union_sql).fetchall()  # noqa: S608 — sqlglot-quoted catalog identifiers and information_schema-sourced names
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

            _render_db_info_header(payload)
            logger.info("  Lock state: unlocked")
            logger.info(f"  Tables: {len(table_rows)}")
            for row in table_rows:
                logger.info(f"    {row['schema']}.{row['table']}: {row['rows']} rows")
            if "duckdb_version" in payload:
                logger.info(f"  DuckDB version: {payload['duckdb_version']}")
    except Exception as e:  # noqa: BLE001 — duckdb raises untyped errors on connection/encryption failure
        logger.error(f"❌ Could not open database: {e}")
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
        logger.error(f"❌ Database file not found: {db_path}")
        raise typer.Exit(1)

    if output:
        backup_path = output
    else:
        backup_dir = settings.database.backup_path or db_path.parent / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        backup_path = backup_dir / f"moneybin_{timestamp}.duckdb"

    shutil.copy2(str(db_path), str(backup_path))

    # Set restrictive permissions
    if sys.platform != "win32":
        try:
            backup_path.chmod(0o600)
        except OSError:
            pass

    logger.info(
        f"✅ Backup created: {backup_path} ({_format_bytes(backup_path.stat().st_size)})"
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
            logger.error(f"❌ No backup directory found: {backup_dir}")
            raise typer.Exit(1)

        backups: list[Path] = sorted(backup_dir.glob("*.duckdb"), reverse=True)
        if not backups:
            logger.error(f"❌ No backups found in {backup_dir}")
            raise typer.Exit(1)

        if latest:
            from_path = backups[0]
        else:
            logger.info("Available backups:")
            for i, b in enumerate(backups, 1):
                logger.info(f"  {i}. {b.name} ({_format_bytes(b.stat().st_size)})")

            choice = typer.prompt("Select backup number", type=int)
            if choice < 1 or choice > len(backups):
                logger.error("❌ Invalid selection")
                raise typer.Exit(1)
            from_path = backups[choice - 1]

    # from_path is guaranteed non-None here (either provided or selected above)
    selected_path: Path = from_path  # type: ignore[assignment]  # Pyright can't narrow across Typer Option | None

    if not selected_path.exists():
        logger.error(f"❌ Backup file not found: {selected_path}")
        raise typer.Exit(1)

    if not yes:
        confirm = typer.confirm(
            f"Restore from {selected_path.name}? Current database will be backed up first."
        )
        if not confirm:
            raise typer.Exit(0)

    # Auto-backup current database
    if db_path.exists():
        backup_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        auto_backup = backup_dir / f"moneybin_{timestamp}_pre_restore.duckdb"
        shutil.copy2(str(db_path), str(auto_backup))
        logger.info(f"Auto-backed up current database: {auto_backup.name}")

    shutil.copy2(str(selected_path), str(db_path))
    if sys.platform != "win32":
        try:
            db_path.chmod(0o600)
        except OSError:
            pass

    store = SecretStore()
    try:
        with Database(db_path, secret_store=store):
            pass
        logger.info(f"✅ Database restored from {selected_path.name}")
    except DatabaseKeyError:
        logger.warning(
            "⚠️  Could not open restored database with current key. "
            "The backup may be from before a key rotation."
        )
        logger.info(
            "💡 Set the original key via MONEYBIN_DATABASE__ENCRYPTION_KEY "
            "and run 'moneybin db key rotate' to re-encrypt."
        )
        raise typer.Exit(1) from None
    except Exception:  # noqa: BLE001 — duckdb raises untyped errors on bad ENCRYPTION_KEY at ATTACH time
        logger.debug("Restore validation failed", exc_info=True)
        logger.warning(
            "⚠️  Could not open restored database. The backup may be corrupted."
        )
        raise typer.Exit(1) from None


@app.command("lock")
def db_lock() -> None:
    """Clear the cached encryption key from OS keychain."""
    from moneybin.secrets import SecretNotFoundError, SecretStore

    store = SecretStore()
    try:
        store.delete_key("DATABASE__ENCRYPTION_KEY")
        logger.info("✅ Database locked — key cleared from keychain")
    except SecretNotFoundError:
        logger.info("Database is already locked (no key in keychain)")
    except Exception as e:  # noqa: BLE001 — keyring backends may raise non-specific errors
        logger.error(f"❌ Failed to lock: {e}")
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

    # Retrieve the stored salt
    try:
        salt_b64 = store.get_key(SALT_NAME)
    except SecretNotFoundError:
        logger.error(
            "❌ No passphrase salt found. Was this database created with --passphrase mode?"
        )
        raise typer.Exit(1) from None

    try:
        salt = base64.b64decode(salt_b64)
    except binascii.Error as e:
        logger.error(
            "❌ Stored passphrase salt is corrupted: %s. "
            "Run 'moneybin db init --passphrase' to reinitialize.",
            e,
        )
        raise typer.Exit(1) from e
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
        logger.error(f"❌ Database file not found: {settings.database.path}")
        logger.info("💡 Run 'moneybin db init --passphrase' to create a new database.")
        raise typer.Exit(1)
    try:
        with Database(settings.database.path, secret_store=store):
            pass
        logger.info("✅ Database unlocked")
    except Exception:  # noqa: BLE001 — duckdb raises untyped errors on bad ENCRYPTION_KEY at ATTACH time
        try:
            store.delete_key("DATABASE__ENCRYPTION_KEY")
        except Exception:  # noqa: BLE001 — keyring backends may raise beyond SecretNotFoundError
            logger.debug(
                "Could not remove key from keychain during unlock failure",
                exc_info=True,
            )
        logger.error("❌ Wrong passphrase — database remains locked")
        raise typer.Exit(1) from None


@key_app.command("show")
def db_key_show(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — security warning is unconditional
) -> None:
    """Print the database encryption key."""
    from moneybin.secrets import SecretNotFoundError, SecretStore

    store = SecretStore()
    try:
        key = store.get_key("DATABASE__ENCRYPTION_KEY")
    except SecretNotFoundError as e:
        from moneybin.database import database_key_error_hint

        logger.error("❌ No encryption key found")
        logger.info(database_key_error_hint())
        raise typer.Exit(1) from e

    # Security warning is unconditional — the key provides full database
    # access, so the warning must reach stderr regardless of -q/--quiet or
    # --output json. Hoisted above all branches so neither path can suppress
    # it.
    logger.warning(
        "⚠️  Security warning: this key provides full access to your "
        "database. Do not share it or store it in plain text."
    )

    if output == OutputFormat.JSON:
        emit_json("encryption_key", key)
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
    from moneybin.secrets import SecretNotFoundError, SecretStore

    settings = get_settings()
    db_path = settings.database.path

    if not db_path.exists():
        logger.error(f"❌ Database file not found: {db_path}")
        raise typer.Exit(1)

    if not yes:
        logger.warning("⚠️  Existing backups will remain encrypted with the old key.")
        confirm = typer.confirm("Proceed with key rotation?")
        if not confirm:
            raise typer.Exit(0)

    store = SecretStore()
    try:
        old_key = store.get_key("DATABASE__ENCRYPTION_KEY")
    except SecretNotFoundError:
        logger.error("❌ Database is locked — run 'moneybin db unlock' first")
        raise typer.Exit(1) from None
    new_key = secrets_mod.token_hex(32)

    from moneybin.database import build_attach_sql

    rotated_path = db_path.with_suffix(".rotated.duckdb")
    # Direct duckdb.connect() required here: COPY FROM DATABASE needs two
    # simultaneous open connections; the Database class wraps a single one.
    conn = duckdb_mod.connect()
    try:
        conn.execute("LOAD httpfs;")
        conn.execute(build_attach_sql(db_path, old_key, alias="old_db"))
        conn.execute(build_attach_sql(rotated_path, new_key, alias="new_db"))
        conn.execute("COPY FROM DATABASE old_db TO new_db")
    except Exception as e:  # noqa: BLE001 — duckdb raises untyped errors on ATTACH/COPY failure
        logger.error(f"❌ Key rotation failed: {e}")
        rotated_path.unlink(missing_ok=True)
        raise typer.Exit(1) from e
    finally:
        conn.close()

    old_backup = db_path.with_suffix(".old.duckdb")
    shutil.move(str(db_path), str(old_backup))
    shutil.move(str(rotated_path), str(db_path))

    if sys.platform != "win32":
        try:
            db_path.chmod(0o600)
        except OSError:
            pass

    try:
        store.set_key("DATABASE__ENCRYPTION_KEY", new_key)
    except Exception as e:  # noqa: BLE001 — keyring backends may raise non-specific errors
        # The DB file now holds new_key but the keychain still has old_key.
        # old_backup is intact — recovery is possible.
        # Print the new key to stderr directly (not via logger) so it does
        # not appear in log files or get processed by SanitizedLogFormatter.
        logger.error(f"❌ Key rotation failed to update keychain: {e}")
        typer.echo("Recovery: set the following env var to regain access:", err=True)
        typer.echo(f"  MONEYBIN_DATABASE__ENCRYPTION_KEY={new_key}", err=True)
        typer.echo(f"  (old database backup: {old_backup})", err=True)
        raise typer.Exit(1) from e
    old_backup.unlink(missing_ok=True)

    logger.info("✅ Database re-encrypted with new key")
    logger.info("💡 Existing backups are still encrypted with the old key")


@key_app.command("export")
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
    typer.echo(
        "db key export is not yet implemented. Tracked in private/followups.md.",
        err=True,
    )
    raise typer.Exit(1)


@key_app.command("import")
def db_key_import(
    envelope: Annotated[
        Path,
        typer.Argument(help="Path to the encrypted key envelope to import"),
    ],
) -> None:
    """Import an encryption key from an envelope (not yet implemented)."""
    del envelope
    typer.echo(
        "db key import is not yet implemented. Tracked in private/followups.md.",
        err=True,
    )
    raise typer.Exit(1)


@key_app.command("verify")
def db_key_verify() -> None:
    """Verify the encryption key matches the database (not yet implemented)."""
    typer.echo(
        "db key verify is not yet implemented. Tracked in private/followups.md.",
        err=True,
    )
    raise typer.Exit(1)


def _find_db_processes(db_path: Path) -> list[dict[str, str | int]]:
    """Find processes that have the DuckDB file open, excluding the current process.

    Args:
        db_path: Path to the DuckDB database file.

    Returns:
        List of dicts with keys: pid (int), command (str), cmdline (str).
    """
    own_pid = os.getpid()
    try:
        result = subprocess.run(  # noqa: S603 — lsof with static args, db_path is a validated Path
            ["lsof", "-F", "pcn", str(db_path)],  # noqa: S607 — lsof is a standard system utility
            capture_output=True,
            text=True,
            timeout=5,
        )
    except FileNotFoundError:
        logger.error("❌ lsof not found — cannot inspect file locks")
        return []
    except subprocess.TimeoutExpired:
        logger.error("❌ lsof timed out")
        return []

    if not result.stdout:
        return []

    # lsof -F output: each process block starts with p<pid>, then c<cmd>, then n<file>
    processes: list[dict[str, str | int]] = []
    seen_pids: set[int] = set()
    current_pid: int | None = None
    current_cmd: str = ""

    for line in result.stdout.splitlines():
        if line.startswith("p"):
            current_pid = int(line[1:])
            current_cmd = ""
        elif line.startswith("c") and current_pid is not None:
            current_cmd = line[1:]
        elif (
            line.startswith("n")
            and current_pid is not None
            and current_pid not in seen_pids
        ):
            seen_pids.add(current_pid)
            if current_pid == own_pid:
                continue
            ps_result = subprocess.run(  # noqa: S603 — ps with static args and validated int PID
                ["ps", "-p", str(current_pid), "-o", "args="],  # noqa: S607 — ps is a standard system utility
                capture_output=True,
                text=True,
            )
            cmdline = ps_result.stdout.strip()
            processes.append({
                "pid": current_pid,
                "command": current_cmd,
                "cmdline": cmdline,
            })

    return processes


def _list_db_processes(db_path: Path) -> list[dict[str, str | int]]:
    """Print the table of processes holding `db_path` open and return them.

    Returns an empty list when the file is missing or no other process holds it.
    """
    if not db_path.exists():
        logger.info(f"Database file does not exist yet: {db_path}")
        return []
    processes = _find_db_processes(db_path)
    if not processes:
        logger.info(f"No other processes have {db_path.name} open")
        return []
    typer.echo(f"Processes holding {db_path} open:\n")
    typer.echo(f"  {'PID':<8} {'COMMAND':<16} ARGS")
    typer.echo(f"  {'-' * 7:<8} {'-' * 15:<16} {'-' * 40}")
    for proc in processes:
        typer.echo(f"  {proc['pid']:<8} {proc['command']:<16} {proc['cmdline']}")
    return processes


@app.command("ps")
def db_ps(
    database: Path | None = typer.Option(
        None, "--database", "-d", help="Path to DuckDB database file"
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Show processes holding the MoneyBin database file open."""
    from moneybin.config import get_settings

    db_path = database or get_settings().database.path

    if output == OutputFormat.JSON:
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
        if not quiet:
            logger.info(f"Database file does not exist yet: {db_path}")
        return
    procs = _find_db_processes(db_path)
    if not procs:
        if not quiet:
            logger.info(f"No other processes have {db_path.name} open")
        return
    typer.echo(f"Processes holding {db_path} open:\n")
    typer.echo(f"  {'PID':<8} {'COMMAND':<16} ARGS")
    typer.echo(f"  {'-' * 7:<8} {'-' * 15:<16} {'-' * 40}")
    for proc in procs:
        typer.echo(f"  {proc['pid']:<8} {proc['command']:<16} {proc['cmdline']}")


@app.command("kill")
def db_kill(
    database: Path | None = typer.Option(
        None, "--database", "-d", help="Path to DuckDB database file"
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
) -> None:
    """Kill processes holding the MoneyBin database file open."""
    from moneybin.config import get_settings

    db_path = database or get_settings().database.path
    processes = _list_db_processes(db_path)
    if not processes:
        return
    typer.echo()

    count = len(processes)
    noun = "process" if count == 1 else "processes"
    if not yes and not typer.confirm(f"Send SIGTERM to {count} {noun}?"):
        raise typer.Exit(0)

    killed = 0
    for proc in processes:
        pid = int(proc["pid"])
        try:
            os.kill(pid, signal.SIGTERM)
            logger.info(f"Sent SIGTERM to PID {pid} ({proc['command']})")
            killed += 1
        except ProcessLookupError:
            logger.warning(f"⚠️  PID {pid} already exited")
        except PermissionError:
            logger.error(f"❌ No permission to kill PID {pid} ({proc['command']})")
    if killed:
        logger.info(f"✅ Sent SIGTERM to {killed} {noun}")
