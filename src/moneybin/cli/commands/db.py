"""Database management commands for MoneyBin CLI.

This module provides commands for creating, exploring, backing up, and
managing the encryption lifecycle of the MoneyBin DuckDB database.
"""

import logging
import os
import shutil
import signal
import subprocess  # noqa: S404 — subprocess used with static args for DuckDB CLI invocation and lsof/ps process inspection
import sys
import tempfile
from pathlib import Path

import typer

app = typer.Typer(help="Database management commands", no_args_is_help=True)
logger = logging.getLogger(__name__)


def _derive_key_from_passphrase(passphrase: str, salt: bytes) -> str:
    """Derive a hex encryption key from a passphrase using Argon2id.

    Used by both init_db (at creation) and db_unlock (at re-derivation).
    Both callers must use the same DatabaseConfig parameters — this helper
    ensures they can never diverge and silently lock users out.

    Args:
        passphrase: User-supplied passphrase string.
        salt: Random 16-byte salt (stored at init, retrieved at unlock).

    Returns:
        64-character hex string (256-bit key).
    """
    import argon2.low_level

    from moneybin.config import get_settings

    db_cfg = get_settings().database
    raw_key = argon2.low_level.hash_secret_raw(
        secret=passphrase.encode(),
        salt=salt,
        time_cost=db_cfg.argon2_time_cost,
        memory_cost=db_cfg.argon2_memory_cost,
        parallelism=db_cfg.argon2_parallelism,
        hash_len=db_cfg.argon2_hash_len,
        type=argon2.low_level.Type.ID,
    )
    return raw_key.hex()


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
def init_db(
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
    import secrets as secrets_mod

    from moneybin.config import get_settings
    from moneybin.secrets import SecretStore

    settings = get_settings()
    db_path = database or settings.database.path

    if db_path.exists() and not yes:
        overwrite = typer.confirm(
            f"Database already exists at {db_path}. Reinitialize?"
        )
        if not overwrite:
            raise typer.Exit(0)

    store = SecretStore()

    if passphrase:
        # Passphrase mode: prompt, derive key via Argon2id, store derived key + salt
        import base64

        pp = typer.prompt("Enter passphrase", hide_input=True)
        pp_confirm = typer.prompt("Confirm passphrase", hide_input=True)
        if pp != pp_confirm:
            logger.error("❌ Passphrases do not match")
            raise typer.Exit(1)

        # Generate a fixed salt to allow re-derivation during unlock
        salt = secrets_mod.token_bytes(16)
        # Derive deterministic key from passphrase + salt via shared helper.
        # _derive_key_from_passphrase must be used here and in db_unlock so
        # the Argon2id parameters can never diverge.
        encryption_key = _derive_key_from_passphrase(pp, salt)

        # Store key and salt so unlock can re-derive
        store.set_key("DATABASE__ENCRYPTION_KEY", encryption_key)
        store.set_key("DATABASE__PASSPHRASE_SALT", base64.b64encode(salt).decode())
        logger.info("Passphrase-derived key stored in OS keychain")
    else:
        # Auto-key mode: generate random 256-bit key
        encryption_key = secrets_mod.token_hex(32)
        store.set_key("DATABASE__ENCRYPTION_KEY", encryption_key)
        logger.info("Auto-generated encryption key stored in OS keychain")

    # Create the database using the Database class
    from moneybin.database import Database

    db = Database(db_path, secret_store=store)
    db.close()

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
        logger.info(f"\n{exit_msg}")
        sys.exit(0)
    finally:
        init_script.unlink(missing_ok=True)


@app.command("shell")
def open_shell(
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
def open_ui(
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
def run_query(
    sql: str = typer.Argument(..., help="SQL query to execute"),
    database: Path | None = typer.Option(
        None,
        "--database",
        "-d",
        help="Path to DuckDB database file (default: profile config)",
    ),
    output_format: str = typer.Option(
        "table",
        "--format",
        "-f",
        help="Output format: table, csv, json, markdown, box",
    ),
) -> None:
    """Execute a SQL query against the encrypted DuckDB database."""
    from moneybin.config import get_settings

    format_map = {
        "table": "-table",
        "csv": "-csv",
        "json": "-json",
        "markdown": "-markdown",
        "box": "-box",
    }
    extra_args: list[str] = []
    if output_format in format_map:
        extra_args.append(format_map[output_format])
    else:
        logger.warning(f"⚠️  Unknown format '{output_format}', using table")
    extra_args.extend(["-c", sql])

    _run_duckdb_cli(
        database or get_settings().database.path,
        extra_args=extra_args,
        start_msg="",
        hint_msg="",
        error_noun="Query",
    )


@app.command("info")
def db_info(
    database: Path | None = typer.Option(
        None,
        "--database",
        "-d",
        help="Path to DuckDB database file (default: profile config)",
    ),
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

    # File info
    file_size = db_path.stat().st_size
    if file_size < 1024:
        size_str = f"{file_size} B"
    elif file_size < 1024 * 1024:
        size_str = f"{file_size / 1024:.1f} KB"
    else:
        size_str = f"{file_size / (1024 * 1024):.1f} MB"

    logger.info(f"Database: {db_path}")
    logger.info(f"  File size: {size_str}")
    logger.info("  Encryption: AES-256-GCM (always on)")
    logger.info(f"  Key mode: {settings.database.encryption_key_mode}")

    # Check lock state
    store = SecretStore()
    try:
        store.get_key("DATABASE__ENCRYPTION_KEY")
        logger.info("  Lock state: unlocked")
    except SecretNotFoundError:
        logger.info("  Lock state: locked (no key in keychain or env)")
        return

    # Open database to get table info
    try:
        db = Database(db_path, secret_store=store)
        try:
            tables = db.execute("""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                ORDER BY table_schema, table_name
            """).fetchall()

            from sqlglot import exp

            logger.info(f"  Tables: {len(tables)}")
            for schema, table in tables:
                safe_schema = exp.to_identifier(schema, quoted=True).sql("duckdb")  # type: ignore[reportUnknownMemberType]  # sqlglot has no stubs
                safe_table = exp.to_identifier(table, quoted=True).sql("duckdb")  # type: ignore[reportUnknownMemberType]  # sqlglot has no stubs
                count_result = db.execute(
                    f"SELECT COUNT(*) FROM {safe_schema}.{safe_table}"  # noqa: S608 — sqlglot-quoted catalog identifiers
                ).fetchone()
                count = count_result[0] if count_result else 0
                logger.info(f"    {schema}.{table}: {count} rows")

            # DuckDB version
            version = db.sql("SELECT version()").fetchone()
            if version:
                logger.info(f"  DuckDB version: {version[0]}")
        finally:
            db.close()
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

    file_size = backup_path.stat().st_size / (1024 * 1024)
    logger.info(f"✅ Backup created: {backup_path} ({file_size:.1f} MB)")


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

    if from_path is None:
        backup_dir = settings.database.backup_path or db_path.parent / "backups"
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
                size = b.stat().st_size / (1024 * 1024)
                logger.info(f"  {i}. {b.name} ({size:.1f} MB)")

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
        backup_dir = settings.database.backup_path or db_path.parent / "backups"
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
        db = Database(db_path, secret_store=store)
        db.close()
        logger.info(f"✅ Database restored from {selected_path.name}")
    except DatabaseKeyError:
        logger.warning(
            "⚠️  Could not open restored database with current key. "
            "The backup may be from before a key rotation."
        )
        logger.info(
            "💡 Set the original key via MONEYBIN_DATABASE__ENCRYPTION_KEY "
            "and run 'moneybin db rotate-key' to re-encrypt."
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
    from moneybin.database import Database
    from moneybin.secrets import SecretNotFoundError, SecretStore

    settings = get_settings()
    store = SecretStore()

    # Retrieve the stored salt
    try:
        salt_b64 = store.get_key("DATABASE__PASSPHRASE_SALT")
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
    # _derive_key_from_passphrase must be used here and in init_db so
    # the Argon2id parameters can never diverge.
    encryption_key = _derive_key_from_passphrase(pp, salt)

    store.set_key("DATABASE__ENCRYPTION_KEY", encryption_key)

    if not settings.database.path.exists():
        store.delete_key("DATABASE__ENCRYPTION_KEY")
        logger.error(f"❌ Database file not found: {settings.database.path}")
        logger.info("💡 Run 'moneybin db init --passphrase' to create a new database.")
        raise typer.Exit(1)
    try:
        db = Database(settings.database.path, secret_store=store)
        db.close()
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


@app.command("key")
def db_key() -> None:
    """Print the database encryption key."""
    from moneybin.secrets import SecretNotFoundError, SecretStore

    store = SecretStore()
    try:
        key = store.get_key("DATABASE__ENCRYPTION_KEY")
    except SecretNotFoundError as e:
        logger.error(
            "❌ No encryption key found. Database may be locked. "
            "Run 'moneybin db unlock' first."
        )
        raise typer.Exit(1) from e

    logger.warning(
        "⚠️  Security warning: this key provides full access to your "
        "database. Do not share it or store it in plain text."
    )
    typer.echo(key)


@app.command("rotate-key")
def db_rotate_key(
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


@app.command("ps")
def db_ps(
    database: Path | None = typer.Option(
        None, "--database", "-d", help="Path to DuckDB database file"
    ),
) -> None:
    """Show processes holding the MoneyBin database file open."""
    from moneybin.config import get_settings

    db_path = database or get_settings().database.path
    if not db_path.exists():
        logger.info(f"Database file does not exist yet: {db_path}")
        return
    processes = _find_db_processes(db_path)
    if not processes:
        logger.info(f"No other processes have {db_path.name} open")
        return
    typer.echo(f"Processes holding {db_path} open:\n")
    typer.echo(f"  {'PID':<8} {'COMMAND':<16} ARGS")
    typer.echo(f"  {'-' * 7:<8} {'-' * 15:<16} {'-' * 40}")
    for proc in processes:
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
    if not db_path.exists():
        logger.info(f"Database file does not exist yet: {db_path}")
        return
    processes = _find_db_processes(db_path)
    if not processes:
        logger.info(f"No other processes have {db_path.name} open")
        return
    typer.echo(f"Processes holding {db_path} open:\n")
    typer.echo(f"  {'PID':<8} {'COMMAND':<16} ARGS")
    typer.echo(f"  {'-' * 7:<8} {'-' * 15:<16} {'-' * 40}")
    for proc in processes:
        typer.echo(f"  {proc['pid']:<8} {proc['command']:<16} {proc['cmdline']}")
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
