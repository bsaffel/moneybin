"""Database management commands for MoneyBin CLI.

This module provides commands for creating, exploring, backing up, and
managing the encryption lifecycle of the MoneyBin DuckDB database.
"""

import logging
import os
import shutil
import subprocess  # noqa: S404 — subprocess used with static args for DuckDB CLI invocation
import sys
import tempfile
from pathlib import Path

import typer

app = typer.Typer(help="Database management commands", no_args_is_help=True)
logger = logging.getLogger(__name__)


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
    from moneybin.secrets import SecretStore

    store = SecretStore()
    encryption_key = store.get_key("DATABASE__ENCRYPTION_KEY")

    # Write temp script with restrictive permissions
    fd, script_path = tempfile.mkstemp(suffix=".sql", prefix="moneybin_init_")
    try:
        with os.fdopen(fd, "w") as f:
            f.write("LOAD httpfs;\n")
            safe_db_path = str(db_path).replace("'", "''")
            safe_key = encryption_key.replace("'", "''")
            f.write(
                f"ATTACH '{safe_db_path}' AS moneybin "
                f"(TYPE DUCKDB, ENCRYPTION_KEY '{safe_key}');\n"  # noqa: S608  # trusted internal values, single-quote escaped
            )
            f.write("USE moneybin;\n")
        if sys.platform != "win32":
            os.chmod(script_path, 0o600)
    except Exception:
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
    from moneybin.logging.config import setup_logging

    setup_logging(cli_mode=True)

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

        import argon2.low_level

        pp = typer.prompt("Enter passphrase", hide_input=True)
        pp_confirm = typer.prompt("Confirm passphrase", hide_input=True)
        if pp != pp_confirm:
            logger.error("❌ Passphrases do not match")
            raise typer.Exit(1)

        # Generate a fixed salt to allow re-derivation during unlock
        salt = secrets_mod.token_bytes(16)
        # Derive deterministic key from passphrase + salt using Argon2id
        raw_key = argon2.low_level.hash_secret_raw(
            secret=pp.encode(),
            salt=salt,
            time_cost=3,
            memory_cost=65536,
            parallelism=4,
            hash_len=32,
            type=argon2.low_level.Type.ID,
        )
        encryption_key = raw_key.hex()

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

    logger.info("✅ Encrypted database created: %s", db_path)


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

    db_path = database or get_settings().database.path

    if not db_path.exists():
        logger.error(f"❌ Database file not found: {db_path}")
        logger.info("💡 Run 'moneybin db init' to create the database first")
        raise typer.Exit(1)

    duckdb_path = _check_duckdb_cli()
    if duckdb_path is None:
        logger.error("❌ DuckDB CLI not found in PATH")
        logger.info("💡 Install from: https://duckdb.org/docs/installation/")
        raise typer.Exit(1)

    init_script = _create_init_script(db_path)
    try:
        logger.info("🦆 Opening DuckDB interactive shell...")
        logger.info("   Type .help for commands, .quit to exit")
        cmd = [duckdb_path, "-init", str(init_script)]
        subprocess.run(cmd, check=True)  # noqa: S603 — cmd built from static args
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ DuckDB shell failed: {e}")
        raise typer.Exit(1) from e
    except KeyboardInterrupt:
        logger.info("\n✅ DuckDB shell closed")
        sys.exit(0)
    finally:
        init_script.unlink(missing_ok=True)


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

    db_path = database or get_settings().database.path

    if not db_path.exists():
        logger.error(f"❌ Database file not found: {db_path}")
        logger.info("💡 Run 'moneybin db init' to create the database first")
        raise typer.Exit(1)

    duckdb_path = _check_duckdb_cli()
    if duckdb_path is None:
        logger.error("❌ DuckDB CLI not found in PATH")
        logger.info("💡 Install from: https://duckdb.org/docs/installation/")
        raise typer.Exit(1)

    init_script = _create_init_script(db_path)
    try:
        logger.info("🚀 Opening DuckDB web UI...")
        logger.info("   Press Ctrl+C to stop the server")
        cmd = [duckdb_path, "-init", str(init_script), "-ui"]
        subprocess.run(cmd, check=True)  # noqa: S603 — cmd built from static args
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ DuckDB UI failed to start: {e}")
        raise typer.Exit(1) from e
    except KeyboardInterrupt:
        logger.info("\n✅ DuckDB UI stopped")
        sys.exit(0)
    finally:
        init_script.unlink(missing_ok=True)


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

    db_path = database or get_settings().database.path

    if not db_path.exists():
        logger.error(f"❌ Database file not found: {db_path}")
        logger.info("💡 Run 'moneybin db init' to create the database first")
        raise typer.Exit(1)

    duckdb_path = _check_duckdb_cli()
    if duckdb_path is None:
        logger.error("❌ DuckDB CLI not found in PATH")
        logger.info("💡 Install from: https://duckdb.org/docs/installation/")
        raise typer.Exit(1)

    format_map = {
        "table": "-table",
        "csv": "-csv",
        "json": "-json",
        "markdown": "-markdown",
        "box": "-box",
    }

    init_script = _create_init_script(db_path)
    try:
        cmd = [duckdb_path, "-init", str(init_script), "-c", sql]
        if output_format in format_map:
            cmd.append(format_map[output_format])
        else:
            logger.warning(f"⚠️  Unknown format '{output_format}', using table")

        subprocess.run(cmd, check=True)  # noqa: S603 — cmd built from static args and format flag
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Query failed: {e}")
        raise typer.Exit(1) from e
    finally:
        init_script.unlink(missing_ok=True)


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
    from moneybin.logging.config import setup_logging

    setup_logging(cli_mode=True)

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

    logger.info("Database: %s", db_path)
    logger.info("  File size: %s", size_str)
    logger.info("  Encryption: AES-256-GCM (always on)")
    logger.info("  Key mode: %s", settings.database.encryption_key_mode)

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

            logger.info("  Tables: %d", len(tables))
            for schema, table in tables:
                count_result = db.execute(
                    f'SELECT COUNT(*) FROM "{schema}"."{table}"'  # noqa: S608 — schema/table from information_schema
                ).fetchone()
                count = count_result[0] if count_result else 0
                logger.info("    %s.%s: %d rows", schema, table, count)

            # DuckDB version
            version = db.sql("SELECT version()").fetchone()
            if version:
                logger.info("  DuckDB version: %s", version[0])
        finally:
            db.close()
    except Exception as e:
        logger.error("❌ Could not open database: %s", e)
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
    from moneybin.logging.config import setup_logging

    setup_logging(cli_mode=True)

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
    logger.info("✅ Backup created: %s (%.1f MB)", backup_path, file_size)


@app.command("restore")
def db_restore(
    from_path: Path | None = typer.Option(
        None,
        "--from",
        help="Path to backup file to restore from",
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Restore database from a backup file."""
    from moneybin.logging.config import setup_logging

    setup_logging(cli_mode=True)

    from datetime import datetime

    from moneybin.config import get_settings
    from moneybin.database import Database
    from moneybin.secrets import SecretStore

    settings = get_settings()
    db_path = settings.database.path

    if from_path is None:
        backup_dir = settings.database.backup_path or db_path.parent / "backups"
        if not backup_dir.exists():
            logger.error("❌ No backup directory found: %s", backup_dir)
            raise typer.Exit(1)

        backups: list[Path] = sorted(backup_dir.glob("*.duckdb"), reverse=True)
        if not backups:
            logger.error("❌ No backups found in %s", backup_dir)
            raise typer.Exit(1)

        logger.info("Available backups:")
        for i, b in enumerate(backups, 1):
            size = b.stat().st_size / (1024 * 1024)
            logger.info("  %d. %s (%.1f MB)", i, b.name, size)

        choice = typer.prompt("Select backup number", type=int)
        if choice < 1 or choice > len(backups):
            logger.error("❌ Invalid selection")
            raise typer.Exit(1)
        from_path = backups[choice - 1]

    # from_path is guaranteed non-None here (either provided or selected above)
    assert from_path is not None  # noqa: S101 — guaranteed non-None after selection
    from typing import cast

    resolved_path = cast(Path, from_path)  # narrow type for pyright

    if not resolved_path.exists():
        logger.error(f"❌ Backup file not found: {resolved_path}")
        raise typer.Exit(1)

    if not yes:
        confirm = typer.confirm(
            f"Restore from {resolved_path.name}? Current database will be backed up first."
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
        logger.info("Auto-backed up current database: %s", auto_backup.name)

    shutil.copy2(str(resolved_path), str(db_path))
    if sys.platform != "win32":
        try:
            db_path.chmod(0o600)
        except OSError:
            pass

    store = SecretStore()
    try:
        db = Database(db_path, secret_store=store)
        db.close()
        logger.info("✅ Database restored from %s", resolved_path.name)
    except Exception:
        logger.warning(
            "⚠️  Could not open restored database with current key. "
            "The backup may be from before a key rotation."
        )
        logger.info(
            "💡 Set the original key via MONEYBIN_DATABASE__ENCRYPTION_KEY "
            "and run 'moneybin db rotate-key' to re-encrypt."
        )
        raise typer.Exit(1) from None


@app.command("lock")
def db_lock() -> None:
    """Clear the cached encryption key from OS keychain."""
    from moneybin.logging.config import setup_logging

    setup_logging(cli_mode=True)

    from moneybin.secrets import SecretNotFoundError, SecretStore

    store = SecretStore()
    try:
        store.delete_key("DATABASE__ENCRYPTION_KEY")
        logger.info("✅ Database locked — key cleared from keychain")
    except SecretNotFoundError:
        logger.info("Database is already locked (no key in keychain)")
    except Exception as e:
        logger.error(f"❌ Failed to lock: {e}")
        raise typer.Exit(1) from e


@app.command("unlock")
def db_unlock() -> None:
    """Derive key from passphrase and cache in OS keychain."""
    from moneybin.logging.config import setup_logging

    setup_logging(cli_mode=True)

    import base64

    import argon2.low_level

    from moneybin.config import get_settings
    from moneybin.database import Database
    from moneybin.secrets import SecretNotFoundError, SecretStore

    store = SecretStore()

    # Retrieve the stored salt
    try:
        salt_b64 = store.get_key("DATABASE__PASSPHRASE_SALT")
    except SecretNotFoundError:
        logger.error(
            "❌ No passphrase salt found. Was this database created with --passphrase mode?"
        )
        raise typer.Exit(1) from None

    salt = base64.b64decode(salt_b64)
    pp = typer.prompt("Enter passphrase", hide_input=True)

    # Re-derive key using same params and stored salt
    raw_key = argon2.low_level.hash_secret_raw(
        secret=pp.encode(),
        salt=salt,
        time_cost=3,
        memory_cost=65536,
        parallelism=4,
        hash_len=32,
        type=argon2.low_level.Type.ID,
    )
    encryption_key = raw_key.hex()

    store.set_key("DATABASE__ENCRYPTION_KEY", encryption_key)

    settings = get_settings()
    try:
        db = Database(settings.database.path, secret_store=store)
        db.close()
        logger.info("✅ Database unlocked")
    except Exception:
        store.delete_key("DATABASE__ENCRYPTION_KEY")
        logger.error("❌ Wrong passphrase — database remains locked")
        raise typer.Exit(1) from None


@app.command("key")
def db_key() -> None:
    """Print the database encryption key."""
    from moneybin.logging.config import setup_logging

    setup_logging(cli_mode=True)

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
    from moneybin.logging.config import setup_logging

    setup_logging(cli_mode=True)

    import secrets as secrets_mod

    import duckdb as duckdb_mod

    from moneybin.config import get_settings
    from moneybin.secrets import SecretStore

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
    old_key = store.get_key("DATABASE__ENCRYPTION_KEY")
    new_key = secrets_mod.token_hex(32)

    rotated_path = db_path.with_suffix(".rotated.duckdb")
    conn = duckdb_mod.connect()
    try:
        conn.execute("LOAD httpfs;")
        safe_db_path = str(db_path).replace("'", "''")
        safe_old_key = old_key.replace("'", "''")
        conn.execute(
            f"ATTACH '{safe_db_path}' AS old_db "
            f"(TYPE DUCKDB, ENCRYPTION_KEY '{safe_old_key}')"  # noqa: S608  # trusted internal values, single-quote escaped
        )
        safe_rotated_path = str(rotated_path).replace("'", "''")
        safe_new_key = new_key.replace("'", "''")
        conn.execute(
            f"ATTACH '{safe_rotated_path}' AS new_db "
            f"(TYPE DUCKDB, ENCRYPTION_KEY '{safe_new_key}')"  # noqa: S608  # trusted internal values, single-quote escaped
        )
        conn.execute("COPY FROM DATABASE old_db TO new_db")
    except Exception as e:
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

    store.set_key("DATABASE__ENCRYPTION_KEY", new_key)
    old_backup.unlink(missing_ok=True)

    logger.info("✅ Database re-encrypted with new key")
    logger.info("💡 Existing backups are still encrypted with the old key")
