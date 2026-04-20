# Data Protection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Protect financial data at rest through DuckDB encryption, centralized connection management (`Database` class), OS keychain key management (`SecretStore`), file permission enforcement, and PII log sanitization.

**Architecture:** A `SecretStore` class wraps OS keychain access (`keyring` library) and env var fallback. A `Database` class uses `SecretStore` to retrieve the encryption key, opens an in-memory DuckDB connection, attaches the encrypted database file, runs schema init, and exposes a singleton via `get_database()`. All existing `duckdb.connect()` calls are replaced. A `SanitizedLogFormatter` masks PII patterns in log output as a runtime safety net.

**Tech Stack:** DuckDB encryption extension (AES-256-GCM), `keyring` (OS keychain), `argon2-cffi` (passphrase KDF), Python `secrets` (key generation), `re` (PII pattern matching)

**Spec:** [`docs/specs/privacy-data-protection.md`](../../specs/privacy-data-protection.md)

---

## Scope Note

This spec covers multiple coupled subsystems. The `Database` class depends on `SecretStore`, and all consumer migrations depend on `Database`. The log sanitizer is independent. One plan with clear phases is appropriate because the subsystems share a dependency chain and must ship together for encryption to work end-to-end.

The **database-migration spec** (`database-migration.md`) is `ready` but not yet implemented. The `Database` class initialization sequence includes migration steps (g, h, i from the spec) as stubs that log a TODO — they'll be filled in when the migration spec is implemented.

## File Structure

### Files to Create

| File | Responsibility |
|---|---|
| `src/moneybin/secrets.py` | `SecretStore` class — sole `keyring` consumer. `get_key()`, `set_key()`, `delete_key()` for keychain; `get_env()` for env vars. `SecretNotFoundError` exception. |
| `src/moneybin/database.py` | `Database` class — encrypted connection lifecycle. `get_database()` singleton. `DatabaseKeyError` exception. |
| `src/moneybin/log_sanitizer.py` | `SanitizedLogFormatter` — PII pattern detection and masking in log output. |
| `tests/moneybin/test_secrets.py` | `SecretStore` unit tests |
| `tests/moneybin/test_database.py` | `Database` class unit tests |
| `tests/moneybin/test_log_sanitizer.py` | `SanitizedLogFormatter` unit tests |

### Files to Modify

| File | Change |
|---|---|
| `pyproject.toml` | Add `keyring`, `argon2-cffi` dependencies |
| `src/moneybin/config.py` | Add `encryption_key_mode`, `temp_directory`, `backup_path` defaults to `DatabaseConfig`; directory permissions in `create_directories()` |
| `src/moneybin/logging/config.py` | Wire `SanitizedLogFormatter` into file handler |
| `src/moneybin/cli/commands/db.py` | Rewrite all commands to use `Database`; add `info`, `backup`, `restore`, `lock`, `unlock`, `key`, `rotate-key`; use `-init` temp script for shell/ui/query |
| `src/moneybin/loaders/ofx_loader.py` | Accept `Database` instead of `Path`; remove internal `duckdb.connect()` |
| `src/moneybin/loaders/csv_loader.py` | Accept `Database` instead of `Path`; remove internal `duckdb.connect()` |
| `src/moneybin/loaders/w2_loader.py` | Accept `Database` instead of `Path`; remove internal `duckdb.connect()` |
| `src/moneybin/services/import_service.py` | Change `db_path: Path` → `db: Database`; remove `duckdb.connect()` calls |
| `src/moneybin/services/categorization_service.py` | Change `conn: DuckDBPyConnection` → `db: Database` |
| `src/moneybin/cli/commands/categorize.py` | Replace `duckdb.connect()` with `get_database()` |
| `src/moneybin/cli/commands/import_cmd.py` | Replace `duckdb.connect()` / `db_path` with `get_database()` |
| `src/moneybin/mcp/server.py` | Replace `_db`/`_db_path`/`get_db()`/`get_write_db()` with `get_database()` |
| `tests/moneybin/conftest.py` | Add `db` fixture that provides a `Database` instance for tests |
| `tests/moneybin/db_helpers.py` | Update to use `Database` instead of raw connection |

### Files to Delete

| File | Reason |
|---|---|
| `src/moneybin/utils/secrets_manager.py` | Replaced by `src/moneybin/secrets.py` |
| `tests/moneybin/test_utils/test_secrets_manager.py` | Tests for deleted module |

---

## Task 1: Add Dependencies

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add keyring and argon2-cffi to dependencies**

In `pyproject.toml`, add these two lines to the `dependencies` list after the existing security pins section:

```toml
    # Secret management and key derivation
    "keyring>=25.6.0",
    "argon2-cffi>=23.1.0",
```

- [ ] **Step 2: Install dependencies**

Run: `uv sync`
Expected: Both packages install successfully.

- [ ] **Step 3: Verify imports work**

Run: `uv run python -c "import keyring; import argon2; print('OK')"`
Expected: `OK`

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "feat: add keyring and argon2-cffi dependencies for data protection"
```

---

## Task 2: SecretStore Class

**Files:**
- Create: `src/moneybin/secrets.py`
- Create: `tests/moneybin/test_secrets.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/moneybin/test_secrets.py`:

```python
"""Tests for SecretStore — centralized secret management."""

from unittest.mock import MagicMock, patch

import pytest

from moneybin.secrets import SecretNotFoundError, SecretStore


class TestGetKey:
    """SecretStore.get_key() — keychain → env var → error."""

    def test_returns_key_from_keychain(self) -> None:
        """Keychain contains the secret — returns it directly."""
        store = SecretStore()
        with patch("moneybin.secrets.keyring") as mock_kr:
            mock_kr.get_password.return_value = "secret-from-keychain"
            result = store.get_key("DATABASE__ENCRYPTION_KEY")

        assert result == "secret-from-keychain"
        mock_kr.get_password.assert_called_once_with(
            "moneybin", "DATABASE__ENCRYPTION_KEY"
        )

    def test_falls_back_to_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Keychain miss + env var set — returns env var value."""
        monkeypatch.setenv("MONEYBIN_DATABASE__ENCRYPTION_KEY", "secret-from-env")
        store = SecretStore()
        with patch("moneybin.secrets.keyring") as mock_kr:
            mock_kr.get_password.return_value = None
            result = store.get_key("DATABASE__ENCRYPTION_KEY")

        assert result == "secret-from-env"

    def test_raises_when_both_miss(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Both keychain and env var miss — raises SecretNotFoundError."""
        monkeypatch.delenv("MONEYBIN_DATABASE__ENCRYPTION_KEY", raising=False)
        store = SecretStore()
        with patch("moneybin.secrets.keyring") as mock_kr:
            mock_kr.get_password.return_value = None
            with pytest.raises(SecretNotFoundError, match="DATABASE__ENCRYPTION_KEY"):
                store.get_key("DATABASE__ENCRYPTION_KEY")


class TestGetEnv:
    """SecretStore.get_env() — env var only, no keychain."""

    def test_returns_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MONEYBIN_SYNC__API_KEY", "api-key-123")
        store = SecretStore()
        assert store.get_env("SYNC__API_KEY") == "api-key-123"

    def test_raises_when_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("MONEYBIN_SYNC__API_KEY", raising=False)
        store = SecretStore()
        with pytest.raises(SecretNotFoundError, match="SYNC__API_KEY"):
            store.get_env("SYNC__API_KEY")


class TestSetAndDeleteKey:
    """SecretStore.set_key() and delete_key() — keychain writes."""

    def test_set_key_writes_to_keychain(self) -> None:
        store = SecretStore()
        with patch("moneybin.secrets.keyring") as mock_kr:
            store.set_key("DATABASE__ENCRYPTION_KEY", "new-key-value")

        mock_kr.set_password.assert_called_once_with(
            "moneybin", "DATABASE__ENCRYPTION_KEY", "new-key-value"
        )

    def test_delete_key_clears_from_keychain(self) -> None:
        store = SecretStore()
        with patch("moneybin.secrets.keyring") as mock_kr:
            store.delete_key("DATABASE__ENCRYPTION_KEY")

        mock_kr.delete_password.assert_called_once_with(
            "moneybin", "DATABASE__ENCRYPTION_KEY"
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_secrets.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'moneybin.secrets'`

- [ ] **Step 3: Implement SecretStore**

Create `src/moneybin/secrets.py`:

```python
"""Centralized secret management for MoneyBin.

SecretStore is the sole module that imports ``keyring``. All other modules
access secrets through this interface — the Database class for encryption
keys, CLI commands for key lifecycle, MoneyBinSettings for sensitive config.

SecretStore does NOT cache, derive, rotate, or orchestrate secret lifecycle.
Passphrase derivation (Argon2id) and rotation sequencing live in the CLI
commands that call set_key() / delete_key().
"""

import logging
import os

import keyring

logger = logging.getLogger(__name__)

_SERVICE_NAME = "moneybin"
_ENV_PREFIX = "MONEYBIN_"


class SecretNotFoundError(Exception):
    """Raised when a secret cannot be found in keychain or environment."""


class SecretStore:
    """Keychain and environment variable interface for secrets.

    Three operations for keychain-backed secrets (encryption keys, E2E keys):
    - get_key(name): keychain → env var → SecretNotFoundError
    - set_key(name, value): write to keychain
    - delete_key(name): clear from keychain

    One operation for env-var-only secrets (API keys, server credentials):
    - get_env(name): env var → SecretNotFoundError
    """

    def get_key(self, name: str) -> str:
        """Retrieve a secret from OS keychain, falling back to env var.

        Args:
            name: Secret name (e.g. "DATABASE__ENCRYPTION_KEY").
                  Keychain lookup uses service="moneybin", username=name.
                  Env var lookup uses MONEYBIN_{name}.

        Returns:
            The secret value.

        Raises:
            SecretNotFoundError: If the secret is not in keychain or env var.
        """
        # Try OS keychain first
        value = keyring.get_password(_SERVICE_NAME, name)
        if value is not None:
            return value

        # Fall back to environment variable
        env_var = f"{_ENV_PREFIX}{name}"
        value = os.environ.get(env_var)
        if value is not None:
            return value

        raise SecretNotFoundError(
            f"Secret '{name}' not found. Set it via OS keychain "
            f"(moneybin db init) or env var {env_var}."
        )

    def get_env(self, name: str) -> str:
        """Retrieve a secret from environment variable only.

        Use for secrets that don't need keychain storage (API keys,
        server credentials).

        Args:
            name: Secret name (e.g. "SYNC__API_KEY").
                  Looks up MONEYBIN_{name}.

        Returns:
            The secret value.

        Raises:
            SecretNotFoundError: If the env var is not set.
        """
        env_var = f"{_ENV_PREFIX}{name}"
        value = os.environ.get(env_var)
        if value is not None:
            return value

        raise SecretNotFoundError(
            f"Secret '{name}' not found. Set env var {env_var}."
        )

    def set_key(self, name: str, value: str) -> None:
        """Store a secret in the OS keychain.

        Args:
            name: Secret name (e.g. "DATABASE__ENCRYPTION_KEY").
            value: Secret value to store.
        """
        keyring.set_password(_SERVICE_NAME, name, value)
        logger.debug("Stored secret '%s' in OS keychain", name)

    def delete_key(self, name: str) -> None:
        """Remove a secret from the OS keychain.

        Args:
            name: Secret name to remove.
        """
        keyring.delete_password(_SERVICE_NAME, name)
        logger.debug("Removed secret '%s' from OS keychain", name)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_secrets.py -v`
Expected: All 6 tests PASS.

- [ ] **Step 5: Run linting and type checking**

Run: `uv run ruff format src/moneybin/secrets.py tests/moneybin/test_secrets.py && uv run ruff check src/moneybin/secrets.py tests/moneybin/test_secrets.py && uv run pyright src/moneybin/secrets.py`
Expected: Clean.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/secrets.py tests/moneybin/test_secrets.py
git commit -m "feat: add SecretStore for centralized keychain/env secret access"
```

---

## Task 3: Config Changes

**Files:**
- Modify: `src/moneybin/config.py`

- [ ] **Step 1: Add encryption_key_mode and temp_directory to DatabaseConfig**

In `src/moneybin/config.py`, update the `DatabaseConfig` class. After the existing `create_dirs` field, add:

```python
    encryption_key_mode: Literal["auto", "passphrase"] = Field(
        default="auto",
        description="How the encryption key is managed: auto-generated or user passphrase",
    )
    temp_directory: Path | None = Field(
        default=None,
        description="DuckDB temp spill directory. Defaults to data/<profile>/temp/",
    )
```

- [ ] **Step 2: Add backup_path and temp_directory defaults in MoneyBinSettings.__init__**

In `MoneyBinSettings.__init__`, after the database path resolution block (around line 247), update the database config construction to include backup and temp defaults. Replace the existing database config construction with:

```python
        if "database" not in kwargs or (
            "database" in kwargs
            and kwargs["database"].path == Path("data/default/moneybin.duckdb")
        ):
            if duckdb_path:
                db_path = _resolve_path(base, Path(duckdb_path))
            else:
                db_path = base / f"data/{profile}/moneybin.duckdb"
            kwargs["database"] = DatabaseConfig(
                path=db_path,
                backup_path=base / f"data/{profile}/backups",
                temp_directory=base / f"data/{profile}/temp",
            )
```

- [ ] **Step 3: Add backup and temp directories to create_directories()**

In `MoneyBinSettings.create_directories()`, add the new directories and set permissions. Replace the method body:

```python
    def create_directories(self) -> None:
        """Create necessary directories for the application."""
        import stat
        import sys

        directories = [
            self.database.path.parent,
            self.data.raw_data_path,
            self.data.temp_data_path,
            self.logging.log_file_path.parent,
        ]

        if self.database.backup_path:
            directories.append(self.database.backup_path)
        if self.database.temp_directory:
            directories.append(self.database.temp_directory)

        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

            # Set restrictive permissions on data directories (macOS/Linux)
            if sys.platform != "win32" and directory != self.logging.log_file_path.parent:
                try:
                    directory.chmod(stat.S_IRWXU)  # 0700
                except OSError:
                    pass  # Best-effort on platforms that don't support chmod
```

- [ ] **Step 4: Run existing config tests**

Run: `uv run pytest tests/moneybin/test_config_profiles.py -v`
Expected: All existing tests still pass.

- [ ] **Step 5: Run linting**

Run: `uv run ruff format src/moneybin/config.py && uv run ruff check src/moneybin/config.py && uv run pyright src/moneybin/config.py`
Expected: Clean.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/config.py
git commit -m "feat: add encryption_key_mode, temp_directory, backup_path to DatabaseConfig"
```

---

## Task 4: Database Class

**Files:**
- Create: `src/moneybin/database.py`
- Create: `tests/moneybin/test_database.py`

This is the largest and most important task. The `Database` class is the sole entry point for all database access.

- [ ] **Step 1: Write the failing tests**

Create `tests/moneybin/test_database.py`:

```python
"""Tests for Database class — centralized encrypted connection management."""

import os
import stat
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from moneybin.database import Database, DatabaseKeyError, get_database


@pytest.fixture()
def db_dir(tmp_path: Path) -> Path:
    """Provide a temporary directory for test databases."""
    return tmp_path / "data" / "test"


@pytest.fixture()
def encryption_key() -> str:
    return "test-encryption-key-256bit-placeholder"


@pytest.fixture()
def mock_secret_store(encryption_key: str) -> MagicMock:
    """Mock SecretStore that returns a test encryption key."""
    store = MagicMock()
    store.get_key.return_value = encryption_key
    return store


class TestDatabaseInit:
    """Database initialization and encrypted attachment."""

    def test_creates_encrypted_database(
        self, db_dir: Path, mock_secret_store: MagicMock, encryption_key: str
    ) -> None:
        """New database file is created and encrypted."""
        db_path = db_dir / "moneybin.duckdb"
        db = Database(db_path, secret_store=mock_secret_store)
        try:
            assert db_path.exists()
            mock_secret_store.get_key.assert_called_once_with(
                "DATABASE__ENCRYPTION_KEY"
            )
        finally:
            db.close()

    def test_sets_file_permissions_0600(
        self, db_dir: Path, mock_secret_store: MagicMock
    ) -> None:
        """Database file is created with owner-only permissions."""
        if sys.platform == "win32":
            pytest.skip("File permissions not enforced on Windows")
        db_path = db_dir / "moneybin.duckdb"
        db = Database(db_path, secret_store=mock_secret_store)
        try:
            mode = db_path.stat().st_mode & 0o777
            assert mode == 0o600
        finally:
            db.close()

    def test_encrypted_file_unreadable_without_key(
        self, db_dir: Path, mock_secret_store: MagicMock
    ) -> None:
        """Database file cannot be opened without the encryption key."""
        db_path = db_dir / "moneybin.duckdb"
        db = Database(db_path, secret_store=mock_secret_store)
        db.execute(
            "CREATE TABLE test_data (id INTEGER, name VARCHAR)"
        )
        db.execute("INSERT INTO test_data VALUES (1, 'Alice')")
        db.close()

        # Try to open without key — should fail
        with pytest.raises(duckdb.IOException):
            bad_conn = duckdb.connect(str(db_path))
            bad_conn.execute("SELECT * FROM test_data")

    def test_runs_init_schemas(
        self, db_dir: Path, mock_secret_store: MagicMock
    ) -> None:
        """Schema initialization runs on first open."""
        db_path = db_dir / "moneybin.duckdb"
        db = Database(db_path, secret_store=mock_secret_store)
        try:
            # init_schemas creates the raw, core, and app schemas
            result = db.execute(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name IN ('raw', 'core', 'app') ORDER BY schema_name"
            ).fetchall()
            schemas = [r[0] for r in result]
            assert "app" in schemas
            assert "raw" in schemas
        finally:
            db.close()

    def test_raises_database_key_error_when_no_key(
        self, db_dir: Path
    ) -> None:
        """DatabaseKeyError raised when SecretStore cannot find the key."""
        store = MagicMock()
        from moneybin.secrets import SecretNotFoundError

        store.get_key.side_effect = SecretNotFoundError("not found")
        db_path = db_dir / "moneybin.duckdb"
        with pytest.raises(DatabaseKeyError, match="encryption key"):
            Database(db_path, secret_store=store)


class TestDatabaseOperations:
    """Database.execute(), .sql(), .conn property."""

    @pytest.fixture()
    def db(self, db_dir: Path, mock_secret_store: MagicMock) -> Database:
        db_path = db_dir / "moneybin.duckdb"
        database = Database(db_path, secret_store=mock_secret_store)
        yield database  # type: ignore[misc]
        database.close()

    def test_execute_with_params(self, db: Database) -> None:
        """Parameterized query works on attached encrypted database."""
        db.execute("CREATE TABLE test (id INTEGER, val VARCHAR)")
        db.execute("INSERT INTO test VALUES (?, ?)", [1, "hello"])
        result = db.execute("SELECT val FROM test WHERE id = ?", [1]).fetchone()
        assert result is not None
        assert result[0] == "hello"

    def test_sql_convenience(self, db: Database) -> None:
        """sql() method works for parameter-free queries."""
        db.execute("CREATE TABLE test2 (id INTEGER)")
        db.execute("INSERT INTO test2 VALUES (42)")
        result = db.sql("SELECT * FROM test2").fetchone()
        assert result is not None
        assert result[0] == 42

    def test_conn_property(self, db: Database) -> None:
        """conn property exposes the underlying DuckDB connection."""
        conn = db.conn
        assert isinstance(conn, duckdb.DuckDBPyConnection)

    def test_close_releases_resources(
        self, db_dir: Path, mock_secret_store: MagicMock
    ) -> None:
        """After close(), conn access raises."""
        db_path = db_dir / "moneybin.duckdb"
        db = Database(db_path, secret_store=mock_secret_store)
        db.close()
        with pytest.raises(RuntimeError, match="closed"):
            _ = db.conn


class TestGetDatabase:
    """get_database() singleton behavior."""

    def test_returns_same_instance(
        self, db_dir: Path, mock_secret_store: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Repeated calls return the same Database instance."""
        from moneybin import database as db_module

        db_path = db_dir / "moneybin.duckdb"

        # Patch get_settings to return our test path
        mock_settings = MagicMock()
        mock_settings.database.path = db_path
        mock_settings.database.temp_directory = db_dir / "temp"
        mock_settings.database.create_dirs = True
        monkeypatch.setattr(db_module, "_database_instance", None)
        monkeypatch.setattr(
            "moneybin.database.get_settings", lambda: mock_settings
        )
        monkeypatch.setattr(
            "moneybin.database.SecretStore", lambda: mock_secret_store
        )

        db1 = get_database()
        db2 = get_database()
        assert db1 is db2
        db1.close()
        monkeypatch.setattr(db_module, "_database_instance", None)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_database.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'moneybin.database'`

- [ ] **Step 3: Implement the Database class**

Create `src/moneybin/database.py`:

```python
"""Centralized encrypted database connection management.

The Database class is the sole entry point for all database access in
MoneyBin. It handles encryption key retrieval, encrypted file attachment,
extension loading, schema initialization, and migrations.

Usage::

    from moneybin.database import get_database

    db = get_database()
    db.execute("SELECT * FROM core.fct_transactions WHERE account_id = ?", [acct_id])

Never call ``duckdb.connect()`` directly. See the data-protection spec
(``docs/specs/privacy-data-protection.md``) for the full design.
"""

import logging
import os
import stat
import sys
from pathlib import Path
from typing import Any

import duckdb

from moneybin.secrets import SecretNotFoundError, SecretStore

logger = logging.getLogger(__name__)

_KEY_NAME = "DATABASE__ENCRYPTION_KEY"


class DatabaseKeyError(Exception):
    """Raised when the database encryption key cannot be retrieved."""


class Database:
    """Encrypted DuckDB connection manager.

    One long-lived read-write connection per process. The initialization
    sequence:
        a. Retrieve encryption key via SecretStore
        b. Open in-memory DuckDB connection
        c. Load required extensions (httpfs)
        d. Attach encrypted database file
        e. USE <attached_db>
        f. Run init_schemas() (idempotent baseline DDL)
        g-i. Migration steps (not yet implemented — see database-migration.md)

    The Database class does NOT own query logic, transaction boundaries,
    domain rules, or data access patterns. It is infrastructure.

    Args:
        db_path: Path to the DuckDB database file.
        secret_store: SecretStore instance for key retrieval. If None,
            creates a new one.
    """

    def __init__(
        self,
        db_path: Path,
        *,
        secret_store: SecretStore | None = None,
    ) -> None:
        self._db_path = db_path
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._closed = False

        store = secret_store or SecretStore()

        # Step a: Retrieve encryption key
        try:
            encryption_key = store.get_key(_KEY_NAME)
        except SecretNotFoundError as e:
            raise DatabaseKeyError(
                f"Cannot open database — encryption key not found. "
                f"Run 'moneybin db init' to create a new database, or set "
                f"MONEYBIN_{_KEY_NAME} for CI/headless environments."
            ) from e

        # Ensure parent directory exists
        db_path.parent.mkdir(parents=True, exist_ok=True)

        is_new = not db_path.exists()

        # Step b: Open in-memory connection
        self._conn = duckdb.connect()

        # Step c: Load required extensions
        self._conn.execute("INSTALL httpfs; LOAD httpfs;")

        # Step d: Attach encrypted database file
        self._conn.execute(
            "ATTACH ? AS moneybin (TYPE DUCKDB, ENCRYPTION_KEY ?)",
            [str(db_path), encryption_key],
        )

        # Step e: USE attached database
        self._conn.execute("USE moneybin")

        # Set file permissions on new databases (macOS/Linux)
        if is_new and sys.platform != "win32":
            try:
                db_path.chmod(stat.S_IRUSR | stat.S_IWUSR)  # 0600
            except OSError:
                logger.warning(
                    "Could not set file permissions on %s", db_path
                )

        # Validate permissions on existing databases
        if not is_new and sys.platform != "win32":
            self._check_permissions(db_path)

        # Step f: Run init_schemas (idempotent)
        from moneybin.schema import init_schemas

        init_schemas(self._conn)

        # Steps g-i: Migration (stub — see database-migration.md spec)
        if not os.environ.get("MONEYBIN_NO_AUTO_UPGRADE"):
            logger.debug(
                "Migration steps skipped — MigrationRunner not yet implemented"
            )

        logger.info("Database connection established: %s", db_path)

    def _check_permissions(self, db_path: Path) -> None:
        """Warn if database file has overly permissive permissions.

        Args:
            db_path: Path to the database file.
        """
        try:
            mode = db_path.stat().st_mode & 0o777
            if mode & 0o077:  # group or world readable/writable
                logger.warning(
                    "⚠️  Database file %s has permissive permissions (%04o). "
                    "Run: chmod 600 %s",
                    db_path,
                    mode,
                    db_path,
                )
        except OSError:
            pass

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        """The underlying DuckDB connection.

        Use this for bulk operations that require the raw connection
        (e.g., ingest_dataframe). For normal queries, prefer execute().

        Raises:
            RuntimeError: If the database has been closed.
        """
        if self._closed or self._conn is None:
            raise RuntimeError(
                "Database connection is closed. "
                "Call get_database() to get a new instance."
            )
        return self._conn

    @property
    def path(self) -> Path:
        """Path to the database file."""
        return self._db_path

    def execute(
        self, query: str, params: list[Any] | None = None
    ) -> duckdb.DuckDBPyRelation:
        """Execute a parameterized SQL query.

        Args:
            query: SQL query string with ? placeholders.
            params: Parameter values for placeholders.

        Returns:
            DuckDB relation with query results.
        """
        if params:
            return self.conn.execute(query, params)
        return self.conn.execute(query)

    def sql(self, query: str) -> duckdb.DuckDBPyRelation:
        """Execute a parameter-free SQL query.

        Convenience method for queries that don't need parameters.

        Args:
            query: SQL query string.

        Returns:
            DuckDB relation with query results.
        """
        return self.conn.sql(query)

    def close(self) -> None:
        """Close the database connection and release resources."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
        self._closed = True
        logger.debug("Database connection closed: %s", self._db_path)


# Singleton instance
_database_instance: Database | None = None


def get_database() -> Database:
    """Get the singleton Database instance for the current profile.

    Creates the Database on first call, reuses on subsequent calls.
    The database path comes from get_settings().database.path.

    Returns:
        The Database singleton instance.
    """
    global _database_instance  # noqa: PLW0603 — module-level singleton is intentional

    if _database_instance is not None:
        return _database_instance

    from moneybin.config import get_settings

    settings = get_settings()
    db = Database(settings.database.path)
    _database_instance = db
    return db


def close_database() -> None:
    """Close and clear the singleton Database instance."""
    global _database_instance  # noqa: PLW0603 — module-level singleton is intentional

    if _database_instance is not None:
        _database_instance.close()
        _database_instance = None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_database.py -v`
Expected: All tests PASS.

- [ ] **Step 5: Run linting and type checking**

Run: `uv run ruff format src/moneybin/database.py tests/moneybin/test_database.py && uv run ruff check src/moneybin/database.py tests/moneybin/test_database.py && uv run pyright src/moneybin/database.py`
Expected: Clean.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/database.py tests/moneybin/test_database.py
git commit -m "feat: add Database class for encrypted connection management"
```

---

## Task 5: SanitizedLogFormatter

**Files:**
- Create: `src/moneybin/log_sanitizer.py`
- Create: `tests/moneybin/test_log_sanitizer.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/moneybin/test_log_sanitizer.py`:

```python
"""Tests for SanitizedLogFormatter — PII pattern detection and masking."""

import logging

import pytest

from moneybin.log_sanitizer import SanitizedLogFormatter


@pytest.fixture()
def formatter() -> SanitizedLogFormatter:
    return SanitizedLogFormatter("%(message)s")


@pytest.fixture()
def make_record() -> callable:
    """Factory for log records."""
    logger = logging.getLogger("test.sanitizer")

    def _make(msg: str, level: int = logging.INFO) -> logging.LogRecord:
        return logger.makeRecord(
            "test.sanitizer", level, "test.py", 1, msg, (), None
        )

    return _make


class TestSSNMasking:
    def test_masks_ssn_pattern(
        self, formatter: SanitizedLogFormatter, make_record: callable
    ) -> None:
        record = make_record("User SSN is 123-45-6789")
        result = formatter.format(record)
        assert "123-45-6789" not in result
        assert "***-**-****" in result

    def test_does_not_mask_non_ssn_dashes(
        self, formatter: SanitizedLogFormatter, make_record: callable
    ) -> None:
        record = make_record("Date is 2026-04-20")
        result = formatter.format(record)
        assert "2026-04-20" in result


class TestAccountNumberMasking:
    def test_masks_long_digit_sequence(
        self, formatter: SanitizedLogFormatter, make_record: callable
    ) -> None:
        record = make_record("Account 12345678901234")
        result = formatter.format(record)
        assert "12345678901234" not in result
        assert "****...1234" in result

    def test_does_not_mask_short_numbers(
        self, formatter: SanitizedLogFormatter, make_record: callable
    ) -> None:
        record = make_record("Loaded 142 transactions")
        result = formatter.format(record)
        assert "142" in result


class TestDollarAmountMasking:
    def test_masks_dollar_amount(
        self, formatter: SanitizedLogFormatter, make_record: callable
    ) -> None:
        record = make_record("Balance is $1,234.56")
        result = formatter.format(record)
        assert "$1,234.56" not in result
        assert "$***" in result

    def test_masks_simple_dollar(
        self, formatter: SanitizedLogFormatter, make_record: callable
    ) -> None:
        record = make_record("Amount: $500.00")
        result = formatter.format(record)
        assert "$500.00" not in result
        assert "$***" in result


class TestCleanPassthrough:
    def test_clean_log_passes_unchanged(
        self, formatter: SanitizedLogFormatter, make_record: callable
    ) -> None:
        msg = "Loaded 142 transactions for account_id abc-123"
        record = make_record(msg)
        result = formatter.format(record)
        assert result == msg

    def test_record_counts_pass(
        self, formatter: SanitizedLogFormatter, make_record: callable
    ) -> None:
        msg = "Processed 50 records in 2.3 seconds"
        record = make_record(msg)
        result = formatter.format(record)
        assert result == msg


class TestWarningOnMask:
    def test_emits_warning_when_masking(
        self, formatter: SanitizedLogFormatter, make_record: callable, caplog: pytest.LogCaptureFixture
    ) -> None:
        """When masking occurs, a WARNING is emitted identifying the source."""
        record = make_record("SSN: 123-45-6789")
        with caplog.at_level(logging.WARNING, logger="moneybin.log_sanitizer"):
            formatter.format(record)
        assert any("PII pattern detected" in r.message for r in caplog.records)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_log_sanitizer.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'moneybin.log_sanitizer'`

- [ ] **Step 3: Implement SanitizedLogFormatter**

Create `src/moneybin/log_sanitizer.py`:

```python
"""PII-aware log formatter for MoneyBin.

SanitizedLogFormatter scans formatted log output for PII patterns and
masks them before they reach the log file. It is a runtime safety net,
not a substitute for writing clean log statements.

The formatter masks and emits a warning — it never suppresses log entries.
"""

import logging
import re

_sanitizer_logger = logging.getLogger(__name__)

# SSN: NNN-NN-NNNN (but not dates like 2026-04-20)
_SSN_PATTERN = re.compile(r"\b(\d{3})-(\d{2})-(\d{4})\b")

# Account numbers: 8+ consecutive digits (not preceded by year-like context)
_ACCOUNT_PATTERN = re.compile(r"(?<!\d)(\d{8,})(?!\d)")

# Dollar amounts: $N or $N,NNN or $N.NN etc.
_DOLLAR_PATTERN = re.compile(r"\$[\d,]+(?:\.\d{2})?")

# Date-like patterns to exclude from SSN matching: YYYY-MM-DD
_DATE_PATTERN = re.compile(r"\b(19|20)\d{2}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\b")


def _is_date(match: re.Match[str]) -> bool:
    """Check if an SSN-like match is actually a date."""
    full = match.group(0)
    # Check if the three-digit prefix is a plausible year prefix
    year = int(match.group(1))
    month = int(match.group(2))
    day = int(match.group(3))
    return (1900 <= year <= 2099) and (1 <= month <= 12) and (1 <= day <= 31)


class SanitizedLogFormatter(logging.Formatter):
    """Log formatter that detects and masks PII patterns.

    Patterns detected:
    - SSN: NNN-NN-NNNN → ***-**-****
    - Account numbers: 8+ digits → ****...NNNN (last 4)
    - Dollar amounts: $N,NNN.NN → $***

    When a pattern is masked, a separate WARNING is emitted identifying
    the leak source (module, line number).
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record, masking any PII patterns found.

        Args:
            record: The log record to format.

        Returns:
            Formatted and sanitized log string.
        """
        formatted = super().format(record)
        masked = False

        # Mask SSNs (but not dates)
        def ssn_replacer(match: re.Match[str]) -> str:
            nonlocal masked
            if _is_date(match):
                return match.group(0)
            masked = True
            return "***-**-****"

        result = _SSN_PATTERN.sub(ssn_replacer, formatted)

        # Mask dollar amounts
        new_result = _DOLLAR_PATTERN.sub("$***", result)
        if new_result != result:
            masked = True
            result = new_result

        # Mask account numbers (8+ digit sequences)
        def account_replacer(match: re.Match[str]) -> str:
            nonlocal masked
            digits = match.group(1)
            if len(digits) >= 8:
                masked = True
                return f"****...{digits[-4:]}"
            return digits

        result = _ACCOUNT_PATTERN.sub(account_replacer, result)

        if masked:
            _sanitizer_logger.warning(
                "PII pattern detected and masked in log output "
                "(source: %s:%s)",
                record.pathname,
                record.lineno,
            )

        return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_log_sanitizer.py -v`
Expected: All tests PASS.

- [ ] **Step 5: Run linting and type checking**

Run: `uv run ruff format src/moneybin/log_sanitizer.py tests/moneybin/test_log_sanitizer.py && uv run ruff check src/moneybin/log_sanitizer.py tests/moneybin/test_log_sanitizer.py && uv run pyright src/moneybin/log_sanitizer.py`
Expected: Clean.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/log_sanitizer.py tests/moneybin/test_log_sanitizer.py
git commit -m "feat: add SanitizedLogFormatter for PII detection and masking"
```

---

## Task 6: Wire SanitizedLogFormatter into Logging

**Files:**
- Modify: `src/moneybin/logging/config.py`

- [ ] **Step 1: Import and use SanitizedLogFormatter for file handler**

In `src/moneybin/logging/config.py`, in the `setup_logging()` function, change the file handler formatter (around line 135) from:

```python
        file_handler.setFormatter(logging.Formatter(config.format_string))
```

to:

```python
        from moneybin.log_sanitizer import SanitizedLogFormatter

        file_handler.setFormatter(SanitizedLogFormatter(config.format_string))
```

Also set the log file permissions. After `log_file.parent.mkdir(parents=True, exist_ok=True)` (line 132), add:

```python
        # Set restrictive permissions on log file (macOS/Linux)
        if sys.platform != "win32" and log_file.exists():
            try:
                import stat as stat_mod

                log_file.chmod(stat_mod.S_IRUSR | stat_mod.S_IWUSR)  # 0600
            except OSError:
                pass
```

Note: `sys` is already imported in this file.

- [ ] **Step 2: Run existing logging tests**

Run: `uv run pytest tests/moneybin/test_logging_config.py -v`
Expected: All existing tests still pass.

- [ ] **Step 3: Commit**

```bash
git add src/moneybin/logging/config.py
git commit -m "feat: wire SanitizedLogFormatter into file logging handler"
```

---

## Task 7: Migrate Loaders to Accept Database

**Files:**
- Modify: `src/moneybin/loaders/ofx_loader.py`
- Modify: `src/moneybin/loaders/csv_loader.py`
- Modify: `src/moneybin/loaders/w2_loader.py`

Each loader currently accepts `database_path: Path` in `__init__` and calls `duckdb.connect()` in every method. Change them to accept a `Database` instance and use `db.conn` for all operations.

- [ ] **Step 1: Migrate OFXLoader**

Read `src/moneybin/loaders/ofx_loader.py` fully first. Then make these changes:

1. Replace the `duckdb` import and add `Database` import:
```python
from moneybin.database import Database
```

2. Change `__init__` signature from `database_path: Path | str` to `db: Database`:
```python
    def __init__(self, db: Database):
        """Initialize the OFX loader.

        Args:
            db: Database instance for all database operations.
        """
        self.db = db
        self.sql_dir = Path(__file__).parent.parent / "sql" / "schema"
        logger.info(f"Initialized OFX loader for database: {db.path}")
```

3. In every method, replace `conn = duckdb.connect(str(self.database_path))` and the surrounding `try/finally` with `conn = self.db.conn`. Remove the `try/finally conn.close()` blocks — the Database owns the connection lifecycle.

4. Remove the `import duckdb` line if no longer needed.

- [ ] **Step 2: Migrate CSVLoader**

Same pattern as OFXLoader. Read `src/moneybin/loaders/csv_loader.py` fully, then:

1. Replace `database_path: Path | str` with `db: Database` in `__init__`.
2. Replace all `duckdb.connect()` calls with `self.db.conn`.
3. Remove `try/finally conn.close()` blocks.

- [ ] **Step 3: Migrate W2Loader**

Same pattern. Read `src/moneybin/loaders/w2_loader.py` fully, then:

1. Replace `database_path: Path | str` with `db: Database` in `__init__`.
2. Replace all `duckdb.connect()` calls with `self.db.conn`.
3. Remove `try/finally conn.close()` blocks.

- [ ] **Step 4: Update loader tests**

Read the existing loader tests and update them to provide a `Database` instance instead of a `Path`. The test pattern:

```python
from moneybin.database import Database

# In fixtures, create a Database with a mock SecretStore:
from unittest.mock import MagicMock
mock_store = MagicMock()
mock_store.get_key.return_value = "test-key"
db = Database(tmp_path / "test.duckdb", secret_store=mock_store)

# Pass db to loader:
loader = OFXLoader(db)
```

Update each test file:
- `tests/moneybin/test_loaders/test_ofx_loader.py`
- `tests/moneybin/test_loaders/test_csv_loader.py`
- `tests/moneybin/test_loaders/test_w2_loader.py`

- [ ] **Step 5: Run loader tests**

Run: `uv run pytest tests/moneybin/test_loaders/ -v`
Expected: All loader tests pass.

- [ ] **Step 6: Run linting**

Run: `uv run ruff format src/moneybin/loaders/ && uv run ruff check src/moneybin/loaders/`
Expected: Clean.

- [ ] **Step 7: Commit**

```bash
git add src/moneybin/loaders/ tests/moneybin/test_loaders/
git commit -m "refactor: migrate loaders from duckdb.connect() to Database"
```

---

## Task 8: Migrate Services to Accept Database

**Files:**
- Modify: `src/moneybin/services/import_service.py`
- Modify: `src/moneybin/services/categorization_service.py`

- [ ] **Step 1: Migrate import_service.py**

Read the full file first. Key changes:

1. Replace `import duckdb` with `from moneybin.database import Database`.

2. Change `import_file()` signature: `db_path: Path` → `db: Database`. Remove the `db_path` parameter everywhere.

3. Change `_import_ofx()`, `_import_w2()`, `_import_csv()` signatures: `db_path: Path` → `db: Database`.

4. In `_import_ofx()`, change `OFXLoader(db_path)` to `OFXLoader(db)`. Replace the date-range query `duckdb.connect(str(db_path), read_only=True)` with `db.execute(...)`.

5. In `_import_w2()`, change `W2Loader(db_path)` to `W2Loader(db)`.

6. In `_import_csv()`, change `CSVLoader(db_path)` to `CSVLoader(db)`. Replace the date-range query similarly.

7. In `_run_transforms()`, this is tricky — SQLMesh manages its own connection. Keep `db_path: Path` for this function only, getting it from `db.path`:
```python
def _run_transforms(db_path: Path) -> bool:
    # ... unchanged — SQLMesh needs its own connection
```

8. In `import_file()`, call `_run_transforms(db.path)` instead of `_run_transforms(db_path)`.

9. In `_apply_categorization()`, change `db_path: Path` → `db: Database`. Replace `duckdb.connect(str(db_path))` with `db`:
```python
def _apply_categorization(db: Database) -> None:
    from moneybin.services.categorization_service import apply_deterministic_categorization
    try:
        stats = apply_deterministic_categorization(db)
        # ...
```

- [ ] **Step 2: Migrate categorization_service.py**

Read the full file. Key changes:

1. Replace `import duckdb` with `from moneybin.database import Database`.

2. Change all function signatures from `conn: duckdb.DuckDBPyConnection` to `db: Database`.

3. Inside functions, replace `conn.execute(...)` with `db.execute(...)` or `db.conn.execute(...)` as appropriate.

- [ ] **Step 3: Update service tests**

Update `tests/moneybin/test_services/test_categorization_service.py` to pass `Database` instances instead of raw connections.

- [ ] **Step 4: Run service tests**

Run: `uv run pytest tests/moneybin/test_services/ -v`
Expected: All service tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/services/ tests/moneybin/test_services/
git commit -m "refactor: migrate services from duckdb.connect()/conn to Database"
```

---

## Task 9: Migrate CLI Commands

**Files:**
- Modify: `src/moneybin/cli/commands/categorize.py`
- Modify: `src/moneybin/cli/commands/import_cmd.py`

- [ ] **Step 1: Migrate categorize.py**

Replace the pattern in every command function:

**Before:**
```python
db_path = get_database_path()
try:
    conn = duckdb.connect(str(db_path), read_only=False)
    try:
        stats = apply_deterministic_categorization(conn)
        # ...
    finally:
        conn.close()
```

**After:**
```python
from moneybin.database import get_database
db = get_database()
stats = apply_deterministic_categorization(db)
# ...
```

Remove `import duckdb` and `from ...config import get_database_path`. Add `from moneybin.database import get_database`.

Apply this pattern to all 4 commands: `apply-rules`, `seed`, `stats`, `list-rules`. For read-only commands (`stats`, `list-rules`), the single r/w connection is fine — no distinction needed.

- [ ] **Step 2: Migrate import_cmd.py**

1. In `import_file()`, change:
```python
from moneybin.database import get_database
db = get_database()
result = do_import(
    db=db,
    file_path=source,
    # ...
)
```

2. In `import_status()`, change:
```python
from moneybin.database import get_database
db = get_database()
_print_import_status(db.conn)
```

Remove the `duckdb` import and `get_database_path` import.

- [ ] **Step 3: Update CLI tests**

Update `tests/moneybin/test_cli/test_db_commands.py` and `tests/moneybin/test_cli/test_import_commands.py` to mock `get_database` instead of `duckdb.connect`.

- [ ] **Step 4: Run CLI tests**

Run: `uv run pytest tests/moneybin/test_cli/ -v`
Expected: All CLI tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/cli/commands/categorize.py src/moneybin/cli/commands/import_cmd.py tests/moneybin/test_cli/
git commit -m "refactor: migrate CLI commands from duckdb.connect() to get_database()"
```

---

## Task 10: Migrate MCP Server

**Files:**
- Modify: `src/moneybin/mcp/server.py`

This is a significant refactor. The MCP server currently has its own connection lifecycle (`_db`, `_db_path`, `get_db()`, `get_write_db()`, `refresh_read_connection()`, `init_db()`, `close_db()`). All of this is replaced by the `Database` class.

- [ ] **Step 1: Replace connection management with get_database()**

Read `src/moneybin/mcp/server.py` fully. Key changes:

1. Remove the module-level state (`_db`, `_db_path`) and connection management functions (`get_db_path()`, `get_db()`, `refresh_read_connection()`, `get_write_db()`).

2. Replace with:
```python
from moneybin.database import get_database

def get_db() -> duckdb.DuckDBPyConnection:
    """Get the DuckDB connection for queries.

    Returns:
        The active DuckDB connection.
    """
    return get_database().conn


def get_db_path() -> Path:
    """Get the path to the DuckDB database file."""
    return get_database().path
```

3. Remove the `get_write_db()` context manager — the `Database` class uses a single r/w connection, so there's no read-only/read-write distinction.

4. Find all call sites of `get_write_db()` in MCP tools (these will be in other files under `src/moneybin/mcp/`). Replace:
```python
# Before:
with get_write_db() as conn:
    conn.execute(...)
    refresh_read_connection()

# After:
db = get_database()
db.execute(...)
```

5. Simplify `init_db()`:
```python
def init_db(db_path: Path) -> None:
    """Initialize the database.

    The Database class handles encryption, schema init, and migrations.
    This function just ensures get_database() is called to trigger init.

    Args:
        db_path: Path to the DuckDB database file (used for backwards
            compatibility — actual path comes from settings).
    """
    get_database()
    logger.info("Database initialized: %s", db_path)
```

6. Simplify `close_db()`:
```python
def close_db() -> None:
    """Close the DuckDB connection if open."""
    from moneybin.database import close_database
    close_database()
```

- [ ] **Step 2: Update MCP tool files that use get_write_db()**

Search for `get_write_db` in `src/moneybin/mcp/` and replace all usages. Each `with get_write_db() as conn:` block becomes direct `get_database()` usage. Also remove any `refresh_read_connection()` calls — no longer needed with a single connection.

- [ ] **Step 3: Run MCP tests**

Run: `uv run pytest tests/moneybin/test_mcp/ -v`
Expected: All MCP tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/moneybin/mcp/
git commit -m "refactor: migrate MCP server from ad-hoc connections to Database class"
```

---

## Task 11: Rewrite db CLI — Init with Encryption

**Files:**
- Modify: `src/moneybin/cli/commands/db.py`

- [ ] **Step 1: Rewrite db init command**

Replace the current `init_schemas` command with an encryption-aware version:

```python
@app.command("init")
def init_db(
    database: Path | None = typer.Option(
        None, "--database", "-d",
        help="Path to DuckDB database file (default: profile config)",
    ),
    passphrase: bool = typer.Option(
        False, "--passphrase",
        help="Use passphrase-based key derivation instead of auto-generated key",
    ),
    yes: bool = typer.Option(
        False, "--yes", "-y",
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
        # Passphrase mode: prompt, derive key via Argon2id, store derived key
        import argon2

        pp = typer.prompt("Enter passphrase", hide_input=True)
        pp_confirm = typer.prompt("Confirm passphrase", hide_input=True)
        if pp != pp_confirm:
            logger.error("❌ Passphrases do not match")
            raise typer.Exit(1)

        # Derive key using Argon2id
        hasher = argon2.PasswordHasher(
            time_cost=3, memory_cost=65536, parallelism=4, hash_len=32, type=argon2.Type.ID
        )
        # Use the hash as the encryption key (it includes salt)
        key_hash = hasher.hash(pp)
        # Extract the raw hash for use as DuckDB key (base64-encoded portion)
        encryption_key = key_hash.split("$")[-1]

        store.set_key("DATABASE__ENCRYPTION_KEY", encryption_key)
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
```

- [ ] **Step 2: Run linting**

Run: `uv run ruff format src/moneybin/cli/commands/db.py && uv run ruff check src/moneybin/cli/commands/db.py`
Expected: Clean.

- [ ] **Step 3: Commit**

```bash
git add src/moneybin/cli/commands/db.py
git commit -m "feat: rewrite db init with encryption key generation and keychain storage"
```

---

## Task 12: db shell/ui/query with -init Temp Script

**Files:**
- Modify: `src/moneybin/cli/commands/db.py`

- [ ] **Step 1: Add helper to create -init temp script**

Add a helper function that creates a temporary SQL init script for the DuckDB CLI:

```python
import tempfile

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
            f.write(f"LOAD httpfs;\n")
            f.write(
                f"ATTACH '{db_path}' AS moneybin "
                f"(TYPE DUCKDB, ENCRYPTION_KEY '{encryption_key}');\n"
            )
            f.write("USE moneybin;\n")
        if sys.platform != "win32":
            os.chmod(script_path, 0o600)
    except Exception:
        os.unlink(script_path)
        raise

    return Path(script_path)
```

- [ ] **Step 2: Rewrite db shell command**

```python
@app.command("shell")
def open_shell(
    database: Path | None = typer.Option(
        None, "--database", "-d",
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
        cmd = ["duckdb", "-init", str(init_script)]
        subprocess.run(cmd, check=True)  # noqa: S603 — cmd built from static args
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ DuckDB shell failed: {e}")
        raise typer.Exit(1) from e
    except KeyboardInterrupt:
        logger.info("\n✅ DuckDB shell closed")
        sys.exit(0)
    finally:
        init_script.unlink(missing_ok=True)
```

- [ ] **Step 3: Rewrite db ui command**

Same pattern but with `-ui` flag:

```python
@app.command("ui")
def open_ui(
    database: Path | None = typer.Option(
        None, "--database", "-d",
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
        cmd = ["duckdb", "-init", str(init_script), "-ui"]
        subprocess.run(cmd, check=True)  # noqa: S603 — cmd built from static args
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ DuckDB UI failed to start: {e}")
        raise typer.Exit(1) from e
    except KeyboardInterrupt:
        logger.info("\n✅ DuckDB UI stopped")
        sys.exit(0)
    finally:
        init_script.unlink(missing_ok=True)
```

- [ ] **Step 4: Rewrite db query command**

```python
@app.command("query")
def run_query(
    sql: str = typer.Argument(..., help="SQL query to execute"),
    database: Path | None = typer.Option(
        None, "--database", "-d",
        help="Path to DuckDB database file (default: profile config)",
    ),
    output_format: str = typer.Option(
        "table", "--format", "-f",
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
        cmd = ["duckdb", "-init", str(init_script), "-c", sql]
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
```

- [ ] **Step 5: Update imports at top of db.py**

Make sure the top of `db.py` has these imports and remove `duckdb`:

```python
import logging
import os
import shutil
import subprocess  # noqa: S404
import sys
import tempfile
from pathlib import Path

import typer
```

Remove: `import duckdb` and `from moneybin.config import get_database_path`.

- [ ] **Step 6: Run linting and tests**

Run: `uv run ruff format src/moneybin/cli/commands/db.py && uv run ruff check src/moneybin/cli/commands/db.py && uv run pytest tests/moneybin/test_cli/test_db_commands.py -v`
Expected: Clean and tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/moneybin/cli/commands/db.py
git commit -m "feat: rewrite db shell/ui/query with encrypted -init temp script"
```

---

## Task 13: db info Command

**Files:**
- Modify: `src/moneybin/cli/commands/db.py`

- [ ] **Step 1: Add db info command**

```python
@app.command("info")
def db_info(
    database: Path | None = typer.Option(
        None, "--database", "-d",
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
```

- [ ] **Step 2: Commit**

```bash
git add src/moneybin/cli/commands/db.py
git commit -m "feat: add db info command for database metadata display"
```

---

## Task 14: db backup and restore Commands

**Files:**
- Modify: `src/moneybin/cli/commands/db.py`

- [ ] **Step 1: Add db backup command**

```python
@app.command("backup")
def db_backup(
    output: Path | None = typer.Option(
        None, "--output", "-o",
        help="Output path for backup (default: data/<profile>/backups/)",
    ),
) -> None:
    """Create a timestamped backup of the encrypted database file.

    Backups are encrypted with the same key — safe to store anywhere.
    """
    import shutil
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
```

- [ ] **Step 2: Add db restore command**

```python
@app.command("restore")
def db_restore(
    from_path: Path | None = typer.Option(
        None, "--from",
        help="Path to backup file to restore from",
    ),
    yes: bool = typer.Option(
        False, "--yes", "-y",
        help="Skip confirmation prompt",
    ),
) -> None:
    """Restore database from a backup file.

    Auto-backs-up the current database before restoring. If the backup
    was made before a key rotation, you'll be prompted for the original key.
    """
    import shutil
    from datetime import datetime

    from moneybin.config import get_settings
    from moneybin.database import Database
    from moneybin.secrets import SecretStore

    settings = get_settings()
    db_path = settings.database.path

    if from_path is None:
        # List available backups
        backup_dir = settings.database.backup_path or db_path.parent / "backups"
        if not backup_dir.exists():
            logger.error("❌ No backup directory found: %s", backup_dir)
            raise typer.Exit(1)

        backups = sorted(backup_dir.glob("*.duckdb"), reverse=True)
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

    if not from_path.exists():
        logger.error(f"❌ Backup file not found: {from_path}")
        raise typer.Exit(1)

    if not yes:
        confirm = typer.confirm(
            f"Restore from {from_path.name}? Current database will be backed up first."
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

    # Restore
    shutil.copy2(str(from_path), str(db_path))
    if sys.platform != "win32":
        try:
            db_path.chmod(0o600)
        except OSError:
            pass

    # Verify the restored database opens with current key
    store = SecretStore()
    try:
        db = Database(db_path, secret_store=store)
        db.close()
        logger.info("✅ Database restored from %s", from_path.name)
    except Exception:
        logger.warning(
            "⚠️  Could not open restored database with current key. "
            "The backup may be from before a key rotation."
        )
        logger.info(
            "💡 Set the original key via MONEYBIN_DATABASE__ENCRYPTION_KEY "
            "and run 'moneybin db rotate-key' to re-encrypt."
        )
```

- [ ] **Step 3: Commit**

```bash
git add src/moneybin/cli/commands/db.py
git commit -m "feat: add db backup and db restore commands"
```

---

## Task 15: db lock, unlock, key Commands

**Files:**
- Modify: `src/moneybin/cli/commands/db.py`

- [ ] **Step 1: Add db lock command**

```python
@app.command("lock")
def db_lock() -> None:
    """Clear the cached encryption key from OS keychain.

    Passphrase mode only. Subsequent commands will fail until
    'moneybin db unlock' is run.
    """
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
```

- [ ] **Step 2: Add db unlock command**

```python
@app.command("unlock")
def db_unlock() -> None:
    """Derive key from passphrase and cache in OS keychain.

    Passphrase mode only. Validates the derived key by attempting
    to attach the database — wrong passphrase errors immediately.
    """
    import argon2

    from moneybin.config import get_settings
    from moneybin.database import Database
    from moneybin.secrets import SecretStore

    pp = typer.prompt("Enter passphrase", hide_input=True)

    # Derive key using Argon2id (same params as db init)
    hasher = argon2.PasswordHasher(
        time_cost=3, memory_cost=65536, parallelism=4, hash_len=32, type=argon2.Type.ID
    )
    key_hash = hasher.hash(pp)
    encryption_key = key_hash.split("$")[-1]

    # Validate by trying to open the database
    store = SecretStore()
    store.set_key("DATABASE__ENCRYPTION_KEY", encryption_key)

    settings = get_settings()
    try:
        db = Database(settings.database.path, secret_store=store)
        db.close()
        logger.info("✅ Database unlocked")
    except Exception:
        # Wrong passphrase — clear the bad key
        store.delete_key("DATABASE__ENCRYPTION_KEY")
        logger.error("❌ Wrong passphrase — database remains locked")
        raise typer.Exit(1)
```

- [ ] **Step 3: Add db key command**

```python
@app.command("key")
def db_key() -> None:
    """Print the database encryption key.

    Auto-key mode: prints from keychain. Passphrase mode: prints cached
    key if unlocked, prompts for passphrase if locked (does NOT cache).
    """
    from moneybin.secrets import SecretNotFoundError, SecretStore

    store = SecretStore()
    try:
        key = store.get_key("DATABASE__ENCRYPTION_KEY")
    except SecretNotFoundError:
        logger.error(
            "❌ No encryption key found. Database may be locked. "
            "Run 'moneybin db unlock' first."
        )
        raise typer.Exit(1)

    logger.warning(
        "⚠️  Security warning: this key provides full access to your "
        "database. Do not share it or store it in plain text."
    )
    typer.echo(key)
```

- [ ] **Step 4: Commit**

```bash
git add src/moneybin/cli/commands/db.py
git commit -m "feat: add db lock, unlock, and key commands"
```

---

## Task 16: db rotate-key Command

**Files:**
- Modify: `src/moneybin/cli/commands/db.py`

- [ ] **Step 1: Add db rotate-key command**

```python
@app.command("rotate-key")
def db_rotate_key(
    yes: bool = typer.Option(
        False, "--yes", "-y",
        help="Skip confirmation prompt",
    ),
) -> None:
    """Re-encrypt the database with a new key.

    Generates a new encryption key, copies data via COPY FROM DATABASE,
    swaps files, and updates the keychain. Existing backups remain
    encrypted with the old key.
    """
    import secrets as secrets_mod
    import shutil

    from moneybin.config import get_settings
    from moneybin.secrets import SecretStore

    settings = get_settings()
    db_path = settings.database.path

    if not db_path.exists():
        logger.error(f"❌ Database file not found: {db_path}")
        raise typer.Exit(1)

    if not yes:
        logger.warning(
            "⚠️  Existing backups will remain encrypted with the old key."
        )
        confirm = typer.confirm("Proceed with key rotation?")
        if not confirm:
            raise typer.Exit(0)

    store = SecretStore()
    old_key = store.get_key("DATABASE__ENCRYPTION_KEY")
    new_key = secrets_mod.token_hex(32)

    # Create new database with new key, copy data
    import duckdb as duckdb_mod

    rotated_path = db_path.with_suffix(".rotated.duckdb")
    conn = duckdb_mod.connect()
    try:
        conn.execute("LOAD httpfs;")
        conn.execute(
            "ATTACH ? AS old_db (TYPE DUCKDB, ENCRYPTION_KEY ?)",
            [str(db_path), old_key],
        )
        conn.execute(
            "ATTACH ? AS new_db (TYPE DUCKDB, ENCRYPTION_KEY ?)",
            [str(rotated_path), new_key],
        )
        conn.execute("COPY FROM DATABASE old_db TO new_db")
    except Exception as e:
        logger.error(f"❌ Key rotation failed: {e}")
        rotated_path.unlink(missing_ok=True)
        raise typer.Exit(1) from e
    finally:
        conn.close()

    # Swap files
    old_backup = db_path.with_suffix(".old.duckdb")
    shutil.move(str(db_path), str(old_backup))
    shutil.move(str(rotated_path), str(db_path))

    if sys.platform != "win32":
        try:
            db_path.chmod(0o600)
        except OSError:
            pass

    # Update keychain
    store.set_key("DATABASE__ENCRYPTION_KEY", new_key)

    # Clean up old file
    old_backup.unlink(missing_ok=True)

    logger.info("✅ Database re-encrypted with new key")
    logger.info(
        "💡 Existing backups are still encrypted with the old key"
    )
```

- [ ] **Step 2: Run linting**

Run: `uv run ruff format src/moneybin/cli/commands/db.py && uv run ruff check src/moneybin/cli/commands/db.py`
Expected: Clean.

- [ ] **Step 3: Commit**

```bash
git add src/moneybin/cli/commands/db.py
git commit -m "feat: add db rotate-key command for encryption key rotation"
```

---

## Task 17: Update Test Fixtures and Helpers

**Files:**
- Modify: `tests/moneybin/conftest.py`
- Modify: `tests/moneybin/db_helpers.py`

- [ ] **Step 1: Add Database fixture to conftest.py**

Add a shared fixture for tests that need a `Database` instance:

```python
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database


@pytest.fixture()
def mock_secret_store() -> MagicMock:
    """Mock SecretStore that returns a test encryption key."""
    store = MagicMock()
    store.get_key.return_value = "test-encryption-key-for-unit-tests"
    return store


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    """Provide a test Database instance with encryption."""
    db_path = tmp_path / "test.duckdb"
    database = Database(db_path, secret_store=mock_secret_store)
    yield database  # type: ignore[misc]
    database.close()
```

- [ ] **Step 2: Update db_helpers.py**

Change `create_core_tables` to accept `Database`:

```python
from moneybin.database import Database

def create_core_tables(db: Database) -> None:
    """Create core tables for testing."""
    db.execute(CORE_DIM_ACCOUNTS_DDL)
    db.execute(CORE_FCT_TRANSACTIONS_DDL)
```

Keep the raw connection version as a fallback for any tests that still use it:

```python
def create_core_tables_raw(conn: duckdb.DuckDBPyConnection) -> None:
    """Create core tables for testing (raw connection version)."""
    conn.execute(CORE_DIM_ACCOUNTS_DDL)
    conn.execute(CORE_FCT_TRANSACTIONS_DDL)
```

- [ ] **Step 3: Run all tests**

Run: `uv run pytest tests/ -v`
Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add tests/moneybin/conftest.py tests/moneybin/db_helpers.py
git commit -m "feat: add Database test fixtures and update db_helpers"
```

---

## Task 18: Delete secrets_manager.py and Clean Up

**Files:**
- Delete: `src/moneybin/utils/secrets_manager.py`
- Delete: `tests/moneybin/test_utils/test_secrets_manager.py`

- [ ] **Step 1: Verify no imports remain**

Search the codebase for any remaining references to `secrets_manager`:

Run: `uv run ruff check . 2>&1 | grep secrets_manager` and `grep -r "secrets_manager" src/ tests/ --include="*.py"`

If any files still import from `secrets_manager`, update them to use `SecretStore` from `moneybin.secrets`.

- [ ] **Step 2: Delete the files**

```bash
git rm src/moneybin/utils/secrets_manager.py
git rm tests/moneybin/test_utils/test_secrets_manager.py
```

- [ ] **Step 3: Verify no duckdb.connect() calls remain in production code**

Search for direct `duckdb.connect()` calls:

Run: `grep -rn "duckdb.connect" src/moneybin/ --include="*.py"`

Expected: Zero results in production code (only allowed in `db rotate-key` command which needs two separate connections for the copy operation, and in tests).

**Exception:** `_run_transforms()` in `import_service.py` passes `db.path` to SQLMesh, which manages its own connection. This is acceptable — SQLMesh is an external system.

- [ ] **Step 4: Verify no direct keyring imports outside SecretStore**

Run: `grep -rn "import keyring" src/moneybin/ --include="*.py"`

Expected: Only `src/moneybin/secrets.py` imports `keyring`.

- [ ] **Step 5: Run the full test suite**

Run: `uv run pytest tests/ -v`
Expected: All tests pass.

- [ ] **Step 6: Run the full pre-commit checklist**

Run: `uv run ruff format . && uv run ruff check . && uv run pyright && uv run pytest tests/`
Expected: All clean.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "chore: delete secrets_manager.py, replaced by SecretStore"
```

---

## Task 19: Update Spec Status

**Files:**
- Modify: `docs/specs/privacy-data-protection.md`
- Modify: `docs/specs/INDEX.md`

- [ ] **Step 1: Update spec status to in-progress**

In `docs/specs/privacy-data-protection.md`, change line 4 from `ready` to `in-progress`.

In `docs/specs/INDEX.md`, update the Data Protection row status to `in-progress`.

- [ ] **Step 2: Commit**

```bash
git add docs/specs/privacy-data-protection.md docs/specs/INDEX.md
git commit -m "docs: mark data-protection spec as in-progress"
```

---

## Verification Checklist

After all tasks are complete, verify these success criteria from the spec:

- [ ] `moneybin db init` creates an encrypted database with zero extra flags
- [ ] A copied `.duckdb` file is unreadable without the encryption key
- [ ] All `duckdb.connect()` calls replaced with `Database` / `get_database()`
- [ ] All `keyring` imports go through `SecretStore`
- [ ] `secrets_manager.py` is deleted
- [ ] All services accept `db: Database` as their first parameter
- [ ] `db shell`, `db ui`, `db query` open encrypted databases via `-init` script
- [ ] `SanitizedLogFormatter` catches and masks PII patterns
- [ ] `db backup` / `db restore` round-trips successfully
- [ ] `db rotate-key` re-encrypts and updates keychain
- [ ] `db info` reports encryption status and database health
- [ ] Database files created with `0600`, directories with `0700`
- [ ] Log files created with `0600`
