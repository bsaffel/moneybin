"""Database migration system — discovery, tracking, and execution.

Migrations are versioned SQL or Python files that apply schema changes to
MoneyBin's DuckDB database. The MigrationRunner receives an open database
connection from Database.__init__() and is encryption-unaware.

Migration files live in src/moneybin/sql/migrations/ and follow Flyway
naming: V<NNN>__<snake_case>.{sql,py} (3+ digit version, double underscore).
"""

from __future__ import annotations

import hashlib
import importlib.util
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from moneybin.database import Database

logger = logging.getLogger(__name__)

_MIGRATIONS_DIR = Path(__file__).resolve().parent / "sql" / "migrations"

# V<3+ digits>__<snake_case>.<sql|py>
_MIGRATION_PATTERN = re.compile(r"^V(\d{3,})__(\w+)\.(sql|py)$")


@dataclass(frozen=True)
class Migration:
    """A single migration file with parsed metadata.

    Attributes:
        version: Monotonic integer parsed from filename prefix.
        name: Snake-case description parsed from filename.
        filename: Full filename including extension.
        checksum: Lowercase hex SHA-256 of file contents.
        path: Absolute path to the migration file.
        file_type: File extension — "sql" or "py".
    """

    version: int
    name: str
    filename: str
    checksum: str
    path: Path
    file_type: str

    @classmethod
    def from_file(cls, path: Path) -> Migration:
        """Parse a migration file path into a Migration instance.

        Args:
            path: Path to a migration file.

        Returns:
            Parsed Migration with computed checksum.

        Raises:
            ValueError: If the filename doesn't match the expected pattern.
        """
        match = _MIGRATION_PATTERN.match(path.name)
        if not match:
            raise ValueError(
                f"Migration filename '{path.name}' does not match "
                f"expected pattern V<NNN>__<snake_case>.{{sql,py}}"
            )
        version = int(match.group(1))
        name = match.group(2)
        file_type = match.group(3)
        content = path.read_bytes()
        checksum = hashlib.sha256(content).hexdigest()
        return cls(
            version=version,
            name=name,
            filename=path.name,
            checksum=checksum,
            path=path,
            file_type=file_type,
        )


def discover_migrations(migrations_dir: Path | None = None) -> list[Migration]:
    """Discover and parse all migration files in the migrations directory.

    Args:
        migrations_dir: Directory to scan. Defaults to the built-in
            sql/migrations/ directory.

    Returns:
        List of Migration objects sorted by version number.

    Raises:
        ValueError: If duplicate version numbers are found.
    """
    directory = migrations_dir or _MIGRATIONS_DIR
    if not directory.exists():
        return []

    migrations: list[Migration] = []
    for path in directory.iterdir():
        if not path.is_file():
            continue
        if not _MIGRATION_PATTERN.match(path.name):
            continue
        migrations.append(Migration.from_file(path))

    # Check for duplicate versions
    seen: dict[int, str] = {}
    for m in migrations:
        if m.version in seen:
            raise ValueError(
                f"Duplicate migration version {m.version}: "
                f"'{seen[m.version]}' and '{m.filename}'"
            )
        seen[m.version] = m.filename

    migrations.sort(key=lambda m: m.version)
    return migrations


class MigrationError(Exception):
    """Raised when a migration fails to apply."""


@dataclass
class MigrationResult:
    """Summary of a migration batch run.

    Attributes:
        applied_count: Number of migrations successfully applied.
        failed: Whether any migration failed.
        failed_migration: Filename of the failed migration, if any.
        error_message: Human-readable details for display to the user.
    """

    applied_count: int = 0
    failed: bool = False
    failed_migration: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class DriftWarning:
    """Warning about a migration file that has changed since it was applied.

    Attributes:
        version: Migration version number.
        filename: Migration filename.
        reason: Human-readable explanation of the drift.
    """

    version: int
    filename: str
    reason: str


class MigrationRunner:
    """Discovers and applies database migrations.

    Receives an open Database instance — encryption-unaware. Follows
    the service pattern: business logic only, no connection management.

    Args:
        db: Open Database instance.
        migrations_dir: Directory containing migration files. Defaults
            to the built-in sql/migrations/ directory.
    """

    def __init__(
        self,
        db: Database,
        *,
        migrations_dir: Path | None = None,
    ) -> None:
        """Initialize the runner."""
        self._db = db
        self._migrations_dir = migrations_dir or _MIGRATIONS_DIR

    def check_drift(self) -> list[DriftWarning]:
        """Check for checksum drift between applied migrations and current files.

        Returns:
            List of DriftWarning for files that have changed or gone missing.
        """
        applied_rows = self._db.execute(
            "SELECT version, filename, checksum FROM app.schema_migrations "
            "WHERE success = TRUE"
        ).fetchall()
        if not applied_rows:
            return []

        # Build lookup of current files
        current_files: dict[int, Migration] = {}
        for m in discover_migrations(self._migrations_dir):
            current_files[m.version] = m

        warnings: list[DriftWarning] = []
        for version, filename, stored_checksum in applied_rows:
            if version not in current_files:
                warnings.append(
                    DriftWarning(
                        version=version,
                        filename=filename,
                        reason=(
                            f"File missing — {filename} was applied but no longer"
                            " exists on disk"
                        ),
                    )
                )
            elif current_files[version].checksum != stored_checksum:
                warnings.append(
                    DriftWarning(
                        version=version,
                        filename=filename,
                        reason=(
                            f"Checksum mismatch — {filename} has been modified"
                            " since it was applied"
                        ),
                    )
                )

        return warnings

    def check_stuck(self) -> None:
        """Check for stuck migrations (success=false in tracking table).

        Raises:
            MigrationError: If any migration has success=false.
        """
        stuck = self._db.execute(
            "SELECT version, filename FROM app.schema_migrations "
            "WHERE success = FALSE ORDER BY version LIMIT 1"
        ).fetchone()
        if stuck is not None:
            raise MigrationError(
                f"Stuck migration: {stuck[1]} (version {stuck[0]}) failed previously. "
                f"Fix the issue and delete the row from app.schema_migrations to retry, "
                f"or apply a corrective migration with a higher version number."
            )

    def applied_versions(self) -> dict[int, str]:
        """Return applied migration versions and their checksums.

        Returns:
            Dict mapping version number to checksum string.
        """
        rows = self._db.execute(
            "SELECT version, checksum FROM app.schema_migrations"
        ).fetchall()
        return {row[0]: row[1] for row in rows}

    def pending(self) -> list[Migration]:
        """Return migrations that have not yet been applied, sorted by version.

        Returns:
            List of unapplied Migration objects in version order.
        """
        applied = self.applied_versions()
        all_migrations = discover_migrations(self._migrations_dir)
        return [m for m in all_migrations if m.version not in applied]

    def apply_one(self, migration: Migration) -> None:
        """Apply a single migration within a transaction.

        If the migration version is already recorded in the tracking table,
        this is a silent no-op (idempotent). On failure, the migration DDL
        is rolled back but a tracking row with success=false is recorded.

        Args:
            migration: The migration to apply.

        Raises:
            MigrationError: If the migration fails to execute.
        """
        # Idempotent: skip if already applied
        existing = self._db.execute(
            "SELECT version FROM app.schema_migrations WHERE version = ?",
            [migration.version],
        ).fetchone()
        if existing is not None:
            logger.debug(
                f"Migration V{migration.version:03d} already applied, skipping"
            )
            return

        logger.info(f"Applying migration {migration.filename}")
        start = time.monotonic()

        try:
            self._db.execute("BEGIN TRANSACTION")

            if migration.file_type == "sql":
                sql = migration.path.read_text()
                self._db.execute(sql)
            else:
                self._execute_python_migration(migration)

            elapsed_ms = int((time.monotonic() - start) * 1000)

            # Record success inside the transaction so DDL and tracking
            # are committed atomically — prevents orphaned DDL on crash.
            self._db.execute(
                "INSERT INTO app.schema_migrations "
                "(version, filename, checksum, success, execution_ms) "
                "VALUES (?, ?, ?, TRUE, ?)",
                [migration.version, migration.filename, migration.checksum, elapsed_ms],
            )
            self._db.execute("COMMIT")
            logger.info(f"Applied {migration.filename} in {elapsed_ms}ms")

        except Exception as exc:  # noqa: BLE001 — must catch all to record failure and re-raise as MigrationError
            elapsed_ms = int((time.monotonic() - start) * 1000)
            try:
                self._db.execute("ROLLBACK")
            except Exception:  # noqa: BLE001 S110 — rollback is best-effort; original exc re-raised below
                pass

            # Record failure — best-effort; original exception takes priority
            try:
                self._db.execute(
                    "INSERT INTO app.schema_migrations "
                    "(version, filename, checksum, success, execution_ms) "
                    "VALUES (?, ?, ?, FALSE, ?)",
                    [
                        migration.version,
                        migration.filename,
                        migration.checksum,
                        elapsed_ms,
                    ],
                )
            except Exception:  # noqa: BLE001 S110 — failure tracking is best-effort; original exc re-raised below
                logger.warning("Failed to record migration failure in tracking table")
            raise MigrationError(
                f"Migration {migration.filename} failed: {exc}"
            ) from exc

    def apply_all(self) -> MigrationResult:
        """Apply all pending migrations in version order.

        Checks for stuck migrations first. Stops on first failure.

        Returns:
            MigrationResult with counts and failure info.
        """
        # Check for stuck state before doing anything
        try:
            self.check_stuck()
        except MigrationError as exc:
            return MigrationResult(failed=True, error_message=str(exc))

        pending = self.pending()
        if not pending:
            logger.debug("No pending migrations")
            return MigrationResult()

        result = MigrationResult()
        for migration in pending:
            try:
                self.apply_one(migration)
                result.applied_count += 1
            except MigrationError:
                result.failed = True
                result.failed_migration = migration.filename
                break

        return result

    def _execute_python_migration(self, migration: Migration) -> None:
        """Import and execute a Python migration's migrate() function.

        Args:
            migration: A Python migration file.

        Raises:
            MigrationError: If the module lacks a migrate() function or cannot
                be loaded.
        """
        spec = importlib.util.spec_from_file_location(
            f"migration_v{migration.version}", migration.path
        )
        if spec is None or spec.loader is None:
            raise MigrationError(f"Cannot load Python migration {migration.filename}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore[union-attr]  # loader existence checked above

        migrate_fn = getattr(module, "migrate", None)
        if migrate_fn is None:
            raise MigrationError(
                f"Python migration {migration.filename} has no migrate() function"
            )
        migrate_fn(self._db.conn)


def get_current_versions(db: Database) -> dict[str, str]:
    """Read all component versions from app.versions.

    Args:
        db: Open Database instance.

    Returns:
        Dict mapping component name to version string.
    """
    rows = db.execute("SELECT component, version FROM app.versions").fetchall()
    return {row[0]: row[1] for row in rows}


def record_version(db: Database, component: str, version: str) -> None:
    """Record or update a component version in app.versions.

    If the component already has the same version, this is a no-op.
    If the version has changed, previous_version is updated.

    Args:
        db: Open Database instance.
        component: Component identifier (e.g. 'moneybin', 'sqlmesh').
        version: Current version string.
    """
    existing = db.execute(
        "SELECT version FROM app.versions WHERE component = ?", [component]
    ).fetchone()

    if existing is None:
        db.execute(
            "INSERT INTO app.versions (component, version) VALUES (?, ?)",
            [component, version],
        )
        logger.info(f"Recorded {component} version {version} (first install)")
    elif existing[0] != version:
        db.execute(
            "UPDATE app.versions SET previous_version = version, "
            "version = ?, updated_at = CURRENT_TIMESTAMP WHERE component = ?",
            [version, component],
        )
        logger.info(f"Updated {component} version {existing[0]} -> {version}")
