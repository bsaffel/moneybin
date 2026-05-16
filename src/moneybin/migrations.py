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
    from datetime import datetime

    from moneybin.database import Database

logger = logging.getLogger(__name__)

_MIGRATIONS_DIR = Path(__file__).resolve().parent / "sql" / "migrations"

# V<3+ digits>__<snake_case>.<sql|py>
_MIGRATION_PATTERN = re.compile(r"^V(\d{3,})__(\w+)\.(sql|py)$")

# Truncated SHA-256 length used for content_hash. 16 hex chars (64 bits) per
# .claude/rules/identifiers.md "Content Hashes" — readable in logs, ample
# collision margin across the small migration ladder.
_CONTENT_HASH_LEN = 16


def short_hash(content: bytes) -> str:
    """Return the 16-hex-char truncated SHA-256 used for content_hash."""
    return hashlib.sha256(content).hexdigest()[:_CONTENT_HASH_LEN]


@dataclass(frozen=True)
class Migration:
    """A single migration file with parsed metadata.

    Attributes:
        version: Monotonic integer parsed from filename prefix.
        name: Snake-case description parsed from filename.
        filename: Full filename including extension.
        checksum: Lowercase hex SHA-256 of file contents.
        content: Raw file bytes (cached from discovery to avoid re-reads).
        path: Absolute path to the migration file.
        file_type: File extension — "sql" or "py".
    """

    version: int
    name: str
    filename: str
    checksum: str
    content: bytes
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
            content=content,
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
        failed_migration: Filename of the failed migration, if any.
        error_message: Human-readable details for display to the user.
    """

    applied_count: int = 0
    failed_migration: str | None = None
    error_message: str | None = None

    @property
    def failed(self) -> bool:
        """Whether any migration failed."""
        return self.failed_migration is not None or self.error_message is not None

    def log_failure(self) -> None:
        """Log the failure details with standard icon formatting."""
        msg = self.error_message or f"Migration {self.failed_migration} failed"
        logger.error(f"❌ {msg}")
        logger.info("💡 See logs for details")
        logger.error("🐛 Report issues at https://github.com/bsaffel/moneybin/issues")


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


@dataclass(frozen=True)
class AppliedMigration:
    """A migration record from the tracking table.

    Attributes:
        version: Migration version number.
        filename: Migration filename.
        success: Whether the migration succeeded.
        execution_ms: Execution time in milliseconds (may be None).
        applied_at: Timestamp when the migration was applied.
    """

    version: int
    filename: str
    success: bool
    execution_ms: int | None
    applied_at: datetime


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
        self._cached_migrations: list[Migration] | None = None
        self._tracking_schema_bootstrapped = False

    @property
    def _migrations(self) -> list[Migration]:
        """Lazily discover and cache migration files."""
        if self._cached_migrations is None:
            self._cached_migrations = discover_migrations(self._migrations_dir)
        return self._cached_migrations

    def _ensure_tracking_schema(self) -> None:
        """Add app.schema_migrations.content_hash on pre-V013 DBs.

        The runner's self-heal logic queries content_hash before V013 has had
        a chance to add it on databases created before this column existed
        — including from inside the very ``pending()`` call that decides
        whether V013 should run. Bootstrap the column here so the query
        never references a missing identifier. Idempotent.
        """
        if self._tracking_schema_bootstrapped:
            return
        row = self._db.execute(
            "SELECT 1 FROM duckdb_columns() "
            "WHERE schema_name = 'app' "
            "AND table_name = 'schema_migrations' "
            "AND column_name = 'content_hash'"
        ).fetchone()
        if row is None:
            self._db.execute(
                "ALTER TABLE app.schema_migrations ADD COLUMN content_hash VARCHAR"
            )
        self._tracking_schema_bootstrapped = True

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

        current_files: dict[int, Migration] = {m.version: m for m in self._migrations}

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

    def _stuck_blocker(
        self,
        *,
        version: int,
        filename: str,
        stored_hash: str | None,
        migration: Migration | None,
    ) -> MigrationError | None:
        """Classify a stuck (success=false) row.

        Returns ``None`` when the row is self-heal-eligible (a migration body
        whose hash no longer matches the recorded failure). Returns a ready-to-
        raise ``MigrationError`` for the unrecoverable cases: matching hash,
        NULL hash (pre-V013), and missing file.
        """
        if migration is None:
            return MigrationError(
                f"Stuck migration: {filename} (version {version}) failed "
                f"previously and the migration file no longer exists on disk. "
                f"Delete the row from app.schema_migrations or restore the file."
            )
        if stored_hash is None:
            return MigrationError(
                f"Stuck migration: {filename} (version {version}) failed "
                f"previously (existing row has no content_hash; pre-dates "
                f"self-heal). Manually clear the row if the code has been fixed."
            )
        if stored_hash == short_hash(migration.content):
            return MigrationError(
                f"Stuck migration: {filename} (version {version}) failed "
                f"previously with the same code (hash {stored_hash}). Fix the "
                f"issue and re-run; the runner will auto-clear the failure row "
                f"once the migration body changes."
            )
        return None

    def check_stuck(self) -> None:
        """Raise for stuck rows the runner can't self-heal.

        Self-heal-eligible rows are skipped — ``apply_one`` clears them on
        retry.
        """
        self._ensure_tracking_schema()
        rows = self._db.execute(
            "SELECT version, filename, content_hash FROM app.schema_migrations "
            "WHERE success = FALSE ORDER BY version"
        ).fetchall()
        if not rows:
            return

        files_by_version = {m.version: m for m in self._migrations}
        for version, filename, stored_hash in rows:
            blocker = self._stuck_blocker(
                version=version,
                filename=filename,
                stored_hash=stored_hash,
                migration=files_by_version.get(version),
            )
            if blocker is not None:
                raise blocker

    def applied_versions(self) -> dict[int, str]:
        """Return applied migration versions and their checksums.

        Returns:
            Dict mapping version number to checksum string.
        """
        rows = self._db.execute(
            "SELECT version, checksum FROM app.schema_migrations"
        ).fetchall()
        return {row[0]: row[1] for row in rows}

    def applied_details(self) -> list[AppliedMigration]:
        """Return full details of all applied migrations, ordered by version.

        Returns:
            List of AppliedMigration records.
        """
        rows = self._db.execute(
            "SELECT version, filename, success, execution_ms, applied_at "
            "FROM app.schema_migrations ORDER BY version"
        ).fetchall()
        return [
            AppliedMigration(
                version=row[0],
                filename=row[1],
                success=row[2],
                execution_ms=row[3],
                applied_at=row[4],
            )
            for row in rows
        ]

    def pending(self) -> list[Migration]:
        """Return migrations needing application, sorted by version.

        Includes never-applied migrations and self-heal-eligible stuck
        migrations (failure row present with a content_hash that no longer
        matches the file body). Excludes successful runs and unrecoverable
        stuck rows — check_stuck() raises for those before apply_all gets
        here.
        """
        self._ensure_tracking_schema()
        rows = self._db.execute(
            "SELECT version, success, content_hash FROM app.schema_migrations"
        ).fetchall()
        state_by_version: dict[int, tuple[bool, str | None]] = {
            row[0]: (row[1], row[2]) for row in rows
        }

        result: list[Migration] = []
        for migration in self._migrations:
            state = state_by_version.get(migration.version)
            if state is None:
                result.append(migration)
                continue
            success, stored_hash = state
            if success:
                continue
            # Stuck row — include only when self-heal can clear it. Conservative
            # for standalone callers; apply_one re-validates before clearing.
            if stored_hash is not None and stored_hash != short_hash(migration.content):
                result.append(migration)
        return result

    def _record_migration(
        self,
        migration: Migration,
        *,
        success: bool,
        elapsed_ms: int,
    ) -> None:
        """Write a row to the migration tracking table.

        Args:
            migration: The migration being recorded.
            success: Whether the migration succeeded.
            elapsed_ms: Execution time in milliseconds.
        """
        self._db.execute(
            "INSERT INTO app.schema_migrations "
            "(version, filename, checksum, success, execution_ms, content_hash) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                migration.version,
                migration.filename,
                migration.checksum,
                success,
                elapsed_ms,
                short_hash(migration.content),
            ],
        )

    def apply_one(self, migration: Migration) -> None:
        """Apply a single migration within a transaction.

        Idempotent on success: a recorded success row makes this a no-op.
        Self-healing on failure: if a previous attempt left a success=false
        row but the migration body has changed since that failure, the
        stale row is cleared and the migration is retried once. A matching
        hash (or a legacy NULL hash) preserves the guard and raises
        MigrationError so a human can decide.

        Args:
            migration: The migration to apply.

        Raises:
            MigrationError: If a stuck row blocks the retry, or if the
                migration itself fails.
        """
        self._ensure_tracking_schema()
        existing = self._db.execute(
            "SELECT success, content_hash FROM app.schema_migrations WHERE version = ?",
            [migration.version],
        ).fetchone()
        if existing is not None:
            success, stored_hash = existing
            if success:
                logger.debug(
                    f"Migration V{migration.version:03d} already applied, skipping"
                )
                return
            blocker = self._stuck_blocker(
                version=migration.version,
                filename=migration.filename,
                stored_hash=stored_hash,
                migration=migration,
            )
            if blocker is not None:
                raise blocker
            # Hash mismatch — maintainer has shipped a fix since the failure.
            # Clear the stale row outside the migration transaction so the
            # retry sees a clean slate. A failed retry records a fresh
            # success=false row with the new hash; the next attempt against
            # newer code will self-heal again.
            logger.info(
                f"Migration V{migration.version:03d} previously failed but body "
                f"has changed (old hash {stored_hash} → new "
                f"{short_hash(migration.content)}); clearing failure record "
                f"and retrying."
            )
            self._db.execute(
                "DELETE FROM app.schema_migrations WHERE version = ?",
                [migration.version],
            )

        logger.debug(f"Applying migration {migration.filename}")
        start = time.monotonic()

        try:
            self._db.execute("BEGIN TRANSACTION")

            if migration.file_type == "sql":
                self._db.execute(migration.content.decode())
            else:
                self._execute_python_migration(migration)

            elapsed_ms = int((time.monotonic() - start) * 1000)

            # Record success inside the transaction so DDL and tracking
            # are committed atomically — prevents orphaned DDL on crash.
            self._record_migration(migration, success=True, elapsed_ms=elapsed_ms)
            self._db.execute("COMMIT")
            logger.debug(f"Applied {migration.filename} in {elapsed_ms}ms")

        except Exception as exc:  # noqa: BLE001 — must catch all to record failure and re-raise as MigrationError
            elapsed_ms = int((time.monotonic() - start) * 1000)
            try:
                self._db.execute("ROLLBACK")
            except Exception:  # noqa: BLE001 S110 — rollback is best-effort; original exc re-raised below
                pass

            try:
                self._record_migration(migration, success=False, elapsed_ms=elapsed_ms)
            except Exception:  # noqa: BLE001 S110 — failure tracking is best-effort; original exc re-raised below
                logger.warning("Failed to record migration failure in tracking table")
            raise MigrationError(
                f"Migration {migration.filename} failed: {exc}"
            ) from exc

    def apply_all(self) -> MigrationResult:
        """Apply all pending migrations in version order.

        Returns:
            MigrationResult with counts and failure info.
        """
        try:
            self.check_stuck()
        except MigrationError as exc:
            return MigrationResult(error_message=str(exc))

        pending = self.pending()
        if not pending:
            logger.debug("No pending migrations")
            return MigrationResult()

        result = MigrationResult()
        for migration in pending:
            try:
                self.apply_one(migration)
                result.applied_count += 1
            except MigrationError as exc:
                result.failed_migration = migration.filename
                result.error_message = str(exc)
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
        logger.debug(f"Recorded {component} version {version} (first install)")
    elif existing[0] != version:
        db.execute(
            "UPDATE app.versions SET previous_version = version, "
            "version = ?, updated_at = CURRENT_TIMESTAMP WHERE component = ?",
            [version, component],
        )
        logger.debug(f"Updated {component} version {existing[0]} -> {version}")
