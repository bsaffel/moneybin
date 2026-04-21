"""Database migration system — discovery, tracking, and execution.

Migrations are versioned SQL or Python files that apply schema changes to
MoneyBin's DuckDB database. The MigrationRunner receives an open database
connection from Database.__init__() and is encryption-unaware.

Migration files live in src/moneybin/sql/migrations/ and follow Flyway
naming: V<NNN>__<snake_case>.{sql,py} (3+ digit version, double underscore).
"""

import hashlib
import logging
import re
from dataclasses import dataclass
from pathlib import Path

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
    def from_file(cls, path: Path) -> "Migration":
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
    for path in sorted(directory.iterdir()):
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
