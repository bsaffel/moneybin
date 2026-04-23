"""TabularFormat model and format loading/persistence.

Formats describe how to read a specific institution's tabular export:
column mapping, date format, sign convention, delimiter, etc. Built-in
formats ship as YAML files; user formats are stored in the database.
"""

from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import yaml
from pydantic import BaseModel

if TYPE_CHECKING:
    from moneybin.database import Database

logger = logging.getLogger(__name__)

_BUILTIN_FORMATS_DIR = (
    Path(__file__).resolve().parent.parent.parent / "data" / "tabular_formats"
)

SignConventionType = Literal[
    "negative_is_expense", "negative_is_income", "split_debit_credit"
]
NumberFormatType = Literal["us", "european", "swiss_french", "zero_decimal"]


class TabularFormat(BaseModel, frozen=True):
    """Column mapping for a specific institution's tabular export format.

    Immutable (frozen) for safety — create a new instance to modify.
    """

    name: str
    """Machine identifier, e.g. ``chase_credit``."""

    institution_name: str
    """Human-readable institution name, e.g. ``Chase``."""

    file_type: str = "auto"
    """Expected file type: csv, tsv, xlsx, parquet, feather, pipe, or auto."""

    delimiter: str | None = None
    """Explicit delimiter for text formats; None = auto-detect."""

    encoding: str = "utf-8"
    """Character encoding for text formats."""

    skip_rows: int = 0
    """Non-data rows to skip before the header row."""

    sheet: str | None = None
    """Excel sheet name; None = auto-select largest."""

    header_signature: list[str]
    """Column names that fingerprint this format (case-insensitive subset)."""

    field_mapping: dict[str, str]
    """Destination field → source column name mapping."""

    sign_convention: SignConventionType
    """How amounts are represented in the source."""

    date_format: str
    """strftime format string for date parsing."""

    number_format: NumberFormatType = "us"
    """Number convention: us, european, swiss_french, zero_decimal."""

    skip_trailing_patterns: list[str] | None = None
    """Regex patterns for trailing junk rows. None = use defaults."""

    multi_account: bool = False
    """Whether this format has per-row account identification."""

    source: str = "detected"
    """How created: detected, manual, built-in-override."""

    times_used: int = 0
    """Successful import count."""

    last_used_at: str | None = None
    """Timestamp of last successful import."""

    def matches_headers(self, file_headers: list[str]) -> bool:
        """Check if a file's headers match this format's signature.

        Case-insensitive subset match: every header in the signature must
        appear in the file's headers.

        Args:
            file_headers: Column headers from the source file.

        Returns:
            True if signature is a subset of file_headers.
        """
        normalized_file = {h.strip().lower() for h in file_headers}
        return all(
            sig.strip().lower() in normalized_file for sig in self.header_signature
        )

    def to_yaml(self, path: Path) -> None:
        """Serialize this format to a YAML file.

        Args:
            path: File path to write.
        """
        data = self.model_dump(
            exclude={"times_used", "last_used_at", "source"},
            exclude_none=True,
        )
        # Convert file_type 'auto' default to omit from YAML
        if data.get("file_type") == "auto":
            data.pop("file_type", None)
        with open(path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)

    @classmethod
    def from_yaml(cls, path: Path) -> TabularFormat:
        """Load a format from a YAML file.

        Args:
            path: Path to YAML file.

        Returns:
            TabularFormat instance.
        """
        with open(path) as f:
            data = yaml.safe_load(f)
        # YAML files use 'format' key for file_type (legacy compat)
        if "format" in data and "file_type" not in data:
            data["file_type"] = data.pop("format")
        return cls(**data)


@lru_cache(maxsize=1)
def load_builtin_formats() -> dict[str, TabularFormat]:
    """Load all built-in format YAML files.

    Returns:
        Dict mapping format name to TabularFormat instance.
    """
    formats: dict[str, TabularFormat] = {}
    if not _BUILTIN_FORMATS_DIR.exists():
        logger.warning(f"Built-in formats directory not found: {_BUILTIN_FORMATS_DIR}")
        return formats

    for yaml_path in sorted(_BUILTIN_FORMATS_DIR.glob("*.yaml")):
        try:
            fmt = TabularFormat.from_yaml(yaml_path)
            formats[fmt.name] = fmt
            logger.debug(f"Loaded built-in format: {fmt.name}")
        except (yaml.YAMLError, OSError, ValueError):
            logger.warning(f"Failed to load format: {yaml_path}", exc_info=True)

    return formats


def save_format_to_db(db: Database, fmt: TabularFormat) -> None:
    """Persist a TabularFormat to the app.tabular_formats table.

    Uses INSERT OR REPLACE so re-saving a format with the same name
    updates all fields in place.

    Args:
        db: Active Database connection.
        fmt: Format to persist.
    """
    db.execute(
        """
        INSERT OR REPLACE INTO app.tabular_formats (
            name, institution_name, file_type, delimiter, encoding,
            skip_rows, sheet, header_signature, field_mapping,
            sign_convention, date_format, number_format,
            skip_trailing_patterns, multi_account, source,
            times_used, last_used_at, updated_at
        ) VALUES (
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?, CURRENT_TIMESTAMP
        )
        """,
        [
            fmt.name,
            fmt.institution_name,
            fmt.file_type,
            fmt.delimiter,
            fmt.encoding,
            fmt.skip_rows,
            fmt.sheet,
            json.dumps(fmt.header_signature),
            json.dumps(fmt.field_mapping),
            fmt.sign_convention,
            fmt.date_format,
            fmt.number_format,
            json.dumps(fmt.skip_trailing_patterns)
            if fmt.skip_trailing_patterns is not None
            else None,
            fmt.multi_account,
            fmt.source,
            fmt.times_used,
            fmt.last_used_at,
        ],
    )
    logger.debug(f"Saved format to DB: {fmt.name}")


def load_formats_from_db(db: Database) -> dict[str, TabularFormat]:
    """Load all user-saved formats from app.tabular_formats.

    Args:
        db: Active Database connection.

    Returns:
        Dict mapping format name to TabularFormat. Returns empty dict if
        the table does not exist yet or contains no rows.
    """
    try:
        rows = db.execute(
            """
            SELECT
                name, institution_name, file_type, delimiter, encoding,
                skip_rows, sheet, header_signature, field_mapping,
                sign_convention, date_format, number_format,
                skip_trailing_patterns, multi_account, source,
                times_used, last_used_at
            FROM app.tabular_formats
            ORDER BY name
            """
        ).fetchall()
    except Exception:  # noqa: BLE001  # table may not exist before first migration
        logger.debug("app.tabular_formats not available; returning empty format set")
        return {}

    formats: dict[str, TabularFormat] = {}
    for row in rows:
        (
            name,
            institution_name,
            file_type,
            delimiter,
            encoding,
            skip_rows,
            sheet,
            header_signature_raw,
            field_mapping_raw,
            sign_convention,
            date_format,
            number_format,
            skip_trailing_raw,
            multi_account,
            source,
            times_used,
            last_used_at,
        ) = row
        try:
            fmt = TabularFormat(
                name=name,
                institution_name=institution_name,
                file_type=file_type,
                delimiter=delimiter,
                encoding=encoding,
                skip_rows=skip_rows,
                sheet=sheet,
                header_signature=json.loads(header_signature_raw),
                field_mapping=json.loads(field_mapping_raw),
                sign_convention=sign_convention,
                date_format=date_format,
                number_format=number_format,
                skip_trailing_patterns=json.loads(skip_trailing_raw)
                if skip_trailing_raw is not None
                else None,
                multi_account=bool(multi_account),
                source=source,
                times_used=times_used or 0,
                last_used_at=str(last_used_at) if last_used_at is not None else None,
            )
            formats[fmt.name] = fmt
        except (ValueError, TypeError):
            logger.warning(f"Skipping malformed DB format row: {name!r}", exc_info=True)

    logger.debug(f"Loaded {len(formats)} format(s) from DB")
    return formats


def delete_format_from_db(db: Database, name: str) -> bool:
    """Delete a user-saved format by name.

    Args:
        db: Active Database connection.
        name: Format name to delete.

    Returns:
        True if the format was found and deleted, False if not found.
    """
    row = db.execute(
        "SELECT name FROM app.tabular_formats WHERE name = ?",
        [name],
    ).fetchone()
    if row is None:
        return False
    db.execute(
        "DELETE FROM app.tabular_formats WHERE name = ?",
        [name],
    )
    logger.debug(f"Deleted format from DB: {name!r}")
    return True


def merge_formats(
    builtins: dict[str, TabularFormat],
    user_formats: dict[str, TabularFormat],
) -> dict[str, TabularFormat]:
    """Merge built-in and user-defined formats, with user formats taking priority.

    Args:
        builtins: Formats loaded from built-in YAML files.
        user_formats: Formats loaded from the database.

    Returns:
        Combined dict where user_formats override builtins on name collision.
    """
    return {**builtins, **user_formats}
