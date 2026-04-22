"""TabularFormat model and format loading/persistence.

Formats describe how to read a specific institution's tabular export:
column mapping, date format, sign convention, delimiter, etc. Built-in
formats ship as YAML files; user formats are stored in the database.
"""

import logging
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, field_validator

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

    @field_validator("sign_convention", mode="before")
    @classmethod
    def _validate_sign_convention(cls, v: str) -> str:
        valid = {"negative_is_expense", "negative_is_income", "split_debit_credit"}
        if v not in valid:
            raise ValueError(f"sign_convention must be one of {valid}, got {v!r}")
        return v

    @field_validator("number_format", mode="before")
    @classmethod
    def _validate_number_format(cls, v: str) -> str:
        valid = {"us", "european", "swiss_french", "zero_decimal"}
        if v not in valid:
            raise ValueError(f"number_format must be one of {valid}, got {v!r}")
        return v

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
    def from_yaml(cls, path: Path) -> "TabularFormat":
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
        except Exception:
            logger.warning(f"Failed to load format: {yaml_path}", exc_info=True)

    return formats
