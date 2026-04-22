"""CSV profile system for institution-specific column mappings.

Each institution exports CSVs in a different format (column names, date formats,
amount representations). A CSVProfile defines the mapping from a specific
institution's format to MoneyBin's canonical schema. Profiles are stored as YAML
files and auto-detected by matching CSV headers.

Two tiers:
  1. Built-in profiles ship in src/moneybin/data/csv_profiles/
  2. User profiles in data/{profile}/csv_profiles/ (take precedence)
"""

import logging
import shutil
from enum import Enum
from pathlib import Path

import yaml
from pydantic import BaseModel, model_validator

logger = logging.getLogger(__name__)

_BUILTIN_PROFILES_DIR = Path(__file__).resolve().parent.parent / "data" / "csv_profiles"


class SignConvention(Enum):
    """How the source CSV represents transaction amounts."""

    NEGATIVE_IS_EXPENSE = "negative_is_expense"
    """Single amount column: negative = expense, positive = income (MoneyBin native)."""

    NEGATIVE_IS_INCOME = "negative_is_income"
    """Single amount column: negative = income, positive = expense (inverted)."""

    SPLIT_DEBIT_CREDIT = "split_debit_credit"
    """Separate debit and credit columns."""


class CSVProfile(BaseModel):
    """Column mapping for a specific institution's CSV export format.

    Profiles are serialized as YAML and loaded at import time. The
    ``header_signature`` field enables auto-detection by matching against
    a CSV file's header row.
    """

    name: str
    """Machine identifier, e.g. ``chase_credit``."""

    institution_name: str
    """Human-readable institution name, e.g. ``Chase``."""

    header_signature: list[str]
    """Column names that uniquely fingerprint this format (case-insensitive)."""

    # -- Date columns --
    date_column: str
    """Primary transaction date column name."""

    date_format: str
    """strftime format string for parsing dates, e.g. ``%m/%d/%Y``."""

    post_date_column: str | None = None
    """Optional posting/settlement date column."""

    # -- Amount columns --
    amount_column: str | None = None
    """Single amount column (for non-split formats)."""

    debit_column: str | None = None
    """Debit column for split-amount formats."""

    credit_column: str | None = None
    """Credit column for split-amount formats."""

    sign_convention: SignConvention
    """How amounts are represented in the source CSV."""

    # -- Description / detail columns --
    description_column: str
    """Transaction description / payee column."""

    memo_column: str | None = None
    category_column: str | None = None
    subcategory_column: str | None = None
    type_column: str | None = None
    """Transaction type (Sale, Return, Payment, etc.)."""

    status_column: str | None = None
    """Transaction status (Cleared, Pending, etc.)."""

    check_number_column: str | None = None
    reference_column: str | None = None
    """Reference / confirmation number column."""

    balance_column: str | None = None
    """Running balance column."""

    member_name_column: str | None = None
    """Account holder / member name column."""

    # -- File format options --
    skip_rows: int = 0
    """Number of rows to skip before the header row."""

    encoding: str = "utf-8"
    """File encoding (e.g. utf-8, latin-1, windows-1252)."""

    @model_validator(mode="after")
    def _validate_amount_columns(self) -> "CSVProfile":
        """Ensure amount columns match the sign convention."""
        if self.sign_convention == SignConvention.SPLIT_DEBIT_CREDIT:
            if not self.debit_column or not self.credit_column:
                raise ValueError(
                    "SPLIT_DEBIT_CREDIT requires both debit_column and credit_column"
                )
        else:
            if not self.amount_column:
                raise ValueError(f"{self.sign_convention.value} requires amount_column")
        return self

    model_config = {"extra": "forbid"}


def save_profile(profile: CSVProfile, profiles_dir: Path) -> Path:
    """Write a CSVProfile to a YAML file.

    Args:
        profile: The profile to save.
        profiles_dir: Directory to write the YAML file to.

    Returns:
        Path to the written YAML file.
    """
    profiles_dir.mkdir(parents=True, exist_ok=True)
    output_path = profiles_dir / f"{profile.name}.yaml"

    data = profile.model_dump(mode="python", exclude_none=True)
    # Convert enum to string for clean YAML
    data["sign_convention"] = profile.sign_convention.value

    with open(output_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)

    logger.info(f"Saved CSV profile '{profile.name}' to {output_path}")
    return output_path


def _load_profile_from_yaml(yaml_path: Path) -> CSVProfile:
    """Load a single CSVProfile from a YAML file.

    Args:
        yaml_path: Path to the YAML file.

    Returns:
        Parsed CSVProfile.

    Raises:
        ValueError: If the YAML file is invalid.
    """
    with open(yaml_path) as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        raise ValueError(f"Invalid CSV profile YAML: {yaml_path}")

    profile_data: dict[str, object] = data
    return CSVProfile(**profile_data)  # type: ignore[arg-type]  # YAML dict keys match CSVProfile fields


def load_profiles(user_profiles_dir: Path) -> dict[str, CSVProfile]:
    """Load all CSV profiles, with user profiles overriding built-ins.

    Args:
        user_profiles_dir: User's profile directory
            (e.g. ``data/{profile}/csv_profiles/``).

    Returns:
        Dict mapping profile name to CSVProfile.
    """
    profiles: dict[str, CSVProfile] = {}

    # Load built-in profiles first
    if _BUILTIN_PROFILES_DIR.is_dir():
        for yaml_path in sorted(_BUILTIN_PROFILES_DIR.glob("*.yaml")):
            try:
                profile = _load_profile_from_yaml(yaml_path)
                profiles[profile.name] = profile
            except Exception:
                logger.warning(f"Skipping invalid built-in profile: {yaml_path}")

    # Load user profiles (override built-ins with same name)
    if user_profiles_dir.is_dir():
        for yaml_path in sorted(user_profiles_dir.glob("*.yaml")):
            try:
                profile = _load_profile_from_yaml(yaml_path)
                profiles[profile.name] = profile
            except Exception:
                logger.warning(f"Skipping invalid user profile: {yaml_path}")

    return profiles


def detect_profile(headers: list[str], user_profiles_dir: Path) -> CSVProfile | None:
    """Auto-detect a CSV profile by matching header columns.

    Normalizes both the file headers and profile signatures to lowercase
    and checks if the file headers are a superset of the signature.

    Args:
        headers: Column names from the CSV file's header row.
        user_profiles_dir: User's profile directory.

    Returns:
        The matching CSVProfile, or None if no match found.
    """
    normalized_headers = {h.strip().lower() for h in headers}
    profiles = load_profiles(user_profiles_dir)

    for profile in profiles.values():
        signature = {col.strip().lower() for col in profile.header_signature}
        if signature.issubset(normalized_headers):
            logger.info(
                "Auto-detected CSV profile '%s' (%s)",
                profile.name,
                profile.institution_name,
            )
            return profile

    return None


def ensure_default_profiles(user_profiles_dir: Path) -> None:
    """Copy built-in profiles to user directory if it is empty or missing.

    Args:
        user_profiles_dir: User's profile directory.
    """
    user_profiles_dir.mkdir(parents=True, exist_ok=True)

    existing = list(user_profiles_dir.glob("*.yaml"))
    if existing:
        return

    if not _BUILTIN_PROFILES_DIR.is_dir():
        return

    for yaml_path in _BUILTIN_PROFILES_DIR.glob("*.yaml"):
        dest = user_profiles_dir / yaml_path.name
        shutil.copy2(yaml_path, dest)
        logger.info(f"Copied built-in profile {yaml_path.name} to {dest}")


def list_profile_names(user_profiles_dir: Path) -> list[str]:
    """Return names of all available profiles.

    Args:
        user_profiles_dir: User's profile directory.

    Returns:
        Sorted list of profile names.
    """
    return sorted(load_profiles(user_profiles_dir).keys())
