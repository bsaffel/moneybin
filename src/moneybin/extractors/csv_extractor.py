"""CSV file extractor for institution transaction exports.

Parses CSV files from various institutions using YAML-based column mapping
profiles. Each institution's format is described by a :class:`CSVProfile` that
maps source columns to MoneyBin's canonical schema.

The extractor normalises amounts to MoneyBin's sign convention
(negative = expense, positive = income), generates deterministic synthetic
transaction IDs, and outputs Polars DataFrames ready for the CSV loader.
"""

import csv
import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from moneybin.extractors.csv_profiles import (
    CSVProfile,
    SignConvention,
    detect_profile,
    load_profiles,
)

logger = logging.getLogger(__name__)


@dataclass
class CSVExtractionConfig:
    """Configuration for CSV file extraction."""

    raw_data_path: Path | None = None


class CSVExtractor:
    """Extract financial transactions from bank CSV files."""

    def __init__(self, config: CSVExtractionConfig | None = None) -> None:
        """Initialize the CSV extractor.

        Args:
            config: Extraction configuration settings.
        """
        from moneybin.config import get_raw_data_path

        self.config = config or CSVExtractionConfig()

        if self.config.raw_data_path is None:
            self.config.raw_data_path = get_raw_data_path() / "csv"

        self.config.raw_data_path.mkdir(parents=True, exist_ok=True)
        logger.info(
            "Initialized CSV extractor with output: %s", self.config.raw_data_path
        )

    def extract_from_file(
        self,
        file_path: Path,
        *,
        profile: CSVProfile | None = None,
        account_id: str | None = None,
        user_profiles_dir: Path | None = None,
    ) -> dict[str, pl.DataFrame]:
        """Extract transactions from a bank CSV file.

        Args:
            file_path: Path to the CSV file.
            profile: Explicit profile to use. If None, auto-detects from headers.
            account_id: Account identifier. Required — CSVs do not contain
                account IDs natively.
            user_profiles_dir: Directory containing user YAML profiles.
                Falls back to the profile-aware data path if None.

        Returns:
            Dictionary with ``accounts`` and ``transactions`` DataFrames.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the profile cannot be detected or account_id is missing.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        if not account_id:
            raise ValueError(
                "account_id is required for CSV imports "
                "(CSVs do not contain account identifiers). "
                "Use --account-id on the CLI."
            )

        logger.info(f"Extracting data from CSV file: {file_path}")

        # Resolve profile
        if profile is None:
            profile = self._resolve_profile(file_path, user_profiles_dir)

        # Read and transform CSV
        source_file = str(file_path)
        extraction_timestamp = datetime.now()

        df = self._read_csv(file_path, profile)
        transactions_df = self._transform_transactions(
            df, profile, account_id, source_file, extraction_timestamp
        )
        accounts_df = self._build_accounts_df(
            profile, account_id, source_file, extraction_timestamp
        )

        results = {
            "accounts": accounts_df,
            "transactions": transactions_df,
        }

        logger.info(
            "Extracted %d transaction(s) for account '%s' (%s)",
            len(transactions_df),
            account_id,
            profile.institution_name,
        )

        return results

    def _resolve_profile(
        self, file_path: Path, user_profiles_dir: Path | None
    ) -> CSVProfile:
        """Detect the CSV profile from file headers.

        Args:
            file_path: Path to the CSV file.
            user_profiles_dir: User profiles directory.

        Returns:
            Detected CSVProfile.

        Raises:
            ValueError: If no matching profile is found.
        """
        if user_profiles_dir is None:
            from moneybin.config import get_raw_data_path

            user_profiles_dir = get_raw_data_path().parent / "csv_profiles"

        headers = self._read_headers(file_path)
        profile = detect_profile(headers, user_profiles_dir)

        if profile is None:
            available = load_profiles(user_profiles_dir)
            names = ", ".join(sorted(available.keys())) or "(none)"
            raise ValueError(
                f"Could not auto-detect CSV format for: {file_path.name}\n"
                f"Headers found: {headers}\n"
                f"Available profiles: {names}\n"
                f"Use --institution to specify a profile, or create one via MCP."
            )

        return profile

    @staticmethod
    def _read_headers(file_path: Path) -> list[str]:
        """Read the header row from a CSV file.

        Args:
            file_path: Path to the CSV file.

        Returns:
            List of column names.
        """
        with open(file_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            headers = next(reader)
        return [h.strip() for h in headers]

    @staticmethod
    def _read_csv(file_path: Path, profile: CSVProfile) -> pl.DataFrame:
        """Read a CSV file into a Polars DataFrame.

        Args:
            file_path: Path to the CSV file.
            profile: CSV profile with encoding and skip_rows.

        Returns:
            Raw DataFrame from the CSV file.
        """
        return pl.read_csv(
            file_path,
            encoding=profile.encoding if profile.encoding != "utf-8" else "utf8",
            skip_rows=profile.skip_rows,
            infer_schema_length=0,  # Read everything as strings first
            truncate_ragged_lines=True,
        )

    def _transform_transactions(
        self,
        df: pl.DataFrame,
        profile: CSVProfile,
        account_id: str,
        source_file: str,
        extraction_timestamp: datetime,
    ) -> pl.DataFrame:
        """Transform raw CSV data into the canonical transaction schema.

        Args:
            df: Raw DataFrame from CSV.
            profile: Column mapping profile.
            account_id: Account identifier.
            source_file: Source file path string.
            extraction_timestamp: When extraction occurred.

        Returns:
            DataFrame with canonical transaction columns.
        """
        rows: list[dict[str, Any]] = []

        for row_index, row in enumerate(df.iter_rows(named=True)):
            # Parse date
            date_str = str(row.get(profile.date_column, "")).strip()
            if not date_str:
                logger.debug(f"Skipping row {row_index}: empty date")
                continue

            try:
                transaction_date = datetime.strptime(
                    date_str, profile.date_format
                ).date()
            except ValueError:
                logger.debug(
                    "Skipping row %d: unparseable date '%s'", row_index, date_str
                )
                continue

            # Parse post date
            post_date = None
            if profile.post_date_column:
                post_date_str = str(row.get(profile.post_date_column, "")).strip()
                if post_date_str:
                    try:
                        post_date = datetime.strptime(
                            post_date_str, profile.date_format
                        ).date()
                    except ValueError:
                        pass

            # Parse amount
            amount = self._parse_amount(row, profile)
            if amount is None:
                logger.debug(f"Skipping row {row_index}: could not parse amount")
                continue

            # Build description
            description = str(row.get(profile.description_column, "")).strip()

            # Generate deterministic transaction ID
            transaction_id = _generate_transaction_id(
                date=str(transaction_date),
                amount=f"{amount:.2f}",
                description=description,
                account_id=account_id,
            )

            tx: dict[str, Any] = {
                "transaction_id": transaction_id,
                "account_id": account_id,
                "transaction_date": str(transaction_date),
                "post_date": str(post_date) if post_date else None,
                "amount": amount,
                "description": description,
                "memo": _get_optional(row, profile.memo_column),
                "category": _get_optional(row, profile.category_column),
                "subcategory": _get_optional(row, profile.subcategory_column),
                "transaction_type": _get_optional(row, profile.type_column),
                "transaction_status": _get_optional(row, profile.status_column),
                "check_number": _get_optional(row, profile.check_number_column),
                "reference_number": _get_optional(row, profile.reference_column),
                "balance": _parse_decimal(_get_optional(row, profile.balance_column)),
                "member_name": _get_optional(row, profile.member_name_column),
                "source_file": source_file,
                "extracted_at": extraction_timestamp.isoformat(),
            }
            rows.append(tx)

        if rows:
            return pl.DataFrame(rows)

        return pl.DataFrame(
            schema={
                "transaction_id": pl.String,
                "account_id": pl.String,
                "transaction_date": pl.String,
                "post_date": pl.String,
                "amount": pl.Float64,
                "description": pl.String,
                "memo": pl.String,
                "category": pl.String,
                "subcategory": pl.String,
                "transaction_type": pl.String,
                "transaction_status": pl.String,
                "check_number": pl.String,
                "reference_number": pl.String,
                "balance": pl.Float64,
                "member_name": pl.String,
                "source_file": pl.String,
                "extracted_at": pl.String,
            }
        )

    @staticmethod
    def _parse_amount(row: dict[str, Any], profile: CSVProfile) -> float | None:
        """Parse the transaction amount from a CSV row.

        Normalises to MoneyBin convention: negative = expense, positive = income.

        Args:
            row: Named row from the DataFrame.
            profile: Column mapping profile.

        Returns:
            Normalised amount, or None if unparseable.
        """
        if profile.sign_convention == SignConvention.SPLIT_DEBIT_CREDIT:
            assert profile.debit_column is not None  # noqa: S101 — validated by CSVProfile
            assert profile.credit_column is not None  # noqa: S101 — validated by CSVProfile
            debit = _parse_decimal(str(row.get(profile.debit_column, "")).strip())
            credit = _parse_decimal(str(row.get(profile.credit_column, "")).strip())

            if debit is not None and debit != 0.0:
                # Debits are expenses → negative
                return -abs(debit)
            if credit is not None and credit != 0.0:
                # Credits are income → positive
                return abs(credit)
            if debit == 0.0 and (credit is None or credit == 0.0):
                # Zero amount (e.g. waived fee)
                return 0.0
            return None

        assert profile.amount_column is not None  # noqa: S101 — validated by CSVProfile
        raw_amount = str(row.get(profile.amount_column, "")).strip()
        amount = _parse_decimal(raw_amount)
        if amount is None:
            return None

        if profile.sign_convention == SignConvention.NEGATIVE_IS_INCOME:
            return -amount

        # NEGATIVE_IS_EXPENSE: already matches MoneyBin convention
        return amount

    @staticmethod
    def _build_accounts_df(
        profile: CSVProfile,
        account_id: str,
        source_file: str,
        extraction_timestamp: datetime,
    ) -> pl.DataFrame:
        """Build a synthetic accounts DataFrame.

        Args:
            profile: Column mapping profile.
            account_id: Account identifier.
            source_file: Source file path string.
            extraction_timestamp: When extraction occurred.

        Returns:
            Single-row DataFrame for the account.
        """
        return pl.DataFrame([
            {
                "account_id": account_id,
                "account_type": None,
                "institution_name": profile.institution_name,
                "source_file": source_file,
                "extracted_at": extraction_timestamp.isoformat(),
            }
        ])


def _generate_transaction_id(
    date: str,
    amount: str,
    description: str,
    account_id: str,
) -> str:
    """Generate a deterministic synthetic transaction ID.

    The hash is derived solely from the transaction's logical fields, so the
    same transaction produces the same ID regardless of which CSV export it
    appears in or its row position. This enables cross-file dedup in the
    staging layer (``PARTITION BY transaction_id, account_id``).

    Two transactions that are identical in every field are indistinguishable
    from CSV data alone and are correctly collapsed by dedup.

    Args:
        date: Transaction date as string.
        amount: Normalised amount as string.
        description: Transaction description.
        account_id: Account identifier.

    Returns:
        Transaction ID prefixed with ``csv_``.
    """
    raw = f"{date}|{amount}|{description}|{account_id}"
    digest = hashlib.sha256(raw.encode()).hexdigest()[:16]
    return f"csv_{digest}"


def _parse_decimal(value: str | None) -> float | None:
    """Parse a string to a float, handling common bank formatting.

    Handles commas (``1,234.56``), parentheses for negatives (``(50.00)``),
    currency symbols (``$``), and empty strings.

    Args:
        value: Raw string value from CSV.

    Returns:
        Parsed float, or None if the value is empty or unparseable.
    """
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None

    # Remove currency symbols and whitespace
    value = value.replace("$", "").replace(",", "").strip()

    # Handle parentheses as negatives: (50.00) → -50.00
    if value.startswith("(") and value.endswith(")"):
        value = "-" + value[1:-1]

    try:
        return float(value)
    except ValueError:
        return None


def _get_optional(row: dict[str, Any], column: str | None) -> str | None:
    """Get a trimmed string value from an optional column.

    Args:
        row: Named row from the DataFrame.
        column: Column name, or None if not mapped.

    Returns:
        Trimmed string, or None if column is unmapped or value is empty.
    """
    if column is None:
        return None
    value = str(row.get(column, "")).strip()
    return value if value else None
