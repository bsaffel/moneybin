"""OFX/QFX file extractor using ofxparse library.

This module extracts financial data from OFX (Open Financial Exchange) and QFX
(Quicken Web Connect) files and converts them into raw table structures suitable
for data warehousing and analysis.

Documentation: https://github.com/jseutter/ofxparse
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import ofxparse
import polars as pl
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


# Pydantic schemas for OFX data validation
class OFXInstitutionSchema(BaseModel):
    """OFX financial institution information."""

    organization: str | None = Field(None, description="Financial institution name")
    fid: str | None = Field(None, description="Financial institution ID")

    model_config = {"extra": "allow"}


class OFXAccountSchema(BaseModel):
    """OFX account information."""

    account_id: str = Field(..., description="Account identifier")
    routing_number: str | None = Field(None, description="Bank routing number")
    account_type: str | None = Field(None, description="Account type (e.g., CHECKING)")
    institution: OFXInstitutionSchema | None = Field(
        None, description="Institution information"
    )

    model_config = {"extra": "allow"}


class OFXTransactionSchema(BaseModel):
    """OFX transaction data with validation."""

    id: str = Field(..., description="Financial institution transaction ID (FITID)")
    type: str = Field(..., description="Transaction type (e.g., DEBIT, CREDIT)")
    date: datetime = Field(..., description="Transaction posting date")
    amount: Decimal = Field(..., description="Transaction amount")
    payee: str | None = Field(None, description="Transaction payee/merchant name")
    memo: str | None = Field(None, description="Transaction memo/description")
    checknum: str | None = Field(None, description="Check number if applicable")

    @field_validator("amount", mode="before")
    @classmethod
    def validate_amount(cls, v: Any) -> Decimal:
        """Convert amount to Decimal for precision."""
        if isinstance(v, Decimal):
            return v
        if isinstance(v, (int, float, str)):
            return Decimal(str(v))
        raise ValueError(f"Cannot convert {type(v)} to Decimal")

    model_config = {"extra": "allow"}


class OFXStatementSchema(BaseModel):
    """OFX statement with balance information."""

    start_date: datetime | None = Field(None, description="Statement start date")
    end_date: datetime | None = Field(None, description="Statement end date")
    balance: Decimal | None = Field(None, description="Ledger balance")
    balance_date: datetime | None = Field(None, description="Balance as-of date")
    available_balance: Decimal | None = Field(
        None, description="Available balance if provided"
    )

    @field_validator("balance", "available_balance", mode="before")
    @classmethod
    def validate_decimal(cls, v: Any) -> Decimal | None:
        """Convert balance to Decimal for precision."""
        if v is None:
            return None
        if isinstance(v, Decimal):
            return v
        if isinstance(v, (int, float, str)):
            return Decimal(str(v))
        raise ValueError(f"Cannot convert {type(v)} to Decimal")

    model_config = {"extra": "allow"}


@dataclass
class OFXExtractionConfig:
    """Configuration for OFX file extraction."""

    save_raw_data: bool = True
    raw_data_path: Path | None = None  # Will use profile-aware path if None
    preserve_source_files: bool = True
    validate_balances: bool = True


class OFXExtractor:
    """Extract financial data from OFX/QFX files into raw table structures."""

    def __init__(self, config: OFXExtractionConfig | None = None):
        """Initialize the OFX extractor.

        Args:
            config: Extraction configuration settings
        """
        from moneybin.config import get_raw_data_path

        self.config = config or OFXExtractionConfig()

        # Use profile-aware path if not explicitly provided
        if self.config.raw_data_path is None:
            self.config.raw_data_path = get_raw_data_path() / "ofx"

        # Ensure output directory exists
        self.config.raw_data_path.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Initialized OFX extractor with output: {self.config.raw_data_path}"
        )

    def _preprocess_ofx_content(self, content: str) -> str:
        """Preprocess OFX content to handle SGML format without newlines.

        Args:
            content: Raw OFX file content

        Returns:
            str: Preprocessed content with proper formatting
        """
        # Check if this is SGML format (headers without newlines)
        if content.startswith("OFXHEADER:") and "\n" not in content[:100]:
            # Split the header section from the XML body
            if "<OFX>" in content:
                header_part, xml_part = content.split("<OFX>", 1)

                # Add newlines after each header tag
                header_part = header_part.replace("OFXHEADER:", "\nOFXHEADER:")
                header_part = header_part.replace("DATA:", "\nDATA:")
                header_part = header_part.replace("VERSION:", "\nVERSION:")
                header_part = header_part.replace("SECURITY:", "\nSECURITY:")
                header_part = header_part.replace("ENCODING:", "\nENCODING:")
                header_part = header_part.replace("CHARSET:", "\nCHARSET:")
                header_part = header_part.replace("COMPRESSION:", "\nCOMPRESSION:")
                header_part = header_part.replace("OLDFILEUID:", "\nOLDFILEUID:")
                header_part = header_part.replace("NEWFILEUID:", "\nNEWFILEUID:")

                # Remove leading newline if added
                header_part = header_part.lstrip("\n")

                # Reconstruct the content
                content = header_part + "\n<OFX>" + xml_part

        return content

    def extract_from_file(
        self, file_path: Path, institution_name: str | None = None
    ) -> dict[str, pl.DataFrame]:
        """Extract all data from an OFX/QFX file.

        Args:
            file_path: Path to the OFX/QFX file
            institution_name: Optional institution name override

        Returns:
            dict: Dictionary containing DataFrames for:
                - institutions: Financial institution information
                - accounts: Account details
                - transactions: Transaction records
                - balances: Account balance snapshots

        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If the file cannot be parsed
        """
        if not file_path.exists():
            raise FileNotFoundError(f"OFX file not found: {file_path}")

        logger.info(f"Extracting data from OFX file: {file_path}")

        try:
            # Read and preprocess OFX file to handle various formats
            with open(file_path, "rb") as f:
                content = f.read().decode("utf-8", errors="ignore")

            # Preprocess SGML-format OFX files (like Wells Fargo QFX)
            # These files have headers on one line separated by tags rather than newlines
            content = self._preprocess_ofx_content(content)

            from io import BytesIO

            # ofxparse library has incomplete type annotations
            ofx = ofxparse.OfxParser.parse(BytesIO(content.encode("utf-8")))  # type: ignore[reportUnknownMemberType]

            # Extract data into structured tables
            extraction_timestamp = datetime.now()
            source_file = str(file_path)

            results = {
                "institutions": self._extract_institutions(
                    ofx, institution_name, source_file, extraction_timestamp
                ),
                "accounts": self._extract_accounts(
                    ofx, source_file, extraction_timestamp
                ),
                "transactions": self._extract_transactions(
                    ofx, source_file, extraction_timestamp
                ),
                "balances": self._extract_balances(
                    ofx, source_file, extraction_timestamp
                ),
            }

            # Log extraction summary
            logger.info(
                f"Extracted {len(results['institutions'])} institution(s), "
                f"{len(results['accounts'])} account(s), "
                f"{len(results['transactions'])} transaction(s)"
            )

            # Save raw data if configured
            if self.config.save_raw_data:
                self._save_raw_data(results, file_path)

            return results

        except Exception as e:
            logger.error(f"Failed to parse OFX file {file_path}: {e}")
            raise ValueError(f"Invalid OFX file format: {e}") from e

    def _extract_institutions(
        self,
        ofx: Any,
        institution_name_override: str | None,
        source_file: str,
        extraction_timestamp: datetime,
    ) -> pl.DataFrame:
        """Extract institution information from OFX data."""
        institutions_data: list[dict[str, Any]] = []

        for account in ofx.accounts:
            if account.institution:
                institution_data = {
                    "organization": institution_name_override
                    or account.institution.organization,
                    "fid": account.institution.fid,
                    "source_file": source_file,
                    "extracted_at": extraction_timestamp.isoformat(),
                }
                institutions_data.append(institution_data)

        # Deduplicate institutions
        if institutions_data:
            df = pl.DataFrame(institutions_data)
            return df.unique(subset=["organization", "fid"], maintain_order=True)
        return pl.DataFrame(
            schema={
                "organization": pl.String,
                "fid": pl.String,
                "source_file": pl.String,
                "extracted_at": pl.String,
            }
        )

    def _extract_accounts(
        self, ofx: Any, source_file: str, extraction_timestamp: datetime
    ) -> pl.DataFrame:
        """Extract account information from OFX data."""
        accounts_data: list[dict[str, Any]] = []

        for account in ofx.accounts:
            account_info = {
                "account_id": account.account_id,
                "routing_number": account.routing_number
                if hasattr(account, "routing_number")
                else None,
                "account_type": account.account_type
                if hasattr(account, "account_type")
                else None,
                "institution_org": account.institution.organization
                if account.institution
                else None,
                "institution_fid": account.institution.fid
                if account.institution
                else None,
                "source_file": source_file,
                "extracted_at": extraction_timestamp.isoformat(),
            }
            accounts_data.append(account_info)

        if accounts_data:
            return pl.DataFrame(accounts_data)
        return pl.DataFrame(
            schema={
                "account_id": pl.String,
                "routing_number": pl.String,
                "account_type": pl.String,
                "institution_org": pl.String,
                "institution_fid": pl.String,
                "source_file": pl.String,
                "extracted_at": pl.String,
            }
        )

    def _extract_transactions(
        self, ofx: Any, source_file: str, extraction_timestamp: datetime
    ) -> pl.DataFrame:
        """Extract transaction data from OFX file."""
        transactions_data: list[dict[str, Any]] = []

        for account in ofx.accounts:
            for transaction in account.statement.transactions:
                # Validate transaction data
                tx_schema = OFXTransactionSchema(
                    id=transaction.id,
                    type=transaction.type,
                    date=transaction.date,
                    amount=transaction.amount,
                    payee=transaction.payee,
                    memo=transaction.memo,
                    checknum=transaction.checknum
                    if hasattr(transaction, "checknum")
                    else None,
                )

                tx_data = {
                    "transaction_id": tx_schema.id,
                    "account_id": account.account_id,
                    "transaction_type": tx_schema.type,
                    "date_posted": tx_schema.date.isoformat(),
                    "amount": float(tx_schema.amount),
                    "payee": tx_schema.payee,
                    "memo": tx_schema.memo,
                    "check_number": tx_schema.checknum,
                    "source_file": source_file,
                    "extracted_at": extraction_timestamp.isoformat(),
                }
                transactions_data.append(tx_data)

        if transactions_data:
            return pl.DataFrame(transactions_data)
        return pl.DataFrame(
            schema={
                "transaction_id": pl.String,
                "account_id": pl.String,
                "transaction_type": pl.String,
                "date_posted": pl.String,
                "amount": pl.Float64,
                "payee": pl.String,
                "memo": pl.String,
                "check_number": pl.String,
                "source_file": pl.String,
                "extracted_at": pl.String,
            }
        )

    def _extract_balances(
        self, ofx: Any, source_file: str, extraction_timestamp: datetime
    ) -> pl.DataFrame:
        """Extract balance information from OFX file."""
        balances_data: list[dict[str, Any]] = []

        for account in ofx.accounts:
            statement = account.statement
            if statement:
                balance_info = {
                    "account_id": account.account_id,
                    "statement_start_date": statement.start_date.isoformat()
                    if statement.start_date
                    else None,
                    "statement_end_date": statement.end_date.isoformat()
                    if statement.end_date
                    else None,
                    "ledger_balance": float(statement.balance)
                    if statement.balance is not None
                    else None,
                    "ledger_balance_date": statement.balance_date.isoformat()
                    if hasattr(statement, "balance_date") and statement.balance_date
                    else None,
                    "available_balance": float(statement.available_balance)
                    if hasattr(statement, "available_balance")
                    and statement.available_balance is not None
                    else None,
                    "source_file": source_file,
                    "extracted_at": extraction_timestamp.isoformat(),
                }
                balances_data.append(balance_info)

        if balances_data:
            return pl.DataFrame(balances_data)
        return pl.DataFrame(
            schema={
                "account_id": pl.String,
                "statement_start_date": pl.String,
                "statement_end_date": pl.String,
                "ledger_balance": pl.Float64,
                "ledger_balance_date": pl.String,
                "available_balance": pl.Float64,
                "source_file": pl.String,
                "extracted_at": pl.String,
            }
        )

    def _save_raw_data(
        self, results: dict[str, pl.DataFrame], source_file: Path
    ) -> None:
        """Save extracted data to parquet files in an organized directory structure.

        Creates a directory structure like:
            data/raw/ofx/extracted/WellsFargo_2025/
                institutions.parquet
                accounts.parquet
                transactions.parquet
                balances.parquet

        Args:
            results: Dictionary of DataFrames to save
            source_file: Original source file path (for naming)
        """
        # Create extraction directory named after source file (without extension)
        assert self.config.raw_data_path is not None  # noqa: S101 - Set in __init__, safe for type narrowing
        file_stem = source_file.stem
        extraction_dir = self.config.raw_data_path / "extracted" / file_stem
        extraction_dir.mkdir(parents=True, exist_ok=True)

        for table_name, df in results.items():
            if len(df) > 0:
                # Simple filename without timestamps - idempotent extraction
                output_path = extraction_dir / f"{table_name}.parquet"
                df.write_parquet(output_path)
                logger.info(
                    f"Saved {table_name} data ({len(df)} rows) to {output_path}"
                )


def extract_ofx_file(
    file_path: Path | str, institution_name: str | None = None
) -> dict[str, pl.DataFrame]:
    """Convenience function to extract data from an OFX/QFX file.

    Args:
        file_path: Path to the OFX/QFX file
        institution_name: Optional institution name override

    Returns:
        dict: Dictionary containing DataFrames for institutions, accounts,
              transactions, and balances
    """
    extractor = OFXExtractor()
    return extractor.extract_from_file(Path(file_path), institution_name)
