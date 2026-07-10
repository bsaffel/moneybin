"""OFX/QFX file extractor using ofxparse library.

This module extracts financial data from OFX (Open Financial Exchange) and QFX
(Quicken Web Connect) files and converts them into raw table structures suitable
for data warehousing and analysis.

Documentation: https://github.com/jseutter/ofxparse
"""

import hashlib
import html
import json
import logging
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import ofxparse
import polars as pl
from pydantic import BaseModel, Field, field_validator

from moneybin.extractors._types import ExtractionResult, FilePath, ProviderSource
from moneybin.extractors.ofx.config import OFXProviderConfig
from moneybin.utils.parsing import coerce_to_decimal

logger = logging.getLogger(__name__)


def _decode_text_field(value: str | None) -> str | None:
    """Repeatedly HTML-unescape a text field until it stabilizes.

    Some banks (notably Wells Fargo) emit SGML payee/memo content that is
    already entity-escaped (e.g. ``AT&amp;T``) and ofxparse decodes only one
    level, so ``AT&amp;amp;T`` survives one pass as ``AT&amp;T`` and lands in
    the database with stale entities. Looping ``html.unescape`` is idempotent
    on already-clean strings — ``html.unescape("AT&T")`` returns ``"AT&T"``.
    """
    if value is None:
        return None
    current = value
    for _ in range(3):  # 3 passes covers single + double + paranoid triple-escape
        decoded = html.unescape(current)
        if decoded == current:
            return current
        current = decoded
    return current


# Fields that distinguish two transactions the institution stamped with the same
# FITID. Order is fixed so the derived suffix is stable across re-imports.
_FITID_SIGNATURE_FIELDS = (
    "transaction_type",
    "date_posted",
    "amount",
    "payee",
    "memo",
    "check_number",
)
# Separates the raw FITID from the content-derived disambiguation suffix. Chosen
# because FITIDs are alphanumeric and never contain it, so the marked id can
# never collide with a real one.
_FITID_COLLISION_MARKER = "#"


def _fitid_content_signature(row: dict[str, Any]) -> str:
    """Unambiguous signature of the fields that make a transaction distinct.

    JSON-encodes the field values rather than joining on a delimiter: a
    free-text ``payee``/``memo`` that itself contains the delimiter would
    otherwise let two genuinely distinct transactions serialize identically
    (``payee="A|B",memo="C"`` vs ``payee="A",memo="B|C"``), making the collision
    check treat them as one and drop a row — the very bug this repairs.
    """
    return json.dumps(
        [str(row[field]) for field in _FITID_SIGNATURE_FIELDS],
        separators=(",", ":"),
    )


def _disambiguate_colliding_fitids(transactions: list[dict[str, Any]]) -> int:
    """Repair non-unique OFX FITIDs within a file so distinct rows aren't lost.

    The OFX spec promises FITID is unique per account, and MoneyBin keys the raw
    primary key and every dedup layer on ``(source_transaction_id, account_id)``.
    Some institutions violate this — Chase stamps a foreign purchase and its
    foreign-transaction fee (two distinct transactions posted the same day) with
    one shared FITID. Left unrepaired, the raw write path (``on_conflict="upsert"``
    → ``INSERT OR REPLACE``, keyed on that primary key) and the
    ``stg_ofx__transactions`` dedup window each keep only one of the two.

    For each ``(account_id, source_transaction_id)`` group whose members differ
    in content, append a deterministic content-hash suffix to *every* member's
    ``source_transaction_id``. Members with identical content hash to the same
    suffix (so genuine in-file duplicates still collapse); members with differing
    content get distinct ids and both survive. The suffix is a pure function of
    the row's own content, so re-importing the same file reproduces the same ids
    and dedup stays idempotent.

    Suffixing *all* colliding members (rather than leaving one "plain") is
    deliberate: a suffixed id can never equal a plain FITID, so the worst case is
    a missed cross-file dedup (a visible duplicate surfaced for review) — never a
    silent false merge, which is the data-loss failure mode this repairs.

    Scope is one file per call — also deliberate, and the boundary where this is
    provably sound. Within a single export a bank lists each transaction once, so
    two same-FITID rows there are genuinely distinct and safe to split. *Across*
    files a second same-FITID row is ambiguous: it may be a distinct transaction,
    or the same one re-exported with drifted ``payee``/``memo`` (e.g.
    pending→posted). Disambiguating that by content would turn a re-export into a
    duplicate — the opposite failure — and content alone can't tell the two
    apart. Cross-file same-FITID collisions are therefore intentionally not
    repaired here (rare in practice: the observed pattern, a foreign purchase and
    its same-day fee, always co-occurs in one export); a fuller solution needs a
    stronger signal and is tracked as a follow-up.

    Mutates ``transactions`` in place; returns the number of rows rewritten.
    """
    by_key: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in transactions:
        by_key[(row["account_id"], row["source_transaction_id"])].append(row)

    rewritten = 0
    for rows in by_key.values():
        if len(rows) < 2:
            continue
        if len({_fitid_content_signature(row) for row in rows}) < 2:
            # Every member is byte-identical → a genuine duplicate; leave the id
            # untouched and let the raw PK / staging window collapse it.
            continue
        for row in rows:
            signature = _fitid_content_signature(row)
            digest = hashlib.sha256(signature.encode()).hexdigest()[:8]
            row["source_transaction_id"] = (
                f"{row['source_transaction_id']}{_FITID_COLLISION_MARKER}{digest}"
            )
            rewritten += 1
    return rewritten


_DECIMAL_AMOUNT = pl.Decimal(precision=18, scale=2)
_BALANCE_AMOUNT_OVERRIDES = {
    "ledger_balance": _DECIMAL_AMOUNT,
    "available_balance": _DECIMAL_AMOUNT,
}
_TRANSACTIONS_AMOUNT_OVERRIDES = {"amount": _DECIMAL_AMOUNT}


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
        """Coerce numeric input to Decimal (required field — None rejected)."""
        result = coerce_to_decimal(v)
        if result is None:
            raise ValueError("amount is required")
        return result

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
        return coerce_to_decimal(v)

    model_config = {"extra": "allow"}


def preprocess_ofx_content(content: str) -> str:
    """Preprocess OFX content to handle SGML format without newlines.

    Some institutions (notably Wells Fargo QFX exports) emit a single-line
    SGML header that ofxparse rejects. Inserting newlines before each header
    tag normalizes both single- and multi-line forms.
    """
    if content.startswith("OFXHEADER:") and "\n" not in content[:100]:
        if "<OFX>" in content:
            header_part, xml_part = content.split("<OFX>", 1)
            for tag in (
                "OFXHEADER:",
                "DATA:",
                "VERSION:",
                "SECURITY:",
                "ENCODING:",
                "CHARSET:",
                "COMPRESSION:",
                "OLDFILEUID:",
                "NEWFILEUID:",
            ):
                header_part = header_part.replace(tag, f"\n{tag}")
            header_part = header_part.lstrip("\n")
            content = header_part + "\n<OFX>" + xml_part
    return content


class OFXExtractor:
    """Extract financial data from OFX/QFX files into raw table structures."""

    name = "ofx"
    """Provider name; matches raw.ofx_* table prefix."""

    source_type = "ofx"
    """Written into source_type column on every row produced by this provider."""

    def __init__(self, config: OFXProviderConfig | None = None):
        """Initialize the OFX extractor.

        Args:
            config: Extraction configuration settings
        """
        from moneybin.config import get_raw_data_path

        self.config = config or OFXProviderConfig()

        # Resolve raw_data_path locally so the (frozen) config stays
        # immutable. When None, fall back to the profile-aware default.
        self.raw_data_path: Path = (
            self.config.raw_data_path or get_raw_data_path() / "ofx"
        )
        self.raw_data_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"Initialized OFX extractor with output: {self.raw_data_path}")

    def extract(self, source: ProviderSource) -> ExtractionResult:
        """Provider Protocol entry point.

        OFX accepts ``FilePath`` only. ``import_id`` and ``source_origin``
        are framework-supplied — currently still threaded through callers
        via ``extract_from_file()`` directly until Task 5 (the framework
        wiring) lands. This stub satisfies the Protocol's structural shape
        but is not yet a live call path.
        """
        if not isinstance(source, FilePath):
            raise TypeError(
                f"OFXExtractor expects FilePath; got {type(source).__name__}"
            )
        raise NotImplementedError(
            "OFXExtractor.extract() requires framework-supplied import_id and "
            "source_origin; wiring lands in Task 5 of the provider-framework "
            "refactor. Call extract_from_file() directly for now."
        )

    def schema_files(self) -> list[Path]:
        """Return paths to raw.ofx_* DDL files bundled with this package."""
        schema_dir = Path(__file__).parent / "schema"
        return sorted(schema_dir.glob("raw_ofx_*.sql"))

    def extract_from_file(
        self,
        file_path: Path,
        *,
        import_id: str,
        source_origin: str,
    ) -> dict[str, pl.DataFrame]:
        """Extract all data from an OFX/QFX/QBO file.

        Args:
            file_path: Path to the file.
            import_id: UUID of the import batch this extraction belongs to.
                Stamped on every row in every returned DataFrame.
            source_origin: Institution slug resolved by the caller (service layer).
                Stamped on transactions.

        Returns:
            dict with DataFrames for institutions, accounts, transactions, balances.

        Raises:
            FileNotFoundError: If the file doesn't exist.
            ValueError: If the file cannot be parsed.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"OFX file not found: {file_path}")

        logger.info(f"Extracting data from OFX file: {file_path}")

        try:
            with open(file_path, "rb") as f:
                content = f.read().decode("utf-8", errors="replace")
            if "�" in content:
                logger.warning(
                    f"OFX file contained non-UTF-8 bytes; replaced with U+FFFD: "
                    f"{file_path.name}"
                )
            content = preprocess_ofx_content(content)

            from io import BytesIO

            ofx = ofxparse.OfxParser.parse(BytesIO(content.encode("utf-8")))  # type: ignore[reportUnknownMemberType]

            extraction_timestamp = datetime.now()
            source_file = str(file_path)

            results = {
                "institutions": self._extract_institutions(
                    ofx, source_file, extraction_timestamp, import_id, source_origin
                ),
                "accounts": self._extract_accounts(
                    ofx, source_file, extraction_timestamp, import_id, source_origin
                ),
                "transactions": self._extract_transactions(
                    ofx, source_file, extraction_timestamp, import_id, source_origin
                ),
                "balances": self._extract_balances(
                    ofx, source_file, extraction_timestamp, import_id, source_origin
                ),
            }

            logger.info(
                f"Extracted {len(results['institutions'])} institution(s), "
                f"{len(results['accounts'])} account(s), "
                f"{len(results['transactions'])} transaction(s)"
            )

            return results

        except Exception as e:
            # Don't interpolate `e` into the log message: ofxparse exception
            # strings can embed payee/amount/memo content from the file. The
            # exception type name + file path is enough for diagnostics.
            logger.error(f"Failed to parse OFX file {file_path}: {type(e).__name__}")
            raise ValueError(f"Invalid OFX file format: {type(e).__name__}") from e

    def _extract_institutions(
        self,
        ofx: Any,
        source_file: str,
        extraction_timestamp: datetime,
        import_id: str,
        source_origin: str,
    ) -> pl.DataFrame:
        """Extract institution information from OFX data.

        ``raw.ofx_institutions.organization`` is part of the primary key, so a
        NULL ORG element would break the insert. Fall back to ``source_origin``
        (the resolved slug) so files lacking ``<FI><ORG>`` still load.
        """
        institutions_data: list[dict[str, Any]] = []

        for account in ofx.accounts:
            if account.institution:
                institution_data = {
                    "organization": account.institution.organization or source_origin,
                    "fid": account.institution.fid,
                    "source_file": source_file,
                    "extracted_at": extraction_timestamp.isoformat(),
                    "import_id": import_id,
                    "source_type": "ofx",
                }
                institutions_data.append(institution_data)

        # Deduplicate institutions
        if institutions_data:
            df = pl.DataFrame(institutions_data)
            return df.unique(  # pyright: ignore[reportUnknownMemberType]  # polars stubs partially unknown
                subset=["organization", "fid"], maintain_order=True
            )
        return pl.DataFrame(
            schema={
                "organization": pl.String,
                "fid": pl.String,
                "source_file": pl.String,
                "extracted_at": pl.String,
                "import_id": pl.String,
                "source_type": pl.String,
            }
        )

    def _extract_accounts(
        self,
        ofx: Any,
        source_file: str,
        extraction_timestamp: datetime,
        import_id: str,
        source_origin: str,
    ) -> pl.DataFrame:
        """Extract account information from OFX data."""
        accounts_data: list[dict[str, Any]] = []

        for account in ofx.accounts:
            inst_org = account.institution.organization if account.institution else None
            account_info = {
                "account_id": account.account_id,
                "routing_number": account.routing_number
                if hasattr(account, "routing_number")
                else None,
                "account_type": account.account_type
                if hasattr(account, "account_type")
                else None,
                "institution_org": inst_org or source_origin,
                "institution_fid": account.institution.fid
                if account.institution
                else None,
                "source_file": source_file,
                "extracted_at": extraction_timestamp.isoformat(),
                "import_id": import_id,
                "source_type": "ofx",
                # source_origin must match app.account_links.source_origin so the
                # staging translation JOIN in stg_ofx__accounts is total (B1).
                # Do NOT change how source_origin is derived here.
                "source_origin": source_origin,
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
                "import_id": pl.String,
                "source_type": pl.String,
                "source_origin": pl.String,
            }
        )

    def _extract_transactions(
        self,
        ofx: Any,
        source_file: str,
        extraction_timestamp: datetime,
        import_id: str,
        source_origin: str,
    ) -> pl.DataFrame:
        """Extract transaction data from OFX file."""
        transactions_data: list[dict[str, Any]] = []

        for account in ofx.accounts:
            for transaction in account.statement.transactions:
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
                    "source_transaction_id": tx_schema.id,
                    "account_id": account.account_id,
                    "transaction_type": tx_schema.type,
                    "date_posted": tx_schema.date.isoformat(),
                    "amount": tx_schema.amount,
                    "payee": _decode_text_field(tx_schema.payee),
                    "memo": _decode_text_field(tx_schema.memo),
                    "check_number": tx_schema.checknum,
                    "source_file": source_file,
                    "extracted_at": extraction_timestamp.isoformat(),
                    "import_id": import_id,
                    "source_type": "ofx",
                    "source_origin": source_origin,
                }
                transactions_data.append(tx_data)

        if transactions_data:
            repaired = _disambiguate_colliding_fitids(transactions_data)
            if repaired:
                logger.warning(
                    f"Repaired {repaired} OFX transaction(s) sharing a non-unique "
                    f"FITID within one file (institution reused a FITID for distinct "
                    f"transactions); disambiguated by content to prevent dedup loss"
                )
            return pl.DataFrame(
                transactions_data,
                schema_overrides=_TRANSACTIONS_AMOUNT_OVERRIDES,
            )
        return self._build_empty_transactions_df()

    def _build_empty_transactions_df(self) -> pl.DataFrame:
        """Build an empty transactions DataFrame with the correct schema."""
        return pl.DataFrame(
            schema={
                "source_transaction_id": pl.String,
                "account_id": pl.String,
                "transaction_type": pl.String,
                "date_posted": pl.String,
                "amount": _DECIMAL_AMOUNT,
                "payee": pl.String,
                "memo": pl.String,
                "check_number": pl.String,
                "source_file": pl.String,
                "extracted_at": pl.String,
                "import_id": pl.String,
                "source_type": pl.String,
                "source_origin": pl.String,
            }
        )

    def _extract_balances(
        self,
        ofx: Any,
        source_file: str,
        extraction_timestamp: datetime,
        import_id: str,
        source_origin: str,
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
                    "ledger_balance": statement.balance
                    if statement.balance is not None
                    else None,
                    "ledger_balance_date": statement.balance_date.isoformat()
                    if hasattr(statement, "balance_date") and statement.balance_date
                    else None,
                    "available_balance": statement.available_balance
                    if hasattr(statement, "available_balance")
                    and statement.available_balance is not None
                    else None,
                    "source_file": source_file,
                    "extracted_at": extraction_timestamp.isoformat(),
                    "import_id": import_id,
                    "source_type": "ofx",
                    # source_origin must match app.account_links.source_origin so the
                    # staging translation JOIN in stg_ofx__balances is total (B2).
                    # Do NOT change how source_origin is derived here.
                    "source_origin": source_origin,
                }
                balances_data.append(balance_info)

        if balances_data:
            return pl.DataFrame(
                balances_data, schema_overrides=_BALANCE_AMOUNT_OVERRIDES
            )
        return pl.DataFrame(
            schema={
                "account_id": pl.String,
                "statement_start_date": pl.String,
                "statement_end_date": pl.String,
                "ledger_balance": _DECIMAL_AMOUNT,
                "ledger_balance_date": pl.String,
                "available_balance": _DECIMAL_AMOUNT,
                "source_file": pl.String,
                "extracted_at": pl.String,
                "import_id": pl.String,
                "source_type": pl.String,
                "source_origin": pl.String,
            }
        )


def extract_ofx_file(
    file_path: Path | str,
    *,
    import_id: str,
    source_origin: str,
) -> dict[str, pl.DataFrame]:
    """Convenience function to extract data from an OFX/QFX/QBO file."""
    extractor = OFXExtractor()
    return extractor.extract_from_file(
        Path(file_path), import_id=import_id, source_origin=source_origin
    )
