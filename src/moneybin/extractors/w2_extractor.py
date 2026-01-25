"""W2 form extractor using dual extraction strategy (text + OCR).

This module extracts wage and tax information from IRS Form W-2 PDFs using
both text extraction (pdfplumber) and OCR (pytesseract) to ensure accuracy.
Results are compared for confidence before proceeding.

The data model balances structure with flexibility:
- Core fields (employee, employer, wages, taxes) as typed columns
- State/local tax info as JSON (supports 0-2 states)
- Optional boxes as JSON (only stored when present)

Documentation:
- pdfplumber: https://pdfplumber.readthedocs.io/
- pytesseract: https://github.com/madmaze/pytesseract
"""

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pdfplumber
import polars as pl
import pytesseract
from pdf2image import convert_from_path
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


class W2StateLocalInfo(BaseModel):
    """State and local tax information from W2 (boxes 15-20).

    A W2 can contain information for multiple states (typically 1-2).
    """

    state: str | None = Field(None, description="State postal code (box 15)")
    employer_state_id: str | None = Field(
        None, description="Employer's state ID number (box 15)"
    )
    state_wages: Decimal | None = Field(None, description="State wages (box 16)")
    state_income_tax: Decimal | None = Field(
        None, description="State income tax withheld (box 17)"
    )
    local_wages: Decimal | None = Field(None, description="Local wages (box 18)")
    local_income_tax: Decimal | None = Field(
        None, description="Local income tax withheld (box 19)"
    )
    locality_name: str | None = Field(None, description="Locality name (box 20)")

    @field_validator(
        "state_wages",
        "state_income_tax",
        "local_wages",
        "local_income_tax",
        mode="before",
    )
    @classmethod
    def validate_decimal(cls, v: Any) -> Decimal | None:
        """Convert numeric fields to Decimal for precision."""
        if v is None or v == "":
            return None
        if isinstance(v, Decimal):
            return v
        if isinstance(v, (int, float, str)):
            return Decimal(str(v))
        raise ValueError(f"Cannot convert {type(v)} to Decimal")

    model_config = {"extra": "forbid"}


class W2OptionalBoxes(BaseModel):
    """Optional/less common W2 boxes (typically box 12 codes and box 14).

    Box 12 codes (up to 4 per W2):
    - A: Uncollected social security or RRTA tax on tips
    - B: Uncollected Medicare tax on tips
    - C: Taxable cost of group-term life insurance over $50,000
    - D: Elective deferrals to a 401(k), 403(b), or 457(b) plan
    - E: Elective deferrals under a 403(b) salary reduction agreement
    - And many more...

    Box 14: Other - employer can use this for various information
    """

    box_12_codes: dict[str, str] | None = Field(
        None,
        description="Box 12 codes and amounts (e.g., {'D': '19500.00', 'DD': '8450.00'})",
    )
    box_14_other: str | None = Field(
        None, description="Box 14: Other information from employer"
    )

    model_config = {"extra": "allow"}  # Allow additional custom fields


class W2FormSchema(BaseModel):
    """IRS Form W-2 Wage and Tax Statement schema.

    Standard W2 fields with validation. This model captures the essential
    information from the W2 form while maintaining data quality.
    """

    # Tax year
    tax_year: int = Field(..., description="Tax year for this W2", ge=2000, le=2100)

    # Employee information (boxes a, c-f)
    employee_ssn: str = Field(
        ..., description="Employee's Social Security Number (box a)"
    )
    employee_first_name: str = Field(..., description="Employee's first name (box e)")
    employee_last_name: str = Field(..., description="Employee's last name (box e)")
    employee_address: str | None = Field(
        None, description="Employee's full address (box f)"
    )

    # Employer information (boxes b, c)
    employer_ein: str = Field(
        ..., description="Employer Identification Number (EIN) (box b)"
    )
    employer_name: str = Field(..., description="Employer's name (box c)")
    employer_address: str | None = Field(
        None, description="Employer's full address (box c)"
    )

    # Core wage and tax information (boxes 1-6)
    wages: Decimal = Field(..., description="Wages, tips, other compensation (box 1)")
    federal_income_tax: Decimal = Field(
        ..., description="Federal income tax withheld (box 2)"
    )
    social_security_wages: Decimal | None = Field(
        None, description="Social security wages (box 3)"
    )
    social_security_tax: Decimal | None = Field(
        None, description="Social security tax withheld (box 4)"
    )
    medicare_wages: Decimal | None = Field(None, description="Medicare wages (box 5)")
    medicare_tax: Decimal | None = Field(
        None, description="Medicare tax withheld (box 6)"
    )

    # Additional compensation (boxes 7-8)
    social_security_tips: Decimal | None = Field(
        None, description="Social security tips (box 7)"
    )
    allocated_tips: Decimal | None = Field(None, description="Allocated tips (box 8)")

    # Dependent care benefits (box 10)
    dependent_care_benefits: Decimal | None = Field(
        None, description="Dependent care benefits (box 10)"
    )

    # Nonqualified plans (box 11)
    nonqualified_plans: Decimal | None = Field(
        None, description="Nonqualified plans (box 11)"
    )

    # Checkboxes (box 13)
    is_statutory_employee: bool = Field(
        False, description="Statutory employee checkbox (box 13)"
    )
    is_retirement_plan: bool = Field(
        False, description="Retirement plan checkbox (box 13)"
    )
    is_third_party_sick_pay: bool = Field(
        False, description="Third-party sick pay checkbox (box 13)"
    )

    # Control number and state/local info stored as JSON
    control_number: str | None = Field(None, description="Control number (box d)")
    state_local_info: list[W2StateLocalInfo] = Field(
        default_factory=list,
        description="State and local tax information (boxes 15-20)",
    )
    optional_boxes: W2OptionalBoxes | None = Field(
        None, description="Optional boxes (12, 14) stored as structured data"
    )

    @field_validator(
        "wages",
        "federal_income_tax",
        "social_security_wages",
        "social_security_tax",
        "medicare_wages",
        "medicare_tax",
        "social_security_tips",
        "allocated_tips",
        "dependent_care_benefits",
        "nonqualified_plans",
        mode="before",
    )
    @classmethod
    def validate_decimal(cls, v: Any) -> Decimal | None:
        """Convert monetary fields to Decimal for precision."""
        if v is None or v == "":
            return None
        if isinstance(v, Decimal):
            return v
        if isinstance(v, (int, float, str)):
            # Remove common formatting (commas, dollar signs)
            if isinstance(v, str):
                v = v.replace(",", "").replace("$", "").strip()
                if not v:
                    return None
            return Decimal(str(v))
        raise ValueError(f"Cannot convert {type(v)} to Decimal")

    model_config = {"extra": "allow"}


@dataclass
class ExtractionResult:
    """Result from a single extraction method."""

    method: str  # "text" or "ocr"
    success: bool
    data: dict[str, Any] | None
    error: str | None
    confidence_score: float  # 0.0 to 1.0


@dataclass
class W2ExtractionConfig:
    """Configuration for W2 PDF extraction."""

    save_raw_data: bool = True
    raw_data_path: Path | None = None  # Will use profile-aware path if None
    preserve_source_files: bool = True
    require_dual_extraction: bool = True  # Require both methods to agree
    min_confidence_score: float = 0.8  # Minimum confidence to proceed
    enable_ocr: bool = True  # Enable OCR extraction (can disable for testing)


class W2Extractor:
    """Extract W2 wage and tax data from PDF forms using dual extraction strategy."""

    def __init__(self, config: W2ExtractionConfig | None = None):
        """Initialize the W2 extractor.

        Args:
            config: Extraction configuration settings
        """
        from moneybin.config import get_raw_data_path

        self.config = config or W2ExtractionConfig()

        # Use profile-aware path if not explicitly provided
        if self.config.raw_data_path is None:
            self.config.raw_data_path = get_raw_data_path() / "w2"

        # Ensure output directory exists
        self.config.raw_data_path.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Initialized W2 extractor with output: {self.config.raw_data_path}"
        )

    def extract_from_file(
        self, file_path: Path, tax_year: int | None = None
    ) -> pl.DataFrame:
        """Extract W2 data from a PDF file using dual extraction strategy.

        This method attempts both text extraction and OCR, then compares results
        for confidence before proceeding.

        Args:
            file_path: Path to the W2 PDF file
            tax_year: Optional tax year (e.g., 2024). If not provided, will attempt
                     to extract from PDF or derive from metadata.

        Returns:
            pl.DataFrame: DataFrame containing extracted W2 data

        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If extraction fails or confidence is too low
        """
        if not file_path.exists():
            raise FileNotFoundError(f"W2 PDF file not found: {file_path}")

        logger.info(f"Extracting W2 data from: {file_path}")

        if self.config.enable_ocr:
            logger.info("Using dual extraction strategy (text + OCR)")
        else:
            logger.info("Using text-only extraction (OCR disabled)")

        # Attempt text extraction
        text_result = self._extract_using_text(file_path, tax_year)

        # Attempt OCR extraction (if enabled)
        if self.config.enable_ocr:
            ocr_result = self._extract_using_ocr(file_path, tax_year)
        else:
            # Create a "not attempted" result
            ocr_result = ExtractionResult(
                method="ocr",
                success=False,
                data=None,
                error="OCR disabled by configuration",
                confidence_score=0.0,
            )

        # Compare and validate results
        final_data = self._compare_and_validate(text_result, ocr_result, file_path)

        # Create DataFrame
        try:
            extraction_timestamp = datetime.now()
            source_file = str(file_path)

            # Validate with Pydantic schema
            w2_schema = W2FormSchema(**final_data)

            # Convert to DataFrame format
            df_data = self._schema_to_dataframe_dict(
                w2_schema, source_file, extraction_timestamp
            )
            df = pl.DataFrame([df_data])

            logger.info(
                f"✅ Extracted W2 for {w2_schema.tax_year}: "
                f"{w2_schema.employee_first_name} {w2_schema.employee_last_name}"
            )

            # Save raw data if configured
            if self.config.save_raw_data:
                self._save_raw_data(df, file_path)

            return df

        except Exception as e:
            logger.error(f"Failed to validate W2 data: {e}")
            raise ValueError(f"Invalid W2 data: {e}") from e

    def _extract_using_text(
        self, file_path: Path, tax_year: int | None
    ) -> ExtractionResult:
        """Extract W2 data using text extraction (pdfplumber).

        Args:
            file_path: Path to PDF file
            tax_year: Optional explicit tax year

        Returns:
            ExtractionResult: Result of text extraction attempt
        """
        logger.info("📄 Attempting text extraction...")
        try:
            with pdfplumber.open(file_path) as pdf:
                if len(pdf.pages) == 0:
                    return ExtractionResult(
                        method="text",
                        success=False,
                        data=None,
                        error="PDF contains no pages",
                        confidence_score=0.0,
                    )

                # Extract text from all pages
                # W-2 PDFs often contain 4 copies in a 2x2 grid (employee/employer copies)
                # Crop to top-left quarter to avoid duplicate data
                full_text = ""
                for page in pdf.pages:
                    # Crop to top-left quarter (0, 0) to (width/2, height/2)
                    bbox = (0, 0, page.width / 2, page.height / 2)
                    cropped_page = page.crop(bbox)
                    full_text += cropped_page.extract_text() or ""

                if not full_text.strip():
                    return ExtractionResult(
                        method="text",
                        success=False,
                        data=None,
                        error="No text extracted",
                        confidence_score=0.0,
                    )

                logger.debug(f"Extracted {len(full_text)} characters via text method")

                # Get PDF metadata
                pdf_metadata = pdf.metadata  # type: ignore[reportUnknownMemberType] - pdfplumber has incomplete type stubs

                # Parse W2 data
                w2_data = self._parse_w2_text(
                    full_text,
                    file_path,
                    tax_year,
                    pdf_metadata,  # type: ignore[reportUnknownArgumentType] - pdfplumber metadata type
                )

                # Calculate confidence score based on completeness
                confidence = self._calculate_confidence(w2_data)

                return ExtractionResult(
                    method="text",
                    success=True,
                    data=w2_data,
                    error=None,
                    confidence_score=confidence,
                )

        except Exception as e:
            logger.warning(f"Text extraction failed: {e}")
            return ExtractionResult(
                method="text",
                success=False,
                data=None,
                error=str(e),
                confidence_score=0.0,
            )

    def _extract_using_ocr(
        self, file_path: Path, tax_year: int | None
    ) -> ExtractionResult:
        """Extract W2 data using OCR (pytesseract).

        Args:
            file_path: Path to PDF file
            tax_year: Optional explicit tax year

        Returns:
            ExtractionResult: Result of OCR extraction attempt
        """
        logger.info("🔍 Attempting OCR extraction...")
        try:
            # Convert PDF to images
            images = convert_from_path(str(file_path), dpi=300)

            if not images:
                return ExtractionResult(
                    method="ocr",
                    success=False,
                    data=None,
                    error="Failed to convert PDF to images",
                    confidence_score=0.0,
                )

            # Perform OCR on each page
            # W-2 PDFs often contain 4 copies in a 2x2 grid (employee/employer copies)
            # Crop to top-left quarter to avoid duplicate data
            full_text = ""
            for i, image in enumerate(images):
                logger.debug(f"Performing OCR on page {i + 1}...")
                # Crop to top-left quarter
                width, height = image.size
                cropped_image = image.crop((0, 0, width // 2, height // 2))
                page_text = pytesseract.image_to_string(cropped_image)  # type: ignore[reportUnknownMemberType] - pytesseract has incomplete type stubs
                full_text += page_text  # type: ignore[reportOperatorIssue] - pytesseract returns str

            if not full_text.strip():  # type: ignore[reportUnknownMemberType] - pytesseract returns str
                return ExtractionResult(
                    method="ocr",
                    success=False,
                    data=None,
                    error="No text extracted via OCR",
                    confidence_score=0.0,
                )

            logger.debug(f"Extracted {len(full_text)} characters via OCR")  # type: ignore[reportUnknownArgumentType] - pytesseract returns str

            # Parse W2 data (without PDF metadata since we're using images)
            w2_data = self._parse_w2_text(full_text, file_path, tax_year, None)  # type: ignore[reportUnknownArgumentType] - pytesseract returns str

            # Calculate confidence score
            confidence = self._calculate_confidence(w2_data)

            return ExtractionResult(
                method="ocr",
                success=True,
                data=w2_data,
                error=None,
                confidence_score=confidence,
            )

        except Exception as e:
            logger.warning(f"OCR extraction failed: {e}")
            return ExtractionResult(
                method="ocr",
                success=False,
                data=None,
                error=str(e),
                confidence_score=0.0,
            )

    def _compare_and_validate(
        self,
        text_result: ExtractionResult,
        ocr_result: ExtractionResult,
        file_path: Path,
    ) -> dict[str, Any]:
        """Compare results from both extraction methods and validate.

        Args:
            text_result: Result from text extraction
            ocr_result: Result from OCR extraction
            file_path: Path to source file (for error messages)

        Returns:
            dict: Validated W2 data with highest confidence

        Raises:
            ValueError: If extraction fails or confidence is too low
        """
        # Check if both methods succeeded
        if text_result.success and ocr_result.success:
            logger.info(
                f"✅ Both methods succeeded - "
                f"Text confidence: {text_result.confidence_score:.2f}, "
                f"OCR confidence: {ocr_result.confidence_score:.2f}"
            )

            # Compare key fields for agreement
            agreement = self._check_agreement(text_result.data, ocr_result.data)  # type: ignore[arg-type]
            logger.info(f"Field agreement: {agreement:.1%}")

            if agreement >= 0.8:
                logger.info("✅ High agreement between methods")
                # Use result with higher confidence
                if text_result.confidence_score >= ocr_result.confidence_score:
                    logger.info("Using text extraction result")
                    return text_result.data  # type: ignore[return-value]
                else:
                    logger.info("Using OCR extraction result")
                    return ocr_result.data  # type: ignore[return-value]
            else:
                logger.warning(f"⚠️  Low agreement ({agreement:.1%}) between methods")
                # If require_dual_extraction, fail
                if self.config.require_dual_extraction:
                    raise ValueError(
                        f"Extraction methods disagree (agreement: {agreement:.1%}). "
                        f"Cannot proceed with low confidence."
                    )
                # Otherwise use method with higher confidence
                if text_result.confidence_score >= ocr_result.confidence_score:
                    return text_result.data  # type: ignore[return-value]
                else:
                    return ocr_result.data  # type: ignore[return-value]

        elif text_result.success:
            logger.info(
                f"✅ Text extraction succeeded (confidence: {text_result.confidence_score:.2f})"
            )
            logger.warning(f"⚠️  OCR extraction failed: {ocr_result.error}")

            if text_result.confidence_score >= self.config.min_confidence_score:
                logger.info("Using text extraction result")
                return text_result.data  # type: ignore[return-value]
            else:
                raise ValueError(
                    f"Text extraction confidence too low: {text_result.confidence_score:.2f} "
                    f"< {self.config.min_confidence_score:.2f}"
                )

        elif ocr_result.success:
            logger.info(
                f"✅ OCR extraction succeeded (confidence: {ocr_result.confidence_score:.2f})"
            )
            logger.warning(f"⚠️  Text extraction failed: {text_result.error}")

            if ocr_result.confidence_score >= self.config.min_confidence_score:
                logger.info("Using OCR extraction result")
                return ocr_result.data  # type: ignore[return-value]
            else:
                raise ValueError(
                    f"OCR extraction confidence too low: {ocr_result.confidence_score:.2f} "
                    f"< {self.config.min_confidence_score:.2f}"
                )

        else:
            # Both methods failed
            raise ValueError(
                f"Both extraction methods failed for {file_path}:\n"
                f"  Text: {text_result.error}\n"
                f"  OCR: {ocr_result.error}"
            )

    def _calculate_confidence(self, data: dict[str, Any]) -> float:
        """Calculate the extraction confidence score using required and important fields.

        The confidence score is a float between 0.0 (no information extracted)
        and 1.0 (all essential and important fields present). It reflects how
        complete and reliable the extracted W-2 data is, and is used to decide
        whether to accept, reject, or escalate for manual review.

        Calculation steps:
            1. Define "required" fields (core W-2 identity and wage/tax info).
            2. Define "important" fields (useful but not essential).
            3. For each category, count how many fields are present (not None/empty).
            4. Compute the proportion present in each category.
            5. The final confidence score is a weighted average:
                - 70% weight from required field completeness
                - 30% weight from important field completeness

        A high confidence score (e.g., ≥ 0.8) indicates that the extractor found most
        core information; a low score signals missing or ambiguous data.

        Args:
            data: Extracted W2 data

        Returns:
            float: Confidence score in [0.0, 1.0] reflecting data completeness

        Example:
            A data dict with all required fields, but only half of the important
            fields present: confidence = 0.7 * 1.0 + 0.3 * 0.5 = 0.85

        See Also:
            - self._calculate_confidence() implementation
            - Logs explaining low-confidence extractions
        """
        # Required fields for confidence calculation
        required_fields = [
            "tax_year",
            "employee_ssn",
            "employee_first_name",
            "employee_last_name",
            "employer_ein",
            "employer_name",
            "wages",
            "federal_income_tax",
        ]

        # Optional but important fields
        important_fields = [
            "social_security_wages",
            "social_security_tax",
            "medicare_wages",
            "medicare_tax",
            "employee_address",
            "employer_address",
        ]

        # Count present required fields
        required_present = sum(1 for field in required_fields if data.get(field))
        required_score = required_present / len(required_fields)

        # Count present important fields
        important_present = sum(1 for field in important_fields if data.get(field))
        important_score = important_present / len(important_fields)

        # Weighted average (70% required, 30% important)
        confidence = 0.7 * required_score + 0.3 * important_score

        return confidence

    def _check_agreement(self, data1: dict[str, Any], data2: dict[str, Any]) -> float:
        """Check agreement between two extraction results by comparing key fields.

        For each field in a predefined set of critical W2 fields (such as tax year,
        employee SSN, employer EIN, wages, and federal income tax), this method examines
        whether both extraction results include the field, and then compares the values:

        - For numeric fields (int, float, or Decimal), the method considers the values as
          agreed if they match exactly. If there is a small difference (within 5% of the
          first value), the method assigns partial credit (0.5) for that field, reflecting
          close numeric agreement that may arise due to OCR imperfections.
        - For string fields, the comparison requires an exact match to count as an agreement.

        The method calculates an overall agreement ratio by dividing the number of
        agreements (full or partial) by the total number of fields compared.

        Args:
            data1: First extraction result as a dictionary
            data2: Second extraction result as a dictionary

        Returns:
            float: Agreement ratio between 0.0 (no agreement) and 1.0 (full agreement)
        """
        # Key fields to compare
        compare_fields = [
            "tax_year",
            "employee_ssn",
            "employer_ein",
            "wages",
            "federal_income_tax",
        ]

        agreements = 0
        total = 0

        for field in compare_fields:
            if field in data1 and field in data2:
                total += 1
                val1 = data1[field]
                val2 = data2[field]

                # For numeric fields, check if within 5% tolerance
                if isinstance(val1, (int, float, Decimal)) and isinstance(
                    val2, (int, float, Decimal)
                ):
                    val1_dec = Decimal(str(val1))
                    val2_dec = Decimal(str(val2))
                    if val1_dec == val2_dec:
                        agreements += 1
                    elif val1_dec != 0:
                        diff_ratio = abs(val1_dec - val2_dec) / val1_dec
                        if diff_ratio < Decimal("0.05"):  # 5% tolerance
                            agreements += 0.5  # Partial credit
                # For strings, check exact match
                elif str(val1) == str(val2):
                    agreements += 1

        return agreements / total if total > 0 else 0.0

    def _parse_w2_text(
        self,
        text: str,
        source_file: Path | None = None,
        tax_year: int | None = None,
        pdf_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Parse W2 form data from extracted text using regex-based pattern matching.

        This method handles text from both extraction methods (pdfplumber text extraction
        and pytesseract OCR) and parses IRS Form W-2 data using a series of regex patterns
        and heuristics. The parsing is designed to be resilient to OCR errors and varying
        PDF layouts.

        Extraction Strategy:
            1. Text Cleanup: Normalize whitespace to handle multi-line text
            2. Tax Year: Multi-tier fallback (explicit param → text → OCR correction →
               filename → PDF metadata creation date)
            3. Identifiers: Extract SSN (employee) and EIN (employer) using fixed formats
            4. Monetary Amounts: Find all decimal amounts and map to W-2 boxes in order
               of appearance (wages, federal tax, SS wages, SS tax, Medicare wages, etc.)
            5. Employer Info: Extract name (with company suffix) and address (street pattern)
            6. Employee Info: Find name near SSN position using heuristics to filter out
               address components and company names
            7. State/Local Info: Extract state code, employer state ID, and amounts
            8. Optional Boxes: Parse box 12 codes (retirement contributions, etc.)

        OCR Error Handling:
            - Tax Year: Corrects common OCR errors like 'e024' → '2024'
            - Identifiers: Uses flexible patterns for SSN/EIN with spaces or hyphens
            - Names: Filters out false positives (address words, city names, ordinals)

        Text Preprocessing:
            Before parsing, W-2 PDFs are cropped to the top-left quadrant to avoid
            duplicate data from side-by-side employee/employer copies in a 2x2 grid layout.

        Args:
            text: Extracted text from PDF (already preprocessed/cropped)
            source_file: Source file path (used for tax year extraction from filename)
            tax_year: Explicit tax year (bypasses extraction if provided)
            pdf_metadata: PDF metadata dict (used for tax year derivation from creation date)

        Returns:
            dict: Parsed W2 data with keys matching W2FormSchema fields:
                - Core identifiers: tax_year, employee_ssn, employer_ein
                - Employee: employee_first_name, employee_last_name, employee_address
                - Employer: employer_name, employer_address
                - Wages/taxes: wages, federal_income_tax, social_security_wages,
                  social_security_tax, medicare_wages, medicare_tax
                - State: state_local_info (list of W2StateLocalInfo objects)
                - Optional: optional_boxes (W2OptionalBoxes with box_12_codes)
                - Defaults: control_number, tips, benefits, checkboxes (all None/False)

        Raises:
            ValueError: If required fields cannot be extracted (tax_year, SSN, EIN,
                       employee name, or minimum wage amounts)

        Example:
            >>> text = pdf_page.extract_text()
            >>> data = self._parse_w2_text(text, tax_year=2024)
            >>> data['employee_first_name']
            'Brandon'
            >>> data['wages']
            '319075.95'

        See Also:
            - W2FormSchema: Pydantic model that validates parsed data
            - _extract_using_text(): Calls this method with pdfplumber text
            - _extract_using_ocr(): Calls this method with pytesseract text
            - docs/w2-extraction-architecture.md: Detailed extraction documentation
        """
        data: dict[str, Any] = {}

        # Clean up text
        text = " ".join(text.split())

        # Extract tax year (multiple strategies)
        if tax_year:
            data["tax_year"] = tax_year
            logger.debug(f"Using provided tax year: {tax_year}")
        else:
            # Try text extraction - look for standard 4-digit year
            year_match = re.search(r"\b(202[0-9]|203[0-9])\b", text)
            if year_match:
                data["tax_year"] = int(year_match.group(1))
                logger.debug(f"Extracted tax year {data['tax_year']} from text")
            else:
                # Try OCR error correction - '2024' often becomes 'e024', 'o024', etc.
                # Pattern: letter/digit + '0' + two digits (24, 25, etc.)
                ocr_year_match = re.search(r"\b[eEoO0]0(2[0-9]|3[0-9])\b", text)
                if ocr_year_match:
                    # Extract last 2 digits and prepend '20'
                    year_suffix = ocr_year_match.group(1)
                    data["tax_year"] = int(f"20{year_suffix}")
                    logger.info(
                        f"Extracted tax year {data['tax_year']} from OCR text "
                        f"(corrected '{ocr_year_match.group(0)}' → '{data['tax_year']}')"
                    )

            if "tax_year" not in data and source_file:
                # Try filename
                year_from_filename = re.search(
                    r"\b(202[0-9]|203[0-9])\b", source_file.name
                )
                if year_from_filename:
                    data["tax_year"] = int(year_from_filename.group(1))
                    logger.debug(f"Extracted tax year {data['tax_year']} from filename")
                elif pdf_metadata and "CreationDate" in pdf_metadata:
                    # Derive from creation date (W2 created in year after tax year)
                    creation_date = pdf_metadata["CreationDate"]
                    creation_year_match = re.search(r"D:(\d{4})", creation_date)
                    if creation_year_match:
                        creation_year = int(creation_year_match.group(1))
                        # W2s are typically for previous tax year
                        data["tax_year"] = creation_year - 1
                        logger.info(
                            f"Derived tax year {data['tax_year']} from PDF creation date "
                            f"(created in {creation_year})"
                        )
                    else:
                        raise ValueError("Could not determine tax year")
                else:
                    raise ValueError(
                        "Could not determine tax year from text, filename, or metadata"
                    )
            elif "tax_year" not in data:
                raise ValueError("Could not determine tax year from text or OCR")

        # Extract SSN
        ssn_match = re.search(r"\b(\d{3}[-\s]?\d{2}[-\s]?\d{4})\b", text)
        if not ssn_match:
            raise ValueError("Could not extract employee SSN")
        data["employee_ssn"] = ssn_match.group(1).replace(" ", "-")

        # Extract EIN
        ein_match = re.search(r"\b(\d{2}[-\s]?\d{7})\b", text)
        if not ein_match:
            raise ValueError("Could not extract employer EIN")
        data["employer_ein"] = ein_match.group(1).replace(" ", "-")

        # Extract monetary amounts
        amounts = re.findall(r"\b\d{1,7}\.\d{2}\b", text)
        if len(amounts) < 2:
            raise ValueError("Could not extract wage and tax amounts")

        data["wages"] = amounts[0]
        data["federal_income_tax"] = amounts[1]
        data["social_security_wages"] = amounts[2] if len(amounts) > 2 else amounts[0]
        data["social_security_tax"] = amounts[3] if len(amounts) > 3 else None
        data["medicare_wages"] = amounts[4] if len(amounts) > 4 else amounts[0]
        data["medicare_tax"] = amounts[5] if len(amounts) > 5 else None

        # Extract employer name
        employer_match = re.search(
            r"([A-Z][A-Za-z\s,\.]+(?:Inc|LLC|Corp|Corporation|Company|Co)\.?)",
            text,
        )
        if employer_match:
            data["employer_name"] = employer_match.group(1).strip()
        else:
            data["employer_name"] = "Unknown Employer"

        # Extract addresses
        all_addresses = re.findall(
            r"(\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Court|Ct|Lane|Ln|Way|Boulevard|Blvd)[^,]*,?\s*(?:[A-Za-z\s]+,\s*)?[A-Z]{2}\s+\d{5})",
            text,
        )
        if all_addresses:
            data["employer_address"] = all_addresses[0].strip()
            if len(all_addresses) > 1:
                data["employee_address"] = all_addresses[1].strip()
            else:
                data["employee_address"] = None
        else:
            data["employer_address"] = None
            data["employee_address"] = None

        # Extract employee name
        # Strategy: Employee SSN and name appear together on W2
        # Find names near the employee SSN (within ~200 characters after)
        ssn_pos = text.find(data["employee_ssn"].replace("-", ""))
        if ssn_pos == -1:
            ssn_pos = text.find(data["employee_ssn"])

        # Look for names in the vicinity of the SSN (after employer section)
        search_start = max(0, ssn_pos)
        search_end = min(len(text), ssn_pos + 500)
        name_search_region = text[search_start:search_end]

        # Find all potential names (two capitalized words)
        name_pattern = r"\b([A-Z][a-z]+)\s+([A-Z][a-z]+)\b"
        potential_names: list[tuple[str, str]] = []

        for match in re.finditer(name_pattern, name_search_region):
            first = match.group(1)
            last = match.group(2)

            # Skip if contains company indicators
            if any(
                word in match.group(0)
                for word in ["Inc", "LLC", "Corp", "Company", "Co"]
            ):
                continue

            # Skip common address components (as first or last name)
            address_words = [
                "Street",
                "Avenue",
                "Road",
                "Drive",
                "Court",
                "Lane",
                "Way",
                "Boulevard",
                "Floor",
                "Suite",
                "Building",
            ]
            if first in address_words or last in address_words:
                continue

            # Skip city + state patterns (e.g., "San Francisco")
            if first in ["San", "Los", "New", "Fort", "Port", "Saint", "Santa"]:
                continue

            # Skip ordinal/direction patterns
            ordinals = [
                "First",
                "Second",
                "Third",
                "Fourth",
                "Fifth",
                "Sixth",
                "Seventh",
                "Eighth",
                "Ninth",
                "Tenth",
            ]
            directions = ["North", "South", "East", "West"]
            if first in ordinals + directions:
                continue

            potential_names.append((first, last))

        if potential_names:
            # Use the first valid name found near the SSN
            data["employee_first_name"] = potential_names[0][0]
            data["employee_last_name"] = potential_names[0][1]
        else:
            raise ValueError("Could not extract employee name")

        # Extract state info
        state_match = re.search(
            r"\b([A-Z]{2})\s+([A-Z0-9]+)\s+(\d+\.\d{2})\s+(\d+\.\d{2})\b", text
        )
        if state_match:
            state_info = W2StateLocalInfo(  # type: ignore[reportCallIssue] - optional fields have defaults
                state=state_match.group(1),
                employer_state_id=state_match.group(2),
                state_wages=state_match.group(3),
                state_income_tax=state_match.group(4),
            )
            data["state_local_info"] = [state_info]  # type: ignore[reportUnknownMemberType] - list append is valid
        else:
            data["state_local_info"] = []

        # Extract box 12 codes
        box_12_pattern = re.compile(r"\b([A-Z]{1,2})\s+(\d+\.\d{2})\b")
        box_12_matches = box_12_pattern.findall(text)
        if box_12_matches:
            valid_codes = {}
            for code, amount in box_12_matches:
                if len(code) == 1 or code in ["DD", "EE", "FF", "GG", "HH"]:
                    valid_codes[code] = amount

            if valid_codes:
                data["optional_boxes"] = W2OptionalBoxes(box_12_codes=valid_codes)  # type: ignore[reportCallIssue] - optional field has default

        # Set defaults
        data.setdefault("control_number", None)
        data.setdefault("social_security_tips", None)
        data.setdefault("allocated_tips", None)
        data.setdefault("dependent_care_benefits", None)
        data.setdefault("nonqualified_plans", None)
        data.setdefault("is_statutory_employee", False)
        data.setdefault("is_retirement_plan", False)
        data.setdefault("is_third_party_sick_pay", False)

        return data

    def _schema_to_dataframe_dict(
        self, w2_schema: W2FormSchema, source_file: str, extraction_timestamp: datetime
    ) -> dict[str, Any]:
        """Convert W2 schema to dictionary format for DataFrame.

        Args:
            w2_schema: Validated W2 schema
            source_file: Source file path
            extraction_timestamp: Timestamp of extraction

        Returns:
            dict: DataFrame-ready dictionary
        """
        return {
            "tax_year": w2_schema.tax_year,
            "employee_ssn": w2_schema.employee_ssn,
            "employee_first_name": w2_schema.employee_first_name,
            "employee_last_name": w2_schema.employee_last_name,
            "employee_address": w2_schema.employee_address,
            "employer_ein": w2_schema.employer_ein,
            "employer_name": w2_schema.employer_name,
            "employer_address": w2_schema.employer_address,
            "control_number": w2_schema.control_number,
            "wages": float(w2_schema.wages),
            "federal_income_tax": float(w2_schema.federal_income_tax),
            "social_security_wages": float(w2_schema.social_security_wages)
            if w2_schema.social_security_wages
            else None,
            "social_security_tax": float(w2_schema.social_security_tax)
            if w2_schema.social_security_tax
            else None,
            "medicare_wages": float(w2_schema.medicare_wages)
            if w2_schema.medicare_wages
            else None,
            "medicare_tax": float(w2_schema.medicare_tax)
            if w2_schema.medicare_tax
            else None,
            "social_security_tips": float(w2_schema.social_security_tips)
            if w2_schema.social_security_tips
            else None,
            "allocated_tips": float(w2_schema.allocated_tips)
            if w2_schema.allocated_tips
            else None,
            "dependent_care_benefits": float(w2_schema.dependent_care_benefits)
            if w2_schema.dependent_care_benefits
            else None,
            "nonqualified_plans": float(w2_schema.nonqualified_plans)
            if w2_schema.nonqualified_plans
            else None,
            "is_statutory_employee": w2_schema.is_statutory_employee,
            "is_retirement_plan": w2_schema.is_retirement_plan,
            "is_third_party_sick_pay": w2_schema.is_third_party_sick_pay,
            "state_local_info": json.dumps([
                info.model_dump(exclude_none=True, mode="json")
                for info in w2_schema.state_local_info
            ])
            if w2_schema.state_local_info
            else None,
            "optional_boxes": json.dumps(
                w2_schema.optional_boxes.model_dump(exclude_none=True, mode="json")
            )
            if w2_schema.optional_boxes
            else None,
            "source_file": source_file,
            "extracted_at": extraction_timestamp.isoformat(),
        }

    def _save_raw_data(self, df: pl.DataFrame, source_file: Path) -> None:
        """Save extracted W2 data to parquet file.

        Creates a directory structure like:
            data/raw/w2/extracted/<filename>/
                w2_form.parquet

        Args:
            df: DataFrame containing W2 data
            source_file: Original source file path
        """
        assert self.config.raw_data_path is not None  # noqa: S101 - Set in __init__, safe for type narrowing
        file_stem = source_file.stem
        extraction_dir = self.config.raw_data_path / "extracted" / file_stem
        extraction_dir.mkdir(parents=True, exist_ok=True)

        output_path = extraction_dir / "w2_form.parquet"
        df.write_parquet(output_path)
        logger.info(f"Saved W2 data ({len(df)} rows) to {output_path}")


def extract_w2_file(file_path: Path | str, tax_year: int | None = None) -> pl.DataFrame:
    """Convenience function to extract W2 data from a PDF file.

    Args:
        file_path: Path to the W2 PDF file
        tax_year: Optional tax year (e.g., 2024)

    Returns:
        pl.DataFrame: DataFrame containing extracted W2 data
    """
    extractor = W2Extractor()
    return extractor.extract_from_file(Path(file_path), tax_year)
