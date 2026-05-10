# src/moneybin/services/tax_service.py
"""Tax data service.

Business logic for tax document retrieval (W-2 forms). PII fields
(SSN, EIN) are excluded from all results. Consumed by both MCP tools
and CLI commands.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from moneybin.database import Database
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.tables import W2_FORMS

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class W2Summary:
    """W-2 form data with PII fields excluded."""

    tax_year: int
    employer_name: str
    wages: Decimal
    federal_income_tax: Decimal
    social_security_wages: Decimal | None
    social_security_tax: Decimal | None
    medicare_wages: Decimal | None
    medicare_tax: Decimal | None
    state_local_info: Any  # JSON — list of state/local tax entries

    def to_dict(self) -> dict[str, Any]:
        """Convert to a plain dict for JSON serialization."""
        d: dict[str, Any] = {
            "tax_year": self.tax_year,
            "employer_name": self.employer_name,
            "wages": self.wages,
            "federal_income_tax": self.federal_income_tax,
        }
        if self.social_security_wages is not None:
            d["social_security_wages"] = self.social_security_wages
        if self.social_security_tax is not None:
            d["social_security_tax"] = self.social_security_tax
        if self.medicare_wages is not None:
            d["medicare_wages"] = self.medicare_wages
        if self.medicare_tax is not None:
            d["medicare_tax"] = self.medicare_tax
        if self.state_local_info is not None:
            d["state_local_info"] = self.state_local_info
        return d


@dataclass(slots=True)
class W2Result:
    """Result of W-2 query."""

    forms: list[W2Summary]

    def to_envelope(self) -> ResponseEnvelope:
        """Build a ResponseEnvelope for MCP/CLI output."""
        return build_envelope(
            data=[f.to_dict() for f in self.forms],
            sensitivity="high",
            actions=[
                "Use reports_spending_get for spending overview",
            ],
        )


class TaxService:
    """Tax document operations.

    All methods return typed dataclasses with a ``to_envelope()`` method.
    PII fields (employee_ssn, employer_ein) are never included in results.
    """

    def __init__(self, db: Database) -> None:
        """Initialize TaxService with an open Database connection."""
        self._db = db

    def w2(self, tax_year: int | None = None) -> W2Result:
        """Retrieve W-2 form data.

        Note: employee_ssn and employer_ein are never returned — they
        are PII fields excluded at the query level.

        Args:
            tax_year: Filter to a specific tax year. Returns all years
                when None.

        Returns:
            W2Result with W-2 summaries (no PII).
        """
        conditions: list[str] = []
        params: list[object] = []

        if tax_year is not None:
            conditions.append("tax_year = ?")
            params.append(tax_year)

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        sql = f"""
            SELECT
                tax_year,
                employer_name,
                wages,
                federal_income_tax,
                social_security_wages,
                social_security_tax,
                medicare_wages,
                medicare_tax,
                state_local_info
            FROM {W2_FORMS.full_name}
            {where}
            ORDER BY tax_year DESC, employer_name
        """

        result = self._db.execute(sql, params)
        rows = result.fetchall()

        forms = [
            W2Summary(
                tax_year=int(row[0]),
                employer_name=str(row[1]),
                wages=Decimal(str(row[2])),
                federal_income_tax=Decimal(str(row[3])),
                social_security_wages=(
                    Decimal(str(row[4])) if row[4] is not None else None
                ),
                social_security_tax=(
                    Decimal(str(row[5])) if row[5] is not None else None
                ),
                medicare_wages=(Decimal(str(row[6])) if row[6] is not None else None),
                medicare_tax=(Decimal(str(row[7])) if row[7] is not None else None),
                state_local_info=row[8],
            )
            for row in rows
        ]

        logger.info(f"Retrieved {len(forms)} W-2 forms")
        return W2Result(forms=forms)
