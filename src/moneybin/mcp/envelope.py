# src/moneybin/mcp/envelope.py
"""Response envelope for MCP tools and CLI --output json.

Every MCP tool and every CLI command with ``--output json`` returns this
shape: ``{summary, data, actions}``. The envelope gives AI consumers
consistent metadata (counts, truncation, sensitivity, currency) and
contextual next-step hints.

See ``mcp-architecture.md`` section 4 for design rationale.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Any


class DetailLevel(StrEnum):
    """Detail level for tool responses.

    Controls response verbosity:
    - ``summary``: aggregates only (always tier-1 safe)
    - ``standard``: default view
    - ``full``: every available field
    """

    SUMMARY = "summary"
    STANDARD = "standard"
    FULL = "full"


@dataclass(frozen=True, slots=True)
class SummaryMeta:
    """Metadata section of the response envelope.

    Provides AI consumers with context about the response: counts,
    whether results are truncated, sensitivity tier, and currency.
    """

    total_count: int
    returned_count: int
    has_more: bool = False
    period: str | None = None
    sensitivity: str = "low"
    display_currency: str = "USD"
    degraded: bool = False
    degraded_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict, omitting None fields and False degraded."""
        d: dict[str, Any] = {
            "total_count": self.total_count,
            "returned_count": self.returned_count,
            "has_more": self.has_more,
            "sensitivity": self.sensitivity,
            "display_currency": self.display_currency,
        }
        if self.period is not None:
            d["period"] = self.period
        if self.degraded:
            d["degraded"] = True
            if self.degraded_reason:
                d["degraded_reason"] = self.degraded_reason
        return d


class _DecimalEncoder(json.JSONEncoder):
    """JSON encoder that serializes Decimal as string to avoid float imprecision."""

    def default(self, o: object) -> Any:
        if isinstance(o, Decimal):
            return str(o)
        return super().default(o)


@dataclass(slots=True)
class ResponseEnvelope:
    """Standard response shape for all MCP tools.

    Three sections:
    - ``summary``: metadata for the AI (counts, truncation, sensitivity)
    - ``data``: the payload (list of objects or single result dict)
    - ``actions``: contextual next-step hints
    """

    summary: SummaryMeta
    data: list[dict[str, Any]] | dict[str, Any]
    actions: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to a plain dict suitable for JSON serialization."""
        return {
            "summary": self.summary.to_dict(),
            "data": self.data,
            "actions": self.actions,
        }

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), cls=_DecimalEncoder, default=str)


def build_envelope(
    *,
    data: list[dict[str, Any]] | dict[str, Any],
    sensitivity: str,
    total_count: int | None = None,
    period: str | None = None,
    display_currency: str = "USD",
    actions: list[str] | None = None,
    degraded: bool = False,
    degraded_reason: str | None = None,
) -> ResponseEnvelope:
    """Build a ResponseEnvelope with computed metadata.

    Args:
        data: The payload — list of records or a write-result dict.
        sensitivity: Sensitivity tier of the response.
        total_count: Total matching records (if known and different from
            returned count). When None, inferred from data length.
        period: Human-readable period string (e.g., ``"2026-01 to 2026-04"``).
        display_currency: Currency for all amounts in the response.
        actions: Contextual next-step hints.
        degraded: Whether this is a degraded (no-consent) response.
        degraded_reason: Why the response is degraded.

    Returns:
        A fully populated ResponseEnvelope.
    """
    if isinstance(data, list):
        returned = len(data)
    else:
        returned = 1

    actual_total = total_count if total_count is not None else returned
    has_more = actual_total > returned

    summary = SummaryMeta(
        total_count=actual_total,
        returned_count=returned,
        has_more=has_more,
        period=period,
        sensitivity=sensitivity,
        display_currency=display_currency,
        degraded=degraded,
        degraded_reason=degraded_reason,
    )

    return ResponseEnvelope(
        summary=summary,
        data=data,
        actions=actions or [],
    )
