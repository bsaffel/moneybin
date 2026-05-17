"""Cross-transport response envelope.

Every MCP tool and every CLI command with ``--output json`` returns this
shape: ``{summary, data, actions}``. A future HTTP/FastAPI surface will
use the same envelope. The shape gives consumers consistent metadata
(counts, truncation, sensitivity, currency) and contextual next-step hints.

See ``mcp-architecture.md`` section 4 for design rationale.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal

from moneybin.errors import UserError


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
    sensitivity: Literal["low", "medium", "high"] = "low"
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
    """JSON encoder that serializes Decimal as a JSON number.

    MoneyBin holds money as `Decimal` internally for precision-safe arithmetic;
    on the wire we emit JSON numbers so agents and JSON tooling consume them
    as numerics, not strings that need re-parsing. Float64 carries ~15.95
    significant digits — comfortably wider than realistic personal-finance
    magnitudes (balances < $10^{10}, transaction amounts < $10^{8}, prices /
    quantities / FX rates well within `DECIMAL(18,8)`). A `DECIMAL(18,2)`
    value above ~$10^{13} would round on the way out; the wire contract
    documents money types up to that cap.

    Catch-all for non-serializable types (datetime, UUID, etc.) falls back
    to `str(o)` here rather than via `json.dumps(..., default=str)` because
    passing both `cls=` and `default=` to `json.dumps` causes `default=` to
    REPLACE the encoder's `default()` method — silently dropping the
    Decimal-to-float conversion.
    """

    def default(self, o: object) -> Any:
        if isinstance(o, Decimal):
            return float(o)
        try:
            return super().default(o)
        except TypeError:
            return str(o)


@dataclass(slots=True)
class ResponseEnvelope:
    """Standard response shape for all MCP tools.

    Sections:
    - ``summary``: metadata for the AI (counts, truncation, sensitivity)
    - ``data``: the payload (list of objects or single result dict)
    - ``actions``: contextual next-step hints
    - ``error``: populated when the tool failed with a classified user error;
      ``data`` is empty in this case
    - ``next_cursor``: opaque pagination token when more results are available
    """

    summary: SummaryMeta
    data: list[dict[str, Any]] | dict[str, Any]
    actions: list[str] = field(default_factory=list)
    error: UserError | None = None
    next_cursor: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to a plain dict suitable for JSON serialization."""
        d: dict[str, Any] = {
            "status": "error" if self.error is not None else "ok",
            "summary": self.summary.to_dict(),
            "data": self.data,
            "actions": self.actions,
        }
        if self.error is not None:
            d["error"] = self.error.to_dict()
        if self.next_cursor is not None:
            d["next_cursor"] = self.next_cursor
        return d

    def to_json(self) -> str:
        """Serialize to JSON string.

        Uses ``_DecimalEncoder`` (which handles Decimal → float and falls
        back to str for other non-serializable types). Do NOT pass
        ``default=`` to ``json.dumps`` alongside ``cls=``: ``default=``
        replaces the encoder's ``default()`` and silently breaks the
        Decimal-to-number conversion.
        """
        return json.dumps(self.to_dict(), cls=_DecimalEncoder)


def build_envelope(
    *,
    data: list[dict[str, Any]] | dict[str, Any],
    sensitivity: Literal["low", "medium", "high"],
    total_count: int | None = None,
    next_cursor: str | None = None,
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
        next_cursor: Opaque pagination token. When provided, ``summary.has_more``
            is forced to ``True`` regardless of count comparison.
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
    has_more = next_cursor is not None or actual_total > returned

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
        next_cursor=next_cursor,
    )


def not_implemented_envelope(
    *,
    action: str,
    spec: str,
    actions: list[str] | None = None,
) -> ResponseEnvelope:
    """Build a stub envelope for taxonomy tools whose body isn't implemented yet.

    Used by tool surfaces (e.g., sync_*, transform_*) that exist for v2
    discoverability but whose business logic is owned by a downstream spec.
    Returns status="error" with code="not_implemented" so agents can branch
    on the top-level status field consistently.
    """
    return build_error_envelope(
        error=UserError(
            f"{action} is not yet implemented",
            code="not_implemented",
            hint=f"See {spec} for the design",
            details={"spec": spec},
        ),
        actions=actions,
    )


def build_error_envelope(
    *,
    error: UserError,
    sensitivity: Literal["low", "medium", "high"] = "low",
    actions: list[str] | None = None,
) -> ResponseEnvelope:
    """Build a ResponseEnvelope carrying a classified user error.

    ``data`` is an empty list — the ``error`` field is the canonical signal
    that the tool failed. Sensitivity defaults to ``low`` because error
    messages must not leak row-level data. ``actions`` preserves any
    caller-provided next-step hints (e.g. CLI fallbacks on stub tools).
    """
    summary = SummaryMeta(
        total_count=0,
        returned_count=0,
        has_more=False,
        sensitivity=sensitivity,
    )
    return ResponseEnvelope(
        summary=summary, data=[], actions=actions or [], error=error
    )
