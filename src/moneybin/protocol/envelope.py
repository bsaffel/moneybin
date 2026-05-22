"""Cross-transport response envelope.

Every MCP tool and every CLI command with ``--output json`` returns this
shape: ``{summary, data, actions}``. A future HTTP/FastAPI surface will
use the same envelope. The shape gives consumers consistent metadata
(counts, truncation, sensitivity, currency) and contextual next-step hints.

See ``mcp-architecture.md`` section 4 for design rationale.
"""

from __future__ import annotations

import dataclasses
import json
from dataclasses import dataclass, field, is_dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal, cast

from pydantic import BaseModel

from moneybin.errors import RecoveryAction, UserError


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
    sensitivity: Literal["low", "medium", "high", "critical"] = "low"
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


class PayloadEncoder(json.JSONEncoder):
    """JSON encoder for envelope payloads.

    Extends the original Decimal-to-float semantics to also handle typed
    dataclass and Pydantic model payloads so ``to_json()`` works on both
    bare-dict and typed-payload envelopes.

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
        """Serialize types ``json`` can't handle natively (Decimal, dataclass, BaseModel)."""
        # Existing: Decimal → float
        if isinstance(o, Decimal):
            return float(o)
        # New: dataclass instances → asdict (recurses through nested dataclasses)
        if is_dataclass(o) and not isinstance(o, type):
            return dataclasses.asdict(o)
        # New: Pydantic v2 models → model_dump. isinstance(BaseModel) — NOT a
        # duck-type `hasattr("model_dump")` — because MagicMock auto-generates
        # any non-dunder attribute. With the duck check, a mock returned from a
        # mocked service flowed into model_dump() which returned ANOTHER mock,
        # and the JSON encoder looped allocating ~10 KiB per pass until the
        # process OOMed. See test_payload_encoder_does_not_chain_mock_model_dump.
        if isinstance(o, BaseModel):
            try:
                return o.model_dump()
            except Exception:  # noqa: BLE001,S110 — fall through to str()
                pass
        # Existing fallback: str(o) for datetime, UUID, Enum, etc.
        try:
            return super().default(o)
        except TypeError:
            return str(o)


@dataclass(slots=True)
class ResponseEnvelope[T]:
    """Standard response shape for all MCP tools.

    Generic over the payload type ``T`` (a typed dataclass, Pydantic
    model, TypedDict instance, or — for back-compat — a plain dict or
    list of dicts). ``T`` carries field-level
    ``Annotated[..., DataClass]`` metadata that the privacy middleware
    reads to apply redaction.

    Sections:
    - ``summary``: metadata for the AI (counts, truncation, sensitivity)
    - ``data``: the payload — typed object or bare dict/list
    - ``actions``: contextual next-step hints
    - ``error``: populated when the tool failed with a classified user error;
      ``data`` is empty in this case
    - ``next_cursor``: opaque pagination token when more results are available
    - ``recovery_actions``: structured actions an agent can execute to fix
      a failure; carried from the UserError when present
    """

    summary: SummaryMeta
    data: T
    actions: list[str] = field(default_factory=list)
    error: UserError | None = None
    next_cursor: str | None = None
    recovery_actions: list[RecoveryAction] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to a plain dict suitable for JSON serialization.

        Typed payloads (dataclass, Pydantic) are recursively serialized
        via ``dataclasses.asdict`` / ``model_dump``; bare dicts/lists pass
        through unchanged.
        """
        data_serialized: Any
        if is_dataclass(self.data) and not isinstance(self.data, type):
            data_serialized = dataclasses.asdict(self.data)
        elif isinstance(self.data, BaseModel):
            # isinstance(BaseModel), not duck-type — see PayloadEncoder.default
            # comment for the MagicMock-chain leak this guards against.
            data_serialized = self.data.model_dump()
        else:
            data_serialized = self.data
        d: dict[str, Any] = {
            "status": "error" if self.error is not None else "ok",
            "summary": self.summary.to_dict(),
            "data": data_serialized,
            "actions": self.actions,
        }
        # Effective recovery_actions: the envelope-level field is the canonical
        # wire location, but fall back to the error's own list when the
        # envelope field wasn't populated — e.g. a caller building
        # ResponseEnvelope(error=UserError(..., recovery_actions=[...]))
        # directly, bypassing build_error_envelope. Without this fallback the
        # actions would vanish (nested error copy is stripped below, top-level
        # not emitted). An explicit suppression via recovery_actions=[] is a
        # non-None empty list, so it is honored — not overridden by the error.
        effective_recovery = self.recovery_actions
        if (
            effective_recovery is None
            and self.error is not None
            and self.error.recovery_actions is not None
        ):
            effective_recovery = self.error.recovery_actions

        if self.error is not None:
            # Strip recovery_actions from the nested error dict: the envelope
            # top-level field (emitted below from effective_recovery) is the
            # single canonical wire location. Without this strip, an explicit
            # suppression via recovery_actions=[] would still leak the
            # original actions through error.to_dict() — defeating the
            # redaction use case. Direct UserError.to_dict() callers (logging,
            # debugging) still see recovery_actions; only the envelope-nested
            # form drops the field.
            err_dict = self.error.to_dict()
            err_dict.pop("recovery_actions", None)
            d["error"] = err_dict
        if self.next_cursor is not None:
            d["next_cursor"] = self.next_cursor
        if effective_recovery is not None:
            # Coerce plain dicts defensively: callers SHOULD pass
            # RecoveryAction instances (the type annotation says so), but a
            # dict slipping in (e.g., from deserialized JSON) would otherwise
            # AttributeError here and convert a classified UserError into an
            # internal failure at the wire boundary.
            d["recovery_actions"] = [
                ra if isinstance(ra, dict) else ra.model_dump()
                for ra in effective_recovery
            ]
        return d

    def to_json(self) -> str:
        """Serialize to JSON string via PayloadEncoder.

        Uses ``PayloadEncoder`` (which handles Decimal → float, dataclass
        → dict, and falls back to str for other non-serializable types). Do
        NOT pass ``default=`` to ``json.dumps`` alongside ``cls=``:
        ``default=`` replaces the encoder's ``default()`` and silently breaks
        the Decimal-to-number conversion.
        """
        return json.dumps(self.to_dict(), cls=PayloadEncoder)


def build_envelope(
    *,
    data: Any,
    sensitivity: Literal["low", "medium", "high", "critical"] = "low",
    total_count: int | None = None,
    next_cursor: str | None = None,
    period: str | None = None,
    display_currency: str = "USD",
    actions: list[str] | None = None,
    degraded: bool = False,
    degraded_reason: str | None = None,
) -> ResponseEnvelope[Any]:
    """Build a ResponseEnvelope with computed metadata.

    ``data`` may be a typed dataclass / Pydantic model / TypedDict
    (preferred — carries privacy classification metadata), or a bare
    dict / list of dicts (back-compat). When used inside ``@mcp_tool``
    functions, the decorator derives sensitivity from the return type
    annotation — the ``sensitivity`` parameter here is informational
    metadata stored in ``summary.sensitivity``. Defaults to ``"low"``
    so callers can omit it when the decorator owns enforcement.

    Args:
        data: The payload — typed dataclass/model, list of records, or a
            write-result dict.
        sensitivity: Sensitivity tier stored in ``summary.sensitivity``.
            Defaults to ``"low"``; the ``@mcp_tool`` decorator derives the
            effective tier from the return type and governs redaction.
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
    data_any: Any = data  # widen to Any to avoid union-narrowing issues below
    if isinstance(data_any, list):
        returned = len(cast(list[Any], data_any))
    elif is_dataclass(data_any) and not isinstance(data_any, type):
        returned = _count_typed_payload(data_any)
    elif isinstance(data_any, BaseModel):
        returned = _count_pydantic_payload(data_any)
    else:
        returned = 1

    actual_total = total_count if total_count is not None else returned
    # For write-result payloads (aggregate dataclasses with no primary row list),
    # _count_typed_payload returns 0 from an empty error_details field.  When the
    # caller explicitly supplied total_count, treat all inputs as "returned".
    if returned == 0 and total_count is not None and total_count > 0:
        returned = actual_total
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
        data=cast(Any, data_any),
        actions=actions or [],
        next_cursor=next_cursor,
    )


# Auxiliary list fields that commonly accompany single-object write results
# but are NOT the primary row collection. Without this filter the first-list
# heuristic returns len(warnings)=0 for a successful write whose only list is
# an empty diagnostic field, and propagates that into summary.returned_count
# and the privacy log's row_count.
_AUXILIARY_LIST_FIELDS = frozenset({
    "warnings",
    "errors",
    "error_details",
    "unmapped_columns",
    "flagged_fields",
})


def _count_typed_payload(data: Any) -> int:
    """For a typed payload, return the row count if a primary list field exists, else 1.

    - Skips auxiliary diagnostic list fields (see ``_AUXILIARY_LIST_FIELDS``)
      so write-result payloads like ``AccountSettingsPayload(warnings=[])``
      report ``returned_count=1`` instead of ``0``.
    - Falls back to ``1`` when the payload has more than one non-auxiliary
      list field — these are aggregate result objects (e.g.
      ``ImportInboxSyncPayload`` with ``processed``/``failed``/``skipped``/
      ``ignored``), not row collections, so no single list represents the
      "returned" count.
    """
    return _count_primary_lists(data, [f.name for f in dataclasses.fields(data)])


def _count_pydantic_payload(data: BaseModel) -> int:
    """Pydantic equivalent of ``_count_typed_payload``.

    A Pydantic wrapper with a single primary list field (e.g. a payload whose
    one list is the result set) reports that list's length; an aggregate model
    with zero or multiple non-auxiliary lists reports 1. Without this, every
    ``BaseModel`` payload fell through to ``returned=1``, misreporting
    ``summary.returned_count`` / ``has_more`` and the privacy log's row_count.
    """
    return _count_primary_lists(data, list(type(data).model_fields))


def _count_primary_lists(data: Any, field_names: list[str]) -> int:
    """Return the length of the sole non-auxiliary list field, else 1.

    Shared by the dataclass and Pydantic counters. Auxiliary diagnostic lists
    (see ``_AUXILIARY_LIST_FIELDS``) are skipped; more than one remaining list
    means an aggregate result object with no single "returned" collection.
    """
    primary_lists: list[list[Any]] = []
    for name in field_names:
        if name in _AUXILIARY_LIST_FIELDS:
            continue
        v: Any = getattr(data, name)
        if isinstance(v, list):
            primary_lists.append(cast(list[Any], v))
    if len(primary_lists) == 1:
        return len(primary_lists[0])
    return 1


def not_implemented_envelope(
    *,
    action: str,
    spec: str,
    actions: list[str] | None = None,
) -> ResponseEnvelope[list[dict[str, Any]]]:
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
    sensitivity: Literal["low", "medium", "high", "critical"] = "low",
    actions: list[str] | None = None,
    recovery_actions: list[RecoveryAction] | None = None,
) -> ResponseEnvelope[Any]:
    """Build a ResponseEnvelope carrying a classified user error.

    Typed as ``ResponseEnvelope[Any]`` (``data`` is always an empty list) so an
    error early-return unifies with any ``-> ResponseEnvelope[T]`` tool
    signature without a per-call-site ``# type: ignore[return-value]``.

    ``data`` is an empty list — the ``error`` field is the canonical signal
    that the tool failed. Sensitivity defaults to ``low`` because error
    messages must not leak row-level data. ``actions`` preserves any
    caller-provided next-step hints (e.g. CLI fallbacks on stub tools).

    ``recovery_actions`` precedence:

    - An explicit list (including ``[]``) overrides ``error.recovery_actions``.
    - An empty list ``[]`` is honored and reaches the envelope — meaning
      "explicitly no recovery available; agent must escalate to the user".
    - ``None`` (the default) means "no opinion; use the error's actions".
      Callers who want to clear recovery_actions for a specific surface
      (e.g., redacting before sending to a low-trust client) MUST pass
      ``[]``, not ``None`` — None is reserved for "fall through to the
      error's value".
    """
    # Resolve recovery_actions: explicit list (including empty) overrides;
    # None means "no opinion, fall back to error.recovery_actions" per the
    # contract documented above.
    if recovery_actions is None and error.recovery_actions is not None:
        recovery_actions = error.recovery_actions

    summary = SummaryMeta(
        total_count=0,
        returned_count=0,
        has_more=False,
        sensitivity=sensitivity,
    )
    return ResponseEnvelope(
        summary=summary,
        data=[],
        actions=actions or [],
        error=error,
        recovery_actions=recovery_actions,
    )
