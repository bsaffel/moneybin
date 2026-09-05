"""Cross-transport response envelope.

Every MCP tool and every CLI command with ``--output json`` returns this
shape: ``{summary, data, actions}``. A future HTTP/FastAPI surface will
use the same envelope. The shape gives consumers consistent metadata
(counts, truncation, sensitivity, currency) and contextual next-step hints.

See ``mcp-architecture.md`` section 4 for design rationale.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, fields, is_dataclass, replace
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal, cast

from pydantic import BaseModel

from moneybin import error_codes
from moneybin.errors import ErrorDetail, RecoveryAction, UserError


def serialize_payload(value: Any) -> Any:
    """Recursively expose payload containers without copying scalar leaves."""
    if is_dataclass(value) and not isinstance(value, type):
        return {
            item.name: serialize_payload(getattr(value, item.name))
            for item in fields(value)
        }
    if isinstance(value, BaseModel):
        try:
            return serialize_payload(value.model_dump())
        except Exception:  # noqa: BLE001,S110  # preserve existing fallback contract
            return value
    if isinstance(value, Mapping):
        mapping = cast(Mapping[Any, Any], value)
        return {key: serialize_payload(item) for key, item in mapping.items()}
    if isinstance(value, (tuple, list)):
        return [serialize_payload(item) for item in value]
    return value


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


def resolve_display_currency(codes: Iterable[str | None]) -> str | None:
    """The one currency every row is denominated in, else ``None``.

    Rows are segmented per currency (multi-currency.md Requirement 5), so the
    envelope may only name a display currency when they agree on exactly one
    known one. An unknown (NULL) currency is its own segment and never resolves
    to whichever currency the other rows happen to carry — that would be the
    Requirement 5 blend, relabelled. No rows at all names nothing.

    Lives here rather than beside any one caller because ``display_currency`` is
    an envelope field: reports and the balance reads must answer it the same
    way, or the same data reads as two different currencies across surfaces.
    """
    seen = set(codes)
    if len(seen) != 1:
        return None
    only = next(iter(seen))
    return str(only) if only else None


class Unset:
    """Sentinel type distinguishing "caller said nothing" from "caller said None".

    ``None`` is a real answer for ``display_currency`` — it means the rows span
    more than one currency, or none is known. A caller that resolved that over a
    wider set than the returned page (the reports framework, which sees every
    matching row before truncation) must be able to state it and keep it, so
    silence needs its own value.
    """

    __slots__ = ()

    def __repr__(self) -> str:
        """Render as ``<unset>`` so a stray sentinel is obvious in a traceback."""
        return "<unset>"


UNSET = Unset()


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
    # Nullable: a response whose rows span more than one currency — or whose
    # currency is unknown — has no single display currency, and naming one
    # would contradict the rows (multi-currency.md Requirement 5). Null means
    # "read each row's currency_code", not "unset". The claim is scoped to the
    # rows in THIS response; when has_more is true, later pages may carry other
    # currencies. Defaults to unknown, never USD: a summary built without one
    # has not been told a currency, and inventing the author's home currency is
    # what mislabelled every non-USD balance before M1K.1.
    display_currency: str | None = None
    degraded: bool = False
    degraded_reason: str | None = None
    # The rates behind this response's converted amounts (multi-currency.md
    # Requirement 10). Set only when a conversion actually happened: amounts
    # already in their original currency were priced by nothing, and an empty
    # list would imply a conversion whose rate went unrecorded. Sits beside
    # display_currency because the two answer one question together — that
    # names the unit, this says how the unit was reached.
    applied_rates: list[dict[str, Any]] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict, omitting a null ``period`` and a false ``degraded``.

        ``display_currency`` is emitted unconditionally, null included: null is
        the answer "not one known currency", and a consumer that finds no key
        cannot tell that answer from a tool that never set one.
        ``applied_rates`` is the opposite case and is omitted when absent —
        "nothing was converted" and "converted at an unrecorded rate" must not
        serialize alike, so the key appears only when rates really were applied.
        """
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
        if self.applied_rates:
            d["applied_rates"] = self.applied_rates
        return d


class PayloadEncoder(json.JSONEncoder):
    """JSON encoder for envelope payloads.

    Extends the original Decimal-to-float semantics to also handle typed
    dataclass, Pydantic model, immutable mapping, and sequence payloads through
    one recursive field walker so ``to_json()`` works on both bare-dict and
    typed-payload envelopes without ``deepcopy``.

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
        converted = serialize_payload(o)
        if converted is not o:
            return converted
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
    - ``error``: populated when the tool failed; this field (mirrored by
      ``status``) is the canonical failure signal. Envelopes built by
      ``build_error_envelope`` carry an empty ``data``, but a tool MAY attach
      ``error`` to a payload-carrying envelope when the payload itself explains
      the failure — ``import_files`` keeps its per-file ``files[]`` results on
      an all-failed batch. Never infer "``data`` is empty" from ``error``.
    - ``next_cursor``: opaque pagination token when more results are available
    - ``recovery_actions``: structured actions an agent can execute to fix
      a failure; carried from the UserError when present
    """

    summary: SummaryMeta
    data: T
    actions: list[str] = field(default_factory=list)
    error: ErrorDetail | None = None
    next_cursor: str | None = None
    recovery_actions: list[RecoveryAction] | None = None
    # Derived in __post_init__ — see the method's docstring for the one value a
    # caller may declare.
    status: Literal["ok", "conflict", "error"] = "ok"
    # Internal observability only: per-call DataClass names for dynamic-SQL
    # tools, read by the @mcp_tool decorator to log accurate classes_returned.
    # NOT part of the wire contract — `to_dict()` omits it, and `to_dict()` is
    # what `_wire_result_adapter` sends, so it never reaches the MCP wire.
    classes_returned: list[str] | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        """Derive `status` from `error` so the two can never disagree.

        `status` is a real field rather than a `to_dict()` computation so that
        every consumer of the envelope — the wire, direct dataclass readers,
        and tests — sees one value from one source.

        `"conflict"` is the one value a caller may declare: an operation that
        ran to completion and deliberately changed nothing because live state
        disagrees with the request, and the response carries what disagreed.
        That is not a failure — `error` stays null, `data` is the conflict —
        so it cannot be spelled as an `ErrorDetail`. `error` still wins:
        anything genuinely broken reads as `"error"`, and every other supplied
        value normalizes to `"ok"`.
        """
        if self.error is not None:
            self.status = "error"
        elif self.status != "conflict":
            self.status = "ok"

    def with_error(self, error: ErrorDetail) -> ResponseEnvelope[T]:
        """Return a copy carrying `error`, with `status` re-derived.

        Use this instead of `dataclasses.replace(envelope, error=...)`.
        `replace` is typed `**changes: Any`, so it silently accepts a
        `UserError` — which then raises `AttributeError` inside `to_dict()`
        at serialization time, turning a structured partial-failure response
        into an empty one. This signature makes pyright reject that at the
        call site instead. Rebuilding (rather than assigning) is required
        regardless: `status` is derived in `__post_init__`.
        """
        return replace(self, error=error)  # pyright: ignore[reportUnknownArgumentType]

    def to_dict(self) -> dict[str, Any]:
        """Convert to a plain dict suitable for JSON serialization.

        Typed payloads and JSON containers are recursively converted without
        deep-copying scalar leaves.
        """
        data_serialized = serialize_payload(self.data)
        d: dict[str, Any] = {
            "status": self.status,
            "summary": self.summary.to_dict(),
            "data": data_serialized,
            "actions": self.actions,
        }
        if self.error is not None:
            d["error"] = self.error.model_dump(exclude_none=True)
        if self.next_cursor is not None:
            d["next_cursor"] = self.next_cursor
        if self.recovery_actions is not None:
            # Coerce plain dicts defensively: callers SHOULD pass
            # RecoveryAction instances (the type annotation says so), but a
            # dict slipping in (e.g., from deserialized JSON) would otherwise
            # AttributeError here and convert a classified UserError into an
            # internal failure at the wire boundary.
            d["recovery_actions"] = [
                ra if isinstance(ra, dict) else ra.model_dump()
                for ra in self.recovery_actions
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
    returned_count: int | None = None,
    next_cursor: str | None = None,
    period: str | None = None,
    display_currency: str | None | Unset = UNSET,
    actions: list[str] | None = None,
    degraded: bool = False,
    degraded_reason: str | None = None,
    recovery_actions: list[RecoveryAction] | None = None,
    classes_returned: list[str] | None = None,
    applied_rates: list[dict[str, Any]] | None = None,
    conflict: bool = False,
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
        returned_count: Explicit number of result rows returned. When provided,
            overrides payload-shape heuristics and must not exceed total_count.
        next_cursor: Opaque pagination token. When provided, ``summary.has_more``
            is forced to ``True`` regardless of count comparison.
        period: Human-readable period string (e.g., ``"2026-01 to 2026-04"``).
        display_currency: Currency for all amounts in this response; None when
            they span more than one currency, or the currency is unknown.
            Scoped to the returned rows — later pages may differ when
            has_more is true. Omit it and the payload's own ``currency_code``
            answers; pass it only to state a currency the payload cannot show
            — resolved over a wider set than the returned page, or held
            somewhere other than a ``currency_code`` field. Passing ``None``
            explicitly states "not one currency" and is preserved.
        actions: Contextual next-step hints.
        degraded: Whether the response is less than the caller asked for —
            aggregates in place of rows without consent, a section that could
            not be read, or a report whose stored classification is stale.
        degraded_reason: Which of those applies. Set it whenever ``degraded``
            is set: one flag carrying several meanings is only usable if the
            response says which one it means.
        recovery_actions: Structured actions an agent can execute to fix a
            partial-success failure (e.g. a best-effort step that crashed).
            The navigational ``actions`` field stays distinct — it answers
            "what next", not "how to fix what broke".
        applied_rates: The exchange rates behind this response's converted
            amounts, one entry per distinct pair and date
            (multi-currency.md Requirement 10). Pass it only when a conversion
            actually happened: omitted means nothing was converted, which is a
            different claim from "converted, rate unrecorded".
        classes_returned: Internal observability only — DataClass value strings
            for dynamic-SQL tools that self-classify per call (``dynamic_classification``
            mode). Read by the ``@mcp_tool`` decorator for privacy audit logging.
            NOT serialized to the wire: ``to_dict()`` never emits this field.
        conflict: The operation completed and deliberately changed nothing
            because live state disagrees with the request; ``data`` carries
            what disagreed. Sets ``status="conflict"``. Not a failure — pass
            an ``ErrorDetail`` for that instead.

    Returns:
        A fully populated ResponseEnvelope.
    """
    data_any: Any = data  # widen to Any to avoid union-narrowing issues below
    if returned_count is not None:
        if returned_count < 0:
            raise ValueError("returned_count must be non-negative")
        actual_total = total_count if total_count is not None else returned_count
        if actual_total < returned_count:
            raise ValueError(
                "total_count must be greater than or equal to returned_count"
            )
        returned = returned_count
    else:
        if isinstance(data_any, list):
            returned = len(cast(list[Any], data_any))
        elif is_dataclass(data_any) and not isinstance(data_any, type):
            returned = _count_typed_payload(data_any)
        elif isinstance(data_any, BaseModel):
            returned = _count_pydantic_payload(data_any)
        else:
            returned = 1

        actual_total = total_count if total_count is not None else returned
        # For write-result payloads (aggregate dataclasses with no primary row
        # list), _count_typed_payload returns 0 from an empty error_details
        # field. When the caller explicitly supplied total_count, treat all
        # inputs as "returned".
        if returned == 0 and total_count is not None and total_count > 0:
            returned = actual_total
    has_more = next_cursor is not None or actual_total > returned

    resolved_currency = (
        _derive_display_currency(data_any)
        if isinstance(display_currency, Unset)
        else display_currency
    )

    summary = SummaryMeta(
        total_count=actual_total,
        returned_count=returned,
        has_more=has_more,
        period=period,
        sensitivity=sensitivity,
        display_currency=resolved_currency,
        degraded=degraded,
        degraded_reason=degraded_reason,
        applied_rates=applied_rates,
    )

    return ResponseEnvelope(
        summary=summary,
        data=cast(Any, data_any),
        actions=actions or [],
        next_cursor=next_cursor,
        recovery_actions=recovery_actions,
        classes_returned=classes_returned,
        status="conflict" if conflict else "ok",
    )


# Auxiliary list fields that commonly accompany single-object write results
# but are NOT the primary row collection. Without this filter the first-list
# heuristic returns len(warnings)=0 for a successful write whose only list is
# an empty diagnostic field, and propagates that into summary.returned_count
# and the privacy log's row_count.
AUXILIARY_LIST_FIELDS = frozenset({
    "warnings",
    "errors",
    "error_details",
    "unmapped_columns",
    "flagged_fields",
    # Rate provenance describes the rows; it is not a second set of them. A
    # payload carrying both (`InvestmentHoldingsPayload`) has two lists, and the
    # heuristic answers "several, so neither" — reporting an unknown currency
    # and a count of 1 for a read that returned N priced positions. Empty on
    # almost every call, so the field's presence alone would break the count.
    "applied_rates",
    # The post-load refresh's four best-effort diagnostics, on the same
    # rationale: they report what the refresh *did to* the rows, not a second
    # set of rows. `GsheetPullPayload` carries all four beside its `pulls`
    # collection, so without them the heuristic saw five lists and reported
    # `returned_count=1` for a pull that returned N per-connection outcomes.
    "identity_errors",
    "rate_pairs_failed",
    "rate_pairs_unsupported",
    "rate_pairs_discarded",
    # `RefreshRunPayload`'s record of the self-heal recipes that ran. Listed
    # with the four above rather than after them: it is the only other list on
    # that payload, so excluding the four alone would promote it to "the" row
    # collection and report `returned_count=0` for a clean refresh that healed
    # nothing. `refresh_run` returns one pipeline outcome, not N recipes.
    "self_heal_actions",
    # `SyncPullPayload`'s warning that some accounts carry both manual and Plaid
    # investment history. Same rationale again: it describes a condition
    # affecting the rows the pull returned, not a second set of them. It sits
    # beside `identity_errors` and the three `rate_pairs_*` lists on that
    # payload; without it the heuristic saw two lists and reported
    # `returned_count=1` for a pull covering N institutions.
    "investment_source_overlap_accounts",
})


_CURRENCY_FIELD = "currency_code"


def _derive_display_currency(data: Any) -> str | None:
    """Read the currency off the payload; ``None`` when it doesn't state one.

    Called only when the caller passed nothing. A payload that carries its own
    ``currency_code`` — every ``investments.*`` row, ``AccountSummary``,
    ``AccountDetail``, ``AccountSettingsPayload``, ``BalanceObservationRow`` —
    already holds the single correct answer, so reading it is the opposite of
    guessing. The former ``"USD"`` default guessed, and guessed wrong for every
    non-USD profile on the nine money-bearing tools that never overrode it.

    Not inference: ``resolve_display_currency`` collapses the rows to one code
    or declines. A payload with no currency field anywhere stays unknown rather
    than borrowing a neighbour's.
    """
    if isinstance(data, dict):
        return None
    if isinstance(data, list):
        rows = cast(list[Any], data)
        return _currency_of_rows(rows)
    if _has_currency_field(data):
        return resolve_display_currency([getattr(data, _CURRENCY_FIELD)])
    rows = _primary_rows(data)
    return _currency_of_rows(rows) if rows is not None else None


def _has_currency_field(data: Any) -> bool:
    """True when ``data`` is a record carrying a currency_code field.

    Mapping rows count. ``sql_query`` returns ``list[dict[str, Any]]``
    (``SqlQueryResult.records``), so a dataclass/``BaseModel``-only test made
    every ad-hoc query report an unknown currency — even one projecting
    ``currency_code`` with every row in agreement.
    """
    if isinstance(data, type):
        return False
    if isinstance(data, Mapping):
        return _CURRENCY_FIELD in data
    if is_dataclass(data):
        return any(item.name == _CURRENCY_FIELD for item in fields(data))
    if isinstance(data, BaseModel):
        return _CURRENCY_FIELD in type(data).model_fields
    return False


def _currency_value(row: Any) -> str | None:
    """The row's currency_code, however that row carries it."""
    if isinstance(row, Mapping):
        code = cast(Mapping[str, Any], row)[_CURRENCY_FIELD]
        return None if code is None else str(code)
    return cast("str | None", getattr(row, _CURRENCY_FIELD))


def _currency_of_rows(rows: list[Any]) -> str | None:
    """Resolve the one currency a row list agrees on, else ``None``."""
    if not rows or not all(_has_currency_field(row) for row in rows):
        return None
    return resolve_display_currency(_currency_value(row) for row in rows)


def _primary_rows(data: Any) -> list[Any] | None:
    """The payload's sole non-auxiliary list field, if it has exactly one.

    Mirrors ``_count_primary_lists``' choice of "the" row collection so the
    derived currency describes the same rows that ``returned_count`` counts.
    """
    if isinstance(data, type):
        return None
    if is_dataclass(data):
        names = [item.name for item in fields(data)]
    elif isinstance(data, BaseModel):
        names = list(type(data).model_fields)
    else:
        return None
    primary: list[list[Any]] = []
    for name in names:
        if name in AUXILIARY_LIST_FIELDS:
            continue
        value: Any = getattr(data, name)
        if isinstance(value, list):
            primary.append(cast(list[Any], value))
    return primary[0] if len(primary) == 1 else None


def _count_typed_payload(data: Any) -> int:
    """For a typed payload, return the row count if a primary list field exists, else 1.

    - Skips auxiliary diagnostic list fields (see ``AUXILIARY_LIST_FIELDS``)
      so write-result payloads like ``AccountSettingsPayload(warnings=[])``
      report ``returned_count=1`` instead of ``0``.
    - Falls back to ``1`` when the payload has more than one non-auxiliary
      list field — these are aggregate result objects (e.g.
      ``ImportInboxSyncPayload`` with ``processed``/``failed``/``skipped``/
      ``ignored``), not row collections, so no single list represents the
      "returned" count.
    """
    return _count_primary_lists(data, [item.name for item in fields(data)])


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
    (see ``AUXILIARY_LIST_FIELDS``) are skipped; more than one remaining list
    means an aggregate result object with no single "returned" collection.
    """
    primary_lists: list[list[Any]] = []
    for name in field_names:
        if name in AUXILIARY_LIST_FIELDS:
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
    Returns status="error" with code=error_codes.INFRA_NOT_IMPLEMENTED so agents can branch
    on the top-level status field consistently.
    """
    return build_error_envelope(
        error=UserError(
            f"{action} is not yet implemented",
            code=error_codes.INFRA_NOT_IMPLEMENTED,
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

    ``data`` is an empty list for every envelope built here — the ``error``
    field is the canonical signal that the tool failed. The converse does not
    hold: ``error`` on an envelope does NOT imply empty ``data``, because a
    tool may attach ``error`` to a payload-carrying envelope when the payload
    explains the failure (see ``ResponseEnvelope.error``). Use this builder
    when there is no payload worth returning; attach ``error`` directly when
    there is. Sensitivity defaults to ``low`` because error messages must not
    leak row-level data. ``actions`` preserves any caller-provided next-step
    hints (e.g. CLI fallbacks on stub tools).

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
        error=ErrorDetail.from_user_error(error),
        recovery_actions=recovery_actions,
    )
