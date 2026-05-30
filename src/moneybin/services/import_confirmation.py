"""Channel-agnostic confirm/confidence primitive.

`resolve_or_confirm` (added in a later task) is the single seam every
smart-import channel calls when it has detected a layout but does NOT
know whether to auto-load. Its inputs and outputs are the types in this
module.

Channel-specific known-layout lookup (saved tabular formats / pdf_formats
/ gsheet connection match) stays in the channel — the primitive is
invoked only when a confirm decision is needed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from moneybin.extractors.confidence import Confidence

Channel = Literal["tabular", "gsheet", "pdf"]
ActorKind = Literal["human", "agent"]
ConfirmationReason = Literal["unknown_layout", "validation_failure"]
ConfirmationOutcome = Literal["accepted", "overridden", "declined"]


@dataclass(frozen=True)
class ProposedMapping:
    """Detector output for tabular / gsheet channels.

    `field_mapping` is destination_field -> source_column. `sample_values`
    keys match `field_mapping` keys. `unmapped_columns` are source columns
    the detector did not consume (shown so the user can spot a missed dest).
    """

    field_mapping: dict[str, str]
    sample_values: dict[str, list[str]]
    unmapped_columns: tuple[str, ...]


@dataclass(frozen=True)
class BridgePayload:
    """PDF-channel bridge payload. Shape is channel-specific.

    v1 carries an opaque dict (the PDF extractor populates IR + extraction
    request); this gives `import_confirm` a stable signature without
    pulling PDF types into this module.
    """

    payload: dict[str, Any]


@dataclass(frozen=True)
class Accept:
    """Caller signal: accept the proposed mapping/recipe as-is."""


@dataclass(frozen=True)
class Override:
    """Caller signal: partial-merge override.

    `mapping` carries ONLY the destination fields the caller is correcting;
    unspecified fields fall back to the proposed mapping.
    """

    mapping: dict[str, str]


@dataclass(frozen=True)
class Resolved:
    """Terminal outcome: the import can load.

    `format_ref` names the saved format (tabular: `app.tabular_formats.name`;
    gsheet: connection_id; PDF: `app.pdf_formats.name`) — None when no
    format was saved (e.g. agent self-accept without persistence, not v1).
    `self_accepted=True` records that an agent accepted at `high` without
    surfacing to the human.
    """

    field_mapping: dict[str, str]
    format_ref: str | None
    self_accepted: bool


@dataclass(frozen=True)
class ConfirmationRequired:
    """Terminal outcome: caller must confirm before data lands.

    `reason='unknown_layout'` surfaces the proposal + samples for first-
    encounter confirm. `reason='validation_failure'` surfaces a known
    layout that failed its replay/validation guard (Req 9) — for tabular
    the proposed mapping carries the failing signal; for PDF this re-
    escalates to the bridge per `smart-import-pdf.md`.
    """

    channel: Channel
    confidence: Confidence
    proposed: ProposedMapping | BridgePayload
    reason: ConfirmationReason
    samples: dict[str, list[str]] = field(default_factory=dict)
    error_message: str = ""


class ImportConfirmationRequiredError(Exception):
    """Raised when an import cannot proceed without explicit confirmation.

    Carries the ConfirmationRequired outcome so MCP/CLI surfaces can render
    the proposal + actions[]. The data load did NOT happen.
    """

    def __init__(self, outcome: ConfirmationRequired) -> None:
        """Wrap a ConfirmationRequired outcome as an exception."""
        super().__init__(
            f"{outcome.channel} import requires confirmation: {outcome.reason}"
        )
        self.outcome = outcome


class MappingValidationError(ValueError):
    """A merged mapping is missing required fields or names unknown columns."""


def validate_partial_mapping(
    *,
    proposed: dict[str, str],
    override: dict[str, str],
    available_columns: tuple[str, ...] | list[str],
    required_fields: tuple[str, ...],
    valid_destinations: tuple[str, ...] | None = None,
) -> dict[str, str]:
    """Merge override onto proposed, validate, and return the merged mapping.

    Override is partial-merge: it overrides only the destination fields it
    names; unspecified fields fall back to `proposed`. Three failure modes:

    1. After merging, a required destination field is unmapped.
    2. The merged mapping names a source column not present in the file/
       sheet (a transform-time error surfaced early as a user-fixable
       validation error).
    3. The override names a destination field not in `valid_destinations`
       (when supplied) — catches typos like ``--mapping descrption=Memo``
       silently passing because `description` is already proposed.

    Args:
        proposed: Detector-emitted mapping (destination -> source column).
        override: Caller-supplied corrections (same shape; partial).
        available_columns: Source columns actually present in the data.
        required_fields: Destination fields that MUST appear in the merged
            mapping for this channel (tabular and gsheet differ — see the
            spec's "channel scorers feed the contract" note).
        valid_destinations: Optional allowlist of acceptable destination
            field names for this channel. When provided, every override
            key must be in this set; ``None`` skips the check (back-compat
            for callers that don't know the full schema).

    Returns:
        The merged mapping. Callers should pass this to the loader rather
        than recomputing it.
    """
    if valid_destinations is not None:
        valid_set = set(valid_destinations)
        unknown_dests = [k for k in override if k not in valid_set]
        if unknown_dests:
            raise MappingValidationError(
                f"Mapping override names unknown destination field(s): "
                f"{unknown_dests}. Valid destinations: {list(valid_destinations)}."
            )
    merged = {**proposed, **override}
    missing = [f for f in required_fields if f not in merged]
    if missing:
        raise MappingValidationError(
            f"Mapping is missing required dest field(s): {missing}. "
            f"Channel needs {list(required_fields)}."
        )
    available_set = set(available_columns)
    unknown = [src for src in merged.values() if src not in available_set]
    if unknown:
        raise MappingValidationError(
            f"Mapping references source column(s) not in the source: "
            f"{unknown}. Available columns: {list(available_columns)}."
        )
    return merged


def resolve_or_confirm(
    *,
    channel: Channel,
    confidence: Confidence,
    proposed: ProposedMapping,
    available_columns: tuple[str, ...] | list[str],
    required_fields: tuple[str, ...],
    signal: Accept | Override | None,
    self_accept_enabled: bool,
    actor_kind: ActorKind,
) -> Resolved | ConfirmationRequired:
    """Decide whether to auto-load, self-accept, or surface for confirmation.

    Invoked by a channel only when a confirm decision is needed (i.e. the
    channel has already failed to match a known layout, or a known layout
    failed its replay/validation guard). Channel-agnostic.

    Decision order (each step short-circuits):

    1. Override signal — partial-merge + validate the user's correction;
       honored at every tier, INCLUDING `low`. An explicit override is the
       spec's first-class recovery path (Req 11); short-circuiting `low`
       before Override would block the documented `import_confirm
       --mapping ...` recovery.
       Pass -> Resolved. Fail -> ConfirmationRequired(reason=validation_failure)
       (carrying the validator's message so callers see why their override
       was rejected).
    2. `low` tier without an Override — never auto-accept (Req 4). Even an
       explicit Accept on `low` surfaces, because Accept ratifies the
       detector's proposal as-is and `low` means the detector could not
       form a complete one.
    3. Accept signal — Resolved with the proposed mapping (validated).
    4. No signal, actor=agent, tier=high, self_accept_enabled=True —
       self-accept (Req 10, behind the calibration gate).
    5. Otherwise — ConfirmationRequired(reason=unknown_layout).
    """
    if isinstance(signal, Override):
        try:
            merged = validate_partial_mapping(
                proposed=proposed.field_mapping,
                override=signal.mapping,
                available_columns=available_columns,
                required_fields=required_fields,
            )
        except MappingValidationError as e:
            return ConfirmationRequired(
                channel=channel,
                confidence=confidence,
                proposed=proposed,
                reason="validation_failure",
                samples=dict(proposed.sample_values),
                error_message=str(e),
            )
        return Resolved(field_mapping=merged, format_ref=None, self_accepted=False)

    if confidence.tier == "low":
        return ConfirmationRequired(
            channel=channel,
            confidence=confidence,
            proposed=proposed,
            reason="unknown_layout",
            samples=dict(proposed.sample_values),
        )

    if isinstance(signal, Accept):
        merged = validate_partial_mapping(
            proposed=proposed.field_mapping,
            override={},
            available_columns=available_columns,
            required_fields=required_fields,
        )
        return Resolved(field_mapping=merged, format_ref=None, self_accepted=False)

    if actor_kind == "agent" and confidence.tier == "high" and self_accept_enabled:
        merged = validate_partial_mapping(
            proposed=proposed.field_mapping,
            override={},
            available_columns=available_columns,
            required_fields=required_fields,
        )
        return Resolved(field_mapping=merged, format_ref=None, self_accepted=True)

    return ConfirmationRequired(
        channel=channel,
        confidence=confidence,
        proposed=proposed,
        reason="unknown_layout",
        samples=dict(proposed.sample_values),
    )
