"""Channel-agnostic confirm/confidence primitive.

`resolve_or_confirm` (added in a later task) is the single seam every
smart-import channel calls when it has detected a layout but does NOT
know whether to auto-load. Its inputs and outputs are the types in this
module.

Channel-specific known-layout lookup (saved tabular formats / pdf_formats
/ gsheet connection match) stays in the channel — the primitive is
invoked only when a confirm decision is needed.
"""

from dataclasses import dataclass, field
from typing import Any, Literal

from moneybin.extractors.confidence import Confidence

Channel = Literal["tabular", "gsheet", "pdf"]
ActorKind = Literal["human", "agent"]
ConfirmationReason = Literal["unknown_layout", "validation_failure"]


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
