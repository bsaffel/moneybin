"""Bounded local source-coverage metadata, without native identifiers."""

from dataclasses import dataclass
from typing import Annotated, Literal

from moneybin.privacy.taxonomy import DataClass
from moneybin.protocol.row_set import NO_ROW_SET, row_set

type SyncCompletenessKind = Literal[
    "unresolved_identity",
    "unresolved_account_type",
    "unsupported_account_type",
    "unsupported_currency",
    "unsupported_precision",
    "invalid_observation",
    "retrieval_partial",
    "retrieval_failed",
    "retrieval_deferred",
    "history_unknown",
    "history_pending",
    "history_gap",
    "evidence_unavailable",
    "suspected_overlap",
]


@row_set(NO_ROW_SET)
@dataclass(frozen=True, slots=True)
class SyncCompletenessReason:
    """A fixed reason with counts; IDs are withheld without mint provenance."""

    kind: Annotated[SyncCompletenessKind, DataClass.TXN_TYPE]
    affected_account_ids: Annotated[tuple[str, ...], DataClass.RECORD_ID]
    affected_account_count: Annotated[int, DataClass.AGGREGATE]
    unresolved_scope_count: Annotated[int, DataClass.AGGREGATE]


@row_set(NO_ROW_SET)
@dataclass(frozen=True, slots=True)
class SyncCompleteness:
    """Local sync evidence only; neither current broker health nor FX coverage."""

    complete: Annotated[bool, DataClass.TXN_TYPE]
    reasons: tuple[SyncCompletenessReason, ...]
    applicable: Annotated[bool, DataClass.TXN_TYPE] = True
