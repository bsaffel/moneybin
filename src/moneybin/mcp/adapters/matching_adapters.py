"""Adapters that turn `app.match_decisions` read rows into typed payload rows.

`get_pending` and `get_log` hand back untyped dicts, and four call sites — two
MCP tools, the normalized reviews queue, and the CLI — each need the same
projection of them. Written once here so a column added to the decision table
reaches every surface, and so a coercion (`confidence_score` may be NULL for an
exact-id match) cannot be right on one surface and wrong on the next.

Pure: no I/O, no side-effects. The envelope's `actions` are the caller's,
because the two surfaces speak disjoint vocabularies — MCP names tools, the CLI
names commands — and a shared hint is wrong for one of them whichever it names.
"""

from __future__ import annotations

from typing import Any, cast

from moneybin.privacy.payloads.transactions import (
    MatchesHistoryPayload,
    MatchesPendingPayload,
    MatchHistoryRow,
    MatchPendingRow,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope


def match_pending_row(row: dict[str, Any]) -> MatchPendingRow:
    """Project one pending decision row.

    `confidence_score` falls back to 0.0 rather than raising: an exact-id match
    records no score at all. The *text* surface keeps that distinction (it
    prints a dash), which is why the coercion lives here and not in the query.
    """
    return MatchPendingRow(
        match_id=str(row["match_id"]),
        match_type=str(row.get("match_type") or "dedup"),
        match_tier=cast("str | None", row.get("match_tier")),
        confidence_score=float(row.get("confidence_score") or 0.0),
        source_type_a=str(row["source_type_a"]),
        source_transaction_id_a=str(row["source_transaction_id_a"]),
        source_type_b=str(row["source_type_b"]),
        source_transaction_id_b=str(row["source_transaction_id_b"]),
        match_status=str(row["match_status"]),
        component_key=str(row["component_key"]),
    )


def match_history_row(row: dict[str, Any]) -> MatchHistoryRow:
    """Project one terminal decision row."""
    decided_at = row.get("decided_at")
    return MatchHistoryRow(
        match_id=str(row["match_id"]),
        match_type=str(row.get("match_type") or "dedup"),
        match_status=str(row["match_status"]),
        match_tier=cast("str | None", row.get("match_tier")),
        confidence_score=float(row.get("confidence_score") or 0.0),
        decided_by=str(row.get("decided_by") or "unknown"),
        decided_at=None if decided_at is None else str(decided_at),
        source_type_a=str(row["source_type_a"]),
        source_type_b=str(row["source_type_b"]),
    )


def matches_pending_envelope(
    rows: list[dict[str, Any]],
    *,
    total_count: int,
    n_dedup_groups: int,
    actions: list[str],
) -> ResponseEnvelope[MatchesPendingPayload]:
    """Wrap one page of the pending queue.

    ``n_dedup_groups`` counts components across the FULL queue rather than this
    page, so a paginated reviewer still sees how many transactions really need
    review.
    """
    return build_envelope(
        data=MatchesPendingPayload(
            n_dedup_groups=n_dedup_groups,
            matches=[match_pending_row(row) for row in rows],
        ),
        total_count=total_count,
        actions=actions,
    )


def matches_history_envelope(
    rows: list[dict[str, Any]],
    *,
    actions: list[str],
) -> ResponseEnvelope[MatchesHistoryPayload]:
    """Wrap the recent-decisions log."""
    return build_envelope(
        data=MatchesHistoryPayload(matches=[match_history_row(row) for row in rows]),
        actions=actions,
    )
