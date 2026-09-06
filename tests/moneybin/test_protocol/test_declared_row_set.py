"""The row set is declared on the payload, never inferred from field shapes.

``summary.returned_count`` and ``--json-fields`` both need one answer to "which
of this payload's fields IS the collection it returned". They used to infer it:
the sole list field that was not in a name-keyed auxiliary set. A payload with
two collections got "several, so neither" — a count of 1 whatever it returned,
and a documented flag that silently did nothing.

Every payload now states the answer once, at the type. These tests pin one
payload per case the declaration has to serve, and pin that a payload which
carries a collection but declares nothing fails loudly rather than falling back
to a guess.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pytest

from moneybin.cli.output import OutputFormat, render_or_json
from moneybin.privacy.payloads.imports import (
    ImportInboxProcessedEntry,
    ImportInboxSyncPayload,
)
from moneybin.privacy.payloads.networth import (
    NetWorthAccountRow,
    NetWorthCurrencySegment,
    NetWorthSnapshotPayload,
)
from moneybin.privacy.payloads.reports import (
    ReportOutputColumn,
    ReportResultPayload,
    ReportSemanticsPayload,
)
from moneybin.protocol.envelope import build_envelope
from moneybin.protocol.row_set import NO_ROW_SET, RowSetContractError, row_set


def _report_result(row_count: int) -> ReportResultPayload:
    """A report result carrying ``row_count`` rows beside its column metadata."""
    return ReportResultPayload(
        report_id="core:spending",
        parameters={},
        columns=[
            ReportOutputColumn(name="amount", data_class="txn_amount"),
            ReportOutputColumn(name="category", data_class="category"),
        ],
        rows=[
            {"amount": -index, "category": "Groceries"} for index in range(row_count)
        ],
        semantics=ReportSemanticsPayload(
            unit="currency",
            currency=None,
            sign="signed",
            kind="flow",
            valuation_basis="transaction amount",
            fx_basis="no FX conversion",
            time_basis="calendar month",
            denominator=None,
            comparison_window=None,
            exclusions=(),
            provenance=("reports.spending",),
        ),
        period="2026-07",
        sensitivity="high",
        count=row_count,
        truncated=False,
    )


def _inbox_sync() -> ImportInboxSyncPayload:
    """A drain result with its five outcome buckets at five distinct sizes.

    No bucket holds exactly one file. A fixture where one did would report the
    same count whether the payload declared that bucket or declared nothing, so
    the assertion below could not tell the two apart.
    """
    processed: list[ImportInboxProcessedEntry] = [
        {"filename": f"p{index}.csv", "transactions": 7} for index in range(2)
    ]
    return ImportInboxSyncPayload(
        processed=processed,
        failed=[
            {"filename": f"f{index}.csv", "error": "unreadable"} for index in range(3)
        ],
        pending=[],
        skipped=[{"filename": f"s{index}.csv"} for index in range(4)],
        ignored=[{"filename": f"i{index}.csv"} for index in range(5)],
        transforms_applied=True,
        transforms_duration_seconds=1.5,
        transforms_error=None,
    )


def _networth_snapshot() -> NetWorthSnapshotPayload:
    """A snapshot whose per-currency and per-account breakdowns are peers."""
    return NetWorthSnapshotPayload(
        balance_date=None,
        currency_code=None,
        net_worth=None,
        total_assets=None,
        total_liabilities=None,
        account_count=4,
        per_currency=[
            NetWorthCurrencySegment(
                currency_code="USD",
                net_worth=None,
                total_assets=None,
                total_liabilities=None,
                account_count=3,
            ),
            NetWorthCurrencySegment(
                currency_code="EUR",
                net_worth=None,
                total_assets=None,
                total_liabilities=None,
                account_count=1,
            ),
        ],
        per_account=[
            NetWorthAccountRow(
                account_id=f"acct-{index}",
                display_name=None,
                balance=Decimal(index),
                observation_source="statement",
            )
            for index in range(4)
        ],
    )


class TestCountsTheDeclaredRowSet:
    """One payload per case the issue named, each pinned to its declared answer."""

    @pytest.mark.unit
    def test_a_report_result_counts_its_rows_not_its_columns(self) -> None:
        """Case 1 — one collection beside metadata describing it.

        ``columns`` names the shape of ``rows``; it is not a second set of
        them. The inference saw two lists and reported 1 for every report run,
        however many rows came back.
        """
        envelope = build_envelope(data=_report_result(3))
        assert envelope.summary.returned_count == 3

    @pytest.mark.unit
    def test_an_inbox_drain_counts_one_run_not_its_buckets(self) -> None:
        """Case 2 — a genuine multi-bucket outcome.

        ``processed`` / ``failed`` / ``pending`` / ``skipped`` / ``ignored``
        partition one drain's files. No bucket is "the" result, so the payload
        declares that it has no row set and the count describes the run: one.
        The buckets hold 2/3/0/4/5 files, so no bucket could produce this 1.
        """
        envelope = build_envelope(data=_inbox_sync())
        assert envelope.summary.returned_count == 1

    @pytest.mark.unit
    def test_a_networth_snapshot_counts_one_snapshot_not_a_peer_collection(
        self,
    ) -> None:
        """Case 3 — peer collections, neither subordinate to the other.

        ``per_currency`` and ``per_account`` cut the same snapshot two ways.
        Nominating either would publish a count of the cut the caller did not
        ask about, so the payload declares neither.
        """
        envelope = build_envelope(data=_networth_snapshot())
        assert envelope.summary.returned_count == 1


class TestJsonFieldsFollowsTheSameDeclaration:
    """``--json-fields`` reads the declaration the count reads.

    Sharing one declaration is what keeps "the payload's collection" from
    meaning one field to the counter and another to the projection.
    """

    @pytest.mark.unit
    def test_projects_into_the_declared_row_set(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        render_or_json(
            build_envelope(data=_report_result(2)),
            OutputFormat.JSON,
            json_fields="amount",
        )
        out = json.loads(capsys.readouterr().out)
        assert out["data"]["rows"] == [{"amount": 0}, {"amount": -1}]

    @pytest.mark.unit
    def test_no_ops_when_the_payload_declares_no_row_set(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """A payload with no row set has nothing the flag can narrow.

        Projecting one peer collection would hand back a payload narrowed in a
        place the caller cannot see.
        """
        render_or_json(
            build_envelope(data=_networth_snapshot()),
            OutputFormat.JSON,
            json_fields="account_id",
        )
        out = json.loads(capsys.readouterr().out)
        assert set(out["data"]["per_account"][0]) == {
            "account_id",
            "display_name",
            "balance",
            "observation_source",
            "currency_code",
        }


class TestAnUndeclaredPayloadFailsLoudly:
    """A missing or stale declaration must not degrade into a guess.

    A declaration that can go quietly out of date reproduces the original
    defect one level up, so both halves raise.
    """

    @pytest.mark.unit
    def test_building_an_envelope_over_an_undeclared_collection_raises(self) -> None:
        @dataclass(frozen=True, slots=True)
        class _Undeclared:
            rows: list[dict[str, Any]]

        with pytest.raises(RowSetContractError, match="_Undeclared"):
            build_envelope(data=_Undeclared(rows=[{"id": "a1"}]))

    @pytest.mark.unit
    def test_declaring_a_field_the_payload_does_not_carry_raises(self) -> None:
        @dataclass(frozen=True, slots=True)
        class _Renamed:
            rows: list[dict[str, Any]]

        with pytest.raises(RowSetContractError, match="records"):
            row_set("records")(_Renamed)

    @pytest.mark.unit
    def test_declaring_a_field_that_is_not_a_collection_raises(self) -> None:
        @row_set("total")
        @dataclass(frozen=True, slots=True)
        class _NotACollection:
            total: int
            rows: list[dict[str, Any]]

        with pytest.raises(RowSetContractError, match="total"):
            build_envelope(data=_NotACollection(total=2, rows=[{"id": "a1"}]))

    @pytest.mark.unit
    def test_a_payload_with_no_collection_needs_no_declaration(self) -> None:
        @dataclass(frozen=True, slots=True)
        class _Scalars:
            total: int

        assert build_envelope(data=_Scalars(total=4)).summary.returned_count == 1

    @pytest.mark.unit
    def test_no_row_set_is_a_declaration_not_an_absence(self) -> None:
        """The explicit "no row set" arm must not read as "nothing declared"."""

        @row_set(NO_ROW_SET)
        @dataclass(frozen=True, slots=True)
        class _Peers:
            left: list[str]
            right: list[str]

        envelope = build_envelope(data=_Peers(left=["a"], right=["b", "c"]))
        assert envelope.summary.returned_count == 1
