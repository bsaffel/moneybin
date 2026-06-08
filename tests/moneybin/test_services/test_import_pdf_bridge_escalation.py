"""Tests for Phase 2b Option B: import_files escalates bridge-eligible PDFs.

When a driving agent is present (``actor_kind="agent"``), ``_import_pdf`` hands
a bridge-eligible layout to the agent (raises ``ImportConfirmationRequiredError``
with a ``BridgePayload``) instead of silently seeding — coherent with how the
tabular path escalates unknown layouts. With no agent (``actor_kind="human"``),
the Phase 2a seed fallback is preserved.

These stub ``PDFExtractor.extract`` + ``route_pdf_import`` so the unit under
test is ``_import_pdf``'s escalate-vs-seed branch, not routing math.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from moneybin.database import Database
from moneybin.extractors.pdf.ir import PdfDocument, PdfTable
from moneybin.extractors.pdf.metadata import StatementMetadata
from moneybin.extractors.pdf.routing import RouteDecision
from moneybin.metrics.registry import PDF_BRIDGE_EGRESS_TOTAL
from moneybin.services.import_confirmation import (
    BridgePayload,
    ImportConfirmationRequiredError,
)
from moneybin.services.import_service import ImportService


def _doc() -> PdfDocument:
    return PdfDocument(
        source_file="chase.pdf",
        text_lines=[
            "Chase Bank — Checking Statement",
            "Date  Description  Amount",
            "05/01  Coffee  -4.50",
            "Total: -4.50",
        ],
        tables=[
            PdfTable(
                page=1,
                header=["Date", "Description", "Amount"],
                rows=[["05/01", "Coffee", "-4.50"]],
            ),
        ],
    )


def _decision(*, reason: str, outcome: str = "seed") -> RouteDecision:
    return RouteDecision(
        outcome=outcome,  # type: ignore[arg-type]
        recipe=None,
        rows=[],
        metadata=StatementMetadata(
            account_id=None,
            period_start=None,
            period_end=None,
            opening_balance=None,
            closing_balance=None,
        ),
        confidence=0.4,
        reason=reason,  # type: ignore[arg-type]
        matched_format_name=None,
        fp={
            "issuer": "chase",
            "headers": ["Date", "Description", "Amount"],
            "page_bucket": "1",
        },
    )


@pytest.fixture()
def stub_pipeline(monkeypatch: pytest.MonkeyPatch) -> list[RouteDecision]:
    """Stub PDFExtractor + route_pdf_import. Caller sets decisions[0]."""
    decisions: list[RouteDecision] = []

    class _StubExtractor:
        def extract(self, _path: Path) -> PdfDocument:
            return _doc()

    monkeypatch.setattr(
        "moneybin.extractors.pdf.extractor.PDFExtractor", _StubExtractor
    )

    def _fake_route(_doc: PdfDocument, _db: Database) -> RouteDecision:
        return decisions[0]

    monkeypatch.setattr("moneybin.extractors.pdf.routing.route_pdf_import", _fake_route)
    return decisions


def _pdf_path(tmp_path: Path) -> Path:
    path = tmp_path / "chase.pdf"
    path.write_bytes(b"%PDF-1.4\n%stub\n")
    return path


def _audit_count(db: Database) -> int:
    row = db.conn.execute(
        "SELECT COUNT(*) FROM app.audit_log WHERE action = 'smart_import_parse'"
    ).fetchone()
    return row[0] if row else 0


def _import_log_count(db: Database, source_file: Path) -> int:
    row = db.conn.execute(
        "SELECT COUNT(*) FROM raw.import_log WHERE source_file = ?",
        [str(source_file)],
    ).fetchone()
    return row[0] if row else 0


# ---------------------------------------------------------------------------
# Agent present → escalate
# ---------------------------------------------------------------------------


def test_agent_bridge_eligible_escalates(
    db: Database, tmp_path: Path, stub_pipeline: list[RouteDecision]
) -> None:
    stub_pipeline.append(_decision(reason="low_confidence"))
    path = _pdf_path(tmp_path)

    with pytest.raises(ImportConfirmationRequiredError) as excinfo:
        ImportService(db).import_file(path, refresh=False, actor_kind="agent")

    outcome = excinfo.value.outcome
    assert outcome.channel == "pdf"
    assert isinstance(outcome.proposed, BridgePayload)


def test_agent_escalation_writes_no_import_log_row(
    db: Database, tmp_path: Path, stub_pipeline: list[RouteDecision]
) -> None:
    # A hand-off loads nothing, so begin_import must not have run.
    stub_pipeline.append(_decision(reason="low_confidence"))
    path = _pdf_path(tmp_path)

    with pytest.raises(ImportConfirmationRequiredError):
        ImportService(db).import_file(path, refresh=False, actor_kind="agent")

    assert _import_log_count(db, path) == 0


def test_agent_escalation_audits_and_bumps_metric(
    db: Database, tmp_path: Path, stub_pipeline: list[RouteDecision]
) -> None:
    stub_pipeline.append(_decision(reason="low_confidence"))
    path = _pdf_path(tmp_path)

    before = PDF_BRIDGE_EGRESS_TOTAL.labels(outcome="proposed")._value.get()  # type: ignore[reportPrivateUsage]
    audit_before = _audit_count(db)
    with pytest.raises(ImportConfirmationRequiredError):
        ImportService(db).import_file(path, refresh=False, actor_kind="agent")

    assert _audit_count(db) == audit_before + 1
    after = PDF_BRIDGE_EGRESS_TOTAL.labels(outcome="proposed")._value.get()  # type: ignore[reportPrivateUsage]
    assert after == before + 1


@pytest.mark.parametrize("reason", ["no_transaction_table", "no_rows"])
def test_agent_non_bridge_reason_seeds_not_escalates(
    db: Database, tmp_path: Path, reason: str, stub_pipeline: list[RouteDecision]
) -> None:
    # Non-bridge-eligible failures always seed, even with an agent present.
    stub_pipeline.append(_decision(reason=reason))
    path = _pdf_path(tmp_path)

    result = ImportService(db).import_file(path, refresh=False, actor_kind="agent")

    assert result.import_id is not None
    assert _import_log_count(db, path) == 1


# ---------------------------------------------------------------------------
# No agent → Phase 2a seed fallback preserved
# ---------------------------------------------------------------------------


def test_human_bridge_eligible_seeds_not_escalates(
    db: Database, tmp_path: Path, stub_pipeline: list[RouteDecision]
) -> None:
    # Same bridge-eligible decision, but no agent → seed, no escalation.
    stub_pipeline.append(_decision(reason="low_confidence"))
    path = _pdf_path(tmp_path)

    result = ImportService(db).import_file(path, refresh=False, actor_kind="human")

    assert result.import_id is not None
    assert _import_log_count(db, path) == 1
    # And no bridge hand-off was audited.
    assert _audit_count(db) == 0
