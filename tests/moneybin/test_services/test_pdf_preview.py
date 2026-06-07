"""Tests for ``ImportService.pdf_preview`` — the Phase 2b egress seam.

The preview method runs the deterministic rung, then either returns a typed
``PdfPreviewResult`` (when the deterministic outcome is final) or raises
``ImportConfirmationRequiredError`` carrying a ``BridgePayload`` (when the
agent can help — bridge-eligible failure modes). The escalation also writes
a ``smart_import_parse`` audit row (Req 14) and bumps the egress metric.

These tests stub ``route_pdf_import`` and ``PDFExtractor.extract`` because
the unit under test is the dispatch logic in ``pdf_preview``, not the
routing math (which has its own dedicated tests in ``test_routing.py``).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.extractors.pdf.ir import PdfDocument, PdfTable
from moneybin.extractors.pdf.recipe import Recipe
from moneybin.extractors.pdf.routing import RouteDecision
from moneybin.metrics.registry import PDF_BRIDGE_EGRESS_TOTAL
from moneybin.services.import_confirmation import (
    BridgePayload,
    ImportConfirmationRequiredError,
)
from moneybin.services.import_service import ImportService, PdfPreviewResult


def _make_recipe() -> Recipe:
    """Build a minimal valid Recipe for replay-payload assertions."""
    return Recipe.model_validate({
        "metadata_anchors": [],
        "row_region": {"start_anchor": "TRANSACTIONS", "end_anchor": "TOTAL"},
        "row_split": r"\s{2,}",
        "fields": [
            {
                "name": "date",
                "pattern": r"\d{2}/\d{2}/\d{4}",
                "cast": "date",
                "date_format": "%m/%d/%Y",
            },
            {"name": "amount", "pattern": r"-?\d+\.\d{2}", "cast": "decimal"},
        ],
        "sign_convention": "negative_is_expense",
        "routing": "transactions",
    })


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


def _decision(
    *,
    outcome: str = "seed",
    reason: str = "low_confidence",
    confidence: float = 0.55,
    rows: list[dict[str, Any]] | None = None,
    matched_format_name: str | None = None,
    recipe: Recipe | None = None,
) -> RouteDecision:
    from moneybin.extractors.pdf.metadata import StatementMetadata

    return RouteDecision(
        outcome=outcome,  # type: ignore[arg-type]
        recipe=recipe,
        rows=rows or [],
        metadata=StatementMetadata(
            account_id=None,
            period_start=None,
            period_end=None,
            opening_balance=None,
            closing_balance=None,
        ),
        confidence=confidence,
        reason=reason,  # type: ignore[arg-type]
        matched_format_name=matched_format_name,
        fp={
            "issuer": "chase",
            "headers": ["Date", "Description", "Amount"],
            "page_bucket": "1",
        },
    )


@pytest.fixture()
def stub_pdf_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[RouteDecision], list[PdfDocument]]:
    """Stub PDFExtractor + route_pdf_import. Caller appends decisions/docs."""
    decisions: list[RouteDecision] = []
    docs: list[PdfDocument] = []

    class _StubExtractor:
        def extract(self, _path: Path) -> PdfDocument:
            return docs[0] if docs else _doc()

    monkeypatch.setattr(
        "moneybin.extractors.pdf.extractor.PDFExtractor", _StubExtractor
    )

    def _fake_route(_doc: PdfDocument, _db: Database) -> RouteDecision:
        return decisions[0]

    monkeypatch.setattr("moneybin.extractors.pdf.routing.route_pdf_import", _fake_route)
    return decisions, docs


def _make_pdf_path(tmp_path: Path) -> Path:
    """Create a dummy file path with .pdf suffix (content irrelevant — stubbed)."""
    path = tmp_path / "stub.pdf"
    path.write_bytes(b"%PDF-1.4\n%dummy\n")
    return path


def test_pdf_preview_deterministic_returns_typed_result(
    db: Database,
    tmp_path: Path,
    stub_pdf_pipeline: tuple[list[RouteDecision], list[PdfDocument]],
) -> None:
    decisions, docs = stub_pdf_pipeline
    docs.append(_doc())
    decisions.append(
        _decision(
            outcome="transactions",
            reason="passed",
            confidence=0.95,
            rows=[{"date": "05/01", "amount": "-4.50"}],
        )
    )

    result = ImportService(db).pdf_preview(_make_pdf_path(tmp_path))

    assert isinstance(result, PdfPreviewResult)
    assert result.deterministic is True
    assert result.decision_reason == "passed"
    assert result.row_count == 1
    assert result.confidence == 0.95
    assert result.fingerprint is not None


def test_pdf_preview_low_confidence_raises_bridge_escalation(
    db: Database,
    tmp_path: Path,
    stub_pdf_pipeline: tuple[list[RouteDecision], list[PdfDocument]],
) -> None:
    decisions, docs = stub_pdf_pipeline
    docs.append(_doc())
    decisions.append(_decision(reason="low_confidence", confidence=0.4))

    with pytest.raises(ImportConfirmationRequiredError) as excinfo:
        ImportService(db).pdf_preview(_make_pdf_path(tmp_path))

    outcome = excinfo.value.outcome
    assert outcome.channel == "pdf"
    assert outcome.reason == "unknown_layout"
    assert isinstance(outcome.proposed, BridgePayload)
    payload = outcome.proposed.payload
    assert payload["request_kind"] == "propose_recipe"
    assert payload["saved_recipe_for_re_derive"] is None
    assert "transparency_notice" in payload
    # compute_fingerprint resolves issuer from the document — not the stub.fp.
    assert set(payload["fingerprint"].keys()) >= {"issuer", "headers", "page_bucket"}


def test_pdf_preview_replay_failed_uses_replay_request_kind(
    db: Database,
    tmp_path: Path,
    stub_pdf_pipeline: tuple[list[RouteDecision], list[PdfDocument]],
) -> None:
    decisions, docs = stub_pdf_pipeline
    docs.append(_doc())
    recipe = _make_recipe()
    decisions.append(
        _decision(
            reason="replay_reconciliation_failed",
            confidence=0.9,
            matched_format_name="chase_checking_pdf",
            recipe=recipe,
        )
    )

    with pytest.raises(ImportConfirmationRequiredError) as excinfo:
        ImportService(db).pdf_preview(_make_pdf_path(tmp_path))

    outcome = excinfo.value.outcome
    assert outcome.reason == "validation_failure"
    assert isinstance(outcome.proposed, BridgePayload)
    payload = outcome.proposed.payload
    assert payload["request_kind"] == "replay_failed_re_derive"
    # The replay payload carries both the saved format name AND the actual
    # recipe patterns the agent needs to diagnose and refresh the failed match.
    assert payload["saved_recipe_for_re_derive"] == {
        "name": "chase_checking_pdf",
        "recipe": recipe.model_dump(),
    }


@pytest.mark.parametrize(
    "reason",
    [
        "low_confidence",
        "replay_reconciliation_failed",
        "reconciliation_failed",
        "metadata_incomplete",
    ],
)
def test_pdf_preview_all_bridge_eligible_reasons_escalate(
    db: Database,
    tmp_path: Path,
    reason: str,
    stub_pdf_pipeline: tuple[list[RouteDecision], list[PdfDocument]],
) -> None:
    """Every entry in `_BRIDGE_ELIGIBLE_REASONS` must trigger bridge escalation.

    Without parametrize coverage, `reconciliation_failed` and
    `metadata_incomplete` would silently regress to the non-escalating
    fallback if either were removed from the frozenset.
    """
    decisions, docs = stub_pdf_pipeline
    docs.append(_doc())
    decisions.append(
        _decision(
            reason=reason,
            confidence=0.4,
            matched_format_name=(
                "chase_checking_pdf"
                if reason == "replay_reconciliation_failed"
                else None
            ),
            recipe=(
                _make_recipe() if reason == "replay_reconciliation_failed" else None
            ),
        )
    )

    with pytest.raises(ImportConfirmationRequiredError):
        ImportService(db).pdf_preview(_make_pdf_path(tmp_path))


def test_pdf_preview_escalation_writes_smart_import_parse_audit_row(
    db: Database,
    tmp_path: Path,
    stub_pdf_pipeline: tuple[list[RouteDecision], list[PdfDocument]],
) -> None:
    decisions, docs = stub_pdf_pipeline
    docs.append(_doc())
    decisions.append(_decision(reason="low_confidence", confidence=0.4))

    with pytest.raises(ImportConfirmationRequiredError):
        ImportService(db).pdf_preview(_make_pdf_path(tmp_path))

    rows = db.conn.execute(
        "SELECT actor, action, target_schema, target_table, after_value, context_json "
        "FROM app.audit_log WHERE action = 'smart_import_parse'"
    ).fetchall()
    assert len(rows) == 1
    actor, action, schema, table, after, context = rows[0]
    assert actor == "system"
    assert action == "smart_import_parse"
    assert (schema, table) == ("raw", "pdf_seeds")
    after_json = json.loads(after)
    assert after_json["request_kind"] == "propose_recipe"
    assert after_json["decision_reason"] == "low_confidence"
    context_json = json.loads(context)
    assert context_json["confidence"] == 0.4
    assert context_json["decision_reason"] == "low_confidence"


def test_pdf_preview_escalation_increments_egress_metric(
    db: Database,
    tmp_path: Path,
    stub_pdf_pipeline: tuple[list[RouteDecision], list[PdfDocument]],
) -> None:
    decisions, docs = stub_pdf_pipeline
    docs.append(_doc())
    decisions.append(_decision(reason="low_confidence", confidence=0.4))

    before = PDF_BRIDGE_EGRESS_TOTAL.labels(outcome="proposed")._value.get()  # type: ignore[reportPrivateUsage] — prometheus internals
    with pytest.raises(ImportConfirmationRequiredError):
        ImportService(db).pdf_preview(_make_pdf_path(tmp_path))
    after = PDF_BRIDGE_EGRESS_TOTAL.labels(outcome="proposed")._value.get()  # type: ignore[reportPrivateUsage]
    assert after == before + 1


@pytest.mark.parametrize(
    "reason",
    ["no_transaction_table", "no_rows", "unsupported_number_format"],
)
def test_pdf_preview_non_bridge_reason_returns_non_deterministic(
    db: Database,
    tmp_path: Path,
    reason: str,
    stub_pdf_pipeline: tuple[list[RouteDecision], list[PdfDocument]],
) -> None:
    decisions, docs = stub_pdf_pipeline
    docs.append(_doc())
    decisions.append(_decision(reason=reason, confidence=0.0))

    result = ImportService(db).pdf_preview(_make_pdf_path(tmp_path))

    assert isinstance(result, PdfPreviewResult)
    assert result.deterministic is False
    assert result.decision_reason == reason
    assert result.row_count == 0


def test_pdf_preview_non_bridge_reason_does_not_write_audit_row(
    db: Database,
    tmp_path: Path,
    stub_pdf_pipeline: tuple[list[RouteDecision], list[PdfDocument]],
) -> None:
    decisions, docs = stub_pdf_pipeline
    docs.append(_doc())
    decisions.append(_decision(reason="no_transaction_table", confidence=0.0))

    ImportService(db).pdf_preview(_make_pdf_path(tmp_path))

    rows = db.conn.execute(
        "SELECT COUNT(*) FROM app.audit_log WHERE action = 'smart_import_parse'"
    ).fetchone()
    assert rows[0] == 0  # type: ignore[index]  # COUNT(*) always returns a 1-tuple
