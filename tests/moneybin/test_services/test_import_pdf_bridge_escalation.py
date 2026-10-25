"""Tests for Phase 2b Option B: import_files escalates bridge-eligible PDFs.

When a driving agent is present (``actor_kind="agent"``), ``_import_pdf`` stops
on a bridge-eligible layout instead of silently seeding: without a recorded
consent it raises a content-free ``AIAssistanceRequired``; with one it raises a
redacted ``BridgePayload``. With no agent (``actor_kind="human"``), the Phase 2a
seed fallback is preserved and a consent argument changes nothing.

These stub ``PDFExtractor.extract`` + ``route_pdf_import`` so the unit under
test is ``_import_pdf``'s escalate-vs-seed branch, not routing math.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from moneybin.database import Database
from moneybin.extractors.pdf.ir import PdfDocument, PdfTable
from moneybin.extractors.pdf.metadata import StatementMetadata
from moneybin.extractors.pdf.routing import RouteDecision
from moneybin.metrics.registry import PDF_BRIDGE_EGRESS_TOTAL
from moneybin.privacy.disclosure import host_for_recipient_name
from moneybin.services.import_confirmation import (
    AIAssistanceRequired,
    BridgePayload,
    DisclosureConsent,
    ImportConfirmationRequiredError,
    confirmation_payload_dict,
)
from moneybin.services.import_service import ImportService


def _consent() -> DisclosureConsent:
    host = host_for_recipient_name("claude-code")
    assert host is not None
    return DisclosureConsent(
        host=host,
        approved_via="agent_relay",
        grant_id=None,
        binding_id="binding-0001",
        remembered=False,
    )


def _released() -> float:
    return PDF_BRIDGE_EGRESS_TOTAL.labels(outcome="released")._value.get()  # type: ignore[reportPrivateUsage]  # prometheus internals


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
        def extract(
            self, _path: Path, *, source_bytes: bytes | None = None
        ) -> PdfDocument:
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
        "SELECT COUNT(*) FROM app.import_log WHERE source_file = ?",
        [str(source_file)],
    ).fetchone()
    return row[0] if row else 0


# ---------------------------------------------------------------------------
# Agent present → escalate
# ---------------------------------------------------------------------------


# First-contact bridge-eligible reasons (matched_format_name=None). The
# replay-failure reason carries a saved format and a different request_kind, so
# it is exercised separately by the apply-path tests.
@pytest.mark.parametrize(
    "reason",
    ["low_confidence", "metadata_incomplete", "reconciliation_failed"],
)
def test_agent_bridge_eligible_escalates(
    db: Database, tmp_path: Path, reason: str, stub_pipeline: list[RouteDecision]
) -> None:
    stub_pipeline.append(_decision(reason=reason))
    path = _pdf_path(tmp_path)

    with pytest.raises(ImportConfirmationRequiredError) as excinfo:
        ImportService(db).import_file(path, refresh=False, actor_kind="agent")

    outcome = excinfo.value.outcome
    assert outcome.channel == "pdf"
    assert outcome.reason == "ai_assistance_required"
    assert isinstance(outcome.proposed, AIAssistanceRequired)
    assert outcome.proposed.decision_reason == reason


def test_agent_bridge_eligible_with_consent_releases_payload(
    db: Database, tmp_path: Path, stub_pipeline: list[RouteDecision]
) -> None:
    stub_pipeline.append(_decision(reason="low_confidence"))
    path = _pdf_path(tmp_path)

    with pytest.raises(ImportConfirmationRequiredError) as excinfo:
        ImportService(db).import_file(
            path, refresh=False, actor_kind="agent", disclosure_consent=_consent()
        )

    assert isinstance(excinfo.value.outcome.proposed, BridgePayload)


def test_agent_escalation_writes_no_import_log_row(
    db: Database, tmp_path: Path, stub_pipeline: list[RouteDecision]
) -> None:
    # A hand-off loads nothing, so begin_import must not have run.
    stub_pipeline.append(_decision(reason="low_confidence"))
    path = _pdf_path(tmp_path)

    with pytest.raises(ImportConfirmationRequiredError):
        ImportService(db).import_file(path, refresh=False, actor_kind="agent")

    assert _import_log_count(db, path) == 0


def test_agent_escalation_without_consent_neither_audits_nor_counts(
    db: Database, tmp_path: Path, stub_pipeline: list[RouteDecision]
) -> None:
    stub_pipeline.append(_decision(reason="low_confidence"))
    path = _pdf_path(tmp_path)

    before = _released()
    with pytest.raises(ImportConfirmationRequiredError):
        ImportService(db).import_file(path, refresh=False, actor_kind="agent")

    assert _audit_count(db) == 0
    assert _released() == before


def test_agent_release_audits_and_bumps_metric(
    db: Database, tmp_path: Path, stub_pipeline: list[RouteDecision]
) -> None:
    stub_pipeline.append(_decision(reason="low_confidence"))
    path = _pdf_path(tmp_path)

    before = _released()
    with pytest.raises(ImportConfirmationRequiredError):
        ImportService(db).import_file(
            path, refresh=False, actor_kind="agent", disclosure_consent=_consent()
        )

    assert _audit_count(db) == 1
    assert _released() == before + 1


def test_import_files_reports_content_free_payload_per_bridge_file(
    db: Database, tmp_path: Path, stub_pipeline: list[RouteDecision]
) -> None:
    """R3: a batch reports each bridge file as a content-free confirmation."""
    stub_pipeline.append(_decision(reason="metadata_incomplete"))
    path = _pdf_path(tmp_path)

    batch = ImportService(db).import_files([path], refresh=False, actor_kind="agent")

    (row,) = batch.per_file
    assert row.status == "confirmation_required"
    payload = row.confirmation_payload
    assert payload is not None
    assert payload["reason"] == "ai_assistance_required"
    assert payload["bridge_payload"] is None
    assert payload["ai_assistance"] == {
        "file_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "decision_reason": "metadata_incomplete",
        "request_kind": "propose_recipe",
    }
    assert "Coffee" not in repr(payload)


def test_import_files_applies_consent_to_bridge_files(
    db: Database, tmp_path: Path, stub_pipeline: list[RouteDecision]
) -> None:
    stub_pipeline.append(_decision(reason="metadata_incomplete"))
    path = _pdf_path(tmp_path)

    batch = ImportService(db).import_files(
        [path], refresh=False, actor_kind="agent", disclosure_consent=_consent()
    )

    (row,) = batch.per_file
    assert row.confirmation_payload is not None
    assert row.confirmation_payload["bridge_payload"] is not None


def test_confirmation_payload_dict_ai_assistance_is_none_for_other_channels() -> None:
    from moneybin.extractors.confidence import Confidence
    from moneybin.services.import_confirmation import (
        ConfirmationRequired,
        ProposedMapping,
    )

    outcome = ConfirmationRequired(
        channel="tabular",
        confidence=Confidence(
            score=0.5, tier="medium", flagged=(), missing_required=()
        ),
        proposed=ProposedMapping(
            field_mapping={}, sample_values={}, unmapped_columns=()
        ),
        reason="unknown_layout",
    )
    assert confirmation_payload_dict(outcome)["ai_assistance"] is None


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

    result = ImportService(db).import_file(
        path, refresh=False, actor_kind="human", disclosure_consent=_consent()
    )

    assert result.import_id is not None
    assert _import_log_count(db, path) == 1
    # A consent never turns a human-actor import into a disclosure.
    assert _audit_count(db) == 0
