"""Tests for ``ImportService.pdf_preview`` — the Phase 2b egress seam.

The preview method runs the deterministic rung, then either returns a typed
``PdfPreviewResult`` (when the deterministic outcome is final) or raises
``ImportConfirmationRequiredError``. On a bridge-eligible failure without a
recorded consent the proposal is a content-free ``AIAssistanceRequired``; with
consent it is a redacted ``BridgePayload``, and only that release writes the
``smart_import_parse`` audit row and bumps the egress metric.

These tests stub ``route_pdf_import`` and ``PDFExtractor.extract`` because
the unit under test is the dispatch logic in ``pdf_preview``, not the
routing math (which has its own dedicated tests in ``test_routing.py``).
"""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path
from typing import Any

import pytest

import moneybin.extractors.pdf.bridge as bridge_mod
from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.extractors.pdf.ir import PdfDocument, PdfTable
from moneybin.extractors.pdf.recipe import Recipe
from moneybin.extractors.pdf.routing import RouteDecision
from moneybin.metrics.registry import (
    PDF_BRIDGE_EGRESS_TOTAL,
    PDF_DISCLOSURE_CONSENT_TOTAL,
)
from moneybin.privacy.disclosure import host_for_recipient_name
from moneybin.services.import_confirmation import (
    AIAssistanceRequired,
    BridgePayload,
    DisclosureConsent,
    ImportConfirmationRequiredError,
    SignConventionProposal,
)
from moneybin.services.import_service import ImportService, PdfPreviewResult
from tests.moneybin.pdf_statement_fixtures import (
    CANARIES,
    CANARY_ACCOUNT_LAST_FOUR,
    CANARY_ACCOUNT_MIDDLE,
    CANARY_AMOUNT,
    CANARY_DESCRIPTION,
    CANARY_SSN_PREFIX,
    write_card_statement_pdf,
)


def _consent(
    *,
    approved_via: str = "prompt",
    grant_id: str | None = None,
    binding_id: str | None = "binding-0001",
) -> DisclosureConsent:
    host = host_for_recipient_name("claude-code")
    assert host is not None
    return DisclosureConsent(
        host=host,
        approved_via=approved_via,  # type: ignore[arg-type]  # test literal
        grant_id=grant_id,
        binding_id=binding_id,
        remembered=approved_via == "remembered",
    )


def _released() -> float:
    return PDF_BRIDGE_EGRESS_TOTAL.labels(outcome="released")._value.get()  # type: ignore[reportPrivateUsage]  # prometheus internals


def _parse_audit_count(db: Database) -> int:
    row = db.conn.execute(
        "SELECT COUNT(*) FROM app.audit_log WHERE action = 'smart_import_parse'"
    ).fetchone()
    return row[0] if row else 0


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


def test_pdf_preview_low_confidence_without_consent_requires_ai_assistance(
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
    assert outcome.reason == "ai_assistance_required"
    assert isinstance(outcome.proposed, AIAssistanceRequired)
    assert outcome.proposed.request_kind == "propose_recipe"
    assert outcome.proposed.decision_reason == "low_confidence"
    assert len(outcome.proposed.file_sha256) == 64


def test_pdf_preview_low_confidence_with_consent_releases_bridge_payload(
    db: Database,
    tmp_path: Path,
    stub_pdf_pipeline: tuple[list[RouteDecision], list[PdfDocument]],
) -> None:
    decisions, docs = stub_pdf_pipeline
    docs.append(_doc())
    decisions.append(_decision(reason="low_confidence", confidence=0.4))

    with pytest.raises(ImportConfirmationRequiredError) as excinfo:
        ImportService(db).pdf_preview(
            _make_pdf_path(tmp_path), disclosure_consent=_consent()
        )

    outcome = excinfo.value.outcome
    assert outcome.channel == "pdf"
    assert outcome.reason == "unknown_layout"
    assert isinstance(outcome.proposed, BridgePayload)
    payload = outcome.proposed.payload
    assert payload["request_kind"] == "propose_recipe"
    assert payload["saved_recipe_for_re_derive"] is None
    assert "transparency_notice" not in payload
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
        ImportService(db).pdf_preview(
            _make_pdf_path(tmp_path), disclosure_consent=_consent()
        )

    outcome = excinfo.value.outcome
    assert outcome.reason == "validation_failure"
    assert isinstance(outcome.proposed, BridgePayload)
    payload = outcome.proposed.payload
    assert payload["request_kind"] == "replay_failed_re_derive"
    # The replay payload carries both the saved format name AND the actual
    # recipe patterns the agent needs to diagnose and refresh the failed match —
    # minus `sign_ratified`, which is the human's ratification and neither the
    # agent's to see nor to return (bridge.recipe_for_agent / parse_bridge_response).
    assert payload["saved_recipe_for_re_derive"] == {
        "name": "chase_checking_pdf",
        "recipe": recipe.model_dump(exclude={"sign_ratified"}),
    }
    saved = payload["saved_recipe_for_re_derive"]
    assert isinstance(saved, dict)
    assert "sign_ratified" not in saved["recipe"]


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

    with pytest.raises(ImportConfirmationRequiredError) as excinfo:
        ImportService(db).pdf_preview(_make_pdf_path(tmp_path))
    assert isinstance(excinfo.value.outcome.proposed, AIAssistanceRequired)


def test_pdf_preview_release_writes_smart_import_parse_audit_row(
    db: Database,
    tmp_path: Path,
    stub_pdf_pipeline: tuple[list[RouteDecision], list[PdfDocument]],
) -> None:
    decisions, docs = stub_pdf_pipeline
    docs.append(_doc())
    decisions.append(_decision(reason="low_confidence", confidence=0.4))

    with pytest.raises(ImportConfirmationRequiredError):
        ImportService(db).pdf_preview(
            _make_pdf_path(tmp_path),
            disclosure_consent=_consent(approved_via="remembered", grant_id="g-1"),
        )

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
    assert context_json["approved_via"] == "remembered"
    assert context_json["grant_id"] == "g-1"
    assert context_json["binding_id"] == "binding-0001"
    assert context_json["recipient"] == "local:claude-code"


def test_pdf_preview_release_increments_egress_metric(
    db: Database,
    tmp_path: Path,
    stub_pdf_pipeline: tuple[list[RouteDecision], list[PdfDocument]],
) -> None:
    decisions, docs = stub_pdf_pipeline
    docs.append(_doc())
    decisions.append(_decision(reason="low_confidence", confidence=0.4))

    before = _released()
    with pytest.raises(ImportConfirmationRequiredError):
        ImportService(db).pdf_preview(
            _make_pdf_path(tmp_path), disclosure_consent=_consent()
        )
    assert _released() == before + 1


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


def test_pdf_preview_underivable_transaction_table_escalates_to_bridge(
    db: Database,
    tmp_path: Path,
    stub_pdf_pipeline: tuple[list[RouteDecision], list[PdfDocument]],
) -> None:
    """A transaction-shaped doc that defeated derivation reaches the agent bridge.

    This is the case routing used to bury: every derivation failure reported
    `no_transaction_table`, which is excluded from bridge escalation, so real
    transactions landed in an opaque seed instead of being handed to the agent
    that could read them.
    """
    decisions, docs = stub_pdf_pipeline
    docs.append(_doc())
    decisions.append(_decision(reason="transaction_table_underivable", confidence=0.0))

    with pytest.raises(ImportConfirmationRequiredError):
        ImportService(db).pdf_preview(_make_pdf_path(tmp_path))


# ---------------------------------------------------------------------------
# Sign-convention gate — pdf_preview must agree with _import_pdf on whether an
# auto-derived credit-card inversion needs ratification. These import a
# committed card-statement PDF through the real extractor (not the stubbed
# pipeline above) because the gate acts on the card disclosures the extractor
# has to actually surface — a hand-built decision can't stand in for that.
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_pdf_preview_card_statement_raises_sign_convention_confirmation(
    db: Database, tmp_path: Path
) -> None:
    """Preview must gate the same auto-derived inversion `_import_pdf` gates.

    Without this, an agent could preview a card statement clean, then hit the
    confirmation requirement only on the real import — the two paths would
    disagree about what the statement is going to do to the ledger.
    """
    pdf = write_card_statement_pdf(tmp_path)

    with pytest.raises(ImportConfirmationRequiredError) as excinfo:
        ImportService(db).pdf_preview(pdf)

    outcome = excinfo.value.outcome
    assert outcome.channel == "pdf"
    assert outcome.reason == "sign_convention"
    assert isinstance(outcome.proposed, SignConventionProposal)
    assert outcome.proposed.sign_convention == "negative_is_income"


@pytest.mark.integration
def test_pdf_preview_card_statement_confirm_true_returns_deterministic_result(
    db: Database, tmp_path: Path
) -> None:
    """`confirm=True` previews past the gate instead of raising."""
    pdf = write_card_statement_pdf(tmp_path)

    result = ImportService(db).pdf_preview(pdf, confirm=True)

    assert isinstance(result, PdfPreviewResult)
    assert result.deterministic is True
    assert result.decision_reason == "passed"
    assert result.row_count == 2


# ---------------------------------------------------------------------------
# Disclosure consent — the canary statement goes through the REAL extractor
# and router; nothing it contains may leave before consent.
# ---------------------------------------------------------------------------


def test_preview_without_consent_is_content_free(
    db: Database, canary_pdf: Path
) -> None:
    with pytest.raises(ImportConfirmationRequiredError) as exc:
        ImportService(db).pdf_preview(canary_pdf)

    outcome = exc.value.outcome
    proposed = outcome.proposed
    assert isinstance(proposed, AIAssistanceRequired)
    assert outcome.reason == "ai_assistance_required"
    dumped = json.dumps(dataclasses.asdict(outcome), default=str)
    for canary in CANARIES:
        assert canary not in dumped
    assert "fingerprint" not in dumped


def test_preview_without_consent_writes_no_parse_audit_and_no_release_metric(
    db: Database, canary_pdf: Path
) -> None:
    before = _released()
    with pytest.raises(ImportConfirmationRequiredError):
        ImportService(db).pdf_preview(canary_pdf)

    assert _parse_audit_count(db) == 0
    assert _released() == before
    audit_dump = json.dumps(
        db.conn.execute("SELECT * FROM app.audit_log").fetchall(), default=str
    )
    for canary in CANARIES:
        assert canary not in audit_dump


def test_build_bridge_request_not_called_without_consent(
    db: Database, canary_pdf: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _boom(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("build_bridge_request called before consent")

    monkeypatch.setattr(bridge_mod, "build_bridge_request", _boom)
    with pytest.raises(ImportConfirmationRequiredError) as exc:
        ImportService(db).pdf_preview(canary_pdf)
    assert isinstance(exc.value.outcome.proposed, AIAssistanceRequired)


def test_preview_with_consent_releases_redacted_payload(
    db: Database, canary_pdf: Path
) -> None:
    with pytest.raises(ImportConfirmationRequiredError) as exc:
        ImportService(db).pdf_preview(canary_pdf, disclosure_consent=_consent())

    proposed = exc.value.outcome.proposed
    assert isinstance(proposed, BridgePayload)
    payload = proposed.payload
    text = json.dumps(payload)
    assert CANARY_DESCRIPTION in text
    assert CANARY_AMOUNT in text
    assert CANARY_ACCOUNT_LAST_FOUR in text
    assert CANARY_ACCOUNT_MIDDLE not in text
    assert CANARY_SSN_PREFIX not in text
    assert "transparency_notice" not in payload


def test_release_writes_parse_audit_with_consent_reference(
    db: Database, canary_pdf: Path
) -> None:
    with pytest.raises(ImportConfirmationRequiredError):
        ImportService(db).pdf_preview(
            canary_pdf, disclosure_consent=_consent(approved_via="agent_relay")
        )

    rows = db.conn.execute(
        "SELECT after_value, context_json FROM app.audit_log "
        "WHERE action = 'smart_import_parse'"
    ).fetchall()
    assert len(rows) == 1
    after, context = rows[0]
    context_json = json.loads(context)
    assert context_json["approved_via"] == "agent_relay"
    assert context_json["binding_id"] == "binding-0001"
    assert context_json["grant_id"] is None
    for canary in CANARIES:
        assert canary not in after
        assert canary not in context


def test_record_disclosure_decision_declined(db: Database) -> None:
    host = host_for_recipient_name("claude-code")
    assert host is not None
    counter = PDF_DISCLOSURE_CONSENT_TOTAL.labels(
        outcome="declined", approved_via="cli_prompt"
    )
    before = counter._value.get()  # type: ignore[reportPrivateUsage]  # prometheus internals

    ImportService(db).record_disclosure_decision(
        host=host,
        outcome="declined",
        approved_via="cli_prompt",
        remembered=False,
        grant_id=None,
        file_sha256s=["b" * 64, "a" * 64],
        decision_reason="metadata_incomplete",
    )

    rows = db.conn.execute(
        "SELECT actor, target_schema, target_table, after_value, context_json "
        "FROM app.audit_log WHERE action = 'smart_import_consent'"
    ).fetchall()
    assert len(rows) == 1
    _actor, schema, table, after, _context = rows[0]
    assert (schema, table) == ("app", "ai_consent_grants")
    after_json = json.loads(after)
    assert after_json["outcome"] == "declined"
    assert after_json["approved_via"] == "cli_prompt"
    assert after_json["recipient"] == "local:claude-code"
    assert after_json["identified"] is True
    assert after_json["terms_version"] == "anthropic-2026-10"
    assert after_json["remembered"] is False
    assert after_json["grant_id"] is None
    assert after_json["file_sha256s"] == ["a" * 64, "b" * 64]
    assert after_json["decision_reason"] == "metadata_incomplete"
    assert "fingerprint" not in after_json
    assert counter._value.get() == before + 1  # type: ignore[reportPrivateUsage]


def test_recipe_from_redacted_text_replays_against_original(
    db: Database, canary_pdf: Path
) -> None:
    """A recipe written from the redacted release reconciles locally."""
    with pytest.raises(ImportConfirmationRequiredError) as exc:
        ImportService(db).pdf_preview(canary_pdf, disclosure_consent=_consent())
    proposed = exc.value.outcome.proposed
    assert isinstance(proposed, BridgePayload)
    released_text = proposed.payload["document_text"]
    # What the agent sees and patterns against: the redacted account line.
    assert "XXXX XXXX 5358" in released_text

    recipe = {
        "metadata_anchors": [
            {
                "name": "account_id",
                "pattern": r"Account\s+Number:\s+([X\d]{4} [X\d]{4} \d{4})",
                "cast": "str",
            },
            {
                "name": "period_start",
                "pattern": r"Statement\s+Period:\s+(\d{2}/\d{2}/\d{4})",
                "cast": "date",
                "date_format": "%m/%d/%Y",
            },
            {
                "name": "period_end",
                "pattern": r"to\s+(\d{2}/\d{2}/\d{4})",
                "cast": "date",
                "date_format": "%m/%d/%Y",
            },
            {
                "name": "opening_balance",
                "pattern": r"Opening\s+Funds:\s+\$([\d,]+\.\d{2})",
                "cast": "decimal",
            },
            {
                "name": "closing_balance",
                "pattern": r"Closing\s+Funds:\s+\$([\d,]+\.\d{2})",
                "cast": "decimal",
            },
        ],
        "row_region": {"start_anchor": "Date", "end_anchor": "@@END@@"},
        "row_split": r"\s{2,}",
        "fields": [
            {
                "name": "date",
                "pattern": r"\d{2}/\d{2}/\d{4}",
                "cast": "date",
                "date_format": "%m/%d/%Y",
            },
            {"name": "description", "pattern": r".+", "cast": "str"},
            {"name": "amount", "pattern": r"-?[\d,]+\.\d{2}", "cast": "decimal"},
        ],
        "sign_convention": "negative_is_expense",
        "routing": "transactions",
    }
    rows = [
        {"date": "01/03/2024", "description": CANARY_DESCRIPTION, "amount": "-47.31"},
        {"date": "01/10/2024", "description": "PAYROLL DEPOSIT", "amount": "1500.00"},
        {"date": "01/20/2024", "description": "UTILITY PAYMENT", "amount": "-152.69"},
    ]

    result = ImportService(db).apply_pdf_bridge_response(
        canary_pdf,
        {"recipe": recipe, "rows": rows},
        account_bindings={"@0": "new"},
    )

    assert result.outcome == "applied", result.reject_reason
    assert result.rows_loaded == 3
    loaded = db.conn.execute(
        "SELECT DISTINCT account_number_masked FROM raw.tabular_accounts "
        "WHERE source_type = 'pdf'"
    ).fetchall()
    assert loaded and all(str(r[0]).endswith(CANARY_ACCOUNT_LAST_FOUR) for r in loaded)


def test_scanned_pdf_unchanged(db: Database, scanned_pdf: Path) -> None:
    with pytest.raises(UserError) as exc:
        ImportService(db).pdf_preview(scanned_pdf)
    assert exc.value.code == error_codes.IMPORT_PDF_NO_TEXT_LAYER


def test_release_audit_row_fingerprint_headers_are_redacted(
    db: Database,
    tmp_path: Path,
    stub_pdf_pipeline: tuple[list[RouteDecision], list[PdfDocument]],
) -> None:
    """The fingerprint released and audited carries no raw account digits."""
    from moneybin.extractors.pdf.metadata import StatementMetadata

    decisions, docs = stub_pdf_pipeline
    docs.append(
        PdfDocument(
            source_file="stub.pdf",
            text_lines=["Account Number: 314159265358", "Summary"],
            tables=[
                PdfTable(
                    page=1,
                    header=["Account 314159265358", "Balance"],
                    rows=[["Opening", "10.00"]],
                )
            ],
        )
    )
    decision = _decision(reason="low_confidence", confidence=0.4)
    decisions.append(
        dataclasses.replace(
            decision,
            metadata=StatementMetadata(
                account_id="314159265358",
                period_start=None,
                period_end=None,
                opening_balance=None,
                closing_balance=None,
            ),
        )
    )

    with pytest.raises(ImportConfirmationRequiredError) as exc:
        ImportService(db).pdf_preview(
            _make_pdf_path(tmp_path), disclosure_consent=_consent()
        )

    proposed = exc.value.outcome.proposed
    assert isinstance(proposed, BridgePayload)
    released_fp = json.dumps(proposed.payload["fingerprint"])
    assert "314159265358" not in released_fp
    assert "5358" in released_fp
    row = db.conn.execute(
        "SELECT after_value FROM app.audit_log WHERE action = 'smart_import_parse'"
    ).fetchone()
    assert row is not None
    audited_fp = json.dumps(json.loads(str(row[0]))["fingerprint"])
    assert "314159265358" not in audited_fp
    assert "5358" in audited_fp


@pytest.mark.parametrize(
    ("outcome", "reason"),
    [("transactions", "passed"), ("seed", "no_transaction_table")],
    ids=["deterministic", "non_bridge_fallback"],
)
def test_pdf_preview_returned_fingerprint_headers_are_redacted(
    db: Database,
    tmp_path: Path,
    stub_pdf_pipeline: tuple[list[RouteDecision], list[PdfDocument]],
    outcome: str,
    reason: str,
) -> None:
    """The preview fingerprint reaches MCP and CLI JSON, so it is redacted too."""
    decisions, docs = stub_pdf_pipeline
    docs.append(
        PdfDocument(
            source_file="stub.pdf",
            text_lines=["Account Number: 3141 5926 5358", "Date Description Amount"],
            tables=[],
        )
    )
    decision = _decision(outcome=outcome, reason=reason, confidence=0.9)
    decisions.append(
        dataclasses.replace(
            decision,
            fp={
                "issuer": "example",
                "headers": ["Account 314159265358", "Balance"],
                "page_bucket": "1",
            },
        )
    )

    result = ImportService(db).pdf_preview(_make_pdf_path(tmp_path))

    dumped = json.dumps(result.fingerprint)
    assert "314159265358" not in dumped
    assert "5926" not in dumped
    assert "5358" in dumped
    assert "Balance" in dumped
