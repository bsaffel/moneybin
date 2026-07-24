"""Tests for ``ImportService.apply_pdf_bridge_response`` — the Phase 2b apply seam.

The driving agent returns ``{recipe, rows}``; apply re-executes the recipe
against the freshly-extracted document (the *actual*), runs the reconciliation
gate on those rows, and — on pass — persists the recipe and loads the rows.
The agent's returned rows are the *expectation*: apply verifies they match the
re-executed count and surfaces any divergence rather than trusting them
blindly (reconciliation on the re-executed rows is the authority).

These tests stub ``PDFExtractor.extract`` to return a fixed native-text
statement IR (the routing math has its own tests in ``test_routing.py``), but
exercise the REAL ``route_forced_recipe`` + reconciliation + load against a
real in-process DuckDB, because the reconciliation gate is the behavior under
test.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.extractors.pdf.ir import PdfDocument, PdfTable
from moneybin.metrics.registry import PDF_BRIDGE_EGRESS_TOTAL, PDF_SIGN_GATE_TOTAL
from moneybin.services.import_service import BridgeApplyResult, ImportService
from moneybin.tables import PDF_FORMATS, TABULAR_TRANSACTIONS

# ---------------------------------------------------------------------------
# Fixtures — a reconciling Chase-style statement + the recipe the agent returns
# ---------------------------------------------------------------------------

_HEADERS = ["Date", "Description", "Amount"]
_ROW_REGION_START = "Date  Description  Amount"
_ROW_REGION_END = "Total:"


def _standard_doc(
    opening: str = "1000.00",
    closing: str = "1100.00",
    *,
    card_markers: bool = False,
) -> PdfDocument:
    """Chase statement IR whose rows net to closing - opening (reconciles)."""
    return PdfDocument(
        source_file="chase_may.pdf",
        text_lines=[
            "Chase Bank Statement",
            "Account Number: 1234",
            "Statement Period: 01/01/2024",
            "To: 01/31/2024",
            f"Beginning Balance: ${opening}",
            f"Ending Balance: ${closing}",
            *(["Minimum Payment Due: $25.00"] if card_markers else []),
            _ROW_REGION_START,
            "01/15/2024  Coffee Shop  -50.00",
            "01/20/2024  Paycheck  150.00",
            _ROW_REGION_END,
        ],
        tables=[
            PdfTable(
                page=1,
                header=_HEADERS,
                rows=[
                    ["01/15/2024", "Coffee Shop", "-50.00"],
                    ["01/20/2024", "Paycheck", "150.00"],
                ],
            )
        ],
    )


def _valid_recipe_dict() -> dict[str, Any]:
    """A Recipe the agent would propose; execute_recipe reproduces 2 rows."""
    return {
        "row_region": {
            "start_anchor": _ROW_REGION_START,
            "end_anchor": _ROW_REGION_END,
        },
        "row_split": r"\s{2,}",
        "fields": [
            {
                "name": "Date",
                "pattern": r"\d{2}/\d{2}/\d{4}",
                "cast": "date",
                "date_format": "%m/%d/%Y",
            },
            {"name": "Description", "pattern": r".+", "cast": "str"},
            {"name": "Amount", "pattern": r"-?\$?[\d,]+\.\d{2}", "cast": "decimal"},
        ],
        "sign_convention": "negative_is_expense",
        "routing": "transactions",
    }


def _agent_rows(extra: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """Rows keyed by recipe field names, as the bridge contract specifies."""
    rows = [
        {"Date": "01/15/2024", "Description": "Coffee Shop", "Amount": "-50.00"},
        {"Date": "01/20/2024", "Description": "Paycheck", "Amount": "150.00"},
    ]
    if extra:
        rows.extend(extra)
    return rows


def _bridge_response(
    *,
    recipe: dict[str, Any] | None = None,
    rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "recipe": recipe if recipe is not None else _valid_recipe_dict(),
        "rows": rows if rows is not None else _agent_rows(),
    }


@pytest.fixture()
def stub_extract(monkeypatch: pytest.MonkeyPatch) -> list[PdfDocument]:
    """Stub PDFExtractor.extract to return docs[0]. Caller sets docs[0]."""
    docs: list[PdfDocument] = [_standard_doc()]

    class _StubExtractor:
        def extract(self, _path: Path) -> PdfDocument:
            return docs[0]

    monkeypatch.setattr(
        "moneybin.extractors.pdf.extractor.PDFExtractor", _StubExtractor
    )
    return docs


def _pdf_path(tmp_path: Path) -> Path:
    path = tmp_path / "chase_may.pdf"
    path.write_bytes(b"%PDF-1.4\n%stub\n")
    return path


def _applied_count() -> float:
    return PDF_BRIDGE_EGRESS_TOTAL.labels(outcome="applied")._value.get()  # type: ignore[reportPrivateUsage]


def _invalid_count() -> float:
    return PDF_BRIDGE_EGRESS_TOTAL.labels(outcome="invalid")._value.get()  # type: ignore[reportPrivateUsage]


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_apply_reconciling_response_loads_transactions(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    before = _applied_count()
    result = ImportService(db).apply_pdf_bridge_response(
        _pdf_path(tmp_path), _bridge_response()
    )

    assert isinstance(result, BridgeApplyResult)
    assert result.outcome == "applied"
    assert result.import_id is not None
    assert result.rows_loaded == 2
    assert result.reject_reason is None
    assert _applied_count() == before + 1

    loaded = db.conn.execute(
        f"SELECT COUNT(*) FROM {TABULAR_TRANSACTIONS.full_name} "  # noqa: S608  # TableRef constant, not user input
        "WHERE source_type = 'pdf'"
    ).fetchone()
    assert loaded is not None and loaded[0] == 2


def test_apply_persists_new_format(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    result = ImportService(db).apply_pdf_bridge_response(
        _pdf_path(tmp_path), _bridge_response()
    )

    assert result.format_name is not None
    saved = db.conn.execute(
        f"SELECT COUNT(*) FROM {PDF_FORMATS.full_name} WHERE name = ?",  # noqa: S608  # TableRef constant, not user input
        [result.format_name],
    ).fetchone()
    assert saved is not None and saved[0] == 1


def test_apply_records_bridge_provenance_on_the_saved_format(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    """A bridge-authored recipe must be distinguishable from a machine guess.

    Both rungs share one persist path, so `source` was hardcoded to "detected"
    for each — making a human-vetted, agent-authored recipe byte-identical in
    provenance to an auto-derive. Self-heal's Guard A keys on exactly this
    column to decide whether it may overwrite a recipe with a fresh derivation,
    so without the distinction it would silently discard the anchors the bridge
    round deliberately authored.
    """
    result = ImportService(db).apply_pdf_bridge_response(
        _pdf_path(tmp_path), _bridge_response()
    )

    row = db.conn.execute(
        f"SELECT source FROM {PDF_FORMATS.full_name} WHERE name = ?",  # noqa: S608  # TableRef constant, not user input
        [result.format_name],
    ).fetchone()
    assert row is not None and row[0] == "bridge"


def test_apply_save_format_false_skips_persist(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    result = ImportService(db).apply_pdf_bridge_response(
        _pdf_path(tmp_path), _bridge_response(), save_format=False
    )

    assert result.outcome == "applied"
    rows = db.conn.execute(
        f"SELECT COUNT(*) FROM {PDF_FORMATS.full_name}"  # noqa: S608  # TableRef constant, not user input
    ).fetchone()
    assert rows is not None and rows[0] == 0


def test_apply_inverted_recipe_requires_human_confirmation(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    """A bridge agent cannot silently choose a whole-ledger inversion."""
    from moneybin.services.import_confirmation import ImportConfirmationRequiredError

    # Reconciliation operates on the statement's source signs, before the
    # loader canonicalizes them, so this is a genuinely reconciling proposal.
    recipe = {**_valid_recipe_dict(), "sign_convention": "negative_is_income"}
    before = PDF_SIGN_GATE_TOTAL.labels(outcome="proposed")._value.get()  # type: ignore[reportPrivateUsage]

    with pytest.raises(ImportConfirmationRequiredError) as exc:
        ImportService(db).apply_pdf_bridge_response(
            _pdf_path(tmp_path), _bridge_response(recipe=recipe)
        )

    from moneybin.services.import_confirmation import SignConventionProposal

    assert exc.value.outcome.reason == "sign_convention"
    assert isinstance(exc.value.outcome.proposed, SignConventionProposal)
    assert exc.value.outcome.proposed.sign_convention == "negative_is_income"
    assert PDF_SIGN_GATE_TOTAL.labels(outcome="proposed")._value.get() == before + 1  # type: ignore[reportPrivateUsage]
    loaded = db.conn.execute(
        f"SELECT COUNT(*) FROM {TABULAR_TRANSACTIONS.full_name} "  # noqa: S608  # TableRef constant, not user input
        "WHERE source_type = 'pdf'"
    ).fetchone()
    assert loaded is not None and loaded[0] == 0


def test_apply_inverted_recipe_loads_after_human_confirmation(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    """Only the explicit human-confirmed service path may load an inversion."""
    recipe = {**_valid_recipe_dict(), "sign_convention": "negative_is_income"}

    result = ImportService(db).apply_pdf_bridge_response(
        _pdf_path(tmp_path),
        _bridge_response(recipe=recipe),
        confirm=True,
    )

    assert result.outcome == "applied"
    assert result.rows_loaded == 2
    assert result.format_name is not None

    import json as _json

    row = db.conn.execute(
        f"SELECT extraction_recipe FROM {PDF_FORMATS.full_name} WHERE name = ?",  # noqa: S608  # TableRef constant, not user input
        [result.format_name],
    ).fetchone()
    assert row is not None
    assert _json.loads(row[0])["sign_ratified"] is True


def test_marker_backed_inverted_recipe_keeps_polarity_guard_after_confirmation(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    """Card disclosures keep future replay checks active after confirmation."""
    import json as _json

    stub_extract[0] = _standard_doc(card_markers=True)
    recipe = {**_valid_recipe_dict(), "sign_convention": "negative_is_income"}

    result = ImportService(db).apply_pdf_bridge_response(
        _pdf_path(tmp_path), _bridge_response(recipe=recipe), confirm=True
    )

    assert result.format_name is not None
    row = db.conn.execute(
        f"SELECT extraction_recipe FROM {PDF_FORMATS.full_name} WHERE name = ?",  # noqa: S608  # TableRef constant, not user input
        [result.format_name],
    ).fetchone()
    assert row is not None
    assert _json.loads(row[0])["sign_ratified"] is False


def test_apply_writes_revertable_import_log(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    result = ImportService(db).apply_pdf_bridge_response(
        _pdf_path(tmp_path), _bridge_response()
    )

    log = db.conn.execute(
        "SELECT status, source_type FROM raw.import_log WHERE import_id = ?",
        [result.import_id],
    ).fetchone()
    assert log is not None
    assert log[0] == "complete"
    assert log[1] == "pdf"


# ---------------------------------------------------------------------------
# Reconciliation gate — invalid proposals are rejected, nothing loads
# ---------------------------------------------------------------------------


def test_apply_non_reconciling_response_rejected(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    # closing far from opening so the 2 rows (net 100) can't tie out.
    stub_extract[0] = _standard_doc(opening="1000.00", closing="9999.00")

    before_invalid = _invalid_count()
    result = ImportService(db).apply_pdf_bridge_response(
        _pdf_path(tmp_path), _bridge_response()
    )

    assert result.outcome == "invalid"
    assert result.reject_reason == "reconciliation_failed"
    assert result.import_id is None
    assert result.rows_loaded == 0
    assert _invalid_count() == before_invalid + 1

    loaded = db.conn.execute(
        f"SELECT COUNT(*) FROM {TABULAR_TRANSACTIONS.full_name} "  # noqa: S608  # TableRef constant, not user input
        "WHERE source_type = 'pdf'"
    ).fetchone()
    assert loaded is not None and loaded[0] == 0


def test_apply_invalid_does_not_persist_format(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    stub_extract[0] = _standard_doc(opening="1000.00", closing="9999.00")

    ImportService(db).apply_pdf_bridge_response(_pdf_path(tmp_path), _bridge_response())

    rows = db.conn.execute(
        f"SELECT COUNT(*) FROM {PDF_FORMATS.full_name}"  # noqa: S608  # TableRef constant, not user input
    ).fetchone()
    assert rows is not None and rows[0] == 0


# ---------------------------------------------------------------------------
# Divergence — agent's claimed rows vs the recipe's re-executed rows
# ---------------------------------------------------------------------------


def test_apply_no_divergence_when_agent_rows_match(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    result = ImportService(db).apply_pdf_bridge_response(
        _pdf_path(tmp_path), _bridge_response()
    )

    assert result.rows_diverged is False
    assert result.expected_row_count == 2
    assert result.actual_row_count == 2


def test_apply_row_count_divergence_reported_but_still_loads(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    # Agent claims a 3rd row that the recipe (run against the doc) won't
    # reproduce — the recipe only sees 2 lines in the text region. The
    # reconciliation gate runs on the 2 re-executed rows (which tie out),
    # so the load proceeds, but the divergence is surfaced.
    phantom = [{"Date": "01/25/2024", "Description": "Ghost", "Amount": "-5.00"}]
    result = ImportService(db).apply_pdf_bridge_response(
        _pdf_path(tmp_path), _bridge_response(rows=_agent_rows(extra=phantom))
    )

    assert result.outcome == "applied"
    assert result.rows_diverged is True
    assert result.expected_row_count == 3
    assert result.actual_row_count == 2
    # Loaded the re-executed (actual) rows, NOT the agent's claimed 3.
    assert result.rows_loaded == 2


# ---------------------------------------------------------------------------
# Malformed input
# ---------------------------------------------------------------------------


def test_apply_malformed_response_raises_bridge_response_error(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    from moneybin.extractors.pdf.bridge import BridgeResponseError

    # Pin the exact type — parse_bridge_response raises BridgeResponseError, not
    # a bare ValueError, so a regression at the raise site is caught.
    with pytest.raises(BridgeResponseError, match="recipe"):
        ImportService(db).apply_pdf_bridge_response(_pdf_path(tmp_path), {"rows": []})


def test_apply_malformed_response_bumps_invalid_metric(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    from moneybin.extractors.pdf.bridge import BridgeResponseError

    # A parse/validation failure is an "invalid" bridge egress per the metric's
    # documented semantics — it must bump the counter even though it raises
    # before the reconciliation gate's own invalid bump.
    before = _invalid_count()
    with pytest.raises(BridgeResponseError):
        ImportService(db).apply_pdf_bridge_response(_pdf_path(tmp_path), {"rows": []})
    assert _invalid_count() == before + 1


def test_apply_persists_a_bridge_recipe_unratified(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    """The bridge's own recipes save with the guard armed.

    ``sign_ratified`` records a human's `--sign` override. A bridge-authored
    recipe is an agent's inference, so the polarity guard must keep second-guessing
    it on every replay, exactly as it does an auto-derived one.
    """
    import json as _json

    result = ImportService(db).apply_pdf_bridge_response(
        _pdf_path(tmp_path), _bridge_response()
    )

    assert result.format_name is not None
    row = db.conn.execute(
        f"SELECT extraction_recipe FROM {PDF_FORMATS.full_name} WHERE name = ?",  # noqa: S608  # TableRef constant, not user input
        [result.format_name],
    ).fetchone()
    assert row is not None
    assert _json.loads(row[0])["sign_ratified"] is False


def test_apply_rejects_a_response_that_self_grants_sign_ratified(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    """The bridge is not the user, and cannot ratify a sign convention as one.

    ``apply_pdf_bridge_response`` skips the sign confirm gate by design and
    persists the recipe it is handed. A response carrying ``sign_ratified`` would
    therefore short-circuit ``recipe_polarity_fits`` on every future statement of
    that fingerprint — a silent, agent-granted ledger inversion, with no human in
    the loop. Nothing loads and nothing persists.
    """
    from moneybin.extractors.pdf.bridge import BridgeResponseError

    hijacked = {**_valid_recipe_dict(), "sign_ratified": True}
    with pytest.raises(BridgeResponseError, match="sign_ratified"):
        ImportService(db).apply_pdf_bridge_response(
            _pdf_path(tmp_path), _bridge_response(recipe=hijacked)
        )

    formats = db.conn.execute(
        f"SELECT COUNT(*) FROM {PDF_FORMATS.full_name}"  # noqa: S608  # TableRef constant, not user input
    ).fetchone()
    assert formats is not None and formats[0] == 0
    loaded = db.conn.execute(
        f"SELECT COUNT(*) FROM {TABULAR_TRANSACTIONS.full_name} "  # noqa: S608  # TableRef constant, not user input
        "WHERE source_type = 'pdf'"
    ).fetchone()
    assert loaded is not None and loaded[0] == 0


def test_apply_uncompilable_regex_raises_bridge_response_error(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    from moneybin.extractors.pdf.bridge import BridgeResponseError

    # An uncompilable regex is rejected at parse (→ bridge_response_invalid at
    # the MCP boundary), not left to raise a cryptic regex.error inside
    # route_forced_recipe after being counted as a failed PDF import.
    bad = {**_valid_recipe_dict(), "row_split": "["}
    with pytest.raises(BridgeResponseError, match="invalid regex"):
        ImportService(db).apply_pdf_bridge_response(
            _pdf_path(tmp_path), _bridge_response(recipe=bad)
        )


# ---------------------------------------------------------------------------
# Format-name honesty + extraction-failure metric
# ---------------------------------------------------------------------------


def test_apply_bumps_version_when_format_preexists(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    # First apply persists the format at version 1. A second apply of the same
    # layout fingerprint can't save_new again (ConstraintException); instead of
    # leaving the stale recipe untouched (the old replay-failure dead end), it
    # bumps the recipe to a new version (Req 9a auto-bump) so the next
    # same-fingerprint statement replays the corrected recipe rather than
    # re-escalating. format_name is reported (the bump persisted) and version
    # increments.
    svc = ImportService(db)
    first = svc.apply_pdf_bridge_response(_pdf_path(tmp_path), _bridge_response())
    assert first.format_name is not None
    assert first.rows_loaded == 2  # happy-path load works (not silently failing)

    second = svc.apply_pdf_bridge_response(_pdf_path(tmp_path), _bridge_response())
    assert second.outcome == "applied"
    assert second.import_id is not None
    # The bump persisted a new recipe version, so format_name is reported (not
    # None) — and it's the same fingerprint-derived name as the first save.
    assert second.format_name == first.format_name

    row = db.conn.execute(
        f"SELECT version FROM {PDF_FORMATS.full_name} WHERE name = ?",  # noqa: S608  # TableRef constant, not user input
        [first.format_name],
    ).fetchone()
    assert row is not None and row[0] == 2


def test_apply_bump_updates_stored_recipe(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    # The replay-failure bridge case: a saved recipe stopped serving the layout
    # and the agent returns a corrected recipe. The bump must replace the stored
    # extraction_recipe with the new one (not just increment version), so the
    # next replay uses the corrected recipe.
    import json

    svc = ImportService(db)
    svc.apply_pdf_bridge_response(_pdf_path(tmp_path), _bridge_response())

    # A detectable, still-reconciling change to the Description field pattern.
    # Date + Amount (what reconciliation reads) are unchanged, so the corrected
    # recipe still ties out and reaches the save branch.
    corrected = _valid_recipe_dict()
    corrected["fields"][1]["pattern"] = r"\S.*"
    result = svc.apply_pdf_bridge_response(
        _pdf_path(tmp_path), _bridge_response(recipe=corrected)
    )
    assert result.format_name is not None

    stored = db.conn.execute(
        f"SELECT extraction_recipe FROM {PDF_FORMATS.full_name} WHERE name = ?",  # noqa: S608  # TableRef constant, not user input
        [result.format_name],
    ).fetchone()
    assert stored is not None
    recipe_json = json.loads(stored[0])
    desc_field = next(f for f in recipe_json["fields"] if f["name"] == "Description")
    assert desc_field["pattern"] == r"\S.*"


def test_apply_format_name_none_when_save_fails(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    # save_new is best-effort and swallows failures. If it fails for a
    # non-preexisting reason (DB unavailable, concurrent race), the rows still
    # load but format_name must be None — the result must not claim a recipe
    # was persisted when it wasn't (the agent can't read the warning log).
    import pytest as _pytest

    def _boom(self: object, **_kw: object) -> None:
        raise RuntimeError("app.pdf_formats unavailable")

    with _pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "moneybin.repositories.pdf_formats_repo.PdfFormatsRepo.save_new", _boom
        )
        result = ImportService(db).apply_pdf_bridge_response(
            _pdf_path(tmp_path), _bridge_response()
        )

    assert result.outcome == "applied"
    assert result.rows_loaded == 2
    assert result.format_name is None


def test_apply_extraction_failure_bumps_failed_metric(
    db: Database, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # An extraction failure on the bridge path bumps PDF_IMPORT_TOTAL with
    # rung="bridge", mirroring the deterministic _import_pdf path.
    from moneybin.metrics.registry import PDF_IMPORT_TOTAL

    class _BoomExtractor:
        def extract(self, _path: Path) -> PdfDocument:
            raise ValueError("could not extract text from PDF")

    monkeypatch.setattr(
        "moneybin.extractors.pdf.extractor.PDFExtractor", _BoomExtractor
    )
    before = PDF_IMPORT_TOTAL.labels(outcome="failed", rung="bridge")._value.get()  # type: ignore[reportPrivateUsage]

    with pytest.raises(ValueError, match="extract"):
        ImportService(db).apply_pdf_bridge_response(
            _pdf_path(tmp_path), _bridge_response()
        )

    after = PDF_IMPORT_TOTAL.labels(outcome="failed", rung="bridge")._value.get()  # type: ignore[reportPrivateUsage]
    assert after == before + 1
