"""Routing state machine for PDF import (Task 9).

Orchestrates the Phase 2a decision: should this PDF route to
``raw.tabular_transactions`` (deterministic-extraction path) or
``raw.pdf_seeds`` (Phase 1 fallback)?

State machine:

  PdfDocument
    → fingerprint
    → match in app.pdf_formats?
        yes: replay saved recipe → rows
        no:  auto-derive recipe → rows
    → confidence score ≥ threshold?
        no:  RouteDecision(seed, reason="low_confidence")
        yes: capture metadata
             reconcile?
                 pass: RouteDecision(transactions, recipe, rows, metadata)
                 fail (matched recipe): RouteDecision(seed, reason="replay_reconciliation_failed",
                                                      replay_guard_failed=True)
                 fail (auto-derived):   RouteDecision(seed, reason="reconciliation_failed")

Confidence model is intentionally permissive in Phase 2a — reconciliation is
the primary gate.  Phase 2b will introduce per-row partial-fill signals when
LLM extraction enters the loop.
"""

from __future__ import annotations

import logging
import re as _stdlib_re
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import ValidationError

from moneybin.database import Database
from moneybin.extractors.pdf.auto_derive import derive_recipe
from moneybin.extractors.pdf.confidence import is_high_confidence, score
from moneybin.extractors.pdf.fingerprint import compute_fingerprint, match_format
from moneybin.extractors.pdf.ir import PdfDocument
from moneybin.extractors.pdf.metadata import StatementMetadata, capture_metadata
from moneybin.extractors.pdf.recipe import Recipe, execute_recipe
from moneybin.extractors.pdf.reconciliation import reconcile
from moneybin.metrics.registry import (
    PDF_EXTRACTION_CONFIDENCE,
    PDF_RECIPE_HIT_TOTAL,
    PDF_REPLAY_GUARD_FAILURE_TOTAL,
)
from moneybin.repositories.pdf_formats_repo import PdfFormatsRepo

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

_Reason = Literal[
    "passed",
    "low_confidence",
    "no_transaction_table",
    "reconciliation_failed",  # auto-derived recipe failed reconciliation
    "replay_reconciliation_failed",  # saved recipe stopped reconciling — Phase 2b bridge
    "metadata_incomplete",  # opening or closing balance not captured
    "no_rows",  # recipe matched zero rows
    "unsupported_number_format",  # executor doesn't yet handle this locale
]
# Note: a saved recipe failing Recipe.model_validate is NOT a terminal reason —
# the router logs a warning and falls through to auto-derive, then reports the
# auto-derive outcome via the appropriate reason above.


@dataclass(frozen=True)
class RouteDecision:
    """Outcome of the Phase 2a routing state machine."""

    outcome: Literal["transactions", "seed"]
    recipe: Recipe | None
    rows: list[dict[str, Any]]
    metadata: StatementMetadata
    confidence: float
    reason: _Reason
    replay_guard_failed: bool = False
    # saved_format.name when a saved format matched (Replay path); None on auto-derive.
    # The service uses this to decide whether to persist a new recipe (first contact).
    matched_format_name: str | None = None


# ---------------------------------------------------------------------------
# Confidence helpers
# ---------------------------------------------------------------------------

_DATE_FIELD_NAMES = frozenset({
    "date",
    "trans date",
    "transaction date",
    "posting date",
})


# Canonical-key regexes — the rows in RouteDecision.rows use these canonical
# names regardless of what the PDF column headers were called ("Transaction
# Amount" / "Withdrawals" / "Deposit"), so reconcile() and the service layer
# can both read by stable keys instead of re-implementing the same regexes.
_DEBIT_NAME_RE = _stdlib_re.compile(r"debit|withdraw", _stdlib_re.IGNORECASE)
_CREDIT_NAME_RE = _stdlib_re.compile(r"credit|deposit", _stdlib_re.IGNORECASE)
_AMOUNT_NAME_RE = _stdlib_re.compile(r"amount", _stdlib_re.IGNORECASE)
_DESC_NAME_RE = _stdlib_re.compile(r"description|memo|payee", _stdlib_re.IGNORECASE)


def _canonical_key(field: Any) -> str:
    """Map a recipe FieldExtraction to the canonical row-dict key."""
    name = field.name
    if field.cast == "date":
        return "date"
    if field.cast in ("decimal", "int"):
        if _DEBIT_NAME_RE.search(name):
            return "debit"
        if _CREDIT_NAME_RE.search(name):
            return "credit"
        if _AMOUNT_NAME_RE.search(name):
            return "amount"
    if _DESC_NAME_RE.search(name):
        return "description"
    return name.lower()


def _canonicalize_rows(
    recipe: Recipe, rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Map each row's PDF-header keys to canonical names (date/amount/etc.).

    Build the per-field mapping once from recipe.fields and reuse it across
    every row so the work is O(rows · fields) once, not per-cell-per-call.
    """
    if not rows:
        return rows
    key_map = {f.name: _canonical_key(f) for f in recipe.fields}
    return [{key_map.get(k, k.lower()): v for k, v in row.items()} for row in rows]


def _compute_confidence(recipe: Recipe, rows: list[dict[str, Any]]) -> float:
    """Compute confidence from recipe fields and extracted rows.

    Phase 2a simplification: since execute_recipe only emits rows where all
    fields matched, per-field "filled" is binary — a field either produced at
    least one non-None value (i.e. any rows exist) or it didn't (no rows).
    Required fields are date + amount/debit+credit; everything else is important.
    """
    required_total = 0
    important_total = 0

    for f in recipe.fields:
        is_date = f.cast == "date"
        is_amount = f.cast in ("decimal", "int") and f.name.lower() in (
            "amount",
            "debit",
            "credit",
        )
        if is_date or is_amount:
            required_total += 1
        else:
            important_total += 1

    # Binary fill: all fields are filled if any rows were extracted.
    required_filled = required_total if rows else 0
    important_filled = important_total if rows else 0

    return score(
        required_filled=required_filled,
        required_total=required_total,
        important_filled=important_filled,
        important_total=important_total,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def route_pdf_import(doc: PdfDocument, db: Database) -> RouteDecision:
    """Run the Phase 2a state machine and return a routing decision.

    Args:
        doc: Parsed PDF document (Phase 1 IR).
        db: Open Database instance; used only for ``PdfFormatsRepo`` reads.

    Returns:
        A ``RouteDecision`` whose ``outcome`` is either ``"transactions"`` or
        ``"seed"``.  On the ``"seed"`` path, ``reason`` explains why.
    """
    document_text = "\n".join(doc.text_lines)

    # ------------------------------------------------------------------
    # 1. Fingerprint + format lookup
    # ------------------------------------------------------------------
    fp = compute_fingerprint(doc)
    repo = PdfFormatsRepo(db)
    saved_format = match_format(fp, repo)

    is_replay = saved_format is not None
    recipe: Recipe | None = None

    if saved_format is not None:
        # Try to deserialise the saved recipe. Validation failure is not fatal —
        # it may mean the schema evolved; fall through to auto-derive and log a
        # warning (Req 19: no silent failure).
        try:
            recipe = Recipe.model_validate(saved_format.extraction_recipe)
        except ValidationError:
            logger.warning(
                f"Saved recipe for format {saved_format.name!r} failed "
                f"model_validate — falling back to auto-derive"
            )
            recipe = None
            is_replay = False  # treat as auto-derive for replay_guard semantics

    if recipe is None:
        # Auto-derive: metadata not yet captured; derive_recipe accepts an
        # empty StatementMetadata (documented as forward-compatible unused).
        empty_meta = StatementMetadata(
            account_id=None,
            period_start=None,
            period_end=None,
            opening_balance=None,
            closing_balance=None,
        )
        recipe = derive_recipe(doc, empty_meta)
        if recipe is None:
            return RouteDecision(
                outcome="seed",
                recipe=None,
                rows=[],
                metadata=StatementMetadata(None, None, None, None, None),
                confidence=0.0,
                reason="no_transaction_table",
                # matched_format_name stays None: early return before saved_format lookup
            )

    # ------------------------------------------------------------------
    # 2. Execute recipe → rows
    # ------------------------------------------------------------------
    try:
        extracted = execute_recipe(recipe, document_text)
    except NotImplementedError:
        # Recipe declares a number_format the executor can't handle yet
        # (e.g. european, swiss_french). Route to seed instead of failing
        # the whole import — the PDF can still land as a seed for later
        # reprocessing once the executor adds that locale.
        logger.warning(
            f"execute_recipe: unsupported number_format "
            f"{recipe.number_format!r} — routing to seed"
        )
        return RouteDecision(
            outcome="seed",
            recipe=recipe,
            rows=[],
            metadata=StatementMetadata(None, None, None, None, None),
            confidence=0.0,
            reason="unsupported_number_format",
            matched_format_name=saved_format.name if saved_format is not None else None,
        )
    rows = _canonicalize_rows(recipe, extracted.rows)

    if not rows:
        return RouteDecision(
            outcome="seed",
            recipe=recipe,
            rows=[],
            metadata=StatementMetadata(None, None, None, None, None),
            confidence=0.0,
            reason="no_rows",
            matched_format_name=saved_format.name if saved_format is not None else None,
        )

    # ------------------------------------------------------------------
    # 3. Confidence score
    # ------------------------------------------------------------------
    conf = _compute_confidence(recipe, rows)
    # Observe every routing call — histogram reveals confidence distribution
    # across all PDFs and helps tune the is_high_confidence threshold.
    PDF_EXTRACTION_CONFIDENCE.observe(conf)
    if not is_high_confidence(conf):
        return RouteDecision(
            outcome="seed",
            recipe=recipe,
            rows=rows,
            metadata=StatementMetadata(None, None, None, None, None),
            confidence=conf,
            reason="low_confidence",
            matched_format_name=saved_format.name if saved_format is not None else None,
        )

    # ------------------------------------------------------------------
    # 4. Capture metadata
    # ------------------------------------------------------------------
    # Replay path: use the saved recipe's metadata_anchors so a bridge-authored
    # or manually corrected format with non-default balance/account labels can
    # actually find its values on replay. Empty list ⇒ fall back to DEFAULT_ANCHORS.
    anchors_dict: dict[str, list[str]] | None = None
    if is_replay and recipe.metadata_anchors:
        anchors_dict = {f.name: [f.pattern] for f in recipe.metadata_anchors}
    metadata = capture_metadata(document_text, anchors=anchors_dict)

    if not metadata.is_complete_for_reconciliation():
        return RouteDecision(
            outcome="seed",
            recipe=recipe,
            rows=rows,
            metadata=metadata,
            confidence=conf,
            reason="metadata_incomplete",
            matched_format_name=saved_format.name if saved_format is not None else None,
        )

    # ------------------------------------------------------------------
    # 5. Reconcile
    # ------------------------------------------------------------------
    # rows are already canonical-keyed (date/amount/debit/credit/description)
    # by _canonicalize_rows above, so reconcile() can read by stable keys
    # regardless of the original PDF column headers ("Transaction Amount",
    # "Withdrawals", "Deposit Amount", etc.).
    recon = reconcile(rows, metadata, recipe.sign_convention)

    if recon.passed:
        if is_replay:
            PDF_RECIPE_HIT_TOTAL.labels(outcome="replay_success").inc()
        return RouteDecision(
            outcome="transactions",
            recipe=recipe,
            rows=rows,
            metadata=metadata,
            confidence=conf,
            reason="passed",
            matched_format_name=saved_format.name if saved_format is not None else None,
        )

    # Reconciliation failed.
    if is_replay:
        PDF_RECIPE_HIT_TOTAL.labels(outcome="replay_failed").inc()
        PDF_REPLAY_GUARD_FAILURE_TOTAL.inc()
        # Balance values intentionally omitted — `.claude/rules/security.md`
        # forbids logging financial values; the reason code suffices.
        _format_name = saved_format.name if saved_format is not None else "unknown"
        logger.warning(
            f"Replay recipe for format {_format_name!r} failed reconciliation "
            f"(reason={recon.reason}) — falling back to seed"
        )
        return RouteDecision(
            outcome="seed",
            recipe=recipe,
            rows=rows,
            metadata=metadata,
            confidence=conf,
            reason="replay_reconciliation_failed",
            replay_guard_failed=True,
            matched_format_name=saved_format.name if saved_format is not None else None,
        )

    return RouteDecision(
        outcome="seed",
        recipe=recipe,
        rows=rows,
        metadata=metadata,
        confidence=conf,
        reason="reconciliation_failed",
        replay_guard_failed=False,
        matched_format_name=None,  # auto-derive path, never a replay
    )
