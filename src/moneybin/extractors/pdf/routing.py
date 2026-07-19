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
from dataclasses import dataclass, replace
from typing import Any, Literal

from pydantic import ValidationError

from moneybin.database import Database
from moneybin.extractors.pdf.auto_derive import (
    credit_card_markers,
    derivation_failure_reason,
    derive_recipe,
    recipe_polarity_fits,
)
from moneybin.extractors.pdf.column_names import (
    AMOUNT_NAME_RE as _AMOUNT_NAME_RE,
)
from moneybin.extractors.pdf.column_names import (
    CREDIT_NAME_RE as _CREDIT_NAME_RE,
)
from moneybin.extractors.pdf.column_names import (
    DEBIT_NAME_RE as _DEBIT_NAME_RE,
)
from moneybin.extractors.pdf.column_names import (
    DESC_NAME_RE as _DESC_NAME_RE,
)
from moneybin.extractors.pdf.column_names import (
    POST_DATE_NAME_RE as _POST_DATE_NAME_RE,
)
from moneybin.extractors.pdf.confidence import is_high_confidence, score
from moneybin.extractors.pdf.fingerprint import compute_fingerprint, match_format
from moneybin.extractors.pdf.ir import PdfDocument
from moneybin.extractors.pdf.metadata import StatementMetadata, capture_metadata
from moneybin.extractors.pdf.recipe import (
    FieldExtraction,
    Recipe,
    YearlessDateError,
    execute_recipe,
    group_anchors,
)
from moneybin.extractors.pdf.reconciliation import reconcile
from moneybin.metrics.registry import (
    PDF_EXTRACTION_CONFIDENCE,
    PDF_RECIPE_HIT_TOTAL,
    PDF_REPLAY_GUARD_FAILURE_TOTAL,
    PDF_SELF_HEAL_TOTAL,
)
from moneybin.repositories.pdf_formats_repo import PdfFormat, PdfFormatsRepo

logger = logging.getLogger(__name__)

#: Metadata placeholder for derive_recipe, which documents its StatementMetadata
#: parameter as forward-compatible and unused. Safe to share: the dataclass is
#: frozen.
_EMPTY_METADATA = StatementMetadata(
    account_id=None,
    period_start=None,
    period_end=None,
    opening_balance=None,
    closing_balance=None,
)

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

_Reason = Literal[
    "passed",
    "low_confidence",
    "no_transaction_table",  # not a transaction document at all (e.g. positions statement)
    "transaction_table_underivable",  # IS transaction-shaped; derivation failed — Phase 2b bridge
    "reconciliation_failed",  # auto-derived recipe failed reconciliation
    "replay_reconciliation_failed",  # saved recipe stopped reconciling — Phase 2b bridge
    "metadata_incomplete",  # opening or closing balance not captured
    "no_rows",  # recipe matched zero rows
    "unsupported_number_format",  # executor doesn't yet handle this locale
]
# Note: a saved recipe failing Recipe.model_validate is NOT a terminal reason —
# the router logs a warning and falls through to auto-derive, then reports the
# auto-derive outcome via the appropriate reason above.

# Where the recipe the pipeline executes came from. Drives metadata-anchor use
# and replay-metric/reason semantics in _run_recipe_pipeline (see its docstring).
RecipeSource = Literal["replay", "auto_derive", "bridge"]


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
    # Layout fingerprint computed once at the head of route_pdf_import. Threading
    # it through here avoids the importing service having to recompute it (one
    # O(tables × rows) pass) and removes the risk of divergent results if the
    # doc were somehow mutated between calls. None on early seed returns that
    # short-circuit before the fingerprint is computed.
    fp: dict[str, Any] | None = None
    # Card-statement disclosures matched on this document (empty when not a card).
    # The service surfaces these as the evidence behind a negative_is_income
    # proposal — an inversion the user cannot see the basis for is not reviewable.
    card_markers: tuple[str, ...] = ()
    # True when a saved recipe stopped reconciling and was repaired by
    # re-deriving from this document (see _attempt_self_heal). The recipe on
    # this decision is then the FRESH one, not what app.pdf_formats holds, so
    # the service must persist it via bump_version rather than record_use alone.
    rederived: bool = False


# ---------------------------------------------------------------------------
# Confidence helpers
# ---------------------------------------------------------------------------

# Canonical-key regexes live in column_names (imported above) so both
# auto_derive (header → sign convention) and routing (header → canonical
# row-dict key) stay in sync when a header synonym is added. The rows in
# RouteDecision.rows use canonical names ("date", "amount", "debit",
# "credit", "post_date", "description") regardless of what the original
# PDF column headers were called ("Transaction Amount" / "Withdrawals"
# / "Deposit"), so reconcile() and the service layer can both read by
# stable keys instead of re-implementing the same regexes.
#
# "Posting Date" / "Post Date" deserve a separate regex from the generic
# date column: credit-card statements expose BOTH "Transaction Date" and
# "Posting Date", and without splitting them both columns would collapse
# to a single "date" key and the row loop would overwrite the transaction
# date with the posting date.


def _canonical_key(field: FieldExtraction) -> str:
    """Map a recipe FieldExtraction to the canonical row-dict key."""
    name = field.name
    if field.cast == "date":
        if _POST_DATE_NAME_RE.search(name):
            return "post_date"
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


_AMOUNT_FIELD_KEYS: frozenset[str] = frozenset({"amount", "debit", "credit"})


def is_amount_field(field: FieldExtraction) -> bool:
    """True if the field extracts a numeric transaction amount.

    The single definition of "amount field", shared by the confidence model
    (required-fields gate) and the Phase 2b bridge parser (which rejects an
    agent recipe lacking one — otherwise confidence passes on the date field
    alone and a zero-delta statement reconciles all-zero rows). Public because
    ``bridge.parse_bridge_response`` imports it for that gate.
    """
    return (
        field.cast in ("decimal", "int") and _canonical_key(field) in _AMOUNT_FIELD_KEYS
    )


def is_primary_date_field(field: FieldExtraction) -> bool:
    """True if the field is the primary transaction date (canonical ``date``).

    A ``post_date``-only recipe does NOT qualify: the loader writes ``row['date']``
    into the NOT NULL ``transaction_date`` column, so the bridge parser requires
    a primary date field to reject an amount-only/post-date-only recipe with a
    clean error rather than a downstream DB constraint failure. Public because
    ``bridge.parse_bridge_response`` imports it for that gate.

    Requires ``cast == "date"`` (mirroring ``is_amount_field``'s cast check):
    ``_canonical_key`` maps a non-date-cast field named "Date" to ``date`` via
    its fallback, but ``execute_recipe`` only date-parses ``cast == "date"``
    fields, so a str/int "Date" would write an unparsed value to the column.
    """
    return field.cast == "date" and _canonical_key(field) == "date"


def amount_shape_matches_sign_convention(
    fields: list[FieldExtraction], sign_convention: str
) -> bool:
    """True if the recipe's amount fields match the keys ``reconcile`` reads for the convention.

    ``reconciliation._sum_pre_normalization`` reads convention-specific canonical
    keys: only ``amount`` for ``negative_is_expense``/``negative_is_income``, only
    ``credit``/``debit`` for ``split_debit_credit``. A recipe that declares one
    convention but supplies the other shape (e.g. ``negative_is_expense`` with only
    debit/credit, or ``split_debit_credit`` with only ``amount``) sums absent keys
    to 0, so a zero-delta statement reconciles and the loader writes every amount as
    0. The bridge parser rejects such a mismatch up front. Kept beside
    ``_canonical_key`` (the canonical-key authority ``reconcile`` also reads by) so
    the two cannot drift. Public because ``bridge.parse_bridge_response`` imports it.
    """
    if sign_convention in ("negative_is_expense", "negative_is_income"):
        return any(
            f.cast in ("decimal", "int") and _canonical_key(f) == "amount"
            for f in fields
        )
    # split_debit_credit — reconcile reads the credit/debit keys, not amount.
    return any(
        f.cast in ("decimal", "int") and _canonical_key(f) in ("debit", "credit")
        for f in fields
    )


def _canonicalize_rows(
    recipe: Recipe, rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Map each row's PDF-header keys to canonical names (date/amount/etc.).

    Build the per-field mapping once from recipe.fields and reuse it across
    every row so the work is O(rows · fields) once, not per-cell-per-call.

    Multiple source columns can canonicalise to ``description`` — a wrapped row
    reconstructed by shape splits into several middle cells (merchant + a
    post-date or reference column), each named ``Description_n`` so
    ``execute_recipe`` keeps them distinct, and every such name matches
    ``DESC_NAME_RE``. Their values are JOINED in field order rather than
    overwritten, so no merchant/detail component is silently dropped. Only
    ``description`` is joined: it is the sole string-cast key a recipe can
    legitimately repeat (two ``amount``/``date`` columns would be a malformed
    recipe, and joining cast Decimals/dates is meaningless).
    """
    if not rows:
        return rows
    key_map = {f.name: _canonical_key(f) for f in recipe.fields}
    canonical: list[dict[str, Any]] = []
    for row in rows:
        merged: dict[str, Any] = {}
        for k, v in row.items():
            ckey = key_map.get(k, k.lower())
            if ckey == "description" and ckey in merged:
                merged[ckey] = f"{merged[ckey]} {v}".strip()
            else:
                merged[ckey] = v
        canonical.append(merged)
    return canonical


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
        # `f.name` carries the original PDF column header ("Transaction
        # Amount", "Withdrawals", "Deposits", "Post Date" …); a literal
        # lowercase compare against {"amount","debit","credit"} would never
        # match those, defeating the required-fields gate. is_amount_field uses
        # _canonical_key so the same canonicalisation that maps row keys also
        # drives the confidence model.
        is_amount = is_amount_field(f)
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
            # Clear saved_format too so the success path below doesn't set
            # matched_format_name from this invalid saved row — the service
            # treats a populated matched_format_name as "this is a replay,
            # skip save_new", which would leave the broken recipe in place
            # for every future import.
            saved_format = None

    if recipe is not None and not recipe_polarity_fits(recipe, doc):
        # The fingerprint matched but the sign convention doesn't fit this
        # document — see recipe_polarity_fits. Fall through to auto-derive, whose
        # own all-positive guard routes the document to seed / the bridge instead
        # of importing every charge with its sign inverted.
        logger.warning(
            f"Saved recipe {saved_format.name!r} is negative_is_expense but this "
            f"document has no negative amounts — refusing to replay"
            if saved_format is not None
            else "Refusing to replay a sign-mismatched recipe"
        )
        recipe = None
        is_replay = False
        saved_format = None

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
            # derive_recipe collapses every failure into None, and reporting them
            # all as "no_transaction_table" (excluded from bridge escalation)
            # buried real statements in an opaque seed. Ask why it failed so the
            # three outcomes route differently — not a statement (seed), a locale
            # the executor can't replay (seed; the bridge provably can't help),
            # or a statement the deterministic rung couldn't crack (bridge).
            return RouteDecision(
                outcome="seed",
                recipe=None,
                rows=[],
                metadata=StatementMetadata(
                    account_id=None,
                    period_start=None,
                    period_end=None,
                    opening_balance=None,
                    closing_balance=None,
                ),
                confidence=0.0,
                reason=derivation_failure_reason(doc),
                # matched_format_name stays None: early return before saved_format lookup
                fp=fp,
                card_markers=credit_card_markers(doc),
            )

    # Steps 2-5 (execute → reconcile) are shared with the Phase 2b
    # bridge-apply path; see _run_recipe_pipeline.
    matched_name = saved_format.name if saved_format is not None else None
    decision = _run_recipe_pipeline(
        recipe,
        document_text,
        fp,
        recipe_source="replay" if is_replay else "auto_derive",
        matched_format_name=matched_name,
        card_markers=credit_card_markers(doc),
    )

    if decision.reason == "replay_reconciliation_failed" and saved_format is not None:
        healed = _attempt_self_heal(doc, document_text, fp, saved_format, decision)
        if healed is not None:
            return healed
    return decision


def _attempt_self_heal(
    doc: PdfDocument,
    document_text: str,
    fp: dict[str, Any],
    saved_format: PdfFormat,
    failed: RouteDecision,
) -> RouteDecision | None:
    """Re-derive a saved recipe that stopped reconciling; None if unrepairable.

    A persisted recipe is a frozen snapshot of the derivation logic that produced
    it, so a fix to auto_derive can never reach a format already in
    app.pdf_formats — the saved copy keeps mis-parsing forever and every future
    statement of that layout seeds. This adds the missing rung to the existing
    escalation ladder: replay → **re-derive** → seed.

    Repair is deliberately narrow, because a recipe rewrite the user never sees
    is exactly the kind of magic that has to stay bounded:

    - **Only machine-derived formats** (``source == "detected"``). A bridge- or
      manually-authored recipe encodes human intent; replacing it with an
      auto-derived guess would silently discard that work.
    - **Never a sign-convention change.** ``bump_version`` mirrors the recipe's
      ``sign_convention`` into the column every reader trusts, so an unguarded
      heal could invert every amount on the statement with no human in the loop.
      Repairing a *pattern* is safe; flipping *polarity* is not.

    The fresh recipe earns its place by clearing the same ±1c reconciliation gate
    a first-contact recipe must clear — it is proven against this document, not
    assumed. Anything short of that returns None and the caller keeps the
    original seed decision.
    """
    # Reuses the failed decision's markers: a pure function of doc, which has not
    # changed, and scanning the whole document a second time buys nothing.
    card_markers = failed.card_markers
    saved_recipe = failed.recipe

    if saved_format.source != "detected":
        logger.info(
            f"Saved format {saved_format.name!r} failed reconciliation but its "
            f"source is {saved_format.source!r}, not 'detected' — declining to "
            f"overwrite a human-authored recipe; routing to seed"
        )
        PDF_SELF_HEAL_TOTAL.labels(outcome="refused_not_detected").inc()
        return None

    fresh = derive_recipe(doc, _EMPTY_METADATA)
    if fresh is None:
        logger.info(
            f"Saved format {saved_format.name!r} failed reconciliation and the "
            f"document could not be re-derived ({derivation_failure_reason(doc)})"
            f" — routing to seed"
        )
        PDF_SELF_HEAL_TOTAL.labels(outcome="underivable").inc()
        return None

    if saved_recipe is not None:
        if fresh.sign_convention != saved_recipe.sign_convention:
            logger.warning(
                f"Re-derived recipe for format {saved_format.name!r} changes the "
                f"sign convention ({saved_recipe.sign_convention} → "
                f"{fresh.sign_convention}) — refusing to invert the ledger without "
                f"review; routing to seed"
            )
            PDF_SELF_HEAL_TOTAL.labels(outcome="refused_sign_change").inc()
            return None

        # Carry the human's ratification forward. The sign convention is identical
        # (guarded just above), so the decision the user already made still holds;
        # letting it reset to False would re-prompt for an inversion they signed off
        # on.
        fresh.sign_ratified = saved_recipe.sign_ratified

    # recipe_source="auto_derive": the recipe is freshly derived, so it carries no
    # metadata_anchors of its own and must fall back to DEFAULT_ANCHORS — and the
    # replay-guard metrics already fired for the failed attempt above.
    # matched_format_name is kept so the repair updates the EXISTING format row
    # instead of colliding with it via save_new (which raises on a duplicate name).
    retry = _run_recipe_pipeline(
        fresh,
        document_text,
        fp,
        recipe_source="auto_derive",
        matched_format_name=saved_format.name,
        card_markers=card_markers,
    )
    if retry.outcome != "transactions":
        logger.info(
            f"Saved format {saved_format.name!r} failed reconciliation and the "
            f"re-derived recipe did not reconcile either (reason={retry.reason})"
            f" — routing to seed"
        )
        PDF_SELF_HEAL_TOTAL.labels(outcome="still_unreconciled").inc()
        return None

    logger.info(
        f"Saved format {saved_format.name!r} failed reconciliation but a "
        f"re-derived recipe reconciles — repairing the saved recipe in place"
    )
    PDF_SELF_HEAL_TOTAL.labels(outcome="repaired").inc()
    return replace(retry, rederived=True)


def route_forced_recipe(doc: PdfDocument, recipe: Recipe) -> RouteDecision:
    """Route a PDF through a caller-supplied (bridge-authored) recipe.

    The Phase 2b bridge apply entry point. The driving agent proposed this
    recipe; rather than trust the agent's returned rows, the service re-runs
    the recipe here through the same execute → confidence → reconcile gate a
    replay uses, so the persisted recipe is proven to reconcile against the
    document before any rows load. Skips fingerprint lookup and auto-derive —
    the recipe is given, not selected.

    ``matched_format_name`` stays None: a bridge apply is first contact, so the
    service persists the recipe via ``save_new``. The replay metrics never
    fire (this is not a replay of a *saved* recipe); a reconciliation failure
    reports ``reconciliation_failed`` (an invalid proposal), not
    ``replay_reconciliation_failed`` (a drifted saved recipe).
    """
    document_text = "\n".join(doc.text_lines)
    fp = compute_fingerprint(doc)
    return _run_recipe_pipeline(
        recipe,
        document_text,
        fp,
        recipe_source="bridge",
        matched_format_name=None,
        card_markers=credit_card_markers(doc),
    )


def _run_recipe_pipeline(
    recipe: Recipe,
    document_text: str,
    fp: dict[str, Any],
    *,
    recipe_source: RecipeSource,
    matched_format_name: str | None,
    card_markers: tuple[str, ...] = (),
) -> RouteDecision:
    """Execute a recipe and apply the confidence + reconciliation gates.

    Shared engine downstream of recipe *selection*: given a recipe and the
    document text, run execute → canonicalize → confidence → metadata →
    reconcile and return the routing decision. All three recipe sources feed
    this — fingerprint ``replay``, first-contact ``auto_derive``, and the
    Phase 2b ``bridge`` apply — so a bridge-authored recipe passes the *same*
    gate as a saved one; a persisted recipe is always proven to reconcile
    before the service loads it.

    ``recipe_source`` drives two source-specific behaviors:

    - **metadata anchors** — ``replay`` and ``bridge`` recipes carry their own
      ``metadata_anchors`` (non-default balance/account labels); ``auto_derive``
      has none and falls back to ``DEFAULT_ANCHORS``.
    - **replay metrics + reason** — only a true ``replay`` of a *saved* recipe
      trips the replay guard (``PDF_REPLAY_GUARD_FAILURE_TOTAL``,
      ``replay_reconciliation_failed``). A ``bridge`` proposal that doesn't tie
      out is just an invalid proposal (``reconciliation_failed``), not a drift
      signal.

    Args:
        recipe: Recipe to execute (from replay, auto-derive, or bridge).
        document_text: The document's text lines joined by newlines.
        fp: Layout fingerprint, threaded onto every returned decision.
        recipe_source: Where the recipe came from — see behaviors above.
        matched_format_name: Saved format name on replay; None otherwise
            (signals first-contact save to the service).
        card_markers: Card disclosures matched on the document, threaded onto
            every returned decision (empty when not a card).
    """
    use_recipe_anchors = recipe_source in ("replay", "bridge")
    is_saved_replay = recipe_source == "replay"
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
            metadata=StatementMetadata(
                account_id=None,
                period_start=None,
                period_end=None,
                opening_balance=None,
                closing_balance=None,
            ),
            confidence=0.0,
            reason="unsupported_number_format",
            matched_format_name=matched_format_name,
            fp=fp,
            card_markers=card_markers,
        )
    except YearlessDateError:
        # A year-less row couldn't be placed in time (no capturable period, or a
        # date beyond posting drift). Fail the WHOLE extraction rather than let the
        # executor silently drop the row. Both reasons are bridge-eligible so an
        # agent can supply the period anchors the deterministic path lacked; on a
        # saved-recipe replay use the replay reason + guard flag + the same
        # replay-failure telemetry the reconciliation path emits, so a saved format
        # whose year-less rows stop resolving is counted rather than silently
        # escaping the guard metric. (First-contact uses transaction_table_underivable
        # — a recipe existed but a row wouldn't cast; it is bridge-eligible either
        # way, so the label difference from reconciliation_failed is cosmetic.)
        if is_saved_replay:
            PDF_RECIPE_HIT_TOTAL.labels(outcome="replay_failed").inc()
            PDF_REPLAY_GUARD_FAILURE_TOTAL.inc()
        logger.warning(
            "execute_recipe: year-less date row unresolvable — routing to bridge"
        )
        return RouteDecision(
            outcome="seed",
            recipe=recipe,
            rows=[],
            metadata=StatementMetadata(
                account_id=None,
                period_start=None,
                period_end=None,
                opening_balance=None,
                closing_balance=None,
            ),
            confidence=0.0,
            reason=(
                "replay_reconciliation_failed"
                if is_saved_replay
                else "transaction_table_underivable"
            ),
            replay_guard_failed=is_saved_replay,
            matched_format_name=matched_format_name,
            fp=fp,
            card_markers=card_markers,
        )
    rows = _canonicalize_rows(recipe, extracted.rows)

    if not rows:
        return RouteDecision(
            outcome="seed",
            recipe=recipe,
            rows=[],
            metadata=StatementMetadata(
                account_id=None,
                period_start=None,
                period_end=None,
                opening_balance=None,
                closing_balance=None,
            ),
            confidence=0.0,
            reason="no_rows",
            matched_format_name=matched_format_name,
            fp=fp,
            card_markers=card_markers,
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
            metadata=StatementMetadata(
                account_id=None,
                period_start=None,
                period_end=None,
                opening_balance=None,
                closing_balance=None,
            ),
            confidence=conf,
            reason="low_confidence",
            matched_format_name=matched_format_name,
            fp=fp,
            card_markers=card_markers,
        )

    # ------------------------------------------------------------------
    # 4. Capture metadata
    # ------------------------------------------------------------------
    # Replay path: use the saved recipe's metadata_anchors so a bridge-authored or
    # manually corrected format with non-default balance/account labels can find
    # its values on replay. group_anchors regroups the flat FieldExtraction list
    # into capture_metadata's {name: [pattern, ...]} shape — preserving multiple
    # alternative patterns per field (e.g. the two default account_id anchors) and
    # the tri-state None / [] / populated semantics of metadata_anchors (see its
    # docstring). First-contact path (use_recipe_anchors False) leaves anchors_dict
    # None so capture falls back to DEFAULT_ANCHORS.
    anchors_dict: dict[str, list[str]] | None = None
    if use_recipe_anchors:
        anchors_dict = group_anchors(recipe.metadata_anchors)
    metadata = capture_metadata(document_text, anchors=anchors_dict)

    if not metadata.is_complete_for_reconciliation():
        return RouteDecision(
            outcome="seed",
            recipe=recipe,
            rows=rows,
            metadata=metadata,
            confidence=conf,
            reason="metadata_incomplete",
            matched_format_name=matched_format_name,
            fp=fp,
            card_markers=card_markers,
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
        if is_saved_replay:
            PDF_RECIPE_HIT_TOTAL.labels(outcome="replay_success").inc()
        return RouteDecision(
            outcome="transactions",
            recipe=recipe,
            rows=rows,
            metadata=metadata,
            confidence=conf,
            reason="passed",
            matched_format_name=matched_format_name,
            fp=fp,
            card_markers=card_markers,
        )

    # Reconciliation failed.
    if is_saved_replay:
        PDF_RECIPE_HIT_TOTAL.labels(outcome="replay_failed").inc()
        PDF_REPLAY_GUARD_FAILURE_TOTAL.inc()
        # Balance values intentionally omitted — `.claude/rules/security.md`
        # forbids logging financial values; the reason code suffices.
        _format_name = matched_format_name or "unknown"
        # States the failure, not the outcome: route_pdf_import re-derives before
        # deciding, so a "falling back to seed" claim here is contradicted by the
        # very next log line whenever the repair succeeds.
        logger.warning(
            f"Replay recipe for format {_format_name!r} failed reconciliation "
            f"(reason={recon.reason}) — attempting re-derivation"
        )
        return RouteDecision(
            outcome="seed",
            recipe=recipe,
            rows=rows,
            metadata=metadata,
            confidence=conf,
            reason="replay_reconciliation_failed",
            replay_guard_failed=True,
            matched_format_name=matched_format_name,
            fp=fp,
            card_markers=card_markers,
        )

    return RouteDecision(
        outcome="seed",
        recipe=recipe,
        rows=rows,
        metadata=metadata,
        confidence=conf,
        reason="reconciliation_failed",
        replay_guard_failed=False,
        # Reached by first-contact auto-derive and by route_forced_recipe
        # (bridge); neither is a saved-recipe replay, so there is no matched
        # format to carry.
        matched_format_name=None,
        fp=fp,
        card_markers=card_markers,
    )
