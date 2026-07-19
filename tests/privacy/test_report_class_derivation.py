"""Build-time derivation of reports.*/core.* column classes (no DB connection)."""

from __future__ import annotations

from pathlib import Path

import pytest

from moneybin.privacy.report_class_derivation import (
    ReportDerivationError,
    derive_core_view_classes,
    derive_report_classes,
)
from moneybin.privacy.taxonomy import DataClass


def _derive_one(model_sql: str, models_root: Path) -> dict[tuple[str, str], object]:
    """Derive a single hand-written model from an isolated models root."""
    (models_root / "probe.sql").write_text(model_sql)
    return derive_report_classes(models_root=models_root)  # type: ignore[return-value]


def test_derives_every_deployed_reports_view() -> None:
    derived = derive_report_classes()
    names = {view for (_schema, view) in derived}
    assert names == {
        "balance_drift",
        "cash_flow",
        "large_transactions",
        "merchant_activity",
        "net_worth",
        "recurring_subscriptions",
        "spending_trend",
    }


def test_account_id_derives_from_classification_not_the_gap_fallback() -> None:
    """The exact column #330 leaked — but not to the class the bridge guessed.

    #330 left uncategorized_queue.account_id with no declared class at all, so
    it fell through to the unmasked AGGREGATE fallback. uncategorized_queue
    itself has since moved to core.* (reports-foundation.md R5, Task 5) and no
    longer appears in this map at all — but every reports.* view that still
    selects account_id unchanged derives the identical answer, because
    core.fct_transactions.account_id (and every other account_id column) was
    deliberately reclassified to RECORD_ID in spec D6 (commit c465f181,
    "account_id is now opaque by construction, which makes the RECORD_ID
    privacy unmask safe"): it's a minted surrogate key, not PII — see
    taxonomy.py's CLASSIFICATION and
    docs/specs/privacy-data-classification.md. cash_flow's runner over-declares
    it ACCOUNT_IDENTIFIER anyway (safe — over-declaring never leaks), but this
    pins derivation's own answer, which is exactly the drift Task 3/4 exist to
    surface.
    """
    derived = derive_report_classes()
    assert derived[("reports", "cash_flow")]["account_id"] is DataClass.RECORD_ID


def test_counting_aggregate_is_not_over_classified() -> None:
    """COUNT(DISTINCT account_id) is AGGREGATE, not account_id's own class.

    Raw lineage would trace this to account_id. The shared classifier's
    counting-aggregate rule is why we reuse it instead of writing a deriver.
    """
    derived = derive_report_classes()
    assert derived[("reports", "net_worth")]["account_count"] is DataClass.AGGREGATE


def test_balance_columns_derive_from_app_schema() -> None:
    """balance_drift reads app.balance_assertions, which has no SQLMesh model.

    Asserts the CLASS, not the tier: BALANCE shares Tier.HIGH with TXN_AMOUNT,
    so a tier-only assertion passed even if the column derived as a transaction
    amount — exactly the confusion this test exists to rule out.
    """
    drift = derive_report_classes()[("reports", "balance_drift")]
    assert drift["asserted_balance"] is DataClass.BALANCE


def test_derivation_never_falls_back_silently(tmp_path: Path) -> None:
    """Derivation must raise, not log-and-guess, on an unresolvable projection.

    The spec's binding constraint. ``resolve_output_classes`` answers a
    projection it cannot resolve with a conservative fallback — correct for user
    SQL, wrong for a map that claims to be *verified* — so the deriver runs it
    with ``strict=True``. This pins the wiring: without it, a model could drift
    into an unresolvable shape and the derived map would absorb the guess with
    only a WARNING to show for it.
    """
    model = """
        MODEL (name reports.unresolvable_probe, kind VIEW);
        SELECT l.x AS probe
        FROM core.fct_transactions AS t,
             LATERAL (SELECT t.amount AS x) AS l
    """
    with pytest.raises(ReportDerivationError, match="probe"):
        _derive_one(model, tmp_path)


def test_derivation_rejects_star_in_a_cte_body(tmp_path: Path) -> None:
    """``SELECT *`` is rejected anywhere, not just in the final projection.

    A star in a CTE body is equally underivable — nothing expands it, so
    ``_output_index`` cannot name-match through it and the column degrades to a
    silent fallback instead of the hard error this check exists to raise.
    """
    model = """
        MODEL (name reports.star_cte_probe, kind VIEW);
        WITH b AS (SELECT * FROM core.fct_transactions)
        SELECT b.amount AS amount FROM b
    """
    with pytest.raises(ReportDerivationError, match="SELECT \\*"):
        _derive_one(model, tmp_path)


# ---------------------------------------------------------------------------
# derive_core_view_classes: the generalized engine, applied to core.* views
#
# Unlike reports.* (every model must derive — see report_class_derivation.py's
# module docstring), most of core.* is out of scope for this connectionless
# deriver: 2 kind-FULL SQL models, 3 Python models, and most of the remaining
# kind-VIEW models read prep.*/seeds.* (ordinary medallion data flow) or use a
# shape (bare SELECT *, an unaliased single-table projection, UNNEST(...)
# struct access) this no-qualify() deriver was never built to resolve. The
# tests below pin BOTH sides of that split exactly, so a change to either
# (a model starting to derive that used not to, or vice versa) requires a
# deliberate edit here rather than silently passing or silently vanishing.
# ---------------------------------------------------------------------------


def test_derives_every_derivable_core_view() -> None:
    derived, _excluded = derive_core_view_classes()
    assert set(derived) == {
        ("core", "dim_merchants"),
        ("core", "uncategorized_queue"),
    }


def test_core_excludes_materialized_tables_by_kind() -> None:
    """A kind-FULL SQL model is excluded before derivation is even attempted."""
    _derived, excluded = derive_core_view_classes()
    assert "kind=FULL" in excluded["core.dim_accounts"]
    assert "kind=FULL" in excluded["core.fct_investment_transactions"]


def test_core_excludes_python_models_by_filename() -> None:
    """A SQLMesh Python model has no SQL text; excluded without being loaded."""
    _derived, excluded = derive_core_view_classes()
    for stem in ("fct_balances_daily", "fct_investment_lots", "fct_realized_gains"):
        assert "python model" in excluded[stem]


def test_core_excludes_views_the_deriver_cannot_resolve() -> None:
    """kind-VIEW models that fail derivation are excluded with the real reason.

    Each of these legitimately reads prep.*/seeds.* (outside CLASSIFICATION's
    core/app ground truth) or uses a shape this deriver can't walk without
    ``qualify()`` (bare ``SELECT *``, or ``UNNEST(...)`` struct-field access) —
    none is a bug to fix here; each is a real, stated scope boundary.
    """
    _derived, excluded = derive_core_view_classes()
    unresolvable = {
        "core.bridge_category_source_map",  # reads seeds.category_source_map
        "core.bridge_transfers",  # reads prep.int_transactions__matched/merged
        "core.dim_categories",  # reads seeds.categories
        "core.dim_holdings",  # reads prep.stg_plaid__investment_holdings*
        "core.dim_securities",  # unaliased single-table SELECT (no qualify())
        "core.fct_balances",  # bare SELECT * inside a UNION ALL branch
        "core.fct_transaction_lines",  # UNNEST(t.splits) struct-field access
        "core.fct_transactions",  # reads prep.int_transactions__merged
    }
    assert unresolvable <= set(excluded)
    for name in unresolvable:
        assert "not resolvable" in excluded[name] or "SELECT *" in excluded[name]


def test_uncategorized_queue_age_days_and_priority_score_derive_correctly() -> None:
    """The two columns #330's finding singled out as the least-verified.

    ``age_days`` is ``CURRENT_DATE - transaction_date`` (a date arithmetic
    expression referencing only ``transaction_date``) and ``priority_score``
    is ``ABS(amount) * age_days`` (referencing both ``amount`` and
    ``transaction_date``, whose classes are TXN_AMOUNT/HIGH and
    TXN_DATE/MEDIUM respectively) — derivation's max-tier-referenced-column
    rule must answer TXN_DATE and TXN_AMOUNT, matching what taxonomy.py
    declares.
    """
    derived, _excluded = derive_core_view_classes()
    queue = derived[("core", "uncategorized_queue")]
    assert queue["age_days"] is DataClass.TXN_DATE
    assert queue["priority_score"] is DataClass.TXN_AMOUNT
