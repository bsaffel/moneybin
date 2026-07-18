"""Build-time derivation of reports.* column classes (no DB connection)."""

from __future__ import annotations

from pathlib import Path

import pytest

from moneybin.privacy.report_class_derivation import (
    ReportDerivationError,
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
        "uncategorized_queue",
    }


def test_account_id_derives_from_classification_not_the_gap_fallback() -> None:
    """The exact column #330 leaked — but not to the class the bridge guessed.

    #330 left uncategorized_queue.account_id with no declared class at all, so
    it fell through to the unmasked AGGREGATE fallback. The hand-written bridge
    (reports/definitions/_bridged_classes.py) plugged that hole by declaring it
    ACCOUNT_IDENTIFIER — but core.fct_transactions.account_id (and every other
    account_id column) was deliberately reclassified to RECORD_ID in spec D6
    (commit c465f181, "account_id is now opaque by construction, which makes
    the RECORD_ID privacy unmask safe"): it's a minted surrogate key, not
    PII — see taxonomy.py's CLASSIFICATION and
    docs/specs/privacy-data-classification.md. The bridge's ACCOUNT_IDENTIFIER
    entry now disagrees with the authoritative registry; this pins the
    CORRECT derived answer, which is exactly the drift Task 3/4 exist to
    surface and Task 4 to resolve by deleting the stale bridge entry.
    """
    derived = derive_report_classes()
    assert (
        derived[("reports", "uncategorized_queue")]["account_id"] is DataClass.RECORD_ID
    )


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
