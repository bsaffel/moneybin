"""Build-time derivation of reports.* column classes (no DB connection)."""

from __future__ import annotations

from moneybin.privacy.report_class_derivation import derive_report_classes
from moneybin.privacy.taxonomy import DataClass


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
    """balance_drift reads app.balance_assertions, which has no SQLMesh model."""
    drift = derive_report_classes()[("reports", "balance_drift")]
    assert drift["asserted_balance"].tier is DataClass.BALANCE.tier
