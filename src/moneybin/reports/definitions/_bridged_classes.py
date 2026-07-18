"""Transitional privacy-class declarations for runner-less reports.* views.

# DEPRECATED: reports-classes-bridge — remove each entry when its view becomes
an @report runner (M2O Phase 2: fold reports_networth into an @report runner;
give uncategorized_queue its own runner; add the report-authoring rule + skill).

Why this exists: sql_query allows the whole `reports` schema, so every deployed
reports.* view's columns must resolve to a declared class or they fall through to
the unmasked AGGREGATE fallback (a masking hole). net_worth predates @report (it
is served by the bespoke reports_networth tool + payloads.networth);
uncategorized_queue has no tool yet. reports_class_map() merges these; the
reports-classification completeness test enforces that every deployed reports.*
view is covered here or by a runner.

Classes mirror the canonical sources: net_worth <- payloads.networth
(NetWorthSnapshotPayload); uncategorized_queue <- large_transactions' shared
columns.
"""

from __future__ import annotations

from moneybin.privacy.taxonomy import DataClass

# (schema, table) -> {column: DataClass}
BRIDGED_REPORT_CLASSES: dict[tuple[str, str], dict[str, DataClass]] = {
    ("reports", "net_worth"): {
        "balance_date": DataClass.TXN_DATE,
        "net_worth": DataClass.BALANCE,
        "account_count": DataClass.AGGREGATE,
        "total_assets": DataClass.BALANCE,
        "total_liabilities": DataClass.BALANCE,
    },
    ("reports", "uncategorized_queue"): {
        "transaction_id": DataClass.RECORD_ID,
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
        "account_name": DataClass.USER_NOTE,
        "txn_date": DataClass.TXN_DATE,
        "amount": DataClass.TXN_AMOUNT,
        "description": DataClass.DESCRIPTION,
        "merchant_id": DataClass.RECORD_ID,
        "merchant_normalized": DataClass.MERCHANT_NAME,
        "age_days": DataClass.AGGREGATE,
        "priority_score": DataClass.AGGREGATE,
        "source_type": DataClass.TXN_TYPE,
        "source_id": DataClass.RECORD_ID,
    },
}
