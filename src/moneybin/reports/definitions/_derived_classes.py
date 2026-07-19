"""Generated: privacy classes for reports.* views with no @report runner.

DO NOT EDIT BY HAND. Regenerate with:
    uv run python scripts/generate_derived_report_classes.py

Replaces the former hand-written reports/definitions/_bridged_classes.py.
Every entry here comes straight from derive_report_classes()
(privacy/report_class_derivation.py), which parses the model's SQL and
reuses resolve_output_classes — the same classifier that masks user SQL
at runtime — so this file cannot drift from the model the way a
hand-maintained bridge could.
tests/privacy/test_sql_query.py::test_generated_classes_are_current
fails CI if this file is stale.
"""

from __future__ import annotations

from moneybin.privacy.taxonomy import DataClass
from moneybin.tables import REPORTS_NET_WORTH, REPORTS_UNCATEGORIZED_QUEUE

# (schema, view) -> {column: DataClass}. Excludes every view already
# covered by an @report runner's own classes= map — see
# generate_derived_report_classes.py.
DERIVED_REPORT_CLASSES: dict[tuple[str, str], dict[str, DataClass]] = {
    (REPORTS_NET_WORTH.schema, REPORTS_NET_WORTH.name): {
        "balance_date": DataClass.TXN_DATE,
        "net_worth": DataClass.BALANCE,
        "account_count": DataClass.AGGREGATE,
        "total_assets": DataClass.BALANCE,
        "total_liabilities": DataClass.BALANCE,
    },
    (REPORTS_UNCATEGORIZED_QUEUE.schema, REPORTS_UNCATEGORIZED_QUEUE.name): {
        "transaction_id": DataClass.RECORD_ID,
        "account_id": DataClass.RECORD_ID,
        "account_name": DataClass.USER_NOTE,
        "txn_date": DataClass.TXN_DATE,
        "amount": DataClass.TXN_AMOUNT,
        "description": DataClass.DESCRIPTION,
        "merchant_id": DataClass.RECORD_ID,
        "merchant_normalized": DataClass.MERCHANT_NAME,
        "age_days": DataClass.TXN_DATE,
        "priority_score": DataClass.TXN_AMOUNT,
        "source_type": DataClass.TXN_TYPE,
        "source_id": DataClass.AGGREGATE,
    },
}
