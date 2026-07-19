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
from moneybin.tables import REPORTS_NET_WORTH

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
}
