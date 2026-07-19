"""Per-report column classification from the report's declared ``classes`` map.

A report declares its output-column→DataClass map on ``@report`` (the privacy
contract); this module maps a query's actual result columns onto it. We declare
rather than derive because SQLMesh deploys each ``reports.*`` view as a
``SELECT * FROM <internal physical table>`` pointer — lineage on the view body
classifies the pointer, not the logic, and falls through to passthrough (a PII
leak). See ADR-013. Declared classes reuse the one redaction path
(``redact_records``) the SQL surface already uses.
"""

from __future__ import annotations

import logging

from moneybin.privacy.sql_lineage import FAIL_CLOSED_CLASS
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import ReportSpec

logger = logging.getLogger(__name__)

# Fail-closed fallback for a result column the report did not declare — the SAME
# constant sql_lineage and sql_query fail closed to, not a second one beside it.
# An undeclared column is a column we cannot name, which is exactly what
# UNRESOLVED means, and it masks WHOLE. The partial-masking CRITICAL classes
# (ACCOUNT_IDENTIFIER, INSTITUTION_ACCOUNT_NUMBER) are the wrong tool here for
# the reason given in UNRESOLVED's taxonomy comment: ``"****" + value[-4:]``
# publishes the last four characters of a value we could not identify, and not
# knowing what those characters are is the whole premise of the fallback.
_FAIL_CLOSED = FAIL_CLOSED_CLASS


def classify_columns(spec: ReportSpec, columns: list[str]) -> dict[str, DataClass]:
    """Class for each result column from the report's declared ``classes`` map.

    A column the report did not declare fails closed to ``_FAIL_CLOSED`` so an
    undeclared column can never leak in the clear. The completeness test asserts
    every real view column is declared, so this fallback should never fire for a
    correctly-declared report — it is defense in depth.
    """
    classified: dict[str, DataClass] = {}
    for col in columns:
        declared = spec.classes.get(col)
        if declared is None:
            logger.warning(
                f"Report {spec.name!r} column {col!r} is undeclared; "
                "failing closed (masked)."
            )
            declared = _FAIL_CLOSED
        classified[col] = declared
    return classified
