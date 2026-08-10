"""Tests for the saved-report save contract in ``reports/_framework/derive.py``.

``test_user_reports.py`` drives the same pipeline *through* the service. These
sit at the contract itself: which schemas a durable report may read, and what a
report that reads one of the newly-opened schemas can and cannot become.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.privacy.sql_query import (
    _ALLOWED_QUERY_SCHEMAS,  # pyright: ignore[reportPrivateUsage]
)
from moneybin.reports._framework.derive import SAVE_SCHEMAS
from moneybin.reports._framework.explain import explain_report
from moneybin.services.user_reports_service import UserReportsService


def test_save_schemas_tracks_the_query_gate() -> None:
    """Whatever a user may read ad hoc, they may also save a report over.

    Asserted as equality against the live constant rather than against a literal
    set, so the two can only be changed together. Two hand-kept copies of one
    allowlist is exactly how a schema ends up readable through ``sql_query`` and
    refused at save — or, far worse, the reverse.
    """
    assert SAVE_SCHEMAS == _ALLOWED_QUERY_SCHEMAS


@pytest.fixture
def prep_db(saved_db: Database) -> Database:
    """``saved_db`` plus a prep model, deployed as a VIEW the way SQLMesh does."""
    saved_db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    saved_db.execute(
        "CREATE OR REPLACE VIEW prep.int_transactions__merged AS "
        "SELECT transaction_id, amount FROM core.fct_transactions"
    )
    return saved_db


def test_saved_prep_report_is_not_materializable(prep_db: Database) -> None:
    """A saved ``prep`` report runs, and reports itself unable to graduate.

    The load-bearing check on the decision that ``SAVE_SCHEMAS`` may simply track
    the query gate. Widening the save allowlist is safe only because a *second*,
    independent allowlist governs graduation into a materialized ``reports.*``
    view: ``report_materialization.DERIVABLE_UPSTREAM_SCHEMAS`` is ``{core, app}``,
    and ``assert_acyclic`` refuses anything else before lineage resolution runs.
    So the save succeeds, the report serves rows under its derived contract, and
    ``reports explain`` says plainly why it can never become a SQLMesh model.

    If this ever passes only because the save was refused, it proves nothing —
    hence the assertion that the report actually saved and that the blocker names
    the schema rather than merely being non-empty.
    """
    outcome = UserReportsService(prep_db).create(
        name="prep-peek",
        query_sql="SELECT transaction_id FROM prep.int_transactions__merged",
        actor="test",
    )

    explanation = explain_report(prep_db, handle=outcome.report_id, parameters={})

    assert explanation.graduation == "blocked"
    assert any("prep" in blocker for blocker in explanation.graduation_blockers)
