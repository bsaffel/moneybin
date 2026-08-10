"""Tests for the saved-report save contract in ``reports/_framework/derive.py``.

``test_user_reports.py`` drives the same pipeline *through* the service. These
sit at the contract itself: which schemas a durable report may read, and what a
report that reads one of the newly-opened schemas can and cannot become.
"""

from __future__ import annotations

from typing import Any

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.sql_query import (
    _ALLOWED_QUERY_SCHEMAS,  # pyright: ignore[reportPrivateUsage]
)
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import ParamSpec
from moneybin.reports._framework.derive import SAVE_SCHEMAS
from moneybin.reports._framework.explain import explain_report
from moneybin.services.user_reports_service import UserReportsService


def _param(name: str, annotation: type = str, **overrides: Any) -> ParamSpec:
    """One declared parameter; ``data_class`` is derived, never read from here."""
    declared: dict[str, Any] = {
        "name": name,
        "annotation": annotation,
        "default": None,
        "required": True,
        "help": "",
        "data_class": DataClass.UNRESOLVED,
    }
    return ParamSpec(**(declared | overrides))


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


def test_a_floored_parameter_default_is_refused_at_save(prep_db: Database) -> None:
    """Task 6 CREATED this path; it did not weaken an existing one.

    Before the ``SAVE_SCHEMAS`` widening, ``_qualified_or_refuse`` rejected a
    ``prep`` query at the schema gate *before* ``_refuse_sensitive_defaults`` ever
    ran, so no raw/prep default could be stored either way. Opening the schema is
    what made the default check reachable — and it arrived answering wrongly,
    because it asked ``tier > Tier.LOW`` and FLOORED is LOW.

    The content net cannot cover this: it masks result values at execution and
    never touches the parameter schema, which ``_parameter_schema`` copies
    verbatim into the published catalog entry. So the value would be returned in
    the clear by a bare catalog listing, no execution required.

    The default deliberately contains **no digits at all**. Withholding here is a
    decision about the column's CLASS, not about the value's shape — a
    shape-triggered fixture would pass just as well against a content-net
    implementation and would not distinguish the two.
    """
    with pytest.raises(UserError) as caught:
        UserReportsService(prep_db).create(
            name="floored-default",
            query_sql=(
                "SELECT transaction_id FROM prep.int_transactions__merged "
                "WHERE transaction_id = $txn"
            ),
            actor="test",
            params=[_param("txn", str, default="north branch", required=False)],
        )

    assert caught.value.code == error_codes.REPORT_PARAMETER_DEFAULT_NOT_ALLOWED
    assert caught.value.details is not None
    assert caught.value.details["parameter"] == "txn"


def test_a_floored_parameter_may_still_be_declared_required(
    prep_db: Database,
) -> None:
    """The benign twin: the gate refuses the *default*, not the parameter.

    Without this, a fix that refused every FLOORED parameter outright would
    satisfy the test above while removing the ability to parameterize a raw/prep
    report at all — which is the feature M2O.2 exists to add.
    """
    outcome = UserReportsService(prep_db).create(
        name="floored-required",
        query_sql=(
            "SELECT transaction_id FROM prep.int_transactions__merged "
            "WHERE transaction_id = $txn"
        ),
        actor="test",
        params=[_param("txn", str, required=True)],
    )

    assert outcome.report_id


def test_a_save_names_the_columns_riding_the_content_floor(prep_db: Database) -> None:
    """The save note that tells an author which columns have no declaration.

    ``unresolved_columns`` already names the columns masked *whole*. A FLOORED
    column is the other uncertain case and reads as the certain one: it returns
    values in the clear, so nothing in the response says its protection is a
    value-shape scan with known gaps (4-to-7 digit runs, separator-formatted
    values, and every DECIMAL or FLOAT pass through untouched) rather than a
    declared class.
    """
    outcome = UserReportsService(prep_db).create(
        name="floored-note",
        query_sql="SELECT transaction_id FROM prep.int_transactions__merged",
        actor="test",
    )

    assert outcome.floored_columns == ("transaction_id",)


def test_a_declared_raw_prep_column_is_not_named_as_floored(
    prep_db: Database,
) -> None:
    """Isolation: the note names the *undeclared* columns, not the schema.

    Both columns here come from one ``prep`` view, so a note that simply
    reported every raw/prep column would pass the test above and fail this one.
    ``account_id`` carries an ``INTERNAL_CRITICAL`` declaration and is masked by
    class; ``amount`` carries none and rides the scan.
    """
    prep_db.execute(
        "CREATE OR REPLACE VIEW prep.stg_ofx__transactions AS "
        "SELECT account_id, amount FROM core.fct_transactions"
    )

    outcome = UserReportsService(prep_db).create(
        name="ofx-peek",
        query_sql="SELECT account_id, amount FROM prep.stg_ofx__transactions",
        actor="test",
    )

    assert outcome.floored_columns == ("amount",)


def test_a_fully_declared_query_is_named_as_floored_nowhere(
    saved_db: Database,
) -> None:
    """The benign twin: no note where every column carries a declared class.

    Required by the fail-closed lesson — no privacy test in this repo fails on
    *over*-reporting, so the quiet path needs its own assertion.
    """
    outcome = UserReportsService(saved_db).create(
        name="declared",
        query_sql="SELECT account_id, routing_number FROM core.dim_accounts",
        actor="test",
    )

    assert outcome.floored_columns == ()


def test_an_above_low_passthrough_default_is_still_refused(saved_db: Database) -> None:
    """The non-regression, proven at the call site rather than at the predicate.

    ``TXN_AMOUNT`` is HIGH tier and *passes through* today (PR 3 adds bucketing),
    so it is the exact shape that a mask-strength-only gate would have started
    ALLOWING — a silent widening of ``core``/``app`` by a change meant only to
    add ``raw``/``prep``. The existing refusal tests all use CRITICAL columns,
    which mask, so none of them would have caught it.

    The default is a plain small integer, not a balance: what is under test is
    that the *class* refuses a default at all, and a realistic amount would put a
    financial value in the repo for no added coverage.
    """
    with pytest.raises(UserError) as caught:
        UserReportsService(saved_db).create(
            name="amount-default",
            query_sql="SELECT transaction_id FROM core.fct_transactions WHERE amount = $amt",
            actor="test",
            params=[_param("amt", int, default=1, required=False)],
        )

    assert caught.value.code == error_codes.REPORT_PARAMETER_DEFAULT_NOT_ALLOWED
