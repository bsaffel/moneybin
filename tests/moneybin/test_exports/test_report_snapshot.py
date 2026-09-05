"""Catalog report snapshot contract tests."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import replace
from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest
from pydantic import JsonValue
from pytest_mock import MockerFixture

import moneybin.reports._framework.catalog as report_catalog
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.exports.renderers import render_parquet
from moneybin.exports.service import ExportService
from moneybin.exports.snapshot import PreparedExport
from moneybin.privacy.payloads.networth import (
    NetWorthHistoryPayload,
    NetWorthHistoryPoint,
)
from moneybin.privacy.taxonomy import CLASSIFICATION, DataClass
from moneybin.reports._framework.catalog import (
    ReportCatalog,
    ReportStatus,
    ServiceReportSpec,
    report_tier,
)
from moneybin.reports._framework.contract import (
    Binding,
    OutputColumn,
    ParamSpec,
    ReportQuery,
    ReportSpec,
)
from moneybin.reports._framework.dynamic import (
    DEGRADED_PENDING_DEDUP,
    DEGRADED_STALE_CLASSIFICATION,
)
from moneybin.reports._framework.execute import (
    CatalogReportExecution,
    build_catalog_execution,
)
from moneybin.reports._framework.introspect import build_spec
from moneybin.reports.service_reports import NETWORTH_HISTORY_REPORT
from moneybin.services.user_reports_service import UserReportsService
from moneybin.tables import TableRef
from tests.moneybin.db_helpers import create_core_tables_raw, seed_pending_dedup_pair
from tests.moneybin.test_reports._metadata import TEST_SEMANTICS, output_columns

_VIEW = TableRef("reports", "test_export")


def _report(
    db: Database,
    *,
    top: int = 2,
    account_number: str = "acct_11112222",
) -> ReportQuery:
    """Account totals for export.

    Args:
        db: Open database connection.
        top: Maximum rows to return.
        account_number: Institution account number to include.
    """
    return ReportQuery(
        "SELECT account_number, amount FROM reports.test_export "
        "WHERE account_number = ? ORDER BY account_number LIMIT ?",
        [
            # The binding carries the class of the value bound: an institution
            # account number, which the provenance renderer must withhold.
            Binding(account_number, DataClass.INSTITUTION_ACCOUNT_NUMBER),
            Binding(top, DataClass.AGGREGATE),
        ],
        actions=("reports.inspect",),
        period="all time",
    )


def _spec() -> ReportSpec:
    classes = {
        "account_number": DataClass.ACCOUNT_IDENTIFIER,
        "amount": DataClass.TXN_AMOUNT,
    }
    return build_spec(
        _report,
        report_id="test:export",
        name="test_export",
        view=_VIEW,
        classes=classes,
        parameter_classes={
            "top": DataClass.AGGREGATE,
            "account_number": DataClass.ACCOUNT_IDENTIFIER,
        },
        columns=output_columns(classes),
        semantics=TEST_SEMANTICS,
    )


def _service(db: Database, *, catalog: ReportCatalog | None = None) -> ExportService:
    db.execute("CREATE SCHEMA IF NOT EXISTS reports")
    db.execute(
        """
        CREATE TABLE reports.test_export (
            account_number VARCHAR,
            amount DECIMAL(18, 2)
        )
        """
    )
    db.execute(
        """
        INSERT INTO reports.test_export VALUES
            ('acct_11112222', -30.00),
            ('acct_99998888', 100.00)
        """
    )
    return ExportService(db, report_catalog=catalog or ReportCatalog((_spec(),)))


def _first_row(snapshot: PreparedExport) -> dict[str, object]:
    table = snapshot.tables[0]
    return dict(
        zip(
            (column.name for column in table.columns),
            table.rows[0],
            strict=True,
        )
    )


def test_prepare_report_executes_once_and_preserves_the_report_receipt(
    db: Database,
    mocker: MockerFixture,
) -> None:
    service = _service(db)
    execute_spy = mocker.spy(report_catalog, "execute_catalog_report")

    snapshot = service.prepare_report(
        profile="test",
        report_id="test:export",
        report_parameters={},
        redaction_mode="unredacted",
    )

    assert execute_spy.call_count == 1
    assert snapshot.subject.as_manifest() == {
        "kind": "report",
        "report_id": "test:export",
        "parameters": {"top": 2, "account_number": "acct_11112222"},
    }
    assert len(snapshot.tables) == 1
    table = snapshot.tables[0]
    assert table.name == "test:export"
    assert table.source == _VIEW
    assert [
        (column.name, column.duckdb_type, column.data_class) for column in table.columns
    ] == [
        ("account_number", "VARCHAR", DataClass.ACCOUNT_IDENTIFIER),
        ("amount", "DECIMAL(18,2)", DataClass.TXN_AMOUNT),
    ]
    assert _first_row(snapshot) == {
        "account_number": "acct_11112222",
        "amount": Decimal("-30.00"),
    }

    assert snapshot.provenance is not None
    assert snapshot.provenance.report_id == "test:export"
    assert snapshot.provenance.receipt == {
        "report_id": "test:export",
        "parameters": {"top": 2, "account_number": "acct_11112222"},
        "parameter_classes": {
            "top": "aggregate",
            "account_number": "account_identifier",
        },
        "sql": (
            "SELECT account_number, amount FROM reports.test_export "
            "WHERE account_number = ? ORDER BY account_number LIMIT ?"
        ),
        "lineage": ("reports.test_summary",),
        "output_classes": {
            "account_number": "account_identifier",
            "amount": "txn_amount",
        },
        "freshness": None,
        "graduation_eligibility": None,
        # A report the catalog holds no drift verdict for — every packaged report,
        # and a saved one whose stored class map still matches its SQL.
        "degraded": False,
        "degraded_reason": None,
        "semantics": {
            "unit": "count",
            "currency": None,
            "sign": "non-negative",
            "kind": "count",
            "valuation_basis": None,
            "fx_basis": None,
            "fx_date": None,
            "time_basis": "point-in-time query result",
            "denominator": None,
            "comparison_window": None,
            "exclusions": (),
            "provenance": ("reports.test_summary",),
        },
    }
    manifest_provenance = snapshot.manifest["provenance"]
    assert manifest_provenance is not None
    assert manifest_provenance["report_id"] == "test:export"  # type: ignore[index]
    manifest_receipt = manifest_provenance["receipt"]  # type: ignore[index]
    assert manifest_receipt["lineage"] == ["reports.test_summary"]  # type: ignore[index]
    assert manifest_receipt["semantics"]["provenance"] == [  # type: ignore[index]
        "reports.test_summary"
    ]
    json.dumps(snapshot.manifest)


def test_a_report_with_no_graph_backed_view_records_a_null_manifest_source(
    db: Database,
) -> None:
    """A report with no ``reports.*`` view records ``source: null``, not a guess.

    A user-created report is evaluated at query time over whatever ``core`` /
    ``app`` tables its SQL names, so no single source view exists. The
    alternative — falling back to ``TableRef("reports", <name>)`` — writes a view
    that does not exist into the artifact, and provenance that cannot be checked
    is worse than none. Nothing is lost: the complete read-table set is already
    carried by ``provenance.receipt.lineage``, which this asserts stays intact.
    """
    viewless = replace(_spec(), view=None)
    service = _service(db, catalog=ReportCatalog((viewless,)))

    snapshot = service.prepare_report(
        profile="test",
        report_id="test:export",
        report_parameters={},
        redaction_mode="unredacted",
    )

    assert snapshot.tables[0].source is None
    assert snapshot.manifest["tables"][0]["source"] is None  # type: ignore[index]
    assert snapshot.data_dictionary["tables"][0]["source"] is None  # type: ignore[index]
    receipt = snapshot.manifest["provenance"]["receipt"]  # type: ignore[index]
    assert receipt["lineage"] == ["reports.test_summary"]  # type: ignore[index]
    json.dumps(snapshot.manifest)


def test_prepare_report_carries_a_degraded_reports_drift_into_the_receipt(
    db: Database,
) -> None:
    """A drifting saved report still exports, and the artifact says it drifted.

    R4 serves a stale class map fail-closed: every column whose upstream class
    moved is masked to ``UNRESOLVED``. The rows written to the file are therefore
    not the rows the report's own ``output_classes`` describe, and an artifact
    outlives the session that produced it — a reader months later has no other
    way to learn that the columns were masked by drift rather than empty at
    source.

    It does not *refuse*: masking more than declared is not an availability
    failure, and refusing would turn one upstream reclassification into an export
    outage for every saved report that reads the affected column.
    """
    reason = "stale_classification: upstream classification changed for amount"
    saved = replace(_spec(), report_id="user:rab12cd34ef56", name="my_export")
    service = _service(
        db,
        catalog=ReportCatalog(
            (saved,),
            status={
                saved.report_id: ReportStatus(degraded=True, degraded_reason=reason)
            },
        ),
    )

    snapshot = service.prepare_report(
        profile="test",
        report_id=saved.report_id,
        report_parameters={},
        redaction_mode="unredacted",
    )

    assert snapshot.provenance is not None
    assert snapshot.provenance.receipt["degraded"] is True
    assert snapshot.provenance.receipt["degraded_reason"] == reason
    # The artifact, not just the in-memory receipt: the manifest is what ships.
    manifest_receipt = snapshot.manifest["provenance"]["receipt"]  # type: ignore[index]
    assert manifest_receipt["degraded_reason"] == reason  # type: ignore[index]
    json.dumps(snapshot.manifest)


def test_prepare_report_carries_a_pending_duplicate_caveat_into_the_receipt(
    db: Database,
) -> None:
    """Issue #409: the durable artifact of an inflated total says it is provisional.

    The export runs the catalog beneath ``ReportCatalog.execute``, so the caveat
    has to be attached at the execution, not the interactive result, or the one
    copy that outlives the review queue would be the one copy without it.
    """
    create_core_tables_raw(db.conn)
    seed_pending_dedup_pair(db)
    fed_by_transactions = replace(
        _spec(),
        semantics=replace(TEST_SEMANTICS, provenance=("core.fct_transactions",)),
    )
    service = _service(db, catalog=ReportCatalog((fed_by_transactions,)))

    snapshot = service.prepare_report(
        profile="test",
        report_id=fed_by_transactions.report_id,
        report_parameters={},
        redaction_mode="unredacted",
    )

    assert snapshot.provenance is not None
    assert snapshot.provenance.receipt["degraded"] is True
    reason = str(snapshot.provenance.receipt["degraded_reason"])
    assert reason.startswith(f"{DEGRADED_PENDING_DEDUP}: 1 ")


def test_prepare_report_applies_redaction_after_raw_execution(db: Database) -> None:
    snapshot = _service(db).prepare_report(
        profile="test",
        report_id="test:export",
        report_parameters={},
    )

    assert snapshot.redaction_mode == "redacted"
    assert _first_row(snapshot)["account_number"] == "****2222"
    assert snapshot.subject.as_manifest()["parameters"] == {
        "top": 2,
        "account_number": "****2222",
    }
    assert snapshot.manifest["provenance"]["receipt"]["parameters"] == {  # type: ignore[index]
        "top": 2,
        "account_number": "****2222",
    }


def test_a_redacted_builtin_report_export_keeps_its_reviewed_sql(
    db: Database,
) -> None:
    """A built-in's statement is repo-authored, so withholding it buys nothing.

    A receipt exists to make an artifact reproducible: ``lineage`` says which
    tables were read and ``sql`` says what was asked of them. A built-in's query is
    reviewed, already public in the repo, and binds its values rather than inlining
    them — the receipt redacts those bindings separately — so there is no literal
    to withhold, and dropping the statement costs every default export its
    verifiability for no privacy gain.

    Same tier rule as the column headers above: repo-authored names and
    repo-authored SQL are withheld together or not at all.
    """

    def runner(db: Database) -> ReportQuery:  # noqa: ARG001  # report contract handle
        """Reviewed query binding its filter rather than inlining it.

        Args:
            db: Open database connection.
        """
        return ReportQuery(
            "SELECT account_number, amount FROM reports.test_export "
            "WHERE account_number = ?",
            [Binding("acct_11112222", DataClass.INSTITUTION_ACCOUNT_NUMBER)],
            actions=("reports.inspect",),
            period="all time",
        )

    classes = {
        "account_number": DataClass.ACCOUNT_IDENTIFIER,
        "amount": DataClass.TXN_AMOUNT,
    }
    spec = build_spec(
        runner,
        report_id="test:saved",
        name="saved_export",
        view=_VIEW,
        classes=classes,
        parameter_classes={},
        columns=output_columns(classes),
        semantics=TEST_SEMANTICS,
    )
    service = _service(db, catalog=ReportCatalog((spec,)))

    redacted = service.prepare_report(
        profile="test",
        report_id="test:saved",
        report_parameters={},
    )
    assert redacted.redaction_mode == "redacted"
    assert report_tier(spec) == "builtin"
    assert redacted.provenance is not None
    # The artifact, not just the in-memory receipt: the manifest is what ships.
    manifest_sql = redacted.manifest["provenance"]["receipt"]["sql"]  # type: ignore[index]
    assert manifest_sql == (
        "SELECT account_number, amount FROM reports.test_export "
        "WHERE account_number = ?"
    )
    # Retaining the statement publishes no value, because the value is a binding.
    assert "acct_11112222" not in json.dumps(redacted.manifest)


def test_a_redacted_user_report_export_withholds_the_saved_query(
    db: Database,
) -> None:
    """A saved report's SQL is user-authored, so a critical literal can sit in it.

    ``apply_export_redaction`` transforms table rows only, so a receipt carrying
    the statement verbatim republishes in the manifest and the workbook metadata
    tab exactly what the redacted policy withheld from the cells — beside
    correctly-masked values. Nothing checks a user's SQL for inline literals,
    because nothing can: the statement is the author's own text.
    """
    create_core_tables_raw(db.conn)
    db.execute(
        "INSERT INTO core.dim_accounts (account_id, routing_number) VALUES (?, ?)",
        ["acct_11112222", "021000021"],
    )
    report_id = (
        UserReportsService(db)
        .create(
            name="inline_literal",
            query_sql=(
                "SELECT account_id FROM core.dim_accounts "
                "WHERE routing_number = '021000021'"
            ),
            actor="cli",
        )
        .report_id
    )
    service = ExportService(db)

    redacted = service.prepare_report(
        profile="test",
        report_id=report_id,
        report_parameters={},
    )

    assert redacted.provenance is not None
    assert redacted.provenance.receipt["sql"] is None
    assert redacted.manifest["provenance"]["receipt"]["sql"] is None  # type: ignore[index]
    assert "021000021" not in json.dumps(redacted.manifest)

    unredacted = service.prepare_report(
        profile="test",
        report_id=report_id,
        report_parameters={},
        redaction_mode="unredacted",
    )

    # The author's own statement, published only where the values are too.
    assert unredacted.provenance is not None
    assert "021000021" in str(unredacted.provenance.receipt["sql"])


def test_a_redacted_user_report_export_withholds_a_sensitive_column_alias(
    db: Database,
) -> None:
    """A masked column's header must not publish what its own cells withhold.

    A saved report's output name is user-authored, so
    ``routing_number AS "021000021"`` copies a critical literal into the workbook
    header, the data-dictionary entry, and the receipt's ``output_classes`` key.
    ``apply_export_redaction`` transforms row values only, so all three survive
    beside a cell masked to ``*****`` — in the same artifact that already
    withholds the SQL for exactly this threat.

    Every authored name goes, not only the masked column's — ``my_account`` is
    the author's text too, and its sensitivity is not decided by the column it
    happens to label. Only for the user tier: a built-in's ``routing_number``
    header is repo-authored, names the column's meaning rather than a value, and
    blanking it would cost readability for no gain.
    """
    create_core_tables_raw(db.conn)
    db.execute(
        "INSERT INTO core.dim_accounts (account_id, routing_number) VALUES (?, ?)",
        ["acct_11112222", "021000021"],
    )
    report_id = (
        UserReportsService(db)
        .create(
            name="routing_alias",
            query_sql=(
                'SELECT routing_number AS "021000021", account_id AS my_account '
                "FROM core.dim_accounts"
            ),
            actor="cli",
        )
        .report_id
    )
    service = ExportService(db)

    redacted = service.prepare_report(
        profile="test",
        report_id=report_id,
        report_parameters={},
    )

    table = redacted.tables[0]
    assert [column.name for column in table.columns] == [
        "redacted_column_1",
        "redacted_column_2",
    ]
    assert table.rows == (("*****", "acct_11112222"),)
    assert "021000021" not in json.dumps(redacted.manifest)
    assert "021000021" not in json.dumps(redacted.data_dictionary)
    assert redacted.provenance is not None
    assert set(redacted.provenance.receipt["output_classes"]) == {  # type: ignore[arg-type]
        "redacted_column_1",
        "redacted_column_2",
    }

    unredacted = service.prepare_report(
        profile="test",
        report_id=report_id,
        report_parameters={},
        redaction_mode="unredacted",
    )

    # The alias is the author's own label for their own data: an unredacted
    # export publishes the value itself, so withholding the name buys nothing.
    assert [column.name for column in unredacted.tables[0].columns] == [
        "021000021",
        "my_account",
    ]


def test_a_redacted_user_report_export_withholds_a_sensitive_parameter_name(
    db: Database,
) -> None:
    """A parameter's name is user-authored on the same terms as a column alias.

    The receipt keys both ``parameters`` and ``parameter_classes`` by declared
    name and the subject repeats them, so
    ``WHERE routing_number = $acct_021000021`` publishes the literal three more
    times beside a value masked to ``*****``. ``only_account`` goes with it: a
    declared name is authored text whatever its value's class turns out to be.

    The ``builtin``-tier half of this is
    ``test_prepare_report_applies_redaction_after_raw_execution``, which pins that
    a declared ``account_number`` parameter keeps its name while its value masks.
    """
    create_core_tables_raw(db.conn)
    db.execute(
        "INSERT INTO core.dim_accounts (account_id, routing_number) VALUES (?, ?)",
        ["acct_11112222", "021000021"],
    )
    report_id = (
        UserReportsService(db)
        .create(
            name="param_alias",
            query_sql=(
                "SELECT account_id FROM core.dim_accounts "
                "WHERE routing_number = $acct_021000021 AND account_id = $only_account"
            ),
            params=[
                ParamSpec(
                    name="acct_021000021",
                    annotation=str,
                    default=None,
                    required=True,
                    help="",
                    data_class=DataClass.UNRESOLVED,
                ),
                ParamSpec(
                    name="only_account",
                    annotation=str,
                    default=None,
                    required=True,
                    help="",
                    data_class=DataClass.UNRESOLVED,
                ),
            ],
            actor="cli",
        )
        .report_id
    )

    redacted = ExportService(db).prepare_report(
        profile="test",
        report_id=report_id,
        report_parameters={
            "acct_021000021": "021000021",
            "only_account": "acct_11112222",
        },
    )

    expected = {
        "redacted_parameter_1": "*****",
        "redacted_parameter_2": "acct_11112222",
    }
    assert redacted.subject.as_manifest()["parameters"] == expected
    assert redacted.provenance is not None
    assert redacted.provenance.receipt["parameters"] == expected
    assert redacted.provenance.receipt["parameter_classes"] == {
        "redacted_parameter_1": DataClass.ROUTING_NUMBER.value,
        "redacted_parameter_2": DataClass.RECORD_ID.value,
    }
    assert "021000021" not in json.dumps(redacted.manifest)


def test_a_redacted_user_report_export_withholds_a_name_beside_a_published_value(
    db: Database,
) -> None:
    """A name can carry the literal on its own, with no masked value to key on.

    ``SELECT 1 AS "021000021"`` puts a routing number in the header while the
    value beside it is a benign ``1``, so the earlier value-keyed rule — withhold
    a name exactly where its own value is masked — published it. The premise was
    wrong: a user-authored name is arbitrary text, and its sensitivity is not a
    function of the column it labels.

    So a redacted user-tier export withholds *every* authored name. MoneyBin
    cannot classify arbitrary text — the same reason ``catalog.py`` withholds a
    report's name wholesale from its collision warning rather than judging it —
    and a redacted artifact outlives the session, so the fail-closed answer is
    the only sound one.
    """
    create_core_tables_raw(db.conn)
    db.execute(
        "INSERT INTO core.dim_accounts (account_id) VALUES (?)", ["acct_11112222"]
    )
    report_id = (
        UserReportsService(db)
        .create(
            name="benign_value_sensitive_name",
            query_sql=(
                'SELECT 1 AS "021000021", account_id AS my_account '
                "FROM core.dim_accounts"
            ),
            actor="cli",
        )
        .report_id
    )
    service = ExportService(db)

    redacted = service.prepare_report(
        profile="test", report_id=report_id, report_parameters={}
    )

    table = redacted.tables[0]
    assert [column.name for column in table.columns] == [
        "redacted_column_1",
        "redacted_column_2",
    ]
    # The values are untouched: withholding the name is not masking the column.
    assert table.rows == ((1, "acct_11112222"),)
    assert "021000021" not in json.dumps(redacted.manifest)
    assert "021000021" not in json.dumps(redacted.data_dictionary)

    unredacted = service.prepare_report(
        profile="test",
        report_id=report_id,
        report_parameters={},
        redaction_mode="unredacted",
    )

    assert [column.name for column in unredacted.tables[0].columns] == [
        "021000021",
        "my_account",
    ]


def test_a_redacted_user_report_export_keeps_its_column_names_distinct(
    db: Database,
) -> None:
    """An alias shaped like the scheme earns no exemption from it.

    Column names are dict keys downstream — ``redact_records`` zips them against
    each row — so two columns sharing one silently collapse into a single entry,
    publishing one column's value under both headers. Renaming every name
    positionally makes that unreachable by construction, and this is the fixture
    that would notice a "keep this one, it looks fine" branch coming back: the
    second column is authored as ``redacted_column_1``, so any rule that
    preserved a name it recognized would mint exactly the collision above.
    """
    create_core_tables_raw(db.conn)
    db.execute(
        "INSERT INTO core.dim_accounts (account_id, routing_number) VALUES (?, ?)",
        ["acct_11112222", "021000021"],
    )
    report_id = (
        UserReportsService(db)
        .create(
            name="alias_collision",
            query_sql=(
                'SELECT routing_number AS "021000021", '
                "account_id AS redacted_column_1 FROM core.dim_accounts"
            ),
            actor="cli",
        )
        .report_id
    )

    redacted = ExportService(db).prepare_report(
        profile="test",
        report_id=report_id,
        report_parameters={},
    )

    table = redacted.tables[0]
    assert [column.name for column in table.columns] == [
        "redacted_column_1",
        "redacted_column_2",
    ]
    assert table.rows == (("*****", "acct_11112222"),)


def test_a_redacted_builtin_report_export_keeps_its_declared_column_names(
    db: Database,
) -> None:
    """The benign twin: a repo-authored header must survive its column's masking.

    ``account_number`` is masked to ``****2222`` here, so a rename keyed on
    masking alone — without the user-tier condition — would blank a name that
    describes the column rather than disclosing a value, and no privacy test in
    this repo fails on over-masking.
    """
    redacted = _service(db).prepare_report(
        profile="test",
        report_id="test:export",
        report_parameters={},
    )

    assert [column.name for column in redacted.tables[0].columns] == [
        "account_number",
        "amount",
    ]


def test_a_redacted_user_report_export_withholds_a_drifted_column_alias(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The drift sentence names the same columns the header rename withholds.

    ``spec_from_row`` writes every moved column into ``degraded_reason`` so a
    reader learns which ones went stale — the author's own labels, which is the
    right answer on their own terminal. Copied verbatim into a redacted artifact
    it republishes exactly the text the rename, the class-map key, and the
    withheld SQL all withhold, one field over.

    A redacted receipt publishes the reason *code* instead: still enough to tell
    a stale class map from an unreadable row months later, without the names.
    """
    create_core_tables_raw(db.conn)
    db.execute(
        "INSERT INTO core.dim_accounts (account_id) VALUES (?)", ["acct_11112222"]
    )
    report_id = (
        UserReportsService(db)
        .create(
            name="drifting_alias",
            query_sql='SELECT account_id AS "021000021" FROM core.dim_accounts',
            actor="cli",
        )
        .report_id
    )
    # The saved map says RECORD_ID; this is the upward move R4 serves fail-closed.
    reclassified = dict(CLASSIFICATION)
    reclassified[("core", "dim_accounts")] = {
        **CLASSIFICATION[("core", "dim_accounts")],
        "account_id": DataClass.ROUTING_NUMBER,
    }
    monkeypatch.setattr("moneybin.privacy.sql_lineage.CLASSIFICATION", reclassified)

    redacted = ExportService(db).prepare_report(
        profile="test",
        report_id=report_id,
        report_parameters={},
    )

    assert redacted.provenance is not None
    assert redacted.provenance.receipt["degraded"] is True
    assert (
        redacted.provenance.receipt["degraded_reason"] == DEGRADED_STALE_CLASSIFICATION
    )
    assert "021000021" not in json.dumps(redacted.manifest)

    unredacted = ExportService(db).prepare_report(
        profile="test",
        report_id=report_id,
        report_parameters={},
        redaction_mode="unredacted",
    )

    # Same rule as the header: an unredacted export publishes the value itself,
    # so naming the column that carries it costs nothing.
    assert unredacted.provenance is not None
    reason = unredacted.provenance.receipt["degraded_reason"]
    assert isinstance(reason, str)
    assert reason.startswith(DEGRADED_STALE_CLASSIFICATION)
    assert "021000021" in reason


def test_prepare_report_exports_every_row_without_the_mcp_response_cap(
    db: Database,
) -> None:
    """Artifact completeness is independent from interactive response limits."""
    rows = [{"value": value} for value in range(5)]

    def executor(
        database: Database,  # noqa: ARG001  # service contract handle
        parameters: Mapping[str, JsonValue],
        limit: int | None,
    ) -> CatalogReportExecution:
        assert limit is None
        return build_catalog_execution(
            spec,
            parameters=parameters,
            records=rows,
            columns=["value"],
            column_types=["BIGINT"],
            max_rows=limit,
            sql=None,
        )

    spec = ServiceReportSpec(
        report_id="test:complete_export",
        name="complete_export",
        description="Synthetic complete report export.",
        parameters=(),
        columns=(OutputColumn("value", "Value.", DataClass.AGGREGATE),),
        semantics=TEST_SEMANTICS,
        classes={"value": DataClass.AGGREGATE},
        examples=(),
        executor=executor,
    )

    snapshot = ExportService(
        db,
        report_catalog=ReportCatalog((spec,)),
    ).prepare_report(
        profile="test",
        report_id="test:complete_export",
        report_parameters={},
        redaction_mode="redacted",
    )

    assert snapshot.tables[0].rows == tuple((value,) for value in range(5))
    assert snapshot.manifest["tables"][0]["row_count"] == 5  # type: ignore[index]


@pytest.mark.parametrize(
    ("report_id", "parameters", "code"),
    [
        ("missing:report", {}, "report_id_not_found"),
        ("test:export,test:other", {}, "report_id_not_found"),
        ("SELECT * FROM reports.test_export", {}, "report_id_not_found"),
        ("test:export", {"unknown": 1}, "report_parameter_unknown"),
        ("test:export", {"top": "two"}, "report_parameter_invalid_type"),
    ],
)
def test_prepare_report_uses_catalog_errors_for_invalid_subjects_and_parameters(
    db: Database,
    report_id: str,
    parameters: dict[str, object],
    code: str,
) -> None:
    service = _service(db)

    with pytest.raises(UserError) as exc_info:
        service.prepare_report(
            profile="test",
            report_id=report_id,
            report_parameters=parameters,  # type: ignore[arg-type]  # invalid runtime input under test
        )

    assert exc_info.value.code == code


def test_prepare_service_report_uses_one_raw_execution_for_each_output_policy(
    db: Database,
) -> None:
    calls = 0

    def executor(
        database: Database,  # noqa: ARG001  # service contract handle
        parameters: Mapping[str, JsonValue],
        limit: int | None,
    ) -> CatalogReportExecution:
        nonlocal calls
        calls += 1
        return build_catalog_execution(
            spec,
            parameters=parameters,
            records=[{"account_number": parameters["account_number"]}],
            columns=["account_number"],
            column_types=["VARCHAR"],
            max_rows=limit,
            actions=["reports.inspect"],
            period="all time",
            sql=None,
        )

    spec = ServiceReportSpec(
        report_id="test:service_export",
        name="service_export",
        description="Synthetic service-backed export.",
        parameters=(
            ParamSpec(
                "account_number",
                str,
                "acct_11112222",
                False,
                "Institution account number.",
                DataClass.ACCOUNT_IDENTIFIER,
            ),
        ),
        columns=(
            OutputColumn(
                "account_number",
                "Institution account number.",
                DataClass.ACCOUNT_IDENTIFIER,
            ),
        ),
        semantics=TEST_SEMANTICS,
        classes={"account_number": DataClass.ACCOUNT_IDENTIFIER},
        examples=(),
        executor=executor,
    )
    service = ExportService(db, report_catalog=ReportCatalog((spec,)))

    redacted = service.prepare_report(
        profile="test",
        report_id="test:service_export",
        report_parameters={},
    )
    assert calls == 1
    assert _first_row(redacted)["account_number"] == "****2222"
    assert redacted.manifest["provenance"]["receipt"]["sql"] is None  # type: ignore[index]

    unredacted = service.prepare_report(
        profile="test",
        report_id="test:service_export",
        report_parameters={},
        redaction_mode="unredacted",
    )
    assert calls == 2
    assert _first_row(unredacted)["account_number"] == "acct_11112222"

    with pytest.raises(UserError) as exc_info:
        service.prepare_report(
            profile="test",
            report_id="test:service_export",
            report_parameters={"unknown": 1},
        )
    assert exc_info.value.code == "report_parameter_unknown"
    assert calls == 2


def test_a_saved_report_masking_a_numeric_column_still_exports_to_parquet(
    db: Database,
    tmp_path: Path,
) -> None:
    """The whole chain a saved report walks to a typed artifact, driven end to end.

    Lineage hands ``INSTITUTION_ACCOUNT_NUMBER`` down through ``length()`` without
    the column's type, so a saved report is the only way to reach a masked value
    whose declared type is not text: no built-in report declares a masking class
    on any output column, and both masked bundle columns are already ``VARCHAR``.
    That makes this reachable only through the feature this branch adds, which is
    why the coverage is here and not at the renderer alone.
    """
    create_core_tables_raw(db.conn)
    db.execute(
        "INSERT INTO core.dim_accounts (account_id, last_four) VALUES (?, ?)",
        ["acct_11112222", "4021"],
    )
    report_id = (
        UserReportsService(db)
        .create(
            name="masked_length",
            query_sql="SELECT length(last_four) AS n FROM core.dim_accounts",
            actor="cli",
        )
        .report_id
    )

    snapshot = ExportService(db).prepare_report(
        profile="test",
        report_id=report_id,
        report_parameters={},
    )

    table = snapshot.tables[0]
    assert [(column.duckdb_type, column.data_class) for column in table.columns] == [
        ("VARCHAR", DataClass.INSTITUTION_ACCOUNT_NUMBER)
    ]
    assert table.rows == (("*****",),)

    rendered = render_parquet(snapshot, tmp_path / "bundle")

    assert duckdb.read_parquet(str(rendered.table_files[report_id])).fetchall() == [
        ("*****",)
    ]


def test_networth_history_export_retains_native_values_with_truthful_types(
    db: Database,
    mocker: MockerFixture,
) -> None:
    history = mocker.patch(
        "moneybin.reports.service_reports.NetworthService.history",
        return_value=NetWorthHistoryPayload(
            points=[
                NetWorthHistoryPoint(
                    period="2026-07-01",
                    currency_code="USD",
                    net_worth=Decimal("1000.12345678"),
                    change_abs=Decimal("100.75308643"),
                    change_pct=Decimal("0.100740651234567890"),
                )
            ]
        ),
    )

    snapshot = ExportService(
        db,
        report_catalog=ReportCatalog((NETWORTH_HISTORY_REPORT,)),
    ).prepare_report(
        profile="test",
        report_id="core:networth_history",
        report_parameters={
            "from_date": "2026-07-01",
            "to_date": "2026-07-31",
        },
        redaction_mode="unredacted",
    )

    history.assert_called_once_with(
        date(2026, 7, 1),
        date(2026, 7, 31),
        interval="monthly",
    )
    table = snapshot.tables[0]
    # Grain-first, and `net_worth` ahead of the two changes measured from it
    # (`column-ordering.md` Rules B and C: a comparative's base leads its group
    # even when it is also the headline). Each type is asserted beside its own
    # column: the executor now keys them by name, and this is what would catch a
    # regression back to a list bound by position.
    assert [(column.name, column.duckdb_type) for column in table.columns] == [
        ("currency_code", "VARCHAR"),
        ("period", "VARCHAR"),
        ("net_worth", "DECIMAL(12,8)"),
        ("change_abs", "DECIMAL(11,8)"),
        ("change_pct", "DECIMAL(18,18)"),
    ]
    assert table.rows == (
        (
            "USD",
            "2026-07-01",
            Decimal("1000.12345678"),
            Decimal("100.75308643"),
            Decimal("0.100740651234567890"),
        ),
    )
    manifest_columns = snapshot.manifest["tables"][0]["columns"]  # type: ignore[index]
    assert [column["duckdb_type"] for column in manifest_columns] == [  # type: ignore[index]
        "VARCHAR",
        "VARCHAR",
        "DECIMAL(12,8)",
        "DECIMAL(11,8)",
        "DECIMAL(18,18)",
    ]
