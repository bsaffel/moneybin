"""Prepared export redaction boundary tests."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pytest_mock import MockerFixture

import moneybin.exports.redaction as export_redaction
from moneybin.database import Database
from moneybin.exports.redaction import apply_export_redaction
from moneybin.exports.service import ExportService
from moneybin.exports.snapshot import (
    ExportSubject,
    PreparedColumn,
    PreparedExport,
    PreparedTable,
    build_data_dictionary,
    prepared_table_checksum,
)
from moneybin.privacy.taxonomy import DataClass
from moneybin.tables import TableRef
from tests.moneybin.db_helpers import create_core_dim_stub_views, create_core_tables


def _seed_critical_account(db: Database) -> ExportService:
    create_core_tables(db)
    create_core_dim_stub_views(db)
    db.execute(
        """
        INSERT INTO core.dim_accounts (
            account_id, routing_number, account_type, source_type, source_file
        ) VALUES (?, ?, ?, ?, ?)
        """,
        ["account-1", "021000021", "depository", "ofx", "statement.ofx"],
    )
    return ExportService(db)


def _account_value(snapshot: PreparedExport, column_name: str) -> object:
    accounts = next(table for table in snapshot.tables if table.name == "accounts")
    values = dict(
        zip(
            (column.name for column in accounts.columns),
            accounts.rows[0],
            strict=True,
        )
    )
    return values[column_name]


def test_prepare_bundle_masks_critical_columns_by_default(db: Database) -> None:
    service = _seed_critical_account(db)

    snapshot = service.prepare_bundle(profile="test")

    assert snapshot.redaction_mode == "redacted"
    assert _account_value(snapshot, "routing_number") == "*****"
    accounts = next(table for table in snapshot.tables if table.name == "accounts")
    routing = next(
        column for column in accounts.columns if column.name == "routing_number"
    )
    assert routing.data_class is DataClass.ROUTING_NUMBER
    assert snapshot.manifest["redaction_mode"] == "redacted"


def test_prepare_bundle_exposes_critical_columns_only_when_explicit(
    db: Database,
) -> None:
    service = _seed_critical_account(db)

    snapshot = service.prepare_bundle(profile="test", redaction_mode="unredacted")

    assert _account_value(snapshot, "routing_number") == "021000021"
    assert snapshot.redaction_mode == "unredacted"


def test_unredacted_request_does_not_change_the_next_request(db: Database) -> None:
    service = _seed_critical_account(db)

    unredacted = service.prepare_bundle(profile="test", redaction_mode="unredacted")
    next_default = service.prepare_bundle(profile="test")

    assert _account_value(unredacted, "routing_number") == "021000021"
    assert _account_value(next_default, "routing_number") == "*****"


def test_apply_export_redaction_returns_a_copy_without_mutating_input(
    db: Database,
) -> None:
    service = _seed_critical_account(db)
    original = service.prepare_bundle(profile="test", redaction_mode="unredacted")

    redacted = apply_export_redaction(original, "redacted")

    assert redacted is not original
    assert _account_value(redacted, "routing_number") == "*****"
    assert _account_value(original, "routing_number") == "021000021"


def _one_column_snapshot(
    duckdb_type: str, data_class: DataClass, value: object
) -> PreparedExport:
    """An unredacted one-column report snapshot, as ``prepare_report`` builds one."""
    columns = (PreparedColumn("n", duckdb_type, data_class),)
    rows = ((value,),)
    table = PreparedTable(
        name="user:probe",
        source=TableRef("reports", "probe"),
        columns=columns,
        rows=rows,
        checksum_sha256=prepared_table_checksum(columns, rows),
    )
    return PreparedExport(
        artifact_version=1,
        profile="test",
        created_at=datetime(2026, 7, 28, tzinfo=UTC),
        subject=ExportSubject(kind="report", report_id="user:probe", parameters={}),
        redaction_mode="unredacted",
        tables=(table,),
        data_dictionary=build_data_dictionary((table,)),
        provenance=None,
    )


def _declared_types(snapshot: PreparedExport) -> tuple[str, ...]:
    """Every place a prepared export states a column's type, in one tuple.

    A masked column's type is published three times from two sources: the live
    ``manifest`` property reads ``tables``, while ``data_dictionary`` is a stored
    field that ``apply_export_redaction`` has to rebuild. Asserting on the tuple
    keeps a fix to one of them from reading as a fix to all three.
    """
    manifest_tables = snapshot.manifest["tables"]
    dictionary_tables = snapshot.data_dictionary["tables"]
    return (
        snapshot.tables[0].columns[0].duckdb_type,
        manifest_tables[0]["columns"][0]["duckdb_type"],  # type: ignore[index]
        dictionary_tables[0]["columns"][0]["duckdb_type"],  # type: ignore[index]
    )


def test_a_masked_column_declares_the_type_redaction_actually_produced() -> None:
    """A saved report can put a masking class on a column that is not text.

    ``SELECT length(last_four) AS n`` keeps ``INSTITUTION_ACCOUNT_NUMBER`` through
    lineage while the column's type becomes ``BIGINT``, and the mask replaces the
    value with ``"*****"``. Leaving ``BIGINT`` declared beside that string is a
    manifest that describes the unredacted export instead of this one — and
    ``parquet_schema_for`` believes it, so the Parquet artifact cannot be written
    at all. The type travels with the value it describes.
    """
    original = _one_column_snapshot(
        "BIGINT", DataClass.INSTITUTION_ACCOUNT_NUMBER, value=4
    )

    redacted = apply_export_redaction(original, "redacted")

    assert redacted.tables[0].rows == (("*****",),)
    assert _declared_types(redacted) == ("VARCHAR", "VARCHAR", "VARCHAR")
    # The input is untouched, so an unredacted export of the same query still
    # promises the type it really returns.
    assert _declared_types(original) == ("BIGINT", "BIGINT", "BIGINT")


@pytest.mark.parametrize(
    ("duckdb_type", "data_class", "value"),
    [
        ("DECIMAL(18,2)", DataClass.TXN_AMOUNT, 4),
        ("BIGINT", DataClass.AGGREGATE, 4),
        ("DATE", DataClass.TXN_DATE, None),
    ],
)
def test_a_passthrough_column_keeps_its_own_type_through_redaction(
    duckdb_type: str, data_class: DataClass, value: object
) -> None:
    """Only a masked column is retyped — redaction is not a cast to text.

    Every non-CRITICAL class passes through in PR 2, so a redacted export is
    mostly native typed values. Retyping the whole table would destroy the
    Parquet round-trip that
    ``test_parquet_round_trips_native_typed_values_through_duckdb`` pins, and
    would misdescribe a column whose value redaction never touched.
    """
    original = _one_column_snapshot(duckdb_type, data_class, value=value)

    redacted = apply_export_redaction(original, "redacted")

    assert redacted.tables[0].rows == ((value,),)
    assert _declared_types(redacted) == (duckdb_type,) * 3


def test_a_masked_text_column_keeps_the_type_it_already_had() -> None:
    """The retype is a no-op where the mask returns text for text.

    Both masked bundle columns (``last_four``, ``routing_number``) are already
    ``VARCHAR``, so this pins that the fix leaves every artifact that exists
    today byte-identical: same declared type, therefore same table checksum.
    """
    original = _one_column_snapshot(
        "VARCHAR", DataClass.ROUTING_NUMBER, value="021000021"
    )

    redacted = apply_export_redaction(original, "redacted")

    assert redacted.tables[0].rows == (("*****",),)
    assert _declared_types(redacted) == ("VARCHAR", "VARCHAR", "VARCHAR")


def test_floored_column_values_are_stringified_before_export() -> None:
    """A FLOORED column always exports as text, even where masking left a value alone.

    FLOORED masks per VALUE (``_mask_floored``), not per column, so one BIGINT
    column can hold an untouched ``42`` alongside a masked
    ``"****...4321"`` — a mixed-type column no Parquet schema can describe.
    ``_redacted_column`` already declares the column ``VARCHAR`` because
    ``mask_strength(FLOORED)`` is not PASSTHROUGH; this pins that
    ``apply_export_redaction`` actually delivers on that promise for every
    row, not just the ones the transform happened to mask.

    The unmasked ``42`` case is load-bearing: it is the value that proves
    stringifying happens unconditionally rather than only as a side effect of
    masking (a test using only already-masked values couldn't tell the two
    apart).
    """
    columns = (
        PreparedColumn("n", "BIGINT", DataClass.FLOORED),
        PreparedColumn("blob", "VARCHAR", DataClass.FLOORED),
    )
    rows = (
        (42, "groceries"),
        (987654321, "acct 987654321"),
        (None, None),
        (7, {"inner": "value"}),
    )
    table = PreparedTable(
        name="user:probe",
        source=TableRef("reports", "probe"),
        columns=columns,
        rows=rows,
        checksum_sha256=prepared_table_checksum(columns, rows),
    )
    original = PreparedExport(
        artifact_version=1,
        profile="test",
        created_at=datetime(2026, 7, 28, tzinfo=UTC),
        subject=ExportSubject(kind="report", report_id="user:probe", parameters={}),
        redaction_mode="unredacted",
        tables=(table,),
        data_dictionary=build_data_dictionary((table,)),
        provenance=None,
    )

    redacted = apply_export_redaction(original, "redacted")

    out_rows = redacted.tables[0].rows
    assert out_rows[0] == ("42", "groceries")  # unmasked int, still stringified
    assert out_rows[1] == ("****...4321", "acct ****...4321")
    assert out_rows[2] == (None, None)  # None stays None
    assert out_rows[3] == ("7", "{'inner': 'value'}")
    for row in out_rows:
        for value in row:
            assert value is None or isinstance(value, str)
    for column in redacted.tables[0].columns:
        assert column.duckdb_type == "VARCHAR"


def test_redacted_policy_calls_the_shared_engine_once_per_table(
    db: Database, mocker: MockerFixture
) -> None:
    service = _seed_critical_account(db)
    redact_spy = mocker.spy(export_redaction, "redact_records")

    snapshot = service.prepare_bundle(profile="test")

    assert redact_spy.call_count == len(snapshot.tables)
