"""UTC contracts at ingestion, provider and transformed-model boundaries."""

import time
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from pydantic import ValidationError

from moneybin.connectors.sync_models import SyncMetadata
from moneybin.database import Database
from moneybin.services.transform_service import TransformService


@pytest.fixture(params=["UTC", "America/New_York", "Asia/Tokyo"])
def host_zone(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("TZ", request.param)
    time.tzset()
    yield request.param
    monkeypatch.undo()
    time.tzset()


def test_all_owned_instant_columns_are_aware(db: Database) -> None:
    result = TransformService(db).apply()
    assert result.applied, result.error
    naive = db.execute(
        "SELECT schema_name, table_name, column_name, data_type "
        "FROM duckdb_columns() WHERE database_name = current_database() "
        "AND schema_name IN ('raw', 'app', 'prep', 'core', 'reports', 'meta') "
        "AND (data_type = 'TIMESTAMP' OR data_type LIKE '%TIMESTAMP)%' "
        "OR data_type LIKE '%TIMESTAMP,%')"
    ).fetchall()
    assert naive == []
    assert db.execute(
        "SELECT data_type FROM duckdb_columns() WHERE schema_name = 'raw' "
        "AND table_name = 'manual_transactions' AND column_name = 'transaction_date'"
    ).fetchone() == ("DATE",)


@pytest.mark.parametrize("mode", ["insert", "replace", "upsert", "ignore"])
@pytest.mark.parametrize("value", [datetime(2025, 11, 2, 1, 30), "2025-11-02T01:30:00"])
def test_bulk_ingestion_rejects_unknown_naive_instants(
    db: Database, mode: Any, value: Any
) -> None:
    db.execute(
        "CREATE TABLE raw.utc_probe (id INTEGER PRIMARY KEY, event_at TIMESTAMPTZ)"
    )
    with pytest.raises(ValueError, match="offset|aware|timezone"):
        db.ingest_dataframe(
            "raw.utc_probe",
            pl.DataFrame({"id": [1], "event_at": [value]}),
            on_conflict=mode,
        )
    assert db.execute("SELECT count(*) FROM raw.utc_probe").fetchone() == (0,)


def test_bulk_ingestion_preserves_epoch_defaults_and_dates(
    db: Database, host_zone: str
) -> None:
    db.execute(
        "CREATE TABLE raw.utc_probe (id INTEGER, event_at TIMESTAMPTZ, "
        "business_date DATE, loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP)"
    )
    instant = datetime(
        2025, 11, 2, 1, 30, 0, 123456, tzinfo=timezone(timedelta(hours=-4))
    )
    before = datetime.now(UTC)
    db.ingest_dataframe(
        "raw.utc_probe",
        pl.DataFrame({
            "id": [1],
            "event_at": [instant],
            "business_date": [date(2025, 11, 2)],
        }),
    )
    row = db.execute(
        "SELECT event_at, epoch_us(event_at), business_date, loaded_at FROM raw.utc_probe"
    ).fetchone()
    assert row is not None
    assert row[0] == datetime(2025, 11, 2, 5, 30, 0, 123456, tzinfo=UTC)
    assert row[1] == 1762061400123456
    assert row[2] == date(2025, 11, 2)
    assert before <= row[3] <= datetime.now(UTC)


def test_sync_metadata_rejects_naive_instant() -> None:
    with pytest.raises(ValidationError):
        SyncMetadata(
            job_id="synthetic-job", synced_at=datetime(2025, 1, 1), institutions=[]
        )


def test_sync_metadata_normalizes_typed_offset() -> None:
    metadata = SyncMetadata.model_validate({
        "job_id": "synthetic-job",
        "institutions": [],
        "synced_at": "2025-11-02T01:30:00.123456-04:00",
    })
    assert metadata.synced_at == datetime(2025, 11, 2, 5, 30, 0, 123456, tzinfo=UTC)
    assert metadata.synced_at.tzinfo is UTC


def test_freshness_epoch_milliseconds_remain_absolute(
    db: Database, host_zone: str
) -> None:
    from sqlmesh.core.dialect import parse

    source = Path("src/moneybin/sqlmesh/models/meta/model_freshness.sql").read_text()
    query = parse(source, default_dialect="duckdb")[-1]
    assert query is not None
    db.execute("CREATE SCHEMA IF NOT EXISTS sqlmesh")
    db.execute(
        "CREATE TABLE sqlmesh._snapshots "
        "(name VARCHAR, version VARCHAR, updated_ts BIGINT, kind_name VARCHAR)"
    )
    db.execute(
        "CREATE TABLE sqlmesh._intervals "
        "(name VARCHAR, version VARCHAR, created_ts BIGINT, is_dev BOOLEAN, "
        "is_removed BOOLEAN, is_pending_restatement BOOLEAN)"
    )
    db.execute(
        "INSERT INTO sqlmesh._snapshots VALUES ('core.example', 'v1', 1762061400123, 'FULL')"
    )
    db.execute(
        "INSERT INTO sqlmesh._intervals VALUES ('core.example', 'v1', 1762061400456, false, false, false)"
    )
    row = db.execute(query.sql(dialect="duckdb")).fetchone()
    assert row is not None
    assert row[1] == datetime(2025, 11, 2, 5, 30, 0, 123000, tzinfo=UTC)
    assert row[2] == row[1]
    assert row[3] == datetime(2025, 11, 2, 5, 30, 0, 456000, tzinfo=UTC)


def test_bulk_ingestion_cannot_bypass_instant_contract_with_identifier_case(
    db: Database,
) -> None:
    db.execute("CREATE TABLE raw.utc_probe (id INTEGER, event_at TIMESTAMPTZ)")
    with pytest.raises(ValueError, match="offset"):
        db.ingest_dataframe(
            "RAW.UTC_PROBE",
            pl.DataFrame({"id": [1], "EVENT_AT": ["2025-11-02T01:30:00"]}),
        )
    assert db.execute("SELECT count(*) FROM raw.utc_probe").fetchone() == (0,)


@pytest.mark.parametrize("mode", ["insert", "replace", "upsert", "ignore"])
def test_bulk_ingestion_normalizes_offset_strings_and_preserves_nulls(
    db: Database,
    mode: Any,
) -> None:
    db.execute(
        "CREATE TABLE raw.utc_probe (id INTEGER PRIMARY KEY, event_at TIMESTAMPTZ)"
    )
    db.ingest_dataframe(
        "raw.utc_probe",
        pl.DataFrame({
            "id": [1, 2],
            "event_at": ["2025-11-02T01:30:00.123456-04:00", None],
        }),
        on_conflict=mode,
    )
    assert db.execute("SELECT event_at FROM raw.utc_probe ORDER BY id").fetchall() == [
        (datetime(2025, 11, 2, 5, 30, 0, 123456, tzinfo=UTC),),
        (None,),
    ]
    assert db.execute(
        "SELECT typeof(event_at) FROM raw.utc_probe LIMIT 1"
    ).fetchone() == ("TIMESTAMP WITH TIME ZONE",)


@pytest.mark.parametrize(
    ("model", "date_columns"),
    [
        ("stg_ofx__transactions", ["posted_date"]),
        (
            "stg_ofx__balances",
            ["statement_start_date", "statement_end_date", "ledger_balance_date"],
        ),
    ],
)
def test_ofx_business_dates_preserve_utc_parser_date_in_non_utc_session(
    db: Database, host_zone: str, model: str, date_columns: list[str]
) -> None:
    from sqlmesh.core.dialect import parse

    instant = datetime(2025, 1, 2, tzinfo=UTC)
    db.execute(
        "INSERT INTO raw.ofx_transactions "
        "(source_transaction_id, account_id, source_file, date_posted, amount) "
        "VALUES ('synthetic-date', 'synthetic-account', 'synthetic.ofx', ?, -12.34)",
        [instant],
    )
    db.execute(
        "INSERT INTO raw.ofx_balances "
        "(account_id, source_file, statement_start_date, statement_end_date, ledger_balance_date) "
        "VALUES ('synthetic-account', 'synthetic.ofx', ?, ?, ?)",
        [instant, instant, instant],
    )
    db.execute("SET TimeZone = 'America/Los_Angeles'")
    source = Path(f"src/moneybin/sqlmesh/models/prep/{model}.sql").read_text()
    query = parse(source, default_dialect="duckdb")[-1]
    assert query is not None
    result = db.execute(query.sql(dialect="duckdb"))
    values = result.fetchone()
    assert values is not None
    row = dict(zip([column[0] for column in result.description], values, strict=True))
    assert {name: row[name] for name in date_columns} == {
        name: date(2025, 1, 2) for name in date_columns
    }
