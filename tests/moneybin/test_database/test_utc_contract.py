"""Encrypted startup guards and canonical UTC connections."""

import hashlib
import importlib.metadata
import time
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest
from sqlmesh.core.engine_adapter.duckdb import DuckDBEngineAdapter

from moneybin.database import (
    Database,
    _pin_cursor_to_moneybin,  # pyright: ignore[reportPrivateUsage]  # adapter boundary
)
from moneybin.errors import classify_user_error


@pytest.mark.parametrize(
    "opening",
    [
        {"read_only": True},
        {"read_only": False},
        {"read_only": False, "no_auto_upgrade": True},
        {"read_only": False, "assume_initialized": True},
    ],
    ids=["read-only", "writable", "no-auto-upgrade", "assume-initialized"],
)
def test_legacy_open_refuses_before_any_write(
    tmp_path: Path, mock_secret_store: MagicMock, opening: dict[str, bool]
) -> None:
    path = tmp_path / "legacy.duckdb"
    with Database(
        path, secret_store=mock_secret_store, read_only=False, no_auto_upgrade=True
    ) as db:
        db.execute("DELETE FROM app.versions WHERE component = 'utc_contract'")
        db.execute(
            "CREATE TABLE app.legacy_probe (occurred_at TIMESTAMPTZ, text VARCHAR)"
        )
        db.execute(
            "INSERT INTO app.legacy_probe VALUES ('2025-11-02 01:30:00', 'kept')"
        )
        db.execute("CHECKPOINT")
    original = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(
        Exception, match="UTC.*compatibility|compatibility.*UTC"
    ) as caught:
        Database(
            path,
            secret_store=mock_secret_store,
            read_only=opening.get("read_only", False),
            no_auto_upgrade=opening.get("no_auto_upgrade", False),
            assume_initialized=opening.get("assume_initialized", False),
        )
    error = classify_user_error(caught.value)
    assert error is not None
    assert error.code == "infra_utc_upgrade_required"
    assert error.hint is not None and "offline maintenance" in error.hint
    assert hashlib.sha256(path.read_bytes()).hexdigest() == original


@pytest.mark.parametrize("host_zone", ["UTC", "America/New_York", "Asia/Tokyo"])
@pytest.mark.parametrize("read_only", [False, True])
def test_utc_connection_and_sqlmesh_cursor_preserve_instants(
    tmp_path: Path,
    mock_secret_store: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    host_zone: str,
    read_only: bool,
) -> None:
    monkeypatch.setenv("TZ", host_zone)
    time.tzset()
    try:
        path = tmp_path / "utc.duckdb"
        instant = datetime(2025, 11, 2, 5, 30, 0, 123456, tzinfo=UTC)
        with Database(
            path, secret_store=mock_secret_store, read_only=False, no_auto_upgrade=True
        ) as db:
            db.execute(
                "INSERT INTO app.transaction_notes "
                "(note_id, transaction_id, text, author, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                ["n1", "t1", "kept", "cli", instant],
            )
        with Database(
            path,
            secret_store=mock_secret_store,
            read_only=read_only,
            no_auto_upgrade=True,
        ) as db:
            assert db.execute("SELECT current_setting('TimeZone')").fetchone() == (
                "UTC",
            )
            assert db.execute(
                "SELECT created_at, epoch_us(created_at) FROM app.transaction_notes"
            ).fetchone() == (instant, 1762061400123456)
            adapter = DuckDBEngineAdapter(
                lambda: db.conn,
                default_catalog="moneybin",
                cursor_init=_pin_cursor_to_moneybin,
            )
            cursor = adapter.cursor
            assert cursor.execute("SELECT current_setting('TimeZone')").fetchone() == (
                "UTC",
            )
            assert cursor.execute(
                "SELECT epoch_us(created_at) FROM app.transaction_notes"
            ).fetchone() == (1762061400123456,)
            assert cursor.execute("SELECT current_database()").fetchone() == (
                "moneybin",
            )
            for handle in (db.conn, cursor):
                with pytest.raises(
                    duckdb.PermissionException, match="disabled by configuration"
                ):
                    handle.execute(
                        "SELECT * FROM read_csv('https://example.com/synthetic.csv')"
                    )
                if read_only:
                    with pytest.raises(duckdb.InvalidInputException, match="locked"):
                        handle.execute("SET TimeZone='Asia/Tokyo'")
    finally:
        monkeypatch.undo()
        time.tzset()


@pytest.mark.parametrize("no_auto_upgrade", [True, False])
def test_fresh_bootstrap_certifies_only_aware_schema(
    tmp_path: Path,
    mock_secret_store: MagicMock,
    no_auto_upgrade: bool,
) -> None:
    from moneybin.temporal_contract import inspect_temporal_contract

    path = tmp_path / "fresh.duckdb"
    with Database(
        path,
        secret_store=mock_secret_store,
        read_only=False,
        no_auto_upgrade=no_auto_upgrade,
    ) as db:
        assert inspect_temporal_contract(db.conn) == "utc"
        assert db.execute(
            "SELECT version FROM app.versions WHERE component = 'utc_contract'"
        ).fetchone() == ("2",)
        assert (
            db.execute(
                "SELECT schema_name, table_name, column_name "
                "FROM duckdb_columns() WHERE database_name = current_database() "
                "AND data_type = 'TIMESTAMP'"
            ).fetchall()
            == []
        )
        assert db.execute(
            "SELECT data_type FROM duckdb_columns() WHERE schema_name = 'raw' "
            "AND table_name = 'manual_transactions' "
            "AND column_name = 'transaction_date'"
        ).fetchone() == ("DATE",)

        package_version = db.execute(
            "SELECT version FROM app.versions WHERE component = 'moneybin'"
        ).fetchone()
        assert package_version == (
            None if no_auto_upgrade else (importlib.metadata.version("moneybin"),)
        )
    with Database(path, secret_store=mock_secret_store, read_only=True) as reopened:
        assert inspect_temporal_contract(reopened.conn) == "utc"
        assert (
            reopened.execute(
                "SELECT version FROM app.versions WHERE component = 'moneybin'"
            ).fetchone()
            == package_version
        )


@pytest.mark.parametrize("damage", ["pending", "naive", "missing-column"])
def test_marker_does_not_certify_incomplete_schema(
    tmp_path: Path,
    mock_secret_store: MagicMock,
    damage: str,
) -> None:
    from moneybin.temporal_contract import inspect_temporal_contract

    path = tmp_path / "incomplete.duckdb"
    with Database(
        path, secret_store=mock_secret_store, read_only=False, no_auto_upgrade=True
    ) as db:
        if damage == "pending":
            db.execute(
                "UPDATE app.versions SET version = '2-pending' "
                "WHERE component = 'utc_contract'"
            )
        elif damage == "naive":
            db.execute(
                "ALTER TABLE app.versions ALTER COLUMN installed_at TYPE TIMESTAMP"
            )
        else:
            db.execute("ALTER TABLE app.versions DROP COLUMN installed_at")
        assert inspect_temporal_contract(db.conn) == "incomplete"
    original = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(Exception, match="UTC.*compatibility|compatibility.*UTC"):
        Database(path, secret_store=mock_secret_store, read_only=True)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == original


def _clear_owned_schemas(db: Database) -> None:
    schemas = db.execute(
        "SELECT schema_name FROM duckdb_schemas() "
        "WHERE database_name = current_database() AND NOT internal "
        "AND schema_name <> 'main'"
    ).fetchall()
    for (schema,) in schemas:
        identifier = '"' + schema.replace('"', '""') + '"'
        db.execute(
            f"DROP SCHEMA IF EXISTS {identifier} CASCADE"
        )  # quoted synthetic catalog identifier


def _empty_existing_profile(path: Path, mock_secret_store: MagicMock) -> None:
    with Database(
        path, secret_store=mock_secret_store, read_only=False, no_auto_upgrade=True
    ) as db:
        _clear_owned_schemas(db)


def test_marker_cannot_certify_a_view_in_place_of_owned_table(
    tmp_path: Path,
    mock_secret_store: MagicMock,
) -> None:
    from moneybin.temporal_contract import inspect_temporal_contract

    path = tmp_path / "view_instead_of_table.duckdb"
    with Database(
        path, secret_store=mock_secret_store, read_only=False, no_auto_upgrade=True
    ) as db:
        db.execute("ALTER TABLE app.versions RENAME TO versions_evidence")
        db.execute("CREATE VIEW app.versions AS SELECT * FROM app.versions_evidence")
        assert inspect_temporal_contract(db.conn) == "incomplete"
    original = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(Exception, match="UTC.*compatibility|compatibility.*UTC"):
        with Database(path, secret_store=mock_secret_store, read_only=True):
            pass
    assert hashlib.sha256(path.read_bytes()).hexdigest() == original


def test_unknown_only_populated_profile_is_not_fresh(
    tmp_path: Path,
    mock_secret_store: MagicMock,
) -> None:
    from moneybin.temporal_contract import inspect_temporal_contract

    path = tmp_path / "unknown_populated.duckdb"
    with Database(
        path, secret_store=mock_secret_store, read_only=False, no_auto_upgrade=True
    ) as db:
        _clear_owned_schemas(db)
        db.execute("CREATE SCHEMA unknown")
        db.execute("CREATE TABLE unknown.evidence (id INTEGER)")
        db.execute("INSERT INTO unknown.evidence VALUES (17)")
        assert inspect_temporal_contract(db.conn) == "legacy"
    original = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(Exception, match="UTC.*compatibility|compatibility.*UTC"):
        with Database(
            path, secret_store=mock_secret_store, read_only=False, no_auto_upgrade=True
        ):
            pass
    assert hashlib.sha256(path.read_bytes()).hexdigest() == original


def test_empty_profile_cannot_be_assumed_initialized(
    tmp_path: Path,
    mock_secret_store: MagicMock,
) -> None:
    path = tmp_path / "empty_existing.duckdb"
    _empty_existing_profile(path, mock_secret_store)
    original = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(Exception, match="UTC.*compatibility|compatibility.*UTC"):
        with Database(
            path,
            secret_store=mock_secret_store,
            read_only=False,
            assume_initialized=True,
            no_auto_upgrade=True,
        ):
            pass
    assert hashlib.sha256(path.read_bytes()).hexdigest() == original


@pytest.mark.parametrize("policy", [None, "Invalid/Timezone"])
@pytest.mark.parametrize("read_only", [True, False])
def test_missing_or_invalid_reporting_policy_refuses_before_write(
    tmp_path: Path, mock_secret_store: MagicMock, policy: str | None, read_only: bool
) -> None:
    path = tmp_path / "missing-policy.duckdb"
    with Database(
        path, secret_store=mock_secret_store, read_only=False, no_auto_upgrade=True
    ) as db:
        db.execute("UPDATE app.profile_settings SET reporting_timezone = ?", [policy])
        db.execute("CHECKPOINT")
    before = path.read_bytes()
    with pytest.raises(
        Exception, match="UTC.*compatibility|compatibility.*UTC"
    ) as caught:
        Database(path, secret_store=mock_secret_store, read_only=read_only)
    classified = classify_user_error(caught.value)
    assert classified is not None
    assert classified.code == "infra_utc_upgrade_required"
    assert path.read_bytes() == before


def test_fresh_bootstrap_persists_and_audits_utc_policy(
    tmp_path: Path, mock_secret_store: MagicMock
) -> None:
    with Database(
        tmp_path / "fresh-policy.duckdb",
        secret_store=mock_secret_store,
        read_only=False,
        no_auto_upgrade=True,
    ) as db:
        assert db.execute(
            "SELECT reporting_timezone FROM app.profile_settings"
        ).fetchone() == ("UTC",)
        assert db.execute(
            "SELECT COUNT(*) FROM app.audit_log WHERE actor = 'database.init' AND action = 'profile_settings.set'"
        ).fetchone() == (1,)


def test_constructor_finishes_borrowed_results_before_checkpoint(
    tmp_path: Path, mock_secret_store: MagicMock
) -> None:
    with Database(
        tmp_path / "constructor.duckdb", secret_store=mock_secret_store, read_only=False
    ) as db:
        # A successful constructor must leave its owner able to checkpoint,
        # without FORCE, garbage collection, or draining another cursor here.
        db.checkpoint("post_migration")
        row = db.execute(
            "SELECT COUNT(*) FROM app.schema_migrations WHERE success"
        ).fetchone()
        assert row is not None and row[0] > 0


def test_bootstrap_covers_all_maintained_instant_columns(
    tmp_path: Path, mock_secret_store: MagicMock
) -> None:
    from moneybin.temporal_contract import (
        _expected_instant_columns,  # pyright: ignore[reportPrivateUsage]  # bootstrap inventory boundary
    )

    with Database(
        tmp_path / "inventory.duckdb",
        secret_store=mock_secret_store,
        read_only=False,
        no_auto_upgrade=True,
    ) as db:
        columns = db.execute(
            "SELECT schema_name, table_name, column_name FROM duckdb_columns() "
            "WHERE database_name = current_database() "
            "AND data_type = 'TIMESTAMP WITH TIME ZONE'"
        ).fetchall()
        expected = _expected_instant_columns()
        assert len({(schema, table) for schema, table, _column in expected}) >= 60
        assert expected <= set(columns)
        assert db.execute(
            "SELECT rules FROM app.utc_upgrade_provenance WHERE contract_version = 2"
        ).fetchone() == ("[]",)
