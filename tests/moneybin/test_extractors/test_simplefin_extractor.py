"""Immutable SimpleFIN delivery evidence, precision and protected originals."""

from __future__ import annotations

import importlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from moneybin.connectors.sync_models import ScopeOutcome, SimpleFINSource
from moneybin.database import Database
from moneybin.privacy.sql_query import execute_sql_query

BASE = datetime(2026, 9, 20, tzinfo=UTC)
CONNECTION = UUID(int=100)


def source(
    *,
    batch: int = 1,
    amount: str = "-12.30",
    day: int = 1,
    account_changes: dict[str, Any] | None = None,
    transaction_changes: dict[str, Any] | None = None,
) -> SimpleFINSource:
    transaction = {
        "id": "purchase",
        "amount": amount,
        "posted": 1789862400,
        "description": "Synthetic purchase",
    }
    transaction.update(transaction_changes or {})
    account = {
        "id": "acct",
        "conn_id": "native-connection",
        "name": "Synthetic",
        "currency": "USD",
        "balance": "100.25",
        "available-balance": "90.20",
        "balance-date": 1789862400,
        "transactions": [transaction],
        "extra": {"hostile": [{"secret": "12-34-56"}]},
    }
    account.update(account_changes or {})
    return SimpleFINSource.model_validate({
        "provider": "simplefin",
        "connection_id": str(CONNECTION),
        "credential_generation": 1,
        "protocol_version": "2.0.0-draft",
        "batches": [
            {
                "batch_id": str(UUID(int=batch)),
                "fetched_at": (BASE + timedelta(days=day)).isoformat(),
                "window": {
                    "start": "2026-09-01T00:00:00Z",
                    "end": "2026-09-20T00:00:00Z",
                },
                "payload": {
                    "accounts": [account],
                    "errlist": [],
                    "connections": [
                        {
                            "conn_id": "native-connection",
                            "name": "Synthetic",
                            "org_id": "org",
                            "sfin_url": "https://synthetic.invalid",
                        }
                    ],
                },
            }
        ],
        "outcomes": [],
    })


def loader(db: Database) -> Any:
    tables = db.execute(
        "SELECT table_name FROM duckdb_tables() WHERE schema_name='raw'"
    ).fetchall()
    assert ("simplefin_sync_receipts",) in tables, (
        "SimpleFIN immutable receipts must exist"
    )
    return importlib.import_module("moneybin.extractors.simplefin.extractor")


def load(
    db: Database,
    data: SimpleFINSource,
    job: int = 1,
    *,
    day: int = 1,
    status: str = "completed",
) -> Any:
    module = loader(db)
    return module.SimpleFINExtractor(db).load(
        data,
        job_id=UUID(int=job),
        job_context=module.SimpleFINJobContext(
            status=status,
            started_at=BASE + timedelta(days=day),
            completed_at=BASE + timedelta(days=day, minutes=1),
        ),
    )


def test_replay_is_idempotent(db: Database) -> None:
    first = load(db, source())
    replay = load(db, source())
    assert (first.accounts, first.transactions, first.balances) == (1, 1, 1)
    assert (replay.accounts, replay.transactions, replay.balances) == (0, 0, 0)
    assert replay.batch_ids == [UUID(int=1)]
    load(db, source(), job=2)
    assert db.execute(
        "SELECT count(*) FROM raw.simplefin_sync_receipts"
    ).fetchone() == (2,)
    assert db.execute("SELECT count(*) FROM raw.simplefin_responses").fetchone() == (1,)


def test_correction_appends_revision(db: Database) -> None:
    load(db, source())
    load(db, source(batch=2, amount="-15.00", day=2), job=2)
    load(db, source(batch=3, day=3), job=3)
    rows = db.execute(
        "SELECT transaction_reference, observation_version, amount FROM raw.simplefin_transactions ORDER BY fetched_at"
    ).fetchall()
    assert len(rows) == 3
    assert len({r[0] for r in rows}) == 1
    assert len({r[1] for r in rows}) == 2
    assert rows[0][1] == rows[2][1]
    assert [r[2] for r in rows] == [
        Decimal("-12.30"),
        Decimal("-15.00"),
        Decimal("-12.30"),
    ]


def test_delayed_batch_keeps_newer_projection(db: Database) -> None:
    load(db, source(batch=2, amount="-15.00", day=2), job=2)
    load(db, source(), job=1)
    assert db.execute(
        "SELECT amount FROM raw.simplefin_transactions ORDER BY fetched_at DESC, batch_id DESC, observation_version DESC LIMIT 1"
    ).fetchone() == (Decimal("-15.00"),)


def test_two_accounts_same_native_transaction_id(db: Database) -> None:
    load(db, source())
    load(db, source(batch=2, account_changes={"id": "other"}), job=2)
    load(db, source(batch=3, account_changes={"conn_id": "other-scope"}), job=3)
    rows = db.execute(
        "SELECT transaction_reference FROM raw.simplefin_transactions"
    ).fetchall()
    assert len({r[0] for r in rows}) == 3
    assert all(r[0].startswith("simplefin_") and len(r[0]) == 74 for r in rows)


def test_failed_job_rolls_back_receipts(db: Database) -> None:
    loader(db)
    db.begin()
    load(db, source())
    db.rollback()
    for table in [
        "simplefin_sync_receipts",
        "simplefin_responses",
        "simplefin_transactions",
    ]:
        assert db.execute(f"SELECT count(*) FROM raw.{table}").fetchone() == (0,)  # noqa: S608  # test constants


def test_conflicting_batch_and_job_are_rejected(db: Database) -> None:
    load(db, source())
    with pytest.raises(ValueError, match="immutable"):
        load(db, source(amount="-99.00"), job=2)
    changed = source()
    changed.credential_generation = 2
    with pytest.raises(ValueError, match="immutable"):
        load(db, changed)


def test_balance_corrections_at_same_timestamp_are_auditable(db: Database) -> None:
    load(db, source())
    load(db, source(batch=2, account_changes={"balance": "101.25"}), job=2)
    rows = db.execute(
        "SELECT balance, available_balance, balance_at, observation_version FROM raw.simplefin_balances ORDER BY balance"
    ).fetchall()
    assert [r[:2] for r in rows] == [
        (Decimal("100.25"), Decimal("90.20")),
        (Decimal("101.25"), Decimal("90.20")),
    ]
    assert rows[0][2] == rows[1][2]
    assert rows[0][3] != rows[1][3]


@pytest.mark.parametrize("balance_date", [0, -1])
@pytest.mark.parametrize("balance", ["100.25", "100.001"])
def test_nonpositive_balance_timestamp_is_structural_and_retains_original(
    db: Database, balance_date: int, balance: str
) -> None:
    result = load(
        db, source(account_changes={"balance-date": balance_date, "balance": balance})
    )
    row = db.execute(
        "SELECT balance_at, validation_error, original_record FROM raw.simplefin_balances"
    ).fetchone()
    assert row is not None
    assert row[:2] == (None, "invalid_timestamp")
    assert json.loads(row[2])["balance-date"] == balance_date
    assert json.loads(row[2])["balance"] == balance
    assert any(outcome.error_code == "invalid_timestamp" for outcome in result.outcomes)


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"amount": "-1.001"}, "unsupported_precision"),
        ({"posted": 0}, "nonpositive_posted"),
        ({"posted": "invalid"}, "invalid_timestamp"),
        ({"posted": 9999999999}, "future_timestamp"),
        ({"pending": True}, "pending"),
    ],
)
def test_untrusted_transaction_retains_original(
    db: Database, changes: dict[str, Any], reason: str
) -> None:
    result = load(db, source(transaction_changes=changes))
    row = db.execute(
        "SELECT original_record, validation_error FROM raw.simplefin_transactions"
    ).fetchone()
    assert row is not None
    original = json.loads(row[0])
    assert all(original[k] == v for k, v in changes.items())
    assert row[1] == reason
    assert any(o.error_code == reason for o in result.outcomes)


def test_malformed_identity_remains_only_in_response(db: Database) -> None:
    result = load(db, source(account_changes={"id": ""}))
    assert result.accounts == result.transactions == result.balances == 0
    assert any(o.error_code == "invalid_identity" for o in result.outcomes)
    assert db.execute("SELECT count(*) FROM raw.simplefin_responses").fetchone() == (1,)


def test_zero_batch_receipts_preserve_failure_and_broker_job_order(
    db: Database,
) -> None:
    load(db, source())
    empty = source()
    empty.batches = []
    empty.outcomes = [
        ScopeOutcome(
            account_id=None,
            native_scope=None,
            window=source().batches[0].window,
            status="failed",
            error_code="provider_failure",
        )
    ]
    load(db, empty, job=3, day=3, status="failed")
    load(db, empty, job=2, day=2, status="deferred")
    load(db, empty, job=3, day=3, status="failed")
    rows = db.execute(
        "SELECT job_id, status, source_metadata FROM raw.simplefin_sync_receipts ORDER BY started_at"
    ).fetchall()
    assert [r[0] for r in rows] == [UUID(int=1), UUID(int=2), UUID(int=3)]
    assert rows[-1][1] == "failed"
    assert json.loads(rows[-1][2]) == empty.model_dump(mode="json", exclude={"batches"})
    assert db.execute("SELECT count(*) FROM raw.simplefin_responses").fetchone() == (1,)


def test_nested_originals_mask_through_raw_and_prep_queries(db: Database) -> None:
    load(db, source())
    for query in [
        "SELECT original_record AS secret FROM raw.simplefin_accounts",
        "SELECT json_extract_string(original_record, '$.extra.hostile[0].secret') AS secret FROM raw.simplefin_accounts",
        "WITH nested AS (SELECT original_record FROM raw.simplefin_accounts) SELECT original_record AS secret FROM nested",
    ]:
        result = execute_sql_query(db, query, max_rows=10)
        assert result.records[0]["secret"] == "*****"  # noqa: S105  # expected privacy mask, not a credential


def test_v1_scope_precedence_and_framing(db: Database) -> None:
    loader(db)
    identity = importlib.import_module("moneybin.extractors.simplefin.identity")
    origin = identity.source_origin(
        CONNECTION,
        "1.0.7",
        {"sfin-url": "https://synthetic.invalid", "id": "native", "name": "Old name"},
    )
    renamed = identity.source_origin(
        CONNECTION,
        "1.0.7",
        {"sfin-url": "https://synthetic.invalid", "id": "native", "name": "New name"},
    )
    assert origin == renamed
    assert identity.scope_identity_strength("1.0.7", {"id": "native"}) == "strong"
    assert identity.scope_identity_strength("1.0.7", {"name": "Only name"}) == "weak"
    assert identity.transaction_reference(
        origin, "a|b", "c"
    ) != identity.transaction_reference(origin, "a", "b|c")
    data = source()
    raw = data.model_dump(mode="json")
    raw["protocol_version"] = "1.0.7"
    payload = raw["batches"][0]["payload"]
    payload.pop("connections")
    payload.pop("errlist")
    payload["errors"] = ["Synthetic error with 12-34-56"]
    account = payload["accounts"][0]
    account.pop("conn_id")
    account["org"] = {
        "sfin-url": "https://synthetic.invalid",
        "id": "native",
        "name": "New name",
    }
    load(db, SimpleFINSource.model_validate(raw))
    row = db.execute(
        "SELECT source_origin, native_scope, identity_strength FROM raw.simplefin_accounts"
    ).fetchone()
    assert row is not None and row[0] == origin and row[2] == "strong"
    assert json.loads(row[1]) == account["org"]


@pytest.mark.parametrize("name", [None, "Synthetic Bank"])
def test_v1_institution_uses_domain_when_name_is_absent(
    db: Database, name: str | None
) -> None:
    org = {"domain": "synthetic.invalid", "sfin-url": "https://synthetic.invalid"}
    if name is not None:
        org["name"] = name
    data = source(account_changes={"org": org})
    data.protocol_version = "1.0.7"
    load(db, data)
    expected = name or "synthetic.invalid"
    row = db.execute(
        "SELECT institution_label, original_record FROM raw.simplefin_accounts"
    ).fetchone()
    assert row is not None
    assert row[0] == expected
    assert json.loads(row[1])["org"] == org
    accounts = loader(db).SimpleFINExtractor(db).source_accounts(data)
    assert len(accounts) == 1
    assert accounts[0].institution == expected
    assert accounts[0].name_facts.institution_name == expected


def test_duplicate_native_accounts_fail_scope_without_overwrite(db: Database) -> None:
    data = source()
    payload = data.batches[0].payload
    accounts = cast(list[object], payload["accounts"])
    accounts.append(accounts[0])
    result = load(db, data)
    assert result.accounts == result.transactions == result.balances == 0
    assert any(o.error_code == "ambiguous_identity" for o in result.outcomes)


def test_source_receipt_survives_reopening(
    db: Database, mock_secret_store: MagicMock
) -> None:
    data = source()
    data.batches = []
    load(db, data, status="failed")
    path = db._db_path  # pyright: ignore[reportPrivateUsage]  # reopen test database
    db.close()
    with Database(
        path, read_only=True, secret_store=mock_secret_store, no_auto_upgrade=True
    ) as reopened:
        assert reopened.execute(
            "SELECT status, batch_ids FROM raw.simplefin_sync_receipts"
        ).fetchone() == ("failed", "[]")


@pytest.mark.parametrize("entity", ["accounts", "transactions", "balances"])
def test_prep_original_json_cannot_disclose_nested_identifiers(
    db: Database, entity: str
) -> None:
    load(db, source())
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        f"CREATE VIEW prep.stg_simplefin__{entity} AS "  # noqa: S608  # fixed parameterized test table names
        f"SELECT original_record, native_scope FROM raw.simplefin_{entity}"
    )
    queries = [
        f"SELECT original_record AS evidence FROM prep.stg_simplefin__{entity}",  # noqa: S608  # fixed test table names
        f"SELECT json_extract_string(native_scope, '$.conn_id') AS evidence FROM prep.stg_simplefin__{entity}",  # noqa: S608  # fixed test table names
    ]
    for query in queries:
        result = execute_sql_query(db, query, max_rows=10)
        assert result.records == [{"evidence": "*****"}]


def test_response_and_source_outcome_json_are_lossless_and_whole_masked(
    db: Database,
) -> None:
    data = source()
    errors = [
        {
            "code": "act.failed",
            "msg": "Synthetic ID 12-34-56 ☃",
            "extra": {"nested": ["short-id"]},
        }
    ]
    data.batches[0].payload["errlist"] = errors
    data.outcomes = [
        ScopeOutcome.model_validate({
            "account_id": "12-34-56",
            "native_scope": {"kind": "simplefin_v2_connection", "conn_id": "short-id"},
            "window": data.batches[0].window,
            "status": "failed",
            "error_code": "provider_failure",
        })
    ]
    load(db, data)
    stored = db.execute(
        "SELECT original_payload, source_errors FROM raw.simplefin_responses"
    ).fetchone()
    assert stored is not None
    assert json.loads(stored[0]) == data.batches[0].payload
    assert json.loads(stored[1]) == {"errlist": errors}
    for query in [
        "SELECT original_payload AS evidence FROM raw.simplefin_responses",
        "SELECT json_extract_string(source_errors, '$.errlist[0].msg') AS evidence FROM raw.simplefin_responses",
        "SELECT json_extract_string(source_metadata, '$.outcomes[0].account_id') AS evidence FROM raw.simplefin_sync_receipts",
        "SELECT local_outcomes AS evidence FROM raw.simplefin_sync_receipts",
    ]:
        result = execute_sql_query(db, query, max_rows=10)
        assert result.records == [{"evidence": "*****"}]


def test_equal_fetch_tie_is_independent_of_arrival_order(db: Database) -> None:
    load(db, source(batch=2, amount="-20.00"), job=2)
    load(db, source(batch=1, amount="-10.00"), job=1)
    assert db.execute(
        "SELECT amount FROM raw.simplefin_transactions ORDER BY fetched_at DESC, batch_id DESC, observation_version DESC LIMIT 1"
    ).fetchone() == (Decimal("-20.00"),)


def test_unsupported_balance_precision_retains_provider_sign_and_original(
    db: Database,
) -> None:
    result = load(
        db,
        source(account_changes={"balance": "-100.001", "available-balance": "-20.00"}),
    )
    row = db.execute(
        "SELECT balance, available_balance, balance_original, validation_error FROM raw.simplefin_balances"
    ).fetchone()
    assert row == (None, Decimal("-20.00"), "-100.001", "unsupported_precision")
    assert any(
        outcome.error_code == "unsupported_precision" for outcome in result.outcomes
    )


def test_currency_correction_appends_revision_without_changing_event(
    db: Database,
) -> None:
    load(db, source())
    load(db, source(batch=2, day=2, account_changes={"currency": "EUR"}), job=2)
    rows = db.execute("""SELECT transaction_reference, observation_version,
        currency, original_record FROM raw.simplefin_transactions ORDER BY fetched_at""").fetchall()
    assert len(rows) == 2
    assert rows[0][0] == rows[1][0]
    assert rows[0][1] != rows[1][1]
    assert [row[2] for row in rows] == ["USD", "EUR"]
    assert json.loads(rows[0][3]) == json.loads(rows[1][3])


def test_oversized_exponent_retains_raw_with_typed_rejection(db: Database) -> None:
    result = load(db, source(amount="1e1000000"))
    row = db.execute("""SELECT amount, amount_original, original_record,
        validation_error FROM raw.simplefin_transactions""").fetchone()
    assert row is not None
    assert row[0] is None
    assert row[1] == "1e1000000"
    assert json.loads(row[2])["amount"] == "1e1000000"
    assert row[3] == "unsupported_precision"
    assert any(
        outcome.error_code == "unsupported_precision" for outcome in result.outcomes
    )
    assert result.transactions == 1
    assert db.execute("SELECT count(*) FROM raw.simplefin_responses").fetchone() == (1,)
    assert db.execute(
        "SELECT count(*) FROM raw.simplefin_sync_receipts"
    ).fetchone() == (1,)


@pytest.mark.parametrize(
    ("account_changes", "transaction_changes", "table", "expected"),
    [
        (
            {},
            {"amount": "-1.001", "posted": 9999999999},
            "transactions",
            "future_timestamp",
        ),
        (
            {},
            {"amount": "-1.001", "pending": "invalid"},
            "transactions",
            "invalid_pending",
        ),
        (
            {"balance": "-1.001", "available-balance": "invalid"},
            {},
            "balances",
            "invalid_amount",
        ),
        (
            {"balance": "-1.001", "balance-date": 9999999999},
            {},
            "balances",
            "future_timestamp",
        ),
    ],
)
def test_structural_error_is_not_hidden_by_financial_hold(
    db: Database,
    account_changes: dict[str, Any],
    transaction_changes: dict[str, Any],
    table: str,
    expected: str,
) -> None:
    load(
        db,
        source(
            account_changes=account_changes, transaction_changes=transaction_changes
        ),
    )
    row = db.execute(f"SELECT validation_error FROM raw.simplefin_{table}").fetchone()  # noqa: S608  # fixed test parameter table names
    assert row == (expected,)


@pytest.mark.parametrize("transacted", [None, "invalid", 0, -1, 9999999999])
def test_malformed_optional_transacted_instant_is_structural(
    db: Database, transacted: object
) -> None:
    load(
        db,
        source(transaction_changes={"amount": "-1.001", "transacted_at": transacted}),
    )
    assert db.execute(
        "SELECT validation_error FROM raw.simplefin_transactions"
    ).fetchone() == (
        "future_timestamp" if transacted == 9999999999 else "invalid_timestamp",
    )


def test_absent_optional_transacted_instant_is_valid(db: Database) -> None:
    load(db, source())
    assert db.execute(
        "SELECT validation_error, transacted_at FROM raw.simplefin_transactions"
    ).fetchone() == (None, None)


def test_paired_v1_organization_extension_keeps_identity_children_and_originals(
    db: Database,
) -> None:
    """Native org extensions are lossless Raw, not closed scope-envelope fields."""
    from copy import deepcopy
    from pathlib import Path

    from pydantic import ValidationError

    from moneybin.connectors.sync_models import SyncEnvelopeV2

    fixture = (
        Path(__file__).parents[1]
        / "connectors/fixtures/sync_v2/simplefin_v1_org_extension.json"
    )
    original = json.loads(fixture.read_text())
    extended = SyncEnvelopeV2.model_validate(original).sources[0]
    assert isinstance(extended, SimpleFINSource)
    baseline = deepcopy(original)
    baseline["sources"][0]["batches"][0]["payload"]["accounts"][0]["org"].pop(
        "extension"
    )
    plain = SyncEnvelopeV2.model_validate(baseline).sources[0]
    assert isinstance(plain, SimpleFINSource)
    load(db, plain)
    before = db.execute(
        "SELECT source_origin, account_id, transaction_reference FROM raw.simplefin_transactions ORDER BY source_origin"
    ).fetchall()
    extended.batches[0].batch_id = UUID(int=2)
    result = load(db, extended, job=2)
    assert (result.accounts, result.transactions, result.balances) == (2, 2, 2)
    after = db.execute(
        "SELECT source_origin, account_id, transaction_reference FROM raw.simplefin_transactions WHERE batch_id=? ORDER BY source_origin",
        [str(UUID(int=2))],
    ).fetchall()
    assert before == after
    raw = db.execute(
        "SELECT original_payload FROM raw.simplefin_responses WHERE batch_id=?",
        [str(UUID(int=2))],
    ).fetchone()
    assert raw is not None
    assert json.loads(raw[0]) == original["sources"][0]["batches"][0]["payload"]
    record = db.execute(
        "SELECT original_record FROM raw.simplefin_accounts WHERE batch_id=? AND json_extract(original_record, '$.org.extension') IS NOT NULL",
        [str(UUID(int=2))],
    ).fetchone()
    assert record is not None
    original_accounts = original["sources"][0]["batches"][0]["payload"]["accounts"]
    assert json.loads(record[0]) == {
        key: value
        for key, value in original_accounts[0].items()
        if key != "transactions"
    }
    children = db.execute(
        "SELECT original_record FROM raw.simplefin_transactions WHERE batch_id=?",
        [str(UUID(int=2))],
    ).fetchall()
    assert sorted(
        json.dumps(json.loads(row[0]), sort_keys=True) for row in children
    ) == sorted(
        json.dumps(child, sort_keys=True)
        for account in original_accounts
        for child in account["transactions"]
    )
    invalid_envelope = deepcopy(original)
    invalid_envelope["sources"][0]["outcomes"][0]["native_scope"]["org"][
        "extension"
    ] = "forbidden"
    with pytest.raises(ValidationError):
        SyncEnvelopeV2.model_validate(invalid_envelope)
