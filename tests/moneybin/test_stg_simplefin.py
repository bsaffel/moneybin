"""SimpleFIN financial projections retain source truth and canonical identity."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from moneybin.database import Database, sqlmesh_context
from moneybin.extractors.simplefin.extractor import SimpleFINExtractor
from tests.moneybin.test_extractors.test_simplefin_extractor import load, source

pytestmark = [pytest.mark.integration, pytest.mark.slow]


def bind_account(
    db: Database, *, subtype: str | None = "checking", kind: str = "depository"
) -> None:
    """Seed an accepted identity and explicit user account classification."""
    db.execute("""INSERT INTO app.account_links
        (link_id, account_id, ref_kind, ref_value, source_type, source_origin,
         status, decided_by, decided_at)
        SELECT DISTINCT 'link-' || source_origin, 'canonical', 'source_native',
            account_id, 'simplefin', source_origin, 'accepted', 'user', CURRENT_TIMESTAMP
        FROM raw.simplefin_accounts""")
    db.execute(
        """INSERT INTO app.account_settings
        (account_id, account_type, account_subtype, currency_code)
        VALUES ('canonical', ?, ?, 'USD')""",
        [kind, subtype],
    )


def materialize(db: Database) -> None:
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)


def test_v2_institution_uses_selected_account_batch_connection_name(
    db: Database,
) -> None:
    older = source(batch=1)
    older.batches[0].payload["connections"] = [
        {"conn_id": "native-connection", "name": "Previous Institution"}
    ]
    load(db, older, job=1)
    selected = source(batch=2)
    selected.batches[0].payload["connections"] = [
        {"conn_id": "native-connection", "name": "Chase"}
    ]
    load(db, selected, job=2)
    bind_account(db)
    materialize(db)
    assert db.execute(
        "SELECT institution_name, institution_slug FROM core.dim_accounts WHERE account_id='canonical'"
    ).fetchone() == ("Chase", "chase")


def test_v2_institution_masks_embedded_number_without_changing_raw(
    db: Database,
) -> None:
    data = source(account_changes={"name": ""})
    data.batches[0].payload["connections"] = [
        {"conn_id": "native-connection", "name": "Bank 123456789012"}
    ]
    load(db, data)
    assert db.execute(
        "SELECT original_payload ->> '$.connections[0].name' FROM raw.simplefin_responses"
    ).fetchone() == ("Bank 123456789012",)
    resolver_account = SimpleFINExtractor(db).source_accounts(data)[0]
    assert resolver_account.institution == "Bank ****9012"
    assert resolver_account.name_facts is not None
    assert resolver_account.name_facts.institution_name == "Bank ****9012"
    bind_account(db)
    materialize(db)
    assert db.execute(
        "SELECT institution_name, institution_slug, display_name FROM core.dim_accounts WHERE account_id='canonical'"
    ).fetchone() == (
        "Bank ****9012",
        "Bank ****9012",
        "Bank ****9012 checking",
    )


@pytest.mark.parametrize(
    "connections",
    [
        [],
        [
            {"conn_id": "native-connection", "name": "Chase"},
            {"conn_id": "native-connection", "name": "Other Institution"},
        ],
        {"entry": {"conn_id": "native-connection", "name": "Chase"}},
    ],
    ids=["absent", "ambiguous", "malformed-object"],
)
def test_v2_institution_is_null_without_unambiguous_connection_name(
    db: Database, connections: list[dict[str, str]] | dict[str, object]
) -> None:
    data = source()
    data.batches[0].payload["connections"] = connections
    load(db, data)
    bind_account(db)
    materialize(db)
    assert db.execute(
        "SELECT institution_name, institution_slug FROM core.dim_accounts WHERE account_id='canonical'"
    ).fetchone() == (None, None)


def test_signed_eur_transactions_use_scoped_native_reference_and_utc_day(
    db: Database,
) -> None:
    load(
        db,
        source(
            account_changes={"currency": "EUR"},
            transaction_changes={"posted": 1789862399},
        ),
    )
    load(
        db,
        source(
            batch=2,
            amount="25.00",
            transaction_changes={"id": "refund"},
            account_changes={"currency": "EUR"},
        ),
        job=2,
    )
    bind_account(db)
    materialize(db)
    rows = db.execute("""SELECT account_id, amount, currency_code, transaction_date,
        merchant_name, source_transaction_id FROM prep.int_transactions__unioned
        WHERE source_type = 'simplefin' ORDER BY amount""").fetchall()
    assert [row[:5] for row in rows] == [
        ("canonical", Decimal("-12.30"), "EUR", date(2026, 9, 19), None),
        ("canonical", Decimal("25.00"), "EUR", date(2026, 9, 20), None),
    ]
    assert {row[5] for row in rows} == {
        row[0]
        for row in db.execute(
            "SELECT transaction_reference FROM raw.simplefin_transactions"
        ).fetchall()
    }


def test_latest_fetch_wins_correction_return_and_delayed_delivery(db: Database) -> None:
    load(db, source(batch=3, day=3), job=3)
    load(db, source(batch=2, day=2, amount="-20.00"), job=2)
    load(db, source(), job=1)
    bind_account(db)
    materialize(db)
    assert db.execute("""SELECT amount FROM prep.int_transactions__unioned
        WHERE source_type = 'simplefin'""").fetchall() == [(Decimal("-12.30"),)]
    assert db.execute("SELECT count(*) FROM raw.simplefin_transactions").fetchone() == (
        3,
    )


def test_equal_fetch_tie_uses_immutable_batch_identity(db: Database) -> None:
    load(db, source(batch=2, amount="-20.00"), job=2)
    load(db, source(), job=1)
    bind_account(db)
    materialize(db)
    assert db.execute("""SELECT amount FROM prep.int_transactions__unioned
        WHERE source_type = 'simplefin'""").fetchall() == [(Decimal("-20.00"),)]


@pytest.mark.parametrize(
    "currency",
    ["", "usd", "US", "ZZZ", "XXX", "XTS", "XAU", "https://synthetic.invalid/unit"],
)
def test_invalid_currency_is_held_without_account_currency_fallback(
    db: Database, currency: str
) -> None:
    load(db, source(account_changes={"currency": currency}))
    bind_account(db)
    materialize(db)
    assert db.execute(
        "SELECT count(*) FROM core.dim_accounts WHERE account_id='canonical'"
    ).fetchone() == (1,)
    assert db.execute(
        "SELECT count(*) FROM prep.int_transactions__unioned WHERE source_type='simplefin'"
    ).fetchone() == (0,)
    assert db.execute(
        "SELECT count(*) FROM core.fct_balances WHERE source_type='simplefin'"
    ).fetchone() == (0,)
    assert db.execute("SELECT currency FROM raw.simplefin_accounts").fetchone() == (
        currency,
    )


@pytest.mark.parametrize(
    "changes",
    [{"pending": True}, {"posted": 0}, {"posted": 9999999999}, {"amount": "-1.001"}],
)
def test_held_transaction_does_not_enter_ledger(
    db: Database, changes: dict[str, object]
) -> None:
    load(db, source(transaction_changes=changes))
    bind_account(db)
    materialize(db)
    assert db.execute(
        "SELECT count(*) FROM core.dim_accounts WHERE account_id='canonical'"
    ).fetchone() == (1,)
    assert db.execute(
        "SELECT count(*) FROM prep.int_transactions__unioned WHERE source_type='simplefin'"
    ).fetchone() == (0,)
    assert db.execute("SELECT count(*) FROM raw.simplefin_transactions").fetchone() == (
        1,
    )


def test_unresolved_native_id_never_becomes_canonical(db: Database) -> None:
    load(db, source())
    materialize(db)
    assert db.execute("SELECT count(*) FROM core.dim_accounts").fetchone() == (0,)
    assert db.execute(
        "SELECT count(*) FROM prep.int_transactions__unioned"
    ).fetchone() == (0,)
    assert db.execute("SELECT count(*) FROM raw.simplefin_accounts").fetchone() == (1,)


def test_broad_type_does_not_invent_supported_subtype(db: Database) -> None:
    load(db, source())
    bind_account(db, subtype=None)
    materialize(db)
    assert db.execute(
        "SELECT account_type, account_subtype FROM core.dim_accounts WHERE account_id='canonical'"
    ).fetchone() == ("depository", None)
    assert db.execute(
        "SELECT count(*) FROM prep.int_transactions__unioned WHERE source_type='simplefin'"
    ).fetchone() == (0,)


def test_malformed_newer_transaction_does_not_replace_latest_validated_observation(
    db: Database,
) -> None:
    load(db, source())
    load(db, source(batch=2, day=2, transaction_changes={"amount": "not-money"}), job=2)
    bind_account(db)
    materialize(db)
    assert db.execute(
        "SELECT amount FROM prep.int_transactions__unioned WHERE source_type='simplefin'"
    ).fetchall() == [(Decimal("-12.30"),)]
    assert db.execute(
        "SELECT validation_error FROM raw.simplefin_transactions ORDER BY fetched_at DESC LIMIT 1"
    ).fetchone() == ("invalid_amount",)


def test_newer_custom_currency_holds_previously_supported_event(db: Database) -> None:
    load(db, source())
    load(
        db,
        source(
            batch=2,
            day=2,
            account_changes={"currency": "https://synthetic.invalid/unit"},
        ),
        job=2,
    )
    bind_account(db)
    materialize(db)
    assert db.execute(
        "SELECT count(*) FROM prep.int_transactions__unioned WHERE source_type='simplefin'"
    ).fetchone() == (0,)
    assert db.execute("SELECT count(*) FROM raw.simplefin_transactions").fetchone() == (
        2,
    )


def test_supported_type_requires_corresponding_subtype(db: Database) -> None:
    cases = [
        ("depository", "checking", True),
        ("depository", "savings", True),
        ("depository", "cash", True),
        ("credit", "credit card", True),
        ("depository", "credit card", False),
        ("credit", "checking", False),
        ("loan", "checking", False),
        ("investment", "brokerage", False),
    ]
    expected: set[str] = set()
    for number, (kind, subtype, supported) in enumerate(cases, start=1):
        native = f"native-{number}"
        canonical = f"canonical-{number}"
        load(db, source(batch=number, account_changes={"id": native}), job=number)
        db.execute(
            """INSERT INTO app.account_links
            (link_id, account_id, ref_kind, ref_value, source_type, source_origin,
             status, decided_by, decided_at)
            SELECT ?, ?, 'source_native', account_id, 'simplefin', source_origin,
                'accepted', 'user', CURRENT_TIMESTAMP
            FROM raw.simplefin_accounts WHERE account_id=?""",
            [f"link-{number}", canonical, native],
        )
        db.execute(
            "INSERT INTO app.account_settings (account_id, account_type, account_subtype) VALUES (?, ?, ?)",
            [canonical, kind, subtype],
        )
        if supported:
            expected.add(canonical)
    materialize(db)
    assert {
        row[0]
        for row in db.execute(
            "SELECT account_id FROM prep.int_transactions__unioned WHERE source_type='simplefin'"
        ).fetchall()
    } == expected
    assert {
        row[0]
        for row in db.execute(
            "SELECT account_id FROM core.fct_balances WHERE source_type='simplefin'"
        ).fetchall()
    } == expected
    assert db.execute("SELECT count(*) FROM core.dim_accounts").fetchone() == (
        len(cases),
    )


def test_three_minor_unit_currency_accepts_only_exact_two_decimal_value(
    db: Database,
) -> None:
    load(db, source(amount="-1.20", account_changes={"currency": "BHD"}))
    load(
        db,
        source(
            batch=2,
            amount="-1.201",
            account_changes={"currency": "BHD"},
            transaction_changes={"id": "three-decimal"},
        ),
        job=2,
    )
    bind_account(db)
    materialize(db)
    assert db.execute(
        "SELECT amount, currency_code FROM prep.int_transactions__unioned WHERE source_type='simplefin'"
    ).fetchall() == [(Decimal("-1.20"), "BHD")]


def test_missing_currency_is_retained_but_never_inherited(db: Database) -> None:
    load(db, source(account_changes={"currency": None}))
    bind_account(db)
    materialize(db)
    assert db.execute(
        "SELECT count(*) FROM core.dim_accounts WHERE account_id='canonical'"
    ).fetchone() == (1,)
    assert db.execute(
        "SELECT count(*) FROM prep.int_transactions__unioned WHERE source_type='simplefin'"
    ).fetchone() == (0,)
    assert db.execute(
        "SELECT count(*) FROM core.fct_balances WHERE source_type='simplefin'"
    ).fetchone() == (0,)


def test_shared_timestamps_keep_utc_naive_contract_in_non_utc_session(
    db: Database,
) -> None:
    load(db, source())
    bind_account(db)
    db.execute("SET TimeZone = 'America/New_York'")
    raw = db.execute(
        "SELECT fetched_at, loaded_at FROM raw.simplefin_transactions"
    ).fetchone()
    assert raw is not None
    fetched_at, loaded_at = raw
    assert isinstance(fetched_at, datetime) and fetched_at.tzinfo is not None
    assert isinstance(loaded_at, datetime) and loaded_at.tzinfo is not None
    expected_fetched = datetime(2026, 9, 21)
    expected_loaded = loaded_at.astimezone(UTC).replace(tzinfo=None)
    materialize(db)
    types = {
        "accounts": db.execute(
            "SELECT typeof(extracted_at), typeof(loaded_at), typeof(updated_at) FROM core.dim_accounts"
        ).fetchone(),
        "union": db.execute(
            "SELECT typeof(source_extracted_at), typeof(loaded_at) FROM prep.int_transactions__unioned"
        ).fetchone(),
        "transactions": db.execute(
            "SELECT typeof(updated_at) FROM core.fct_transactions"
        ).fetchone(),
        "balances": db.execute(
            "SELECT typeof(updated_at) FROM core.fct_balances"
        ).fetchone(),
    }
    assert types == {
        "accounts": ("TIMESTAMP", "TIMESTAMP", "TIMESTAMP"),
        "union": ("TIMESTAMP", "TIMESTAMP"),
        "transactions": ("TIMESTAMP",),
        "balances": ("TIMESTAMP",),
    }, repr(types)
    assert db.execute("SELECT extracted_at FROM core.dim_accounts").fetchone() == (
        expected_fetched,
    )
    assert db.execute(
        "SELECT source_extracted_at, loaded_at FROM prep.int_transactions__unioned"
    ).fetchone() == (expected_fetched, expected_loaded)
    raw_account = db.execute("SELECT loaded_at FROM raw.simplefin_accounts").fetchone()
    raw_balance = db.execute("SELECT loaded_at FROM raw.simplefin_balances").fetchone()
    assert raw_account is not None and raw_balance is not None
    assert db.execute("SELECT loaded_at FROM core.dim_accounts").fetchone() == (
        raw_account[0].astimezone(UTC).replace(tzinfo=None),
    )
    assert db.execute("SELECT updated_at FROM core.fct_balances").fetchone() == (
        raw_balance[0].astimezone(UTC).replace(tzinfo=None),
    )
    assert (
        db.execute(
            "SELECT typeof(posted_at), typeof(extracted_at), typeof(loaded_at) FROM prep.stg_simplefin__transactions"
        ).fetchone()
        == ("TIMESTAMP WITH TIME ZONE",) * 3
    )
