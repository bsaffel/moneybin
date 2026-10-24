"""Native event identity survives direct and indirect matching across batches."""

from typing import Any, cast

import pytest

from moneybin.database import Database
from moneybin.matching.assignment import assign_components
from moneybin.matching.scoring import CandidatePair, get_candidates_within_source


def pair(native: str, batch: str) -> CandidatePair:
    return CandidatePair(
        source_transaction_id_a="statement",
        source_type_a="ofx",
        source_origin_a="bank",
        source_file_a="statement.ofx",
        source_transaction_id_b=native,
        source_type_b="simplefin",
        source_origin_b="grant",
        source_file_b=batch,
        account_id="canonical",
        date_distance_days=0,
        description_similarity=1.0,
        confidence_score=1.0,
        description_a="Synthetic purchase",
        description_b="Synthetic purchase",
    )


@pytest.mark.parametrize("persisted", [False, True])
def test_distinct_native_events_cannot_join_through_cross_source_bridge(
    persisted: bool,
) -> None:
    first, second = pair("native-one", "batch-one"), pair("native-two", "batch-two")
    seeds = (
        [(("ofx", "statement", "canonical"), ("simplefin", "native-one", "canonical"))]
        if persisted
        else []
    )
    assigned = assign_components([first, second], seed_edges=seeds)
    assert len(assigned) == (0 if persisted else 1)


def test_distinct_native_events_do_not_become_within_source_candidates(
    db: Database,
) -> None:
    db.execute("""CREATE TABLE native_events (
        source_transaction_id VARCHAR, account_id VARCHAR, transaction_date DATE,
        amount DECIMAL(18,2), description VARCHAR, source_type VARCHAR,
        source_origin VARCHAR, source_file VARCHAR, currency_code VARCHAR)""")
    for event, batch in [("native-one", "batch-one"), ("native-two", "batch-two")]:
        db.execute(
            """INSERT INTO native_events VALUES (?, 'canonical', '2026-09-20',
            -12.30, 'Synthetic purchase', 'simplefin', 'grant', ?, 'EUR')""",
            [event, batch],
        )
    assert get_candidates_within_source(db, table="main.native_events") == []


def test_retained_account_builder_reuses_binding_after_credential_rotation(
    db: Database,
) -> None:
    from moneybin.extractors.simplefin.extractor import SimpleFINExtractor
    from moneybin.services.account_resolver import AccountResolver
    from tests.moneybin.test_extractors.test_simplefin_extractor import load, source

    data = source(account_changes={"currency": "https://synthetic.invalid/unit"})
    load(db, data)
    extractor = SimpleFINExtractor(db)
    accounts = extractor.source_accounts(data)
    assert len(accounts) == 1
    assert accounts[0].persistent_token is None
    assert accounts[0].account_number is None
    assert accounts[0].last_four is None
    resolver = AccountResolver(db, actor="system")
    first = resolver.resolve(accounts[0])
    rotated = source(batch=2, day=2)
    rotated.credential_generation = 2
    load(db, rotated, job=2)
    second = resolver.resolve(extractor.source_accounts(rotated)[0])
    assert second.account_id == first.account_id
    assert not second.is_new


def test_changed_scope_requires_review_instead_of_silent_relink(db: Database) -> None:
    from moneybin.extractors.simplefin.extractor import SimpleFINExtractor
    from moneybin.services.account_resolver import AccountResolver
    from tests.moneybin.test_extractors.test_simplefin_extractor import load, source
    from tests.moneybin.test_stg_simplefin import materialize

    data = source()
    load(db, data)
    extractor = SimpleFINExtractor(db)
    resolver = AccountResolver(db, actor="system")
    first = resolver.resolve(extractor.source_accounts(data)[0])
    materialize(db)
    changed = source(batch=2, day=2, account_changes={"conn_id": "changed-scope"})
    load(db, changed, job=2)
    second = resolver.resolve(extractor.source_accounts(changed)[0])
    assert second.account_id != first.account_id
    assert second.outcome == "pending_review"
    assert second.pending_decision_ids


def test_retained_account_builder_uses_broker_freshness_and_masks_name(
    db: Database,
) -> None:
    from moneybin.extractors.simplefin.extractor import SimpleFINExtractor
    from tests.moneybin.test_extractors.test_simplefin_extractor import load, source

    fresh = source(batch=2, day=2, account_changes={"name": "Checking 123456789"})
    load(db, fresh, job=2)
    delayed = source()
    load(db, delayed)
    account = SimpleFINExtractor(db).source_accounts(delayed)[0]
    assert account.account_name == "Checking ****6789"
    assert account.account_name_is_user_set
    assert account.last_four is None


def test_native_account_scopes_allow_confirmed_alias_overlap() -> None:
    first, second = pair("native-one", "batch-one"), pair("native-two", "batch-two")
    assigned = assign_components(
        [first, second],
        seed_edges=[],
        native_scopes={
            "native-one": ("grant", "account-one"),
            "native-two": ("grant", "account-two"),
        },
    )
    assert len(assigned) == 2


def test_seed_only_native_identity_still_blocks_distinct_event() -> None:
    assigned = assign_components(
        [pair("native-two", "batch-two")],
        seed_edges=[
            (
                ("ofx", "statement", "canonical"),
                ("simplefin", "native-one", "canonical"),
            )
        ],
        native_scopes={
            "native-one": ("grant", "account"),
            "native-two": ("grant", "account"),
        },
    )
    assert assigned == []


def test_unknown_seed_identity_cannot_extend_component() -> None:
    assigned = assign_components(
        [pair("native-two", "batch-two")],
        seed_edges=[
            (
                ("ofx", "statement", "canonical"),
                ("simplefin", "unknown-native", "canonical"),
            )
        ],
        native_scopes={"native-two": ("grant", "account")},
    )
    assert assigned == []


def test_raw_lookup_retains_held_seed_identity(db: Database) -> None:
    from moneybin.matching.native_identity import native_event_scopes
    from tests.moneybin.test_extractors.test_simplefin_extractor import load, source

    load(db, source())
    load(db, source(batch=2, day=2, transaction_changes={"pending": True}), job=2)
    row = db.execute(
        "SELECT DISTINCT transaction_reference, source_origin, account_id FROM raw.simplefin_transactions"
    ).fetchone()
    assert row is not None
    assert native_event_scopes(db, [row[0], "missing"]) == {row[0]: (row[1], row[2])}


def test_confirmed_native_account_aliases_can_match_inside_one_delivery(
    db: Database,
) -> None:
    from moneybin.matching.scoring import get_candidates_within_source
    from tests.moneybin.test_extractors.test_simplefin_extractor import load, source

    data = source()
    account = cast(list[dict[str, Any]], data.batches[0].payload["accounts"])[0]
    data.batches[0].payload["accounts"] = [account, {**account, "id": "alias-account"}]
    load(db, data)
    db.execute("""CREATE TABLE native_aliases AS
        SELECT transaction_reference AS source_transaction_id,
            'canonical' AS account_id, CAST(posted_at AS DATE) AS transaction_date,
            amount, description, source_type, source_origin, source_file,
            currency AS currency_code FROM raw.simplefin_transactions""")
    candidates = get_candidates_within_source(db, table="main.native_aliases")
    assert len(candidates) == 1


def test_weak_v1_institution_and_native_id_collision_need_account_review(
    db: Database,
) -> None:
    from uuid import UUID

    from moneybin.extractors.simplefin.extractor import SimpleFINExtractor
    from moneybin.services.account_resolver import AccountResolver
    from tests.moneybin.db_helpers import create_core_tables
    from tests.moneybin.test_extractors.test_simplefin_extractor import load, source

    create_core_tables(db)
    db.execute("""INSERT INTO core.dim_accounts
        (account_id, display_name, display_name_is_user_set, institution_name, institution_slug)
        VALUES ('existing', 'Synthetic', TRUE, 'Synthetic', 'synthetic')""")
    resolved_ids: list[str] = []
    for number in (1, 2):
        data = source(batch=number)
        data.connection_id = UUID(int=100 + number)
        data.protocol_version = "1.0.7"
        account = cast(list[dict[str, Any]], data.batches[0].payload["accounts"])[0]
        account.pop("conn_id")
        account["org"] = {"sfin-url": "https://synthetic.invalid", "name": "Synthetic"}
        data.batches[0].payload.pop("connections")
        load(db, data, job=number)
        inputs = SimpleFINExtractor(db).source_accounts(data)
        assert len(inputs) == 1
        assert inputs[0].persistent_token is None
        resolved = AccountResolver(db, actor="system").resolve(inputs[0])
        assert resolved.outcome == "pending_review"
        assert resolved.account_id != "existing"
        assert resolved.pending_decision_ids
        resolved_ids.append(resolved.account_id)
    assert len(set(resolved_ids)) == 2
    assert db.execute(
        "SELECT count(DISTINCT transaction_reference) FROM raw.simplefin_transactions"
    ).fetchone() == (2,)
    assert db.execute(
        "SELECT DISTINCT identity_strength FROM raw.simplefin_accounts"
    ).fetchall() == [("weak",)]


@pytest.mark.parametrize("connections", [None, 7])
def test_account_builder_retains_identity_with_malformed_optional_connections(
    db: Database, connections: object
) -> None:
    from moneybin.extractors.simplefin.extractor import SimpleFINExtractor
    from moneybin.services.account_resolver import AccountResolver
    from tests.moneybin.test_extractors.test_simplefin_extractor import load, source

    data = source()
    data.batches[0].payload["connections"] = connections
    load(db, data)
    accounts = SimpleFINExtractor(db).source_accounts(data)
    assert len(accounts) == 1
    assert accounts[0].institution is None
    assert accounts[0].source_account_key == "acct"
    result = AccountResolver(db, actor="system").resolve(accounts[0])
    assert result.account_id
    assert result.is_new
