"""Receipts drive exact Golden revisions across replay and process boundaries."""

import json
import subprocess  # noqa: S404  # fresh-interpreter persistence proof
import sys
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from moneybin.connectors.sync_models import SyncInvestmentTransaction
from moneybin.database import Database
from moneybin.extractors.plaid.extractor import PlaidExtractor
from moneybin.investments.event_projection import read_rows
from moneybin.repositories.investment_event_members_repo import (
    InvestmentEventMembersRepo,
)
from moneybin.services.investment_membership_service import InvestmentMembershipService
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_plaid_event,
)
from tests.moneybin.test_investments.test_investment_golden_projection import (
    install_golden_models,
    ledger,
)
from tests.moneybin.test_investments.test_investment_match_acceptance import (
    registered_proposal,
)


def _buy_fields(amount: str, price: str, description: str) -> dict[str, Any]:
    """Hand-authored financial expectations exclude opaque IDs and timestamps."""
    return {
        "account_id": "account",
        "security_id": "security",
        "type": "buy",
        "subtype": None,
        "provider_type": "buy",
        "provider_subtype": "buy",
        "source_type": "plaid",
        "source_origin": "origin",
        "description": description,
        "currency_code": "USD",
        "quantity": Decimal("1"),
        "price": Decimal(price),
        "amount": Decimal(amount),
        "fees": Decimal("0"),
        "trade_date": date(2026, 1, 10),
        "settlement_date": date(2026, 1, 10),
        "original_acquisition_date": None,
    }


def _assert_buy(row: dict[str, Any], expected: dict[str, Any]) -> None:
    actual = {}
    for key, value in expected.items():
        observed = row[key]
        if isinstance(value, Decimal):
            observed = Decimal(observed)
        elif isinstance(value, date) and isinstance(observed, str):
            observed = date.fromisoformat(observed)
        actual[key] = observed
    assert actual == expected


def test_equal_time_a_b_a_receipts_reconcile_exact_golden_and_ignore_old_job_replay(
    comparison_db: Database,
) -> None:
    # Dropping receipt order or projecting the latest Raw revision would publish
    # B after restored A, or advance the ledger before reconciliation.
    db = comparison_db
    extractor = PlaidExtractor(db)
    membership = InvestmentMembershipService(db)
    members = InvestmentEventMembersRepo(db)
    at = datetime(2026, 2, 1, tzinfo=UTC)
    a = SyncInvestmentTransaction(
        investment_transaction_id="native",
        account_id="native_account",
        provider_item_id="origin",
        security_id="native_security",
        date=date(2026, 1, 10),
        name="Original observation",
        quantity=Decimal("1"),
        amount=Decimal("100"),
        price=Decimal("100"),
        fees=Decimal("0"),
        type="buy",
        subtype="buy",
    )
    b = a.model_copy(
        update={
            "transaction_name": "Corrected observation",
            "amount": Decimal("150"),
            "price": Decimal("150"),
        }
    )
    install_comparison_models(db)
    install_golden_models(db)
    versions: list[str] = []
    member_ids: list[str] = []
    ids: tuple[str, str] | None = None
    previous: list[dict[str, Any]] = []
    expected_a = _buy_fields("-100", "100", "Original observation")
    expected_b = _buy_fields("-150", "150", "Corrected observation")

    for index, (observation, job, expected) in enumerate([
        (a, "sync_z_first", expected_a),
        (b, "sync_a_second", expected_b),
        (a, "sync_m_third", expected_a),
    ]):
        extractor._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]  # exercise real receipt ingestion with tied timestamps
            [observation], job, at, at
        )
        staged = read_rows(db, "SELECT * FROM prep.stg_plaid__investment_transactions")
        assert len(staged) == 1
        assert staged[0]["source_file"] == job
        assert staged[0]["description"] == expected["description"]
        assert staged[0]["amount"] == expected["amount"]
        # Ingestion changes evidence, while published membership remains bound.
        assert ledger(db) == previous
        outcome = membership.reconcile(actor="test")
        assert (outcome.registered, outcome.advanced, outcome.retired) == (
            (1, 0, 0) if index == 0 else (0, 1, 0)
        )
        published = ledger(db)
        assert len(published) == 1
        _assert_buy(published[0], expected)
        current_ids = (
            published[0]["event_group_id"],
            published[0]["investment_transaction_id"],
        )
        if ids is None:
            ids = current_ids
            assert all(len(value) == 12 for value in ids)
            assert "native" not in ids
        assert current_ids == ids
        active = members.active_members()
        assert len(active) == 1
        member = active[0]
        assert (
            member["source_type"],
            member["source_origin"],
            member["native_reference"],
            member["semantic_leg_role"],
            member["bound_account_id"],
            member["bound_security_id"],
            member["bound_effective_currency"],
            member["decision_id"],
        ) == (
            "plaid",
            "origin",
            "native",
            "acquisition",
            "account",
            "security",
            "USD",
            None,
        )
        version = member["observation_version"]
        assert version == staged[0]["observation_version"]
        projected = membership.project()
        assert len(projected) == 1
        _assert_buy(projected[0], expected)
        assert projected[0]["observation_version"] == version
        assert projected[0]["native_reference"] == "native"
        assert projected[0]["source_file"] == job
        exact_revision = read_rows(
            db,
            """SELECT amount, price, transaction_name
               FROM raw.plaid_investment_transactions
               WHERE investment_transaction_id = ? AND source_origin = ?
                 AND observation_version = ?""",
            ["native", "origin", version],
        )
        assert exact_revision == [
            {
                "amount": observation.amount,
                "price": observation.price,
                "transaction_name": expected["description"],
            }
        ]
        versions.append(version)
        member_ids.append(member["membership_id"])
        previous = published

    assert versions[0] == versions[2] != versions[1]
    assert len(set(member_ids)) == 3
    history = members.history()
    by_id = {row["membership_id"]: row for row in history}
    assert set(by_id) == set(member_ids)
    assert [
        by_id[member_id]["observation_version"] for member_id in member_ids
    ] == versions
    assert [by_id[member_id]["retired_at"] is None for member_id in member_ids] == [
        False,
        False,
        True,
    ]
    assert len({row["source_event_key"] for row in history}) == 1
    assert {
        (row["event_group_id"], row["investment_transaction_id"]) for row in history
    } == {ids}
    raw = db.execute("SELECT * FROM raw.plaid_investment_transactions").fetchall()
    assert len(raw) == 2
    receipts = read_rows(
        db,
        """SELECT source_file, observation_version, extracted_at, ingestion_sequence
           FROM raw.plaid_investment_transaction_receipts
           ORDER BY ingestion_sequence""",
    )
    assert [row["source_file"] for row in receipts] == [
        "sync_z_first",
        "sync_a_second",
        "sync_m_third",
    ]
    assert [row["observation_version"] for row in receipts] == versions
    assert all(row["extracted_at"] == at for row in receipts)
    sequences = [row["ingestion_sequence"] for row in receipts]
    assert sequences[0] < sequences[1] < sequences[2]
    extractor._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]  # replay the old B delivery after restored A
        [b], "sync_a_second", at, at
    )
    replay = membership.reconcile(actor="test")
    assert (replay.registered, replay.advanced, replay.retired) == (0, 0, 0)
    assert ledger(db) == previous
    assert members.history() == history
    assert (
        db.execute("SELECT * FROM raw.plaid_investment_transactions").fetchall() == raw
    )
    assert (
        read_rows(
            db,
            """SELECT source_file, observation_version, extracted_at, ingestion_sequence
           FROM raw.plaid_investment_transaction_receipts
           ORDER BY ingestion_sequence""",
        )
        == receipts
    )
    assert membership.project()[0]["source_file"] == "sync_m_third"


_PROCESS = """
import json, sys
from pathlib import Path
from unittest.mock import MagicMock
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.investment_event_members_repo import InvestmentEventMembersRepo
from moneybin.services.investment_matching_service import InvestmentMatchingService
from moneybin.services.mutation_context import operation
from tests.moneybin.test_investments.test_investment_golden_projection import ledger
store = MagicMock()
store.get_key.return_value = 'test-encryption-key-for-unit-tests'
with Database(Path(sys.argv[1]), secret_store=store, read_only=sys.argv[2] == 'read', no_auto_upgrade=True) as db:
    matching = InvestmentMatchingService(db)
    error = None
    if sys.argv[2] != 'read':
        try:
            with operation():
                matching.decide(sys.argv[3], decision='accept', actor='test')
        except UserError as exc:
            error = {'message': str(exc), 'code': exc.code}
    print(json.dumps({
        'error': error,
        'decisions': matching.pending() + matching.history(),
        'members': InvestmentEventMembersRepo(db).history(),
        'ledger': ledger(db),
        'audit_count': db.execute('SELECT COUNT(*) FROM app.audit_log').fetchone()[0],
    }, default=str))
"""


def _reopen_process(path: Path, mode: str, proposal_id: str) -> dict[str, Any]:
    result = subprocess.run(  # noqa: S603  # fixed interpreter and code, test-owned path
        [sys.executable, "-c", _PROCESS, str(path), mode, proposal_id],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _json_value(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


@pytest.mark.integration
def test_fresh_process_acceptance_reopens_exact_golden_and_all_contributors(
    comparison_db: Database, tmp_path: Path
) -> None:
    # An in-memory proposal or membership cache cannot satisfy a second reader.
    db = comparison_db
    matching, proposal_id, oldest = registered_proposal(db)
    assert matching.pending()[0]["status"] == "pending"
    install_golden_models(db)
    prior_members = InvestmentEventMembersRepo(db).history()
    evidence = {
        (
            row["source_type"],
            row["source_origin"],
            row["native_reference"],
            row["observation_version"],
        )
        for row in prior_members
    }
    assert len(evidence) == 2
    assert len(ledger(db)) == 2
    db.close()
    accepted = _reopen_process(tmp_path / "test.duckdb", "accept", proposal_id)
    assert accepted["error"] is None
    assert [(row["proposal_id"], row["status"]) for row in accepted["decisions"]] == [
        (proposal_id, "accepted")
    ]
    assert len(accepted["ledger"]) == 1
    row = accepted["ledger"][0]
    _assert_buy(row, _buy_fields("-100", "100", "Aggregator context"))
    assert row["investment_transaction_id"] == oldest["investment_transaction_id"]
    assert row["event_group_id"] == oldest["event_group_id"]
    active = [member for member in accepted["members"] if member["retired_at"] is None]
    assert len(active) == 2
    assert {
        (
            member["source_type"],
            member["source_origin"],
            member["native_reference"],
            member["observation_version"],
        )
        for member in active
    } == evidence
    assert {member["decision_id"] for member in active} == {proposal_id}
    assert {member["investment_transaction_id"] for member in active} == {
        oldest["investment_transaction_id"]
    }
    assert len(accepted["members"]) == 4
    assert _reopen_process(tmp_path / "test.duckdb", "read", proposal_id) == accepted


@pytest.mark.integration
def test_fresh_process_refuses_old_proposal_after_equally_plausible_arrival(
    comparison_db: Database, tmp_path: Path
) -> None:
    # Persisted planning evidence must not bypass a fresh live-graph check.
    from moneybin import error_codes

    db = comparison_db
    matching, proposal_id, _ = registered_proposal(db)
    install_golden_models(db)
    seed_plaid_event(db, "alternative")
    audit_count = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    assert audit_count is not None
    before = _json_value({
        "decisions": matching.pending() + matching.history(),
        "members": InvestmentEventMembersRepo(db).history(),
        "ledger": ledger(db),
        "audit_count": audit_count[0],
    })
    assert before["decisions"][0]["status"] == "pending"
    assert len(before["ledger"]) == 2
    db.close()
    refused = _reopen_process(tmp_path / "test.duckdb", "accept", proposal_id)
    assert refused["error"]["code"] == error_codes.MUTATION_CONSTRAINT_VIOLATION
    assert "stale" in refused["error"]["message"].lower()
    assert {key: refused[key] for key in before} == before
    persisted = _reopen_process(tmp_path / "test.duckdb", "read", proposal_id)
    assert persisted["error"] is None
    assert {key: persisted[key] for key in before} == before
