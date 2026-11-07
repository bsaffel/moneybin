"""Real match/rebuild/undo keeps deterministic source metadata and field evidence."""

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from sqlglot import exp
from sqlmesh.core.dialect import parse

from moneybin.connectors.sync_models import SyncInvestmentTransaction
from moneybin.database import Database
from moneybin.extractors.plaid.extractor import PlaidExtractor
from moneybin.investments.event_projection import read_rows
from moneybin.repositories.investment_event_members_repo import (
    InvestmentEventMembersRepo,
)
from moneybin.repositories.investment_match_field_resolutions_repo import (
    InvestmentMatchFieldResolutionsRepo,
)
from moneybin.services.investment_matching_service import InvestmentMatchingService
from moneybin.services.investment_membership_service import InvestmentMembershipService
from moneybin.services.mutation_context import operation
from moneybin.services.transform_service import TransformService
from moneybin.services.undo_service import UndoService
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_account_metadata,
    seed_manual_event,
)

pytestmark = pytest.mark.integration

_REVISION_FIELDS = (
    "source_type",
    "source_origin",
    "native_reference",
    "observation_version",
)
_REPRESENTATIVE_FIELDS = (
    "source_type",
    "source_origin",
    "provider_type",
    "provider_subtype",
    "description",
)


def _seed_observation(db: Database, source: str) -> None:
    if source == "manual":
        seed_manual_event(db, "manual", day=9)
        return
    # Both provider spellings normalize to buy; retained metadata is distinct.
    txn = SyncInvestmentTransaction(
        investment_transaction_id="z_native" if source == "origin_a" else "a_native",
        account_id="native_account",
        provider_item_id=source,
        security_id="native_security",
        date=date(2026, 1, 10),
        transaction_datetime=datetime(2026, 1, 10, tzinfo=UTC),
        name="Origin A description" if source == "origin_a" else "Origin B description",
        quantity=Decimal(1),
        amount=Decimal(100),
        price=Decimal(100),
        fees=Decimal(0),
        type="BUY" if source == "origin_a" else "buy",
        subtype="contribution" if source == "origin_a" else "buy",
    )
    at = datetime(2026, 2, 1, tzinfo=UTC)
    PlaidExtractor(db)._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]  # real synthetic revision loader
        [txn], f"synthetic_{source}", at, at
    )


def _rebuild(db: Database) -> None:
    built = TransformService(db).apply()
    assert built.applied, built.error


def _order_comparison_inputs(db: Database, reverse: bool) -> None:
    """Keep production comparison SQL, presenting its rows in opposite orders."""
    models = Path(__file__).resolve().parents[3] / "src/moneybin/sqlmesh/models/prep"
    direction = "DESC" if reverse else "ASC"
    for name, columns in (
        ("int_investment_events__headers", ("source_event_key",)),
        (
            "int_investment_events__legs",
            ("source_type", "source_origin", "native_reference"),
        ),
        (
            "int_investment_events__evidence",
            ("left_source_event_key", "right_source_event_key"),
        ),
    ):
        path = models / f"{name}.sql"
        query = next(
            node
            for node in parse(path.read_text(), default_dialect="duckdb")
            if isinstance(node, exp.Query)
        )
        ordering = ", ".join(f"{column} {direction}" for column in columns)
        db.execute(
            f"CREATE OR REPLACE VIEW prep.{name} AS "  # noqa: S608  # fixed model names and parsed production SQL
            f"SELECT * FROM ({query.sql(dialect='duckdb')}) AS inputs ORDER BY {ordering}"
        )  # fixed local model names/columns and the unchanged production query


def _revision(row: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(row[field] for field in _REVISION_FIELDS)


def _golden_id(row: dict[str, Any]) -> tuple[str, str]:
    return row["event_group_id"], row["investment_transaction_id"]


def _core_rows(db: Database) -> list[dict[str, Any]]:
    return read_rows(
        db,
        "SELECT * FROM core.fct_investment_transactions ORDER BY investment_transaction_id",
    )


def _manual_choices(proposal: dict[str, Any]) -> list[dict[str, str]]:
    return [
        {
            "conflict_id": conflict["conflict_id"],
            "choice_id": next(
                choice["choice_id"]
                for choice in conflict["choices"]
                if choice["source_type"] == "manual"
            ),
        }
        for conflict in proposal["field_choices"]
    ]


def _resolution_evidence(db: Database) -> set[tuple[Any, ...]]:
    return {
        (row["investment_transaction_id"], row["field_name"], *_revision(row))
        for row in InvestmentMatchFieldResolutionsRepo(db).active_resolutions()
    }


def _assert_representative(row: dict[str, Any], *, day: int) -> None:
    # Origin sorts before native reference: z_native must beat a_native.
    assert tuple(row[field] for field in _REPRESENTATIVE_FIELDS) == (
        "plaid",
        "origin_a",
        "BUY",
        "contribution",
        "Origin A description",
    )
    assert row["trade_date"] == date(2026, 1, day)
    assert row["amount"] == Decimal(-100)
    assert row["quantity"] == Decimal(1)


@pytest.mark.parametrize("reverse", [False, True], ids=["forward", "reverse"])
@pytest.mark.parametrize(
    "manual_first", [False, True], ids=["plaid_pair", "curated_pair"]
)
def test_representative_and_exact_field_provenance_survive_third_source_and_undo(
    comparison_db: Database, reverse: bool, manual_first: bool
) -> None:
    db = comparison_db
    for origin in ("origin_a", "origin_b"):
        seed_account_metadata(
            db, "account", native="native_account", origin=origin, currency="USD"
        )
    initial_sources = ["origin_a", "manual" if manual_first else "origin_b"]
    if reverse:
        initial_sources.reverse()
    _seed_observation(db, initial_sources[0])
    install_comparison_models(db)
    membership = InvestmentMembershipService(db)
    membership.reconcile(actor="test")
    first = membership.project()[0]
    oldest_id = _golden_id(first)
    _seed_observation(db, initial_sources[1])
    _rebuild(db)
    expected_revisions = {
        _revision(row)
        for row in read_rows(
            db, "SELECT * FROM prep.int_investment_events__observations"
        )
    }
    assert len(expected_revisions) == 2
    assert {revision[:3] for revision in expected_revisions} == {
        ("plaid", "origin_a", "z_native"),
        ("manual", "user", "manual")
        if manual_first
        else ("plaid", "origin_b", "a_native"),
    }
    members = InvestmentEventMembersRepo(db)
    assert {_revision(row) for row in members.active_members()} == expected_revisions
    _order_comparison_inputs(db, reverse)
    matching = InvestmentMatchingService(db)
    matching.run()
    pending = matching.pending()
    assert len(pending) == 1
    initial = pending[0]
    assert {c["field"] for c in initial["field_choices"]} == (
        {"trade_date"} if manual_first else set()
    )
    with operation():
        matching.decide(
            initial["proposal_id"],
            decision="accept",
            actor="test",
            field_choices=_manual_choices(initial),
        )
    _rebuild(db)
    rows = _core_rows(db)
    assert len(rows) == 1
    assert _golden_id(rows[0]) == oldest_id
    _assert_representative(rows[0], day=9 if manual_first else 10)
    assert {_revision(row) for row in members.active_members()} == expected_revisions
    assert {row["decision_id"] for row in members.active_members()} == {
        initial["proposal_id"]
    }
    prior_resolutions = _resolution_evidence(db)
    manual_revision = next(
        (revision for revision in expected_revisions if revision[0] == "manual"), None
    )
    assert prior_resolutions == (
        {(oldest_id[1], "trade_date", *manual_revision)}
        if manual_revision is not None
        else set()
    )

    _seed_observation(db, "origin_b" if manual_first else "manual")
    _rebuild(db)
    all_revisions = {
        _revision(row)
        for row in read_rows(
            db, "SELECT * FROM prep.int_investment_events__observations"
        )
    }
    assert len(all_revisions) == 3 and expected_revisions < all_revisions
    assert {revision[:3] for revision in all_revisions} == {
        ("plaid", "origin_a", "z_native"),
        ("plaid", "origin_b", "a_native"),
        ("manual", "user", "manual"),
    }
    third_revision = next(iter(all_revisions - expected_revisions))
    third = next(
        row for row in members.active_members() if _revision(row) == third_revision
    )
    third_id = _golden_id(third)
    assert third_id != oldest_id
    before_replacement = _core_rows(db)
    assert len(before_replacement) == 2
    original = next(row for row in before_replacement if _golden_id(row) == oldest_id)
    _assert_representative(original, day=9 if manual_first else 10)
    assert _resolution_evidence(db) == prior_resolutions
    _order_comparison_inputs(db, reverse)
    matching.run()
    replacements = matching.pending()
    assert len(replacements) == 1
    replacement = replacements[0]
    assert replacement["supersedes_decision_ids"] == [initial["proposal_id"]]
    assert {c["field"] for c in replacement["field_choices"]} == {"trade_date"}
    with operation() as replacement_op:
        matching.decide(
            replacement["proposal_id"],
            decision="accept",
            actor="test",
            field_choices=_manual_choices(replacement),
        )
    _rebuild(db)
    rows = _core_rows(db)
    assert len(rows) == 1 and _golden_id(rows[0]) == oldest_id
    _assert_representative(rows[0], day=9)
    active = members.active_members()
    assert {_revision(row) for row in active} == all_revisions
    assert {_golden_id(row) for row in active} == {oldest_id}
    assert {row["decision_id"] for row in active} == {replacement["proposal_id"]}
    manual_revision = next(
        revision for revision in all_revisions if revision[0] == "manual"
    )
    assert _resolution_evidence(db) == {(oldest_id[1], "trade_date", *manual_revision)}

    UndoService(db).undo(replacement_op, actor="test")
    _rebuild(db)
    restored = _core_rows(db)
    assert len(restored) == 2
    assert {_golden_id(row) for row in restored} == {oldest_id, third_id}
    original = next(row for row in restored if _golden_id(row) == oldest_id)
    _assert_representative(original, day=9 if manual_first else 10)
    assert _resolution_evidence(db) == prior_resolutions
    active = members.active_members()
    assert {_revision(row) for row in active} == all_revisions
    expected_topology = {
        (revision, oldest_id, initial["proposal_id"]) for revision in expected_revisions
    }
    expected_topology.add((third_revision, third_id, None))
    assert {
        (_revision(row), _golden_id(row), row["decision_id"]) for row in active
    } == expected_topology
    stable_history = members.history()
    _rebuild(db)
    assert members.history() == stable_history
    assert _core_rows(db) == restored
    assert _resolution_evidence(db) == prior_resolutions
