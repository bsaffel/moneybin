"""Manual staging follows current equivalence routes without losing observations."""

from pathlib import Path

import pytest
from sqlglot import exp
from sqlmesh.core.dialect import parse

from moneybin.database import Database
from moneybin.repositories.account_link_decisions_repo import AccountLinkDecisionsRepo
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.services.mutation_context import operation
from moneybin.services.undo_service import UndoService


def create_identity_views(db: Database) -> None:
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    for name in (
        "int_manual__investment_identity",
        "stg_manual__investment_transactions",
    ):
        path = Path("src/moneybin/sqlmesh/models/prep") / f"{name}.sql"
        if not path.exists():
            continue
        query = next(
            node
            for node in parse(path.read_text(), default_dialect="duckdb")
            if isinstance(node, exp.Query)
        )
        db.execute(
            f"CREATE OR REPLACE VIEW prep.{name} AS {query.sql(dialect='duckdb')}"
        )  # noqa: S608  # fixed model names and parsed repository SQL


def add_manual_observation(db: Database, account: str = "a", native: str = "m") -> None:
    db.execute(
        """INSERT INTO raw.manual_investment_transactions
        (source_transaction_id, import_id, account_id, security_id, type,
         trade_date, quantity, amount, created_by, investment_transaction_id)
        VALUES (?, 'i', ?, 's', 'buy', DATE '2026-01-01', 1, -10, 'cli', ?)""",
        [native, account, native],
    )


def add_account_edge(
    db: Database,
    source: str,
    target: str,
    *,
    status: str = "accepted",
    suffix: str = "",
) -> None:
    AccountLinkDecisionsRepo(db).insert(
        decision_id=f"{source}_{target}_{suffix}",
        provisional_account_id=source,
        candidate_account_id=target,
        confidence_score=None,
        match_signals={},
        decided_by="user",
        actor="cli",
        status=status,
    )


def add_account_terminal(db: Database, account: str) -> None:
    AccountLinksRepo(db).insert(
        link_id=f"link_{account}",
        account_id=account,
        ref_kind="source_native",
        ref_value=account,
        source_type="manual",
        source_origin="user",
        decided_by="user",
        actor="cli",
    )


def _accounts(db: Database) -> list[tuple[str, str | None]]:
    return db.execute(
        "SELECT source_transaction_id, account_id FROM prep.stg_manual__investment_transactions ORDER BY source_transaction_id"
    ).fetchall()


def test_account_chain_and_undo_use_current_decisions(db: Database) -> None:
    add_manual_observation(db)
    add_account_terminal(db, "b")
    add_account_terminal(db, "c")
    with operation() as first:
        add_account_edge(db, "a", "b")
    with operation() as second:
        add_account_edge(db, "b", "c")
    create_identity_views(db)
    assert _accounts(db) == [("m", "c")]
    add_manual_observation(db, "c", "new")
    undo = UndoService(db).undo(second, actor="cli")
    assert _accounts(db) == [("m", "b"), ("new", "c")]
    UndoService(db).undo(undo.undo_operation_id, actor="cli")
    assert _accounts(db) == [("m", "c"), ("new", "c")]
    UndoService(db).undo(second, actor="cli")
    UndoService(db).undo(first, actor="cli")
    assert _accounts(db) == [("m", "a"), ("new", "c")]
    assert db.execute(
        "SELECT account_id FROM raw.manual_investment_transactions WHERE source_transaction_id = 'm'"
    ).fetchone() == ("a",)


@pytest.mark.parametrize(
    "edges",
    [
        [("a", "b"), ("a", "c")],
        [("a", "a")],
        [("a", "b"), ("b", "a")],
        [("a", "missing")],
        [("a", "b"), ("b", "c"), ("b", "d")],
    ],
)
def test_invalid_account_routes_remain_one_unresolved_observation(
    db: Database, edges: list[tuple[str, str]]
) -> None:
    add_manual_observation(db)
    add_account_terminal(db, "b")
    add_account_terminal(db, "c")
    for source, target in edges:
        add_account_edge(db, source, target)
    create_identity_views(db)
    assert _accounts(db) == [("m", None)]


@pytest.mark.parametrize("status", ["pending", "rejected", "reversed"])
def test_inactive_account_decisions_leave_frozen_fallback(
    db: Database, status: str
) -> None:
    add_manual_observation(db)
    add_account_edge(db, "a", "b", status=status)
    create_identity_views(db)
    assert _accounts(db) == [("m", "a")]


def test_duplicate_equivalent_account_edges_do_not_multiply_rows(db: Database) -> None:
    add_manual_observation(db)
    add_account_terminal(db, "b")
    add_account_edge(db, "a", "b")
    add_account_edge(db, "a", "b", suffix="duplicate")
    create_identity_views(db)
    assert _accounts(db) == [("m", "b")]


def test_security_route_overrides_frozen_assignment_in_staging(db: Database) -> None:
    add_manual_observation(db)
    SecuritiesRepo(db).upsert(
        security_id="survivor", name="Test", security_type="equity", actor="cli"
    )
    SecurityLinksRepo(db).insert(
        security_id="survivor",
        ref_kind="manual_investment_transaction_id",
        ref_value="m",
        source_type="manual",
        decided_by="user",
        actor="cli",
    )
    create_identity_views(db)
    assert db.execute(
        "SELECT security_id FROM prep.stg_manual__investment_transactions"
    ).fetchone() == ("survivor",)
    assert db.execute(
        "SELECT security_id FROM raw.manual_investment_transactions"
    ).fetchone() == ("s",)
