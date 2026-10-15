"""Pending review eligibility is shared, scoped, and filtered before limiting."""

from __future__ import annotations

import json
from collections.abc import Mapping
from contextlib import nullcontext
from dataclasses import replace
from typing import Any, cast
from unittest.mock import patch

import duckdb
import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.interactive_review import load_items, run_review
from moneybin.cli.main import app
from moneybin.cli.utils import get_terminal_policy
from moneybin.database import Database
from moneybin.repositories.categorization_decisions_repo import (
    CategorizationDecisionsRepo,
)
from moneybin.repositories.transaction_categories_repo import TransactionCategoriesRepo
from moneybin.services.account_links_service import AccountLinksService
from moneybin.services.categorization import CategorizationService
from moneybin.services.matching_service import MatchingService
from moneybin.services.merchant_links_service import MerchantLinksService
from moneybin.services.review_service import ReviewService
from moneybin.services.security_links_service import SecurityLinksService
from tests.moneybin.db_helpers import (
    create_core_tables,
    derive_is_categorizable,
    simulate_pre_refresh_fct_transactions,
)


def _queue(db: Database) -> None:
    create_core_tables(db)
    db.execute("""INSERT INTO core.dim_accounts (account_id, display_name, archived)
                  VALUES ('synthetic', 'Synthetic account', FALSE)""")


def _transaction(
    db: Database,
    transaction_id: str,
    date: str = "2026-10-01",
    amount: str = "-1.00",
    currency: str = "USD",
) -> None:
    db.execute(
        """
        INSERT INTO core.fct_transactions
        (transaction_id, account_id, transaction_date, amount, currency_code, description, source_type, is_transfer)
        VALUES (?, 'synthetic', ?::DATE, ?::DECIMAL(18,2), ?, 'Synthetic purchase', 'manual', FALSE)
    """,
        [transaction_id, date, amount, currency],
    )


def _reject(db: Database, transaction_id: str, *, in_outer_txn: bool = False) -> str:
    repo = CategorizationDecisionsRepo(db)
    attempt = repo.ensure_pending(
        transaction_id, actor="test", in_outer_txn=in_outer_txn
    )
    repo.update_status(
        attempt["decision_id"],
        status="rejected",
        category_id=None,
        merchant_id=None,
        decided_by="user",
        actor="test",
        in_outer_txn=in_outer_txn,
    )
    return str(attempt["decision_id"])


def test_status_counts_pending_attempts_instead_of_rejected_inventory(
    db: Database,
) -> None:
    _queue(db)
    _transaction(db, "rejected")
    _transaction(db, "pending")
    _reject(db, "rejected")
    service = CategorizationService(db)
    status = ReviewService(
        MatchingService(db),
        service,
        AccountLinksService(db),
        MerchantLinksService(db),
        SecurityLinksService(db),
    ).status()
    assert service.count_uncategorized() == 2
    assert status.categorize_pending == 1
    assert status.total == 1


def test_pending_only_discovers_older_attempt_beyond_thousand_rejections(
    db: Database,
) -> None:
    _queue(db)
    db.execute("""
        INSERT INTO core.fct_transactions
        (transaction_id, account_id, transaction_date, amount, currency_code, description, source_type)
        SELECT 'rejected-' || n, 'synthetic', DATE '2026-10-15', -1.00,
               'USD', 'Synthetic purchase', 'manual' FROM range(1001) AS rows(n)
    """)
    _transaction(db, "older-pending", "2026-01-01")
    db.begin()
    try:
        for index in range(1001):
            _reject(db, f"rejected-{index}", in_outer_txn=True)
        db.commit()
    except BaseException:
        db.rollback()
        raise
    with patch(
        "moneybin.cli.commands.transactions.categorize.get_database",
        return_value=nullcontext(db),
    ):
        result = CliRunner().invoke(
            app,
            [
                "transactions",
                "categorize",
                "pending",
                "--pending-only",
                "--limit",
                "1",
                "--output",
                "json",
            ],
        )
    assert result.exit_code == 0, result.output
    rows = json.loads(result.stdout)["data"]["transactions"]
    assert [row["transaction_id"] for row in rows] == ["older-pending"]
    assert rows[0]["decision_id"]
    assert CategorizationDecisionsRepo(db).fetch_by_id(rows[0]["decision_id"]) is None


class _FetchedRows:
    """Delegate real DuckDB results and measure rows materialized by readers."""

    def __init__(self, result: duckdb.DuckDBPyConnection, sizes: list[int]) -> None:
        self.result = result
        self.sizes = sizes

    @property
    def description(self) -> Any:
        return self.result.description

    def fetchall(self) -> list[tuple[Any, ...]]:
        rows = self.result.fetchall()
        if self.description and self.description[0][0] == "transaction_id":
            self.sizes.append(len(rows))
        return rows

    def fetchone(self) -> Any:
        return self.result.fetchone()


def test_limit_one_fetches_bounded_rows_and_revalidates_exact_target(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    _queue(db)
    db.execute("""
        INSERT INTO core.fct_transactions
        (transaction_id, account_id, transaction_date, amount, currency_code, description, source_type)
        SELECT 'pending-' || lpad(n::VARCHAR, 4, '0'), 'synthetic', DATE '2026-10-01',
               -1.00, 'USD', 'Synthetic purchase', 'manual' FROM range(2000) AS rows(n)
    """)
    from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo

    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute("""
        CREATE TABLE prep.int_transactions__matched (
            transaction_id VARCHAR, source_type VARCHAR,
            source_transaction_id VARCHAR, account_id VARCHAR
        )
    """)
    for index in range(3):
        transaction_id = f"pending-{index:04d}"
        source_id = f"source-{index}"
        db.execute(
            "INSERT INTO prep.int_transactions__matched VALUES (?, 'manual', ?, 'synthetic')",
            [transaction_id, source_id],
        )
        MatchDecisionsRepo(db).insert(
            match_id=f"transfer-{index}",
            source_transaction_id_a=source_id,
            source_type_a="manual",
            source_origin_a="user",
            source_transaction_id_b=f"paired-{index}",
            source_type_b="csv",
            source_origin_b="bank",
            account_id="synthetic",
            account_id_b="other",
            confidence_score=0.9,
            match_signals={},
            match_tier=None,
            match_type="transfer",
            match_status="pending",
            decided_by="matcher",
            actor="test",
        )
    sizes: list[int] = []
    execute = db.execute

    def measured_execute(
        query: str, params: list[Any] | Mapping[str, Any] | None = None
    ) -> duckdb.DuckDBPyConnection:
        result = execute(query, params)
        return cast(duckdb.DuckDBPyConnection, _FetchedRows(result, sizes))

    monkeypatch.setattr(db, "execute", measured_execute)
    with patch(
        "moneybin.cli.commands.interactive_review.get_database",
        return_value=nullcontext(db),
    ):
        selected = load_items("categorize", 1)
    assert len(selected) == 1
    assert sizes and max(sizes) <= 1, sizes
    target = dict(selected[0].evidence)["transaction id"]
    sizes.clear()

    def new_import_then_reject(*_args: object, **_kwargs: object) -> str:
        _transaction(db, "newer-pending", "2026-10-15")
        return "reject"

    with (
        patch(
            "moneybin.cli.commands.interactive_review.get_database",
            return_value=nullcontext(db),
        ),
        patch(
            "moneybin.cli.commands.interactive_review.choose_required",
            side_effect=new_import_then_reject,
        ),
    ):
        run_review("categorize", 1, replace(get_terminal_policy(), interactive=True))
    stored = CategorizationDecisionsRepo(db).fetch_by_transaction_id(target)
    assert stored is not None and stored["status"] == "rejected"
    assert sizes and max(sizes) <= 1, sizes
    assert (
        CategorizationDecisionsRepo(db).fetch_by_transaction_id("newer-pending") is None
    )


def test_pending_selection_matches_revision_and_undo_projection(db: Database) -> None:
    _queue(db)
    repo = CategorizationDecisionsRepo(db)
    categories = TransactionCategoriesRepo(db)
    ids = [
        "fresh",
        "pending",
        "rejected",
        "reversed",
        "changed-revision",
        "stale-pending",
        "categorized",
    ]
    for transaction_id in ids:
        _transaction(db, transaction_id)
    repo.ensure_pending("pending", actor="test")
    _reject(db, "rejected")
    reversed_id = _reject(db, "reversed")
    from moneybin.services.audit_service import AuditService

    event = next(
        event
        for event in AuditService(db).list_events(
            target_table="categorization_decisions", target_id=reversed_id
        )
        if event.action == "categorization_decision.update_status"
    )
    repo.undo_event(event, actor="test")
    _reject(db, "changed-revision")
    repo.ensure_pending("stale-pending", actor="test")
    for transaction_id in ("changed-revision", "stale-pending", "categorized"):
        categories.set(
            transaction_id,
            category="Food",
            subcategory=None,
            category_id="food",
            actor="test",
        )
        if transaction_id != "categorized":
            categories.clear(transaction_id, actor="test")
    expected = repo.project_pending_attempts(ids)
    actual = CategorizationService(db).list_pending_review_transactions(limit=20)
    assert actual is not None
    assert {row["transaction_id"]: row["decision_id"] for row in actual} == {
        key: row["decision_id"] for key, row in expected.items()
    }
    assert set(expected) == {
        "fresh",
        "pending",
        "reversed",
        "changed-revision",
        "stale-pending",
    }
    assert expected["fresh"]["attempt_number"] == 1
    assert expected["pending"]["attempt_number"] == 1
    for transaction_id in ("reversed", "changed-revision", "stale-pending"):
        assert expected[transaction_id]["attempt_number"] == 2


def test_pending_only_keeps_currency_interleaved_impact_order(db: Database) -> None:
    _queue(db)
    _transaction(db, "eur-high", amount="-200.00", currency="EUR")
    _transaction(db, "eur-low", amount="-100.00", currency="EUR")
    _transaction(db, "usd-high", amount="-20.00", currency="USD")
    _transaction(db, "usd-low", amount="-10.00", currency="USD")
    _reject(db, "eur-high")
    rows = CategorizationService(db).list_pending_review_transactions(
        limit=3, sort="impact"
    )
    assert rows is not None
    assert [row["transaction_id"] for row in rows] == ["eur-low", "usd-high", "usd-low"]


def test_pending_read_preserves_no_import_and_missing_schema_distinction(
    db: Database,
) -> None:
    from moneybin import error_codes
    from moneybin.errors import UserError

    service = CategorizationService(db)
    assert service.list_pending_review_transactions(limit=1) is None
    assert service.count_pending_review() == 0
    create_core_tables(db)
    _transaction(db, "schema-drift")
    simulate_pre_refresh_fct_transactions(db)
    with pytest.raises(UserError) as missing_list:
        service.list_pending_review_transactions(limit=1)
    with pytest.raises(UserError) as missing_count:
        service.count_pending_review()
    assert missing_list.value.code == error_codes.INFRA_SCHEMA_DRIFT
    assert missing_count.value.code == error_codes.INFRA_SCHEMA_DRIFT


async def test_mcp_pending_summary_counts_without_materializing_rows(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    from moneybin.mcp.tools.reviews import reviews_coarse

    _queue(db)
    _transaction(db, "closed")
    _transaction(db, "open")
    _reject(db, "closed")
    sizes: list[int] = []
    execute = db.execute

    def measured_execute(
        query: str, params: list[Any] | Mapping[str, Any] | None = None
    ) -> duckdb.DuckDBPyConnection:
        return cast(
            duckdb.DuckDBPyConnection, _FetchedRows(execute(query, params), sizes)
        )

    monkeypatch.setattr(db, "execute", measured_execute)
    with patch("moneybin.mcp.tools.reviews.get_database", return_value=nullcontext(db)):
        result = await reviews_coarse(kind="summary", status="pending")
    data = result.data
    assert data is not None
    from moneybin.privacy.payloads.reviews import ReviewsSummaryView

    assert isinstance(data, ReviewsSummaryView)
    counts = {count.kind: count for count in data.counts if count.status == "pending"}
    assert counts["categorization"].count == 1
    assert sizes == []
    assert CategorizationService(db).count_uncategorized() == 2


def test_pending_count_is_zero_for_empty_fact_before_column_is_built(
    db: Database,
) -> None:
    from moneybin.errors import UserError

    create_core_tables(db)
    simulate_pre_refresh_fct_transactions(db)
    service = CategorizationService(db)
    assert service.count_pending_review() == 0
    # Listing still requires the canonical column, even on empty inventory.
    with pytest.raises(UserError):
        service.list_pending_review_transactions(limit=1)


def test_pending_attempts_use_canonical_categorizable_population(db: Database) -> None:
    _queue(db)
    _transaction(db, "open")
    _transaction(db, "rejected")
    _reject(db, "rejected")
    _transaction(db, "transfer")
    _transaction(db, "archived")
    _transaction(db, "unresolved")
    db.execute("""INSERT INTO core.dim_accounts (account_id, archived)
                  VALUES ('archived-account', TRUE)""")
    db.execute(
        "UPDATE core.fct_transactions SET is_transfer = TRUE WHERE transaction_id = 'transfer'"
    )
    db.execute(
        "UPDATE core.fct_transactions SET account_id = 'archived-account' WHERE transaction_id = 'archived'"
    )
    db.execute(
        "UPDATE core.fct_transactions SET account_id = 'missing-account' WHERE transaction_id = 'unresolved'"
    )
    derive_is_categorizable(db)
    service = CategorizationService(db)
    assert service.count_uncategorized() == 2
    assert service.count_pending_review() == 1
    rows = service.list_pending_review_transactions(limit=1)
    assert rows is not None and [row["transaction_id"] for row in rows] == ["open"]


@pytest.mark.parametrize("change", ["recreated", "inactive", "duplicate", "unchanged"])
def test_interactive_category_selection_preserves_exact_taxonomy_target(
    db: Database, change: str
) -> None:
    from moneybin.cli.commands.interactive_review import apply_decision
    from moneybin.repositories.user_categories_repo import UserCategoriesRepo
    from moneybin.services.review_decisions_service import ReviewDecisionsService

    _queue(db)
    _transaction(db, "category-target")
    service = CategorizationService(db)
    selected_id = service.create_category(
        "Synthetic target", subcategory=None, actor="test"
    )

    def choose(*_args: Any, **_kwargs: Any) -> str:
        if change == "recreated":
            service.delete_category(selected_id, actor="test")
            replacement = service.create_category(
                "Synthetic target", subcategory=None, actor="test"
            )
            assert replacement != selected_id
        elif change == "inactive":
            UserCategoriesRepo(db).update_active(
                selected_id, is_active=False, actor="test"
            )
        elif change == "duplicate":
            # Legacy/repo taxonomy can contain equal labels; the ID PK alone permits it.
            duplicate = UserCategoriesRepo(db).insert(
                category="Synthetic target", actor="test"
            )
            assert duplicate.target_id != selected_id
        return selected_id

    with (
        patch(
            "moneybin.cli.commands.interactive_review.get_database",
            return_value=nullcontext(db),
        ),
        patch(
            "moneybin.cli.commands.interactive_review.choose_required",
            side_effect=choose,
        ),
        patch.object(
            ReviewDecisionsService,
            "apply_ordinary",
            wraps=ReviewDecisionsService(db, actor="cli").apply_ordinary,
        ) as apply,
    ):
        item = load_items("categorize", 1)[0]
        result = apply_decision(item, "accept", get_terminal_policy())
    history = service.list_categorization_history()
    if change == "unchanged":
        assert result is True
        apply.assert_called_once()
        assert len(history) == 1 and history[0]["category_id"] == selected_id
        attempt = CategorizationDecisionsRepo(db).fetch_by_id(item.item_id)
        assert attempt is not None and attempt["status"] == "accepted"
    else:
        assert result is False
        apply.assert_not_called()
        assert history == []
        assert CategorizationDecisionsRepo(db).fetch_by_id(item.item_id) is None
