"""Tests for source-priority precedence enforcement on transaction_categories writes.

Per categorization-matching-mechanics.md §Source precedence, the priority order is:
user(1) > rule(2) > auto_rule(3) > migration(4) > ml(5) > plaid(6) > seed(7) > ai(8).
A higher-priority source can never be overwritten by a lower-priority source.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from prometheus_client import REGISTRY

from moneybin.database import Database
from moneybin.services.auto_rule_service import AutoRuleService
from moneybin.services.categorization_service import (
    _SOURCE_PRIORITY as _PRIORITY,  # pyright: ignore[reportPrivateUsage]  # test reads the canonical ladder
)
from moneybin.services.categorization_service import (
    BulkCategorizationItem,
    CategorizationService,
)
from tests.moneybin.db_helpers import create_core_tables


@pytest.fixture()
def real_db(tmp_path: Path) -> Database:
    """Real DB with core + app schema."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-key"
    db = Database(
        tmp_path / "test.duckdb", secret_store=mock_store, no_auto_upgrade=True
    )
    create_core_tables(db)
    return db


@pytest.fixture()
def fresh_txn(real_db: Database) -> str:
    """Insert one transaction in fct_transactions; return its id."""
    txn_id = "test_txn_001"
    real_db.execute(
        """
        INSERT INTO core.fct_transactions
        (transaction_id, account_id, transaction_date, amount, description,
         memo, source_type, is_transfer)
        VALUES (?, 'acct_test', '2026-05-10', -10.00, 'STARBUCKS #1234',
                NULL, 'ofx', false)
        """,  # noqa: S608  # test input, not executing user SQL
        [txn_id],
    )
    return txn_id


def _write(svc: CategorizationService, txn_id: str, source: str) -> bool:
    outcome = svc.write_categorization(
        transaction_id=txn_id,
        category="Food & Dining",
        subcategory="Coffee Shops",
        categorized_by=source,
    )
    return outcome.written


def _current_source(real_db: Database, txn_id: str) -> str | None:
    row = real_db.execute(
        "SELECT categorized_by FROM app.transaction_categories WHERE transaction_id = ?",
        [txn_id],
    ).fetchone()
    return row[0] if row else None


def test_user_overwrites_everything(real_db: Database, fresh_txn: str) -> None:
    svc = CategorizationService(real_db)
    for predecessor in ["ai", "seed", "plaid", "ml", "migration", "auto_rule", "rule"]:
        svc.write_categorization(
            transaction_id=fresh_txn,
            category="Initial",
            subcategory=None,
            categorized_by=predecessor,
        )
        assert _write(svc, fresh_txn, "user") is True
        assert _current_source(real_db, fresh_txn) == "user"
        real_db.execute(
            "DELETE FROM app.transaction_categories WHERE transaction_id = ?",
            [fresh_txn],
        )


def test_ai_cannot_overwrite_user(real_db: Database, fresh_txn: str) -> None:
    svc = CategorizationService(real_db)
    svc.write_categorization(
        transaction_id=fresh_txn,
        category="Coffee Shops",
        subcategory=None,
        categorized_by="user",
    )
    outcome = svc.write_categorization(
        transaction_id=fresh_txn,
        category="Different Category",
        subcategory=None,
        categorized_by="ai",
    )
    assert outcome.written is False
    assert outcome.skipped_reason == "lower_priority_source"
    assert _current_source(real_db, fresh_txn) == "user"


def test_same_source_overwrites_itself(real_db: Database, fresh_txn: str) -> None:
    """Same-priority writes succeed (the rule is ``<=``, not ``<``).

    Two consecutive ``ai`` categorizations both write — the second replaces
    the first.
    """
    svc = CategorizationService(real_db)
    svc.write_categorization(
        transaction_id=fresh_txn,
        category="First",
        subcategory=None,
        categorized_by="ai",
    )
    outcome = svc.write_categorization(
        transaction_id=fresh_txn,
        category="Second",
        subcategory=None,
        categorized_by="ai",
    )
    assert outcome.written is True
    row = real_db.execute(
        "SELECT category FROM app.transaction_categories WHERE transaction_id = ?",
        [fresh_txn],
    ).fetchone()
    assert row is not None
    assert row[0] == "Second"


def test_full_precedence_ladder(real_db: Database, fresh_txn: str) -> None:
    """Walk every (existing, attempted) pair; verify outcome matches the table."""
    svc = CategorizationService(real_db)
    sources = list(_PRIORITY.keys())
    for existing in sources:
        for attempted in sources:
            real_db.execute(
                "DELETE FROM app.transaction_categories WHERE transaction_id = ?",
                [fresh_txn],
            )
            svc.write_categorization(
                transaction_id=fresh_txn,
                category="Initial",
                subcategory=None,
                categorized_by=existing,
            )
            outcome = svc.write_categorization(
                transaction_id=fresh_txn,
                category="Replacement",
                subcategory=None,
                categorized_by=attempted,
            )
            should_write = _PRIORITY[attempted] <= _PRIORITY[existing]
            assert outcome.written is should_write, (
                f"{attempted!r} writing over {existing!r}: "
                f"expected written={should_write}, got {outcome.written}"
            )


def test_auto_rule_backfill_routes_through_write_categorization(
    real_db: Database, fresh_txn: str
) -> None:
    """Auto-rule backfill must route every write through ``write_categorization``.

    Regression guard for ``categorization-matching-mechanics.md`` §Source
    precedence: every write to ``app.transaction_categories`` — including the
    auto-rule backfill path triggered on rule approval — must go through the
    source-priority guard, never an unguarded direct ``INSERT``.

    Two-part assertion:

    1. Routing: a successful backfill through ``approve()`` writes via the
       guarded ``CategorizationService.write_categorization`` path (proven by
       a wrapping spy that counts invocations). A bypassed INSERT would write
       to the table without going through the spy.
    2. Precedence guard active in this path: re-running the backfill against
       a row that has since been re-categorized as ``user`` results in the
       precedence-skip metric incrementing for ``(user, auto_rule)`` and the
       user category surviving — proving the precedence guard is the only
       thing protecting the row, not just the SQL filter.
    """
    cat_svc = CategorizationService(real_db)
    auto_svc = AutoRuleService(real_db)
    # Force the lazy property to resolve so we can wrap the real instance.
    real_cat = auto_svc._categorization  # pyright: ignore[reportPrivateUsage]  # spying on the wired service

    write_calls: list[tuple[str, str]] = []
    original_write = real_cat.write_categorization

    def spy_write(**kwargs: object) -> object:
        write_calls.append((
            str(kwargs["transaction_id"]),
            str(kwargs["categorized_by"]),
        ))
        return original_write(**kwargs)  # type: ignore[arg-type]

    real_cat.write_categorization = spy_write  # type: ignore[method-assign]

    # Stage and approve a proposal whose pattern matches fresh_txn's
    # description ("STARBUCKS #1234").
    proposal_id = "prop_starbucks"
    real_db.execute(
        """
        INSERT INTO app.proposed_rules
            (proposed_rule_id, merchant_pattern, match_type, category, subcategory,
             trigger_count, status, proposed_at, sample_txn_ids)
        VALUES (?, ?, ?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP, ?)
        """,  # noqa: S608  # test input, not executing user SQL
        [proposal_id, "starbucks", "contains", "Coffee Shops", None, 5, []],
    )

    result = auto_svc.approve([proposal_id])

    # Routing assertion: the backfill wrote via write_categorization.
    assert result.approved == 1
    assert result.newly_categorized == 1
    assert (fresh_txn, "auto_rule") in write_calls, (
        "auto-rule backfill bypassed write_categorization — "
        "the precedence guard would not fire"
    )
    assert _current_source(real_db, fresh_txn) == "auto_rule"

    # Precedence assertion: user overrides, then a direct call to
    # _categorize_existing_with_rule (the post-fix routed path) cannot
    # overwrite the user row. The metric increments for (user, auto_rule).
    cat_svc.write_categorization(
        transaction_id=fresh_txn,
        category="Different Category",
        subcategory=None,
        categorized_by="user",
    )
    assert _current_source(real_db, fresh_txn) == "user"

    metric_labels = {"src_existing": "user", "src_attempted": "auto_rule"}
    before = REGISTRY.get_sample_value(
        "moneybin_categorize_write_skipped_precedence_total", metric_labels
    )
    # Direct call to the now-routed write to simulate Task 6's eventual
    # auto-apply-on-commit flow, where the SQL filter no longer pre-screens
    # already-categorized rows. This proves the guard is the load-bearing
    # invariant, not the SQL filter.
    outcome = real_cat.write_categorization(
        transaction_id=fresh_txn,
        category="Coffee Shops",
        subcategory=None,
        categorized_by="auto_rule",
        rule_id=result.rule_ids[0],
        confidence=1.0,
    )
    assert outcome.written is False
    assert outcome.skipped_reason == "lower_priority_source"
    assert _current_source(real_db, fresh_txn) == "user"
    after = REGISTRY.get_sample_value(
        "moneybin_categorize_write_skipped_precedence_total", metric_labels
    )
    assert (after or 0.0) - (before or 0.0) == 1.0


def test_blocked_write_does_not_mutate_merchant_or_proposal_state(
    real_db: Database, fresh_txn: str
) -> None:
    """A precedence-blocked bulk write must not touch downstream learning state.

    Regression guard: ``_bulk_categorize_inner`` resolves the merchant, writes
    the categorization, and only then records the auto-rule proposal and
    appends to the exemplar set. If the write is rejected by the precedence
    guard (higher-priority source already covers the row), the suggestion was
    refused — mutating the merchant table or proposing a rule from a rejected
    suggestion would poison future matching and learning.
    """
    cat_svc = CategorizationService(real_db)

    # Pre-categorize as user (priority 1 — highest).
    cat_svc.write_categorization(
        transaction_id=fresh_txn,
        category="Coffee Shops",
        subcategory=None,
        categorized_by="user",
    )

    merchants_before = real_db.execute(
        "SELECT COUNT(*) FROM app.user_merchants"
    ).fetchone()
    assert merchants_before is not None
    merchant_count_before = int(merchants_before[0])

    exemplars_before = real_db.execute(
        "SELECT merchant_id, len(exemplars) FROM app.user_merchants"
    ).fetchall()

    proposals_before = real_db.execute(
        "SELECT proposed_rule_id, trigger_count FROM app.proposed_rules"
    ).fetchall()

    # Bulk-categorize with a NEW category + canonical name. This is the
    # rejected-suggestion path: ai (priority 8) is lower than the existing
    # user write, so the write_categorization call should return written=False
    # and no downstream state should change.
    result = cat_svc.bulk_categorize([
        BulkCategorizationItem(
            transaction_id=fresh_txn,
            category="Different",
            subcategory=None,
            canonical_merchant_name="SomeNew",
        ),
    ])

    assert result.applied == 0
    assert result.skipped >= 1
    # The user categorization is intact.
    row = real_db.execute(
        "SELECT category, categorized_by FROM app.transaction_categories "
        "WHERE transaction_id = ?",
        [fresh_txn],
    ).fetchone()
    assert row is not None
    assert row[0] == "Coffee Shops"
    assert row[1] == "user"

    # No new merchant was created from the rejected suggestion.
    merchants_after = real_db.execute(
        "SELECT COUNT(*) FROM app.user_merchants"
    ).fetchone()
    assert merchants_after is not None
    assert int(merchants_after[0]) == merchant_count_before

    # No existing merchant's exemplar set grew.
    exemplars_after = real_db.execute(
        "SELECT merchant_id, len(exemplars) FROM app.user_merchants"
    ).fetchall()
    assert exemplars_after == exemplars_before

    # No auto-rule learning fired (no new proposals; existing trigger_counts
    # unchanged).
    proposals_after = real_db.execute(
        "SELECT proposed_rule_id, trigger_count FROM app.proposed_rules"
    ).fetchall()
    assert proposals_after == proposals_before
