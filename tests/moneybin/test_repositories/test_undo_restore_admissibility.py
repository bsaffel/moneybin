"""``BaseRepo._require_admissible``: undo refuses to restore inadmissible text.

Issue #547: #517 made the write path refuse a whitespace-only
category/subcategory, but the generic undo reverser (``BaseRepo.undo_event``)
writes a captured before-image back verbatim with no validator in the path —
so undoing a delete captured before #517 shipped (or, at the repo layer used
directly here, any row a caller wrote around the service-layer check) could
resurrect a value no live write path can produce. ``_require_admissible``
closes this by revalidating each affected table's declared
``_CATEGORY_TEXT_COLUMNS`` through the same ``validate_category_text`` the
write path calls, refusing with ``error_codes.UNDO_VALUE_INADMISSIBLE``
instead of writing the row back.

Coverage is every ``BaseRepo`` table with a category/subcategory-shaped text
column, found by sweeping the schema rather than trusting a fixed list: the
six the issue named (``user_categories``, ``user_merchants``,
``transaction_splits``, ``budgets``, ``categorization_rules``,
``proposed_rules``) plus three the sweep also found
(``transaction_categories``, ``rule_conflicts``, ``categorization_decisions``).

Repos, not services, are exercised directly: ``BaseRepo.insert``/``delete``
carry no validation of their own (validation is a service-layer concern), so
calling them with a blank category is the same shape a pre-#517 audit row
would have — no service-layer bypass or synthetic ``AuditEvent`` needed.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.budgets_repo import BudgetsRepo
from moneybin.repositories.categorization_decisions_repo import (
    CategorizationDecisionsRepo,
)
from moneybin.repositories.categorization_rules_repo import CategorizationRulesRepo
from moneybin.repositories.proposed_rules_repo import ProposedRulesRepo
from moneybin.repositories.rule_conflicts_repo import RuleConflictsRepo
from moneybin.repositories.transaction_categories_repo import TransactionCategoriesRepo
from moneybin.repositories.transaction_splits_repo import TransactionSplitsRepo
from moneybin.repositories.user_categories_repo import UserCategoriesRepo
from moneybin.repositories.user_merchants_repo import UserMerchantsRepo


class TestUserCategoriesRefusesInadmissibleRestore:
    """app.user_categories: undo refuses a blank category/subcategory."""

    def test_delete_undo_refuses_blank_category(self, db: Database) -> None:
        repo = UserCategoriesRepo(db)
        cid = repo.insert(category="   ", actor="user").target_id
        assert cid is not None
        delete_event = repo.delete(cid, actor="user")

        with pytest.raises(UserError) as exc_info:
            repo.undo_event(delete_event, actor="user")

        assert exc_info.value.code == error_codes.UNDO_VALUE_INADMISSIBLE
        assert "category" in str(exc_info.value)
        # Refused, not partially applied.
        row = db.execute(
            "SELECT 1 FROM app.user_categories WHERE category_id = ?", [cid]
        ).fetchone()
        assert row is None

    def test_update_undo_refuses_blank_category(self, db: Database) -> None:
        """The UPDATE-restore branch shares the same guard as DELETE-restore.

        Simulates a legacy row that already carries a blank category (as if
        written before #517) and is then touched by an unrelated field update
        (``update_active``) — the before-image restore must still refuse.
        """
        repo = UserCategoriesRepo(db)
        cid = repo.insert(category="Dining", actor="user").target_id
        assert cid is not None
        db.conn.execute(
            "UPDATE app.user_categories SET category = '   ' WHERE category_id = ?",
            [cid],
        )
        update_event = repo.update_active(cid, is_active=False, actor="user")

        with pytest.raises(UserError) as exc_info:
            repo.undo_event(update_event, actor="user")

        assert exc_info.value.code == error_codes.UNDO_VALUE_INADMISSIBLE


class TestUserMerchantsRefusesInadmissibleRestore:
    """app.user_merchants: undo refuses a blank category/subcategory."""

    def test_delete_undo_refuses_blank_category(self, db: Database) -> None:
        repo = UserMerchantsRepo(db)
        merchant_id = repo.insert(
            raw_pattern="AMZN",
            match_type="contains",
            canonical_name="Amazon",
            category="   ",
            subcategory=None,
            category_id=None,
            created_by="user",
            exemplars=None,
            actor="user",
        ).target_id
        assert merchant_id is not None
        delete_event = repo.delete(merchant_id, actor="user")

        with pytest.raises(UserError) as exc_info:
            repo.undo_event(delete_event, actor="user")

        assert exc_info.value.code == error_codes.UNDO_VALUE_INADMISSIBLE
        assert "category" in str(exc_info.value)


class TestTransactionSplitsRefusesInadmissibleRestore:
    """app.transaction_splits: undo refuses a blank category/subcategory."""

    def test_delete_undo_refuses_blank_subcategory(self, db: Database) -> None:
        repo = TransactionSplitsRepo(db)
        add = repo.insert(
            split_id="split1",
            transaction_id="txn_1",
            amount=Decimal("10.00"),
            category="Dining",
            subcategory="   ",
            category_id=None,
            note=None,
            ord=0,
            actor="cli",
        )
        assert add.target_id == "split1"
        delete_event = repo.delete(split_id="split1", actor="cli")

        with pytest.raises(UserError) as exc_info:
            repo.undo_event(delete_event, actor="cli")

        assert exc_info.value.code == error_codes.UNDO_VALUE_INADMISSIBLE
        assert "subcategory" in str(exc_info.value)


class TestBudgetsRefusesInadmissibleRestore:
    """app.budgets: undo refuses a blank category."""

    def test_delete_undo_refuses_blank_category(self, db: Database) -> None:
        repo = BudgetsRepo(db)
        event = repo.insert(
            category="   ",
            category_id=None,
            monthly_amount=Decimal("500.00"),
            start_month="2026-01",
            actor="cli",
        )
        budget_id = event.target_id
        assert budget_id is not None
        delete_event = repo.delete(budget_id, actor="cli")

        with pytest.raises(UserError) as exc_info:
            repo.undo_event(delete_event, actor="cli")

        assert exc_info.value.code == error_codes.UNDO_VALUE_INADMISSIBLE
        assert "category" in str(exc_info.value)


class TestCategorizationRulesRefusesInadmissibleRestore:
    """app.categorization_rules: undo refuses a blank category/subcategory."""

    def test_delete_undo_refuses_blank_category(self, db: Database) -> None:
        repo = CategorizationRulesRepo(db)
        event = repo.insert(
            name="Coffee rule",
            merchant_pattern="STARBUCKS",
            match_type="contains",
            min_amount=None,
            max_amount=None,
            account_id=None,
            category="   ",
            subcategory=None,
            category_id=None,
            priority=100,
            created_by="user",
            actor="cli",
        )
        rule_id = event.target_id
        assert rule_id is not None
        delete_event = repo.delete(rule_id, actor="cli")
        assert delete_event is not None

        with pytest.raises(UserError) as exc_info:
            repo.undo_event(delete_event, actor="cli")

        assert exc_info.value.code == error_codes.UNDO_VALUE_INADMISSIBLE
        assert "category" in str(exc_info.value)


class TestProposedRulesRefusesInadmissibleRestore:
    """app.proposed_rules: undo refuses a blank category/subcategory."""

    def test_delete_undo_refuses_blank_category(self, db: Database) -> None:
        repo = ProposedRulesRepo(db)
        event = repo.insert(
            merchant_pattern="STARBUCKS",
            match_type="contains",
            category="   ",
            subcategory=None,
            category_id=None,
            status="pending",
            sample_txn_ids=["txn_1"],
            actor="cli",
        )
        proposed_rule_id = event.target_id
        assert proposed_rule_id is not None
        delete_event = repo.delete(proposed_rule_id, actor="cli")

        with pytest.raises(UserError) as exc_info:
            repo.undo_event(delete_event, actor="cli")

        assert exc_info.value.code == error_codes.UNDO_VALUE_INADMISSIBLE
        assert "category" in str(exc_info.value)


class TestTransactionCategoriesRefusesInadmissibleRestore:
    """app.transaction_categories: undo refuses a blank category/subcategory.

    Found by re-sweeping every ``BaseRepo`` table's schema for a
    category/subcategory text column rather than trusting the issue's
    six-table list — the per-transaction categorization table has the
    identical shape and was missing from the first pass.
    """

    def test_delete_undo_refuses_blank_category(self, db: Database) -> None:
        repo = TransactionCategoriesRepo(db)
        repo.set(
            "txn_1",
            category="   ",
            subcategory=None,
            category_id=None,
            actor="cli",
        )
        delete_event = repo.clear("txn_1", actor="cli")
        assert delete_event is not None

        with pytest.raises(UserError) as exc_info:
            repo.undo_event(delete_event, actor="cli")

        assert exc_info.value.code == error_codes.UNDO_VALUE_INADMISSIBLE
        assert "category" in str(exc_info.value)


class TestRuleConflictsRefusesInadmissibleRestore:
    """app.rule_conflicts: undo refuses a blank existing/proposed category.

    Found by the same schema sweep as ``transaction_categories`` — this repo
    uses the generic ``undo_event`` unmodified, so ``prune_stale``'s
    DELETE-undo (exercised here) and ``resolve``'s UPDATE-undo are both
    exposed the same way ``categorization_rules``/``proposed_rules`` are.
    """

    def test_delete_undo_refuses_blank_proposed_category(self, db: Database) -> None:
        repo = RuleConflictsRepo(db)
        repo.set(
            conflict_id="conflict1",
            matcher_digest="digest1",
            existing_rule_id="rule1",
            existing_rule_updated_at="2026-01-01 00:00:00",
            existing_name="Existing rule",
            existing_category="Dining",
            existing_subcategory=None,
            existing_priority=100,
            proposed_name="Proposed rule",
            proposed_merchant_pattern="STARBUCKS",
            proposed_match_type="contains",
            proposed_min_amount=None,
            proposed_max_amount=None,
            proposed_account_id=None,
            proposed_category="   ",
            proposed_subcategory=None,
            proposed_priority=100,
            proposed_created_by="ai",
            actor="cli",
        )
        # A different existing_rule_updated_at than the one just stored marks
        # the pending conflict stale, so prune_stale deletes it — a real
        # DELETE-undo scenario, not a synthetic before-image.
        delete_events = repo.prune_stale(
            "rule1", keep_updated_at="2026-01-02 00:00:00", actor="cli"
        )
        assert len(delete_events) == 1

        with pytest.raises(UserError) as exc_info:
            repo.undo_event(delete_events[0], actor="cli")

        assert exc_info.value.code == error_codes.UNDO_VALUE_INADMISSIBLE
        assert "proposed_category" in str(exc_info.value)


class TestCategorizationDecisionsRefusesInadmissibleRestore:
    """app.categorization_decisions: undo-of-undo refuses a blank snapshot.

    Found by the same schema sweep. This repo overrides ``undo_event`` and
    never rewrites ``category``/``subcategory`` on its own is_undo=False
    events (it only toggles ``reversed_at``/``reversed_by``) — but delegates
    to the generic ``BaseRepo.undo_event`` when reversing one of its own
    is_undo rows, which is the path exercised here.
    """

    def test_undo_of_reversal_refuses_blank_category_snapshot(
        self, db: Database
    ) -> None:
        from moneybin.services.audit_service import AuditService
        from moneybin.services.mutation_context import operation

        repo = CategorizationDecisionsRepo(db)
        pending = repo.ensure_pending("txn_1", actor="cli")
        decision_id = str(pending["decision_id"])
        # Materialize a blank-category transaction_categories row directly
        # (as if written before #517's write-path guard existed) for
        # update_status's accepted branch to snapshot.
        db.conn.execute(
            "INSERT INTO app.transaction_categories "
            "(transaction_id, category, subcategory, category_id) "
            "VALUES ('txn_1', '   ', NULL, 'cat_1')"
        )
        with operation() as accept_op:
            repo.update_status(
                decision_id,
                status="accepted",
                category_id="cat_1",
                merchant_id=None,
                decided_by="user",
                actor="cli",
            )
        accepted = next(
            e
            for e in AuditService(db).events_for_operation(accept_op)
            if e.action == "categorization_decision.update_status"
        )
        # The custom override reverses liveness (reversed_at/reversed_by)
        # without touching category/subcategory.
        reversal = repo.undo_event(accepted, actor="cli")
        assert reversal is not None
        assert reversal.is_undo is True

        # Undoing THAT reversal delegates to the generic reverser, which
        # restores the full before-image — including the blank snapshot.
        with pytest.raises(UserError) as exc_info:
            repo.undo_event(reversal, actor="cli")

        assert exc_info.value.code == error_codes.UNDO_VALUE_INADMISSIBLE
        assert "category" in str(exc_info.value)


class TestAdmissibleRestoreStillSucceeds:
    """Restraint test: an admissible before-image restores exactly as before.

    The guard must be a no-op on every value ``validate_category_text``
    already accepts — this is the regression check that #547's fix doesn't
    also block ordinary, valid undos.
    """

    def test_delete_undo_restores_valid_category(self, db: Database) -> None:
        repo = BudgetsRepo(db)
        event = repo.insert(
            category="Groceries",
            category_id=None,
            monthly_amount=Decimal("250.00"),
            start_month="2026-01",
            actor="cli",
        )
        budget_id = event.target_id
        assert budget_id is not None
        delete_event = repo.delete(budget_id, actor="cli")

        repo.undo_event(delete_event, actor="cli")

        row = db.execute(
            "SELECT category FROM app.budgets WHERE budget_id = ?", [budget_id]
        ).fetchone()
        assert row == ("Groceries",)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
