"""``BaseRepo._require_admissible``: undo refuses to restore inadmissible text.

Issue #547: #517 made the write path refuse a whitespace-only
category/subcategory, but the generic undo reverser (``BaseRepo.undo_event``)
writes a captured before-image back verbatim with no validator in the path —
so undoing a delete captured before #517 shipped (or, at the repo layer used
directly here, any row a caller wrote around the service-layer check) could
resurrect a value no live write path can produce. ``_require_admissible``
closes this by revalidating the six affected tables' declared columns through
the same ``validate_category_text`` the write path calls, refusing with
``error_codes.UNDO_VALUE_INADMISSIBLE`` instead of writing the row back.

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
from moneybin.repositories.categorization_rules_repo import CategorizationRulesRepo
from moneybin.repositories.proposed_rules_repo import ProposedRulesRepo
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
