"""Tests that ``updated_at`` is set and refreshed on user_categories / user_merchants writes.

Part of the core-updated-at-convention spec: app tables exposing an
``updated_at`` column must have it populated on INSERT and refreshed on every
UPDATE by the service layer.
"""

from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.services.categorization_service import CategorizationService
from tests.moneybin.db_helpers import create_core_tables


@pytest.fixture()
def db(tmp_path: Path) -> Database:
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-key"
    database = Database(
        tmp_path / "test.duckdb", secret_store=mock_store, no_auto_upgrade=True
    )
    create_core_tables(database)
    return database


class TestUserCategoriesUpdatedAt:
    """``app.user_categories.updated_at`` lifecycle via CategorizationService."""

    @pytest.mark.unit
    def test_create_category_sets_updated_at(self, db: Database) -> None:
        cat_id = CategorizationService(db).create_category("Childcare")
        row = db.execute(
            "SELECT updated_at FROM app.user_categories WHERE category_id = ?",
            [cat_id],
        ).fetchone()
        assert row is not None
        assert row[0] is not None

    @pytest.mark.unit
    def test_toggle_category_advances_updated_at(self, db: Database) -> None:
        svc = CategorizationService(db)
        cat_id = svc.create_category("Childcare")
        before = db.execute(
            "SELECT updated_at FROM app.user_categories WHERE category_id = ?",
            [cat_id],
        ).fetchone()
        assert before is not None
        # Ensure timestamp resolution actually advances between create and update.
        time.sleep(0.01)
        svc.toggle_category(cat_id, is_active=False)
        after = db.execute(
            "SELECT updated_at FROM app.user_categories WHERE category_id = ?",
            [cat_id],
        ).fetchone()
        assert after is not None
        assert after[0] > before[0]


class TestUserMerchantsUpdatedAt:
    """``app.user_merchants.updated_at`` lifecycle via CategorizationService."""

    @pytest.mark.unit
    def test_create_merchant_sets_updated_at(self, db: Database) -> None:
        merchant_id = CategorizationService(db).create_merchant(
            raw_pattern=None,
            canonical_name="Starbucks",
            match_type="oneOf",
            exemplars=["STARBUCKS #1234"],
        )
        row = db.execute(
            "SELECT updated_at FROM app.user_merchants WHERE merchant_id = ?",
            [merchant_id],
        ).fetchone()
        assert row is not None
        assert row[0] is not None

    @pytest.mark.unit
    def test_append_exemplar_advances_updated_at(self, db: Database) -> None:
        svc = CategorizationService(db)
        merchant_id = svc.create_merchant(
            raw_pattern=None,
            canonical_name="Starbucks",
            match_type="oneOf",
            exemplars=["STARBUCKS #1234"],
        )
        before = db.execute(
            "SELECT updated_at FROM app.user_merchants WHERE merchant_id = ?",
            [merchant_id],
        ).fetchone()
        assert before is not None
        time.sleep(0.01)
        # _append_exemplar is an internal write site exercised here directly
        # so the updated_at refresh is pinned at the service boundary, not
        # several layers up through a categorization batch.
        svc._append_exemplar(merchant_id, "STARBUCKS #5678")  # pyright: ignore[reportPrivateUsage]
        after = db.execute(
            "SELECT updated_at FROM app.user_merchants WHERE merchant_id = ?",
            [merchant_id],
        ).fetchone()
        assert after is not None
        assert after[0] > before[0]
