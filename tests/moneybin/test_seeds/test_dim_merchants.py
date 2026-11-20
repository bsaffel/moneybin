"""Merchant categories resolve identically at startup and after a transform."""

from __future__ import annotations

import pytest

from moneybin.database import SQLMESH_ROOT, Database
from moneybin.services.transform_service import TransformService
from tests.moneybin.sqlmesh_model_helpers import rendered_model_sql


@pytest.mark.fresh_db
@pytest.mark.parametrize(
    "apply_transform",
    [
        pytest.param(False, id="bootstrap"),
        pytest.param(
            True,
            id="after-transform",
            marks=[pytest.mark.integration, pytest.mark.slow],
        ),
    ],
)
def test_merchant_categories_match_model(db: Database, apply_transform: bool) -> None:
    db.execute(
        """
        INSERT INTO app.user_categories (category_id, category, subcategory)
        VALUES ('linked', 'Current category', 'Current subcategory')
        """
    )
    db.execute(
        """
        INSERT INTO app.user_merchants
            (merchant_id, canonical_name, created_by, category_id, category, subcategory)
        VALUES
            ('a', 'Linked merchant', 'user', 'linked', 'Stale category', 'Stale subcategory'),
            ('b', 'Orphaned merchant', 'user', 'missing', 'Saved category', 'Saved subcategory')
        """
    )
    if apply_transform:
        result = TransformService(db).apply()
        assert result.applied, result.error

    assert db.execute(
        """
        SELECT category, subcategory FROM core.dim_merchants ORDER BY merchant_id
        """
    ).fetchall() == [
        ("Current category", "Current subcategory"),
        ("Saved category", "Saved subcategory"),
    ]
    model_sql = rendered_model_sql(SQLMESH_ROOT / "models/core/dim_merchants.sql")
    model = db.execute(
        f"SELECT * FROM ({model_sql}) ORDER BY merchant_id"  # noqa: S608  # checked-in model SQL
    )
    model_columns = [column[0] for column in model.description]
    model_rows = model.fetchall()
    view = db.execute("SELECT * FROM core.dim_merchants ORDER BY merchant_id")
    assert [column[0] for column in view.description] == model_columns
    assert view.fetchall() == model_rows
