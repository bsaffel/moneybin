"""Tests for the standard normalized taxonomy read."""

from __future__ import annotations

import json
from typing import Any, Literal
from unittest.mock import patch

import pytest

from moneybin import error_codes
from moneybin.mcp.tools.taxonomy import (
    register_taxonomy_coarse_reads,
    register_taxonomy_coarse_writes,
    taxonomy_coarse,
    taxonomy_set_coarse,
)
from moneybin.privacy.payloads.categories import (
    CategoriesPayload,
    CategoryRow,
    MerchantRow,
    MerchantsPayload,
)
from moneybin.protocol.pagination import encode_keyset_cursor
from moneybin.protocol.write_contracts import (
    CategoryStateRequest,
    MerchantStateRequest,
    SourceCategoryStateRequest,
)
from moneybin.services.categorization import CategorizationService

from .schema_assertions import (
    assert_literal_values,
    call_tool_raw,
    isolated_server,
    listed_tool,
)

pytestmark = pytest.mark.usefixtures("mcp_db")


async def test_taxonomy_projects_categories_and_merchants() -> None:
    assert (await taxonomy_coarse(view="categories")).data.kind == "categories"
    assert (await taxonomy_coarse(view="merchants")).data.kind == "merchants"


async def test_taxonomy_batch_routes_category_and_merchant_targets() -> None:
    response = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="present",
                category="Task 6 Dining",
            ),
            MerchantStateRequest(
                kind="merchant",
                state="present",
                raw_pattern="TASK 6 CAFE",
                canonical_name="Task 6 Cafe",
                category="Task 6 Dining",
            ),
        ]
    )

    assert [item.kind for item in response.data.results] == [
        "category",
        "merchant",
    ]


def _import_coffee_term() -> None:
    """One imported csv row carrying the 'Coffee Shops' term, ready to categorize."""
    from moneybin.database import get_database

    with get_database(read_only=False) as db:
        db.execute("CREATE SCHEMA IF NOT EXISTS prep")
        db.execute(
            "CREATE TABLE prep.int_transactions__matched (transaction_id VARCHAR, "
            "source_transaction_id VARCHAR, source_type VARCHAR, "
            "source_origin VARCHAR, category VARCHAR, subcategory VARCHAR)"
        )
        db.execute(
            "INSERT INTO prep.int_transactions__matched VALUES "
            "('t1', 't1', 'csv', 'chase_credit', 'Coffee Shops', NULL)"
        )
        db.execute(
            "INSERT INTO core.fct_transactions (transaction_id, amount, "
            "transaction_date) VALUES ('t1', -4.50, '2026-01-01')"
        )


_COFFEE_TERM: dict[str, Any] = {
    "kind": "source_category",
    "source_origin": "chase_credit",
    "source_category_code": "Coffee Shops",
}


async def test_source_category_round_trip_from_review_to_removal() -> None:
    """A pending term is mapped in one batch, read back as history, then removed."""
    from moneybin.mcp.tools.reviews import reviews_coarse
    from moneybin.mcp.tools.system import system_audit_undo

    _import_coffee_term()
    pending = await reviews_coarse(kind="source_categories", status="pending")
    (row,) = pending.data.rows
    assert row.details.source_category_code == "Coffee Shops"
    assert row.details.transaction_count == 1
    assert any("taxonomy_set" in action for action in pending.actions)

    mapped = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(kind="category", state="present", category="Coffee"),
            SourceCategoryStateRequest(
                **_COFFEE_TERM, state="present", category="Coffee"
            ),
        ]
    )
    assert mapped.error is None
    result = mapped.data.results[1]
    assert (result.kind, result.changed, result.categorized) == (
        "source_category",
        True,
        1,
    )
    assert (await reviews_coarse(kind="source_categories")).data.rows == []
    (history,) = (
        await reviews_coarse(kind="source_categories", status="history")
    ).data.rows
    assert (history.details.target_state, history.details.category) == (
        "present",
        "Coffee",
    )

    first = await taxonomy_set_coarse(
        items=[SourceCategoryStateRequest(**_COFFEE_TERM, state="absent")]
    )
    assert first.error is not None
    removed = await taxonomy_set_coarse(
        items=[SourceCategoryStateRequest(**_COFFEE_TERM, state="absent")],
        confirmation_token=first.error.details["confirmation_token"],
    )
    assert removed.error is None
    assert removed.data.results[0].recategorized == 1
    assert len((await reviews_coarse(kind="source_categories")).data.rows) == 1

    undo = await system_audit_undo(removed.data.operation_id)
    assert undo.error is None
    assert (await reviews_coarse(kind="source_categories")).data.rows == []


async def test_source_category_history_pages_more_than_one_curated_term() -> None:
    """History orders by the term, so several curated terms read back together."""
    from moneybin.database import get_database
    from moneybin.mcp.tools.reviews import reviews_coarse

    _import_coffee_term()
    with get_database(read_only=False) as db:
        db.execute(
            "INSERT INTO prep.int_transactions__matched VALUES "
            "('t2', 't2', 'csv', 'chase_credit', 'Groceries', NULL)"
        )
        db.execute(
            "INSERT INTO core.fct_transactions (transaction_id, amount, "
            "transaction_date) VALUES ('t2', -9.00, '2026-01-02')"
        )
    mapped = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(kind="category", state="present", category="Coffee"),
            SourceCategoryStateRequest(
                **_COFFEE_TERM, state="present", category="Coffee"
            ),
            SourceCategoryStateRequest(
                kind="source_category",
                state="ignored",
                source_origin="chase_credit",
                source_category_code="Groceries",
            ),
        ]
    )
    assert mapped.error is None

    history = await reviews_coarse(kind="source_categories", status="history")

    assert history.error is None
    assert [row.details.source_category_code for row in history.data.rows] == [
        "Coffee Shops",
        "Groceries",
    ]


async def test_source_category_binding_names_the_resolved_destination() -> None:
    """A by-name mapping binds the category id it resolved, not only the name."""
    from moneybin.database import get_database
    from moneybin.mcp.tools.taxonomy import (
        _taxonomy_binding,  # pyright: ignore[reportPrivateUsage]
        _to_taxonomy_target,  # pyright: ignore[reportPrivateUsage]
    )

    _import_coffee_term()
    request = SourceCategoryStateRequest(
        **_COFFEE_TERM, state="present", category="Coffee"
    )
    with get_database(read_only=False) as db:
        service = CategorizationService(db)
        category_id = service.create_category("Coffee", actor="test")
        plan = service.plan_taxonomy_targets([_to_taxonomy_target(request)])

    assert category_id in _taxonomy_binding([request], plan).resolved_ids


@pytest.mark.parametrize(
    "fields",
    [
        {"state": "present"},
        {"state": "present", "category_id": "c1", "category": "Coffee"},
        {"state": "present", "subcategory": "Beans", "category_id": "c1"},
        {"state": "ignored", "category_id": "c1"},
        {"state": "absent", "category": "Coffee"},
    ],
)
def test_source_category_request_rejects_a_misplaced_target(
    fields: dict[str, Any],
) -> None:
    with pytest.raises(ValueError):
        SourceCategoryStateRequest(**_COFFEE_TERM, **fields)


async def test_taxonomy_merchant_absent_is_audited_and_undoable() -> None:
    created = await taxonomy_set_coarse(
        items=[
            MerchantStateRequest(
                kind="merchant",
                state="present",
                raw_pattern="TASK 6 REMOVE",
                canonical_name="Task 6 Remove",
            )
        ]
    )
    merchant_id = created.data.results[0].target_id
    assert merchant_id is not None

    first = await taxonomy_set_coarse(
        items=[
            MerchantStateRequest(
                kind="merchant",
                state="absent",
                merchant_id=merchant_id,
            )
        ]
    )
    assert first.error is not None
    token = first.error.details["confirmation_token"]
    removed = await taxonomy_set_coarse(
        items=[
            MerchantStateRequest(
                kind="merchant",
                state="absent",
                merchant_id=merchant_id,
            )
        ],
        confirmation_token=token,
    )
    assert removed.error is None

    rows = (await taxonomy_coarse(view="merchants")).data.rows
    assert merchant_id not in {row.merchant_id for row in rows}

    from moneybin.mcp.tools.system import system_audit_undo

    undo = await system_audit_undo(removed.data.operation_id)
    assert undo.error is None
    restored = (await taxonomy_coarse(view="merchants")).data.rows
    assert merchant_id in {row.merchant_id for row in restored}


async def test_taxonomy_category_absent_is_audited_and_undoable() -> None:
    created = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="present",
                category="Task 6 Temporary",
            )
        ]
    )
    category_id = created.data.results[0].target_id
    assert category_id is not None

    first = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="absent",
                category_id=category_id,
            )
        ]
    )
    assert first.error is not None
    token = first.error.details["confirmation_token"]
    removed = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="absent",
                category_id=category_id,
            )
        ],
        confirmation_token=token,
    )
    assert removed.error is None
    rows = (await taxonomy_coarse(view="categories", include_inactive=True)).data.rows
    assert category_id not in {row.category_id for row in rows}

    from moneybin.mcp.tools.system import system_audit_undo

    undo = await system_audit_undo(removed.data.operation_id)
    assert undo.error is None
    restored = (
        await taxonomy_coarse(view="categories", include_inactive=True)
    ).data.rows
    assert category_id in {row.category_id for row in restored}


async def test_taxonomy_category_delete_preflights_usage_before_confirmation() -> None:
    created = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="present",
                category="Task 6 Referenced",
            ),
            MerchantStateRequest(
                kind="merchant",
                state="present",
                raw_pattern="TASK 6 REFERENCED",
                canonical_name="Task 6 Referenced",
                category="Task 6 Referenced",
            ),
        ]
    )
    category_id = created.data.results[0].target_id
    assert category_id is not None

    response = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="absent",
                category_id=category_id,
            )
        ]
    )

    assert response.error is not None
    assert response.error.code == "taxonomy_category_has_references"
    assert "confirmation_token" not in response.error.details


async def test_taxonomy_force_delete_cascades_and_undoes_one_operation() -> None:
    created = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="present",
                category="Task 6 Cascade",
            ),
            MerchantStateRequest(
                kind="merchant",
                state="present",
                raw_pattern="TASK 6 CASCADE",
                canonical_name="Task 6 Cascade",
                category="Task 6 Cascade",
            ),
        ]
    )
    category_id = created.data.results[0].target_id
    merchant_id = created.data.results[1].target_id
    assert category_id is not None
    assert merchant_id is not None

    from moneybin.database import get_database

    with get_database(read_only=False) as db:
        db.execute(
            "INSERT INTO app.transaction_categories "
            "(transaction_id, category, category_id, categorized_by) "
            "VALUES ('task6-txn', 'Task 6 Cascade', ?, 'user')",
            [category_id],
        )
        db.execute(
            "INSERT INTO app.budgets "
            "(budget_id, category, category_id, monthly_amount, start_month) "
            "VALUES ('task6-budget', 'Task 6 Cascade', ?, 100, '2026-07')",
            [category_id],
        )
        db.execute(
            "INSERT INTO app.transaction_splits "
            "(split_id, transaction_id, amount, category, category_id, created_by) "
            "VALUES ('task6-split', 'task6-txn', -10, 'Task 6 Cascade', ?, 'mcp')",
            [category_id],
        )
        db.execute(
            "INSERT INTO app.categorization_rules "
            "(rule_id, name, merchant_pattern, category, category_id) "
            "VALUES ('task6-rule', 'Task 6', 'TASK6', 'Task 6 Cascade', ?)",
            [category_id],
        )
        db.execute(
            "INSERT INTO app.proposed_rules "
            "(proposed_rule_id, merchant_pattern, category, category_id) "
            "VALUES ('task6-proposal', 'TASK6', 'Task 6 Cascade', ?)",
            [category_id],
        )
        db.execute(
            "INSERT INTO app.category_source_map "
            "(source_type, source_origin, source_category_code, category_id) "
            "VALUES ('task6', '', 'cascade', ?)",
            [category_id],
        )

    first = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="absent",
                category_id=category_id,
                force=True,
            )
        ]
    )
    assert first.error is not None
    assert first.error.details["blast_radius"] == {
        "targets": 1,
        "changed_targets": 1,
        "explicit_hard_deletes": 1,
        "cascade_hard_deletes": 7,
        "hard_deletes": 8,
    }
    removed = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="absent",
                category_id=category_id,
                force=True,
            )
        ],
        confirmation_token=first.error.details["confirmation_token"],
    )
    assert removed.error is None
    assert merchant_id not in {
        row.merchant_id for row in (await taxonomy_coarse(view="merchants")).data.rows
    }

    with get_database(read_only=True) as db:
        actions = db.execute(
            "SELECT action FROM app.audit_log WHERE operation_id = ? "
            "ORDER BY occurred_at, audit_id",
            [removed.data.operation_id],
        ).fetchall()
    assert {row[0] for row in actions} == {
        "budget.delete",
        "category.clear",
        "categorization_rule.delete",
        "category_source_map.delete",
        "proposed_rule.delete",
        "split.remove",
        "user_merchant.delete",
        "user_category.delete",
    }

    from moneybin.mcp.tools.system import system_audit_undo

    undo = await system_audit_undo(removed.data.operation_id)
    assert undo.error is None
    restored_merchants = (await taxonomy_coarse(view="merchants")).data.rows
    assert merchant_id in {row.merchant_id for row in restored_merchants}
    with get_database(read_only=True) as db:
        restored_counts = db.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM app.transaction_categories
                  WHERE category_id = ?),
                (SELECT COUNT(*) FROM app.budgets WHERE category_id = ?),
                (SELECT COUNT(*) FROM app.user_merchants WHERE category_id = ?),
                (SELECT COUNT(*) FROM app.transaction_splits WHERE category_id = ?),
                (SELECT COUNT(*) FROM app.categorization_rules
                  WHERE category_id = ?),
                (SELECT COUNT(*) FROM app.proposed_rules WHERE category_id = ?),
                (SELECT COUNT(*) FROM app.category_source_map
                  WHERE category_id = ?)
            """,
            [category_id] * 7,
        ).fetchone()
    assert restored_counts == (1, 1, 1, 1, 1, 1, 1)


async def test_taxonomy_force_delete_rejects_same_count_different_row_token() -> None:
    created = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="present",
                category="Task 6 Snapshot",
            )
        ]
    )
    category_id = created.data.results[0].target_id
    assert category_id is not None
    from moneybin.database import get_database

    with get_database(read_only=False) as db:
        db.execute(
            "INSERT INTO app.transaction_categories "
            "(transaction_id, category, category_id, categorized_by) "
            "VALUES ('task6-snapshot-a', 'Task 6 Snapshot', ?, 'user')",
            [category_id],
        )
    request = CategoryStateRequest(
        kind="category",
        state="absent",
        category_id=category_id,
        force=True,
    )
    first = await taxonomy_set_coarse(items=[request])
    assert first.error is not None

    with get_database(read_only=False) as db:
        db.execute(
            "DELETE FROM app.transaction_categories "
            "WHERE transaction_id = 'task6-snapshot-a'"
        )
        db.execute(
            "INSERT INTO app.transaction_categories "
            "(transaction_id, category, category_id, categorized_by) "
            "VALUES ('task6-snapshot-b', 'Task 6 Snapshot', ?, 'user')",
            [category_id],
        )
    stale = await taxonomy_set_coarse(
        items=[request],
        confirmation_token=first.error.details["confirmation_token"],
    )

    assert stale.error is not None
    assert stale.error.code == error_codes.MUTATION_CONFIRMATION_MISMATCH
    with get_database(read_only=True) as db:
        assert db.execute(
            "SELECT transaction_id FROM app.transaction_categories "
            "WHERE category_id = ?",
            [category_id],
        ).fetchall() == [("task6-snapshot-b",)]


async def test_taxonomy_force_delete_rejects_changed_dependent_before_image() -> None:
    created = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="present",
                category="Task 6 Before Image",
            )
        ]
    )
    category_id = created.data.results[0].target_id
    assert category_id is not None
    from moneybin.database import get_database

    with get_database(read_only=False) as db:
        db.execute(
            "INSERT INTO app.budgets "
            "(budget_id, category, category_id, monthly_amount, start_month) "
            "VALUES ('task6-before-image', 'Task 6 Before Image', ?, 100, '2026-07')",
            [category_id],
        )
    request = CategoryStateRequest(
        kind="category",
        state="absent",
        category_id=category_id,
        force=True,
    )
    first = await taxonomy_set_coarse(items=[request])
    assert first.error is not None

    with get_database(read_only=False) as db:
        db.execute(
            "UPDATE app.budgets SET monthly_amount = 200 "
            "WHERE budget_id = 'task6-before-image'"
        )
    stale = await taxonomy_set_coarse(
        items=[request],
        confirmation_token=first.error.details["confirmation_token"],
    )

    assert stale.error is not None
    assert stale.error.code == error_codes.MUTATION_CONFIRMATION_MISMATCH


async def test_taxonomy_rejects_duplicate_normalized_category_candidates() -> None:
    from moneybin.database import get_database

    with get_database(read_only=False) as db:
        service = CategorizationService(db)
        candidate_ids = {
            service.create_category("Task 6 Ambiguous"),
            service.create_category("task 6 ambiguous"),
        }

    response = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="present",
                category="Task 6 Must Not Persist",
            ),
            CategoryStateRequest(
                kind="category",
                state="present",
                category="TASK 6 AMBIGUOUS",
            ),
        ]
    )

    assert response.error is not None
    assert response.error.code == error_codes.MUTATION_AMBIGUOUS
    assert response.error.details["candidate_ids"] == sorted(candidate_ids)
    assert not any(
        row.category == "Task 6 Must Not Persist"
        for row in (await taxonomy_coarse(view="categories")).data.rows
    )


async def test_taxonomy_rejects_duplicate_normalized_merchant_candidates() -> None:
    from moneybin.database import get_database

    with get_database(read_only=False) as db:
        service = CategorizationService(db)
        candidate_ids = {
            service.create_merchant(
                raw_pattern="TASK 6 DUPLICATE",
                canonical_name="Task 6 Duplicate",
                match_type="contains",
                created_by="ai",
            )
            for _ in range(2)
        }

    response = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="present",
                category="Task 6 Merchant Must Not Persist",
            ),
            MerchantStateRequest(
                kind="merchant",
                state="present",
                raw_pattern="task 6 duplicate",
                canonical_name="task 6 duplicate",
                match_type="contains",
            ),
        ]
    )

    assert response.error is not None
    assert response.error.code == error_codes.MUTATION_AMBIGUOUS
    assert response.error.details["candidate_ids"] == sorted(candidate_ids)
    assert not any(
        row.category == "Task 6 Merchant Must Not Persist"
        for row in (await taxonomy_coarse(view="categories")).data.rows
    )


async def test_taxonomy_preflight_composes_merchant_and_category_removal() -> None:
    created = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="present",
                category="Task 6 Composed",
            ),
            MerchantStateRequest(
                kind="merchant",
                state="present",
                raw_pattern="TASK 6 COMPOSED",
                canonical_name="Task 6 Composed",
                category="Task 6 Composed",
            ),
        ]
    )
    category_id = created.data.results[0].target_id
    merchant_id = created.data.results[1].target_id
    assert category_id is not None
    assert merchant_id is not None
    requests: list[CategoryStateRequest | MerchantStateRequest] = [
        MerchantStateRequest(
            kind="merchant",
            state="absent",
            merchant_id=merchant_id,
        ),
        CategoryStateRequest(
            kind="category",
            state="absent",
            category_id=category_id,
        ),
    ]

    first = await taxonomy_set_coarse(items=requests)
    assert first.error is not None
    assert first.error.code == error_codes.MUTATION_CONFIRMATION_REQUIRED
    assert first.error.details["blast_radius"] == {
        "targets": 2,
        "changed_targets": 2,
        "explicit_hard_deletes": 2,
        "cascade_hard_deletes": 0,
        "hard_deletes": 2,
    }
    removed = await taxonomy_set_coarse(
        items=requests,
        confirmation_token=first.error.details["confirmation_token"],
    )

    assert removed.error is None
    assert [result.changed for result in removed.data.results] == [True, True]


async def test_taxonomy_delete_rejects_token_after_live_state_changes() -> None:
    created = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="present",
                category="Task 6 Stale",
            )
        ]
    )
    category_id = created.data.results[0].target_id
    assert category_id is not None
    first = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="absent",
                category_id=category_id,
            )
        ]
    )
    assert first.error is not None

    inactive = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="inactive",
                category_id=category_id,
            )
        ]
    )
    assert inactive.error is None
    stale = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="absent",
                category_id=category_id,
            )
        ],
        confirmation_token=first.error.details["confirmation_token"],
    )

    assert stale.error is not None
    assert stale.error.code == error_codes.MUTATION_CONFIRMATION_MISMATCH


async def test_taxonomy_all_noop_batch_returns_nothing_to_do() -> None:
    response = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="absent",
                category_id="missing-category",
            ),
            MerchantStateRequest(
                kind="merchant",
                state="absent",
                merchant_id="missing-merchant",
            ),
        ]
    )

    assert response.error is not None
    assert response.error.code == error_codes.MUTATION_NOTHING_TO_DO


async def test_taxonomy_write_registrar_advertises_maximum_destructive_risk() -> None:
    mcp = isolated_server(register_taxonomy_coarse_writes)

    tools = await mcp._list_tools()  # pyright: ignore[reportPrivateUsage]
    assert {tool.name for tool in tools} == {"taxonomy_set"}
    tool = await listed_tool(mcp, "taxonomy_set")
    assert tool.output_schema is None
    assert tool.annotations is not None
    assert tool.annotations.destructive_hint is True
    variants = tool.input_schema["properties"]["items"]["items"]["oneOf"]
    assert {variant["properties"]["kind"]["const"] for variant in variants} == {
        "category",
        "merchant",
        "source_category",
    }


async def test_taxonomy_explicit_category_id_must_match_target_fields() -> None:
    created = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="present",
                category="Task 6 Original",
            )
        ]
    )
    category_id = created.data.results[0].target_id
    assert category_id is not None

    mismatch = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="present",
                category_id=category_id,
                category="Task 6 Different",
            )
        ]
    )

    assert mismatch.error is not None
    assert mismatch.error.code == error_codes.MUTATION_INVALID_INPUT


async def test_taxonomy_rejects_merchant_target_for_removed_category() -> None:
    created = await taxonomy_set_coarse(
        items=[
            CategoryStateRequest(
                kind="category",
                state="present",
                category="Task 6 Contradiction",
            )
        ]
    )
    category_id = created.data.results[0].target_id
    assert category_id is not None

    response = await taxonomy_set_coarse(
        items=[
            MerchantStateRequest(
                kind="merchant",
                state="present",
                raw_pattern="TASK 6 CONTRADICTION",
                canonical_name="Task 6 Contradiction",
                category="Task 6 Contradiction",
            ),
            CategoryStateRequest(
                kind="category",
                state="absent",
                category_id=category_id,
            ),
        ]
    )

    assert response.error is not None
    assert response.error.code == error_codes.MUTATION_INVALID_INPUT
    assert not any(
        row.raw_pattern == "TASK 6 CONTRADICTION"
        for row in (await taxonomy_coarse(view="merchants")).data.rows
    )


def _category(
    category_id: str,
    category: str,
    *,
    subcategory: str | None = None,
    is_active: bool = True,
) -> CategoryRow:
    return CategoryRow(
        category_id=category_id,
        category=category,
        subcategory=subcategory,
        description=f"{category} description",
        is_default=True,
        is_active=is_active,
    )


def _merchant(merchant_id: str, canonical_name: str, raw_pattern: str) -> MerchantRow:
    return MerchantRow(
        merchant_id=merchant_id,
        raw_pattern=raw_pattern,
        match_type="contains",
        canonical_name=canonical_name,
        category="Food",
        subcategory=None,
    )


def _taxonomy_payload(
    view: Literal["categories", "merchants"],
    ids: list[str],
) -> CategoriesPayload | MerchantsPayload:
    """Build minimal rows whose stable pagination keys use ``ids``."""
    if view == "categories":
        return CategoriesPayload(
            categories=[_category(row_id, row_id) for row_id in ids]
        )
    return MerchantsPayload(
        merchants=[_merchant(row_id, row_id, row_id) for row_id in ids]
    )


def _taxonomy_method(view: Literal["categories", "merchants"]) -> str:
    """Return the service method backing one taxonomy view."""
    return "get_all_categories" if view == "categories" else "list_merchants"


def _taxonomy_result_id(
    view: Literal["categories", "merchants"],
    row: object,
) -> str:
    """Read the stable pagination id from one surfaced taxonomy row."""
    attribute = "category_id" if view == "categories" else "merchant_id"
    return str(getattr(row, attribute))


async def test_taxonomy_filters_sorts_and_paginates_exactly() -> None:
    payload = CategoriesPayload(
        categories=[
            _category("cat-b", "Travel"),
            _category("cat-c", "Food", subcategory="Coffee"),
            _category("cat-a", "Food", subcategory="Coffee"),
        ]
    )
    with patch.object(
        CategorizationService,
        "get_all_categories",
        return_value=payload,
    ):
        first = await taxonomy_coarse(
            view="categories",
            query=" coffee ",
            limit=1,
        )
        assert [row.category_id for row in first.data.rows] == ["cat-a"]
        assert first.summary.total_count == 2
        assert first.summary.returned_count == 1
        assert first.summary.has_more is True
        assert first.next_cursor is not None
        assert (
            "taxonomy(view='categories', include_inactive=False, query=' coffee ', "
            f"limit=1, cursor='{first.next_cursor}')"
        ) in " ".join(first.actions)

        second = await taxonomy_coarse(
            view="categories",
            query=" coffee ",
            limit=1,
            cursor=first.next_cursor,
        )
        assert [row.category_id for row in second.data.rows] == ["cat-c"]
        assert second.summary.has_more is False

        incompatible = await taxonomy_coarse(
            view="categories",
            query="travel",
            limit=1,
            cursor=first.next_cursor,
        )
        assert incompatible.error is not None
        assert incompatible.error.code == "taxonomy_cursor_invalid"


async def test_taxonomy_merchants_filter_is_deterministic() -> None:
    payload = MerchantsPayload(
        merchants=[
            _merchant("merchant-b", "Coffee Shop", "coffee"),
            _merchant("merchant-a", "Coffee Shop", "coffee"),
            _merchant("merchant-c", "Grocer", "market"),
        ]
    )
    with patch.object(CategorizationService, "list_merchants", return_value=payload):
        response = await taxonomy_coarse(view="merchants", query="COFFEE")

    assert [row.merchant_id for row in response.data.rows] == [
        "merchant-a",
        "merchant-b",
    ]
    assert response.summary.total_count == 2


@pytest.mark.parametrize("view", ["categories", "merchants"])
async def test_taxonomy_continuation_survives_first_page_removal(
    view: Literal["categories", "merchants"],
) -> None:
    with patch.object(
        CategorizationService,
        _taxonomy_method(view),
        side_effect=[
            _taxonomy_payload(view, ["item-b", "item-c"]),
            _taxonomy_payload(view, ["item-c"]),
        ],
    ):
        first = await taxonomy_coarse(view=view, limit=1)
        second = await taxonomy_coarse(
            view=view,
            limit=1,
            cursor=first.next_cursor,
        )

    assert [
        _taxonomy_result_id(view, first.data.rows[0]),
        _taxonomy_result_id(view, second.data.rows[0]),
    ] == ["item-b", "item-c"]
    assert first.summary.total_count == 2
    assert second.summary.total_count == 2
    assert second.next_cursor is None


@pytest.mark.parametrize("view", ["categories", "merchants"])
async def test_taxonomy_continuation_excludes_prepended_row(
    view: Literal["categories", "merchants"],
) -> None:
    with patch.object(
        CategorizationService,
        _taxonomy_method(view),
        side_effect=[
            _taxonomy_payload(view, ["item-b", "item-c"]),
            _taxonomy_payload(view, ["item-a", "item-b", "item-c"]),
        ],
    ):
        first = await taxonomy_coarse(view=view, limit=1)
        second = await taxonomy_coarse(
            view=view,
            limit=1,
            cursor=first.next_cursor,
        )

    assert [
        _taxonomy_result_id(view, first.data.rows[0]),
        _taxonomy_result_id(view, second.data.rows[0]),
    ] == ["item-b", "item-c"]
    assert first.summary.total_count == 2
    assert second.summary.total_count == 2
    assert second.next_cursor is None


@pytest.mark.parametrize("view", ["categories", "merchants"])
async def test_taxonomy_continuation_excludes_appended_row(
    view: Literal["categories", "merchants"],
) -> None:
    with patch.object(
        CategorizationService,
        _taxonomy_method(view),
        side_effect=[
            _taxonomy_payload(view, ["item-b", "item-c"]),
            _taxonomy_payload(view, ["item-b", "item-c", "item-z"]),
        ],
    ):
        first = await taxonomy_coarse(view=view, limit=1)
        second = await taxonomy_coarse(
            view=view,
            limit=1,
            cursor=first.next_cursor,
        )

    assert [
        _taxonomy_result_id(view, first.data.rows[0]),
        _taxonomy_result_id(view, second.data.rows[0]),
    ] == ["item-b", "item-c"]
    assert first.summary.total_count == 2
    assert second.summary.total_count == 2
    assert second.next_cursor is None


@pytest.mark.parametrize("view", ["categories", "merchants"])
async def test_taxonomy_rejects_typed_key_shape_before_reading_live_rows(
    view: Literal["categories", "merchants"],
) -> None:
    cursor = encode_keyset_cursor(
        namespace="taxonomy",
        scope={
            "include_inactive": False,
            "query": None,
            "view": view,
        },
        snapshot=(1,),
        after=(2,),
        total=2,
    )
    with patch.object(CategorizationService, _taxonomy_method(view)) as read:
        response = await taxonomy_coarse(view=view, cursor=cursor)

    assert response.error is not None
    assert response.error.code == "taxonomy_cursor_invalid"
    read.assert_not_called()


@pytest.mark.parametrize("view", ["categories", "merchants"])
async def test_taxonomy_rejects_after_beyond_snapshot_before_reading_live_rows(
    view: Literal["categories", "merchants"],
) -> None:
    cursor = encode_keyset_cursor(
        namespace="taxonomy",
        scope={
            "include_inactive": False,
            "query": None,
            "view": view,
        },
        snapshot=("item-b",),
        after=("item-c",),
        total=2,
    )
    with patch.object(CategorizationService, _taxonomy_method(view)) as read:
        response = await taxonomy_coarse(view=view, cursor=cursor)

    assert response.error is not None
    assert response.error.code == "taxonomy_cursor_invalid"
    read.assert_not_called()


async def test_taxonomy_cursor_is_bound_to_view() -> None:
    payload = CategoriesPayload(
        categories=[
            _category("item-a", "First"),
            _category("item-b", "Second"),
        ]
    )
    with patch.object(
        CategorizationService,
        "get_all_categories",
        return_value=payload,
    ):
        first = await taxonomy_coarse(view="categories", limit=1)

    with patch.object(CategorizationService, "list_merchants") as read:
        response = await taxonomy_coarse(
            view="merchants",
            limit=1,
            cursor=first.next_cursor,
        )

    assert response.error is not None
    assert response.error.code == "taxonomy_cursor_invalid"
    read.assert_not_called()


@pytest.mark.parametrize(
    ("view", "expected_kind"),
    [
        ("categories", "category"),
        ("merchants", "merchant"),
    ],
)
async def test_taxonomy_actions_name_valid_set_kinds(
    view: Literal["categories", "merchants"],
    expected_kind: str,
) -> None:
    response = await taxonomy_coarse(view=view)

    assert response.actions[0] == (
        f"Use taxonomy_set with kind='{expected_kind}' to maintain this taxonomy"
    )


async def test_taxonomy_rejects_category_only_argument_for_merchants() -> None:
    response = await taxonomy_coarse(view="merchants", include_inactive=True)

    assert response.error is not None
    assert response.error.code == "taxonomy_include_inactive_not_allowed"


async def test_taxonomy_standard_registrar_renders_closed_contract() -> None:
    mcp = isolated_server(register_taxonomy_coarse_reads)

    tools = await mcp._list_tools()  # pyright: ignore[reportPrivateUsage]
    assert {tool.name for tool in tools} == {"taxonomy"}
    tool = await listed_tool(mcp, "taxonomy")
    assert tool.output_schema is None
    assert tool.input_schema["properties"]["include_inactive"]["type"] == "boolean"
    assert_literal_values(
        tool.input_schema,
        ("properties", "view"),
        {"categories", "merchants"},
    )


@pytest.mark.parametrize(
    ("view", "expected_sensitivity", "expected_classes"),
    [
        (
            "categories",
            "low",
            ["category", "txn_type"],
        ),
        (
            "merchants",
            "medium",
            ["category", "merchant_name", "record_id", "txn_type"],
        ),
    ],
)
async def test_taxonomy_raw_transport_is_canonical_and_uses_public_actor(
    view: str,
    expected_sensitivity: str,
    expected_classes: list[str],
) -> None:
    captured: list[dict[str, Any]] = []
    mcp = isolated_server(register_taxonomy_coarse_reads)

    with patch("moneybin.mcp.decorator.write_privacy_event", captured.append):
        response = await call_tool_raw(mcp, "taxonomy", {"view": view})

    text = response.content[0]
    assert hasattr(text, "text")
    assert response.structured_content is not None
    assert json.loads(text.text) == response.structured_content  # type: ignore[union-attr]
    assert response.structured_content["data"]["kind"] == view
    assert len(captured) == 1
    assert captured[0]["actor"] == "mcp.taxonomy"
    assert captured[0]["sensitivity"] == expected_sensitivity
    assert captured[0]["classes_returned"] == expected_classes


async def test_taxonomy_cursor_error_is_canonical_and_sanitized() -> None:
    mcp = isolated_server(register_taxonomy_coarse_reads)
    invalid_cursor = "secret-merchant-1234"

    response = await call_tool_raw(
        mcp,
        "taxonomy",
        {"view": "merchants", "cursor": invalid_cursor},
    )

    text = response.content[0]
    assert hasattr(text, "text")
    assert response.structured_content is not None
    assert json.loads(text.text) == response.structured_content  # type: ignore[union-attr]
    assert response.structured_content["error"]["code"] == "taxonomy_cursor_invalid"
    assert invalid_cursor not in text.text  # type: ignore[union-attr]


@pytest.mark.parametrize(
    "arguments",
    [
        {"view": "unknown"},
        {"include_inactive": "false"},
        {"limit": "50"},
        {"unknown": "value"},
    ],
)
async def test_taxonomy_raw_transport_rejects_invalid_arguments(
    arguments: dict[str, Any],
) -> None:
    mcp = isolated_server(register_taxonomy_coarse_reads)

    response = await call_tool_raw(mcp, "taxonomy", arguments)

    assert response.is_error is True
