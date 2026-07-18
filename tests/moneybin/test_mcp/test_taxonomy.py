"""Tests for the dormant normalized taxonomy read."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import patch

import pytest

from moneybin.mcp.tools.taxonomy import (
    register_taxonomy_coarse_reads,
    taxonomy_coarse,
)
from moneybin.privacy.payloads.categories import (
    CategoriesPayload,
    CategoryRow,
    MerchantRow,
    MerchantsPayload,
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
        assert incompatible.error.code == "TAXONOMY_CURSOR_INVALID"


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


async def test_taxonomy_rejects_category_only_argument_for_merchants() -> None:
    response = await taxonomy_coarse(view="merchants", include_inactive=True)

    assert response.error is not None
    assert response.error.code == "TAXONOMY_INCLUDE_INACTIVE_NOT_ALLOWED"


async def test_taxonomy_dormant_registrar_renders_closed_contract() -> None:
    mcp = isolated_server(register_taxonomy_coarse_reads)

    tools = await mcp._list_tools()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert {tool.name for tool in tools} == {"taxonomy"}
    tool = await listed_tool(mcp, "taxonomy")
    assert tool.outputSchema is None
    assert tool.inputSchema["properties"]["include_inactive"]["type"] == "boolean"
    assert_literal_values(
        tool.inputSchema,
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
    assert response.structuredContent is not None
    assert json.loads(text.text) == response.structuredContent  # type: ignore[union-attr]
    assert response.structuredContent["data"]["kind"] == view
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
    assert response.structuredContent is not None
    assert json.loads(text.text) == response.structuredContent  # type: ignore[union-attr]
    assert response.structuredContent["error"]["code"] == "TAXONOMY_CURSOR_INVALID"
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

    assert response.isError is True
