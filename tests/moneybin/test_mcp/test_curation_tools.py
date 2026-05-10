"""Tests for the curation MCP tools (notes, tags, splits, manual entry, audit).

Verifies tool registration, envelope shape, and end-to-end happy paths
against an in-process MCP DB. Service-layer correctness is tested
separately under ``tests/moneybin/test_services/``; these tests cover
the MCP wrapper plumbing — coercion, envelope construction, registration.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest
from fastmcp import FastMCP

from moneybin.mcp.tools.curation import (
    import_labels_set,
    register_curation_tools,
    system_audit_list,
    transactions_create,
    transactions_notes_add,
    transactions_notes_delete,
    transactions_notes_edit,
    transactions_splits_set,
    transactions_tags_rename,
    transactions_tags_set,
)
from moneybin.services.audit_service import AuditService

if TYPE_CHECKING:
    from moneybin.database import Database

pytestmark = pytest.mark.usefixtures("mcp_db")


# ---------- helpers ----------


def _seed_transaction(
    db: Database,
    transaction_id: str,
    *,
    account_id: str = "ACC001",
    amount: str = "-12.34",
    description: str = "Coffee Shop",
    transaction_date: str = "2026-04-10",
) -> None:
    """Insert one minimal core.fct_transactions row for tests that need a target."""
    db.execute(
        """
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            amount_absolute, transaction_direction, description,
            transaction_type, is_pending, currency_code, source_type,
            source_extracted_at, loaded_at,
            transaction_year, transaction_month, transaction_day,
            transaction_day_of_week, transaction_year_month,
            transaction_year_quarter
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'DEBIT', false, 'USD', 'manual',
                  ?, CURRENT_TIMESTAMP, 2026, 4, 10, 3, '2026-04', '2026-Q2')
        """,
        [
            transaction_id,
            account_id,
            transaction_date,
            Decimal(amount),
            abs(Decimal(amount)),
            "expense" if Decimal(amount) < 0 else "income",
            description,
            transaction_date,
        ],
    )


def _seed_import(db: Database, import_id: str = "IMP_TEST_001") -> str:
    """Insert one raw.import_log row that import_labels_set can attach to."""
    db.execute(
        """
        INSERT INTO raw.import_log (
            import_id, source_file, source_type, source_origin,
            format_name, account_names, status, rows_total, rows_imported
        ) VALUES (?, 'inline', 'manual', 'manual',
                  'manual', '[]'::JSON, 'complete', 0, 0)
        """,
        [import_id],
    )
    return import_id


# ---------- registration ----------


class TestCurationToolRegistration:
    """All curation tools register against a fresh FastMCP server."""

    @pytest.mark.unit
    async def test_all_tools_register(self) -> None:
        srv = FastMCP("test")
        register_curation_tools(srv)
        names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        assert {
            "transactions_create",
            "transactions_notes_add",
            "transactions_notes_edit",
            "transactions_notes_delete",
            "transactions_tags_set",
            "transactions_tags_rename",
            "transactions_splits_set",
            "import_labels_set",
            "system_audit_list",
        } <= names


# ---------- transactions_create ----------


class TestTransactionsCreate:
    """Bulk manual entry: atomic batches and validation guards."""

    @pytest.mark.unit
    async def test_bulk_atomic_returns_batch_id_and_results(
        self, mcp_db: Database
    ) -> None:
        env = (
            await transactions_create(
                transactions=[
                    {
                        "account_id": "ACC001",
                        "amount": "-4.50",
                        "description": "Coffee",
                        "transaction_date": "2026-03-14",
                    },
                    {
                        "account_id": "ACC001",
                        "amount": "-10.00",
                        "description": "Lunch",
                        "transaction_date": "2026-03-14",
                    },
                ],
            )
        ).to_dict()
        assert env["summary"]["sensitivity"] == "medium"
        data: dict[str, Any] = dict(env["data"])
        assert data["batch_id"]
        results: list[dict[str, Any]] = list(data["results"])
        assert len(results) == 2
        assert all("transaction_id" in r for r in results)

    @pytest.mark.unit
    async def test_rejects_size_above_100(self, mcp_db: Database) -> None:
        too_many = [
            {
                "account_id": "ACC001",
                "amount": "-1.00",
                "description": f"x{i}",
                "transaction_date": "2026-03-14",
            }
            for i in range(101)
        ]
        env = (await transactions_create(transactions=too_many)).to_dict()
        assert env.get("error") is not None
        assert "batch size" in env["error"]["message"].lower()


# ---------- notes ----------


class TestNotesLifecycle:
    """Add / edit / delete a note end-to-end through the MCP surface."""

    @pytest.mark.unit
    async def test_add_edit_delete_roundtrip(self, mcp_db: Database) -> None:
        _seed_transaction(mcp_db, "TXN_NOTE_1")

        added = (
            await transactions_notes_add(transaction_id="TXN_NOTE_1", text="hello")
        ).to_dict()
        note_id = added["data"]["note_id"]
        assert added["data"]["text"] == "hello"

        edited = (
            await transactions_notes_edit(note_id=note_id, text="world")
        ).to_dict()
        assert edited["data"]["text"] == "world"

        deleted = (await transactions_notes_delete(note_id=note_id)).to_dict()
        assert deleted["data"] == {"note_id": note_id}


# ---------- tags ----------


class TestTagsSetAndRename:
    """Declarative tag set diffing and global rename."""

    @pytest.mark.unit
    async def test_set_tags_computes_diff(self, mcp_db: Database) -> None:
        _seed_transaction(mcp_db, "TXN_TAG_1")

        first = (
            await transactions_tags_set(transaction_id="TXN_TAG_1", tags=["a", "b"])
        ).to_dict()
        assert sorted(first["data"]["tags"]) == ["a", "b"]

        second = (
            await transactions_tags_set(transaction_id="TXN_TAG_1", tags=["b", "c"])
        ).to_dict()
        assert sorted(second["data"]["tags"]) == ["b", "c"]

        third = (
            await transactions_tags_set(transaction_id="TXN_TAG_1", tags=["b", "c"])
        ).to_dict()
        assert sorted(third["data"]["tags"]) == ["b", "c"]

        # Audit log: one tag.add for 'a' (round 1), 'b' (round 1), 'c' (round 2),
        # one tag.remove for 'a' (round 2). Round 3 is a no-op.
        events = AuditService(mcp_db).list_events(
            target_id="TXN_TAG_1", action_pattern="tag.%", limit=100
        )
        adds = [e for e in events if e.action == "tag.add"]
        removes = [e for e in events if e.action == "tag.remove"]
        assert {(e.after_value or {}).get("tag") for e in adds} == {"a", "b", "c"}
        assert {(e.before_value or {}).get("tag") for e in removes} == {"a"}

    @pytest.mark.unit
    async def test_rename_tag(self, mcp_db: Database) -> None:
        _seed_transaction(mcp_db, "TXN_REN_1")
        _seed_transaction(mcp_db, "TXN_REN_2")
        await transactions_tags_set(transaction_id="TXN_REN_1", tags=["old"])
        await transactions_tags_set(transaction_id="TXN_REN_2", tags=["old"])

        env = (await transactions_tags_rename(old_tag="old", new_tag="new")).to_dict()
        assert env["data"]["row_count"] == 2
        assert env["data"]["parent_audit_id"]


# ---------- splits ----------


class TestSplitsSet:
    """Declarative split replacement."""

    @pytest.mark.unit
    async def test_set_splits_replaces_atomically(self, mcp_db: Database) -> None:
        _seed_transaction(mcp_db, "TXN_SPLIT_1", amount="-30.00")

        env = (
            await transactions_splits_set(
                transaction_id="TXN_SPLIT_1",
                splits=[
                    {"amount": "-10.00", "category": "Food", "note": "lunch"},
                    {"amount": "-20.00", "category": "Travel"},
                ],
            )
        ).to_dict()
        data: list[dict[str, Any]] = list(env["data"])
        assert len(data) == 2
        assert [s["category"] for s in data] == ["Food", "Travel"]
        assert all(s["amount"] for s in data)


# ---------- import labels ----------


class TestImportLabelsSet:
    """Declarative import-label replacement."""

    @pytest.mark.unit
    async def test_set_labels_returns_canonical_list(self, mcp_db: Database) -> None:
        import_id = _seed_import(mcp_db)

        first = (
            await import_labels_set(import_id=import_id, labels=["q1", "needs-review"])
        ).to_dict()
        assert sorted(first["data"]["labels"]) == ["needs-review", "q1"]

        # Declarative replace.
        second = (
            await import_labels_set(import_id=import_id, labels=["needs-review"])
        ).to_dict()
        assert second["data"]["labels"] == ["needs-review"]


# ---------- audit list ----------


class TestSystemAuditList:
    """Audit log query tool."""

    @pytest.mark.unit
    async def test_filters_by_action_pattern(self, mcp_db: Database) -> None:
        _seed_transaction(mcp_db, "TXN_AUDIT_1")
        await transactions_tags_set(transaction_id="TXN_AUDIT_1", tags=["alpha"])
        await transactions_notes_add(transaction_id="TXN_AUDIT_1", text="hello")

        env = (
            await system_audit_list(filters={"action_pattern": "tag.%"}, limit=50)
        ).to_dict()
        assert env["summary"]["sensitivity"] == "medium"
        events: list[dict[str, Any]] = list(env["data"])
        assert len(events) >= 1
        assert all(str(e["action"]).startswith("tag.") for e in events)
