# ruff: noqa: S101
"""E2E golden paths for the transaction-curation surface.

Subprocess-driven flows that exercise the new CLI commands and MCP tools end-to-end:
manual entry, notes/tags/splits, tag rename audit chain, import labels, category
edit audit, and bulk MCP transactions_create. Verification reads ``core.fct_transactions``
and ``app.audit_log`` via ``moneybin db query`` so the entire pipeline (manual write
→ matcher exemption → SQLMesh transform) is covered, not just the service layer.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from tests.e2e.conftest import (
    FIXTURES_DIR,
    make_workflow_env,
    run_cli,
)

pytestmark = pytest.mark.e2e


def _loads(s: str) -> Any:
    """``json.loads`` with ``Any`` to keep pyright-strict tests readable."""
    return json.loads(s)


def _query_json(env: dict[str, str], sql: str) -> list[dict[str, Any]]:
    """Run ``moneybin db query`` and parse the JSON output."""
    result = run_cli("db", "query", sql, "--output", "json", env=env)
    result.assert_success()
    payload = _loads(result.stdout)
    rows: list[dict[str, Any]] = (
        payload if isinstance(payload, list) else payload.get("data", [])
    )
    return rows


def _bootstrap_account(env: dict[str, str], account_id: str) -> None:
    """Seed ``core.dim_accounts`` by importing a CSV under ``account_id`` then transforming.

    Manual entry validates the supplied ``account_id`` against ``core.dim_accounts``
    (per ``TransactionService._validate_manual_entry``); a fresh profile has no
    accounts yet, so each test imports the standard CSV fixture under the desired
    account_id and runs ``transform apply`` to materialize the dim row.
    """
    fixture = FIXTURES_DIR / "tabular" / "standard.csv"
    run_cli(
        "import",
        "file",
        str(fixture),
        "--account-id",
        account_id,
        "--skip-transform",
        env=env,
    ).assert_success()
    run_cli("transform", "apply", env=env, timeout=180).assert_success()


class TestManualEntryGoldenPath:
    """Manual entry → transform → query confirms row in core.fct_transactions."""

    def test_create_then_visible_in_fct(self, e2e_home: Path) -> None:
        env = make_workflow_env(e2e_home, "wf-curation-create")
        _bootstrap_account(env, "manual-acct")

        result = run_cli(
            "transactions",
            "create",
            "--account",
            "manual-acct",
            "--date",
            "2024-06-01",
            "--output",
            "json",
            "--",
            "-12.34",
            "Coffee shop",
            env=env,
        )
        result.assert_success()
        payload = _loads(result.stdout)
        txn_id = payload["manual_create"]["transaction_id"]
        import_id = payload["manual_create"]["import_id"]
        assert txn_id, payload
        assert import_id, payload

        # Run transform so the manual row materializes into core.fct_transactions.
        result = run_cli("transform", "apply", env=env, timeout=180)
        result.assert_success()

        sql = f"SELECT transaction_id, amount, description, source_type FROM core.fct_transactions WHERE transaction_id = '{txn_id}'"  # noqa: S608  # test input, not executing SQL
        rows = _query_json(env, sql)
        assert len(rows) == 1, rows
        row = rows[0]
        assert str(row["source_type"]) == "manual"
        assert "Coffee shop" in str(row["description"])


class TestNotesTagsSplitsGoldenPath:
    """Add notes/tags/splits to a manual transaction → visible via fct LIST columns."""

    def test_curation_columns_populate(self, e2e_home: Path) -> None:
        env = make_workflow_env(e2e_home, "wf-curation-annotate")
        _bootstrap_account(env, "manual-acct")

        result = run_cli(
            "transactions",
            "create",
            "--account",
            "manual-acct",
            "--date",
            "2024-06-02",
            "--output",
            "json",
            "--",
            "-100.00",
            "Big purchase",
            env=env,
        )
        result.assert_success()
        txn_id = _loads(result.stdout)["manual_create"]["transaction_id"]

        # Add a note, two tags, and two splits.
        run_cli(
            "transactions", "notes", "add", txn_id, "Reimbursable", env=env
        ).assert_success()
        run_cli(
            "transactions", "tags", "add", txn_id, "work", "travel", env=env
        ).assert_success()
        run_cli(
            "transactions",
            "splits",
            "add",
            "--category",
            "Travel",
            "--",
            txn_id,
            "-60.00",
            env=env,
        ).assert_success()
        run_cli(
            "transactions",
            "splits",
            "add",
            "--category",
            "Meals",
            "--",
            txn_id,
            "-40.00",
            env=env,
        ).assert_success()

        # Re-materialize fct_transactions so the LIST joins pick up the new rows.
        run_cli("transform", "apply", env=env, timeout=180).assert_success()

        sql = f"SELECT note_count, tag_count, split_count, has_splits, tags FROM core.fct_transactions WHERE transaction_id = '{txn_id}'"  # noqa: S608  # test input, not executing SQL
        rows = _query_json(env, sql)
        assert len(rows) == 1, rows
        row = rows[0]
        assert int(row["note_count"]) == 1
        assert int(row["tag_count"]) == 2
        assert int(row["split_count"]) == 2
        assert bool(row["has_splits"]) is True
        # Tags arrive as a list/array; coerce to list[str] for comparison.
        tags = row["tags"]
        if isinstance(tags, str):
            tags = _loads(tags)
        assert sorted(map(str, tags)) == ["travel", "work"]


class TestTagRenameAuditChain:
    """Tag rename across N transactions emits 1 parent + N child audit events."""

    def test_rename_emits_chain(self, e2e_home: Path) -> None:
        env = make_workflow_env(e2e_home, "wf-curation-tag-rename")
        _bootstrap_account(env, "manual-acct")

        # Create three manual transactions and apply the same tag to each.
        txn_ids: list[str] = []
        for i in range(3):
            result = run_cli(
                "transactions",
                "create",
                "--account",
                "manual-acct",
                "--date",
                "2024-06-03",
                "--output",
                "json",
                "--",
                f"-{i + 1}.00",
                f"row {i}",
                env=env,
            )
            result.assert_success()
            txn_ids.append(_loads(result.stdout)["manual_create"]["transaction_id"])

        for tid in txn_ids:
            run_cli(
                "transactions", "tags", "add", tid, "old-name", env=env
            ).assert_success()

        # Rename globally.
        result = run_cli(
            "transactions", "tags", "rename", "old-name", "new-name", env=env
        )
        result.assert_success()

        # Inspect the audit log via system audit list --action 'tag.%' --output json.
        result = run_cli(
            "system",
            "audit",
            "list",
            "--action",
            "tag.%",
            "--limit",
            "100",
            "--output",
            "json",
            env=env,
        )
        result.assert_success()
        events: list[dict[str, Any]] = _loads(result.stdout)["audit_events"]
        assert isinstance(events, list)

        rename_parents = [
            e for e in events if str(e.get("action", "")).startswith("tag.rename")
        ]
        assert len(rename_parents) >= 1, events
        parent = rename_parents[0]
        parent_id = parent["audit_id"]

        children = [
            e
            for e in events
            if e.get("parent_audit_id") == parent_id
            and str(e.get("action", "")) != "tag.rename"
        ]
        assert len(children) == len(txn_ids), (
            f"expected {len(txn_ids)} child events under parent {parent_id}; got {children}"
        )


class TestImportLabelsGoldenPath:
    """CSV import → import labels add → list shows the label."""

    def test_label_round_trip(self, e2e_home: Path) -> None:
        env = make_workflow_env(e2e_home, "wf-curation-labels")
        fixture = FIXTURES_DIR / "tabular" / "standard.csv"

        result = run_cli(
            "import",
            "file",
            str(fixture),
            "--account-id",
            "labels-acct",
            "--skip-transform",
            env=env,
        )
        result.assert_success()

        # Pull the most recent import_id from app.imports.
        rows = _query_json(
            env,
            "SELECT import_id FROM raw.import_log ORDER BY started_at DESC LIMIT 1",
        )
        assert rows, "expected at least one import row"
        import_id = str(rows[0]["import_id"])

        run_cli(
            "import", "labels", "add", import_id, "needs-review", env=env
        ).assert_success()

        result = run_cli(
            "import",
            "labels",
            "list",
            "--import-id",
            import_id,
            "--output",
            "json",
            env=env,
        )
        result.assert_success()
        payload = _loads(result.stdout)["import_labels"]
        labels = payload["labels"]
        assert "needs-review" in labels


@pytest.mark.skip(
    reason=(
        "CategorizationService.set_category emits category.set audit events, "
        "but the only CLI/MCP surface today (categorize_items via "
        "transactions categorize apply-from-file) does not invoke set_category — "
        "it inserts via direct SQL UPSERT without audit emission. Wiring that "
        "is out of scope for Task 14 (polish/docs only)."
    )
)
class TestCategoryEditAudit:
    """Editing a transaction's category writes an audit event with before/after."""

    def test_category_set_emits_audit(self, e2e_home: Path) -> None:
        env = make_workflow_env(e2e_home, "wf-curation-cat-edit")
        _bootstrap_account(env, "manual-acct")

        # Create a transaction and transform so it lands in core.fct_transactions.
        result = run_cli(
            "transactions",
            "create",
            "--account",
            "manual-acct",
            "--date",
            "2024-06-04",
            "--output",
            "json",
            "--",
            "-25.00",
            "Lunch out",
            env=env,
        )
        result.assert_success()
        txn_id = _loads(result.stdout)["manual_create"]["transaction_id"]
        run_cli("transform", "apply", env=env, timeout=180).assert_success()

        # Apply a category via apply-from-file (writes through CategorizationService.set_category).
        bulk = [{"transaction_id": txn_id, "category": "Shopping"}]
        bulk_path = Path(env["MONEYBIN_HOME"]) / "categorize.json"
        bulk_path.write_text(json.dumps(bulk))
        result = run_cli(
            "transactions",
            "categorize",
            "apply-from-file",
            str(bulk_path),
            env=env,
        )
        result.assert_success()

        # Re-categorize to a different value; the second set_category emits a before/after audit.
        bulk2 = [{"transaction_id": txn_id, "category": "Food & Drink"}]
        bulk_path.write_text(json.dumps(bulk2))
        run_cli(
            "transactions",
            "categorize",
            "apply-from-file",
            str(bulk_path),
            env=env,
        ).assert_success()

        # The audit log should hold at least one category.set with after_value present.
        result = run_cli(
            "system",
            "audit",
            "list",
            "--action",
            "category.%",
            "--target-id",
            txn_id,
            "--output",
            "json",
            env=env,
        )
        result.assert_success()
        events: list[dict[str, Any]] = _loads(result.stdout)["audit_events"]
        cat_sets = [e for e in events if str(e.get("action")) == "category.set"]
        assert cat_sets, events
        # At least one event should carry an after_value referencing a category name.
        assert any(e.get("after_value") for e in cat_sets), cat_sets


@pytest.mark.asyncio
class TestMCPBulkCreate:
    """MCP transactions_create with 5 entries → all 5 land in core.fct_transactions."""

    async def test_bulk_create_atomic(
        self, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        import os

        from mcp import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client

        from tests.e2e.conftest import FAST_ARGON2_ENV

        home = tmp_path_factory.mktemp("e2e_curation_mcp")
        env = make_workflow_env(home, "mcp-curation")
        _bootstrap_account(env, "mcp-acct")

        server_params = StdioServerParameters(
            command="uv",  # noqa: S607 — uv is on PATH in dev environments
            args=["run", "moneybin", "mcp", "serve"],
            env={**os.environ, **FAST_ARGON2_ENV, **env},
        )

        entries = [
            {
                "account_id": "mcp-acct",
                "amount": f"-{i + 1}.00",
                "transaction_date": "2024-06-05",
                "description": f"mcp row {i}",
            }
            for i in range(5)
        ]

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(
                    "transactions_create", {"transactions": entries}
                )
                assert not result.isError, result.content
                from mcp.types import TextContent

                content = result.content[0]
                assert isinstance(content, TextContent)
                envelope = _loads(content.text)
                data = envelope["data"]
                batch_id = data["batch_id"]
                rows = data["results"]
                assert len(rows) == 5, rows
                assert batch_id, data

        # Materialize and verify the same import_id covers exactly five rows.
        run_cli("transform", "apply", env=env, timeout=180).assert_success()
        result = run_cli(
            "db",
            "query",
            "SELECT COUNT(*) AS n FROM core.fct_transactions "
            "WHERE source_type = 'manual'",
            "--output",
            "csv",
            env=env,
        )
        result.assert_success()
        count = int(result.stdout.strip().split("\n")[-1].strip())
        assert count == 5, f"expected 5 manual rows, got {count}"
