"""Tests for MCP privacy controls and query validation."""

from pathlib import Path

import pytest

from moneybin.mcp.privacy import (
    check_table_allowed,
    truncate_result,
    validate_managed_write,
    validate_read_only_query,
)
from moneybin.privacy.log import write_privacy_event


async def test_privacy_coarse_status_is_default(mcp_db: object) -> None:
    from moneybin.mcp.tools.privacy import privacy_coarse

    response = await privacy_coarse()

    assert response.data.kind == "status"
    assert response.summary.sensitivity == "low"


@pytest.mark.parametrize(
    ("limit", "cursor"),
    [
        (99, None),
        (100, "opaque"),
    ],
)
async def test_privacy_coarse_status_rejects_pagination_overrides(
    limit: int,
    cursor: str | None,
) -> None:
    from moneybin.mcp.tools.privacy import privacy_coarse

    response = await privacy_coarse(view="status", limit=limit, cursor=cursor)

    assert response.error is not None
    assert response.error.code == "PRIVACY_PAGINATION_NOT_ALLOWED"


async def test_privacy_coarse_log_paginates_exactly_and_preserves_rows(
    mcp_db: object,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from moneybin.mcp.tools.privacy import privacy_coarse

    monkeypatch.setattr(
        "moneybin.privacy.log._resolve_privacy_log_dir",
        lambda: tmp_path,
    )
    for index in range(3):
        write_privacy_event({
            "ts": f"2099-01-01T00:00:0{index}+00:00",
            "actor": f"mcp.tool_{index}",
            "action": "tool_call",
            "sensitivity": "medium",
            "classes_returned": ["description", "record_id"],
            "row_count": index + 1,
        })

    first = await privacy_coarse(view="log", limit=2)

    assert [event.actor for event in first.data.events] == [
        "mcp.tool_2",
        "mcp.tool_1",
    ]
    assert first.data.kind == "log"
    assert first.summary.total_count == 3
    assert first.summary.returned_count == 2
    assert first.next_cursor is not None
    assert first.data.events[0].sensitivity == "medium"
    assert first.data.events[0].classes_returned == [
        "description",
        "record_id",
    ]
    assert any(
        "view='log'" in action and "limit=2" in action and first.next_cursor in action
        for action in first.actions
    )

    second = await privacy_coarse(
        view="log",
        limit=2,
        cursor=first.next_cursor,
    )
    assert [event.actor for event in second.data.events] == ["mcp.tool_0"]
    assert second.summary.total_count == 3
    assert second.summary.returned_count == 1
    assert second.next_cursor is None


async def test_privacy_coarse_log_rejects_malformed_cursor_without_echo() -> None:
    from moneybin.mcp.tools.privacy import privacy_coarse

    cursor_value = "private-cursor-value"
    response = await privacy_coarse(view="log", cursor=cursor_value)

    assert response.error is not None
    assert response.error.code == "PRIVACY_CURSOR_INVALID"
    assert cursor_value not in response.error.message


class TestValidateReadOnlyQuery:
    """Tests for SQL read-only validation."""

    @pytest.mark.unit
    def test_select_allowed(self) -> None:
        assert validate_read_only_query("SELECT * FROM accounts") is None

    @pytest.mark.unit
    def test_with_cte_allowed(self) -> None:
        assert (
            validate_read_only_query("WITH cte AS (SELECT 1) SELECT * FROM cte") is None
        )

    @pytest.mark.unit
    def test_describe_allowed(self) -> None:
        assert validate_read_only_query("DESCRIBE raw.ofx_accounts") is None

    @pytest.mark.unit
    def test_show_allowed(self) -> None:
        assert validate_read_only_query("SHOW TABLES") is None

    @pytest.mark.unit
    def test_pragma_allowed(self) -> None:
        assert validate_read_only_query("PRAGMA database_list") is None

    @pytest.mark.unit
    def test_explain_allowed(self) -> None:
        assert validate_read_only_query("EXPLAIN SELECT 1") is None

    @pytest.mark.unit
    def test_case_insensitive(self) -> None:
        assert validate_read_only_query("select * from t") is None
        assert validate_read_only_query("  SELECT * FROM t") is None

    @pytest.mark.unit
    def test_insert_rejected(self) -> None:
        result = validate_read_only_query("INSERT INTO t VALUES (1)")
        assert result is not None
        assert "read-only" in result.lower() or "Write operations" in result

    @pytest.mark.unit
    def test_update_rejected(self) -> None:
        result = validate_read_only_query("UPDATE t SET x = 1")
        assert result is not None

    @pytest.mark.unit
    def test_delete_rejected(self) -> None:
        result = validate_read_only_query("DELETE FROM t")
        assert result is not None

    @pytest.mark.unit
    def test_drop_rejected(self) -> None:
        result = validate_read_only_query("DROP TABLE t")
        assert result is not None

    @pytest.mark.unit
    def test_create_rejected(self) -> None:
        result = validate_read_only_query("CREATE TABLE t (id INT)")
        assert result is not None

    @pytest.mark.unit
    def test_alter_rejected(self) -> None:
        result = validate_read_only_query("ALTER TABLE t ADD COLUMN x INT")
        assert result is not None

    @pytest.mark.unit
    def test_hidden_write_in_cte_rejected(self) -> None:
        result = validate_read_only_query(
            "WITH cte AS (SELECT 1) INSERT INTO t SELECT * FROM cte"
        )
        assert result is not None

    @pytest.mark.unit
    def test_empty_query_rejected(self) -> None:
        result = validate_read_only_query("")
        assert result is not None

    @pytest.mark.unit
    def test_whitespace_only_rejected(self) -> None:
        result = validate_read_only_query("   ")
        assert result is not None

    @pytest.mark.unit
    def test_copy_rejected(self) -> None:
        result = validate_read_only_query("COPY t TO 'file.csv'")
        assert result is not None

    @pytest.mark.unit
    def test_attach_rejected(self) -> None:
        result = validate_read_only_query("ATTACH 'other.db'")
        assert result is not None

    @pytest.mark.unit
    def test_file_access_functions_rejected(self) -> None:
        for fn in [
            "read_csv",
            "read_parquet",
            "read_json",
            "glob",
            "scan_parquet",
            "scan_csv_auto",
            "scan_json",
            "parquet_scan",
        ]:
            result = validate_read_only_query(f"SELECT * FROM {fn}('data.csv')")  # noqa: S608  # building test input string, not executing SQL
            assert result is not None, f"{fn} should be blocked"
            assert "File-access" in result

    @pytest.mark.unit
    def test_glob_operator_allowed(self) -> None:
        """DuckDB GLOB infix operator must not be blocked by the file-access check."""
        result = validate_read_only_query(
            "SELECT * FROM core.fct_transactions WHERE description GLOB '*AMAZON*'"
        )
        assert result is None

    @pytest.mark.unit
    def test_url_literals_rejected(self) -> None:
        for url in [
            "https://evil.com/data.parquet",
            "http://evil.com/data.parquet",
            "s3://bucket/file.parquet",
            "az://store/container/file",
            "gcs://bucket/file",
        ]:
            result = validate_read_only_query(f"SELECT * FROM '{url}'")  # noqa: S608  # building test input string, not executing SQL
            assert result is not None, f"URL {url!r} should be blocked"
            assert "URL" in result

    @pytest.mark.unit
    def test_quoted_path_scans_rejected(self) -> None:
        for path in [
            "/Users/example/Downloads/transactions.csv",
            "relative/transactions.parquet",
            "~/Downloads/export.json",
        ]:
            result = validate_read_only_query(f"SELECT * FROM '{path}'")  # noqa: S608  # building test input string, not executing SQL
            assert result is not None, f"Path scan {path!r} should be blocked"
            assert "path scans" in result

    @pytest.mark.unit
    def test_quoted_identifiers_and_string_filters_allowed(self) -> None:
        result = validate_read_only_query(
            """
            SELECT * FROM "core"."fct_transactions"
            WHERE description = 'JOIN gym' OR note = 'FROM here'
            """
        )
        assert result is None

    @pytest.mark.unit
    def test_bare_keyword_string_filters_allowed(self) -> None:
        result = validate_read_only_query(
            """
            SELECT * FROM "core"."fct_transactions"
            WHERE action = 'JOIN' AND note = 'something'
            """
        )
        assert result is None


class TestCheckTableAllowed:
    """Tests for table allowlist checking."""

    @pytest.mark.unit
    def test_no_allowlist_allows_all(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from moneybin.mcp import privacy

        monkeypatch.setattr(privacy, "_get_mcp_limits", lambda: (100, 10000, None))
        assert check_table_allowed("any_table") is None

    @pytest.mark.unit
    def test_allowlist_blocks_unlisted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from moneybin.mcp import privacy

        monkeypatch.setattr(
            privacy,
            "_get_mcp_limits",
            lambda: (100, 10000, {"raw.ofx_accounts"}),
        )
        result = check_table_allowed("raw.ofx_transactions")
        assert result is not None
        assert "not in the allowed" in result

    @pytest.mark.unit
    def test_allowlist_permits_listed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from moneybin.mcp import privacy

        monkeypatch.setattr(
            privacy,
            "_get_mcp_limits",
            lambda: (100, 10000, {"raw.ofx_accounts"}),
        )
        assert check_table_allowed("raw.ofx_accounts") is None


class TestTruncateResult:
    """Tests for result truncation."""

    @pytest.mark.unit
    def test_short_text_unchanged(self) -> None:
        text = "short text"
        assert truncate_result(text) == text

    @pytest.mark.unit
    def test_long_text_truncated(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from moneybin.mcp import privacy

        monkeypatch.setattr(privacy, "_get_mcp_limits", lambda: (100, 20, None))
        result = truncate_result("x" * 100)
        # First 20 chars should be the original content
        assert result.startswith("x" * 20)
        # Truncation notice should be appended
        assert "[Result truncated" in result
        # Only the first 20 x's should remain (not all 100)
        assert result.count("x") == 20


class TestValidateManagedWrite:
    """Tests for managed write validation."""

    @pytest.mark.unit
    def test_insert_into_app_schema_allowed(self) -> None:
        assert (
            validate_managed_write(
                "INSERT INTO app.transaction_categories VALUES ('t1', 'Food')"
            )
            is None
        )

    @pytest.mark.unit
    def test_insert_into_raw_schema_allowed(self) -> None:
        assert (
            validate_managed_write(
                "INSERT INTO raw.ofx_transactions VALUES ('t1', 'a1')"
            )
            is None
        )

    @pytest.mark.unit
    def test_update_app_schema_allowed(self) -> None:
        assert (
            validate_managed_write(
                "UPDATE app.budgets SET monthly_amount = 500 WHERE budget_id = 'b1'"
            )
            is None
        )

    @pytest.mark.unit
    def test_drop_rejected(self) -> None:
        result = validate_managed_write("DROP TABLE app.budgets")
        assert result is not None
        assert "DROP" in result

    @pytest.mark.unit
    def test_alter_rejected(self) -> None:
        result = validate_managed_write("ALTER TABLE app.budgets ADD COLUMN x INT")
        assert result is not None

    @pytest.mark.unit
    def test_truncate_rejected(self) -> None:
        result = validate_managed_write("TRUNCATE TABLE app.budgets")
        assert result is not None

    @pytest.mark.unit
    def test_insert_into_core_rejected(self) -> None:
        result = validate_managed_write("INSERT INTO core.dim_accounts VALUES ('x')")
        assert result is not None
        assert "app" in result or "raw" in result

    @pytest.mark.unit
    def test_create_or_replace_in_core_allowed(self) -> None:
        """Core transforms use CREATE OR REPLACE TABLE."""
        assert (
            validate_managed_write(
                "CREATE OR REPLACE TABLE core.dim_accounts AS (SELECT 1)",
                allow_core_transforms=True,
            )
            is None
        )

    @pytest.mark.unit
    def test_create_or_replace_core_rejected_without_flag(self) -> None:
        result = validate_managed_write(
            "CREATE OR REPLACE TABLE core.dim_accounts AS (SELECT 1)"
        )
        assert result is not None
