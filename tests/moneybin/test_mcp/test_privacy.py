"""Tests for MCP privacy controls and query validation."""

from pathlib import Path

import pytest

from moneybin.privacy.log import write_privacy_event
from moneybin.privacy.sensitivity import get_max_rows
from moneybin.privacy.sql_query import validate_read_only_query


async def test_privacy_coarse_status_is_default(mcp_db: object) -> None:
    from moneybin.mcp.tools.privacy import privacy_coarse

    response = await privacy_coarse()

    assert response.data.kind == "status"
    assert response.summary.sensitivity == "low"


def test_get_max_rows_reads_the_current_mcp_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A settings-cache reset takes effect without an MCP-specific cache."""
    from moneybin.config import clear_settings_cache, set_current_profile

    original_limit = get_max_rows()
    monkeypatch.setenv("MONEYBIN_MCP__MAX_ROWS", str(original_limit + 1))
    clear_settings_cache()
    set_current_profile("test")

    assert get_max_rows() == original_limit + 1

    clear_settings_cache()
    set_current_profile("test")


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
    assert response.error.code == "privacy_pagination_not_allowed"


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


async def test_privacy_coarse_log_survives_prepend_and_removal_without_skips(
    mcp_db: object,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from moneybin.mcp.tools.privacy import privacy_coarse

    monkeypatch.setattr(
        "moneybin.privacy.log._resolve_privacy_log_dir",
        lambda: tmp_path,
    )
    for index in range(4):
        write_privacy_event({
            "ts": f"2099-01-01T00:00:0{index}+00:00",
            "actor": f"mcp.tool_{index}",
            "action": "tool_call",
            "sensitivity": "low",
            "classes_returned": [],
            "row_count": index,
        })

    first = await privacy_coarse(view="log", limit=1)
    assert [event.actor for event in first.data.events] == ["mcp.tool_3"]
    assert first.next_cursor is not None

    path = tmp_path / "privacy.log.jsonl"
    events = [
        line for line in path.read_text().splitlines() if "mcp.tool_2" not in line
    ]
    path.write_text("\n".join(events) + "\n")
    write_privacy_event({
        "ts": "2100-01-01T00:00:00+00:00",
        "actor": "mcp.prepended",
        "action": "tool_call",
        "sensitivity": "low",
        "classes_returned": [],
        "row_count": 1,
    })

    second = await privacy_coarse(
        view="log",
        limit=2,
        cursor=first.next_cursor,
    )

    assert [event.actor for event in second.data.events] == [
        "mcp.tool_1",
        "mcp.tool_0",
    ]
    assert second.summary.total_count == 4
    assert second.next_cursor is None


async def test_privacy_coarse_rejects_wrong_key_type_before_log_read(
    mcp_db: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.mcp.tools.privacy import privacy_coarse
    from moneybin.protocol.pagination import encode_keyset_cursor

    cursor = encode_keyset_cursor(
        namespace="privacy.log",
        scope={"filters": {}, "view": "log"},
        snapshot=(1,),
        after=(1,),
        total=1,
    )
    accessed = False

    def fail_if_accessed(*args: object, **kwargs: object) -> object:
        nonlocal accessed
        accessed = True
        raise AssertionError("log read must follow cursor validation")

    monkeypatch.setattr(
        "moneybin.mcp.tools.privacy.read_privacy_events_page",
        fail_if_accessed,
    )

    response = await privacy_coarse(view="log", cursor=cursor)

    assert response.error is not None
    assert response.error.code == "privacy_cursor_invalid"
    assert accessed is False


async def test_privacy_coarse_log_rejects_malformed_cursor_without_echo() -> None:
    from moneybin.mcp.tools.privacy import privacy_coarse

    cursor_value = "private-cursor-value"
    response = await privacy_coarse(view="log", cursor=cursor_value)

    assert response.error is not None
    assert response.error.code == "privacy_cursor_invalid"
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
    @pytest.mark.parametrize(
        "sql", ["PRAGMA database_list", "EXPLAIN SELECT 1", "EXPLAIN ANALYZE SELECT 1"]
    )
    def test_ungateable_statement_refused(self, sql: str) -> None:
        """PRAGMA and EXPLAIN are refused at the prefix gate, before execution.

        Read-only is necessary but not sufficient here. `PRAGMA storage_info`
        only reads, yet its per-segment `stats` are a cleartext prefix of the
        stored values; `EXPLAIN ANALYZE` executes the query it is handed. What
        they share is that neither exposes its target as a table the schema gate
        can resolve, so both are refused for being ungateable rather than for
        being writes.
        """
        error = validate_read_only_query(sql)
        assert error is not None
        assert "PRAGMA and EXPLAIN are not supported" in error

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
