"""Tests for MCP privacy controls and query validation."""

import pytest

from moneybin.mcp.privacy import (
    check_table_allowed,
    not_implemented,
    truncate_result,
    validate_read_only_query,
)


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


class TestCheckTableAllowed:
    """Tests for table allowlist checking."""

    @pytest.mark.unit
    def test_no_allowlist_allows_all(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # With no allowlist, everything is allowed
        from moneybin.mcp import privacy

        monkeypatch.setattr(privacy, "ALLOWED_TABLES", None)
        assert check_table_allowed("any_table") is None

    @pytest.mark.unit
    def test_allowlist_blocks_unlisted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from moneybin.mcp import privacy

        monkeypatch.setattr(privacy, "ALLOWED_TABLES", {"raw.ofx_accounts"})
        result = check_table_allowed("raw.ofx_transactions")
        assert result is not None
        assert "not in the allowed" in result

    @pytest.mark.unit
    def test_allowlist_permits_listed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from moneybin.mcp import privacy

        monkeypatch.setattr(privacy, "ALLOWED_TABLES", {"raw.ofx_accounts"})
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

        monkeypatch.setattr(privacy, "MAX_CHARS", 20)
        result = truncate_result("x" * 100)
        # First 20 chars should be the original content
        assert result.startswith("x" * 20)
        # Truncation notice should be appended
        assert "[Result truncated" in result
        # Only the first 20 x's should remain (not all 100)
        assert result.count("x") == 20


class TestNotImplemented:
    """Tests for not-implemented message generation."""

    @pytest.mark.unit
    def test_message_format(self) -> None:
        result = not_implemented("Test feature", "Run some command")
        assert "[Not Yet Available]" in result
        assert "Test feature" in result
        assert "Run some command" in result
        assert "MoneyBin docs" in result
