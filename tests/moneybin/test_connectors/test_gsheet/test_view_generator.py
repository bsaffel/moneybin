"""Tests for seed-adapter view SQL generator."""

import pytest

from moneybin.connectors.gsheet.view_generator import generate_seed_view_sql


class TestGenerateSeedViewSql:
    """Test SQL generation for per-connection views."""

    def test_basic_view_generation(self):
        """Single column → view with SELECT, WHERE, lifecycle cols."""
        sql = generate_seed_view_sql(
            alias="subscriptions",
            connection_id="conn-123",
            typed_columns={"Name": "VARCHAR"},
        )
        assert 'CREATE OR REPLACE VIEW raw."gsheet_subscriptions"' in sql
        assert "CAST(data->>'Name' AS VARCHAR) AS \"name\"" in sql
        assert "connection_id = 'conn-123'" in sql
        assert "deleted_from_source_at IS NULL" in sql

    def test_multiple_columns(self):
        """Multiple columns each appear in SELECT."""
        sql = generate_seed_view_sql(
            alias="transactions",
            connection_id="conn-456",
            typed_columns={
                "Amount": "DECIMAL(18,2)",
                "Date": "DATE",
                "Description": "VARCHAR",
            },
        )
        assert "CAST(data->>'Amount' AS DECIMAL(18,2)) AS \"amount\"" in sql
        assert "CAST(data->>'Date' AS DATE) AS \"date\"" in sql
        assert "CAST(data->>'Description' AS VARCHAR) AS \"description\"" in sql

    def test_header_with_spaces_normalized(self):
        """Header 'First Name' → normalized to 'first_name'."""
        sql = generate_seed_view_sql(
            alias="people",
            connection_id="conn-789",
            typed_columns={"First Name": "VARCHAR"},
        )
        assert "CAST(data->>'First Name' AS VARCHAR) AS \"first_name\"" in sql

    def test_lifecycle_columns_always_included(self):
        """row_number, deleted_from_source_at, loaded_at surface as _-prefixed columns."""
        sql = generate_seed_view_sql(
            alias="test",
            connection_id="conn-100",
            typed_columns={"Col": "BIGINT"},
        )
        # Exact alias form — guards against silent rename of system columns
        # (the auto-prefix is the system-vs-user-column boundary).
        assert '"row_number" AS "_row_number"' in sql
        assert '"deleted_from_source_at" AS "_deleted_from_source_at"' in sql
        assert '"loaded_at" AS "_loaded_at"' in sql

    def test_safe_connection_id_uuidv4_form(self):
        """connection_id in UUID format passes validation."""
        sql = generate_seed_view_sql(
            alias="test",
            connection_id="550e8400-e29b-41d4-a716-446655440000",
            typed_columns={},
        )
        assert "550e8400-e29b-41d4-a716-446655440000" in sql

    def test_empty_typed_columns_still_includes_lifecycle_cols(self):
        """typed_columns={} → only lifecycle cols (auto-prefixed), no SELECT values."""
        sql = generate_seed_view_sql(
            alias="empty",
            connection_id="conn-1",
            typed_columns={},
        )
        assert 'CREATE OR REPLACE VIEW raw."gsheet_empty"' in sql
        # Exact alias form — system carry columns surface as _-prefixed names.
        assert '"row_number" AS "_row_number"' in sql
        assert '"deleted_from_source_at" AS "_deleted_from_source_at"' in sql
        assert '"loaded_at" AS "_loaded_at"' in sql
        # Should have SELECT with only lifecycle cols (no CAST lines).
        select_lines = [
            line.strip()
            for line in sql.split("\n")
            if '"_row_number"' in line
            or '"_deleted_from_source_at"' in line
            or '"_loaded_at"' in line
        ]
        assert len(select_lines) >= 1  # At least one lifecycle col on its own line.

    def test_alias_with_numeric_start_rejected(self):
        """alias='1foo' fails validation."""
        with pytest.raises(ValueError, match="56-char limit"):
            generate_seed_view_sql(
                alias="1foo",
                connection_id="conn-1",
                typed_columns={},
            )

    def test_alias_with_uppercase_rejected(self):
        """alias='Foo' (uppercase) fails validation."""
        with pytest.raises(ValueError, match="56-char limit"):
            generate_seed_view_sql(
                alias="Foo",
                connection_id="conn-1",
                typed_columns={},
            )

    def test_connection_id_with_quote_rejected(self):
        """connection_id containing single quote fails validation."""
        with pytest.raises(ValueError, match="Invalid connection_id"):
            generate_seed_view_sql(
                alias="test",
                connection_id="conn'1",
                typed_columns={},
            )

    def test_special_chars_in_header_escaped(self):
        """Header with single quote gets escaped in SQL literal."""
        sql = generate_seed_view_sql(
            alias="test",
            connection_id="conn-1",
            typed_columns={"User's Name": "VARCHAR"},
        )
        # Single quote in header should be doubled for SQL escaping.
        assert "data->>'User''s Name'" in sql

    def test_sql_injection_in_type_rejected(self):
        """Allowlist rejects values that aren't canonical DuckDB types."""
        with pytest.raises(ValueError, match="Unsafe SQL type"):
            generate_seed_view_sql(
                alias="test",
                connection_id="conn-1",
                typed_columns={"Col": "VARCHAR); DROP TABLE raw.gsheet_seeds; --"},
            )

    def test_typo_in_type_rejected(self):
        """Common typos (e.g. INT vs BIGINT) fail closed rather than passing through."""
        with pytest.raises(ValueError, match="Unsafe SQL type"):
            generate_seed_view_sql(
                alias="test",
                connection_id="conn-1",
                typed_columns={"Col": "INT"},
            )

    def test_alias_max_56_chars_accepted(self):
        """56-char alias is the maximum accepted (gsheet_ + 56 = 63-char view name)."""
        alias = "a" + "b" * 55  # 1 + 55 = 56 chars total
        sql = generate_seed_view_sql(
            alias=alias,
            connection_id="conn-1",
            typed_columns={},
        )
        assert f"gsheet_{alias}" in sql

    def test_alias_57_chars_rejected(self):
        """57-char alias produces a 64-char view name and is now caught at wrapper."""
        alias = "a" + "b" * 56  # 1 + 56 = 57 chars total
        with pytest.raises(ValueError, match="56-char limit"):
            generate_seed_view_sql(
                alias=alias,
                connection_id="conn-1",
                typed_columns={},
            )

    def test_headers_normalizing_to_same_name_rejected(self):
        """Two headers that produce the same column-name alias fail at connect.

        Without the guard, ``Amount USD`` and ``Amount_USD`` both normalize
        to ``amount_usd`` and DuckDB raises an opaque duplicate-alias error
        during ``CREATE VIEW``. The pre-loop check points at the offending
        headers.
        """
        with pytest.raises(ValueError, match=r"normalize to 'amount_usd'"):
            generate_seed_view_sql(
                alias="test",
                connection_id="conn-1",
                typed_columns={
                    "Amount USD": "DECIMAL(18,2)",
                    "Amount_USD": "VARCHAR",
                },
            )
