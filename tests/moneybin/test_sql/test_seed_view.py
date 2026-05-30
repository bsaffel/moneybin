"""Tests for the shared seed-view SQL builder."""

import pytest

from moneybin.sql.seed_view import generate_seed_view_sql


def test_pdf_view_filters_by_alias() -> None:
    sql = generate_seed_view_sql(
        source_table="raw.pdf_seeds",
        view_name="pdf_fidelity",
        filter_column="alias",
        filter_value="fidelity",
        typed_columns={"Date": "DATE", "Amount": "DECIMAL(18,2)"},
        carry_columns=["page", "loaded_at"],
    )
    assert 'CREATE OR REPLACE VIEW raw."pdf_fidelity"' in sql
    assert "FROM raw.pdf_seeds" in sql
    assert "WHERE alias = 'fidelity'" in sql
    assert "CAST(data->>'Date' AS DATE) AS \"date\"" in sql
    assert "page" in sql and "loaded_at" in sql


def test_rejects_unsafe_filter_value() -> None:
    with pytest.raises(ValueError):
        generate_seed_view_sql(
            source_table="raw.pdf_seeds",
            view_name="x",
            filter_column="alias",
            filter_value="x'; DROP TABLE raw.pdf_seeds; --",
            typed_columns={"A": "VARCHAR"},
            carry_columns=["loaded_at"],
        )


def test_rejects_unsafe_sql_type() -> None:
    with pytest.raises(ValueError):
        generate_seed_view_sql(
            source_table="raw.pdf_seeds",
            view_name="x",
            filter_column="alias",
            filter_value="x",
            typed_columns={"A": "VARCHAR; DROP"},
            carry_columns=["loaded_at"],
        )


def test_rejects_unsafe_view_name() -> None:
    with pytest.raises(ValueError, match="View name"):
        generate_seed_view_sql(
            source_table="raw.pdf_seeds",
            view_name="1bad",
            filter_column="alias",
            filter_value="x",
            typed_columns={"A": "VARCHAR"},
            carry_columns=[],
        )


def test_carry_column_collision_rejected() -> None:
    with pytest.raises(ValueError, match="reserved carry column"):
        generate_seed_view_sql(
            source_table="raw.pdf_seeds",
            view_name="pdf_x",
            filter_column="alias",
            filter_value="x",
            typed_columns={"page": "BIGINT"},
            carry_columns=["page", "loaded_at"],
        )
