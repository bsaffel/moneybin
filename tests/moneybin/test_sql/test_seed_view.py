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
    # Carry columns auto-prefixed (so "Page" / "Loaded At" headers can
    # coexist with the system page/loaded_at columns).
    assert '"page" AS "_page"' in sql
    assert '"loaded_at" AS "_loaded_at"' in sql


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


def test_carry_column_can_coexist_with_normalized_header() -> None:
    """A real PDF column "Page" must coexist with the carry column "page".

    Carry columns auto-prefix to ``_<name>`` so they can never collide
    with normalized user headers (``_normalize_col_name`` strips leading
    underscores). The user-visible "Page" column lands as ``"page"`` and
    the system carry column lands as ``"_page"``.
    """
    sql = generate_seed_view_sql(
        source_table="raw.pdf_seeds",
        view_name="pdf_x",
        filter_column="alias",
        filter_value="x",
        typed_columns={"Page": "BIGINT"},
        carry_columns=["page", "loaded_at"],
    )
    assert "CAST(data->>'Page' AS BIGINT) AS \"page\"" in sql
    assert '"page" AS "_page"' in sql
    assert '"loaded_at" AS "_loaded_at"' in sql
