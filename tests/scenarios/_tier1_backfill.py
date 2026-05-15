"""Helpers for wiring Tier 1 backfill assertions into scenario tests.

The four Tier 1 primitives (source attribution, schema snapshot, amount
precision, date bounds) are computed at test time because their expected
values depend on ``scenario.setup.years`` (date window) and the deterministic
``GeneratorEngine`` output (row count). YAML-only values would either be
stale (hard-coded years) or observe-and-paste (row counts). Keep them in
pytest where the formula is in scope.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Set as AbstractSet
from datetime import date

from moneybin.database import Database
from moneybin.synthetic.engine import GeneratorEngine
from moneybin.validation.assertions import (
    assert_amount_precision,
    assert_date_bounds,
    assert_row_count_exact,
    assert_schema_snapshot,
    assert_source_system_populated,
)
from moneybin.validation.result import AssertionResult
from tests.scenarios._runner.loader import SetupSpec

# Schema enumerated by hand from sqlmesh/models/core/fct_transactions.sql.
# Updating this requires inspecting the SQL — never paste a query result.
# Pyright treats DECIMAL/DATE/etc. types as DuckDB renders them in
# information_schema.columns.
FCT_TRANSACTIONS_SCHEMA: dict[str, str] = {
    "transaction_id": "VARCHAR",
    "account_id": "VARCHAR",
    "transaction_date": "DATE",
    "authorized_date": "DATE",
    "amount": "DECIMAL(18,2)",
    "amount_absolute": "DECIMAL(18,2)",
    "transaction_direction": "VARCHAR",
    "description": "VARCHAR",
    "merchant_name": "VARCHAR",
    "memo": "VARCHAR",
    "category": "VARCHAR",
    "subcategory": "VARCHAR",
    "categorized_by": "VARCHAR",
    "payment_channel": "VARCHAR",
    "transaction_type": "VARCHAR",
    "check_number": "VARCHAR",
    "is_pending": "BOOLEAN",
    "pending_transaction_id": "VARCHAR",
    "location_address": "VARCHAR",
    "location_city": "VARCHAR",
    "location_region": "VARCHAR",
    "location_postal_code": "VARCHAR",
    "location_country": "VARCHAR",
    "location_latitude": "DOUBLE",
    "location_longitude": "DOUBLE",
    "currency_code": "VARCHAR",
    "source_type": "VARCHAR",
    "source_count": "BIGINT",
    "match_confidence": "DECIMAL(5,4)",
    "source_extracted_at": "TIMESTAMP",
    "loaded_at": "TIMESTAMP",
    # Per-row freshness column added by core-updated-at-convention spec;
    # MAX of loaded_at, categorized_at, and per-txn aggregates of note/tag/split
    # timestamps. See sqlmesh/models/core/fct_transactions.sql outer SELECT.
    "updated_at": "TIMESTAMP",
    "is_transfer": "BOOLEAN",
    "transfer_pair_id": "VARCHAR",
    "transaction_year": "BIGINT",
    "transaction_month": "BIGINT",
    "transaction_day": "BIGINT",
    "transaction_day_of_week": "BIGINT",
    "transaction_year_month": "VARCHAR",
    "transaction_year_quarter": "VARCHAR",
    # Curation columns added by transaction-curation spec (V007); see
    # sqlmesh/models/core/fct_transactions.sql notes_agg/tags_agg/splits_agg CTEs.
    "notes": 'STRUCT(note_id VARCHAR, "text" VARCHAR, author VARCHAR, created_at TIMESTAMP)[]',
    "note_count": "BIGINT",
    "tags": "VARCHAR[]",
    "tag_count": "BIGINT",
    "splits": "STRUCT(split_id VARCHAR, amount DECIMAL(18,2), category VARCHAR, subcategory VARCHAR, note VARCHAR)[]",
    "split_count": "BIGINT",
    "has_splits": "BOOLEAN",
}


def expected_generator_txn_count(setup: SetupSpec) -> int:
    """Run the deterministic GeneratorEngine and return its transaction count.

    The persona YAML + seed + years define the formula; the engine
    materializes it. This is the "persona / generator config" derivation
    path from ``.claude/rules/testing.md``.
    """
    return len(
        GeneratorEngine(setup.persona, seed=setup.seed, years=setup.years)
        .generate()
        .transactions
    )


def date_window_for(setup: SetupSpec) -> tuple[date, date]:
    """Return the calendar window the synthetic generator targets.

    Mirrors ``GeneratorEngine``'s logic: N complete calendar years ending at
    ``current_year - 1``. Returns the exact ``(Jan 1 start_year, Dec 31 end_year)``
    boundaries — the generator emits calendar dates directly, so no padding
    is required.
    """
    today = date.today()
    end_year = today.year - 1
    start_year = end_year - setup.years + 1
    return (date(start_year, 1, 1), date(end_year, 12, 31))


def tier1_backfill(
    setup: SetupSpec,
    *,
    expected_sources: AbstractSet[str] = frozenset({"csv", "ofx"}),
    expected_row_count: int | None = None,
    schema: dict[str, str] | None = None,
) -> Callable[[Database], list[AssertionResult]]:
    """Build an ``extra_assertions`` callback wiring all four Tier 1 checks.

    Args:
        setup: Scenario setup (persona, seed, years).
        expected_sources: Source-system labels that must populate
            ``core.fct_transactions.source_type``. Defaults to ``{csv, ofx}``
            for synthetic-generator scenarios.
        expected_row_count: If provided, override the deterministic
            generator-derived count (used for fixture-driven scenarios where
            the generator output is augmented or replaced).
        schema: If provided, override the bundled fct_transactions snapshot
            (used by scenarios that load alternate fixtures).
    """
    sources = set(expected_sources)
    snapshot = schema if schema is not None else FCT_TRANSACTIONS_SCHEMA
    row_count = (
        expected_row_count
        if expected_row_count is not None
        else expected_generator_txn_count(setup)
    )
    window_start, window_end = date_window_for(setup)

    def _check(db: Database) -> list[AssertionResult]:
        return [
            assert_source_system_populated(
                db,
                table="core.fct_transactions",
                expected_sources=sources,
                column="source_type",
            ),
            assert_amount_precision(
                db,
                table="core.fct_transactions",
                column="amount",
                precision=18,
                scale=2,
            ),
            assert_date_bounds(
                db,
                table="core.fct_transactions",
                column="transaction_date",
                min_date=window_start,
                max_date=window_end,
            ),
            assert_row_count_exact(
                db,
                table="core.fct_transactions",
                expected=row_count,
            ),
            assert_schema_snapshot(
                db,
                table="core.fct_transactions",
                expected=snapshot,
            ),
        ]

    return _check


def schema_snapshot_only(
    schema: dict[str, str] | None = None,
) -> Callable[[Database], list[AssertionResult]]:
    """Build a callback that only asserts the fct_transactions schema snapshot.

    Used by scenarios where row count / source mix differs from the standard
    generator output (e.g. fixture-driven dedup scenarios) but the schema
    still must remain stable.
    """
    snapshot = schema if schema is not None else FCT_TRANSACTIONS_SCHEMA

    def _check(db: Database) -> list[AssertionResult]:
        return [
            assert_schema_snapshot(
                db,
                table="core.fct_transactions",
                expected=snapshot,
            ),
        ]

    return _check
