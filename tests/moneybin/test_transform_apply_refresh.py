"""Integration test: TransformService.apply() must reflect NEW raw data.

Regression guard for the "refresh only plans, never runs" bug: the routine
refresh path (`TransformService.apply()`) drove SQLMesh via `ctx.plan()`, which
only re-materializes models whose *definition* changed. A second data load (e.g.
linking/pulling a second institution the same day) changed no model definition,
so the lone FULL model `core.dim_accounts` was never rebuilt and the new
institution's accounts never appeared — even though `raw.*`/`prep.*` had them.

The fix wires SQLMesh's data-processing command into apply() so a second apply
after new raw rows updates the materialized dimension.
"""

from __future__ import annotations

import logging

import pytest

from moneybin.database import Database, sqlmesh_context
from moneybin.services.transform_service import TransformService

pytestmark = pytest.mark.integration


@pytest.mark.slow
def test_sqlmesh_context_silences_sqlglot_transpile_warnings(db: Database) -> None:
    """Dialect-fidelity warnings from sqlglot must be suppressed in the boundary.

    sqlglot emits WARNING-level noise like 'REGEXP_REPLACE with non-literal
    position' while generating SQL for our models. We only ever target DuckDB
    (no cross-dialect transpile), so these are non-actionable and spam stderr 6×
    on every transform. They must be quieted within sqlmesh_context.
    """
    with sqlmesh_context(db):
        assert logging.getLogger("sqlglot").getEffectiveLevel() >= logging.ERROR


def _insert_plaid_account(
    db: Database,
    *,
    native_key: str,
    canonical_id: str,
    institution_name: str,
    account_type: str,
    mask: str,
    source_origin: str,
    extracted_at: str,
) -> None:
    """Seed one Plaid raw account plus its accepted canonical link.

    Mirrors what a real sync pull produces: a raw.plaid_accounts row keyed by
    (native account_id, source_origin=item_id) and an accepted source_native
    row in app.account_links mapping it to a canonical id.
    """
    db.execute(
        """
        INSERT INTO raw.plaid_accounts
            (account_id, account_type, account_subtype, institution_name,
             mask, source_file, source_type, source_origin,
             extracted_at, loaded_at)
        VALUES (?, ?, NULL, ?, ?, '/tmp/sync.json', 'plaid', ?,
                ?::TIMESTAMP, ?::TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [
            native_key,
            account_type,
            institution_name,
            mask,
            source_origin,
            extracted_at,
            extracted_at,
        ],
    )
    db.execute(
        """
        INSERT INTO app.account_links
            (link_id, account_id, ref_kind, ref_value, source_type,
             source_origin, status, decided_by, decided_at)
        VALUES (?, ?, 'source_native', ?, 'plaid', ?, 'accepted', 'auto',
                CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [f"link-{native_key}", canonical_id, native_key, source_origin],
    )


@pytest.mark.slow
def test_apply_reflects_second_data_load(db: Database) -> None:
    """A second apply() after new raw rows must surface the new accounts.

    Reproduces the real flow: link/pull institution A (apply), then link/pull
    institution B the same day (apply again). Both institutions' accounts must
    appear in core.dim_accounts after the second apply.
    """
    # First pull: institution A.
    _insert_plaid_account(
        db,
        native_key="a-native-checking",
        canonical_id="canonA00000001",
        institution_name="Bank A",
        account_type="depository",
        mask="0000",
        source_origin="item_a",
        extracted_at="2026-06-01 12:00:00",
    )
    first = TransformService(db).apply()
    assert first.applied, f"first apply failed: {first.error}"

    institutions_after_first = {
        row[0]
        for row in db.execute(
            "SELECT institution_name FROM core.dim_accounts"
        ).fetchall()
    }
    assert institutions_after_first == {"Bank A"}, (
        f"sanity: first apply should materialize Bank A, got {institutions_after_first}"
    )

    # Second pull, same day: institution B (new raw rows, no model-definition change).
    _insert_plaid_account(
        db,
        native_key="b-native-checking",
        canonical_id="canonB00000001",
        institution_name="Bank B",
        account_type="depository",
        mask="0000",
        source_origin="item_b",
        extracted_at="2026-06-01 12:05:00",
    )
    second = TransformService(db).apply()
    assert second.applied, f"second apply failed: {second.error}"

    institutions_after_second = {
        row[0]
        for row in db.execute(
            "SELECT institution_name FROM core.dim_accounts"
        ).fetchall()
    }
    assert institutions_after_second == {"Bank A", "Bank B"}, (
        "second apply() did not surface the newly-pulled institution; "
        f"core.dim_accounts shows {institutions_after_second}"
    )
