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
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
from sqlmesh import Context

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


def _write_restate_regression_models(root: Path, *, include_view_column: bool) -> None:
    """Write a minimal project with one FULL model and one dependent VIEW."""
    models = root / "models"
    models.mkdir(parents=True, exist_ok=True)
    (models / "items.sql").write_text(
        """
        MODEL (
          name core.restate_items,
          kind FULL,
          grain id
        );

        SELECT id FROM raw.restate_items
        """
    )
    view_model = """
        MODEL (
          name reports.restate_items,
          kind VIEW
        );

        SELECT id FROM core.restate_items
        """
    if include_view_column:
        view_model = """
            MODEL (
              name reports.restate_items,
              kind VIEW
            );

            SELECT id, id AS copy_id FROM core.restate_items
            """
    (models / "items_view.sql").write_text(view_model)


@pytest.mark.slow
def test_apply_restates_full_models_after_a_view_only_plan_change(
    db: Database, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A changed VIEW cannot suppress the FULL rebuild needed for new raw rows."""
    sqlmesh_root = tmp_path / "sqlmesh"
    _write_restate_regression_models(sqlmesh_root, include_view_column=False)

    @contextmanager
    def test_sqlmesh_context(database: Database):  # type: ignore[no-untyped-def]
        with sqlmesh_context(database, sqlmesh_root=sqlmesh_root) as context:
            yield context

    def skip_refresh_views(_db: object) -> None:
        return None

    monkeypatch.setattr(
        "moneybin.services.transform_service.sqlmesh_context", test_sqlmesh_context
    )
    monkeypatch.setattr(
        "moneybin.services.transform_service.refresh_views", skip_refresh_views
    )
    db.execute("CREATE TABLE raw.restate_items (id INTEGER)")
    db.execute("INSERT INTO raw.restate_items VALUES (1)")
    assert TransformService(db).apply().applied is True

    _write_restate_regression_models(sqlmesh_root, include_view_column=True)
    db.execute("INSERT INTO raw.restate_items VALUES (2)")

    assert TransformService(db).apply().applied is True
    assert db.execute("SELECT id FROM core.restate_items ORDER BY id").fetchall() == [
        (1,),
        (2,),
    ]


@pytest.mark.slow
def test_apply_does_not_restate_after_a_fresh_plan(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A fresh plan materializes FULL models without a follow-up restate."""
    plan_calls: list[dict[str, object]] = []
    original_plan = Context.plan

    def record_plan(self: Context, *args: Any, **kwargs: Any) -> Any:
        plan_calls.append(kwargs)
        return original_plan(self, *args, **kwargs)

    monkeypatch.setattr(Context, "plan", record_plan)
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

    result = TransformService(db).apply()

    assert result.applied, f"fresh apply failed: {result.error}"
    assert plan_calls == [{"no_prompts": True}]
    assert db.execute("SELECT COUNT(*) FROM core.dim_accounts").fetchone() == (1,)


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
