"""Integration tests for `moneybin transactions categorize improve-ai` CLI command."""

# ruff: noqa: S101

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.transactions.categorize import app
from moneybin.database import Database

pytestmark = pytest.mark.integration

runner = CliRunner()

_ENCRYPTION_KEY = "integration-test-key-0123456789abcdef"


def _make_secret_store() -> MagicMock:
    store = MagicMock()
    store.get_key.return_value = _ENCRYPTION_KEY
    return store


def _make_db(tmp_path: Path) -> tuple[Database, MagicMock]:
    """Create a test database.

    ``no_auto_upgrade=True`` skips versioned migrations but ``Database.__init__``
    still runs ``refresh_views`` unconditionally, so ``seeds.categories`` /
    ``seeds.category_source_map`` (bootstrap tables) and the
    ``core.dim_categories`` / ``core.bridge_category_source_map`` views the
    upgrade pass reads already exist on open — no manual view setup needed here.
    """
    store = _make_secret_store()
    db = Database(
        tmp_path / "test.duckdb",
        secret_store=store,
        no_auto_upgrade=True,
        read_only=False,
    )
    return db, store


def _seed_bridge_mapping(db: Database) -> None:
    """Seed one confident (HIGH) Plaid bridge mapping: GROCERIES -> Groceries."""
    db.execute(
        "INSERT INTO seeds.category_source_map "
        "(source_type, source_category_code, code_level, category_id, "
        "source_taxonomy_version) VALUES "
        "('plaid', 'FOOD_AND_DRINK_GROCERIES', 'detailed', 'FND-GRO', 'plaid_pfc_v2')"
    )
    db.execute(
        "INSERT INTO seeds.categories (category_id, category, subcategory, description) "
        "VALUES ('FND-GRO', 'Groceries', NULL, 'test category')"
    )


def _seed_upgradeable_ai_row(db: Database, transaction_id: str) -> None:
    """Seed one ai-guessed transaction whose Plaid category bridges confidently.

    ``prep.int_transactions__merged`` is a SQLMesh VIEW in production; this
    creates the bare physical table with just the columns
    ``improve_ai_categories`` reads (mirrors the precedent in
    ``tests/moneybin/test_services/test_categorization_service.py``).
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE TABLE IF NOT EXISTS prep.int_transactions__merged ("
        "  transaction_id VARCHAR PRIMARY KEY, "
        "  category_detailed VARCHAR, "
        "  plaid_category VARCHAR, "
        "  category_confidence VARCHAR"
        ")"
    )
    db.execute(
        "INSERT INTO prep.int_transactions__merged "
        "(transaction_id, category_detailed, plaid_category, category_confidence) "
        "VALUES (?, 'FOOD_AND_DRINK_GROCERIES', 'FOOD_AND_DRINK', 'HIGH')",
        [transaction_id],
    )
    # Pre-existing ai-guessed categorization (priority 7) — the row the
    # upgrade pass should overwrite with a confident provider_native category.
    db.execute(
        "INSERT INTO app.transaction_categories "
        "(transaction_id, category, categorized_by) VALUES (?, 'Shopping', 'ai')",
        [transaction_id],
    )


def _invoke(
    monkeypatch: pytest.MonkeyPatch,
    db: Database,
    store: MagicMock,
    args: list[str],
    **kwargs: object,
) -> object:
    import moneybin.cli.commands.transactions.categorize as _categorize_mod

    @contextmanager
    def _db_ctx(*_a: object, **_kw: object):
        yield db  # noqa: B023 — db is loop-invariant in this helper

    monkeypatch.setattr(_categorize_mod, "get_database", _db_ctx)
    monkeypatch.setattr("moneybin.secrets.SecretStore", lambda: store)
    return runner.invoke(app, args, **kwargs)  # type: ignore[call-overload]


class TestCategorizeImproveAiCLI:
    """Integration tests for the 'moneybin transactions categorize improve-ai' command."""

    def test_json_output_reports_upgraded_count(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """--output json returns a response envelope with the upgraded count."""
        db, store = _make_db(tmp_path)
        _seed_bridge_mapping(db)
        _seed_upgradeable_ai_row(db, "t1")

        result = _invoke(monkeypatch, db, store, ["improve-ai", "--output", "json"])
        assert result.exit_code == 0, result.stderr  # type: ignore[union-attr]
        envelope = json.loads(result.output)  # type: ignore[union-attr]
        assert envelope["data"]["upgraded_count"] == 1

        row = db.execute(
            "SELECT category, categorized_by FROM app.transaction_categories "
            "WHERE transaction_id = 't1'"
        ).fetchone()
        assert row == ("Groceries", "provider_native")

    def test_default_text_output_exits_zero(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Default (text) output exits 0 and performs the real upgrade.

        Mirrors the sibling ``run``/``commit`` CLI tests: the text-mode
        render path runs through ``logger.info``, which isn't captured by a
        bare ``CliRunner`` invocation of the subcommand app (no root-app
        logging setup) — so this asserts on exit code + the actual DB write,
        not on log text.
        """
        db, store = _make_db(tmp_path)
        _seed_bridge_mapping(db)
        _seed_upgradeable_ai_row(db, "t1")

        result = _invoke(monkeypatch, db, store, ["improve-ai"])
        assert result.exit_code == 0, result.stderr  # type: ignore[union-attr]

        row = db.execute(
            "SELECT category, categorized_by FROM app.transaction_categories "
            "WHERE transaction_id = 't1'"
        ).fetchone()
        assert row == ("Groceries", "provider_native")

    def test_no_upgradeable_rows_reports_zero(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With no ai-guessed rows to upgrade, the count is zero."""
        db, store = _make_db(tmp_path)

        result = _invoke(monkeypatch, db, store, ["improve-ai", "--output", "json"])
        assert result.exit_code == 0, result.stderr  # type: ignore[union-attr]
        envelope = json.loads(result.output)  # type: ignore[union-attr]
        assert envelope["data"]["upgraded_count"] == 0
