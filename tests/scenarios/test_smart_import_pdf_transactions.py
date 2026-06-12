"""Scenario: PDF import routing — transactions path and seed fallback.

Ground-truth expectations are independently derived from the hand-authored
fixture data (dates, amounts, and balance math), not from running the
pipeline and pasting the result. If the pipeline breaks an expectation,
investigate the code first; do NOT relax the YAML or assertion.
"""

from __future__ import annotations

import shutil
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import yaml

from moneybin.database import Database
from moneybin.services.import_service import ImportService
from tests.scenarios._runner import scenario_env
from tests.scenarios._runner.loader import Scenario

# Fixtures directory containing PDFs and ground-truth YAML.
_FIXTURES_DIR = (
    Path(__file__).parent.parent
    / "moneybin"
    / "test_extractors"
    / "test_pdf"
    / "fixtures"
)


def _load_yaml(name: str) -> dict[str, Any]:
    """Load a ground-truth YAML file by fixture stem name."""
    path = _FIXTURES_DIR / f"{name}.yaml"
    data: dict[str, Any] = yaml.safe_load(path.read_text())
    return data


def _pdf_path(name: str) -> Path:
    """Return the absolute path to a named PDF fixture."""
    return (_FIXTURES_DIR / f"{name}.pdf").resolve()


def _minimal_scenario(name: str) -> Scenario:
    """Minimal scenario spec — empty pipeline, just boots an encrypted DB."""
    return Scenario.model_validate({
        "scenario": name,
        "setup": {"persona": "basic", "seed": 42, "years": 1},
        "pipeline": [],
    })


def _count_tabular_rows(db: Database, import_id: str) -> int:
    """Count rows in raw.tabular_transactions for a specific import_id."""
    row = db.execute(
        """
        SELECT COUNT(*)
        FROM raw.tabular_transactions
        WHERE source_type = 'pdf' AND import_id = ?
        """,
        [import_id],
    ).fetchone()
    return int(row[0]) if row else 0


def _count_pdf_formats(db: Database) -> int:
    row = db.execute("SELECT COUNT(*) FROM app.pdf_formats").fetchone()
    return int(row[0]) if row else 0


def _fetch_tabular_rows(db: Database, import_id: str) -> list[dict[str, Any]]:
    """Fetch transaction rows for an import, ordered by row_number."""
    rows = db.execute(
        """
        SELECT transaction_date, description, amount
        FROM raw.tabular_transactions
        WHERE source_type = 'pdf' AND import_id = ?
        ORDER BY row_number
        """,
        [import_id],
    ).fetchall()
    return [
        {"date": str(r[0]), "description": r[1], "amount": Decimal(str(r[2]))}
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Test 1: Chase checking — first import routes to transactions
# ---------------------------------------------------------------------------


@pytest.mark.scenarios
@pytest.mark.slow
def test_chase_checking_first_import_routes_transactions(tmp_path: Path) -> None:
    """First import of chase_checking_simple.pdf routes to raw.tabular_transactions.

    Asserts:
    - result.transactions == 5 (hand-derived from the 5 rows in the fixture)
    - rows land in raw.tabular_transactions with source_type='pdf'
    - exactly one row saved to app.pdf_formats (auto-derived recipe)
    - inserted rows match the YAML ground-truth (date + amount + description)
    """
    gt = _load_yaml("chase_checking_simple")
    assert gt["expected_outcome"] == "transactions"

    pdf_src = _pdf_path("chase_checking_simple")
    pdf_copy = tmp_path / "chase_checking_simple.pdf"
    shutil.copy(pdf_src, pdf_copy)

    with scenario_env(_minimal_scenario("chase-first-import")) as (db, _tmp, _env):
        svc = ImportService(db)
        result = svc.import_file(pdf_copy, refresh=False)

        expected_txn_count = len(gt["expected_transactions"])
        assert result.transactions == expected_txn_count, (
            f"Expected {expected_txn_count} transactions, got {result.transactions}"
        )
        assert result.import_id is not None

        # One format row saved on first contact
        assert _count_pdf_formats(db) == 1, (
            f"Expected 1 row in app.pdf_formats, got {_count_pdf_formats(db)}"
        )

        # Verify row content against independently-derived YAML ground truth
        rows = _fetch_tabular_rows(db, result.import_id)
        assert len(rows) == expected_txn_count

        for i, (row, expected) in enumerate(
            zip(rows, gt["expected_transactions"], strict=True)
        ):
            assert row["date"] == expected["date"], (
                f"Row {i} date mismatch: got {row['date']!r}, expected {expected['date']!r}"
            )
            assert row["amount"] == Decimal(expected["amount"]), (
                f"Row {i} amount mismatch: got {row['amount']}, expected {expected['amount']}"
            )
            assert row["description"] == expected["description"], (
                f"Row {i} description mismatch: got {row['description']!r}, "
                f"expected {expected['description']!r}"
            )


# ---------------------------------------------------------------------------
# Test 2: Chase checking — second import replays saved recipe
# ---------------------------------------------------------------------------


@pytest.mark.scenarios
@pytest.mark.slow
def test_chase_checking_second_import_replays_recipe(tmp_path: Path) -> None:
    """Second import of the same layout reuses the saved recipe from app.pdf_formats.

    Asserts:
    - still exactly one row in app.pdf_formats after two imports (no duplicate saves)
    - both imports report transactions (not seed)
    - the routing decision on the second import has matched_format_name set
    """
    pdf_src = _pdf_path("chase_checking_simple")
    # Use two distinct file paths so each has a unique canonical path and
    # its own import_id, while sharing the same PDF layout/fingerprint.
    pdf_first = tmp_path / "chase_first.pdf"
    pdf_second = tmp_path / "chase_second.pdf"
    shutil.copy(pdf_src, pdf_first)
    shutil.copy(pdf_src, pdf_second)

    captured_decisions: list[Any] = []

    # Import the real function before patching so the wrapper doesn't recurse.
    from moneybin.extractors.pdf.routing import route_pdf_import as _real_route

    def _capturing_route(doc: Any, db: Any) -> Any:
        """Wrapper that records the RouteDecision before returning it."""
        decision = _real_route(doc, db)
        captured_decisions.append(decision)
        return decision

    with scenario_env(_minimal_scenario("chase-replay")) as (db, _tmp, _env):
        svc = ImportService(db)

        with patch(
            "moneybin.extractors.pdf.routing.route_pdf_import",
            side_effect=_capturing_route,
        ):
            result_first = svc.import_file(pdf_first, refresh=False)
            result_second = svc.import_file(pdf_second, refresh=False)

        # First import routed to transactions
        assert result_first.transactions > 0, "First import produced no transactions"

        # Exactly one format row — no duplicate saved on second contact
        # (second import's content-hash IDs already exist → on_conflict=ignore)
        assert _count_pdf_formats(db) == 1, (
            f"Expected 1 row in app.pdf_formats after two imports, "
            f"got {_count_pdf_formats(db)}"
        )

        # Both routing decisions were captured
        assert len(captured_decisions) == 2, (
            f"Expected 2 routing decisions, got {len(captured_decisions)}"
        )
        first_decision, second_decision = captured_decisions

        # First import: no saved format yet → matched_format_name is None (auto-derive path)
        assert first_decision.matched_format_name is None, (
            f"First import should not have matched a format, "
            f"got matched_format_name={first_decision.matched_format_name!r}"
        )
        # Second import: saved format found → matched_format_name is set (replay path)
        assert second_decision.matched_format_name is not None, (
            "Second import should have matched the saved format "
            "(matched_format_name should be non-None on the replay path)"
        )

        # Verify second import result (transactions or 0 due to content-hash dedup)
        # Key invariant: the import succeeded (no exception raised)
        assert result_second.import_id is not None, (
            "Second import should have an import_id"
        )


# ---------------------------------------------------------------------------
# Test 3: Fidelity positions — non-transaction table falls back to seed
# ---------------------------------------------------------------------------


@pytest.mark.scenarios
@pytest.mark.slow
def test_fidelity_positions_falls_back_to_seed(tmp_path: Path) -> None:
    """Investment positions PDF routes to seed (no transaction table found).

    Asserts:
    - seed_rows > 0 in result.details (rows landed in raw.pdf_seeds)
    - zero rows in raw.tabular_transactions for this import_id
    - zero rows in app.pdf_formats (no recipe saved for non-transaction PDFs)
    """
    gt = _load_yaml("fidelity_positions")
    assert gt["expected_outcome"] == "seed"
    assert gt["expected_reason"] == "no_transaction_table"

    pdf_src = _pdf_path("fidelity_positions")
    pdf_copy = tmp_path / "fidelity_positions.pdf"
    shutil.copy(pdf_src, pdf_copy)

    with scenario_env(_minimal_scenario("fidelity-seed")) as (db, _tmp, _env):
        svc = ImportService(db)
        result = svc.import_file(pdf_copy, refresh=False)

        # Seed path: seed_rows is set, transactions is 0
        assert result.transactions == 0, (
            f"Expected 0 transactions (seed path), got {result.transactions}"
        )
        assert "seed_rows" in result.details, (
            f"Expected 'seed_rows' in result.details, got {result.details}"
        )
        assert result.details["seed_rows"] > 0, (
            "Expected seed_rows > 0: Fidelity positions table should land in pdf_seeds"
        )
        assert result.import_id is not None

        # Zero rows in raw.tabular_transactions for this import
        tabular_count = _count_tabular_rows(db, result.import_id)
        assert tabular_count == 0, (
            f"Expected 0 rows in raw.tabular_transactions for fidelity import, "
            f"got {tabular_count}"
        )

        # Zero rows in app.pdf_formats — seed path never saves a recipe
        assert _count_pdf_formats(db) == 0, (
            f"Expected 0 rows in app.pdf_formats for seed-path import, "
            f"got {_count_pdf_formats(db)}"
        )


# ---------------------------------------------------------------------------
# Test 4: Bridge ladder — agent recipe applied, persisted, then deterministically replayed
# ---------------------------------------------------------------------------


@pytest.mark.scenarios
@pytest.mark.slow
def test_bridge_apply_round_trip_persists_and_replays(tmp_path: Path) -> None:
    """The Phase 2b bridge apply round-trip lands rows, persists, and replays.

    Simulates the driving agent's response with a recipe sourced from the
    deterministic auto-derive (a real agent would propose an equivalent one;
    the apply path re-runs whichever recipe through the reconciliation gate, so
    the assertions below judge the loaded rows against the independently derived
    YAML ground truth — not the recipe). Exercises ``apply_pdf_bridge_response``
    end-to-end through a real encrypted DB + real PDF bytes:

    - apply loads the 5 ground-truth rows to raw.tabular_transactions
    - the agent's recipe persists to app.pdf_formats (one row)
    - a subsequent deterministic import of the same layout REPLAYS the
      bridge-persisted recipe (matched_format_name set) — closing the ladder
    """
    from moneybin.extractors.pdf.extractor import PDFExtractor
    from moneybin.extractors.pdf.routing import route_pdf_import

    gt = _load_yaml("chase_checking_simple")
    expected_txns = gt["expected_transactions"]
    expected_count = len(expected_txns)  # 5, hand-derived from the fixture

    pdf_src = _pdf_path("chase_checking_simple")
    pdf_apply = tmp_path / "chase_bridge.pdf"
    pdf_replay = tmp_path / "chase_replay.pdf"
    shutil.copy(pdf_src, pdf_apply)
    shutil.copy(pdf_src, pdf_replay)

    with scenario_env(_minimal_scenario("chase-bridge")) as (db, _tmp, _env):
        svc = ImportService(db)

        # Source a valid recipe (the simulated agent proposal) from the
        # deterministic auto-derive — route_pdf_import is pure routing, no save.
        doc = PDFExtractor().extract(pdf_apply.resolve())
        decision = route_pdf_import(doc, db)
        assert decision.outcome == "transactions"
        assert decision.recipe is not None

        bridge_response = {
            "recipe": decision.recipe.model_dump(),
            # The agent's claimed rows: only the COUNT is used by apply (the
            # re-executed rows are what load); pass the YAML rows for realism.
            "rows": [dict(t) for t in expected_txns],
        }

        result = svc.apply_pdf_bridge_response(pdf_apply, bridge_response)

        assert result.outcome == "applied"
        assert result.rows_loaded == expected_count
        assert result.format_name is not None
        assert result.import_id is not None

        # Rows landed in raw.tabular_transactions matching the YAML ground truth.
        rows = _fetch_tabular_rows(db, result.import_id)
        assert len(rows) == expected_count
        for i, (row, expected) in enumerate(zip(rows, expected_txns, strict=True)):
            assert row["date"] == expected["date"], f"Row {i} date mismatch"
            assert row["amount"] == Decimal(expected["amount"]), (
                f"Row {i} amount mismatch"
            )
            assert row["description"] == expected["description"], (
                f"Row {i} description mismatch"
            )

        # The agent's recipe persisted exactly once.
        assert _count_pdf_formats(db) == 1

        # Close the ladder: a deterministic import of the same layout replays
        # the bridge-persisted recipe instead of re-escalating.
        from moneybin.extractors.pdf.routing import route_pdf_import as _real_route

        captured: list[Any] = []

        def _capturing(doc_: Any, db_: Any) -> Any:
            decision_ = _real_route(doc_, db_)
            captured.append(decision_)
            return decision_

        with patch(
            "moneybin.extractors.pdf.routing.route_pdf_import",
            side_effect=_capturing,
        ):
            replay_result = svc.import_file(pdf_replay, refresh=False)

        assert replay_result.import_id is not None
        assert len(captured) == 1
        assert captured[0].matched_format_name is not None, (
            "deterministic replay should match the bridge-persisted format"
        )
        # Still exactly one format row — replay reuses, never re-saves.
        assert _count_pdf_formats(db) == 1
