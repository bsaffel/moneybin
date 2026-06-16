"""M1S.4 — import-time account-binding gate + bindings (service level).

Exercises the conditional gate (interactive human first contact surfaces weak
account-merge candidates; agent / non-interactive load and queue) and the
account_bindings resolution map through the real import_file pipeline.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.services.import_confirmation import ImportConfirmationRequiredError
from moneybin.services.import_service import ImportService
from tests.moneybin.db_helpers import create_core_tables

_STANDARD_CSV = Path(__file__).parents[2] / "fixtures" / "tabular" / "standard.csv"


def _db(mock_secret_store: MagicMock, tmp_path: Path) -> Database:
    return Database(
        tmp_path / "binding.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
        read_only=False,
    )


def _seed_existing_account(db: Database, *, account_id: str, display_name: str) -> None:
    """Materialize core.dim_accounts with one account the name pass can match."""
    create_core_tables(db)
    db.conn.execute(
        "INSERT INTO core.dim_accounts (account_id, display_name) "  # noqa: S608  # test fixture
        "VALUES (?, ?)",
        [account_id, display_name],
    )


# --- the gate + bindings (via the real import_file pipeline) --------------
# Binding application ("new" -> force_standalone, id -> adopt, unbound -> gate)
# is exercised end-to-end below rather than against the private helper.


def test_human_import_gates_on_weak_account_candidate(
    mock_secret_store: MagicMock, tmp_path: Path
) -> None:
    db = _db(mock_secret_store, tmp_path)
    try:
        _seed_existing_account(
            db, account_id="wf_existing01", display_name="WF Checking"
        )
        svc = ImportService(db)
        with pytest.raises(ImportConfirmationRequiredError) as exc:
            svc.import_file(
                _STANDARD_CSV,
                account_name="WF Checking",
                refresh=False,
                confirm=True,
                actor_kind="human",
            )
        outcome = exc.value.outcome
        assert outcome.reason == "account_confirmation"
        cand_ids = [
            c["account_id"]
            for p in outcome.account_proposals
            for c in p["candidates"]  # type: ignore[union-attr,index]
        ]
        assert "wf_existing01" in cand_ids
        # Gate raised before transform/load: no rows landed.
        n = db.execute("SELECT COUNT(*) FROM raw.tabular_transactions").fetchone()
        assert n is not None and n[0] == 0
    finally:
        db.close()


def test_binding_to_candidate_adopts_and_loads(
    mock_secret_store: MagicMock, tmp_path: Path
) -> None:
    db = _db(mock_secret_store, tmp_path)
    try:
        _seed_existing_account(
            db, account_id="wf_existing01", display_name="WF Checking"
        )
        svc = ImportService(db)
        result = svc.import_file(
            _STANDARD_CSV,
            account_name="WF Checking",
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_bindings={"wf-checking": "wf_existing01"},
        )
        assert result.transactions > 0
        # The CSV's source_native ref now maps to the existing account.
        row = db.execute(
            "SELECT account_id FROM app.account_links WHERE ref_kind='source_native' "
            "AND ref_value=? AND status='accepted'",
            ["wf-checking"],
        ).fetchone()
        assert row is not None and row[0] == "wf_existing01"
        # Adopted, not proposed: no pending decision.
        n = db.execute(
            "SELECT COUNT(*) FROM app.account_link_decisions WHERE status='pending'"
        ).fetchone()
        assert n is not None and n[0] == 0
    finally:
        db.close()


def test_binding_new_mints_standalone(
    mock_secret_store: MagicMock, tmp_path: Path
) -> None:
    db = _db(mock_secret_store, tmp_path)
    try:
        _seed_existing_account(
            db, account_id="wf_existing01", display_name="WF Checking"
        )
        svc = ImportService(db)
        result = svc.import_file(
            _STANDARD_CSV,
            account_name="WF Checking",
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_bindings={"wf-checking": "new"},
        )
        assert result.transactions > 0
        # Declared new: source_native maps to a fresh id, NOT the candidate.
        row = db.execute(
            "SELECT account_id FROM app.account_links WHERE ref_kind='source_native' "
            "AND ref_value=? AND status='accepted'",
            ["wf-checking"],
        ).fetchone()
        assert row is not None and row[0] != "wf_existing01"
        n = db.execute(
            "SELECT COUNT(*) FROM app.account_link_decisions WHERE status='pending'"
        ).fetchone()
        assert n is not None and n[0] == 0
    finally:
        db.close()


def test_agent_import_does_not_gate_and_queues(
    mock_secret_store: MagicMock, tmp_path: Path
) -> None:
    db = _db(mock_secret_store, tmp_path)
    try:
        _seed_existing_account(
            db, account_id="wf_existing01", display_name="WF Checking"
        )
        svc = ImportService(db)
        # Agent path never gates — it loads and leaves the merge proposal in the
        # review queue (M1S.5 safety net).
        result = svc.import_file(
            _STANDARD_CSV,
            account_name="WF Checking",
            refresh=False,
            confirm=True,
            actor_kind="agent",
        )
        assert result.transactions > 0
        n = db.execute(
            "SELECT COUNT(*) FROM app.account_link_decisions WHERE status='pending'"
        ).fetchone()
        assert n is not None and n[0] >= 1
    finally:
        db.close()


def test_import_emits_account_link_metrics(
    mock_secret_store: MagicMock, tmp_path: Path
) -> None:
    """A queued candidate observes confidence and refreshes the pending gauge."""
    from prometheus_client import REGISTRY

    db = _db(mock_secret_store, tmp_path)
    try:
        _seed_existing_account(
            db, account_id="wf_existing01", display_name="WF Checking"
        )
        before = (
            REGISTRY.get_sample_value("moneybin_account_link_confidence_count") or 0.0
        )
        ImportService(db).import_file(
            _STANDARD_CSV,
            account_name="WF Checking",
            refresh=False,
            confirm=True,
            actor_kind="agent",  # loads + queues so resolve() observes confidence
        )
        after = (
            REGISTRY.get_sample_value("moneybin_account_link_confidence_count") or 0.0
        )
        assert after > before  # at least one candidate confidence observed
        # Gauge was just refreshed from this DB's live pending count (one proposal).
        gauge = REGISTRY.get_sample_value("moneybin_account_link_review_pending")
        assert gauge == 1.0
    finally:
        db.close()
