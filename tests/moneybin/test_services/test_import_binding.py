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
            c["account_id"] for p in outcome.account_proposals for c in p["candidates"]
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


def _minted_account_id(db: Database, source_key: str) -> str:
    row = db.execute(
        "SELECT account_id FROM app.account_links WHERE ref_kind='source_native' "
        "AND ref_value=? AND status='accepted'",
        [source_key],
    ).fetchone()
    assert row is not None
    return str(row[0])


def test_new_binding_captures_account_metadata(
    mock_secret_store: MagicMock, tmp_path: Path
) -> None:
    """account_metadata for a 'new' binding writes app.account_settings at mint."""
    db = _db(mock_secret_store, tmp_path)
    try:
        svc = ImportService(db)
        svc.import_file(
            _STANDARD_CSV,
            account_name="WF Checking",
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_bindings={"wf-checking": "new"},
            account_metadata={
                "wf-checking": {
                    "display_name": "WF Checking",
                    "account_subtype": "checking",
                    "last_four": "4267",
                    "iso_currency_code": "USD",
                }
            },
        )
        minted = _minted_account_id(db, "wf-checking")
        row = db.execute(
            "SELECT display_name, last_four, account_subtype, iso_currency_code "
            "FROM app.account_settings WHERE account_id=?",
            [minted],
        ).fetchone()
        assert row == ("WF Checking", "4267", "checking", "USD")
    finally:
        db.close()


def test_account_metadata_rejects_unknown_field_before_any_write(
    mock_secret_store: MagicMock, tmp_path: Path
) -> None:
    """A typo'd metadata key fails up-front — no rows are written (no orphans)."""
    db = _db(mock_secret_store, tmp_path)
    try:
        svc = ImportService(db)
        with pytest.raises(ValueError, match="Unknown account_metadata"):
            svc.import_file(
                _STANDARD_CSV,
                account_name="WF Checking",
                refresh=False,
                confirm=True,
                actor_kind="human",
                account_bindings={"wf-checking": "new"},
                account_metadata={"wf-checking": {"subtype": "checking"}},
            )
        # Validation runs before any DB write — no orphaned account_links, no
        # raw rows, no settings.
        for table in (
            "app.account_links",
            "raw.tabular_transactions",
            "app.account_settings",
        ):
            n = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()  # noqa: S608  # constant table name
            assert n is not None and n[0] == 0, table
    finally:
        db.close()


def test_account_metadata_rejects_invalid_value_before_any_write(
    mock_secret_store: MagicMock, tmp_path: Path
) -> None:
    """A malformed value (bad last_four) also fails up-front, before any write."""
    db = _db(mock_secret_store, tmp_path)
    try:
        svc = ImportService(db)
        with pytest.raises(ValueError, match="last_four"):
            svc.import_file(
                _STANDARD_CSV,
                account_name="WF Checking",
                refresh=False,
                confirm=True,
                actor_kind="human",
                account_bindings={"wf-checking": "new"},
                account_metadata={"wf-checking": {"last_four": "42"}},
            )
        n = db.execute("SELECT COUNT(*) FROM app.account_links").fetchone()
        assert n is not None and n[0] == 0
    finally:
        db.close()


def test_account_bindings_rejects_unknown_source_key(
    mock_secret_store: MagicMock, tmp_path: Path
) -> None:
    """A binding for a source key not in the file fails loud, before any write."""
    db = _db(mock_secret_store, tmp_path)
    try:
        svc = ImportService(db)
        with pytest.raises(
            ValueError, match="account_bindings references unknown source key"
        ):
            svc.import_file(
                _STANDARD_CSV,
                account_name="WF Checking",
                refresh=False,
                confirm=True,
                actor_kind="human",
                account_bindings={"typo-key": "new"},
            )
        n = db.execute("SELECT COUNT(*) FROM app.account_links").fetchone()
        assert n is not None and n[0] == 0
    finally:
        db.close()


def test_account_bindings_rejects_empty_value(
    mock_secret_store: MagicMock, tmp_path: Path
) -> None:
    """An empty binding value fails loud, not a silent fall-through to mint.

    `explicit_account_id=""` is falsy, so without the guard the resolver would
    skip the explicit-adopt path and mint fresh as if no binding was given.
    """
    db = _db(mock_secret_store, tmp_path)
    try:
        svc = ImportService(db)
        with pytest.raises(ValueError, match="empty value"):
            svc.import_file(
                _STANDARD_CSV,
                account_name="WF Checking",
                refresh=False,
                confirm=True,
                actor_kind="human",
                account_bindings={"wf-checking": ""},
            )
        n = db.execute("SELECT COUNT(*) FROM app.account_links").fetchone()
        assert n is not None and n[0] == 0
    finally:
        db.close()


def test_metadata_not_captured_for_pending_provisional(
    mock_secret_store: MagicMock, tmp_path: Path
) -> None:
    """Metadata for an unbound account that resolves to pending_review is dropped.

    Writing settings to a provisional id that a later merge re-points would
    orphan them — capture is reserved for genuinely-new mints.
    """
    db = _db(mock_secret_store, tmp_path)
    try:
        _seed_existing_account(
            db, account_id="wf_existing01", display_name="WF Checking"
        )
        ImportService(db).import_file(
            _STANDARD_CSV,
            account_name="WF Checking",
            refresh=False,
            confirm=True,
            actor_kind="agent",  # no gate; the csv mints a pending provisional
            account_metadata={"wf-checking": {"display_name": "Renamed"}},
        )
        # The account resolved to a pending_review provisional, so no settings.
        n = db.execute("SELECT COUNT(*) FROM app.account_settings").fetchone()
        assert n is not None and n[0] == 0
    finally:
        db.close()


def test_pending_gauge_counts_distinct_provisionals(
    mock_secret_store: MagicMock, tmp_path: Path
) -> None:
    """The gauge counts review items (distinct provisionals), not decision rows."""
    from prometheus_client import REGISTRY

    from moneybin.repositories.account_link_decisions_repo import (
        AccountLinkDecisionsRepo,
    )
    from moneybin.services.account_resolver import refresh_account_link_pending_gauge

    db = _db(mock_secret_store, tmp_path)
    try:
        # One provisional with two candidate decisions (two weak signals).
        repo = AccountLinkDecisionsRepo(db)
        for cand in ("cand_a", "cand_b"):
            repo.insert(
                decision_id=f"dec_{cand}",
                provisional_account_id="prov_1",
                candidate_account_id=cand,
                confidence_score=0.5,
                match_signals={"signal": "name", "value": "WF"},
                decided_by="auto",
                actor="system",
                match_reason="name",
            )
        refresh_account_link_pending_gauge(db)
        # Two decision rows, but one provisional → one review item.
        gauge = REGISTRY.get_sample_value("moneybin_account_link_review_pending")
        assert gauge == 1.0
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
