"""Tests for AccountResolver (M1S.2 resolution ladder)."""

from __future__ import annotations

from typing import Any

import pytest

from moneybin.database import Database
from moneybin.services.account_resolution_types import SourceAccount
from moneybin.services.account_resolver import AccountResolver


def _src(**overrides: Any) -> SourceAccount:
    base: dict[str, Any] = {
        "source_type": "csv",
        "source_origin": "wells_fargo",
        "source_account_key": "wf-checking",
        "account_name": "WF Checking 4267",
        "account_number": None,
        "last_four": "4267",
        "institution": "wells_fargo",
        "persistent_token": None,
        "explicit_account_id": None,
    }
    base.update(overrides)
    return SourceAccount(**base)


def test_explicit_binding_adopts_pinned_id_and_writes_mapping(db: Database) -> None:
    """Ladder step 0: a caller-pinned account_id is adopted above all detection.

    An accepted source_native mapping is written so staging is total.
    """
    resolver = AccountResolver(db, actor="system")
    resolved = resolver.resolve(_src(explicit_account_id="acct_pinned1"))

    assert resolved.account_id == "acct_pinned1"
    assert resolved.is_new is False
    row = db.conn.execute(
        "SELECT account_id, ref_kind, status FROM app.account_links "
        "WHERE source_type = ? AND source_origin = ? AND ref_value = ?",
        ["csv", "wells_fargo", "wf-checking"],
    ).fetchone()
    assert row == ("acct_pinned1", "source_native", "accepted")


def test_explicit_rebind_same_id_is_noop(db: Database) -> None:
    """Re-binding the same source key to the same account is idempotent."""
    resolver = AccountResolver(db, actor="system")
    resolver.resolve(_src(explicit_account_id="acct_pinned1"))
    resolver.resolve(_src(explicit_account_id="acct_pinned1"))

    n = db.conn.execute(
        "SELECT COUNT(*) FROM app.account_links WHERE ref_kind = 'source_native' "
        "AND ref_value = 'wf-checking'"
    ).fetchone()
    assert n is not None and n[0] == 1


def test_explicit_rebind_to_different_id_raises(db: Database) -> None:
    """A silent re-point would corrupt the staging JOIN — surface the conflict instead."""
    resolver = AccountResolver(db, actor="system")
    resolver.resolve(_src(explicit_account_id="acct_A"))
    with pytest.raises(ValueError, match="different"):
        resolver.resolve(_src(explicit_account_id="acct_B"))


def test_source_native_reimport_is_idempotent(db: Database) -> None:
    """Re-importing the same source account reuses the canonical id (no dup)."""
    resolver = AccountResolver(db, actor="system")
    first = resolver.resolve(_src(explicit_account_id="acct_wf_1"))
    second = resolver.resolve(_src())  # same source_native key, no explicit_account_id

    assert first.account_id == second.account_id
    assert second.is_new is False
    assert second.outcome == "adopted_strong"
    n = db.conn.execute(
        "SELECT COUNT(*) FROM app.account_links WHERE ref_kind = 'source_native' "
        "AND ref_value = 'wf-checking'"
    ).fetchone()
    assert n is not None and n[0] == 1


def test_persistent_token_auto_adopts_across_source_origin(db: Database) -> None:
    """A remembered persistent_token re-links the same account across connections."""
    resolver = AccountResolver(db, actor="system")
    first = resolver.resolve(
        _src(
            source_type="plaid",
            source_account_key="tok-A",
            persistent_token="pers-1",  # noqa: S106  # test fixture, not a real credential
            explicit_account_id="acct_plaid_1",
        )
    )
    second = resolver.resolve(
        _src(
            source_type="plaid",
            source_origin="plaid_conn_2",
            source_account_key="tok-B",
            persistent_token="pers-1",  # noqa: S106  # test fixture, not a real credential
        )
    )
    assert second.account_id == first.account_id
    assert second.outcome == "adopted_strong"
    # the adopt also wrote tok-B's source_native mapping onto the same account
    row = db.conn.execute(
        "SELECT account_id FROM app.account_links WHERE ref_kind = 'source_native' "
        "AND source_type = ? AND source_origin = ? AND ref_value = ?",
        ["plaid", "plaid_conn_2", "tok-B"],
    ).fetchone()
    assert row == (first.account_id,)


def test_scoped_full_number_auto_adopts_ofx_then_csv(db: Database) -> None:
    """OFX scoped full_number is a strong confirmer a later CSV auto-adopts onto."""
    resolver = AccountResolver(db, actor="system")
    ofx = resolver.resolve(
        _src(
            source_type="ofx",
            source_account_key="ofx-4267",
            account_number="wells_fargo:111000:4267",  # scoped composite
            explicit_account_id="acct_ofx_1",
        )
    )
    csv = resolver.resolve(
        _src(
            source_type="csv",
            source_account_key="wf-checking",
            account_number="wells_fargo:111000:4267",
        )
    )
    assert csv.account_id == ofx.account_id
    assert csv.outcome == "adopted_strong"
