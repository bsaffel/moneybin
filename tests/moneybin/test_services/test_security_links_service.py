"""Tests for ``SecurityLinksService`` — merge accept/reject.

Accept is the app-state cascade for a provisional-security merge: repoint every
accepted provider ref onto the survivor, migrate ``app.lot_selections`` (whose
``lot_id`` hashes ``security_id``), auto-reject sibling candidates, delete the
provisional catalog row — all in ONE transaction. An unremappable selection
blocks the merge rather than silently downgrading a specific-ID election to FIFO.

Core tables don't exist in a unit-test DB (SQLMesh owns them), so the two the
remap reads are fabricated here.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.investments.cost_basis import compute_lot_id
from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.repositories.security_link_decisions_repo import SecurityLinkDecisionsRepo
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.services.security_links_service import SecurityLinksService

_REF_VALUE = "sec_1"
_REF_KIND = "plaid_security_id"


def _create_core_tables(db: Database) -> None:
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS core.fct_investment_lots (
            lot_id VARCHAR, account_id VARCHAR, security_id VARCHAR,
            acquisition_date DATE, source_transaction_id VARCHAR
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS core.fct_investment_transactions (
            investment_transaction_id VARCHAR, security_id VARCHAR
        )
        """
    )


def _mint(db: Database, *, name: str, created_by: str) -> str:
    event = SecuritiesRepo(db).upsert(
        security_id=None,
        name=name,
        security_type="etf",
        created_by=created_by,
        actor="system" if created_by == "plaid" else "cli",
    )
    assert event.target_id is not None
    return event.target_id


def _add_lot(
    db: Database,
    *,
    security_id: str,
    account_id: str = "acc_1",
    acquisition_date: date = date(2024, 3, 1),
    source_transaction_id: str = "itx_buy",
) -> str:
    lot_id = compute_lot_id(
        account_id, security_id, acquisition_date, source_transaction_id
    )
    db.execute(
        "INSERT INTO core.fct_investment_lots VALUES (?, ?, ?, ?, ?)",
        [lot_id, account_id, security_id, acquisition_date, source_transaction_id],
    )
    return lot_id


def _add_disposal(db: Database, txn_id: str, security_id: str) -> None:
    db.execute(
        "INSERT INTO core.fct_investment_transactions VALUES (?, ?)",
        [txn_id, security_id],
    )


def _accepted_binding(db: Database, ref_value: str = _REF_VALUE) -> str | None:
    return SecurityLinksRepo(db).lookup(
        ref_kind=_REF_KIND, ref_value=ref_value, source_type="plaid"
    )


def _security_exists(db: Database, security_id: str) -> bool:
    row = db.execute(
        "SELECT COUNT(*) FROM app.securities WHERE security_id = ?", [security_id]
    ).fetchone()
    return row is not None and row[0] == 1


@pytest.fixture
def merge_setup(db: Database) -> dict[str, str]:
    """Provisional (plaid-minted) security bound to ``sec_1``, proposed to merge."""
    _create_core_tables(db)
    survivor = _mint(db, name="Vanguard Total Stock Market ETF", created_by="user")
    provisional = _mint(db, name="Vanguard Total Stock Mkt ETF", created_by="plaid")
    SecurityLinksRepo(db).insert(
        security_id=provisional,
        ref_kind=_REF_KIND,
        ref_value=_REF_VALUE,
        source_type="plaid",
        decided_by="auto",
        actor="system",
    )
    event = SecurityLinkDecisionsRepo(db).insert(
        ref_kind=_REF_KIND,
        ref_value=_REF_VALUE,
        source_type="plaid",
        candidate_security_id=survivor,
        actor="system",
    )
    assert event.target_id is not None
    return {
        "survivor": survivor,
        "provisional": provisional,
        "decision_id": event.target_id,
    }


# ---------------------------------------------------------------- accept


def test_accept_rebinds_deletes_and_accepts(
    db: Database, merge_setup: dict[str, str]
) -> None:
    SecurityLinksService(db).accept_merge(merge_setup["decision_id"])

    assert _accepted_binding(db) == merge_setup["survivor"]
    assert not _security_exists(db, merge_setup["provisional"])
    assert _security_exists(db, merge_setup["survivor"])
    assert SecurityLinkDecisionsRepo(db).count_pending() == 0
    decision = SecurityLinkDecisionsRepo(db).fetch_by_id(merge_setup["decision_id"])
    assert decision is not None
    assert decision["status"] == "accepted"
    assert decision["decided_by"] == "user"


def test_accept_repoints_every_accepted_ref(
    db: Database, merge_setup: dict[str, str]
) -> None:
    """The institution ref rides along — leaving it on a deleted security orphans it."""
    SecurityLinksRepo(db).insert(
        security_id=merge_setup["provisional"],
        ref_kind="institution_security_id",
        ref_value="ins_1|VTI",
        source_type="plaid",
        decided_by="auto",
        actor="system",
    )

    SecurityLinksService(db).accept_merge(merge_setup["decision_id"])

    assert _accepted_binding(db) == merge_setup["survivor"]
    assert (
        SecurityLinksRepo(db).lookup(
            ref_kind="institution_security_id",
            ref_value="ins_1|VTI",
            source_type="plaid",
        )
        == merge_setup["survivor"]
    )
    orphaned = db.execute(
        "SELECT COUNT(*) FROM app.security_links "
        "WHERE security_id = ? AND status = 'accepted'",
        [merge_setup["provisional"]],
    ).fetchone()
    assert orphaned is not None and orphaned[0] == 0


def test_accept_migrates_lot_selection(
    db: Database, merge_setup: dict[str, str]
) -> None:
    provisional = merge_setup["provisional"]
    old_lot = _add_lot(db, security_id=provisional)
    _add_disposal(db, "itx_sell", provisional)
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="itx_sell",
        selections=[(old_lot, Decimal("5"))],
        actor="cli",
    )

    SecurityLinksService(db).accept_merge(merge_setup["decision_id"])

    new_lot = compute_lot_id(
        "acc_1", merge_setup["survivor"], date(2024, 3, 1), "itx_buy"
    )
    assert LotSelectionsRepo(db).list_for_disposal("itx_sell") == [
        (new_lot, Decimal("5"))
    ]


def test_accept_preserves_untouched_selections_in_a_migrated_disposal(
    db: Database, merge_setup: dict[str, str]
) -> None:
    """A same-disposal selection already on the survivor survives the replace.

    ``set_for_disposal`` replaces the WHOLE set for a disposal — rewriting it
    with only the remapped rows would silently drop the others.
    """
    provisional, survivor = merge_setup["provisional"], merge_setup["survivor"]
    prov_lot = _add_lot(db, security_id=provisional)
    surv_lot = _add_lot(
        db, security_id=survivor, source_transaction_id="itx_buy_survivor"
    )
    _add_disposal(db, "itx_sell", provisional)
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="itx_sell",
        selections=[(prov_lot, Decimal("5")), (surv_lot, Decimal("2"))],
        actor="cli",
    )

    SecurityLinksService(db).accept_merge(merge_setup["decision_id"])

    remapped = compute_lot_id("acc_1", survivor, date(2024, 3, 1), "itx_buy")
    assert sorted(LotSelectionsRepo(db).list_for_disposal("itx_sell")) == sorted([
        (remapped, Decimal("5")),
        (surv_lot, Decimal("2")),
    ])


def test_accept_auto_rejects_sibling_decisions(
    db: Database, merge_setup: dict[str, str]
) -> None:
    other = _mint(db, name="Vanguard Total Market Index", created_by="user")
    sibling = SecurityLinkDecisionsRepo(db).insert(
        ref_kind=_REF_KIND,
        ref_value=_REF_VALUE,
        source_type="plaid",
        candidate_security_id=other,
        actor="system",
    )
    assert sibling.target_id is not None
    # A pending decision for a DIFFERENT ref must not be swept up.
    unrelated = SecurityLinkDecisionsRepo(db).insert(
        ref_kind=_REF_KIND,
        ref_value="sec_2",
        source_type="plaid",
        candidate_security_id=other,
        actor="system",
    )
    assert unrelated.target_id is not None

    SecurityLinksService(db).accept_merge(merge_setup["decision_id"])

    repo = SecurityLinkDecisionsRepo(db)
    sibling_row = repo.fetch_by_id(sibling.target_id)
    assert sibling_row is not None and sibling_row["status"] == "rejected"
    unrelated_row = repo.fetch_by_id(unrelated.target_id)
    assert unrelated_row is not None and unrelated_row["status"] == "pending"
    assert repo.count_pending() == 1


def test_accept_audit_chain_shares_one_parent(
    db: Database, merge_setup: dict[str, str]
) -> None:
    """Every child write carries the decision-update's audit id as parent."""
    provisional = merge_setup["provisional"]
    old_lot = _add_lot(db, security_id=provisional)
    _add_disposal(db, "itx_sell", provisional)
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="itx_sell",
        selections=[(old_lot, Decimal("5"))],
        actor="cli",
    )

    SecurityLinksService(db).accept_merge(merge_setup["decision_id"])

    parent = db.execute(
        """
        SELECT audit_id FROM app.audit_log
        WHERE action = 'security_link_decision.update_status' AND target_id = ?
        """,
        [merge_setup["decision_id"]],
    ).fetchone()
    assert parent is not None
    children = db.execute(
        """
        SELECT action FROM app.audit_log
        WHERE parent_audit_id = ?
        ORDER BY action
        """,
        [parent[0]],
    ).fetchall()
    actions = {row[0] for row in children}
    assert {
        "lot_selections.set",
        "security_link.repoint",
        "securities.delete",
    } <= actions


# ---------------------------------------------------------------- block


def test_unremappable_selection_blocks_merge(
    db: Database, merge_setup: dict[str, str]
) -> None:
    _add_disposal(db, "itx_sell", merge_setup["provisional"])
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="itx_sell",
        selections=[("lot_gone000000", Decimal("5"))],
        actor="cli",
    )

    with pytest.raises(UserError, match="cannot be deterministically remapped"):
        SecurityLinksService(db).accept_merge(merge_setup["decision_id"])

    # Nothing changed: binding still on the provisional, decision still pending,
    # provisional catalog row intact, selection untouched.
    assert _accepted_binding(db) == merge_setup["provisional"]
    assert SecurityLinkDecisionsRepo(db).count_pending() == 1
    assert _security_exists(db, merge_setup["provisional"])
    assert LotSelectionsRepo(db).list_for_disposal("itx_sell") == [
        ("lot_gone000000", Decimal("5"))
    ]


def test_selection_on_a_third_securitys_lot_blocks_merge(
    db: Database, merge_setup: dict[str, str]
) -> None:
    """A resolvable lot_id on an unrelated security is still unremappable.

    The disposal re-keys onto the survivor; a lot from a third security never
    lands in the survivor's pool, so the election would silently become FIFO.
    """
    third = _mint(db, name="Some Other Fund", created_by="user")
    foreign_lot = _add_lot(db, security_id=third)
    _add_disposal(db, "itx_sell", merge_setup["provisional"])
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="itx_sell",
        selections=[(foreign_lot, Decimal("5"))],
        actor="cli",
    )

    with pytest.raises(UserError, match="cannot be deterministically remapped"):
        SecurityLinksService(db).accept_merge(merge_setup["decision_id"])

    assert _accepted_binding(db) == merge_setup["provisional"]
    assert SecurityLinkDecisionsRepo(db).count_pending() == 1


def test_accept_blocks_when_core_absent_and_selections_exist(
    db: Database, merge_setup: dict[str, str]
) -> None:
    """Without core, remappability is unverifiable — refuse rather than guess."""
    db.execute("DROP TABLE core.fct_investment_lots")
    db.execute("DROP TABLE core.fct_investment_transactions")
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="itx_sell",
        selections=[("lot_abcdef0000", Decimal("5"))],
        actor="cli",
    )

    with pytest.raises(UserError, match="not been materialized"):
        SecurityLinksService(db).accept_merge(merge_setup["decision_id"])

    assert _accepted_binding(db) == merge_setup["provisional"]
    assert SecurityLinkDecisionsRepo(db).count_pending() == 1


def test_accept_proceeds_when_core_absent_and_no_selections(
    db: Database, merge_setup: dict[str, str]
) -> None:
    db.execute("DROP TABLE core.fct_investment_lots")
    db.execute("DROP TABLE core.fct_investment_transactions")

    SecurityLinksService(db).accept_merge(merge_setup["decision_id"])

    assert _accepted_binding(db) == merge_setup["survivor"]
    assert not _security_exists(db, merge_setup["provisional"])


# ---------------------------------------------------------------- guards


def test_accept_twice_raises(db: Database, merge_setup: dict[str, str]) -> None:
    service = SecurityLinksService(db)
    service.accept_merge(merge_setup["decision_id"])

    with pytest.raises(UserError, match="not pending"):
        service.accept_merge(merge_setup["decision_id"])


def test_accept_unknown_decision_raises(db: Database) -> None:
    with pytest.raises(UserError, match="No security-link decision"):
        SecurityLinksService(db).accept_merge("deadbeef0000")


def test_accept_raises_when_ref_is_unbound(
    db: Database, merge_setup: dict[str, str]
) -> None:
    link_id = db.execute(
        "SELECT link_id FROM app.security_links WHERE status = 'accepted'"
    ).fetchone()
    assert link_id is not None
    SecurityLinksRepo(db).reverse(
        link_id=str(link_id[0]), reversed_by="user", actor="cli"
    )

    with pytest.raises(UserError, match="No accepted binding"):
        SecurityLinksService(db).accept_merge(merge_setup["decision_id"])

    assert SecurityLinkDecisionsRepo(db).count_pending() == 1


def test_accept_raises_when_candidate_is_missing(
    db: Database, merge_setup: dict[str, str]
) -> None:
    """A dangling candidate must not become the ref's new binding."""
    db.execute(
        "DELETE FROM app.securities WHERE security_id = ?", [merge_setup["survivor"]]
    )

    with pytest.raises(UserError, match="No security found"):
        SecurityLinksService(db).accept_merge(merge_setup["decision_id"])

    assert _accepted_binding(db) == merge_setup["provisional"]
    assert SecurityLinkDecisionsRepo(db).count_pending() == 1


def test_accept_raises_when_ref_is_already_bound_to_the_candidate(
    db: Database, merge_setup: dict[str, str]
) -> None:
    """Nothing to merge — and repointing a link onto itself is not a no-op."""
    link_id = db.execute(
        "SELECT link_id FROM app.security_links WHERE status = 'accepted'"
    ).fetchone()
    assert link_id is not None
    SecurityLinksRepo(db).repoint(
        link_id=str(link_id[0]),
        new_security_id=merge_setup["survivor"],
        decided_by="user",
        actor="cli",
    )

    with pytest.raises(UserError, match="already bound"):
        SecurityLinksService(db).accept_merge(merge_setup["decision_id"])

    assert SecurityLinkDecisionsRepo(db).count_pending() == 1


# ---------------------------------------------------------------- reject


def test_reject_keeps_minted_security(
    db: Database, merge_setup: dict[str, str]
) -> None:
    SecurityLinksService(db).reject_merge(merge_setup["decision_id"])

    assert _security_exists(db, merge_setup["provisional"])
    assert _accepted_binding(db) == merge_setup["provisional"]
    assert SecurityLinkDecisionsRepo(db).count_pending() == 0
    decision = SecurityLinkDecisionsRepo(db).fetch_by_id(merge_setup["decision_id"])
    assert decision is not None and decision["status"] == "rejected"


def test_reject_leaves_sibling_candidates_pending(
    db: Database, merge_setup: dict[str, str]
) -> None:
    """Rejecting one candidate does not answer the others for the same ref."""
    other = _mint(db, name="Vanguard Total Market Index", created_by="user")
    sibling = SecurityLinkDecisionsRepo(db).insert(
        ref_kind=_REF_KIND,
        ref_value=_REF_VALUE,
        source_type="plaid",
        candidate_security_id=other,
        actor="system",
    )
    assert sibling.target_id is not None

    SecurityLinksService(db).reject_merge(merge_setup["decision_id"])

    row = SecurityLinkDecisionsRepo(db).fetch_by_id(sibling.target_id)
    assert row is not None and row["status"] == "pending"
    assert SecurityLinkDecisionsRepo(db).count_pending() == 1


def test_reject_twice_raises(db: Database, merge_setup: dict[str, str]) -> None:
    service = SecurityLinksService(db)
    service.reject_merge(merge_setup["decision_id"])

    with pytest.raises(UserError, match="not pending"):
        service.reject_merge(merge_setup["decision_id"])


def test_reject_unknown_decision_raises(db: Database) -> None:
    with pytest.raises(UserError, match="No security-link decision"):
        SecurityLinksService(db).reject_merge("deadbeef0000")


# ---------------------------------------------------- failure injection


def _fail(*_args: Any, **_kwargs: Any) -> None:
    raise RuntimeError("injected failure")


@pytest.mark.parametrize(
    ("repo_cls", "method"),
    [
        (SecuritiesRepo, "delete"),
        (SecurityLinksRepo, "repoint"),
        (LotSelectionsRepo, "set_for_disposal"),
    ],
)
def test_accept_rolls_back_entirely_on_any_write_failure(
    db: Database,
    merge_setup: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    repo_cls: type,
    method: str,
) -> None:
    """A merge either fully applies or leaves nothing behind — never a half-merge."""
    provisional = merge_setup["provisional"]
    old_lot = _add_lot(db, security_id=provisional)
    _add_disposal(db, "itx_sell", provisional)
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="itx_sell",
        selections=[(old_lot, Decimal("5"))],
        actor="cli",
    )
    sibling = SecurityLinkDecisionsRepo(db).insert(
        ref_kind=_REF_KIND,
        ref_value=_REF_VALUE,
        source_type="plaid",
        candidate_security_id=_mint(db, name="Another Fund", created_by="user"),
        actor="system",
    )
    assert sibling.target_id is not None
    monkeypatch.setattr(repo_cls, method, _fail)

    with pytest.raises(RuntimeError, match="injected failure"):
        SecurityLinksService(db).accept_merge(merge_setup["decision_id"])

    repo = SecurityLinkDecisionsRepo(db)
    assert _accepted_binding(db) == provisional
    assert _security_exists(db, provisional)
    assert repo.count_pending() == 2
    decision = repo.fetch_by_id(merge_setup["decision_id"])
    assert decision is not None and decision["status"] == "pending"
    assert LotSelectionsRepo(db).list_for_disposal("itx_sell") == [
        (old_lot, Decimal("5"))
    ]
    reversed_links = db.execute(
        "SELECT COUNT(*) FROM app.security_links WHERE status = 'reversed'"
    ).fetchone()
    assert reversed_links is not None and reversed_links[0] == 0


# ---------------------------------------------------------------- reads


def test_read_methods_delegate_to_the_decisions_repo(
    db: Database, merge_setup: dict[str, str]
) -> None:
    service = SecurityLinksService(db)

    assert service.count_pending() == 1
    pending = service.list_pending()
    assert [row["decision_id"] for row in pending] == [merge_setup["decision_id"]]
    assert len(service.history(limit=10)) == 1
