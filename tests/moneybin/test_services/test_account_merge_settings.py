"""An accepted account merge carries the absorbed account's settings onto the survivor.

Rule under test: the surviving account wins, the duplicate fills gaps. Nine
fields move under it; ``archived``, ``archived_at`` and ``include_in_net_worth``
never do. Values are synthetic throughout.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from moneybin.database import Database
from moneybin.mcp.tools import accounts as mcp_accounts
from moneybin.orchestration.refresh import RefreshResult
from moneybin.repositories.account_link_decisions_repo import AccountLinkDecisionsRepo
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.repositories.account_settings_repo import AccountSettingsRepo
from moneybin.services.account_links_service import (
    AccountLinksService,
    plan_settings_reconciliation,
)
from moneybin.services.identity_confirmation import identity_confirm_message
from moneybin.services.mutation_context import operation
from moneybin.services.undo_service import UndoService
from tests.moneybin.db_helpers import create_core_tables

_ABSORBED = "absorbed_acct"
_SURVIVOR = "survivor_acct"
_DECISION = "merge_dec_001"


@pytest.fixture(autouse=True)
def rematch(mocker: MockerFixture) -> MagicMock:
    """Stand in for the post-merge match + transform; the mechanics are the claim."""
    return mocker.patch(
        "moneybin.orchestration.refresh.refresh",
        return_value=RefreshResult(applied=True, duration_seconds=0.0),
    )


@pytest.fixture(autouse=True)
def restate_ledger(mocker: MockerFixture) -> MagicMock:
    return mocker.patch(
        "moneybin.services.fx_accounting_refresh.restate_investment_ledger"
    )


@pytest.fixture(autouse=True)
def restate_fx(mocker: MockerFixture) -> MagicMock:
    return mocker.patch("moneybin.services.fx_accounting_refresh.restate_fx_accounting")


@pytest.fixture()
def svc(db: Database) -> AccountLinksService:
    create_core_tables(db)
    for account_id in (_ABSORBED, _SURVIVOR):
        db.execute(
            "INSERT INTO core.dim_accounts (account_id, display_name, source_type) "
            "VALUES (?, ?, 'csv')",
            [account_id, account_id],
        )
    AccountLinksRepo(db).insert(
        link_id="link_absorbed",
        account_id=_ABSORBED,
        ref_kind="source_native",
        ref_value="native-ref-absorbed",
        source_type="csv",
        source_origin="bank_a",
        decided_by="auto",
        actor="system",
        status="accepted",
    )
    AccountLinkDecisionsRepo(db).insert(
        decision_id=_DECISION,
        provisional_account_id=_ABSORBED,
        candidate_account_id=_SURVIVOR,
        confidence_score=0.9,
        match_signals={"signal": "institution_last4", "value": "***"},
        decided_by="auto",
        actor="system",
        status="pending",
    )
    return AccountLinksService(db, actor="cli")


def _put(db: Database, account_id: str, **fields: Any) -> None:
    """Write a settings row through the repo (defaults: nothing set, active)."""
    values: dict[str, Any] = {
        "display_name": None,
        "official_name": None,
        "last_four": None,
        "account_subtype": None,
        "holder_category": None,
        "currency_code": None,
        "credit_limit": None,
        "archived": False,
        "archived_at": None,
        "include_in_net_worth": True,
        "default_cost_basis_method": None,
        "investment_source_type": None,
        "investment_source_type_changed_at": None,
    }
    values.update(fields)
    AccountSettingsRepo(db).set(account_id=account_id, actor="cli", **values)


def _row(db: Database, account_id: str) -> dict[str, Any] | None:
    return AccountSettingsRepo(db).load(account_id)


def _settings_audit_rows(db: Database, account_id: str) -> int:
    row = db.execute(
        "SELECT COUNT(*) FROM app.audit_log "
        "WHERE action = 'account_settings.set' AND target_id = ?",
        [account_id],
    ).fetchone()
    assert row is not None
    return int(row[0])


_ABSORBED_VALUES: dict[str, Any] = {
    "display_name": "Vacation Fund",
    "official_name": "Synthetic Savings Plus",
    "last_four": "1111",
    "account_subtype": "savings",
    "holder_category": "joint",
    "currency_code": "EUR",
    "credit_limit": Decimal("2500.00"),
    "default_cost_basis_method": "hifo",
    "investment_source_type": "manual",
}


def test_a_survivor_with_no_settings_row_receives_all_nine_fields(
    svc: AccountLinksService, db: Database
) -> None:
    _put(db, _ABSORBED, **_ABSORBED_VALUES)
    assert _row(db, _SURVIVOR) is None

    svc.set(_DECISION, target_account_id=_SURVIVOR)

    row = _row(db, _SURVIVOR)
    assert row is not None
    assert {k: row[k] for k in _ABSORBED_VALUES} == _ABSORBED_VALUES
    # What does not move takes the table's defaults.
    assert (row["archived"], row["archived_at"], row["include_in_net_worth"]) == (
        False,
        None,
        True,
    )


def test_a_value_only_the_survivor_has_is_kept(
    svc: AccountLinksService, db: Database
) -> None:
    _put(db, _SURVIVOR, display_name="Household Checking", credit_limit=Decimal("900"))
    _put(db, _ABSORBED, last_four="2222")

    svc.set(_DECISION, target_account_id=_SURVIVOR)

    row = _row(db, _SURVIVOR)
    assert row is not None
    assert row["display_name"] == "Household Checking"
    assert row["credit_limit"] == Decimal("900.00")
    assert row["last_four"] == "2222"


def test_on_a_conflict_the_survivor_keeps_its_own_and_other_gaps_still_fill(
    svc: AccountLinksService, db: Database
) -> None:
    _put(db, _SURVIVOR, display_name="Household Checking", currency_code="USD")
    _put(
        db,
        _ABSORBED,
        display_name="Vacation Fund",
        currency_code="EUR",
        holder_category="business",
    )

    svc.set(_DECISION, target_account_id=_SURVIVOR)

    row = _row(db, _SURVIVOR)
    assert row is not None
    assert (row["display_name"], row["currency_code"]) == ("Household Checking", "USD")
    assert row["holder_category"] == "business"


def test_archived_and_excluded_flags_stay_behind(
    svc: AccountLinksService, db: Database
) -> None:
    _put(db, _SURVIVOR, display_name="Household Checking")
    _put(
        db,
        _ABSORBED,
        official_name="Synthetic Checking",
        archived=True,
        archived_at=date(2026, 3, 1),
        include_in_net_worth=False,
    )

    svc.set(_DECISION, target_account_id=_SURVIVOR)

    row = _row(db, _SURVIVOR)
    assert row is not None
    assert row["official_name"] == "Synthetic Checking"
    assert (row["archived"], row["archived_at"], row["include_in_net_worth"]) == (
        False,
        None,
        True,
    )


def test_a_survivors_own_archive_state_survives_the_merge(
    svc: AccountLinksService, db: Database
) -> None:
    _put(
        db,
        _SURVIVOR,
        archived=True,
        archived_at=date(2026, 2, 1),
        include_in_net_worth=False,
    )
    _put(db, _ABSORBED, official_name="Synthetic Checking")

    svc.set(_DECISION, target_account_id=_SURVIVOR)

    row = _row(db, _SURVIVOR)
    assert row is not None
    assert (row["archived"], row["archived_at"], row["include_in_net_worth"]) == (
        True,
        date(2026, 2, 1),
        False,
    )


def test_the_absorbed_row_is_left_untouched(
    svc: AccountLinksService, db: Database
) -> None:
    _put(db, _ABSORBED, **_ABSORBED_VALUES)
    before = _row(db, _ABSORBED)

    svc.set(_DECISION, target_account_id=_SURVIVOR)

    assert _row(db, _ABSORBED) == before


def test_a_moved_investment_source_is_stamped_and_restates_the_ledger_after_commit(
    svc: AccountLinksService, db: Database, restate_ledger: MagicMock
) -> None:
    _put(db, _ABSORBED, investment_source_type="manual", currency_code="EUR")
    began = db.execute("SELECT NOW()::TIMESTAMP").fetchone()[0]  # type: ignore[index]  # NOW() always returns a row

    svc.set(_DECISION, target_account_id=_SURVIVOR)

    row = _row(db, _SURVIVOR)
    assert row is not None
    assert row["investment_source_type"] == "manual"
    assert isinstance(row["investment_source_type_changed_at"], datetime)
    assert row["investment_source_type_changed_at"] >= began
    restate_ledger.assert_called_once_with(db)


def test_a_moved_currency_restates_fx_accounting_not_the_ledger(
    svc: AccountLinksService,
    db: Database,
    restate_ledger: MagicMock,
    restate_fx: MagicMock,
) -> None:
    _put(db, _ABSORBED, currency_code="EUR", display_name="Vacation Fund")

    svc.set(_DECISION, target_account_id=_SURVIVOR)

    restate_ledger.assert_not_called()
    restate_fx.assert_called_once_with(db, account_currency_changed=True)


def test_a_moved_cost_basis_method_restates_fx_without_a_currency_change(
    svc: AccountLinksService, db: Database, restate_fx: MagicMock
) -> None:
    _put(db, _ABSORBED, default_cost_basis_method="hifo")

    svc.set(_DECISION, target_account_id=_SURVIVOR)

    restate_fx.assert_called_once_with(db, account_currency_changed=False)


def test_a_label_only_move_restates_nothing(
    svc: AccountLinksService,
    db: Database,
    restate_ledger: MagicMock,
    restate_fx: MagicMock,
) -> None:
    _put(db, _ABSORBED, display_name="Vacation Fund")

    svc.set(_DECISION, target_account_id=_SURVIVOR)

    restate_ledger.assert_not_called()
    restate_fx.assert_not_called()


def test_the_batched_path_restates_when_its_caller_runs_the_post_commit_pass(
    svc: AccountLinksService, db: Database, restate_ledger: MagicMock
) -> None:
    """Under ``in_outer_txn`` the caller owns the post-commit work, as for the re-match."""
    _put(db, _ABSORBED, investment_source_type="manual")

    db.begin()
    svc.set(_DECISION, target_account_id=_SURVIVOR, in_outer_txn=True)
    db.commit()
    restate_ledger.assert_not_called()

    svc.rematch_after_merge()

    restate_ledger.assert_called_once_with(db)
    svc.rematch_after_merge()
    restate_ledger.assert_called_once()


def test_a_rolled_back_merge_owes_no_restatement(
    svc: AccountLinksService, db: Database, restate_ledger: MagicMock
) -> None:
    _put(db, _ABSORBED, investment_source_type="manual")

    def refuse(_impact: Any) -> None:
        raise RuntimeError("refused")

    with pytest.raises(RuntimeError):
        svc.set(_DECISION, target_account_id=_SURVIVOR, verify_accept=refuse)

    assert _row(db, _SURVIVOR) is None
    svc.rematch_after_merge()
    restate_ledger.assert_not_called()


def test_nothing_to_move_writes_no_settings_audit_row(
    svc: AccountLinksService, db: Database
) -> None:
    _put(db, _SURVIVOR, display_name="Household Checking")
    _put(db, _ABSORBED, display_name="Household Checking", archived=True)
    survivor_before = _settings_audit_rows(db, _SURVIVOR)

    svc.set(_DECISION, target_account_id=_SURVIVOR)

    assert _settings_audit_rows(db, _SURVIVOR) == survivor_before
    assert _row(db, _SURVIVOR) is not None
    assert _row(db, _SURVIVOR)["archived"] is False  # pyright: ignore[reportOptionalSubscript]  # asserted present above


def test_an_absorbed_account_with_no_settings_creates_no_survivor_row(
    svc: AccountLinksService, db: Database
) -> None:
    svc.set(_DECISION, target_account_id=_SURVIVOR)

    assert _row(db, _SURVIVOR) is None
    assert _settings_audit_rows(db, _SURVIVOR) == 0


def test_accept_impact_reports_what_moves_and_what_the_survivor_keeps(
    svc: AccountLinksService, db: Database
) -> None:
    _put(db, _SURVIVOR, display_name="Household Checking", currency_code="USD")
    _put(
        db,
        _ABSORBED,
        display_name="Vacation Fund",
        currency_code="EUR",
        credit_limit=Decimal("500"),
        holder_category="joint",
        archived=True,
    )

    impact = svc.accept_impact(_DECISION, target_account_id=_SURVIVOR)

    assert impact.settings_moved == ("credit_limit", "holder_category")
    assert impact.settings_kept == ("currency_code", "display_name")


def test_the_prompt_names_fields_and_never_values(
    svc: AccountLinksService, db: Database
) -> None:
    _put(db, _SURVIVOR, display_name="Household Checking")
    _put(
        db,
        _ABSORBED,
        display_name="Vacation Fund",
        currency_code="EUR",
        credit_limit=Decimal("500"),
    )

    facts = svc.merge_facts(
        absorbed_account_id=_ABSORBED, survivor_account_id=_SURVIVOR
    )
    cli = identity_confirm_message(
        {"accounts": 2}, surface="cli", merges=[facts], kinds=["account_link"]
    )
    mcp = identity_confirm_message(
        {"accounts": 2}, surface="mcp", merges=[facts], kinds=["account_link"]
    )

    expected = (
        "The surviving account takes the absorbed account's credit limit and "
        "currency, which it has not set. It keeps its own display name where the "
        "two differ. Archived and net-worth settings are never moved."
    )
    assert expected in cli
    assert expected in mcp
    for value in ("Vacation Fund", "EUR", "500", "Household Checking"):
        assert value not in expected


def test_the_prompt_is_silent_when_no_settings_are_in_play(
    svc: AccountLinksService,
) -> None:
    facts = svc.merge_facts(
        absorbed_account_id=_ABSORBED, survivor_account_id=_SURVIVOR
    )

    message = identity_confirm_message(
        {"accounts": 2}, surface="cli", merges=[facts], kinds=["account_link"]
    )

    assert "settings" not in message


def test_the_mcp_grant_binds_the_planned_settings_moves(
    svc: AccountLinksService, db: Database
) -> None:
    _put(db, _ABSORBED, display_name="Vacation Fund")
    before = svc.accept_impact(_DECISION, target_account_id=_SURVIVOR)
    binding_before = mcp_accounts._account_link_binding(  # pyright: ignore[reportPrivateUsage]  # test exercises confirmation seam
        decision_id=_DECISION, target_account_id=_SURVIVOR, impact=before
    )

    _put(db, _ABSORBED, display_name="Vacation Fund", credit_limit=Decimal("500"))
    after = svc.accept_impact(_DECISION, target_account_id=_SURVIVOR)
    binding_after = mcp_accounts._account_link_binding(  # pyright: ignore[reportPrivateUsage]  # test exercises confirmation seam
        decision_id=_DECISION, target_account_id=_SURVIVOR, impact=after
    )

    assert binding_before.arguments["settings_moved"] == ["display_name"]
    assert binding_after.arguments["settings_moved"] == ["credit_limit", "display_name"]
    assert binding_before != binding_after


def test_the_plan_is_deterministic_and_ignores_blank_text() -> None:
    plan = plan_settings_reconciliation(
        {"display_name": "Vacation Fund", "official_name": "  ", "last_four": "1111"},
        {"display_name": "", "last_four": "2222"},
    )

    assert plan.moved == ("display_name",)
    assert plan.kept == ("last_four",)


def test_undo_restores_both_rows_without_moving_updated_at_back(
    svc: AccountLinksService, db: Database
) -> None:
    _put(db, _SURVIVOR, display_name="Household Checking")
    _put(
        db,
        _ABSORBED,
        display_name="Vacation Fund",
        investment_source_type="manual",
        currency_code="EUR",
    )
    absorbed_before = _row(db, _ABSORBED)
    survivor_before = _row(db, _SURVIVOR)
    assert survivor_before is not None
    assert absorbed_before is not None

    with operation() as op:
        svc.set(_DECISION, target_account_id=_SURVIVOR)
    merged = _row(db, _SURVIVOR)
    assert merged is not None
    assert merged["investment_source_type"] == "manual"

    UndoService(db).undo(op, actor="cli")

    survivor_after = _row(db, _SURVIVOR)
    assert survivor_after is not None
    assert _row(db, _ABSORBED) == absorbed_before
    # Every captured column returns to its pre-merge value. The two source
    # columns are the exception: undoing a source change advances the change
    # time (so re-entering ledger rows are never rewound).
    unchanged = {
        k: v
        for k, v in survivor_before.items()
        if k != "investment_source_type_changed_at"
    }
    assert {k: survivor_after[k] for k in unchanged} == unchanged
    assert survivor_after["investment_source_type"] is None
    assert (
        survivor_after["investment_source_type_changed_at"]
        >= (merged["investment_source_type_changed_at"])
    )
    # The row's own updated_at is restored with the rest of the image; the
    # watermark that must not rewind is the change time, which dim_accounts
    # and the ledger fold in (asserted above).
    assert survivor_after["updated_at"] == survivor_before["updated_at"]


def test_undo_of_a_merge_that_created_a_survivor_row_holding_a_source_resets_it(
    svc: AccountLinksService, db: Database
) -> None:
    """The row carries a change time, so undo resets it to defaults, not deletes."""
    _put(db, _ABSORBED, investment_source_type="manual", display_name="Vacation Fund")
    absorbed_before = _row(db, _ABSORBED)
    with operation() as op:
        svc.set(_DECISION, target_account_id=_SURVIVOR)
    merged = _row(db, _SURVIVOR)
    assert merged is not None

    UndoService(db).undo(op, actor="cli")

    survivor = _row(db, _SURVIVOR)
    assert survivor is not None
    assert survivor["investment_source_type"] is None
    assert survivor["display_name"] is None
    assert (survivor["archived"], survivor["include_in_net_worth"]) == (False, True)
    assert (
        survivor["investment_source_type_changed_at"]
        > (merged["investment_source_type_changed_at"])
    )
    assert _row(db, _ABSORBED) == absorbed_before
    assert _decision_status(db, _DECISION) == "pending"


def test_undo_of_a_merge_that_created_a_survivor_row_without_a_source_deletes_it(
    svc: AccountLinksService, db: Database
) -> None:
    _put(db, _ABSORBED, display_name="Vacation Fund")
    with operation() as op:
        svc.set(_DECISION, target_account_id=_SURVIVOR)
    assert _row(db, _SURVIVOR) is not None

    UndoService(db).undo(op, actor="cli")

    assert _row(db, _SURVIVOR) is None
    assert _decision_status(db, _DECISION) == "pending"


def _decision_status(db: Database, decision_id: str) -> str | None:
    row = db.execute(
        "SELECT status FROM app.account_link_decisions WHERE decision_id = ?",
        [decision_id],
    ).fetchone()
    return row[0] if row else None
