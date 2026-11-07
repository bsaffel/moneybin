"""Recording returns current Golden identities, with honest postcommit recovery."""

from datetime import date
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.services.investment_service import InvestmentService
from moneybin.services.transform_service import ApplyResult, TransformService
from tests.moneybin.test_stg_plaid_investments import (
    _manual_investment_txn,  # pyright: ignore[reportPrivateUsage]  # synthetic recording prerequisite
    _publish_investment_fixture,  # pyright: ignore[reportPrivateUsage]  # real refresh
)


def recording_profile(db: Database) -> InvestmentService:
    _manual_investment_txn(db, account_id="account", security_id="security")
    _publish_investment_fixture(db)
    return InvestmentService(db)


def event(*, amount: str = "20") -> dict[str, Any]:
    return {
        "account_ref": "account",
        "security_ref": None,
        "type_": "deposit",
        "subtype": None,
        "trade_date": date(2026, 7, 9),
        "quantity": None,
        "price": None,
        "amount": Decimal(amount),
        "fees": None,
        "acquired": None,
        "basis": None,
        "currency_code": "USD",
        "description": None,
    }


@pytest.mark.parametrize("batch", [False, True])
def test_record_returns_materialized_golden_ids_in_input_order(
    db: Database, batch: bool
) -> None:
    service = recording_profile(db)
    apply = TransformService.apply
    calls: list[str] = []

    def rebuild(target: TransformService, *, actor: str = "system") -> ApplyResult:
        calls.append(actor)
        return apply(target, actor=actor)

    with patch.object(TransformService, "apply", autospec=True, side_effect=rebuild):
        written = (
            service.record_events(
                [event(), event(amount="30")], actor="test", created_by="cli"
            ).investment_transaction_ids
            if batch
            else service.record_event(**event(), actor="test", created_by="cli")
        )
    assert calls == ["test"]
    assert len(written) == (2 if batch else 1)
    for identity, expected in zip(
        written, [Decimal("20"), Decimal("30")], strict=False
    ):
        assert db.execute(
            "SELECT amount FROM core.fct_investment_transactions WHERE investment_transaction_id = ?",
            [identity],
        ).fetchone() == (expected,)
        assert (
            db.execute(
                "SELECT 1 FROM raw.manual_investment_transactions WHERE investment_transaction_id = ?",
                [identity],
            ).fetchone()
            is None
        )
        assert db.execute(
            "SELECT COUNT(*) FROM app.investment_event_members WHERE investment_transaction_id = ? AND retired_at IS NULL",
            [identity],
        ).fetchone() == (1,)


@pytest.mark.parametrize("raises", [False, True])
def test_record_rebuild_failure_keeps_saved_batch_and_refuses_duplicate_retry(
    db: Database, raises: bool
) -> None:
    service = recording_profile(db)
    with patch.object(
        TransformService,
        "apply",
        side_effect=RuntimeError("injected rebuild failure") if raises else None,
        return_value=ApplyResult(
            applied=False, duration_seconds=0, error="injected rebuild failure"
        ),
    ):
        with pytest.raises(UserError, match="saved") as caught:
            service.record_events(
                [event(), event(amount="30")], actor="test", created_by="cli"
            )
    assert caught.value.code == "refresh_model_failed"
    assert "rebuild" in str(caught.value).lower()
    assert "record" in str(caught.value.hint).lower()
    assert "again" in str(caught.value.hint).lower()
    assert db.execute(
        "SELECT COUNT(*) FROM raw.manual_investment_transactions WHERE type = 'deposit'"
    ).fetchone() == (2,)
    assert db.execute(
        "SELECT status, rows_imported FROM app.import_log WHERE format_name = 'manual_investment_entry'"
    ).fetchall() == [("complete", 2)]


def test_record_reinvest_returns_both_current_semantic_legs(db: Database) -> None:
    service = recording_profile(db)
    purchase = event(amount="-100")
    purchase.update(
        security_ref="Synthetic Security",
        type_="reinvest",
        subtype="dividend",
        quantity=Decimal(1),
        price=Decimal(100),
    )
    written = service.record_event(**purchase, actor="test", created_by="cli")
    assert len(written) == len(set(written)) == 2
    rows = [
        db.execute(
            "SELECT type, event_group_id FROM core.fct_investment_transactions WHERE investment_transaction_id = ?",
            [identity],
        ).fetchone()
        for identity in written
    ]
    assert all(row is not None for row in rows)
    assert [row[0] for row in rows if row] == ["reinvest", "dividend"]
    assert len({row[1] for row in rows if row}) == 1


def test_all_skipped_record_batch_does_not_rebuild(db: Database) -> None:
    service = recording_profile(db)
    purchase = event(amount="-100")
    purchase.update(
        security_ref="missing synthetic security",
        type_="buy",
        quantity=Decimal(1),
        price=Decimal(100),
    )
    with patch.object(
        TransformService, "apply", side_effect=AssertionError("empty batch rebuilt")
    ):
        result = service.record_events([purchase], actor="test", created_by="cli")
    assert result.investment_transaction_ids == []
    assert len(result.error_details) == 1
    assert db.execute(
        "SELECT COUNT(*) FROM app.import_log WHERE format_name = 'manual_investment_entry'"
    ).fetchone() == (0,)
