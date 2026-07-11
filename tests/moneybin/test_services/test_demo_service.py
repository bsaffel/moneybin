"""Tests for DemoService orchestration.

The heavy collaborators (generator, refresh/SQLMesh, doctor, net worth) are
mocked here so these stay fast and focused on the orchestration *logic*; the
real end-to-end pipeline is proven by the `moneybin demo` e2e test.
"""

import datetime
from collections.abc import Generator
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from moneybin.services.demo_service import (
    DemoResult,
    DemoService,
    ProfileHasNonSyntheticDataError,
)


@pytest.fixture(autouse=True)
def _restore_profile_state() -> Generator[None, None, None]:  # pyright: ignore[reportUnusedFunction]  # pytest autouse fixture
    """DemoService.run switches the process-wide active profile; restore it."""
    from moneybin import config

    original = config._current_profile  # pyright: ignore[reportPrivateUsage]
    try:
        yield
    finally:
        config._current_profile = original  # pyright: ignore[reportPrivateUsage]


def _mock_pipeline(mocker: Any, *, net_worth: str = "100.00", failing: int = 0) -> None:
    """Patch the heavy collaborators DemoService.run imports lazily."""
    from moneybin.privacy.payloads.networth import NetWorthSnapshotPayload
    from moneybin.services.doctor_service import DoctorReport, InvariantResult
    from moneybin.services.refresh import RefreshResult

    engine = mocker.patch("moneybin.synthetic.engine.GeneratorEngine")
    engine.return_value.generate.return_value = SimpleNamespace(
        accounts=[object(), object()], transactions=[]
    )
    writer = mocker.patch("moneybin.synthetic.writer.SyntheticWriter")
    writer.return_value.write.return_value = {"tabular_transactions": 5}

    mocker.patch(
        "moneybin.services.refresh.refresh",
        return_value=RefreshResult(applied=True, duration_seconds=0.0),
    )
    invariants = [
        InvariantResult(name=f"chk{i}", status="fail", detail=None, affected_ids=[])
        for i in range(failing)
    ]
    doctor = mocker.patch("moneybin.services.doctor_service.DoctorService")
    doctor.return_value.run_all.return_value = DoctorReport(
        invariants=invariants, transaction_count=5
    )
    net = mocker.patch("moneybin.services.networth_service.NetworthService")
    net.return_value.current.return_value = NetWorthSnapshotPayload(
        balance_date=datetime.date(2025, 1, 1),
        net_worth=Decimal(net_worth),
        total_assets=Decimal("150.00"),
        total_liabilities=Decimal("50.00"),
        account_count=2,
        per_account=[],
    )


@pytest.mark.integration
def test_run_orchestration_assembles_result(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, mocker: Any
) -> None:
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    _mock_pipeline(mocker, net_worth="12345.67")

    result = DemoService().run(persona="basic", profile="demo", seed=42)

    assert isinstance(result, DemoResult)
    assert result.profile == "demo"
    assert result.persona == "basic"
    assert result.seed == 42
    assert result.account_count == 2
    assert result.transaction_count == 5
    assert result.doctor_failing == 0
    assert result.net_worth == Decimal("12345.67")
    # Profile was created and persisted as the active default.
    from moneybin.utils.user_config import get_default_profile

    assert get_default_profile() == "demo"


@pytest.mark.integration
def test_refuses_non_synthetic_profile(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, mocker: Any
) -> None:
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    _mock_pipeline(mocker)  # would succeed if the guard didn't fire first

    from moneybin.config import set_current_profile
    from moneybin.database import get_database
    from moneybin.services.profile_service import ProfileService

    ProfileService().create("demo", init_inbox=False)
    set_current_profile("demo")
    with get_database(read_only=False) as db:
        db.execute(
            "INSERT INTO raw.tabular_transactions "
            "(transaction_id, account_id, transaction_date, amount, "
            "source_file, source_type, source_origin, import_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ["real1", "acct", "2025-01-01", "10.00", "user.csv", "csv", "user", "imp1"],
        )

    with pytest.raises(ProfileHasNonSyntheticDataError):
        DemoService().run(
            persona="basic", profile="demo", seed=42, reset_confirmed=True
        )


@pytest.mark.integration
def test_profile_has_data(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    svc = DemoService()

    # Nonexistent profile / missing DB → False (no exception).
    assert svc.profile_has_data("demo") is False

    from moneybin.config import set_current_profile
    from moneybin.database import get_database
    from moneybin.services.profile_service import ProfileService

    ProfileService().create("demo", init_inbox=False)
    assert svc.profile_has_data("demo") is False  # created but empty

    set_current_profile("demo")
    with get_database(read_only=False) as db:
        db.execute(
            "INSERT INTO raw.tabular_transactions "
            "(transaction_id, account_id, transaction_date, amount, "
            "source_file, source_type, source_origin, import_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ["t1", "acct", "2025-01-01", "10.00", "user.csv", "csv", "user", "imp1"],
        )
    assert svc.profile_has_data("demo") is True
