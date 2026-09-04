"""Tests for DemoService orchestration.

The heavy collaborators (generator, refresh/SQLMesh, doctor, net worth) are
mocked here so these stay fast and focused on the orchestration *logic*; the
real end-to-end pipeline is proven by the `moneybin demo` e2e test.
"""

import datetime
from collections.abc import Generator
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from moneybin.services.demo_service import (
    DEMO_PROFILE,
    DemoProfileNotOursError,
    DemoRefreshFailedError,
    DemoResult,
    DemoService,
)

_INSERT_TXN = (
    "INSERT INTO raw.tabular_transactions "
    "(transaction_id, account_id, transaction_date, amount, "
    "source_file, source_type, source_origin, import_id) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
)

_PERSONAS = ("basic", "family", "freelancer", "international")


@pytest.fixture(autouse=True)
def _restore_profile_state() -> Generator[None, None, None]:  # pyright: ignore[reportUnusedFunction]  # pytest autouse fixture
    """DemoService.run switches the process-wide active profile; restore it."""
    from moneybin import config

    original = config._current_profile  # pyright: ignore[reportPrivateUsage]
    try:
        yield
    finally:
        config._current_profile = original  # pyright: ignore[reportPrivateUsage]


@pytest.fixture(scope="session", params=_PERSONAS)
def demo_profile_for_persona(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Generator[tuple[Path, DemoResult], None, None]:
    """Build each real persona once for the read-only pipeline assertions."""
    import os

    from moneybin import config

    home = tmp_path_factory.mktemp(f"demo_{request.param}")
    original_home = os.environ.get("MONEYBIN_HOME")
    os.environ["MONEYBIN_HOME"] = str(home)
    config.clear_settings_cache()
    try:
        result = DemoService().run(persona=request.param, seed=42, years=1)
    finally:
        if original_home is None:
            os.environ.pop("MONEYBIN_HOME", None)
        else:
            os.environ["MONEYBIN_HOME"] = original_home
        config.clear_settings_cache()
    yield home, result


@pytest.fixture
def built_demo_profile(
    demo_profile_for_persona: tuple[Path, DemoResult],
    monkeypatch: pytest.MonkeyPatch,
) -> DemoResult:
    """Activate one immutable, session-built demo profile for a read-only test."""
    from moneybin import config

    home, result = demo_profile_for_persona
    monkeypatch.setenv("MONEYBIN_HOME", str(home))
    config.clear_settings_cache()
    config.set_current_profile(DEMO_PROFILE)
    return result


def _mock_pipeline(
    mocker: Any,
    *,
    net_worth: str | None = "100.00",
    failing: int = 0,
    segments: list[tuple[str, str]] | None = None,
) -> None:
    """Patch the heavy collaborators DemoService.run imports lazily.

    ``segments`` builds a multi-currency snapshot: the headline scalars go None
    (no single total is meaningful) while ``per_currency`` carries each
    currency's own figure — the real shape NetworthService returns.
    """
    from moneybin.orchestration.refresh import RefreshResult
    from moneybin.privacy.payloads.networth import (
        NetWorthCurrencySegment,
        NetWorthSnapshotPayload,
    )
    from moneybin.services.doctor_service import DoctorReport, InvariantResult

    engine = mocker.patch("moneybin.synthetic.engine.GeneratorEngine")
    engine.return_value.generate.return_value = SimpleNamespace(
        accounts=[object(), object()], transactions=[], merchant_seeds=[]
    )
    writer = mocker.patch("moneybin.synthetic.writer.SyntheticWriter")
    writer.return_value.write.return_value = {"tabular_transactions": 5}

    mocker.patch(
        "moneybin.orchestration.refresh.refresh",
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
    if segments is not None:
        net.return_value.current.return_value = NetWorthSnapshotPayload(
            balance_date=datetime.date(2025, 1, 1),
            currency_code=None,
            net_worth=None,
            total_assets=None,
            total_liabilities=None,
            account_count=len(segments),
            per_currency=[
                NetWorthCurrencySegment(
                    currency_code=code,
                    net_worth=Decimal(value),
                    total_assets=Decimal(value),
                    total_liabilities=Decimal("0.00"),
                    account_count=1,
                )
                for code, value in segments
            ],
            per_account=[],
        )
        return
    net.return_value.current.return_value = NetWorthSnapshotPayload(
        balance_date=datetime.date(2025, 1, 1) if net_worth is not None else None,
        currency_code="USD" if net_worth is not None else None,
        net_worth=Decimal(net_worth) if net_worth is not None else None,
        total_assets=Decimal("150.00") if net_worth is not None else None,
        total_liabilities=Decimal("50.00") if net_worth is not None else None,
        account_count=2 if net_worth is not None else 0,
        per_currency=[
            NetWorthCurrencySegment(
                currency_code="USD",
                net_worth=Decimal(net_worth),
                total_assets=Decimal("150.00"),
                total_liabilities=Decimal("50.00"),
                account_count=2,
            )
        ]
        if net_worth is not None
        else [],
        per_account=[],
    )


def _make_demo_profile(
    *,
    generator_made: bool,
    real_data: bool = False,
    synthetic_transactions: bool = True,
) -> None:
    """Create a `demo` profile, optionally marked generator-made / holding real data.

    `generator_made=True, synthetic_transactions=False` reproduces a run that died
    after `SyntheticWriter` created `synthetic.ground_truth` but before any
    transactions landed — the writer creates that table first.
    """
    from moneybin.config import set_current_profile
    from moneybin.database import get_database
    from moneybin.services.profile_service import ProfileService

    ProfileService().create(DEMO_PROFILE, init_inbox=False)
    set_current_profile(DEMO_PROFILE)
    with get_database(read_only=False) as db:
        if generator_made:
            db.execute("CREATE SCHEMA IF NOT EXISTS synthetic")
            db.execute("CREATE TABLE IF NOT EXISTS synthetic.ground_truth (id VARCHAR)")
            if synthetic_transactions:
                db.execute(
                    _INSERT_TXN,
                    [
                        "s1",
                        "acct",
                        "2025-01-01",
                        "10.00",
                        "synthetic://basic/42/csv",
                        "csv",
                        "syn",
                        "imp1",
                    ],
                )
        if real_data:
            db.execute(
                _INSERT_TXN,
                ["r1", "acct", "2025-01-01", "10.00", "u.csv", "csv", "user", "imp2"],
            )


@pytest.mark.integration
def test_run_populates_fresh_demo_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: Any
) -> None:
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    _mock_pipeline(mocker, net_worth="12345.67")

    result = DemoService().run(persona="basic", seed=42)

    assert isinstance(result, DemoResult)
    assert result.profile == DEMO_PROFILE
    assert result.account_count == 2
    assert result.transaction_count == 5
    assert result.doctor_failing == 0
    assert result.net_worth == Decimal("12345.67")
    # A fully successful run persists demo as the default profile.
    from moneybin.utils.user_config import get_default_profile

    assert get_default_profile() == DEMO_PROFILE


@pytest.mark.integration
def test_run_refuses_missing_net_worth_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: Any
) -> None:
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    _mock_pipeline(mocker, net_worth=None)

    with pytest.raises(DemoRefreshFailedError, match="net worth unavailable"):
        DemoService().run(persona="basic", seed=42)


@pytest.mark.integration
def test_run_accepts_a_multi_currency_position(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: Any
) -> None:
    """A profile holding two currencies has no single total — that is not a failure.

    NetworthService nulls the headline scalars once a second currency appears
    (multi-currency.md Requirement 5) and reports each currency in
    `per_currency`. Demo used to read that null as a broken refresh, which made
    every multi-currency persona unrunnable.
    """
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    _mock_pipeline(mocker, segments=[("EUR", "2500.00"), ("GBP", "900.00")])

    result = DemoService().run(persona="basic", seed=42)

    assert result.net_worth is None
    assert [(s.currency_code, s.net_worth) for s in result.per_currency] == [
        ("EUR", Decimal("2500.00")),
        ("GBP", Decimal("900.00")),
    ]


@pytest.mark.integration
def test_run_still_reports_a_scalar_for_one_currency(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: Any
) -> None:
    """The single-currency case — the common one — is unchanged."""
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    _mock_pipeline(mocker, net_worth="12345.67")

    result = DemoService().run(persona="basic", seed=42)

    assert result.net_worth == Decimal("12345.67")
    assert [s.currency_code for s in result.per_currency] == ["USD"]


@pytest.mark.integration
def test_rerun_rebuilds_the_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: Any
) -> None:
    # A generator-made demo profile is rebuilt from scratch (fresh DB), so no
    # stale synthetic rows or derived app-state survive into the new run.
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    _make_demo_profile(generator_made=True)
    _mock_pipeline(mocker)

    DemoService().run(persona="basic", seed=42, reset_confirmed=True)

    from moneybin.database import get_database

    with get_database(read_only=True) as db:
        rows = db.execute("SELECT COUNT(*) FROM raw.tabular_transactions").fetchone()
        # The pre-existing synthetic row is gone — the DB was rebuilt (the
        # generator itself is mocked, so nothing was written back).
        assert rows is not None
        assert rows[0] == 0


@pytest.mark.integration
def test_refuses_demo_profile_holding_real_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: Any
) -> None:
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    _make_demo_profile(generator_made=True, real_data=True)
    _mock_pipeline(mocker)  # would succeed if the guard didn't fire first

    with pytest.raises(DemoProfileNotOursError):
        DemoService().run(persona="basic", seed=42, reset_confirmed=True)

    # A refused run must NOT have switched the user's persisted default profile.
    from moneybin.utils.user_config import get_default_profile

    assert get_default_profile() != DEMO_PROFILE


@pytest.mark.integration
def test_refuses_demo_profile_we_did_not_create(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: Any
) -> None:
    # A profile named `demo` holding data but with no generator marker is not
    # ours — rebuilding would destroy data we can't prove is synthetic.
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    _make_demo_profile(generator_made=False, real_data=True)
    _mock_pipeline(mocker)

    with pytest.raises(DemoProfileNotOursError):
        DemoService().run(persona="basic", seed=42, reset_confirmed=True)


@pytest.mark.integration
def test_rebuild_requires_confirmation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: Any
) -> None:
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    _make_demo_profile(generator_made=True)
    _mock_pipeline(mocker)

    with pytest.raises(RuntimeError, match="reset not confirmed"):
        DemoService().run(persona="basic", seed=42, reset_confirmed=False)


@pytest.mark.integration
def test_demo_net_worth_covers_every_account(
    built_demo_profile: DemoResult,
) -> None:
    # Demo's one headline answer. It used to print "Net worth: 0.00": every account
    # is carried in `core.fct_balances_daily` only to its OWN last observation, so on
    # the latest date — the one `NetworthService.current()` reports — accounts with
    # older statements had already dropped out. The OFX accounts carry a single
    # opening-day balance, so they vanished entirely, and `basic`'s remaining account
    # happened to sit at zero.
    from moneybin.database import get_database

    result = built_demo_profile

    # A real position exists. For one currency that is the scalar; for several
    # the scalar is null by design and each currency carries its own figure.
    assert result.per_currency
    assert any(segment.net_worth != Decimal("0") for segment in result.per_currency)

    with get_database(read_only=True) as db:
        # reports.net_worth is one row per (date, currency), so the count has to
        # be summed across currencies — a bare LIMIT 1 would compare one
        # currency's account count against the whole profile's and fail.
        row = db.execute(
            "SELECT SUM(account_count) FROM reports.net_worth "
            "WHERE balance_date = (SELECT MAX(balance_date) FROM reports.net_worth)"
        ).fetchone()
        assert row is not None
        # Every generated account contributes on the latest date — none aged out.
        assert row[0] == result.account_count


@pytest.mark.integration
def test_demo_ships_a_categorized_profile(
    built_demo_profile: DemoResult,
) -> None:
    # Demo's headline promise. It previously shipped 0% categorized — the generator
    # never taught the engine about the merchants it invented, and doctor's coverage
    # check is warn-only, so the run still reported "clean" while
    # "What did I spend on dining last month?" returned nothing.
    #
    # The floor is deliberately well under what the personas actually reach (~84-93%)
    # so it tracks the real failure — a collapse to zero — rather than churning on
    # every merchant-catalog tweak.
    from moneybin.database import get_database

    result = built_demo_profile

    assert result.transaction_count > 0
    assert result.categorized_count / result.transaction_count > 0.7

    with get_database(read_only=True) as db:
        merchants = db.execute("SELECT COUNT(*) FROM app.user_merchants").fetchone()
        assert merchants is not None
        assert merchants[0] > 0


@pytest.mark.integration
def test_demo_pipeline_output_is_invisible_to_the_real_data_guard(
    built_demo_profile: DemoResult,
) -> None:
    # The inverse property, driven through the REAL pipeline for every persona.
    # The guard now treats any `app.*` table outside _DEMO_WRITTEN_APP_TABLES as
    # the user's — so if the pipeline ever starts writing another one, demo would
    # refuse to rebuild its OWN profile on the next run. This is the test that
    # catches that in CI rather than in a user's terminal.
    from moneybin.database import get_database
    from moneybin.synthetic.reset import has_non_synthetic_data

    with get_database(read_only=True) as db:
        assert has_non_synthetic_data(db) is False


@pytest.mark.integration
def test_dirty_doctor_does_not_switch_the_default_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: Any
) -> None:
    # A failing doctor is a failed run (the CLI exits 1), so it must not repoint the
    # user's default at a demo profile we just told them is broken.
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    _mock_pipeline(mocker, failing=2)

    result = DemoService().run(persona="basic", seed=42)

    assert result.doctor_failing == 2
    assert result.previous_default is None

    from moneybin.utils.user_config import get_default_profile

    assert get_default_profile() != DEMO_PROFILE


@pytest.mark.integration
def test_rebuilds_an_empty_pre_existing_demo_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: Any
) -> None:
    # A real `init_db` seeds app.schema_migrations / app.versions. The not-ours
    # guard counts ANY row as user content, so those two MUST be excluded — else
    # `moneybin demo` would refuse to build on an empty, freshly-created `demo`
    # profile. The unit test for that exclusion runs against the test fixture,
    # which seeds nothing; only a real profile-create proves the exclusion list.
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    _make_demo_profile(generator_made=False)  # real init, no data
    _mock_pipeline(mocker)

    result = DemoService().run(persona="basic", seed=42, reset_confirmed=False)

    assert result.transaction_count == 5


@pytest.mark.integration
def test_refuses_demo_profile_holding_only_securities(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: Any
) -> None:
    # app.securities is user-authored and needs no transaction behind it. A profile
    # named `demo` holding only a real securities catalog must not be destroyed.
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    _make_demo_profile(generator_made=False)
    _mock_pipeline(mocker)

    from moneybin.database import get_database

    with get_database(read_only=False) as db:
        db.execute(
            "INSERT INTO app.securities (security_id, name, security_type) "
            "VALUES (?, ?, ?)",
            ["sec_1", "Vanguard S&P 500 ETF", "etf"],
        )

    with pytest.raises(DemoProfileNotOursError):
        DemoService().run(persona="basic", seed=42, reset_confirmed=True)


@pytest.mark.integration
def test_rebuilds_a_demo_directory_with_no_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: Any
) -> None:
    # A profile directory that exists with no database file at all — an interrupted
    # create, or a hand-made dir. There is nothing to guard and nothing to lose, so
    # demo builds it. (The guard checks the db path directly: a write-mode open
    # would silently create the database rather than raise.)
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    from moneybin.config import get_settings

    _make_demo_profile(generator_made=False)
    db_path = get_settings().database.path
    db_path.unlink()
    assert not db_path.exists()

    _mock_pipeline(mocker)
    result = DemoService().run(persona="basic", seed=42, reset_confirmed=False)

    assert result.transaction_count == 5
    assert db_path.exists()


@pytest.mark.integration
def test_registers_a_bare_db_init_demo_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: Any
) -> None:
    # `moneybin db init --profile demo` leaves a directory + database but no
    # config.yaml, and `ProfileService.create` raises ProfileExistsError off the
    # directory alone. Demo must finish that registration, or it reports success
    # for a profile `profile list` hides and that has no inbox.
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    from moneybin.services.profile_service import ProfileService

    profiles = ProfileService()
    _make_demo_profile(generator_made=False)
    (profiles._profile_dir(DEMO_PROFILE) / "config.yaml").unlink()  # pyright: ignore[reportPrivateUsage]
    assert profiles.is_registered(DEMO_PROFILE) is False

    _mock_pipeline(mocker)
    DemoService().run(persona="basic", seed=42, reset_confirmed=True)

    assert profiles.is_registered(DEMO_PROFILE) is True
    assert DEMO_PROFILE in [p["name"] for p in profiles.list()]


@pytest.mark.integration
def test_refuses_an_unregistered_demo_directory_holding_real_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: Any
) -> None:
    # The data-safety guard must not depend on `ProfileService.create()` raising.
    # `create()` now completes an unregistered bare directory in place, so demo can
    # no longer infer "this profile already existed" from ProfileExistsError — it
    # asks the filesystem instead. Regress that inference and this profile — a hand
    # `db init --profile demo` (database + real securities, no config.yaml) — is
    # silently rebuilt, destroying the user's data.
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    from moneybin.database import get_database
    from moneybin.services.profile_service import ProfileService

    profiles = ProfileService()
    _make_demo_profile(generator_made=False)
    with get_database(read_only=False) as db:
        db.execute(
            "INSERT INTO app.securities (security_id, name, security_type) "
            "VALUES (?, ?, ?)",
            ["sec_1", "Vanguard S&P 500 ETF", "etf"],
        )
    (profiles._profile_dir(DEMO_PROFILE) / "config.yaml").unlink()  # pyright: ignore[reportPrivateUsage]
    assert profiles.is_registered(DEMO_PROFILE) is False

    _mock_pipeline(mocker)
    with pytest.raises(DemoProfileNotOursError):
        DemoService().run(persona="basic", seed=42, reset_confirmed=True)


@pytest.mark.integration
def test_recovers_from_partially_generated_demo_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: Any
) -> None:
    # A run that died part-way through generating leaves the `synthetic.ground_truth`
    # marker but no transactions. There is nothing the user could lose, and the CLI
    # never prompts (`profile_has_data` sees no transactions) — so demanding a
    # confirmation here would strand the user with no way to re-run `moneybin demo`.
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    _make_demo_profile(generator_made=True, synthetic_transactions=False)
    _mock_pipeline(mocker)

    assert DemoService().profile_has_data() is False

    result = DemoService().run(persona="basic", seed=42, reset_confirmed=False)

    assert result.profile == DEMO_PROFILE
    assert result.transaction_count == 5


@pytest.mark.integration
def test_profile_has_data(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    svc = DemoService()

    # Nonexistent profile / missing DB → False (no exception).
    assert svc.profile_has_data() is False

    _make_demo_profile(generator_made=True)
    assert svc.profile_has_data() is True


@pytest.mark.integration
def test_raises_on_refresh_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: Any
) -> None:
    # A real crash in matching/transform/categorize must abort demo, not ship a
    # half-built profile. Covers the multi-field RefreshResult error check.
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    _mock_pipeline(mocker)

    from moneybin.orchestration.refresh import RefreshResult

    mocker.patch(
        "moneybin.orchestration.refresh.refresh",
        return_value=RefreshResult(
            applied=False, duration_seconds=0.0, categorization_error="boom"
        ),
    )
    # A UserError subclass, not a bare RuntimeError: `refresh()` reports these as
    # returned errors (an anticipated condition), and an unclassified exception
    # would reach the CLI as a traceback with no JSON envelope.
    with pytest.raises(DemoRefreshFailedError, match="Demo refresh failed"):
        DemoService().run(persona="basic", seed=42)

    # A failed run must NOT have switched the user's persisted default profile.
    from moneybin.utils.user_config import get_default_profile

    assert get_default_profile() != DEMO_PROFILE


@pytest.mark.integration
def test_refresh_failure_is_a_classified_user_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # What the CLI actually does with it: classify_user_error() must recognise it,
    # so handle_cli_errors() prints a clean message and exits 1 rather than
    # re-raising an unclassified exception as a traceback.
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    from moneybin.errors import UserError, classify_user_error

    err = DemoRefreshFailedError("categorize crashed")

    assert isinstance(err, UserError)
    assert classify_user_error(err) is not None
