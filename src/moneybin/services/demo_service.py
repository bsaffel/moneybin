"""Orchestrates `moneybin demo`: a populated, categorized, doctor-clean profile.

Thin business-logic layer the CLI wraps. Composes existing primitives — profile
creation, the persona synthetic generator, the refresh cascade, doctor, and net
worth — into a dedicated ``demo`` profile. No new data-generation code lives
here; this is orchestration only.

**Isolation by construction.** Demo only ever targets its own ``demo`` profile —
there is no arbitrary ``--profile`` target, so it cannot be pointed at a real
financial profile. Re-running rebuilds that profile's database from scratch
rather than surgically deleting generated rows. A fresh database leaves no
orphaned derived state (so a re-run with a different persona/seed still ends
doctor-clean) and needs no raw mutation of audited ``app.*`` tables, which may
only be written through their ``*Repo`` (Invariant 10).
"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from moneybin import error_codes
from moneybin.errors import UserError

if TYPE_CHECKING:
    from moneybin.database import Database

logger = logging.getLogger(__name__)

DEMO_PROFILE = "demo"
DEMO_DEFAULT_SEED = 42


@dataclass(frozen=True)
class DemoResult:
    """Structured outcome of a demo run — rendered by the CLI in text or json."""

    profile: str
    persona: str
    seed: int
    account_count: int
    transaction_count: int
    doctor_failing: int
    doctor_failing_names: list[str]
    net_worth: Decimal
    total_assets: Decimal
    total_liabilities: Decimal
    # The default profile demo displaced, so the CLI can name the way back.
    # None when no default was set (or it was already `demo`).
    previous_default: str | None


class DemoProfileNotOursError(UserError):
    """A `demo` profile exists that the generator didn't create, or holds real data.

    Rebuilding it would destroy data we can't prove is synthetic, so refuse.
    """

    def __init__(self) -> None:
        """Build a user-facing refusal naming the safe recovery path."""
        super().__init__(
            f"Profile {DEMO_PROFILE!r} already exists and was not created by "
            f"'moneybin demo' (or holds real data). Refusing to rebuild it.",
            code=error_codes.MUTATION_INVALID_INPUT,
            hint=(
                f"💡 Back up anything you need, then "
                f"'moneybin profile delete {DEMO_PROFILE}' and re-run 'moneybin demo'."
            ),
        )


class DemoRefreshFailedError(UserError):
    """A step of the refresh cascade failed while building the demo profile.

    `refresh()` reports match/transform/categorize crashes as returned errors, not
    exceptions — an anticipated runtime condition, not a programmer error. Demo's
    whole premise is a clean, categorized pipeline, so it can't continue; surface
    it as a `UserError` so the CLI prints a clean message (and still emits a JSON
    envelope) instead of an unclassified traceback.
    """

    def __init__(self, detail: str) -> None:
        """Build a user-facing failure naming the profile to rebuild."""
        super().__init__(
            f"Demo refresh failed: {detail}",
            code=error_codes.REFRESH_MODEL_FAILED,
            hint=(
                f"💡 Re-run 'moneybin demo --yes' to rebuild the "
                f"{DEMO_PROFILE!r} profile from scratch."
            ),
        )


def _count_transactions(db: "Database") -> int:
    try:
        row = db.execute(
            "SELECT (SELECT COUNT(*) FROM raw.ofx_transactions) "
            "+ (SELECT COUNT(*) FROM raw.tabular_transactions)"
        ).fetchone()
        return int(row[0]) if row else 0
    except Exception:  # noqa: BLE001,S110 — tables may not exist in a fresh DB
        return 0


def _rebuild_database(profile: str) -> None:
    """Replace the profile's database with a fresh, empty one.

    Simpler and safer than surgically deleting generated rows: a new database has
    no orphaned derived state, and it needs no raw mutation of audited ``app.*``
    tables. Callers MUST have verified the profile is ours and holds no real data
    first — this destroys the file.
    """
    from pathlib import Path

    from moneybin.config import get_settings
    from moneybin.database import init_db

    db_path = get_settings().database.path
    Path(str(db_path) + ".wal").unlink(missing_ok=True)
    db_path.unlink(missing_ok=True)
    init_db(db_path, profile=profile)
    logger.info(f"⚙️  Rebuilt demo database for profile {profile!r}")


class DemoService:
    """Set up (or refresh) the dedicated demo profile end-to-end."""

    def profile_has_data(self) -> bool:
        """True if the demo profile's database exists and already holds data."""
        from moneybin.config import get_current_profile, set_current_profile
        from moneybin.database import DatabaseNotInitializedError, get_database

        try:
            original: str | None = get_current_profile(auto_resolve=False)
        except RuntimeError:
            original = None
        set_current_profile(DEMO_PROFILE)
        try:
            with get_database(read_only=True) as db:
                return _count_transactions(db) > 0
        except DatabaseNotInitializedError:
            return False
        finally:
            if original is not None:
                set_current_profile(original)

    def _guard_and_rebuild(self, *, reset_confirmed: bool) -> None:
        """Verify an existing demo profile is ours, then rebuild its database.

        Refuses anything we can't prove the generator made and that holds no real
        data — rebuilding destroys the database file.
        """
        from moneybin.database import DatabaseNotInitializedError, get_database
        from moneybin.synthetic.reset import (
            has_any_user_content,
            has_non_synthetic_data,
            has_synthetic_ground_truth,
        )

        try:
            with get_database(read_only=False) as db:
                generator_made = has_synthetic_ground_truth(db)
                has_transactions = _count_transactions(db) > 0

                # What counts as "unsafe to destroy" turns entirely on whether we
                # can prove the generator made this profile.
                if generator_made:
                    # Ours. Its `app.*` rows are derived from the synthetic raw rows
                    # and are regenerated by the rebuild, so only REAL data mixed in
                    # matters.
                    unsafe = has_non_synthetic_data(db)
                else:
                    # Not ours — someone else's profile that happens to be named
                    # `demo`. There is no safe table here: `app.securities`,
                    # `app.budgets`, and `app.user_categories` are all user-authored
                    # state that needs no transaction behind it, and the list grows.
                    # Refuse on any content at all rather than enumerate them;
                    # over-refusing merely declines to destroy something.
                    unsafe = has_any_user_content(db)

                if unsafe:
                    raise DemoProfileNotOursError

                # Only transactions are worth confirming away. A profile with none —
                # an empty shell, or a run that died part-way through generating and
                # left just the `synthetic.ground_truth` marker — has nothing to
                # lose, so rebuild it unprompted. Gating this on the marker instead
                # would strand a half-generated profile: the CLI's `profile_has_data`
                # check sees no transactions, so it never prompts, so
                # `reset_confirmed` is never set, so the run could never proceed.
                if has_transactions and not reset_confirmed:
                    # The CLI confirms before calling; defense in depth for the
                    # service contract.
                    raise RuntimeError(
                        f"Profile {DEMO_PROFILE!r} already has demo data; "
                        f"reset not confirmed."
                    )
        except DatabaseNotInitializedError:
            # The directory exists but was never `db init`'d — nothing to guard and
            # nothing to lose. `_rebuild_database` creates the database below.
            logger.info(f"⚙️  Demo profile {DEMO_PROFILE!r} has no database yet")

        # Connection closed — safe to replace the database file.
        _rebuild_database(DEMO_PROFILE)

    def run(
        self,
        *,
        persona: str,
        seed: int = DEMO_DEFAULT_SEED,
        years: int | None = None,
        reset_confirmed: bool = False,
    ) -> DemoResult:
        """Populate the demo profile, run the pipeline, return the first answer."""
        from moneybin.config import set_current_profile
        from moneybin.database import get_database
        from moneybin.metrics.registry import DEMO_RUN_TOTAL
        from moneybin.services.doctor_service import DoctorService
        from moneybin.services.networth_service import NetworthService
        from moneybin.services.profile_service import (
            ProfileExistsError,
            ProfileService,
        )
        from moneybin.services.refresh import refresh
        from moneybin.synthetic.engine import GeneratorEngine
        from moneybin.synthetic.writer import SyntheticWriter
        from moneybin.utils.user_config import get_default_profile, set_default_profile

        # 1. Ensure the demo profile exists (with an inbox).
        profiles = ProfileService()
        existed = False
        try:
            profiles.create(DEMO_PROFILE, init_inbox=True)
            logger.info(f"⚙️  Created demo profile {DEMO_PROFILE!r}")
        except ProfileExistsError:
            existed = True

        # 2. Point the process at it so we open the right database. The PERSISTED
        #    default switch happens only after a fully successful run (step 7).
        set_current_profile(DEMO_PROFILE)

        # 3. An existing demo profile must be provably ours before we rebuild it.
        if existed:
            self._guard_and_rebuild(reset_confirmed=reset_confirmed)
            # `create()` raises ProfileExistsError off the directory alone, so a
            # bare `moneybin db init --profile demo` (directory + database, no
            # config.yaml) lands here unregistered: `profile list` would hide it and
            # it would have no inbox. Finish the setup we promised — only now that
            # the guard has cleared, so we never scaffold a profile we then refuse.
            profiles.ensure_registered(DEMO_PROFILE, init_inbox=True)

        with get_database(read_only=False) as db:
            # 4. Generate persona data into the (now fresh) database.
            result = GeneratorEngine(persona, seed=seed, years=years).generate()
            counts = SyntheticWriter(db).write(result)
            account_count = len(result.accounts)
            txn_count = sum(
                counts.get(k, 0) for k in ("ofx_transactions", "tabular_transactions")
            )

            # 5. Refresh derived state (match → transform → categorize). Skip the
            #    `gsheet` step: demo generated its own raw data and must never
            #    trigger a live external pull. Surface a real crash in any step —
            #    demo's whole premise is a clean, categorized pipeline.
            refresh_result = refresh(db, steps=["match", "transform", "categorize"])
            refresh_error = (
                refresh_result.error
                or refresh_result.matching_error
                or refresh_result.categorization_error
            )
            if refresh_error:
                raise DemoRefreshFailedError(str(refresh_error))

            # 6. Doctor + the one obvious answer.
            report = DoctorService(db).run_all()
            failing_names = [r.name for r in report.invariants if r.status == "fail"]
            snapshot = NetworthService(db).current()

        # 7. Only now — a complete, successful run — make demo the persisted
        #    default so the next command lands on it. Report the profile we
        #    displaced: silently repointing every later command at `demo` is
        #    exactly the kind of magic that has to stay visible, and the caller
        #    needs the displaced name to offer a way back.
        previous_default = get_default_profile()
        set_default_profile(DEMO_PROFILE)

        DEMO_RUN_TOTAL.labels(persona=persona).inc()
        logger.info(f"✅ Demo profile {DEMO_PROFILE!r} ready (seed={seed})")
        return DemoResult(
            profile=DEMO_PROFILE,
            persona=persona,
            seed=seed,
            account_count=account_count,
            transaction_count=txn_count,
            doctor_failing=report.failing,
            doctor_failing_names=failing_names,
            net_worth=snapshot.net_worth,
            total_assets=snapshot.total_assets,
            total_liabilities=snapshot.total_liabilities,
            previous_default=(
                previous_default if previous_default != DEMO_PROFILE else None
            ),
        )
