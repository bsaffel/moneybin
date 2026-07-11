"""Orchestrates `moneybin demo`: a populated, categorized, doctor-clean profile.

Thin business-logic layer the CLI wraps. Composes existing primitives — profile
creation, the persona synthetic generator, the full refresh cascade, doctor, and
net worth — into a dedicated, safe-to-reset ``demo`` profile. No new
data-generation code lives here; this is orchestration only.
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

DEMO_DEFAULT_PROFILE = "demo"
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


class ProfileHasNonSyntheticDataError(UserError):
    """Target profile holds data the generator did not create — refuse to reset."""

    def __init__(self, profile: str) -> None:
        """Build a user-facing refusal message naming the offending profile."""
        super().__init__(
            f"Profile {profile!r} holds non-synthetic data; refusing to reset.",
            code=error_codes.MUTATION_INVALID_INPUT,
            hint=(
                f"💡 Use a different --profile, or "
                f"'moneybin profile delete {profile}' first."
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


class DemoService:
    """Set up (or refresh) a demo profile end-to-end."""

    def profile_has_data(self, profile: str) -> bool:
        """True if the profile's DB exists and already holds transactions."""
        from moneybin.config import get_current_profile, set_current_profile
        from moneybin.database import DatabaseNotInitializedError, get_database

        try:
            original: str | None = get_current_profile(auto_resolve=False)
        except RuntimeError:
            original = None
        set_current_profile(profile)
        try:
            with get_database(read_only=True) as db:
                return _count_transactions(db) > 0
        except DatabaseNotInitializedError:
            return False
        finally:
            if original is not None:
                set_current_profile(original)

    def run(
        self,
        *,
        persona: str,
        profile: str = DEMO_DEFAULT_PROFILE,
        seed: int = DEMO_DEFAULT_SEED,
        years: int | None = None,
        reset_confirmed: bool = False,
    ) -> DemoResult:
        """Populate ``profile`` with persona data, run the pipeline, return the answer."""
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
        from moneybin.synthetic.reset import (
            has_non_synthetic_data,
            has_synthetic_ground_truth,
            reset_synthetic_rows,
        )
        from moneybin.synthetic.writer import SyntheticWriter
        from moneybin.utils.user_config import set_default_profile

        # 1. Ensure the profile exists (with an inbox), tolerate an existing one.
        try:
            ProfileService().create(profile, init_inbox=True)
            logger.info(f"⚙️  Created demo profile {profile!r}")
        except ProfileExistsError:
            pass

        # 2. Set the process-level active profile so we open the right DB. The
        #    PERSISTED default switch waits until the safety guard passes below —
        #    a refused run must not silently change the user's default profile.
        set_current_profile(profile)

        with get_database(read_only=False) as db:
            # 3a. Refuse any profile holding real (non-synthetic) data — from
            #     ANY source (OFX/tabular non-`synthetic://` rows, Plaid, manual,
            #     gsheet, balances/accounts, balance assertions) and regardless of
            #     the `synthetic.ground_truth` marker. This is the demo-isolation
            #     guard: never seed synthetic rows onto real financial data.
            if has_non_synthetic_data(db):
                raise ProfileHasNonSyntheticDataError(profile)

            # Guard passed — now safe to make demo the persisted default.
            set_default_profile(profile)

            # 3b. Only synthetic data (or empty) remains — safe to regenerate.
            if has_synthetic_ground_truth(db):
                if not reset_confirmed and _count_transactions(db) > 0:
                    # The CLI confirms before calling with reset_confirmed=True;
                    # this guards the service contract as defense in depth.
                    raise RuntimeError(
                        f"Profile {profile!r} already has data; reset not confirmed."
                    )
                reset_synthetic_rows(db)

            # 4. Generate persona data.
            result = GeneratorEngine(persona, seed=seed, years=years).generate()
            counts = SyntheticWriter(db).write(result)
            account_count = len(result.accounts)
            txn_count = sum(
                counts.get(k, 0) for k in ("ofx_transactions", "tabular_transactions")
            )

            # 5. Refresh derived state (match → transform → categorize). Skip the
            #    `gsheet` step: demo generated its own raw data and must never
            #    trigger a live external pull (`pull_all_healthy` would import real
            #    spreadsheet rows into the demo profile). Surface a real crash in
            #    any step — demo's whole premise is a clean, categorized pipeline.
            refresh_result = refresh(db, steps=["match", "transform", "categorize"])
            refresh_error = (
                refresh_result.error
                or refresh_result.matching_error
                or refresh_result.categorization_error
            )
            if refresh_error:
                raise RuntimeError(f"Demo refresh failed: {refresh_error}")

            # 6. Doctor.
            report = DoctorService(db).run_all()
            failing_names = [r.name for r in report.invariants if r.status == "fail"]

            # 7. The one obvious answer.
            snapshot = NetworthService(db).current()

        DEMO_RUN_TOTAL.labels(persona=persona).inc()
        logger.info(f"✅ Demo profile {profile!r} ready (seed={seed})")
        return DemoResult(
            profile=profile,
            persona=persona,
            seed=seed,
            account_count=account_count,
            transaction_count=txn_count,
            doctor_failing=report.failing,
            doctor_failing_names=failing_names,
            net_worth=snapshot.net_worth,
            total_assets=snapshot.total_assets,
            total_liabilities=snapshot.total_liabilities,
        )
