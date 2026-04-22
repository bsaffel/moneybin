"""CLI commands for synthetic data generation and management."""

import logging
import random

import typer

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Generate and manage synthetic financial data for testing",
    no_args_is_help=True,
)

# Persona -> default profile name mapping
_PERSONA_PROFILES = {"basic": "alice", "family": "bob", "freelancer": "charlie"}

# Tables to scope-delete during reset (hardcoded allowlist — not user input)
_RESET_DELETIONS = {
    "synthetic.ground_truth": "WHERE TRUE",
    "raw.ofx_transactions": "WHERE source_file LIKE 'synthetic://%'",
    "raw.ofx_accounts": "WHERE source_file LIKE 'synthetic://%'",
    "raw.ofx_balances": "WHERE source_file LIKE 'synthetic://%'",
    "raw.csv_transactions": "WHERE source_file LIKE 'synthetic://%'",
    "raw.csv_accounts": "WHERE source_file LIKE 'synthetic://%'",
}


def _run_generate(
    persona: str,
    profile: str,
    years: int | None,
    seed: int | None,
    skip_transform: bool,
) -> None:
    """Core generate logic — called by both generate() and reset().

    Args:
        persona: Persona name (basic, family, freelancer).
        profile: Target profile name.
        years: Number of years of history (None for persona default).
        seed: Deterministic seed (None for random).
        skip_transform: If True, skip running SQLMesh after generation.
    """
    from moneybin.config import set_current_profile
    from moneybin.database import DatabaseKeyError, get_database
    from moneybin.services.import_service import run_transforms
    from moneybin.testing.synthetic.engine import GeneratorEngine
    from moneybin.testing.synthetic.writer import SyntheticWriter

    actual_seed = seed if seed is not None else random.randint(1, 999999)  # noqa: S311 — not crypto, just a reproducibility seed

    logger.info(
        f"⚙️  Generating {persona!r} persona into profile {profile!r} "
        f"(seed={actual_seed}{f', {years} years' if years else ''})"
    )

    # Pre-flight: verify database access before switching profile
    set_current_profile(profile)

    try:
        db = get_database()
    except DatabaseKeyError:
        logger.error("❌ Database encryption key not found")
        logger.info("💡 Run 'moneybin db unlock' to set up the encryption key")
        raise typer.Exit(1) from None

    # Check if profile already has data
    try:
        row = db.execute(
            """SELECT (SELECT COUNT(*) FROM raw.ofx_transactions)
                    + (SELECT COUNT(*) FROM raw.csv_transactions)"""
        ).fetchone()
        existing_count = row[0] if row else 0
    except Exception:  # noqa: BLE001,S110 — tables may not exist in a fresh DB
        existing_count = 0

    if existing_count > 0:
        logger.error(
            f"❌ Profile {profile!r} already has data ({existing_count} transactions)"
        )
        logger.info(
            f"💡 Use 'moneybin synthetic reset --persona={persona}' "
            f"to wipe and regenerate"
        )
        raise typer.Exit(1) from None

    # Generate
    try:
        engine = GeneratorEngine(persona, seed=actual_seed, years=years)
        result = engine.generate()
    except FileNotFoundError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from None

    # Write to database
    writer = SyntheticWriter(db)
    counts = writer.write(result)

    acct_count = counts.get("ofx_accounts", 0) + counts.get("csv_accounts", 0)
    txn_count = counts.get("ofx_transactions", 0) + counts.get("csv_transactions", 0)
    gt_count = counts.get("ground_truth", 0)
    transfer_count = sum(1 for t in result.transactions if t.transfer_pair_id) // 2

    logger.info(f"  Created {acct_count} accounts")
    logger.info(
        f"  Generated {txn_count} transactions "
        f"({result.start_date} to {result.end_date})"
    )
    logger.info(
        f"  Wrote ground truth: {gt_count} labels, {transfer_count} transfer pairs"
    )

    # Run SQLMesh transforms
    if not skip_transform:
        logger.info("⚙️  Running SQLMesh to materialize pipeline...")
        try:
            run_transforms(db.path)
        except Exception:  # noqa: BLE001 — SQLMesh failures are non-fatal here
            logger.warning(
                "⚠️  SQLMesh transforms failed — raw data is intact, "
                "run 'moneybin transform apply' manually"
            )

    logger.info(
        f"✅ Profile {profile!r} ready (seed={actual_seed}). "
        f"Use --profile={profile} with any moneybin command."
    )


@app.command("generate")
def generate(
    persona: str = typer.Option(
        ..., "--persona", help="Persona to generate (basic, family, freelancer)"
    ),
    profile: str | None = typer.Option(
        None, "--profile", help="Target profile name (auto-derived from persona)"
    ),
    years: int | None = typer.Option(
        None, "--years", help="Number of years of history"
    ),
    seed: int | None = typer.Option(
        None, "--seed", help="Seed for deterministic output (random if omitted)"
    ),
    skip_transform: bool = typer.Option(
        False, "--skip-transform", help="Skip running SQLMesh after generation"
    ),
) -> None:
    """Generate synthetic financial data for a persona into a profile."""
    target_profile = profile or _PERSONA_PROFILES.get(persona, persona)
    _run_generate(persona, target_profile, years, seed, skip_transform)


@app.command("reset")
def reset(
    persona: str = typer.Option(..., "--persona", help="Persona to regenerate"),
    profile: str | None = typer.Option(
        None, "--profile", help="Target profile to reset"
    ),
    years: int | None = typer.Option(None, "--years", help="Years to regenerate"),
    seed: int | None = typer.Option(None, "--seed", help="Seed for regeneration"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Wipe a generated profile and regenerate from scratch."""
    from moneybin.config import set_current_profile
    from moneybin.database import DatabaseKeyError, close_database, get_database

    target_profile = profile or _PERSONA_PROFILES.get(persona, persona)

    set_current_profile(target_profile)

    try:
        db = get_database()
    except DatabaseKeyError:
        logger.error("❌ Database encryption key not found")
        logger.info("💡 Run 'moneybin db unlock' to set up the encryption key")
        raise typer.Exit(1) from None

    # Safety check: only reset profiles created by the generator
    try:
        gt_row = db.execute(
            """SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'synthetic' AND table_name = 'ground_truth'"""
        ).fetchone()
        gt_exists = gt_row[0] if gt_row else 0
    except Exception:  # noqa: BLE001 — fresh DB with no synthetic schema
        gt_exists = 0

    if not gt_exists:
        logger.error(
            f"❌ Profile {target_profile!r} was not created by the "
            f"generator. Refusing to reset."
        )
        logger.info(
            f"💡 To destroy a non-generated profile, use "
            f"'moneybin db destroy --profile={target_profile}'"
        )
        raise typer.Exit(1) from None

    if not yes:
        confirmed = typer.confirm(
            f"This will destroy all data in profile {target_profile!r} "
            f"and regenerate. Continue?"
        )
        if not confirmed:
            raise typer.Abort()

    logger.info(f"⚙️  Resetting profile {target_profile!r}...")
    for table, where in _RESET_DELETIONS.items():
        try:
            db.execute(f"DELETE FROM {table} {where}")  # noqa: S608 — allowlisted table names + literal WHERE clauses
        except Exception:  # noqa: BLE001,S110 — table may not exist
            pass

    db.close()
    # Close the singleton so _run_generate gets a fresh connection
    close_database()

    # Regenerate
    _run_generate(
        persona=persona,
        profile=target_profile,
        years=years,
        seed=seed,
        skip_transform=False,
    )
