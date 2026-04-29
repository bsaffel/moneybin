"""CLI commands for synthetic data generation and management."""

import json
import logging
import random
from typing import Any, cast

import typer

from moneybin.cli.output import OutputFormat, output_option
from moneybin.tables import (
    GROUND_TRUTH,
    OFX_ACCOUNTS,
    OFX_BALANCES,
    OFX_TRANSACTIONS,
    TABULAR_ACCOUNTS,
    TABULAR_TRANSACTIONS,
)

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Generate and manage synthetic financial data for testing",
    no_args_is_help=True,
)

# Persona -> default profile name mapping
_PERSONA_PROFILES = {"basic": "alice", "family": "bob", "freelancer": "charlie"}

# Tables to scope-delete during reset (allowlist from TableRef constants)
_RESET_DELETIONS = {
    GROUND_TRUTH.full_name: "WHERE TRUE",
    OFX_TRANSACTIONS.full_name: "WHERE source_file LIKE 'synthetic://%'",
    OFX_ACCOUNTS.full_name: "WHERE source_file LIKE 'synthetic://%'",
    OFX_BALANCES.full_name: "WHERE source_file LIKE 'synthetic://%'",
    TABULAR_TRANSACTIONS.full_name: "WHERE source_file LIKE 'synthetic://%'",
    TABULAR_ACCOUNTS.full_name: "WHERE source_file LIKE 'synthetic://%'",
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
    from moneybin.cli.utils import handle_database_errors
    from moneybin.config import get_current_profile, set_current_profile
    from moneybin.database import close_database
    from moneybin.services.import_service import run_transforms
    from moneybin.testing.synthetic.engine import GeneratorEngine
    from moneybin.testing.synthetic.writer import SyntheticWriter

    actual_seed = seed if seed is not None else random.randint(1, 9999)  # noqa: S311 — not crypto, just a reproducibility seed

    logger.info(
        f"⚙️  Generating {persona!r} persona into profile {profile!r} "
        f"(seed={actual_seed}{f', {years} years' if years else ''})"
    )

    try:
        original_profile: str | None = get_current_profile()
    except RuntimeError:
        # Synthetic commands skip main.py's set_current_profile (see
        # cli/main.py is_synthetic_cmd), so there may be nothing to restore.
        original_profile = None
    close_database()
    set_current_profile(profile)

    try:
        with handle_database_errors() as db:
            # Check if profile already has data
            try:
                row = db.execute(
                    """SELECT (SELECT COUNT(*) FROM raw.ofx_transactions)
                            + (SELECT COUNT(*) FROM raw.tabular_transactions)"""
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

            acct_count = counts.get("ofx_accounts", 0) + counts.get(
                "tabular_accounts", 0
            )
            txn_count = counts.get("ofx_transactions", 0) + counts.get(
                "tabular_transactions", 0
            )
            gt_count = counts.get("ground_truth", 0)
            transfer_count = (
                sum(1 for t in result.transactions if t.transfer_pair_id) // 2
            )

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
                    run_transforms()
                except Exception:  # noqa: BLE001 — SQLMesh failures are non-fatal here
                    logger.warning(
                        "⚠️  SQLMesh transforms failed — raw data is intact, "
                        "run 'moneybin transform apply' manually"
                    )

            logger.info(
                f"✅ Profile {profile!r} ready (seed={actual_seed}). "
                f"Use --profile={profile} with any moneybin command."
            )
    finally:
        close_database()
        # When called from reset(), original_profile == profile (reset already
        # switched), so set_current_profile is a no-op; reset()'s own finally
        # does the real restore.
        if original_profile is not None:
            set_current_profile(original_profile)


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
        None,
        "--seed",
        min=1,
        max=9999,
        help="Seed for deterministic output (random if omitted)",
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
    seed: int | None = typer.Option(
        None, "--seed", min=1, max=9999, help="Seed for regeneration"
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
    skip_transform: bool = typer.Option(
        False, "--skip-transform", help="Skip running SQLMesh after regeneration"
    ),
) -> None:
    """Wipe a generated profile and regenerate from scratch."""
    from moneybin.cli.utils import handle_database_errors
    from moneybin.config import get_current_profile, set_current_profile
    from moneybin.database import close_database

    target_profile = profile or _PERSONA_PROFILES.get(persona, persona)

    try:
        original_profile: str | None = get_current_profile()
    except RuntimeError:
        original_profile = None
    close_database()
    set_current_profile(target_profile)

    try:
        with handle_database_errors() as db:
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

            from moneybin.metrics.registry import SYNTHETIC_RESET_TOTAL

            SYNTHETIC_RESET_TOTAL.labels(persona=persona).inc()
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
            skip_transform=skip_transform,
        )
    finally:
        close_database()
        if original_profile is not None:
            set_current_profile(original_profile)


@app.command("verify")
def verify_cmd(
    list_scenarios: bool = typer.Option(False, "--list", help="List shipped scenarios"),
    scenario: str | None = typer.Option(
        None, "--scenario", help="Run a single scenario by name"
    ),
    run_all: bool = typer.Option(False, "--all", help="Run every shipped scenario"),
    fail_fast: bool = typer.Option(
        False, "--fail-fast", help="Stop on first failure with --all"
    ),
    keep_tmpdir: bool = typer.Option(
        False, "--keep-tmpdir", help="Preserve scenario temp directory"
    ),
    output: OutputFormat = output_option,
) -> None:
    """Run scenario verification suites."""
    from moneybin.testing.scenarios.loader import (
        list_shipped_scenarios,
        load_shipped_scenario,
    )
    from moneybin.testing.scenarios.runner import run_scenario

    if list_scenarios:
        scenarios = list_shipped_scenarios()
        if output == OutputFormat.JSON:
            typer.echo(
                json.dumps([
                    {"name": s.name, "description": s.description} for s in scenarios
                ])
            )
        else:
            for s in scenarios:
                typer.echo(f"{s.name:40} {s.description}")
        return

    if scenario:
        single = load_shipped_scenario(scenario)
        if single is None:
            logger.error(f"❌ unknown scenario: {scenario}")
            raise typer.Exit(2)
        targets = [single]
    elif run_all:
        targets = list_shipped_scenarios()
    else:
        logger.error("❌ specify --list, --scenario=NAME, or --all")
        raise typer.Exit(2)

    failures = 0
    total = len(targets)
    for s in targets:
        env = run_scenario(s, keep_tmpdir=keep_tmpdir)
        data = cast("dict[str, Any]", env.data)
        passed = data["passed"]
        if output == OutputFormat.JSON:
            typer.echo(env.to_json())
        else:
            status = "✅" if passed else "❌"
            typer.echo(f"{status} {s.name} ({data['duration_seconds']}s)")
            if data.get("halted"):
                typer.echo(f"   ✗ halted: {data['halted']}")
            for a in data["assertions"]:
                if not a["passed"]:
                    detail = a["error"] or a["details"]
                    typer.echo(f"   ✗ assertion {a['name']}: {detail}")
            for e in data["expectations"]:
                if not e["passed"]:
                    typer.echo(f"   ✗ expectation {e['name']}")
            for v in data["evaluations"]:
                if not v["passed"]:
                    typer.echo(
                        f"   ✗ evaluation {v['name']}: "
                        f"{v['metric']}={v['value']} < threshold={v['threshold']}"
                    )
        if not passed:
            failures += 1
            if fail_fast:
                break

    if output != OutputFormat.JSON:
        passed_count = total - failures
        typer.echo(f"\n{passed_count}/{total} scenarios passed")

    raise typer.Exit(1 if failures else 0)
