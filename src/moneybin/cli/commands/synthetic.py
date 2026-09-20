"""CLI commands for isolated synthetic-data generation and reset."""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

import typer

from moneybin import error_codes
from moneybin.cli.output import emit_human_result
from moneybin.cli.render import build_summary, compose_human_result
from moneybin.cli.terminal import TerminalPolicy
from moneybin.cli.utils import get_terminal_policy
from moneybin.errors import UserError

if TYPE_CHECKING:
    from moneybin.database import Database

app = typer.Typer(
    help="Generate and manage synthetic financial data for testing",
    no_args_is_help=True,
)

_PERSONA_PROFILES = {
    "basic": "alice",
    "family": "bob",
    "freelancer": "charlie",
    "international": "eve",
}


@dataclass(frozen=True, slots=True)
class _GenerationReceipt:
    profile: str
    persona: str
    seed: int
    start_date: date
    end_date: date
    accounts_saved: int
    transactions_saved: int
    ground_truth_saved: int
    transfer_pairs: int
    expected_accounts: int
    expected_transactions: int
    transforms: str
    partial: bool


def _text_policy() -> TerminalPolicy:
    """Resolve text presentation for this text-only command surface."""
    return get_terminal_policy(no_pager=True)


def _render_generation_receipt(
    receipt: _GenerationReceipt, *, terminal: TerminalPolicy
) -> None:
    """Print one factual unpaged receipt from writer-confirmed counts."""
    outcome = (
        "Generation partially completed" if receipt.partial else "Generation complete"
    )
    facts = [
        ("Profile", receipt.profile),
        ("Persona", receipt.persona),
        ("Seed", str(receipt.seed)),
        ("History", f"{receipt.start_date} through {receipt.end_date}"),
        ("Accounts saved", str(receipt.accounts_saved)),
        ("Transactions saved", str(receipt.transactions_saved)),
        ("Ground-truth labels", str(receipt.ground_truth_saved)),
        ("Transfer pairs", str(receipt.transfer_pairs)),
        ("Transforms", receipt.transforms),
    ]
    disclosures: list[str] = []
    if receipt.accounts_saved != receipt.expected_accounts:
        disclosures.append(
            f"Only {receipt.accounts_saved} of {receipt.expected_accounts} generated accounts were saved."
        )
    if receipt.transactions_saved != receipt.expected_transactions:
        disclosures.append(
            f"Only {receipt.transactions_saved} of {receipt.expected_transactions} generated transactions were saved."
        )
    if receipt.partial:
        disclosures.append(
            "Reports are stale. Run `moneybin transform apply` before using reports."
        )
    emit_human_result(
        compose_human_result(
            [build_summary(facts, title=outcome)], disclosures=disclosures
        ),
        policy=terminal,
        finite_read=False,
        no_pager=True,
        receipt=True,
    )


def _render_reset_cancelled(profile: str, *, terminal: TerminalPolicy) -> None:
    """State the pre-write cancellation truth without claiming a rollback."""
    emit_human_result(
        compose_human_result([
            build_summary(
                [("Profile", profile), ("Saved state", "No reset was started.")],
                title="Synthetic reset cancelled",
            )
        ]),
        policy=terminal,
        finite_read=False,
        no_pager=True,
        receipt=True,
    )


def _render_interrupted(profile: str, *, terminal: TerminalPolicy) -> None:
    """Report interruption without pretending earlier writes were rolled back."""
    emit_human_result(
        compose_human_result([
            build_summary(
                [("Profile", profile), ("Saved state", "Saved scope is unknown.")],
                title="Synthetic generation cancelled",
            )
        ]),
        policy=terminal,
        finite_read=False,
        no_pager=True,
        receipt=True,
    )


def _reset_safety_check(db: Database, profile: str) -> None:
    """Refuse a target whose provenance does not make reset safe."""
    from moneybin.synthetic.reset import (
        has_non_synthetic_data,
        has_synthetic_ground_truth,
    )

    if not has_synthetic_ground_truth(db):
        raise UserError(
            f"Profile {profile!r} was not created by the generator. Refusing to reset.",
            code=error_codes.MUTATION_INVALID_INPUT,
            hint=f"To destroy a non-generated profile, use 'moneybin profile delete {profile}'.",
        )
    if has_non_synthetic_data(db):
        raise UserError(
            f"Profile {profile!r} also holds real (non-synthetic) data. Refusing to reset.",
            code=error_codes.MUTATION_INVALID_INPUT,
            hint=f"To destroy a profile with real data, use 'moneybin profile delete {profile}'.",
        )


def _run_generate(
    persona: str,
    profile: str,
    years: int | None,
    seed: int | None,
    skip_transform: bool,
    *,
    terminal: TerminalPolicy,
) -> _GenerationReceipt:
    """Generate one isolated persona, preserving the caller's runtime profile."""
    from moneybin.cli.progress import operation_progress
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.config import (
        clear_current_profile,
        get_current_profile,
        set_current_profile,
    )
    from moneybin.database import get_database
    from moneybin.progress import ProgressEvent
    from moneybin.services.import_service import ImportService
    from moneybin.synthetic.engine import GeneratorEngine
    from moneybin.synthetic.writer import SyntheticWriter
    from moneybin.tables import OFX_TRANSACTIONS, TABULAR_TRANSACTIONS

    actual_seed = seed if seed is not None else random.randint(1, 9999)  # noqa: S311  # reproducibility seed, not cryptography
    try:
        original_profile: str | None = get_current_profile(auto_resolve=False)
    except RuntimeError:
        original_profile = None
    set_current_profile(profile)

    try:
        with handle_cli_errors(cli_actor="synthetic_generate"):
            with get_database(read_only=False) as db:
                try:
                    row = db.execute(
                        f"""SELECT (SELECT COUNT(*) FROM {OFX_TRANSACTIONS.full_name})
                                + (SELECT COUNT(*) FROM {TABULAR_TRANSACTIONS.full_name})"""  # noqa: S608  # TableRef constants
                    ).fetchone()
                    existing_count = row[0] if row else 0
                except Exception:  # fresh database has no raw tables yet
                    existing_count = 0
                if existing_count > 0:
                    raise UserError(
                        f"Profile {profile!r} already has data ({existing_count} transactions).",
                        code=error_codes.MUTATION_INVALID_INPUT,
                        hint=(
                            f"Use 'moneybin synthetic reset --persona={persona}' "
                            "to reset generated data."
                        ),
                    )

                with operation_progress(terminal) as report:
                    report(ProgressEvent("Generating synthetic data"))
                    generated = GeneratorEngine(
                        persona, seed=actual_seed, years=years
                    ).generate()
                    report(ProgressEvent("Saving generated data"))
                    counts = SyntheticWriter(db).write(generated)

                    accounts_saved = counts.get("ofx_accounts", 0) + counts.get(
                        "tabular_accounts", 0
                    )
                    transactions_saved = counts.get("ofx_transactions", 0) + counts.get(
                        "tabular_transactions", 0
                    )
                    transforms = "Skipped by request"
                    transform_failed = False
                    if not skip_transform:
                        report(ProgressEvent("Materializing reports"))
                        try:
                            ImportService(db).run_transforms()
                            transforms = "Completed"
                        except Exception:
                            transforms = "Failed after raw data was saved"
                            transform_failed = True

                expected_accounts = len(generated.accounts)
                expected_transactions = len(generated.transactions)
                count_mismatch = (
                    accounts_saved != expected_accounts
                    or transactions_saved != expected_transactions
                )
                return _GenerationReceipt(
                    profile=profile,
                    persona=persona,
                    seed=actual_seed,
                    start_date=generated.start_date,
                    end_date=generated.end_date,
                    accounts_saved=accounts_saved,
                    transactions_saved=transactions_saved,
                    ground_truth_saved=counts.get("ground_truth", 0),
                    transfer_pairs=sum(
                        1
                        for transaction in generated.transactions
                        if transaction.transfer_pair_id
                    )
                    // 2,
                    expected_accounts=expected_accounts,
                    expected_transactions=expected_transactions,
                    transforms=transforms,
                    partial=transform_failed or count_mismatch,
                )
    finally:
        if original_profile is None:
            clear_current_profile()
        else:
            set_current_profile(original_profile)


@app.command("generate")
def synthetic_generate(
    persona: str = typer.Option(
        ...,
        "--persona",
        help=f"Persona to generate ({', '.join(_PERSONA_PROFILES)})",
    ),
    profile: str | None = typer.Option(
        None, "--profile", help="Target profile name (auto-derived from persona)"
    ),
    years: int | None = typer.Option(
        None, "--years", help="Number of years of history"
    ),
    seed: int | None = typer.Option(
        None, "--seed", min=1, max=9999, help="Seed for deterministic output"
    ),
    skip_transform: bool = typer.Option(
        False, "--skip-transform", help="Skip running transforms after generation"
    ),
) -> None:
    """Generate synthetic financial data for a persona into a profile."""
    terminal = _text_policy()
    target_profile = profile or _PERSONA_PROFILES.get(persona, persona)
    try:
        receipt = _run_generate(
            persona, target_profile, years, seed, skip_transform, terminal=terminal
        )
    except KeyboardInterrupt:
        _render_interrupted(target_profile, terminal=terminal)
        raise typer.Exit(130) from None
    _render_generation_receipt(receipt, terminal=terminal)
    if receipt.partial:
        raise typer.Exit(1)


@app.command("reset")
def synthetic_reset(
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
        False, "--skip-transform", help="Skip running transforms after regeneration"
    ),
) -> None:
    """Wipe a generated profile and regenerate from scratch."""
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.config import (
        clear_current_profile,
        get_current_profile,
        set_current_profile,
    )
    from moneybin.database import get_database
    from moneybin.metrics.registry import SYNTHETIC_RESET_TOTAL
    from moneybin.synthetic.reset import reset_synthetic_rows

    terminal = _text_policy()
    target_profile = profile or _PERSONA_PROFILES.get(persona, persona)
    try:
        original_profile: str | None = get_current_profile(auto_resolve=False)
    except RuntimeError:
        original_profile = None
    set_current_profile(target_profile)

    try:
        with handle_cli_errors(cli_actor="synthetic_reset"):
            if not yes and not terminal.interactive:
                raise UserError(
                    "Synthetic reset requires --yes outside an interactive terminal.",
                    code=error_codes.MUTATION_CONFIRMATION_REQUIRED,
                )

            with get_database(read_only=True) as db:
                _reset_safety_check(db, target_profile)

            if not yes and not typer.confirm(
                f"This will destroy all generated data in profile {target_profile!r} "
                "and regenerate it. Continue?",
                default=False,
                err=True,
            ):
                _render_reset_cancelled(target_profile, terminal=terminal)
                raise typer.Exit(1)

            with get_database(read_only=False) as db:
                _reset_safety_check(db, target_profile)
                SYNTHETIC_RESET_TOTAL.labels(persona=persona).inc()
                reset_synthetic_rows(db)

        try:
            receipt = _run_generate(
                persona=persona,
                profile=target_profile,
                years=years,
                seed=seed,
                skip_transform=skip_transform,
                terminal=terminal,
            )
        except KeyboardInterrupt:
            _render_interrupted(target_profile, terminal=terminal)
            raise typer.Exit(130) from None
        _render_generation_receipt(receipt, terminal=terminal)
        if receipt.partial:
            raise typer.Exit(1)
    finally:
        if original_profile is None:
            clear_current_profile()
        else:
            set_current_profile(original_profile)
