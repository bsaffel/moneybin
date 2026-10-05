"""Recipe for the ``investment_source_overlap`` audit (fail).

One investment account fed by two sources at once has no single ledger, so
``core.dim_holdings`` withholds every figure for it. The remedy is a choice
only the user can make: which history to keep. One ``accounts_set`` action per
source present, both ``suggested`` (design-principles.md, "Magic stays
visible"); each rationale carries that source's trade count and date range
instead of a recommendation. Choosing deletes nothing, and clearing the
setting restores both histories (investment-source-choice.md).

The audit's ``affected_ids`` are masked, so this recipe re-runs the detector
for the raw ids. An id that masking or sanitizing would alter becomes the
``<account_id>`` placeholder, the same rule ``_command_account_id`` applies to
every published command.
"""

from __future__ import annotations

from moneybin.audits.recipes.registry import RecipeContext
from moneybin.errors import RecoveryAction
from moneybin.investments.source_overlap import (
    evidence_phrase,
    history_phrase,
    investment_source_evidence,
    investment_source_overlap,
)

_PLACEHOLDER = "<account_id>"


def recipe(
    affected_ids: list[str],  # masked; the recipe re-queries for raw ids
    context: RecipeContext,
) -> list[RecoveryAction]:
    """Offer one source choice per source present on each overlapping account."""
    if context.db is None:
        return []
    # Lazy: doctor_service imports the recipe registry at module load.
    from moneybin.services.doctor_service import (
        _command_account_id,  # pyright: ignore[reportPrivateUsage]  # one placeholder rule for every published command
        _publishable_account_id,  # pyright: ignore[reportPrivateUsage]  # the masked form the doctor's affected_ids use
    )

    accounts = investment_source_overlap(context.db)
    evidence = investment_source_evidence(context.db, accounts)
    actions: list[RecoveryAction] = []
    for account_id in accounts:
        sources = evidence.get(account_id, [])
        command_id = _command_account_id(account_id, _PLACEHOLDER)
        for keep in sources:
            others = [e for e in sources if e.source_type != keep.source_type]
            ignored = "; ".join(
                f"{history_phrase(o.source_type)} ({evidence_phrase(o)})"
                for o in others
            )
            rationale = (
                f"keeping {history_phrase(keep.source_type)} "
                f"({evidence_phrase(keep)}) and ignoring {ignored}; nothing is "
                "deleted and clearing the setting restores both"
            )
            if command_id == _PLACEHOLDER:
                rationale += (
                    f". Supply account_id for {_publishable_account_id(account_id)}"
                    " — read it from `moneybin accounts list`"
                )
            actions.append(
                RecoveryAction(
                    tool="accounts_set",
                    arguments={
                        "account_id": command_id,
                        "investment_source_type": keep.source_type,
                    },
                    rationale=rationale,
                    confidence="suggested",
                    idempotent=True,
                )
            )
    return actions
