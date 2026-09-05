"""Recipe for the ``investment_source_overlap`` audit (fail).

One investment account fed by two sources at once has no single ledger: the
imported rows and the synced rows interleave, so lots double-count and cost
basis mixes two accountings. ``core.dim_holdings`` withholds every figure for
such a position (``valuation_status = 'source_overlap'``), and nothing the
pipeline can re-run will clear it — the fix is to stop one of the two feeds.

There are exactly two ways out, and they are alternatives, not a sequence:
drop the imported batch (``import_revert``) if the connector is the ledger you
want, or disconnect the duplicate connection (``sync_disconnect``) if the file
import is. Only the user knows which source is authoritative for the account,
so both are ``confidence='suggested'`` and neither is idempotent.

Both actions ship without the one argument that identifies the target — the
audit carries account ids, not an ``import_id`` or an institution — so each
names its missing argument in the rationale, which is the shape
``RecoveryAction`` prescribes for a value unknown at construction time
(``moneybin/errors.py``). Guessing it would be worse than leaving it out:
both tools destroy state, and both are gated on a payload-bound confirmation
that a wrong target would bind to the wrong rows.
"""

from __future__ import annotations

from moneybin.audits.recipes.registry import RecipeContext
from moneybin.errors import RecoveryAction


def recipe(
    affected_ids: list[str],  # noqa: ARG001 — the remedy is per-source, not per-account
    context: RecipeContext,  # noqa: ARG001 — pure recipe
) -> list[RecoveryAction]:
    """Emit the two mutually exclusive ways to leave one source per account."""
    return [
        RecoveryAction(
            tool="import_revert",
            arguments={},
            rationale=(
                "Drop the imported investment batch that duplicates the synced "
                "ledger, if the connector is the source you want to keep. "
                "Supply import_id — read it from import_status; this deletes "
                "the only copy of those rows and has no undo."
            ),
            confidence="suggested",
            idempotent=False,
        ),
        RecoveryAction(
            tool="sync_disconnect",
            arguments={"mode": "institution"},
            rationale=(
                "Disconnect the duplicate connection instead, if the imported "
                "file is the source you want to keep. Supply institution — "
                "read it from sync_status; already-pulled rows stay, so revert "
                "their import batch too if you want them gone."
            ),
            confidence="suggested",
            idempotent=False,
        ),
    ]
