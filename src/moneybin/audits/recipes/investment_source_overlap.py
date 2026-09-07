"""Recipe for the ``investment_source_overlap`` audit (fail).

One investment account fed by two sources at once has no single ledger: the
imported rows and the synced rows interleave, so lots double-count and cost
basis mixes two accountings. ``core.dim_holdings`` withholds every figure for
such a position (``valuation_status = 'source_overlap'``), and nothing the
pipeline can re-run will clear it — the fix is to remove one of the two feeds.

**One action, because MoneyBin can only remove one of the two feeds.**
``import_revert`` deletes the batch's rows outright
(``REVERT_TABLES['manual']`` covers ``raw.manual_investment_transactions``), so
the account is left with one ledger and both readers of the overlap go quiet.
There is no counterpart for the synced feed: ``sync_disconnect`` is a remote
operation — ``SyncService.disconnect_confirmed`` calls ``client.disconnect``
and deletes nothing locally, which the tool's own confirmation states
("Previously pulled local rows remain"). Both readers keep reading exactly
those retained rows: this check joins ``raw.plaid_investment_transactions``,
and ``dim_holdings``'s ``source_overlap_accounts`` counts the ledger they feed.
Offered as a recovery it would cost a user their connection *permanently* and
leave the check failing and the holdings withheld — worse than no suggestion,
because a ``RecoveryAction`` is a claim that running it fixes the failure.

The fact still has to reach the user, because someone whose file import is the
ledger they want will reach for a disconnect on their own. It is named in the
rationale below as a caveat rather than offered as an exit.

The one action ships without the argument that identifies its target — the
audit carries account ids, not an ``import_id`` — so it names its missing
argument in the rationale, which is the shape ``RecoveryAction`` prescribes for
a value unknown at construction time (``moneybin/errors.py``). Guessing it
would be worse than leaving it out: the tool destroys state and is gated on a
payload-bound confirmation that a wrong target would bind to the wrong rows.
"""

from __future__ import annotations

from moneybin.audits.recipes.registry import RecipeContext
from moneybin.errors import RecoveryAction


def recipe(
    affected_ids: list[str],  # noqa: ARG001 — the remedy is per-source, not per-account
    context: RecipeContext,  # noqa: ARG001 — pure recipe
) -> list[RecoveryAction]:
    """Emit the one remedy that can leave a single ledger on the account."""
    return [
        RecoveryAction(
            tool="import_revert",
            arguments={},
            rationale=(
                "Drop the imported investment batch that duplicates the synced "
                "ledger. Supply import_id — read it from import_status; this "
                "deletes the only copy of those rows and has no undo. Keeping "
                "the import instead is not yet a remedy MoneyBin can run: "
                "sync_disconnect stops future pulls but leaves every row "
                "already pulled, which is what this check reads."
            ),
            confidence="suggested",
            idempotent=False,
        ),
    ]
