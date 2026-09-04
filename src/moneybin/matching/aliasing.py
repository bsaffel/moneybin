"""Forward re-keyed canonical transaction ids and carry their curation across.

``core.fct_transactions.transaction_id`` is content-derived (ADR-015): a merged
dedup group takes its *anchor* member's immutable source-identity hash, so the
canonical id changes whenever the anchor does — a more-stable source joining the
group, or Plaid re-minting an id as a pending transaction posts. Two things must
happen at that moment, and `app.transaction_id_aliases` had neither wired
(issue #406): the superseded id must stay resolvable, and the curation hanging
off it must follow the transaction instead of being orphaned.

**One mechanism, one derivation.** Every source row knows its own identity hash
(``old``) and the canonical id its group currently carries (``new``); where the
two differ, that row's id was superseded. The same query yields the second arm
for free: a posted Plaid row's ``pending_transaction_id`` is the source id of
the pending row it replaced, so hashing *that* identity gives the id the pending
transaction used to have — the row itself is gone from ``raw`` by then, deleted
by ``handle_removed_transactions``.

**Forward at re-key, never resolve on read.** ``core.fct_transactions`` joins
``app.transaction_categories`` / ``_notes`` / ``_tags`` / ``_splits`` on
``transaction_id`` directly, and so do the doctor's FK invariants and every
curation repo. Resolving through the alias on read would mean repeating that
resolution in each of those places — several mechanisms for one fact, and the
old rows stay orphaned in the table meanwhile. Moving the rows once, at the
moment the id changes, leaves exactly one id in play everywhere downstream.

Decision history is deliberately *not* forwarded. ``app.categorization_decisions``
keys its ``decision_id`` on ``(transaction_id, attempt_number)`` and
``app.audit_log`` records the id as it stood; both describe what happened, and
their live effect (the category itself) is forwarded through
``app.transaction_categories``. Re-keying history would have to re-mint ids for
events that already occurred.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from moneybin.database import Database
from moneybin.errors import exception_origin
from moneybin.metrics.registry import (
    TRANSACTION_CURATION_FORWARDED_TOTAL,
    TRANSACTION_ID_ALIASES_WRITTEN_TOTAL,
)
from moneybin.services.mutation_context import operation
from moneybin.tables import (
    INT_TRANSACTIONS_MATCHED,
    INT_TRANSACTIONS_UNIONED,
    TRANSACTION_ID_ALIASES,
)

logger = logging.getLogger(__name__)

_SOURCE_IDENTITY_HASH = (
    "SUBSTRING(SHA256({source_type} || '|' || {source_origin} || '|' || "
    "{source_account_key} || '|' || {source_transaction_id}), 1, 16)"
)


def _identity_hash(source_transaction_id: str) -> str:
    """The ADR-015 id for a row's identity tuple, keyed on the given id column."""
    return _SOURCE_IDENTITY_HASH.format(
        source_type="u.source_type",
        source_origin="u.source_origin",
        source_account_key="u.source_account_key",
        source_transaction_id=source_transaction_id,
    )


# Both arms produce the same shape: an id a source row used to answer to, and the
# canonical id its transaction carries now. Two guards keep the append-only map
# honest. `old_id` must not itself be live — a pending row Plaid has not removed
# is still a transaction of its own, and a reversed merge hands an id back — and
# it must not already forward, so successive re-keys chain (old → mid → new)
# instead of colliding on the primary key.
_PENDING_ALIASES_SQL = f"""
WITH live AS (
  SELECT
    m.transaction_id,
    u.source_type,
    u.source_origin,
    u.source_account_key,
    u.source_transaction_id,
    u.pending_transaction_id
  FROM {INT_TRANSACTIONS_MATCHED.full_name} AS m
  JOIN {INT_TRANSACTIONS_UNIONED.full_name} AS u
    ON u.source_type = m.source_type
   AND u.source_transaction_id = m.source_transaction_id
   AND u.account_id = m.account_id
), superseded AS (
  SELECT
    {_identity_hash("u.source_transaction_id")} AS old_id,
    u.transaction_id AS new_id,
    'merge' AS cause
  FROM live AS u
  UNION ALL
  SELECT
    {_identity_hash("u.pending_transaction_id")} AS old_id,
    u.transaction_id AS new_id,
    'pending_posted' AS cause
  FROM live AS u
  WHERE u.pending_transaction_id IS NOT NULL
)
SELECT old_id, new_id, cause
FROM superseded AS s
WHERE s.old_id IS NOT NULL
  AND s.old_id <> s.new_id
  AND NOT EXISTS (SELECT 1 FROM live AS l WHERE l.transaction_id = s.old_id)
  AND NOT EXISTS (
    SELECT 1 FROM {TRANSACTION_ID_ALIASES.full_name} AS a
    WHERE a.old_transaction_id = s.old_id
  )
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.old_id ORDER BY s.cause, s.new_id) = 1
-- old_transaction_id is the map's primary key, so one superseded id forwards to
-- exactly one successor and a tie has to be broken here rather than at insert.
-- Two ways one arises. A row can be superseded both by a merge and by a
-- pending→posted transition; 'merge' sorts first, which is the id the dedup
-- group's anchor actually carries. And Plaid does not document whether one
-- pending authorization may settle as several postings -- if it does, the
-- siblings are indistinguishable to this derivation, so it takes the lowest
-- new_id: an arbitrary but stable choice, which keeps a re-run idempotent
-- rather than letting the curation land on a different sibling each pass.
ORDER BY old_id
"""  # noqa: S608  # TableRef constants and code-supplied column expressions only


@dataclass(frozen=True, slots=True)
class AliasForwardResult:
    """What one forwarding pass wrote, for the caller to report after it commits."""

    aliases_written: int = 0
    curation_rows_forwarded: int = 0

    @property
    def changed(self) -> bool:
        """Whether the pass re-keyed anything at all."""
        return bool(self.aliases_written or self.curation_rows_forwarded)


def forward_rekeyed_transaction_ids(
    db: Database, *, actor: str, in_outer_txn: bool = False
) -> AliasForwardResult:
    """Append an alias for every superseded transaction id and move its curation.

    Idempotent: a second pass over unchanged data finds nothing to write, because
    an id that already forwards is excluded from the derivation.

    Returns the counts rather than recording them — nothing here is durable until
    whoever owns the transaction commits, and a metric incremented as the row is
    written outlives the rollback that takes it back. Pass the result to
    :func:`record_committed_alias_forwarding` once the commit lands.
    """
    if not _staging_views_exist(db):
        # A first load precedes the SQLMesh apply that builds these views. That
        # is a precondition, not a failure — and the catalog is asked rather than
        # the view, because a failed statement poisons a caller's transaction.
        logger.debug("Transaction-id alias forwarding skipped: staging views absent")
        return AliasForwardResult()

    if not in_outer_txn:
        db.begin()
    try:
        # Its own operation id, deliberately not the caller's. `system_audit_undo`
        # reverses an operation as a whole, and `TransactionIdAliasesRepo` refuses
        # to undo an alias row (the map is append-only) — so folding these rows
        # into the merge's operation would make the merge itself un-undoable,
        # which is a regression on the reversibility the matcher already
        # promises. Splitting them also matches what `matches undo` leaves
        # behind: it reverses the decision and lets the alias and the forwarded
        # curation stand, because the surviving id is the anchor's own and is
        # still live after the split.
        with operation():
            result = _forward(db, actor=actor)
    except BaseException:
        if not in_outer_txn:
            db.rollback()
        raise
    if not in_outer_txn:
        db.commit()
    return result


def record_committed_alias_forwarding(result: AliasForwardResult) -> None:
    """Record the committed forwarding counts; never let telemetry escape."""
    if not result.changed:
        return
    try:
        TRANSACTION_ID_ALIASES_WRITTEN_TOTAL.inc(result.aliases_written)
        TRANSACTION_CURATION_FORWARDED_TOTAL.inc(result.curation_rows_forwarded)
    except Exception as exc:  # noqa: BLE001  # metrics must not escape post-commit
        logger.warning(
            f"Could not record committed alias-forwarding metric "
            f"at {exception_origin(exc)}"
        )


def _staging_views_exist(db: Database) -> bool:
    """Whether both staging relations the derivation reads are in the catalog."""
    row = db.execute(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE (table_schema, table_name) IN ((?, ?), (?, ?))
        """,
        [
            INT_TRANSACTIONS_MATCHED.schema,
            INT_TRANSACTIONS_MATCHED.name,
            INT_TRANSACTIONS_UNIONED.schema,
            INT_TRANSACTIONS_UNIONED.name,
        ],
    ).fetchone()
    return bool(row) and int(row[0]) == 2


def _forward(db: Database, *, actor: str) -> AliasForwardResult:
    """Write the derived aliases and move each superseded id's curation."""
    # Deferred imports: the repos' base → services.audit_service chain re-enters
    # `services.__init__`, which imports this package's engine — a module-top
    # import would cycle, the same reason `engine.py` defers its repo import.
    from moneybin.repositories.transaction_categories_repo import (  # noqa: PLC0415
        TransactionCategoriesRepo,
    )
    from moneybin.repositories.transaction_id_aliases_repo import (  # noqa: PLC0415
        TransactionIdAliasesRepo,
    )
    from moneybin.repositories.transaction_notes_repo import (  # noqa: PLC0415
        TransactionNotesRepo,
    )
    from moneybin.repositories.transaction_splits_repo import (  # noqa: PLC0415
        TransactionSplitsRepo,
    )
    from moneybin.repositories.transaction_tags_repo import (  # noqa: PLC0415
        TransactionTagsRepo,
    )

    rows = db.execute(_PENDING_ALIASES_SQL).fetchall()
    if not rows:
        return AliasForwardResult()

    aliases = TransactionIdAliasesRepo(db)
    curation = (
        TransactionCategoriesRepo(db),
        TransactionNotesRepo(db),
        TransactionTagsRepo(db),
        TransactionSplitsRepo(db),
    )
    forwarded = 0
    for old_id, new_id, cause in rows:
        alias_event = aliases.insert(
            old_transaction_id=str(old_id),
            new_transaction_id=str(new_id),
            actor=actor,
            in_outer_txn=True,
        )
        for repo in curation:
            forwarded += len(
                repo.repoint_transaction(
                    old_transaction_id=str(old_id),
                    new_transaction_id=str(new_id),
                    actor=actor,
                    # Chains every moved curation row back to the alias that
                    # caused it, so undo can walk one re-key as a unit.
                    parent_audit_id=alias_event.audit_id,
                    in_outer_txn=True,
                )
            )
        logger.debug(f"Forwarded transaction id ({cause}): {old_id} -> {new_id}")
    return AliasForwardResult(
        aliases_written=len(rows), curation_rows_forwarded=forwarded
    )
