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

**A second pass heals what the first cannot see.** The derivation above builds
its candidate set from source rows that are still present, so it says nothing
about a re-key caused by rows *disappearing*. Delete the anchor's source rows —
``ImportService.revert_confirmed`` drops an import's raw rows while the accepted
``app.match_decisions`` row survives (``REVERT_TABLES`` lists raw tables only),
and ``PlaidExtractor.handle_removed_transactions`` deletes rows on an ordinary
sync — and the merge group re-anchors to a surviving member, flipping the
canonical id back to one that already forwards away. The curation is then
stranded on an id present in no view, and the append-only map cannot be
corrected. :func:`_heal_stranded_curation` repairs that from the orphan side:
it walks the alias map *undirected* to find every id that has ever named the
transaction, and moves the curation onto the one that is live.

**A reversed merge takes the curation back, but not the alias.** `matches undo`
revives both sides of a merge as transactions of their own, so an edit written
against the superseded one has to return to it — and a category or tag the
collision branch *deleted* has to come back, which nothing but the audit trail
can reconstruct. :func:`restore_forwarded_curation` replays that re-key's own
audit rows backwards; :func:`live_superseded_ids` is how the caller names the
ids the reversal handed back, since only the map plus the matched view know.
The alias row stands either way: the map is append-only, and a consumer holding
the superseded id must keep resolving through it.

Decision history is deliberately *not* forwarded. ``app.categorization_decisions``
keys its ``decision_id`` on ``(transaction_id, attempt_number)`` and
``app.audit_log`` records the id as it stood; both describe what happened, and
their live effect (the category itself) is forwarded through
``app.transaction_categories``. Re-keying history would have to re-mint ids for
events that already occurred.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from moneybin.database import Database
from moneybin.errors import exception_origin
from moneybin.metrics.registry import (
    TRANSACTION_CURATION_FORWARDED_TOTAL,
    TRANSACTION_CURATION_RESTORED_TOTAL,
    TRANSACTION_ID_ALIASES_WRITTEN_TOTAL,
)
from moneybin.services.audit_service import AuditEvent, AuditService
from moneybin.services.mutation_context import operation
from moneybin.tables import (
    FCT_TRANSACTIONS,
    INT_TRANSACTIONS_MATCHED,
    INT_TRANSACTIONS_UNIONED,
    TRANSACTION_CATEGORIES,
    TRANSACTION_ID_ALIASES,
    TRANSACTION_NOTES,
    TRANSACTION_SPLITS,
    TRANSACTION_TAGS,
    TableRef,
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


# Curation stranded on an id no view serves, and the live id to move it to.
#
# The alias map is walked as an UNDIRECTED graph: a re-anchor can hand an id
# back, so the live id is as often the stranded id's predecessor as its
# successor, and only the connected component answers "which ids have ever
# named this transaction". `UNION` (not `UNION ALL`) in the recursive term is
# the visited set -- it terminates at the fixpoint, so a cycle cannot hang the
# walk. `core.fct_transactions` is the liveness oracle deliberately: it is what
# `app_transaction_categories_fk` anti-joins, so a component this query calls
# live is one the doctor will too.
#
# A component with no live member produces no row here at all (the JOIN drops
# it), which is the "leave it alone" case; `live_count` distinguishes the other
# one, where several ids in the component are live and nothing says which the
# curation belongs to.
_STRANDED_CURATION_SQL = f"""
WITH RECURSIVE curated AS (
  SELECT DISTINCT transaction_id FROM {TRANSACTION_CATEGORIES.full_name}
  UNION
  SELECT DISTINCT transaction_id FROM {TRANSACTION_NOTES.full_name}
  UNION
  SELECT DISTINCT transaction_id FROM {TRANSACTION_TAGS.full_name}
  UNION
  SELECT DISTINCT transaction_id FROM {TRANSACTION_SPLITS.full_name}
), live AS (
  -- Materialized once and anti-joined, never correlated: core.fct_transactions
  -- is the whole merge/dedup/categorization pipeline, and a per-row subquery
  -- over it is O(N x view). Same reason the doctor's FK invariant does this.
  SELECT DISTINCT transaction_id FROM {FCT_TRANSACTIONS.full_name}
), stranded AS (
  SELECT c.transaction_id
  FROM curated AS c
  LEFT JOIN live AS l ON l.transaction_id = c.transaction_id
  WHERE l.transaction_id IS NULL
), edges AS (
  SELECT old_transaction_id AS src, new_transaction_id AS dst
  FROM {TRANSACTION_ID_ALIASES.full_name}
  UNION ALL
  SELECT new_transaction_id AS src, old_transaction_id AS dst
  FROM {TRANSACTION_ID_ALIASES.full_name}
), component AS (
  SELECT transaction_id AS stranded_id, transaction_id AS member FROM stranded
  UNION
  SELECT c.stranded_id, e.dst
  FROM component AS c
  JOIN edges AS e ON e.src = c.member
)
SELECT
  c.stranded_id,
  MIN(c.member) AS live_id,
  COUNT(*) AS live_count
FROM component AS c
JOIN live AS l ON l.transaction_id = c.member
GROUP BY c.stranded_id
ORDER BY c.stranded_id
"""  # noqa: S608  # TableRef constants only


#: The audit action every alias row is written under; the anchor `matches undo`
#: walks back from a revived id to the curation that re-key carried away.
_ALIAS_INSERT_ACTION = "transaction_id_alias.insert"

# An aliased id that the matched view still serves. Normally empty: an id is in
# the map because it stopped being canonical. A reversed merge puts one back.
_LIVE_SUPERSEDED_IDS_SQL = f"""
SELECT DISTINCT a.old_transaction_id
FROM {TRANSACTION_ID_ALIASES.full_name} AS a
JOIN {INT_TRANSACTIONS_MATCHED.full_name} AS m
  ON m.transaction_id = a.old_transaction_id
"""  # noqa: S608  # TableRef constants only


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
    if not _relations_exist(db, INT_TRANSACTIONS_MATCHED, INT_TRANSACTIONS_UNIONED):
        # A first load precedes the SQLMesh apply that builds these views. That
        # is a precondition, not a failure — and the catalog is asked rather than
        # the view, because a failed statement poisons a caller's transaction.
        logger.debug("Transaction-id alias forwarding skipped: staging views absent")
        return AliasForwardResult()

    if not in_outer_txn:
        db.begin()
    try:
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


def record_committed_curation_restore(rows_restored: int) -> None:
    """Record the committed restore count; never let telemetry escape."""
    if not rows_restored:
        return
    try:
        TRANSACTION_CURATION_RESTORED_TOTAL.inc(rows_restored)
    except Exception as exc:  # noqa: BLE001  # metrics must not escape post-commit
        logger.warning(
            f"Could not record committed curation-restore metric "
            f"at {exception_origin(exc)}"
        )


def live_superseded_ids(db: Database) -> frozenset[str]:
    """Aliased ids the matched view is currently serving under their own name.

    An id in the map is normally *not* live — being superseded is what put it
    there. It comes back when the merge that superseded it is reversed, so
    reading this set on either side of a reversal names exactly the transactions
    that reversal handed back, without the caller having to re-derive an
    identity hash the decision row does not carry.
    """
    if not _relations_exist(db, INT_TRANSACTIONS_MATCHED, TRANSACTION_ID_ALIASES):
        # Same first-load precondition the derivation guards, asked of the
        # catalog rather than the view so a failed statement cannot poison the
        # caller's transaction.
        logger.debug("Superseded-id liveness skipped: the alias map or view is absent")
        return frozenset()
    return frozenset(
        str(row[0]) for row in db.execute(_LIVE_SUPERSEDED_IDS_SQL).fetchall()
    )


def restore_forwarded_curation(
    db: Database, *, revived_ids: Iterable[str], actor: str
) -> int:
    """Give each revived id back the curation its re-key moved away.

    Called when a reversal hands a superseded id back as a transaction of its
    own. Every row the forwarding touched is chained to that id's alias by
    ``parent_audit_id``, and each of those audit rows carries the full
    before/after image, so replaying them backwards through
    :meth:`BaseRepo.undo_event` restores the *exact* prior state — including the
    row a collision deleted outright, which nothing else can reconstruct: two
    identical tags, or two categorizations of equal authority, are
    indistinguishable once one of them is gone.

    **The alias itself is deliberately not reversed.** The map is append-only,
    and a consumer still holding the superseded id has to keep resolving through
    it. Only the curation moves back.

    Must run inside the caller's transaction, so the reversal and the restore
    are one atomic act. Returns the number of rows it put back.
    """
    # Deferred import: the dispatch registry imports every repository module,
    # whose base → services.audit_service chain re-enters `services.__init__`
    # and back into this package. Same cycle the forwarding defers around.
    from moneybin.services.undo_dispatch import repo_for  # noqa: PLC0415

    audit = AuditService(db)
    restored = 0
    for old_id in sorted(revived_ids):
        alias_events = audit.list_events(
            action_pattern=_ALIAS_INSERT_ACTION,
            target_table=TRANSACTION_ID_ALIASES.name,
            target_id=old_id,
            limit=1,
        )
        if not alias_events:
            continue  # live and aliased, but nothing here re-keyed it
        alias_event = alias_events[0]
        moves = [
            event
            for event in audit.events_for_operation(alias_event.operation_id)
            if event.parent_audit_id == alias_event.audit_id
            and event.target_id is not None
        ]
        if not moves:
            continue  # the re-key carried no curation
        # Its own operation, mirroring the forwarding it reverses: the caller's
        # operation stays a plain, still-undoable reversal rather than becoming
        # half an undo, which `UndoService.history` would then hide.
        with operation():
            # Reverse write order: the arrival is undone before the departure is
            # restored, so a primary-key move never collides with itself.
            for event in reversed(moves):
                repo = repo_for(
                    event.target_schema or "",
                    event.target_table or "",
                    db,
                    audit=audit,
                )
                if repo.undo_event(event, actor=actor, in_outer_txn=True) is not None:
                    restored += 1
        logger.debug(f"Restored curation onto a revived transaction id: {old_id}")
    return restored


def _relations_exist(db: Database, *refs: TableRef) -> bool:
    """Whether every named relation (view or table) is in the catalog."""
    pairs = ", ".join("(?, ?)" for _ in refs)
    row = db.execute(
        f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE (table_schema, table_name) IN ({pairs})
        """,  # noqa: S608  # `pairs` is placeholders; every value is parameterized
        [value for ref in refs for value in (ref.schema, ref.name)],
    ).fetchone()
    return bool(row) and int(row[0]) == len(refs)


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
    aliases = TransactionIdAliasesRepo(db)
    curation = (
        TransactionCategoriesRepo(db),
        TransactionNotesRepo(db),
        TransactionTagsRepo(db),
        TransactionSplitsRepo(db),
    )
    forwarded = 0
    for old_id, new_id, cause in rows:
        # One operation per re-key, never one per pass — and never the caller's.
        #
        # Not the caller's, because `system_audit_undo` reverses an operation as
        # a whole and `TransactionIdAliasesRepo` refuses to undo an alias row
        # (the map is append-only): folding these into the merge's operation
        # would make the merge itself un-undoable.
        #
        # One per re-key, because `matches undo` reverses exactly one merge and
        # :func:`restore_forwarded_curation` reverses that merge's curation
        # moves. Those inverse rows name their operation in
        # ``undoes_operation_id``, and `UndoService` reads that at operation
        # grain — so a pass-wide operation would be marked undone by a restore
        # that only touched one of its re-keys, and the *other* re-keys would
        # stop blocking undo of the edits they moved.
        with operation():
            alias_event = aliases.insert(
                old_transaction_id=str(old_id),
                new_transaction_id=str(new_id),
                actor=actor,
                in_outer_txn=True,
            )
            for repo in curation:
                forwarded += _rows_landed(
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

    # Second pass, deliberately after the derivation: the ids it just moved
    # curation onto are live, so they are not stranded and this finds nothing
    # extra to do for them.
    forwarded += _heal_stranded_curation(db, curation, actor=actor)
    return AliasForwardResult(
        aliases_written=len(rows), curation_rows_forwarded=forwarded
    )


def _rows_landed(events: tuple[AuditEvent, ...]) -> int:
    """How many curation rows a repoint actually left on the new id.

    Not ``len(events)``: a repoint emits an event per row *identity* it changed,
    and several of those record a departure rather than an arrival — a
    superseded categorization dropped for a more authoritative one, a duplicate
    tag collapsed, and the delete half of a primary-key move. Counting arrivals
    keeps the counter answering the question its name asks, which is how much of
    the user's curation rode along.
    """
    return sum(1 for event in events if event.after_value is not None)


def _heal_stranded_curation(
    db: Database, curation: tuple[Any, ...], *, actor: str
) -> int:
    """Move curation off an id no view serves onto the live id of its component.

    Appends no alias row. The map is append-only and already records how the id
    got here; what went wrong is only that the curation stopped tracking the
    canonical id, so only the curation moves. Idempotent for the same reason a
    repointed row is no longer stranded.
    """
    if not _relations_exist(db, FCT_TRANSACTIONS, TRANSACTION_ID_ALIASES):
        # A first load precedes the transform that builds the fact view, and the
        # catalog is asked rather than the view for the same reason the
        # derivation asks it: a failed statement poisons a caller's transaction.
        logger.debug(
            "Stranded-curation repair skipped: the fact view or alias map is absent"
        )
        return 0

    forwarded = 0
    for stranded_id, live_id, live_count in db.execute(
        _STRANDED_CURATION_SQL
    ).fetchall():
        if int(live_count) != 1:
            # Several ids in the component are live, so the transaction the
            # curation was written against split back apart. Guessing one would
            # move a user's edit onto a transaction they never edited; the
            # doctor's FK invariant reports it instead.
            logger.debug(
                f"Stranded curation left in place: {stranded_id} resolves to "
                f"{live_count} live transaction ids"
            )
            continue
        # One operation per healed id, for the same reason the derivation takes
        # one per re-key: an operation is the unit `system_audit_undo` reverses.
        with operation():
            for repo in curation:
                forwarded += _rows_landed(
                    repo.repoint_transaction(
                        old_transaction_id=str(stranded_id),
                        new_transaction_id=str(live_id),
                        actor=actor,
                        in_outer_txn=True,
                    )
                )
        logger.debug(f"Healed stranded curation: {stranded_id} -> {live_id}")
    return forwarded
