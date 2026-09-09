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
audit rows backwards; :func:`misdirected_alias_ids` is how the caller names the
members the reversal moved, since only the map plus the matched view know.
The alias row stands either way: the map is append-only, and a consumer holding
the superseded id must keep resolving through it. The healing walk stops
following it, though — the two ids are transactions of their own again, so the
edge no longer means "the same transaction" and the reversal is what says so.

**A reversal moves members the reversed decision never named.**
``app.match_decisions`` is strictly pairwise, and a 3+-member dedup group is the
transitive closure of several accepted edges over one live anchor. Reversing a
non-terminal edge therefore *splits* the component: the far side re-anchors as a
group, so every member on it — not just the id that became canonical again —
stops belonging to the id its alias forwards to. Naming only the ids the matched
view newly serves would leave the rest of that side's curation sitting on the
near side's still-live transaction, where the liveness test in
:func:`_heal_stranded_curation` (and the doctor's FK invariant, which asks the
same question) cannot see it. So the caller names every alias whose target is
now a *different* live transaction, and each restore ends by putting the
curation where that member's transaction lives now.

**What the map does and does not promise.** ``old_transaction_id`` is its
primary key, so a row records the re-key that happened and is never rewritten:
after a reversal or a split the edge still names the id the member forwarded to
*then*, which may since have become a transaction of its own. The curation is
reconciled; the map is not, and it is the *curation* that every consumer joins
on. Nothing in the tree resolves a read through this table today, so treat it as
the audit of past re-keys rather than a current redirect — a consumer that
wanted one would need a supersession marker the schema does not carry, which is
a decision about ``app.transaction_id_aliases``' shape, not a local fix.

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
    AUDIT_LOG,
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
#
# `{{live_edges}}` drops the edges of re-keys a reversal took back — see
# :func:`_reversed_alias_edges`. It is a format hole rather than a fixed
# predicate because the excluded ids arrive as a bind list of unknown length.
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
), alias_edges AS (
  SELECT old_transaction_id, new_transaction_id
  FROM {TRANSACTION_ID_ALIASES.full_name}
  WHERE {{live_edges}}
), edges AS (
  SELECT old_transaction_id AS src, new_transaction_id AS dst FROM alias_edges
  UNION ALL
  SELECT new_transaction_id AS src, old_transaction_id AS dst FROM alias_edges
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

#: Recorded when a reversal takes a re-key back and nothing was actually
#: replayed — the re-key moved no curation, or a later edit blocked the
#: restore. The alias row itself stays — the map is append-only — so nothing
#: is inverted; the row exists only to carry the ``undoes_operation_id`` edge
#: that the reversal would otherwise leave unwritten. See
#: :func:`_reversed_alias_edges`.
_ALIAS_REVERSED_ACTION = f"{_ALIAS_INSERT_ACTION}.reversed"

# Which operation wrote each alias row, so a reversal of that operation can be
# read back off the audit log.
_ALIAS_INSERT_OPERATIONS_SQL = f"""
SELECT target_id, operation_id
FROM {AUDIT_LOG.full_name}
WHERE action = ? AND target_table = ? AND target_id IS NOT NULL
"""  # noqa: S608  # AUDIT_LOG is a TableRef constant; both values are parameterized

# Every live source row's own identity hash beside the canonical id its
# transaction carries *now*. The alias map cannot answer this: a row records
# where one re-key pointed at the time it happened, and a component that splits
# later re-anchors without any row changing.
_MEMBER_CANONICAL_CTE = f"""
member_canonical AS (
  SELECT
    {_identity_hash("u.source_transaction_id")} AS member_id,
    m.transaction_id AS canonical_id
  FROM {INT_TRANSACTIONS_MATCHED.full_name} AS m
  JOIN {INT_TRANSACTIONS_UNIONED.full_name} AS u
    ON u.source_type = m.source_type
   AND u.source_transaction_id = m.source_transaction_id
   AND u.account_id = m.account_id
)
"""  # noqa: S608  # TableRef constants and code-supplied column expressions only

# An alias whose forwarding target is a live transaction that is no longer the
# member's own. Normally empty; a reversal that splits a merge component fills
# it with every member of the far side, the anchor included.
#
# Both predicates are load-bearing, and the second is what keeps an ordinary
# alias *chain* out of the set. In `old -> mid -> new`, `old`'s member sits on
# `new` while its alias still names `mid`, so the ids differ and the curation is
# nevertheless exactly where it belongs — `mid` forwards on, and no live
# transaction answers to it. Only a target something still answers to means the
# curation landed on a transaction of someone else's.
_MISDIRECTED_ALIASES_SQL = f"""
WITH {_MEMBER_CANONICAL_CTE}
SELECT DISTINCT a.old_transaction_id
FROM {TRANSACTION_ID_ALIASES.full_name} AS a
JOIN member_canonical AS m
  ON m.member_id = a.old_transaction_id
WHERE m.canonical_id <> a.new_transaction_id
  AND EXISTS (
    SELECT 1 FROM member_canonical AS t
    WHERE t.canonical_id = a.new_transaction_id
  )
ORDER BY a.old_transaction_id
"""  # noqa: S608  # TableRef constants and code-supplied column expressions only

# Where each named member's transaction lives now, for the members that are not
# their own anchor. `{{ids}}` is a placeholder list, not a value.
_MEMBER_HOME_SQL = f"""
WITH {_MEMBER_CANONICAL_CTE}
SELECT member_id, canonical_id
FROM member_canonical
WHERE member_id <> canonical_id
  AND member_id IN ({{ids}})
ORDER BY member_id
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
    except Exception as exc:  # metrics must not escape post-commit
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
    except Exception as exc:  # metrics must not escape post-commit
        logger.warning(
            f"Could not record committed curation-restore metric "
            f"at {exception_origin(exc)}"
        )


def misdirected_alias_ids(db: Database) -> frozenset[str]:
    """Aliased ids forwarding onto a live transaction that is no longer theirs.

    Normally empty: an id is in the map because its own transaction took a new
    canonical id, and the forwarding moved the curation there in the same pass.
    Reversing a merge is what breaks that agreement, and reading this set on
    either side of a reversal names exactly the members that reversal moved —
    without the caller having to re-derive an identity hash the decision row does
    not carry, and without assuming the split handed back only one id.

    A member is named whether it became canonical again (a two-member merge's
    losing side) or merely re-anchored onto a *different* surviving member (the
    far side of a 3+-member component). Both cases leave its curation on the
    near side's transaction, and only the first is visible as a newly served id.
    """
    if not _relations_exist(
        db, INT_TRANSACTIONS_MATCHED, INT_TRANSACTIONS_UNIONED, TRANSACTION_ID_ALIASES
    ):
        # Same first-load precondition the derivation guards, asked of the
        # catalog rather than the view so a failed statement cannot poison the
        # caller's transaction.
        logger.debug("Alias-target liveness skipped: the alias map or view is absent")
        return frozenset()
    return frozenset(
        str(row[0]) for row in db.execute(_MISDIRECTED_ALIASES_SQL).fetchall()
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
    it. Only the curation moves back — but the *reversal* is always recorded
    against the re-key's operation, because :func:`_heal_stranded_curation`
    reads that to know the edge no longer means "the same transaction".

    **A later edit on the survivor blocks the restore.** The re-key's own
    operation never runs through :meth:`UndoService.undo` — the alias row
    refuses to undo, so this function replays the curation moves by hand — but
    it is still subject to the same block-don't-cascade rule every other undo
    is: if a later write touched the row a move would overwrite,
    :meth:`UndoService.cascade_blockers` says so and the whole re-key's
    curation is left exactly where that later write put it, rather than
    silently replaced by the pre-merge image.

    **The replay is not always the last word.** A reversal that splits a
    3+-member component re-anchors the far side onto a member that is *not* the
    id being restored, so replaying the re-key backwards puts the curation on an
    identity hash no view serves. :func:`_rehome_restored_curation` then moves it
    onto the id that member's transaction carries now — a no-op for the ordinary
    two-member reversal, where the restored id is its own anchor.

    Must run inside the caller's transaction, so the reversal and the restore
    are one atomic act. Returns the number of rows it put back; the re-home that
    follows relocates those same rows rather than adding to them, so it is
    logged rather than counted.
    """
    # Deferred imports: the dispatch registry imports every repository module,
    # and `undo_service` pulls in that same registry — both re-enter
    # `services.__init__` and back into this package, the cycle the forwarding
    # defers around.
    from moneybin.services.undo_dispatch import repo_for
    from moneybin.services.undo_service import UndoService

    audit = AuditService(db)
    undo_service = UndoService(db)
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
        blockers = undo_service.cascade_blockers(alias_event.operation_id)
        if blockers:
            # Nothing replayed, but the reversal is still recorded below — the
            # match decision itself is already reversed by the caller, so the
            # edge must stop reading as live even though this id's curation
            # was left exactly where the later edit put it.
            logger.warning(
                f"↩️  Declined to restore curation onto revived id {old_id}: a "
                f"later edit ({blockers[0]}) touched the row this re-key moved"
            )
            _record_reversal_marker(audit, old_id, alias_event, actor=actor)
            continue
        moves = [
            event
            for event in audit.events_for_operation(alias_event.operation_id)
            if event.parent_audit_id == alias_event.audit_id
            and event.target_id is not None
        ]
        if not moves:
            # Nothing to put back, but the reversal still has to be *recorded*:
            # :func:`_reversed_alias_edges` reads operation-grain undo liveness
            # to tell a stale edge from a live one, and with no curation moved
            # there are no inverse rows to carry that edge. Without this marker
            # a merge that moved no curation, was undone, and was then curated
            # by the user would still walk its dead edge later.
            _record_reversal_marker(audit, old_id, alias_event, actor=actor)
            logger.debug(
                f"Recorded a reversed re-key that carried no curation: {old_id}"
            )
            continue
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
    # Deliberately after every replay, never interleaved: a re-home writes audit
    # rows onto the very rows a later id's replay would restore, and
    # `cascade_blockers` would then read it as the later edit that blocks it.
    _rehome_restored_curation(db, sorted(revived_ids), actor=actor)
    return restored


def _rehome_restored_curation(
    db: Database, member_ids: list[str], *, actor: str
) -> int:
    """Move each restored member's curation onto the id its transaction carries now.

    The replay above returns a member's curation to its own identity hash, which
    is where it belongs only when that member is its group's anchor. Reversing a
    non-terminal edge of a 3+-member component re-anchors the far side onto one
    of its other members, so the rest of that side has to follow — otherwise the
    curation sits on an id no view serves, and the healing pass cannot recover it
    either: the walk stops at the edge this reversal just marked reversed.

    Appends no alias row, for the same reason :func:`_heal_stranded_curation`
    does not — the map is append-only and `old_transaction_id` is its primary
    key, so the member keeps forwarding to the id it forwarded to before. Only
    the curation moves. Returns the rows that landed, for the log.
    """
    if not member_ids or not _relations_exist(
        db, INT_TRANSACTIONS_MATCHED, INT_TRANSACTIONS_UNIONED
    ):
        return 0
    placeholders = ", ".join("?" for _ in member_ids)
    rows = db.execute(
        _MEMBER_HOME_SQL.format(ids=placeholders), list(member_ids)
    ).fetchall()
    curation = _curation_repos(db)
    moved = 0
    for member_id, canonical_id in rows:
        # One operation per member, for the same reason the derivation takes one
        # per re-key: an operation is the unit `system_audit_undo` reverses.
        with operation():
            for repo in curation:
                moved += _rows_landed(
                    repo.repoint_transaction(
                        old_transaction_id=str(member_id),
                        new_transaction_id=str(canonical_id),
                        actor=actor,
                        in_outer_txn=True,
                    )
                )
        logger.debug(
            f"Re-homed restored curation onto the member's current group: "
            f"{member_id} -> {canonical_id}"
        )
    return moved


def _record_reversal_marker(
    audit: AuditService, old_id: str, alias_event: AuditEvent, *, actor: str
) -> None:
    """Mark ``alias_event``'s re-key reversed with no curation-move rows to carry it.

    Shared by the two paths that put nothing back: a re-key that moved no
    curation, and one whose restore a later edit blocked. Either way
    :func:`_reversed_alias_edges` needs an ``undoes_operation_id`` row naming
    this re-key, since no undone curation-move event exists to carry it.
    """
    with operation():
        audit.record_audit_event(
            action=_ALIAS_REVERSED_ACTION,
            target=(
                TRANSACTION_ID_ALIASES.schema,
                TRANSACTION_ID_ALIASES.name,
                old_id,
            ),
            before=None,
            after=None,
            actor=actor,
            is_undo=True,
            undoes_operation_id=alias_event.operation_id,
        )


def _reversed_alias_edges(db: Database) -> frozenset[str]:
    """Superseded ids whose re-key was reversed, so their alias edge is stale.

    `matches undo` revives both halves of a merge but deliberately leaves the
    alias row standing, so the map alone cannot say whether an edge still means
    "the same transaction". Liveness cannot say either: a reversed re-key whose
    old id later died and an ordinary chained re-key are indistinguishable by
    it — old dead, new live, in both cases — so a filter reading only
    `core.fct_transactions` would drop the edges the heal exists to follow and
    still walk the stale one.

    The audit log is the record that survives the source row's deletion. Every
    alias row is written under its own operation (see :func:`_forward`), and a
    reversal marks that operation undone, so "is this re-key still in effect"
    is exactly ``UndoService``'s net-liveness question asked of that operation.
    """
    # Deferred import: `undo_service` pulls in the dispatch registry, which
    # imports every repository module — the same cycle the rest of this module
    # defers around.
    from moneybin.services.undo_service import UndoService

    rows = db.execute(
        _ALIAS_INSERT_OPERATIONS_SQL,
        [_ALIAS_INSERT_ACTION, TRANSACTION_ID_ALIASES.name],
    ).fetchall()
    if not rows:
        return frozenset()
    ids_by_operation: dict[str, list[str]] = {}
    for old_id, operation_id in rows:
        ids_by_operation.setdefault(str(operation_id), []).append(str(old_id))
    undone = UndoService(db).undone_operation_ids(ids_by_operation)
    return frozenset(old_id for op in undone for old_id in ids_by_operation[op])


def _stranded_curation_query(reversed_ids: tuple[str, ...]) -> tuple[str, list[str]]:
    """The stranded-curation query with the reversed re-keys' edges removed."""
    if not reversed_ids:
        return _STRANDED_CURATION_SQL.format(live_edges="TRUE"), []
    placeholders = ", ".join("?" for _ in reversed_ids)
    return (
        _STRANDED_CURATION_SQL.format(
            live_edges=f"old_transaction_id NOT IN ({placeholders})"
        ),
        list(reversed_ids),
    )


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


def _curation_repos(db: Database) -> tuple[Any, ...]:
    """The four repos that own a transaction's user curation, in write order."""
    # Deferred imports: the repos' base → services.audit_service chain re-enters
    # `services.__init__`, which imports this package's engine — a module-top
    # import would cycle, the same reason `engine.py` defers its repo import.
    from moneybin.repositories.transaction_categories_repo import (
        TransactionCategoriesRepo,
    )
    from moneybin.repositories.transaction_notes_repo import (
        TransactionNotesRepo,
    )
    from moneybin.repositories.transaction_splits_repo import (
        TransactionSplitsRepo,
    )
    from moneybin.repositories.transaction_tags_repo import (
        TransactionTagsRepo,
    )

    return (
        TransactionCategoriesRepo(db),
        TransactionNotesRepo(db),
        TransactionTagsRepo(db),
        TransactionSplitsRepo(db),
    )


def _forward(db: Database, *, actor: str) -> AliasForwardResult:
    """Write the derived aliases and move each superseded id's curation."""
    from moneybin.repositories.transaction_id_aliases_repo import (
        TransactionIdAliasesRepo,  # deferred for the cycle `_curation_repos` names
    )

    rows = db.execute(_PENDING_ALIASES_SQL).fetchall()
    aliases = TransactionIdAliasesRepo(db)
    curation = _curation_repos(db)
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

    Walks only edges whose re-key is still in effect — a reversed merge left its
    alias row standing but the two ids are separate transactions again, so
    following it would move a user's edit onto one they never touched.
    """
    if not _relations_exist(db, FCT_TRANSACTIONS, TRANSACTION_ID_ALIASES):
        # A first load precedes the transform that builds the fact view, and the
        # catalog is asked rather than the view for the same reason the
        # derivation asks it: a failed statement poisons a caller's transaction.
        logger.debug(
            "Stranded-curation repair skipped: the fact view or alias map is absent"
        )
        return 0

    sql, params = _stranded_curation_query(tuple(sorted(_reversed_alias_edges(db))))
    forwarded = 0
    for stranded_id, live_id, live_count in db.execute(sql, params).fetchall():
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
