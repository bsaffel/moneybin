"""AccountLinksService — review-queue facade for ``app.account_link_decisions``.

Mirrors :mod:`moneybin.services.matching_service`: a thin service that composes
two Invariant-10 repos (``AccountLinksRepo`` and ``AccountLinkDecisionsRepo``)
and coordinates multi-write atomic operations using the same
``db.begin() / db.commit() / db.rollback()`` pattern.

``actor`` is the audit surface (``cli``/``mcp``); ``decided_by`` is the domain
column (``user``/``system``/``auto``). The caller supplies both.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

import duckdb

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.matching.reconciliation import record_account_merge_retirements
from moneybin.repositories.account_link_decisions_repo import AccountLinkDecisionsRepo
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo
from moneybin.services.account_resolution_types import (
    PendingLinkCandidate,
    PendingLinkGroup,
)
from moneybin.services.identity_confirmation import (
    AccountLedgerFacts,
    AccountMergeFacts,
)
from moneybin.services.ledger_overlap import probe_ledger_overlap
from moneybin.tables import (
    ACCOUNT_LINK_DECISIONS,
    ACCOUNT_LINKS,
    DIM_ACCOUNTS,
    FCT_TRANSACTIONS,
)
from moneybin.utils.parsing import signal_from_match_signals

if TYPE_CHECKING:
    from moneybin.orchestration.refresh import RefreshResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AccountLinkAcceptImpact:
    """Stable identities and physical rows touched by an account merge.

    ``link_ids`` and ``decision_ids`` carry the rows themselves, not just how
    many there are, because a confirmation bound to counts cannot see a swap:
    one pending sibling resolved elsewhere while another arrives for the same
    provisional leaves every count intact and still changes which decision the
    write auto-rejects. Both are sorted so the comparison and the MCP grant
    digest are stable — the queries below carry no ORDER BY of their own.
    """

    provisional_account_id: str
    candidate_account_id: str
    blast_radius: dict[str, int]
    link_ids: tuple[str, ...]
    decision_ids: tuple[str, ...]


def _resolve_display_name(db: Database, account_id: str) -> str:
    """Return ``display_name`` from ``core.dim_accounts``; empty string when absent.

    Thin alias that localizes the sole ``account_resolver`` seam to one place
    rather than scattering the import across both call sites.
    """
    # Function-local for the same reason as run() below: a module-level import
    # of account_resolver cycles (AccountResolver <- AccountLinkDecisionsRepo
    # <- AccountLinksService).
    from moneybin.services.account_resolver import (  # noqa: PLC0415 — circular-import avoidance
        fetch_display_name,
    )

    return fetch_display_name(db, account_id)


def _resolve_display_names(db: Database, account_ids: Iterable[str]) -> dict[str, str]:
    """Batched twin of ``_resolve_display_name`` — same resolver, same fallbacks."""
    from moneybin.services.account_resolver import (  # noqa: PLC0415 — circular-import avoidance
        fetch_display_names,
    )

    return fetch_display_names(db, account_ids)


def _resolve_core_display_names(
    db: Database, account_ids: Iterable[str]
) -> dict[str, str]:
    """Constructed names only — the half that is safe to write down."""
    from moneybin.services.account_resolver import (  # noqa: PLC0415 — circular-import avoidance
        fetch_core_display_names,
    )

    return fetch_core_display_names(db, account_ids)


class AccountLinksService:
    """Review-queue facade over ``app.account_link_decisions`` + ``app.account_links``.

    Composes ``AccountLinkDecisionsRepo`` and ``AccountLinksRepo`` for all
    mutations (Invariant 10). Multi-step atomic operations use
    ``db.begin()`` / ``db.commit()`` / ``db.rollback()`` with each repo method
    called via ``in_outer_txn=True`` — the same pattern as
    :class:`~moneybin.services.matching_service.MatchingService.set_status`.
    """

    def __init__(self, db: Database, *, actor: str = "cli") -> None:
        """Initialize with a Database and the audit surface actor."""
        self._db = db
        self._actor = actor
        self._links = AccountLinksRepo(db)
        self._decisions = AccountLinkDecisionsRepo(db)
        # Accepted transfers the account collapse retired inside set()'s
        # transaction, waiting for rematch_after_merge to disclose them. The
        # retirement has to happen before the commit (the FK invariant), the
        # disclosure is assembled after it, and on the batched path those are
        # two separate calls on this instance — so the count travels here.
        # set() folds in only what survived its writes and rematch_after_merge
        # reads-and-resets, so neither a failed accept nor a second merge can
        # disclose a retirement twice.
        self._transfers_retired_by_collapse = 0

    # ------------------------------------------------------------------
    # Read-only methods
    # ------------------------------------------------------------------

    def count_pending(self) -> int:
        """Number of DISTINCT provisional accounts with pending, non-reversed decisions.

        The review *unit* is the provisional account, not the raw decision row —
        one provisional with two candidate proposals counts as one item, not two.
        Returns 0 when the table does not yet exist.
        """
        try:
            row = self._db.execute(
                f"""
                SELECT COUNT(DISTINCT provisional_account_id)
                FROM {ACCOUNT_LINK_DECISIONS.full_name}
                WHERE status = 'pending' AND reversed_at IS NULL
                """,  # noqa: S608  # TableRef constant, no user values
            ).fetchone()
            return int(row[0]) if row else 0
        except duckdb.CatalogException:
            return 0

    def pending(self) -> list[PendingLinkGroup]:
        """Return pending decisions grouped by provisional account.

        Reads ``AccountLinkDecisionsRepo.list_pending()`` (already ordered
        ``provisional_account_id, decision_id``) and groups into
        ``PendingLinkGroup`` structs. Display names are resolved from
        ``core.dim_accounts``; empty string when the row is absent or the
        table is not yet materialized (``CatalogException`` guard).
        Read-only — no audit emitted.
        """
        rows = self._decisions.list_pending()
        if not rows:
            return []

        # list_pending() orders by provisional_account_id, decision_id — group in order.
        groups: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            pid = row["provisional_account_id"]
            groups.setdefault(pid, []).append(row)

        result: list[PendingLinkGroup] = []
        for provisional_id, decisions in groups.items():
            prov_display = _resolve_display_name(self._db, provisional_id)
            candidates = tuple(
                PendingLinkCandidate(
                    decision_id=d["decision_id"],
                    candidate_account_id=d["candidate_account_id"],
                    candidate_display_name=_resolve_display_name(
                        self._db, d["candidate_account_id"]
                    ),
                    # match_signals is already decoded by list_pending(); cast for pyright.
                    signal=signal_from_match_signals(d["match_signals"]),
                    overlap=probe_ledger_overlap(
                        self._db,
                        account_id=provisional_id,
                        against_account_id=d["candidate_account_id"],
                    ),
                )
                for d in decisions
            )
            result.append(
                PendingLinkGroup(
                    provisional_account_id=provisional_id,
                    provisional_display_name=prov_display,
                    candidates=candidates,
                    transactions=self._ledger_size(provisional_id),
                )
            )
        return result

    def _ledger_size(self, account_id: str) -> int:
        """How many transactions the account holds; 0 when core is not materialized."""
        try:
            row = self._db.execute(
                f"SELECT COUNT(*) FROM {FCT_TRANSACTIONS.full_name} "  # noqa: S608  # TableRef constant + parameterized value
                "WHERE account_id = ?",
                [account_id],
            ).fetchone()
        except duckdb.CatalogException:
            return 0
        return int(row[0]) if row else 0

    def ledger_facts(self, account_id: str) -> AccountLedgerFacts:
        """Describe one account in the terms that tell it apart from its twin.

        The masked last four is carried but never *labels* a side: the
        institution+last-four signal fires because both sides state the same
        one, so a description keyed to it renders both halves of a split account
        identically. It travels as evidence instead — agreement is the reason
        the proposal exists and disagreement argues against it, and the prompt
        used to show neither.
        """
        subtype: str | None = None
        currency: str | None = None
        last_four: str | None = None
        try:
            row = self._db.execute(
                f"""
                SELECT account_subtype, account_type, currency_code, last_four
                FROM {DIM_ACCOUNTS.full_name} WHERE account_id = ?
                """,  # noqa: S608  # TableRef constant + parameterized value
                [account_id],
            ).fetchone()
        except duckdb.CatalogException:
            row = None
        if row is not None:
            # Subtype before type, the way dim_accounts derives its own
            # display_name: "checking" reads to a human where the canonical
            # "depository" does not.
            subtype = str(row[0]) if row[0] else (str(row[1]) if row[1] else None)
            currency = str(row[2]) if row[2] else None
            last_four = str(row[3]) if row[3] else None
        try:
            ledger = self._db.execute(
                f"""
                SELECT
                    COUNT(*),
                    MIN(transaction_date),
                    MAX(transaction_date),
                    LIST(DISTINCT source_type)
                FROM {FCT_TRANSACTIONS.full_name} WHERE account_id = ?
                """,  # noqa: S608  # TableRef constant + parameterized value
                [account_id],
            ).fetchone()
        except duckdb.CatalogException:
            ledger = None
        count, first_date, last_date, sources = ledger or (0, None, None, None)
        source_list: list[Any] = list(sources) if sources else []
        return AccountLedgerFacts(
            account_id=account_id,
            display_name=_resolve_display_name(self._db, account_id),
            # Sorted because LIST() carries no order of its own, and the label it
            # feeds is compared against the other side's.
            source_types=tuple(sorted(str(s) for s in source_list if s)),
            subtype=subtype,
            currency_code=currency,
            transactions=int(count),
            first_date=first_date,
            last_date=last_date,
            last_four=last_four,
        )

    def merge_facts(
        self, *, absorbed_account_id: str, survivor_account_id: str
    ) -> AccountMergeFacts:
        """Everything the merge prompt renders: both sides plus the ledger evidence.

        Keyed on two account ids rather than a decision id so the CLI prompt, the
        single-decision MCP tool, and the identity batch all reach the same
        sentence through one call — three renderings of one merge was the
        coherence failure this replaces.
        """
        return AccountMergeFacts(
            absorbed=self.ledger_facts(absorbed_account_id),
            survivor=self.ledger_facts(survivor_account_id),
            overlap=probe_ledger_overlap(
                self._db,
                account_id=absorbed_account_id,
                against_account_id=survivor_account_id,
            ),
        )

    def _frozen_names(
        self, rows: Sequence[tuple[str, str, str]]
    ) -> dict[str, tuple[str, str]]:
        """Read both account names for each ``(decision, provisional, candidate)``.

        Called while deciding, because that is the last moment the provisional
        still resolves: accepting re-points its links onto the candidate, and
        the next transform then drops it from every name lookup MoneyBin has.

        Core only, and not even all of core: :func:`fetch_core_display_names`
        also refuses an ``'Account ' || account_id`` label, which embeds the
        source-native key of an unlinked account -- defence in depth now that
        the dim's terminal arm is the literal ``'Unnamed account'``. This value
        is persisted under a MEDIUM column
        and copied into the audit log, and the raw fallback builds its label
        from ``account_number`` / ``account_number_masked``, both classed
        CRITICAL. What it emits is already masked to the last four, so showing
        it is safe; storing it would put a CRITICAL-derived value under a MEDIUM
        declaration on the strength of a per-value judgement. An account whose
        name lives only in raw freezes as ``""`` and keeps resolving live, which
        is the same exposure the review queue already has and no new stored one.
        """
        names = _resolve_core_display_names(
            self._db,
            [
                account_id
                for _, provisional, candidate in rows
                for account_id in (provisional, candidate)
            ],
        )
        return {
            decision_id: (names.get(provisional, ""), names.get(candidate, ""))
            for decision_id, provisional, candidate in rows
        }

    def history(self, *, limit: int | None = 50) -> list[dict[str, Any]]:
        """All decisions (any status) newest-first by ``decided_at``. Read-only.

        Mirrors ``MatchingService.get_log``. Delegates to the repo (raw SQL +
        decode live in the repo layer); empty list when the table is absent.

        Each row carries both accounts' display names. A decided row reads the
        pair frozen onto it when the decision was made — the only names that
        survive an accepted merge, which removes the provisional account from
        ``core.dim_accounts`` and from the raw fallback's join in the same
        stroke. Rows that were never frozen resolve live -- a pending decision
        has not reached its freezing point, and rows written before the columns
        existed have nothing stored -- through the same resolver ``pending()``
        uses, so the two surfaces cannot disagree about what one account is
        called. A row frozen as ``""`` is not one of those: the freeze looked
        and deliberately declined to record a raw-only name, so it renders
        unnamed rather than reaching back into raw to undo its own decision.
        """
        rows = self._decisions.history(limit=limit)
        # NULL and "" are different answers, and only NULL may be re-resolved.
        # NULL means the row never reached a freezing point (pending, or written
        # before V051), so the live resolver is the only thing that can name it.
        # "" is a decision: `_frozen_names` looked, found the name only in raw,
        # and declined to record a CRITICAL-classed value. Treating both as
        # falsy sent the second one back through the raw-including resolver and
        # recovered precisely the value the freeze had just refused.
        unfrozen = [
            row[f"{key}_account_id"]
            for row in rows
            for key in ("provisional", "candidate")
            if row.get(f"{key}_display_name") is None and row.get(f"{key}_account_id")
        ]
        live = _resolve_display_names(self._db, unfrozen) if unfrozen else {}
        for row in rows:
            for key in ("provisional", "candidate"):
                if row.get(f"{key}_display_name") is None:
                    row[f"{key}_display_name"] = live.get(
                        row.get(f"{key}_account_id", ""), ""
                    )
        return rows

    def decision_by_id(self, decision_id: str) -> dict[str, Any] | None:
        """Return one exact decision row by ID."""
        return self._decisions.fetch_by_id(decision_id)

    def accept_impact(
        self,
        decision_id: str,
        *,
        target_account_id: str,
    ) -> AccountLinkAcceptImpact:
        """Preview stable identities and rows the account merge will mutate."""
        decision = self._fetch_decision(decision_id)
        if decision is None or decision["status"] != "pending":
            raise UserError(
                f"No pending account-link decision {decision_id!r}.",
                code=error_codes.MUTATION_NOTHING_TO_DO,
            )
        if target_account_id != decision["candidate_account_id"]:
            raise UserError(
                "target_account_id does not match the candidate named in "
                f"decision {decision_id!r}; pass the decision's own "
                "candidate_account_id as a confirming safety check.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        provisional_id = str(decision["provisional_account_id"])
        links = self._db.execute(
            f"""
            SELECT link_id, ref_kind FROM {ACCOUNT_LINKS.full_name}
            WHERE account_id = ? AND status = 'accepted'
            """,  # noqa: S608  # TableRef constant + parameterized value
            [provisional_id],
        ).fetchall()
        if not any(ref_kind == "source_native" for _, ref_kind in links):
            raise UserError(
                f"Cannot apply merge for decision {decision_id!r}: the "
                "provisional account has no source_native mapping to re-point "
                "onto the candidate.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )
        sibling_rows = self._db.execute(
            f"""
            SELECT decision_id FROM {ACCOUNT_LINK_DECISIONS.full_name}
            WHERE (provisional_account_id = ? OR candidate_account_id = ?)
              AND decision_id != ?
              AND status = 'pending'
              AND reversed_at IS NULL
            """,  # noqa: S608  # TableRef constant + parameterized values
            [provisional_id, provisional_id, decision_id],
        ).fetchall()
        # Mirrors set()'s own two queries exactly: every accepted link is
        # repointed, and the named decision plus every pending sibling changes
        # status. Sorted because neither query orders its rows.
        link_ids = tuple(sorted(str(link_id) for link_id, _ in links))
        decision_ids = tuple(
            sorted([decision_id, *(str(sid) for (sid,) in sibling_rows)])
        )
        return AccountLinkAcceptImpact(
            provisional_account_id=provisional_id,
            candidate_account_id=str(decision["candidate_account_id"]),
            blast_radius={
                "accounts": 2,
                "account_links": len(link_ids),
                "account_link_decisions": len(decision_ids),
            },
            link_ids=link_ids,
            decision_ids=decision_ids,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_decision(self, decision_id: str) -> dict[str, Any] | None:
        """Read one decision row by id. Returns None when not found."""
        return self._decisions.fetch_by_id(decision_id)

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    def run(self, *, decided_by: str = "auto") -> int:
        """Backfill pending link proposals for all accounts in core.dim_accounts.

        For each account in core.dim_accounts, calls
        ``AccountResolver.propose_existing`` to find weak-signal candidates
        (institution+last4 or name fuzzy-match), then writes a ``pending``
        ``app.account_link_decisions`` row for each new unordered pair.

        Dedup rules (skips a candidate if either holds):
        - A decision already exists for that pair in either direction (any status).
        - The same unordered pair was already written in this run.

        Returns the count of new ``pending`` decisions written. Returns 0 when
        ``core.dim_accounts`` is not yet materialized.
        """
        # Import here to avoid a circular-import at module level
        # (AccountResolver ← AccountLinkDecisionsRepo ← AccountLinksService would cycle).
        from moneybin.services.account_resolver import (  # noqa: PLC0415
            AccountResolver,
            refresh_account_link_pending_gauge,
        )

        resolver = AccountResolver(self._db, actor=self._actor)
        try:
            account_ids = [
                str(r[0])
                for r in self._db.execute(
                    f"SELECT account_id FROM {DIM_ACCOUNTS.full_name}",  # noqa: S608  # TableRef constant
                ).fetchall()
            ]
        except duckdb.CatalogException:
            logger.debug("core.dim_accounts unavailable in run(); returning 0")
            return 0

        # Only a provisional with an accepted source_native link can be merged
        # (set() re-points that link; without it the merge can't collapse data
        # and set() refuses it). Don't write backfill proposals that would
        # dead-end at the merge step — they'd only be resolvable as standalone.
        mergeable = {
            str(r[0])
            for r in self._db.execute(
                f"SELECT DISTINCT account_id FROM {ACCOUNT_LINKS.full_name} "  # noqa: S608  # TableRef constant
                "WHERE ref_kind = 'source_native' AND status = 'accepted'",
            ).fetchall()
        }

        new_count = 0
        # Track unordered pairs written this run to avoid A→B + B→A double-writes.
        seen_pairs: set[frozenset[str]] = set()

        self._db.begin()
        try:
            for account_id in account_ids:
                if account_id not in mergeable:
                    continue
                proposal = resolver.propose_existing(account_id)
                if proposal is None:
                    continue
                for candidate in proposal.candidates:
                    pair: frozenset[str] = frozenset({account_id, candidate.account_id})
                    if pair in seen_pairs:
                        continue
                    # Skip if any decision (any status, either direction) already covers this pair.
                    existing = self._db.execute(
                        f"""
                        SELECT 1 FROM {ACCOUNT_LINK_DECISIONS.full_name}
                        WHERE (provisional_account_id = ? AND candidate_account_id = ?)
                           OR (provisional_account_id = ? AND candidate_account_id = ?)
                        LIMIT 1
                        """,  # noqa: S608  # TableRef constant + parameterized values
                        [
                            account_id,
                            candidate.account_id,
                            candidate.account_id,
                            account_id,
                        ],
                    ).fetchone()
                    seen_pairs.add(pair)
                    if existing is not None:
                        continue
                    decision_id = uuid.uuid4().hex[:12]
                    self._decisions.insert(
                        decision_id=decision_id,
                        provisional_account_id=account_id,
                        candidate_account_id=candidate.account_id,
                        confidence_score=candidate.confidence,
                        # value = matched candidate's display_name (schema intent:
                        # "which weak signal fired + its value"), mirroring import-time.
                        # Internal field; the queue surfaces signal/display_name, not this.
                        match_signals={
                            "signal": candidate.signal,
                            "value": candidate.display_name,
                        },
                        decided_by=decided_by,
                        actor=self._actor,
                        status="pending",
                        in_outer_txn=True,
                    )
                    new_count += 1
            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise

        refresh_account_link_pending_gauge(self._db)
        logger.info(f"Account-link backfill wrote {new_count} new pending decisions")
        return new_count

    def propose_pair(
        self,
        account_id: str,
        candidate_account_id: str,
        *,
        decided_by: str = "user",
    ) -> str:
        """Queue a merge proposal for two accounts a caller names directly.

        The escape hatch for a twin no signal reaches — different last four,
        different institution, no shared name, nothing in common but a person
        knowing it is one account. Without it, surfacing a missed pair takes a
        resolver change, which puts it out of reach of every surface.

        Writes one ``pending`` decision under signal ``manual`` and merges
        nothing: the pair clears the same confirmation gate as a proposal the
        resolver found. ``confidence_score`` stays NULL — nothing was measured,
        and a number here would rank a bare assertion against real evidence.

        Argument order sets which account is absorbed, but only an account
        holding an accepted ``source_native`` link can be: :meth:`set` re-points
        that link, and without one the merge cannot collapse the data. The pair
        is flipped when only the second side qualifies, rather than queued as a
        decision that dead-ends at accept.

        Raises ``UserError`` when both ids name one account
        (MUTATION_INVALID_INPUT), either names no account this database knows —
        ``AccountResolver.knows_account_id``, which reads the links table as
        well as the dim so a just-imported account is nameable before the next
        transform (MUTATION_NOT_FOUND) — a pending or accepted decision already
        covers the pair, or neither side can be absorbed (both
        MUTATION_CONSTRAINT_VIOLATION). A *rejected* pair is re-proposable: a
        past answer is not a permanent veto, and a reject is the cheapest
        decision to make by mistake.
        """
        from moneybin.services.account_resolver import (  # noqa: PLC0415  # circular at module level
            AccountResolver,
            refresh_account_link_pending_gauge,
        )

        # Deferred so this module does not become a second eager path onto
        # import_service's graph. It is on the CLI cold path today only through
        # inbox_service, and a module-level import here would keep it there
        # even after that one is deferred. Only the refusal below needs it.
        from moneybin.services.import_service import (  # noqa: PLC0415  # cold-start hygiene
            mask_embedded_account_number,
        )

        if account_id == candidate_account_id:
            raise UserError(
                "Both ids name the same account; a link needs two different accounts.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )

        pair = [account_id, candidate_account_id]
        # Both arms, via the resolver: an account an import just minted is in
        # app.account_links and not yet in the SQLMesh-materialized dim, and
        # that freshly imported duplicate is the one this hatch exists to
        # recover. knows_account_id also declines an id whose links are all
        # reversed — already merged away — which the dim alone still answers
        # "yes" for until the next transform.
        resolver = AccountResolver(self._db, actor=self._actor)
        missing = [a for a in pair if not resolver.knows_account_id(a)]
        if missing:
            # Masked because an unresolved ``account_id`` is the source-native
            # key, which on OFX is the institution's ``<ACCTID>``. This refusal
            # reaches the CLI log and the MCP envelope, both of which outlive
            # the call that named it.
            raise UserError(
                "No account in this database for "
                f"{', '.join(repr(mask_embedded_account_number(m)) for m in missing)}.",
                code=error_codes.MUTATION_NOT_FOUND,
            )

        # A rejected or reversed decision does not block: those are past answers.
        existing = self._db.execute(
            f"""
            SELECT decision_id, status FROM {ACCOUNT_LINK_DECISIONS.full_name}
            WHERE ((provisional_account_id = ? AND candidate_account_id = ?)
                OR (provisional_account_id = ? AND candidate_account_id = ?))
              AND status IN ('pending', 'accepted')
            LIMIT 1
            """,  # noqa: S608  # TableRef constant + parameterized values
            [account_id, candidate_account_id, candidate_account_id, account_id],
        ).fetchone()
        if existing is not None:
            raise UserError(
                f"Decision {existing[0]!r} already covers this pair and is "
                f"{existing[1]}; nothing to propose.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )

        mergeable = {
            str(r[0])
            for r in self._db.execute(
                f"SELECT DISTINCT account_id FROM {ACCOUNT_LINKS.full_name} "  # noqa: S608  # TableRef constant + parameterized values
                "WHERE ref_kind = 'source_native' AND status = 'accepted' "
                "AND account_id IN (?, ?)",
                pair,
            ).fetchall()
        }
        if account_id in mergeable:
            absorbed, survivor = account_id, candidate_account_id
        elif candidate_account_id in mergeable:
            absorbed, survivor = candidate_account_id, account_id
        else:
            raise UserError(
                "Neither account holds an accepted source_native link, so a "
                "merge would have nothing to re-point and could not collapse "
                "either ledger. Import at least one of them from a source first.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )

        decision_id = uuid.uuid4().hex[:12]
        self._decisions.insert(
            decision_id=decision_id,
            provisional_account_id=absorbed,
            candidate_account_id=survivor,
            confidence_score=None,
            match_signals={
                "signal": "manual",
                "value": _resolve_display_name(self._db, survivor),
            },
            decided_by=decided_by,
            actor=self._actor,
            status="pending",
        )
        refresh_account_link_pending_gauge(self._db)
        logger.info(f"Queued manual account-link proposal {decision_id} for review")
        return decision_id

    def record_committed_outer_decisions(self) -> None:
        """Refresh metrics after an enclosing transaction commits."""
        from moneybin.services.account_resolver import (  # noqa: PLC0415
            refresh_account_link_pending_gauge,
        )

        refresh_account_link_pending_gauge(self._db)

    def set(  # noqa: A003  # mirrors the existing set_status verb shape; "set" is the surface verb
        self,
        decision_id: str,
        *,
        target_account_id: str | None,
        decided_by: str = "user",
        verify_accept: Callable[[AccountLinkAcceptImpact], None] | None = None,
        in_outer_txn: bool = False,
    ) -> RefreshResult | None:
        """Accept (merge) or standalone-reject a pending link decision atomically.

        Returns the post-merge re-match outcome (see :meth:`rematch_after_merge`)
        so the caller can report what that pass found — it may have auto-merged
        without asking. ``None`` when no pass ran: a standalone reject repoints
        nothing, and under ``in_outer_txn`` the batched caller owns the trigger.

        ``target_account_id`` is not None (accept → merge):
          1. Validates the arg matches the decision's own ``candidate_account_id``.
          2. Re-points every accepted ``source_native`` link for the provisional
             account onto ``target_account_id`` via ``AccountLinksRepo.repoint``.
          3. Marks the named decision ``accepted``.
          4. Auto-rejects every other pending, non-reversed decision for the
             same provisional account.

        ``target_account_id`` is None (standalone):
          Rejects every pending, non-reversed decision for the provisional
          account. No repoint — the provisional stays its own canonical account.

        All writes run inside one ``db.begin()`` / ``db.commit()`` transaction
        (matching :meth:`MatchingService.set_status`). Each repo method is
        called with ``in_outer_txn=True`` so it joins the enclosing transaction
        instead of opening a nested one.

        Raises ``UserError`` when:
        - ``decision_id`` is not found (MUTATION_NOT_FOUND).
        - The decision is not in ``pending`` status (MUTATION_CONSTRAINT_VIOLATION).
        - ``target_account_id`` does not match the decision's ``candidate_account_id``
          (MUTATION_INVALID_INPUT).
        """
        collapsed = 0
        if not in_outer_txn:
            self._db.begin()
        try:
            decision = self._fetch_decision(decision_id)
            if decision is None:
                raise UserError(
                    f"No account-link decision found for id {decision_id!r}.",
                    code=error_codes.MUTATION_NOT_FOUND,
                )
            if decision["status"] != "pending":
                raise UserError(
                    f"Decision {decision_id!r} is {decision['status']!r}, not pending.",
                    code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                )

            provisional_id = decision["provisional_account_id"]

            if target_account_id is not None:
                # Accept path: validate confirming arg then repoint + accept + auto-reject.
                if target_account_id != decision["candidate_account_id"]:
                    raise UserError(
                        "target_account_id does not match the candidate named in "
                        f"decision {decision_id!r}; pass the decision's own "
                        "candidate_account_id as a confirming safety check.",
                        code=error_codes.MUTATION_INVALID_INPUT,
                    )
                # Re-point ALL accepted links for the provisional onto the
                # candidate — not only source_native (the staging-JOIN key) but
                # also strong refs (persistent_token / full_number, used for
                # adoption lookups). Leaving a strong ref on the merged-away
                # provisional would later mis-adopt a source carrying the same
                # token/number onto the dead id instead of the candidate.
                links = self._db.execute(
                    f"""
                    SELECT link_id, ref_kind FROM {ACCOUNT_LINKS.full_name}
                    WHERE account_id = ? AND status = 'accepted'
                    """,  # noqa: S608  # TableRef constant + parameterized values
                    [provisional_id],
                ).fetchall()
                if not any(ref_kind == "source_native" for _, ref_kind in links):
                    # No source_native mapping to re-point means the merge can't
                    # take effect: the staging JOIN translates raw rows via
                    # source_native links, so accepting here would record a
                    # "paper merge" that never collapses the data. Refuse (rolls
                    # back) rather than silently mark the decision accepted.
                    raise UserError(
                        f"Cannot apply merge for decision {decision_id!r}: the "
                        "provisional account has no source_native mapping to "
                        "re-point onto the candidate.",
                        code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                    )
                if verify_accept is not None:
                    verify_accept(
                        self.accept_impact(
                            decision_id,
                            target_account_id=target_account_id,
                        )
                    )
                for link_id, _ref_kind in links:
                    self._links.repoint(
                        link_id=link_id,
                        new_account_id=target_account_id,
                        decided_by=decided_by,
                        actor=self._actor,
                        in_outer_txn=True,
                    )
                # Carry existing match decisions onto the survivor in the same
                # transaction. They store the account_id they were made under,
                # and both the rejected-pair key and the active-edge NodeKey are
                # built from it — so a decision left on the merged-away account
                # stops describing any live pair. The rejection in particular
                # stops matching itself, and the re-match below can then
                # auto-accept a pair the user explicitly rejected. Ordering is
                # load-bearing: this must precede the commit that rematch_after_merge
                # reads.
                repointed = MatchDecisionsRepo(self._db).repoint_account(
                    from_account_id=provisional_id,
                    to_account_id=target_account_id,
                    actor=self._actor,
                    in_outer_txn=True,
                )
                # A transfer whose two accounts just became one is retired in
                # there. That is a decision of the user's being undone, so it
                # is owed the same disclosure as the dedup retirement below —
                # dropping the count would report "0 transfers retired" over
                # an accepted transfer this call had just reversed. Held local
                # until the writes below have all survived; see the fold-in.
                collapsed = repointed.accepted_transfers_retired
                # Accept the named decision.
                accepted_names = self._frozen_names([
                    (decision_id, provisional_id, target_account_id)
                ])[decision_id]
                self._decisions.update_status(
                    decision_id,
                    status="accepted",
                    decided_by=decided_by,
                    actor=self._actor,
                    provisional_display_name=accepted_names[0],
                    candidate_display_name=accepted_names[1],
                    in_outer_txn=True,
                )
                # Auto-reject every other pending decision that touches the
                # provisional — both where it is the provisional (other
                # candidates for it) AND where it is the *candidate* (some other
                # Q→P proposal). The provisional is merged away, so a later
                # accept of Q→P would re-point Q onto a dead account; a fresh
                # run() re-proposes Q→C if Q really is the same account.
                sibling_rows = self._db.execute(
                    f"""
                    SELECT decision_id, provisional_account_id, candidate_account_id
                    FROM {ACCOUNT_LINK_DECISIONS.full_name}
                    WHERE (provisional_account_id = ? OR candidate_account_id = ?)
                      AND decision_id != ?
                      AND status = 'pending'
                      AND reversed_at IS NULL
                    """,  # noqa: S608  # TableRef constant + parameterized values
                    [provisional_id, provisional_id, decision_id],
                ).fetchall()
                sibling_names = self._frozen_names(sibling_rows)
                for sid, _, _ in sibling_rows:
                    self._decisions.update_status(
                        sid,
                        status="rejected",
                        decided_by=decided_by,
                        actor=self._actor,
                        provisional_display_name=sibling_names[sid][0],
                        candidate_display_name=sibling_names[sid][1],
                        in_outer_txn=True,
                    )
            else:
                # Standalone path: reject every pending decision for this provisional.
                pending_rows = self._db.execute(
                    f"""
                    SELECT decision_id, provisional_account_id, candidate_account_id
                    FROM {ACCOUNT_LINK_DECISIONS.full_name}
                    WHERE provisional_account_id = ?
                      AND status = 'pending'
                      AND reversed_at IS NULL
                    """,  # noqa: S608  # TableRef constant + parameterized values
                    [provisional_id],
                ).fetchall()
                rejected_names = self._frozen_names(pending_rows)
                for did, _, _ in pending_rows:
                    self._decisions.update_status(
                        did,
                        status="rejected",
                        decided_by=decided_by,
                        actor=self._actor,
                        provisional_display_name=rejected_names[did][0],
                        candidate_display_name=rejected_names[did][1],
                        in_outer_txn=True,
                    )

            if not in_outer_txn:
                self._db.commit()
        except BaseException:
            if not in_outer_txn:
                self._db.rollback()
            raise
        # Only a merge that survived every write owes the disclosure. DuckDB
        # rolls a failed one back; a counter already folded into the instance
        # would not roll back with it, and the next merge here would report a
        # transfer retired that is still accepted.
        self._transfers_retired_by_collapse += collapsed
        if in_outer_txn:
            return None
        # Accept/reject changed the pending count — refresh the gauge (only
        # reached on a successful commit; the except above re-raises).
        from moneybin.services.account_resolver import (  # noqa: PLC0415
            refresh_account_link_pending_gauge,
        )

        refresh_account_link_pending_gauge(self._db)
        if target_account_id is None:
            return None
        return self.rematch_after_merge()

    def rematch_after_merge(self) -> RefreshResult:
        """Re-run matching now that the merge made two sources' rows co-resident.

        The matcher blocks dedup candidates on ``a.account_id = b.account_id``
        (``matching/scoring.py``), so a reissued card's two sides are not even
        considered while their ids differ. The repoint above is what makes them
        candidates, and the staging views resolve it at read time — so the rows
        are matchable the instant this commits, and nothing else in the pipeline
        looks again. Skipping this is what left 377 duplicates unproposed.

        ``transform`` follows ``match`` so an auto-accepted edge collapses in
        ``core`` in the same step; ``identity`` is deliberately absent, but not
        for recursion — ``_run_identity_step`` calls ``run()`` (propose), never
        ``set()``, so it cannot re-enter here. It is absent because proposing
        new links is not this trigger's job, and running it would re-examine
        the accounts the merge just collapsed.

        Callers on the batched path (``ReviewDecisionsService.apply_identity``)
        invoke this themselves after their own commit — ``set`` returns early
        under ``in_outer_txn`` and never reaches it.

        The pass carries ``self._actor`` rather than letting ``refresh`` default:
        the decisions it writes exist because a user accepted a link on that
        surface, and ``app-integrity-invariant.md`` binds matcher-created
        decisions to the surface that caused them. ``system`` is for the
        automated callers that spec names, which this is not.

        ``transfers_retired`` sums the two ways this merge can invalidate an
        accepted transfer: its two legs turning out to be one transaction and
        its two accounts turning out to be one account. The first is the
        matcher's own reconciliation, which every trigger gets and which
        ``refresh`` already reports
        (``TransactionMatcher._retire_transfers_invalidated_by_dedup``); the
        second happens inside ``set``'s transaction and reaches no matcher, so
        it is added here. One counter because the user is owed one fact — a
        transfer they accepted is gone — and one way back for both.
        """
        # Upward call: the orchestrator's identity step composes this service,
        # so this names it back and the two modules would import each other at
        # their tops. Deferring keeps that out of a cycle; it does not make the
        # dependency go away, and test_orchestration_layering enumerates it as
        # an inversion either way.
        from moneybin.orchestration.refresh import refresh  # noqa: PLC0415

        collapsed = self._transfers_retired_by_collapse
        self._transfers_retired_by_collapse = 0
        # Emitted here rather than where the repo reverses, because this is the
        # one seam both paths reach only after their own commit — `set` calls it
        # at the end of its post-commit tail, and the batched caller calls it
        # itself once `apply_identity` commits. Nothing the repo writes is
        # durable until one of those lands, and a counter cannot be rolled back
        # with it. Best-effort by construction, so it cannot abort the rematch
        # on the next line.
        record_account_merge_retirements(collapsed)
        try:
            result = refresh(self._db, steps=["match", "transform"], actor=self._actor)
        except Exception:
            # The reversal committed with the merge, but `collapsed` has already
            # been taken off the instance and is only re-attached to the result
            # below — so without this the crash takes the one fact the user is
            # owed with it, and nothing later can reconstruct it. Stated here
            # rather than at the callers because this is where the count still
            # exists. Re-raised untouched: the failure is still the caller's to
            # report, this only refuses to lose the disclosure on the way out.
            if collapsed:
                logger.warning(
                    f"⚠️  The merge reversed {collapsed} accepted transfer(s) "
                    "before the rebuild failed; that reversal stands. Inspect "
                    "with 'moneybin system audit list' and restore with "
                    "'moneybin system audit undo <operation-id>' if that was wrong"
                )
            raise
        return replace(result, transfers_retired=collapsed + result.transfers_retired)
