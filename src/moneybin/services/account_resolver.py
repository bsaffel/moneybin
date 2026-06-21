"""AccountResolver — source account -> one canonical, opaque account_id.

Runs on every import/sync (replaces ImportService._resolve_account_via_matcher).
Mirrors the transaction matcher: blocking (strong refs) -> score (weak candidates)
-> adopt / mint / propose. Writes app.account_links + app.account_link_decisions
through their Invariant-10 repos. See docs/specs/account-identity-resolution.md
Decision 3 (resolution ladder).
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass

import duckdb

from moneybin.database import Database
from moneybin.extractors.tabular.account_matching import match_account
from moneybin.metrics.registry import (
    ACCOUNT_LINK_CONFIDENCE,
    ACCOUNT_LINK_REVIEW_PENDING,
)
from moneybin.repositories.account_link_decisions_repo import AccountLinkDecisionsRepo
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.services.account_resolution_types import (
    AccountCandidate,
    AccountProposal,
    ResolvedAccount,
    SourceAccount,
)
from moneybin.tables import ACCOUNT_LINK_DECISIONS, ACCOUNT_LINKS, DIM_ACCOUNTS
from moneybin.utils import slugify

logger = logging.getLogger(__name__)


def refresh_account_link_pending_gauge(db: Database) -> None:
    """Set ACCOUNT_LINK_REVIEW_PENDING from the live review-queue depth.

    Called at the two sites that change the count: the resolver's candidate
    pass (adds proposals) and ``AccountLinksService.set`` (accept/reject clears
    them). Keeps the gauge honest in both directions rather than only counting
    up. Counts DISTINCT provisional accounts — the review *unit* is the
    provisional, not the raw decision row — so the gauge matches
    ``AccountLinksService.count_pending`` and the queue users actually see.
    """
    row = db.execute(
        f"SELECT COUNT(DISTINCT provisional_account_id) "  # noqa: S608  # TableRef constant, no user input
        f"FROM {ACCOUNT_LINK_DECISIONS.full_name} "
        "WHERE status = 'pending' AND reversed_at IS NULL"
    ).fetchone()
    ACCOUNT_LINK_REVIEW_PENDING.set(int(row[0]) if row else 0)


def fetch_display_name(db: Database, account_id: str) -> str:
    """Return ``display_name`` from ``core.dim_accounts``; empty string when absent.

    Shared by the resolver's candidate decode and the account-link review queue.
    Guards ``duckdb.CatalogException`` so callers work before the core layer is
    materialized (e.g. during initial import before a SQLMesh run).
    """
    try:
        row = db.execute(
            f"SELECT display_name FROM {DIM_ACCOUNTS.full_name} "  # noqa: S608  # TableRef constant + parameterized value
            "WHERE account_id = ? LIMIT 1",
            [account_id],
        ).fetchone()
    except duckdb.CatalogException:
        return ""
    return str(row[0]) if row and row[0] is not None else ""


# Cap on the fallback pick-list (existing accounts surfaced for the human to pick
# from when no real signal cleared). Bounds an otherwise-unbounded "list all
# accounts" so a large book doesn't dump everything; a personal-finance user
# rarely exceeds this.
_FALLBACK_CANDIDATE_CAP = 25


@dataclass(frozen=True)
class _Candidate:
    """A weak-signal candidate for a pending merge proposal.

    ``confidence`` is informational metadata only — weak signals always go to
    review regardless of score, so a fixed value per signal type is correct.
    """

    account_id: str
    signal: str  # "institution_last4" | "name" | "institution" | "fallback"
    value: str
    confidence: float


class AccountResolver:
    """Resolve a source account to a canonical account_id via the M1S ladder."""

    def __init__(self, db: Database, *, actor: str = "system") -> None:
        """Bind the resolver to a database + audit actor for its link writes."""
        self._db = db
        self._actor = actor
        self._links = AccountLinksRepo(db)
        self._decisions = AccountLinkDecisionsRepo(db)

    def resolve(self, src: SourceAccount) -> ResolvedAccount:
        """Resolve one source account to a canonical account_id via the ladder.

        Ladder: explicit binding (step 0) -> strong confirmer / idempotency
        (step 1, A3) -> candidate pass / mint + propose (step 2, A4).

        All writes for one account run in a single transaction (atomic per
        account): a mid-resolve failure rolls back, so a later same-id import
        cannot adopt a half-written account. resolve() owns the transaction —
        it is always called outside one (the per-write repo transactions it
        composes succeed today, proving no enclosing transaction), so the
        composed writes pass in_outer_txn=True to join this one.
        """
        self._db.begin()
        try:
            result = self._run_ladder(src)
        except BaseException:
            self._db.rollback()
            raise
        self._db.commit()
        return result

    def _run_ladder(self, src: SourceAccount) -> ResolvedAccount:
        """Resolution-ladder body; runs inside ``resolve()``'s transaction."""
        # Step 0 - explicit binding: caller pinned identity, adopt above detection.
        if src.explicit_account_id:
            self._write_native_mapping(
                src, account_id=src.explicit_account_id, decided_by="user"
            )
            self._write_strong_ref(
                src, account_id=src.explicit_account_id, decided_by="user"
            )
            return ResolvedAccount(
                account_id=src.explicit_account_id,
                is_new=False,
                outcome="adopted_strong",
            )
        # Step 1 - strong confirmer / idempotency: source_native, then
        # persistent_token, then scoped full_number. Hit -> auto-adopt.
        strong = self._lookup_strong_ref(src)
        if strong is not None:
            adopted, _kind = strong
            self._write_native_mapping(src, account_id=adopted, decided_by="auto")
            self._write_strong_ref(src, account_id=adopted)
            return ResolvedAccount(
                account_id=adopted, is_new=False, outcome="adopted_strong"
            )
        # force_standalone: caller declared a NEW account. Mint + record refs but
        # skip the candidate pass (no merge proposal). Placed after the strong-ref
        # lookup so a re-import of the same source_native stays idempotent.
        if src.force_standalone:
            account_id = uuid.uuid4().hex[:12]
            self._write_native_mapping(src, account_id=account_id, decided_by="user")
            self._write_strong_ref(src, account_id=account_id, decided_by="user")
            return ResolvedAccount(
                account_id=account_id, is_new=True, outcome="minted_new"
            )
        # Step 2 - candidate pass. Mint first (never orphaned), then propose.
        account_id = uuid.uuid4().hex[:12]
        self._write_native_mapping(src, account_id=account_id, decided_by="auto")
        # Claim the mint's strong refs (persistent_token / scoped full_number) so
        # a later source carrying the same id auto-adopts (step 1) instead of
        # minting a duplicate. Safe: step 1 above already proved no conflict.
        self._write_strong_ref(src, account_id=account_id, decided_by="auto")

        candidates = self._find_candidates(src, exclude_account_id=account_id)
        if not candidates:
            return ResolvedAccount(
                account_id=account_id, is_new=True, outcome="minted_new"
            )

        pending_ids: list[str] = []
        for cand in candidates:
            decision_id = uuid.uuid4().hex[:12]
            self._decisions.insert(
                decision_id=decision_id,
                provisional_account_id=account_id,
                candidate_account_id=cand.account_id,
                confidence_score=cand.confidence,
                match_signals={"signal": cand.signal, "value": cand.value},
                decided_by="auto",
                actor=self._actor,
                match_reason=cand.signal,
                in_outer_txn=True,  # joins resolve()'s per-account transaction
            )
            ACCOUNT_LINK_CONFIDENCE.observe(cand.confidence)
            pending_ids.append(decision_id)
        refresh_account_link_pending_gauge(self._db)
        return ResolvedAccount(
            account_id=account_id,
            is_new=True,
            pending_decision_ids=tuple(pending_ids),
            outcome="pending_review",
        )

    def propose(self, src: SourceAccount, *, fallback: bool = False) -> AccountProposal:
        """Compute the resolver verdict without writing anything (read-only preview).

        Follows the resolve() ladder — explicit binding, strong ref, candidate
        pass — but performs no writes: no mint is persisted, no account_links row
        is inserted, no account_link_decisions row is created. Safe to call at any
        point in the import flow, including before confirm.

        ``fallback`` (default False) controls the candidate pass only. When True,
        a candidate pass that finds no real last4/name match still returns a
        decision-support pick-list of existing accounts (see _fallback_candidates)
        instead of an empty set. Only the bare single-account import gate opts in
        — there is genuinely no signal there, so an empty pick-list would force a
        raw account id. The multi-account gate leaves it False: a no-match named
        account mints a new standalone account (it never auto-merges), and turning
        on fallback there would gate every fresh multi-account import. resolve()
        never uses fallback, so these candidates are preview-only — confirming
        "new" still mints.

        The proposed_account_id in the mint path (is_new=True) is a preview id
        (uuid4[:12]) that is NOT written anywhere; resolve() will produce a
        different real id when the import is actually committed.
        """
        # Step 0 - explicit binding.
        if src.explicit_account_id:
            return AccountProposal(
                source_account_key=src.source_account_key,
                proposed_account_id=src.explicit_account_id,
                is_new=False,
                adopted_via="explicit",
            )
        # Step 1 - strong ref.
        strong = self._lookup_strong_ref(src)
        if strong is not None:
            adopted, kind = strong
            return AccountProposal(
                source_account_key=src.source_account_key,
                proposed_account_id=adopted,
                is_new=False,
                adopted_via=kind,
            )
        # force_standalone: declared-new verdict, no candidate pass. adopted_via
        # "explicit" so requires_confirm is False (the caller already decided).
        # No preview id — resolve() mints the real one at commit time.
        if src.force_standalone:
            return AccountProposal(
                source_account_key=src.source_account_key,
                proposed_account_id=None,
                is_new=True,
                adopted_via="explicit",
            )
        # Step 2 - candidate pass. Mint a preview id (NOT written anywhere).
        # fallback is caller-controlled (see docstring): the bare single-account
        # gate opts in for a decision-support pick-list; the multi-account gate
        # leaves it off so a no-match named account still mints silently.
        preview_id = uuid.uuid4().hex[:12]
        raw_candidates = self._find_candidates(
            src, exclude_account_id=preview_id, fallback=fallback
        )
        candidates = tuple(
            AccountCandidate(
                account_id=c.account_id,
                display_name=self._fetch_display_name(c.account_id),
                confidence=c.confidence,
                signal=c.signal,
            )
            for c in raw_candidates
        )
        return AccountProposal(
            source_account_key=src.source_account_key,
            proposed_account_id=preview_id,
            is_new=True,
            candidates=candidates,
            adopted_via=None,
        )

    def propose_existing(self, account_id: str) -> AccountProposal | None:
        """Backfill verdict for an account already in core.dim_accounts.

        Looks up the account's institution_name, last_four, and display_name,
        builds a synthetic SourceAccount (source_type/source_origin="backfill";
        the candidate pass only uses last_four, institution, account_name), then
        delegates to _find_candidates excluding the account itself.

        Returns None when the account is absent from dim_accounts, when
        core.dim_accounts is not yet materialized, or when no candidates are
        found. Read-only — writes nothing.
        """
        try:
            row = self._db.execute(
                f"SELECT institution_name, last_four, display_name "  # noqa: S608  # TableRef + parameterized value
                f"FROM {DIM_ACCOUNTS.full_name} WHERE account_id = ? LIMIT 1",
                [account_id],
            ).fetchone()
        except duckdb.CatalogException:
            logger.debug("core.dim_accounts unavailable in propose_existing")
            return None
        if row is None:
            return None
        institution_name, last_four, display_name = row
        src = SourceAccount(
            source_type="backfill",
            source_origin="backfill",
            source_account_key="",
            account_name=str(display_name or ""),
            last_four=str(last_four) if last_four is not None else None,
            institution=str(institution_name) if institution_name is not None else None,
        )
        raw_candidates = self._find_candidates(src, exclude_account_id=account_id)
        if not raw_candidates:
            return None
        candidates = tuple(
            AccountCandidate(
                account_id=c.account_id,
                display_name=self._fetch_display_name(c.account_id),
                confidence=c.confidence,
                signal=c.signal,
            )
            for c in raw_candidates
        )
        return AccountProposal(
            source_account_key="",
            proposed_account_id=account_id,
            is_new=False,
            candidates=candidates,
        )

    def _fetch_display_name(self, account_id: str) -> str:
        """Return display_name from core.dim_accounts for a candidate account_id."""
        return fetch_display_name(self._db, account_id)

    def _write_native_mapping(
        self, src: SourceAccount, *, account_id: str, decided_by: str
    ) -> None:
        """Write (or no-op if already mapped to this account) the source_native mapping.

        If the native key is already accepted onto a *different* canonical account,
        raise rather than silently returning a mismatched verdict — a silent
        re-point would corrupt the staging translation JOIN. Re-pointing is an
        explicit, surfaced operation (M1S.5), never an implicit import-time side
        effect (spec "Magic stays visible").
        """
        existing = self._db.execute(
            f"SELECT account_id FROM {ACCOUNT_LINKS.full_name} "  # noqa: S608  # TableRef + parameterized values
            "WHERE status = 'accepted' AND ref_kind = 'source_native' "
            "AND source_type = ? AND source_origin = ? AND ref_value = ? LIMIT 1",
            [src.source_type, src.source_origin, src.source_account_key],
        ).fetchone()
        if existing is not None:
            if existing[0] != account_id:
                raise ValueError(
                    "account_links: source_native already accepted for a different "
                    f"account_id; existing={existing[0]!r}, requested={account_id!r}"
                )
            return
        self._links.insert(
            link_id=uuid.uuid4().hex[:12],
            account_id=account_id,
            ref_kind="source_native",
            ref_value=src.source_account_key,
            source_type=src.source_type,
            source_origin=src.source_origin,
            decided_by=decided_by,
            actor=self._actor,
            in_outer_txn=True,  # joins resolve()'s per-account transaction
        )

    def source_native_exists(
        self, source_type: str, source_origin: str, source_account_key: str
    ) -> bool:
        """True if an accepted ``source_native`` link already maps this exact key.

        Used by the bare-file import path to detect an exact-same-file re-import
        (content-derived key already seen) and adopt via the Step-1 ladder
        without re-prompting. Read-only.
        """
        row = self._db.execute(
            f"SELECT 1 FROM {ACCOUNT_LINKS.full_name} "  # noqa: S608  # TableRef + parameterized values
            "WHERE status = 'accepted' AND ref_kind = 'source_native' "
            "AND source_type = ? AND source_origin = ? AND ref_value = ? LIMIT 1",
            [source_type, source_origin, source_account_key],
        ).fetchone()
        return row is not None

    def _lookup_strong_ref(self, src: SourceAccount) -> tuple[str, str] | None:
        """Return (account_id, ref_kind) if any accepted strong ref matches, else None.

        Checks source_native first (same-source re-import), then persistent_token
        (cross-connection identity), then scoped full_number (cross-source format).
        The ref_kind is surfaced so propose() can populate adopted_via accurately.
        """
        # A source_native ref is the EXACT source_account_key (a slug). For a
        # mutable-label source (CSV / aggregator export) that slug derives from
        # the account label, so a RENAMED account yields a DIFFERENT slug and
        # misses here by design — it then falls through to the candidate pass,
        # which re-associates it onto the original account via institution+last4
        # as a review PROPOSAL (never a silent merge). Decision 8
        # (account-identity-resolution.md): a mutable label is a Tier-B
        # suggestion, not a hard auto-adopt key.
        row = self._db.execute(
            f"SELECT account_id FROM {ACCOUNT_LINKS.full_name} "  # noqa: S608  # TableRef + parameterized values
            "WHERE status = 'accepted' AND ref_kind = 'source_native' "
            "AND source_type = ? AND source_origin = ? AND ref_value = ? LIMIT 1",
            [src.source_type, src.source_origin, src.source_account_key],
        ).fetchone()
        if row is not None:
            return str(row[0]), "source_native"
        if src.persistent_token:
            row = self._db.execute(
                f"SELECT account_id FROM {ACCOUNT_LINKS.full_name} "  # noqa: S608  # TableRef + parameterized values
                "WHERE status = 'accepted' AND ref_kind = 'persistent_token' "
                "AND ref_value = ? LIMIT 1",
                [src.persistent_token],
            ).fetchone()
            if row is not None:
                return str(row[0]), "persistent_token"
        scoped = self._scoped_full_number(src)
        if scoped is not None:
            row = self._db.execute(
                f"SELECT account_id FROM {ACCOUNT_LINKS.full_name} "  # noqa: S608  # TableRef + parameterized values
                "WHERE status = 'accepted' AND ref_kind = 'full_number' "
                "AND ref_value = ? LIMIT 1",
                [scoped],
            ).fetchone()
            if row is not None:
                return str(row[0]), "full_number"
        return None

    @staticmethod
    def _scoped_full_number(src: SourceAccount) -> str | None:
        """Return the full_number only when institution/routing-scoped.

        A bare number (no ':' scope) is NOT a strong ref — it is demoted to a
        candidate signal (handled in A4).
        """
        n = src.account_number
        if n and ":" in n:
            return n
        return None

    def _write_strong_ref(
        self, src: SourceAccount, *, account_id: str, decided_by: str = "auto"
    ) -> None:
        """Record this source's persistent_token / scoped full_number if not yet present.

        If the ref is already accepted onto a *different* account, log a warning and
        leave it: source_native-wins is the deterministic adoption rule, so the
        token/number is never silently re-pointed. Surfacing the low-certainty
        conflict applies "Magic stays visible" (design-principles.md).
        """
        for ref_kind, ref_value in (
            ("persistent_token", src.persistent_token),
            ("full_number", self._scoped_full_number(src)),
        ):
            if not ref_value:
                continue
            existing = self._db.execute(
                f"SELECT account_id FROM {ACCOUNT_LINKS.full_name} "  # noqa: S608  # TableRef + parameterized values
                "WHERE status = 'accepted' AND ref_kind = ? AND ref_value = ? LIMIT 1",
                [ref_kind, ref_value],
            ).fetchone()
            if existing is not None:
                if existing[0] != account_id:
                    logger.warning(
                        f"Strong ref {ref_kind} already bound to account "
                        f"{existing[0]!r}; adopted {account_id!r} via source_native "
                        "and did not re-point it."
                    )
                continue
            self._links.insert(
                link_id=uuid.uuid4().hex[:12],
                account_id=account_id,
                ref_kind=ref_kind,
                ref_value=ref_value,
                source_type=src.source_type,
                source_origin=src.source_origin,
                decided_by=decided_by,
                actor=self._actor,
                in_outer_txn=True,  # joins resolve()'s per-account transaction
            )

    def _find_candidates(
        self, src: SourceAccount, *, exclude_account_id: str, fallback: bool = False
    ) -> list[_Candidate]:
        """Weak-signal candidates from core.dim_accounts (institution+last4, then name).

        Each is a review proposal, never an auto-merge. Returns no candidates if
        core.dim_accounts is not yet materialized (first import before any transform).

        ``fallback`` (interactive import gate only — never the backfill link
        queue): when no last4/name signal clears, surface existing accounts as a
        low-confidence pick-list so the human picks from a list instead of an
        empty set. Off by default so ``accounts_links_run`` isn't flooded with an
        all-accounts proposal for every provisional account.
        """
        try:
            out: list[_Candidate] = []
            if (
                src.last_four
                and src.institution
                and (target_inst := slugify(src.institution))
            ):
                # institution_name holds raw source text (OFX <ORG> "CHASE"),
                # while src.institution may be a slug ("chase"). An exact SQL
                # match never fires across that case/format gap, so fetch by the
                # exact last_four and slugify-compare the institution in Python.
                # An empty slug (institution is all punctuation) is skipped — it
                # would spuriously match other empty-slug rows sharing last_four.
                rows = self._db.execute(
                    f"SELECT account_id, institution_name FROM {DIM_ACCOUNTS.full_name} "  # noqa: S608  # TableRef + parameterized values
                    "WHERE last_four = ? AND account_id != ?",
                    [src.last_four, exclude_account_id],
                ).fetchall()
                # confidence is informational only — weak signals always go to review.
                out.extend(
                    _Candidate(
                        account_id=str(r[0]),
                        signal="institution_last4",
                        value=f"{target_inst}:{src.last_four}",
                        confidence=0.5,
                    )
                    for r in rows
                    if r[1] and slugify(str(r[1])) == target_inst
                )
            if out:
                return out
            existing = [
                {"account_id": str(r[0]), "account_name": str(r[1] or "")}
                for r in self._db.execute(
                    f"SELECT account_id, display_name FROM {DIM_ACCOUNTS.full_name} "  # noqa: S608  # TableRef + parameterized values
                    "WHERE account_id != ?",
                    [exclude_account_id],
                ).fetchall()
            ]
            result = match_account(src.account_name, existing_accounts=existing)
            if result.matched and result.account_id:
                # Exact slug match: still a weak signal — proposed for review,
                # never auto-merged (match_account returns it via account_id, not
                # via .candidates, so it must be picked up explicitly).
                out.append(
                    _Candidate(
                        account_id=result.account_id,
                        signal="name",
                        value=src.account_name,
                        confidence=0.4,
                    )
                )
            else:
                out.extend(
                    _Candidate(
                        account_id=c["account_id"],
                        signal="name",
                        value=src.account_name,
                        confidence=0.4,
                    )
                    for c in result.candidates
                    if c["account_id"]
                )
            if not out and fallback:
                out = self._fallback_candidates(src, exclude_account_id)
            return out
        except duckdb.CatalogException:
            logger.debug("core.dim_accounts unavailable; no candidates")
            return []

    def _fallback_candidates(
        self, src: SourceAccount, exclude_account_id: str
    ) -> list[_Candidate]:
        """Existing accounts as a last-resort review pick-list (gate only).

        Reached when no last4/name signal cleared. Prefers an institution-scoped
        list (signal ``institution``) when the source resolved an institution
        that matches existing accounts; otherwise lists all accounts (signal
        ``fallback``). Capped at ``_FALLBACK_CANDIDATE_CAP``. Always low
        confidence and review-only — never auto-adopted ("magic stays visible").

        Institution-scoping must never *shrink* the list to empty: the
        CSV-resolved institution slug frequently doesn't match
        ``dim_accounts.institution_name`` (cross-source slug drift, or an
        account name polluting a saved format's institution). When the scoped
        pass matches nothing, fall through to all accounts — the entire point of
        the fallback is a non-empty pick-list, so a mismatched scope must not
        recreate ``candidates: []``.
        """
        rows = self._db.execute(
            f"SELECT account_id, institution_name FROM {DIM_ACCOUNTS.full_name} "  # noqa: S608  # TableRef + parameterized value
            "WHERE account_id != ? ORDER BY institution_name, account_id",
            [exclude_account_id],
        ).fetchall()
        target_inst = slugify(src.institution) if src.institution else None
        if target_inst:
            scoped = [
                _Candidate(
                    account_id=str(r[0]),
                    signal="institution",
                    value=target_inst,
                    confidence=0.2,
                )
                for r in rows
                if r[1] and slugify(str(r[1])) == target_inst
            ]
            if scoped:
                return scoped[:_FALLBACK_CANDIDATE_CAP]
        return [
            _Candidate(
                account_id=str(r[0]), signal="fallback", value="", confidence=0.1
            )
            for r in rows
        ][:_FALLBACK_CANDIDATE_CAP]
