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


@dataclass(frozen=True)
class _Candidate:
    """A weak-signal candidate for a pending merge proposal.

    ``confidence`` is informational metadata only — weak signals always go to
    review regardless of score, so a fixed value per signal type is correct.
    """

    account_id: str
    signal: str  # "institution_last4" | "name"
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
        """
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

    def propose(self, src: SourceAccount) -> AccountProposal:
        """Compute the resolver verdict without writing anything (read-only preview).

        Mirrors the resolve() ladder exactly — explicit binding, strong ref,
        candidate pass — but performs no writes: no mint is persisted, no
        account_links row is inserted, no account_link_decisions row is created.
        Safe to call at any point in the import flow, including before confirm.

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
        preview_id = uuid.uuid4().hex[:12]
        raw_candidates = self._find_candidates(src, exclude_account_id=preview_id)
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
        """Return display_name from core.dim_accounts for a candidate account_id.

        Returns empty string when the row is absent (defensive; if _find_candidates
        returned a candidate, dim_accounts is materialized and the row should exist).
        """
        row = self._db.execute(
            f"SELECT display_name FROM {DIM_ACCOUNTS.full_name} "  # noqa: S608  # TableRef + parameterized value
            "WHERE account_id = ? LIMIT 1",
            [account_id],
        ).fetchone()
        return str(row[0]) if row and row[0] is not None else ""

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
        )

    def _lookup_strong_ref(self, src: SourceAccount) -> tuple[str, str] | None:
        """Return (account_id, ref_kind) if any accepted strong ref matches, else None.

        Checks source_native first (same-source re-import), then persistent_token
        (cross-connection identity), then scoped full_number (cross-source format).
        The ref_kind is surfaced so propose() can populate adopted_via accurately.
        """
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
            )

    def _find_candidates(
        self, src: SourceAccount, *, exclude_account_id: str
    ) -> list[_Candidate]:
        """Weak-signal candidates from core.dim_accounts (institution+last4, then name).

        Each is a review proposal, never an auto-merge. Returns no candidates if
        core.dim_accounts is not yet materialized (first import before any transform).
        """
        try:
            out: list[_Candidate] = []
            if src.last_four and src.institution:
                rows = self._db.execute(
                    f"SELECT account_id FROM {DIM_ACCOUNTS.full_name} "  # noqa: S608  # TableRef + parameterized values
                    "WHERE last_four = ? AND institution_name = ? AND account_id != ?",
                    [src.last_four, src.institution, exclude_account_id],
                ).fetchall()
                # confidence is informational only — weak signals always go to review.
                out.extend(
                    _Candidate(
                        account_id=str(r[0]),
                        signal="institution_last4",
                        value=f"{src.institution}:{src.last_four}",
                        confidence=0.5,
                    )
                    for r in rows
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
            return out
        except duckdb.CatalogException:
            logger.debug("core.dim_accounts unavailable; no candidates")
            return []
