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

from moneybin.database import Database
from moneybin.repositories.account_link_decisions_repo import AccountLinkDecisionsRepo
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.services.account_resolution_types import ResolvedAccount, SourceAccount
from moneybin.tables import ACCOUNT_LINKS

logger = logging.getLogger(__name__)


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
            return ResolvedAccount(
                account_id=src.explicit_account_id,
                is_new=False,
                outcome="adopted_strong",
            )
        raise NotImplementedError("ladder steps 1-2 land in A3/A4")

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
