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
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

import duckdb

from moneybin.database import Database
from moneybin.extractors.institution_resolution import slug_for_institution_name
from moneybin.extractors.tabular.account_matching import AccountMatch, match_account
from moneybin.metrics.observations import MetricObservations, record_observation
from moneybin.metrics.registry import (
    ACCOUNT_LINK_CONFIDENCE,
    ACCOUNT_LINK_REVIEW_PENDING,
)
from moneybin.repositories.account_link_decisions_repo import AccountLinkDecisionsRepo
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.services.account_resolution_types import (
    UNNAMED_ACCOUNT_LABEL,
    AccountCandidate,
    AccountProposal,
    ResolvedAccount,
    SourceAccount,
    is_a_name,
    normalize_account_identifier,
)
from moneybin.services.ledger_overlap import fetch_ledger_spans, probe_ledger_overlap
from moneybin.services.pdf_account_identity import legacy_pdf_identifier_key
from moneybin.tables import (
    ACCOUNT_LINK_DECISIONS,
    ACCOUNT_LINKS,
    DIM_ACCOUNTS,
    TABULAR_ACCOUNTS,
    TABULAR_TRANSACTIONS,
)
from moneybin.utils import slugify

logger = logging.getLogger(__name__)

#: Any Unicode letter, the test that keeps the label rung for names rather than
#: for masked account numbers. Named rather than inlined because the query below
#: is an f-string, where the literal would have to be spelled ``\\p{{L}}`` —
#: two escapes deep, in a pattern that must stay identical to the one in
#: dim_accounts.sql and to ``account_display_name._has_letter``.
_SQL_HAS_LETTER = r"\p{L}"

#: A four-digit run in any script, the test that stops a label already stating
#: an identifier from taking a second one. ``\p{Nd}`` rather than ``[0-9]``
#: because the importer's masker keeps whatever digits it matched: a non-Latin
#: account number reaches this column masked to four non-ASCII digits, which
#: ``[0-9]{4}`` read as none at all, and the raw last four was appended on top
#: of the mask. Named for the same reason ``_SQL_HAS_LETTER`` is -- the
#: f-string below would spell it two escapes deep -- and it must stay identical
#: to dim_accounts.sql, which cannot spell it ``\d{4}`` either: DuckDB's RE2
#: reads that as ASCII, so only the mirror in ``account_display_name`` may.
_SQL_HAS_FOUR_DIGIT_RUN = r"\p{Nd}{4}"

#: The unnamed sentinel as a SQL literal, interpolated for the same reason
#: ``_SQL_HAS_LETTER`` is: the label arms below must refuse exactly what
#: dim_accounts.sql and ``account_display_name.usable_source_label`` refuse,
#: and binding it to the constant is what keeps a rename from splitting them.
_SQL_UNNAMED_LABEL = f"'{UNNAMED_ACCOUNT_LABEL}'"


def refresh_account_link_pending_gauge(db: Database) -> None:
    """Set ACCOUNT_LINK_REVIEW_PENDING from the live review-queue depth.

    Called at the two sites that change the count: the resolver's candidate
    pass (adds proposals) and ``AccountLinksService.set`` (accept/reject clears
    them). Keeps the gauge honest in both directions rather than only counting
    up. Counts DISTINCT provisional accounts — the review *unit* is the
    provisional, not the raw decision row — so the gauge matches
    ``AccountLinksService.count_pending`` and the queue users actually see.

    Best-effort by construction. Two of its callers refresh this gauge in the
    post-commit tail of an accepted merge, immediately before the rematch that
    keeps that merge's newly co-resident duplicates from going unproposed
    (``AccountLinksService.set`` and, on the batched path,
    ``record_committed_outer_decisions``). Propagating a metrics failure there
    would skip the rematch while the accept stayed committed — and an accepted
    decision is refused on a retry, so the duplicates would wait for an
    unrelated refresh with nothing reporting it. A stale gauge is the far
    cheaper loss, so the gauge is what gives way.
    """
    try:
        row = db.execute(
            f"SELECT COUNT(DISTINCT provisional_account_id) "  # noqa: S608  # TableRef constant, no user input
            f"FROM {ACCOUNT_LINK_DECISIONS.full_name} "
            "WHERE status = 'pending' AND reversed_at IS NULL"
        ).fetchone()
        ACCOUNT_LINK_REVIEW_PENDING.set(int(row[0]) if row else 0)
    except Exception as exc:  # noqa: BLE001  # telemetry must not abort its caller's tail
        # Type, not message: this query names the profile database, so a DuckDB
        # connection or encryption error can carry that path into the durable
        # log, and SanitizedLogFormatter masks known PII patterns, not paths.
        logger.warning(
            f"Could not refresh the account-link pending gauge: {type(exc).__name__}"
        )


def fetch_display_name(db: Database, account_id: str) -> str:
    """Return one account's display name, or ``""`` when nothing names it.

    Convenience wrapper over :func:`fetch_display_names`, which holds the
    implementation. Kept as a distinct entry point because most callers hold one
    id and a dict result would only be unpacked again.
    """
    return fetch_display_names(db, [account_id]).get(account_id, "")


def fetch_core_display_names(
    db: Database, account_ids: Iterable[str]
) -> dict[str, str]:
    """Display names from ``core.dim_accounts`` only — the constructed ones.

    Split out from :func:`fetch_display_names` because only this half is safe to
    *persist*. ``dim_accounts.display_name`` is a constructed label — institution
    + subtype + masked last four — which is why the taxonomy classes it
    ``USER_NOTE``, the same class the frozen decision columns declare.

    The model used to contradict that declaration: its label ended in a terminal
    ``'Account ' || w.account_id`` branch, and that id is the source-native key
    for any account carrying no accepted link -- for OFX, the institution's own
    ``<ACCTID>``, classed ``INSTITUTION_ACCOUNT_NUMBER``. The model now ends in
    the literal ``'Unnamed account'`` instead, so the ``USER_NOTE`` declaration
    is true of every branch and the query below refuses a label the model can no
    longer produce. It is kept as a second line of defence: the model is one
    edit away from reintroducing an id-bearing label, and this reader is the
    only thing standing between that and a frozen decision column.

    The raw fallback derives its label from ``account_number`` /
    ``account_number_masked``, both classed ``INSTITUTION_ACCOUNT_NUMBER``
    (CRITICAL). The string it emits is already at that class's mask floor
    (PARTIAL leaves ``"****" + last four``), so *showing* it discloses nothing
    the masker would have hidden. Persisting it is a different question: the
    frozen column would then hold a CRITICAL-derived value under a MEDIUM
    declaration, and "this particular derived string happens to be safe" is
    exactly the per-value inference the declaration exists to replace.

    So: display it live if you must, never freeze it. A decision whose
    provisional was named only by raw freezes ``""`` rather than a class it
    cannot declare.
    """
    ids = sorted({account_id for account_id in account_ids if account_id})
    if not ids:
        return {}
    placeholders = ", ".join("?" * len(ids))
    try:
        rows = db.execute(
            # Second line of defence. `dim_accounts` no longer ends its
            # display-name chain in `'Account ' || w.account_id` -- the terminal
            # arm is now the literal 'Unnamed account', so this CASE matches
            # nothing the current model can produce. It is kept deliberately:
            # the model is one edit away from reintroducing an id-bearing label,
            # and for an account with no accepted link that id is the
            # source-native key -- for OFX, the institution's own <ACCTID>.
            # Equality against that exact expression is a structural test, not a
            # guess at what an account number looks like: such a label carries
            # nothing the id does not, so the masked last four answers instead
            # and an account without one drops out.
            "SELECT account_id, CASE "
            "WHEN display_name = 'Account ' || account_id "
            "THEN '…' || NULLIF(TRIM(last_four), '') "
            f"ELSE display_name END FROM {DIM_ACCOUNTS.full_name} "  # noqa: S608  # TableRef constant + parameterized values
            f"WHERE account_id IN ({placeholders})",
            ids,
        ).fetchall()
    except duckdb.CatalogException:
        return {}
    return {str(r[0]): str(r[1]) for r in rows if r[1] is not None}


def fetch_display_names(db: Database, account_ids: Iterable[str]) -> dict[str, str]:
    """Resolve display names for many accounts: ``core`` first, then unrefreshed raw.

    One implementation, deliberately. Two readers of "what is this account
    called" is how the review queue named an imported account that the decision
    log rendered as a bare id — the queue carried the raw fallback and the log
    queried ``core`` alone. Batched because the decision log asks about every
    account it has ever seen, which a per-id lookup turns into an unbounded N+1
    on a read an agent runs just to browse.

    The raw half never answers with ``account_name``. That is the file's own
    free-text label, classed ``ACCOUNT_IDENTIFIER`` because it can be a bare
    account number, and nothing downstream can tell a name from a number -- so
    returning it put a potentially unmasked account number into surfaces that
    declare far less. It answers with ``account_label`` instead: the same text
    after the importer stripped the trailing last four and masked any embedded
    number, which is the rung ``core.dim_accounts`` names by, so this answer and
    the one on the far side of a refresh agree. Failing that it *constructs*
    one, the same shape the model builds below its top rung: institution name,
    then the last four digits of ``account_number`` (or
    ``account_number_masked``) behind a ``****``. Either half may be missing; an
    account with neither stays absent rather than being named by its file, so an
    account whose only name lived in an unmasked label still reads as
    "Example Bank ****4521".

    Both queries guard ``CatalogException`` so callers work before the core
    layer is materialized (a profile has decisions before its first SQLMesh
    run). Ids that resolve to nothing are absent from the result rather than
    mapped to ``""``, so a caller can tell an unnamed account from one it never
    asked about.
    """
    # Materialize before the first read: the parameter is an Iterable, and this
    # body reads it twice. A one-shot generator would be drained by the core
    # lookup, leaving `missing` empty and skipping the raw fallback for every
    # id -- a partial answer with no error, which is the exact failure this
    # resolver exists to prevent.
    ids = sorted({account_id for account_id in account_ids if account_id})
    names = fetch_core_display_names(db, ids)
    missing = [account_id for account_id in ids if account_id not in names]
    if not missing:
        return names
    placeholders = ", ".join("?" * len(missing))
    try:
        # Reads raw.tabular_accounts by license (Layer Rule 2, account-identity
        # resolver exception): this is the fallback for accounts core cannot
        # name yet, so core has nothing to read.
        #
        # ROW_NUMBER rather than ARG_MAX: ARG_MAX skips a row whose
        # extracted_at is NULL and silently answers from an older import.
        # Newest *nameable* row wins -- the label can now be NULL (an import
        # carrying neither an institution nor four digits), and ordering on
        # recency alone would let such a row hide an older one that does name
        # the account, which is the bare-id rendering this resolver exists to
        # prevent.
        #
        # `resolved_label`, not `account_label`: DuckDB binds a window ORDER BY
        # to a base COLUMN in preference to a same-named SELECT-list alias, so
        # once raw.tabular_accounts gained a real account_label the first sort
        # key silently became that column instead of the expression below --
        # ranking every row equal and letting the nameless newest one win.
        # Verified against DuckDB, and pinned by
        # test_an_unnameable_raw_row_does_not_hide_an_older_nameable_one.
        raw_rows = db.execute(
            f"""
            SELECT account_id, resolved_label FROM (
                SELECT
                    link.account_id AS account_id,
                    COALESCE(
                      -- The name a person wrote, already display-safe: the
                      -- importer masks and strips it before writing. Top rung
                      -- in core.dim_accounts too, letter test and last-four
                      -- append included, so the pre-refresh answer and the
                      -- post-refresh one agree.
                      -- Two arms, exactly as dim_accounts has them: a label
                      -- with no four-digit run of its own carries the last
                      -- four, because a chosen name is not a unique one and
                      -- two accounts sharing a product name would otherwise
                      -- collide back onto one string. A label that already
                      -- holds four digits takes nothing more -- joining
                      -- "Checking ****5678" with "…9012" would publish eight
                      -- digits of a twelve-digit number.
                      -- Neither arm takes the unnamed sentinel. It holds
                      -- letters, so the letter test alone reads it as a name,
                      -- but it is this ladder's own terminal arm: a label equal
                      -- to it says the source could not name the account
                      -- either. A re-imported MoneyBin export carries it
                      -- literally, and accepting it would answer
                      -- "Unnamed account …1234" here while the refreshed
                      -- dimension falls through to its institution rung.
                      -- Falling through costs nothing even when nothing else
                      -- names the account: the row drops out, and every caller
                      -- substitutes the same sentinel for an absent id.
                      CASE
                        WHEN REGEXP_MATCHES(raw.account_label, '{_SQL_HAS_LETTER}')
                        AND NOT REGEXP_MATCHES(raw.account_label, '{_SQL_HAS_FOUR_DIGIT_RUN}')
                        AND raw.account_label <> {_SQL_UNNAMED_LABEL}
                        AND LENGTH(
                          REGEXP_REPLACE(
                            COALESCE(raw.account_number, raw.account_number_masked),
                            '[^0-9]', '', 'g'
                          )
                        ) >= 4
                        THEN raw.account_label || ' …' || RIGHT(
                          REGEXP_REPLACE(
                            COALESCE(raw.account_number, raw.account_number_masked),
                            '[^0-9]', '', 'g'
                          ),
                          4
                        )
                      END,
                      -- The label with no last four to add, and the label that
                      -- already states one. dim_accounts' bare arm covers the
                      -- same two cases by falling through its own NULL.
                      CASE
                        WHEN REGEXP_MATCHES(raw.account_label, '{_SQL_HAS_LETTER}')
                        AND raw.account_label <> {_SQL_UNNAMED_LABEL}
                        THEN raw.account_label
                      END,
                      -- Same derivation core.dim_accounts uses for
                      -- last_four_raw: strip to digits, and only speak if four
                      -- survive. That is what keeps an alphanumeric PDF
                      -- identifier ("ACCT-9Z", one digit) from ever reaching a
                      -- caller -- the column is named "masked" but the PDF path
                      -- stores a whole identifier in it.
                      NULLIF(TRIM(
                        COALESCE(raw.institution_name, '') ||
                        CASE
                          WHEN LENGTH(
                            REGEXP_REPLACE(
                              COALESCE(raw.account_number, raw.account_number_masked),
                              '[^0-9]', '', 'g'
                            )
                          ) >= 4
                          THEN ' ****' || RIGHT(
                            REGEXP_REPLACE(
                              COALESCE(raw.account_number, raw.account_number_masked),
                              '[^0-9]', '', 'g'
                            ),
                            4
                          )
                          ELSE ''
                        END
                      ), '')
                    ) AS resolved_label,
                    ROW_NUMBER() OVER (
                        PARTITION BY link.account_id
                        ORDER BY resolved_label IS NOT NULL DESC,
                                 raw.extracted_at DESC
                    ) AS rn
                FROM {TABULAR_ACCOUNTS.full_name} AS raw
                JOIN {ACCOUNT_LINKS.full_name} AS link
                  ON link.status = 'accepted' AND link.ref_kind = 'source_native'
                 AND link.source_type = raw.source_type
                 AND link.source_origin = raw.source_origin
                 AND link.ref_value = raw.account_id
                WHERE link.account_id IN ({placeholders})
            ) WHERE rn = 1
            """,  # noqa: S608  # TableRef constants + parameterized values
            missing,
        ).fetchall()
    except duckdb.CatalogException:
        return names
    names.update({str(r[0]): str(r[1]) for r in raw_rows if r[1] is not None})
    return names


def _institution_key(institution: str | None) -> str | None:
    """Comparison key for an institution, canonical wherever the registry knows it.

    Both sides of every institution comparison pass through here. Sources carry
    whatever spelling they have — a sheet's hand-written "U.S. Bank", a
    filename heuristic's "us_bank", the registry slug an OFX ``<FID>`` resolves
    to — and slugifying alone never makes those meet, because the registry's
    slug is curated rather than derived from the name ("u-s-bank" against
    "us-bank"). Resolving through the registry first collapses every known
    spelling of one institution onto a single key; an unregistered name falls
    back to a plain slug, which still absorbs case and separator differences.
    """
    if not institution:
        return None
    return slugify(slug_for_institution_name(institution) or institution) or None


def _last_fours_disagree(
    source_last_four: str | None, candidate_last_four: str | None
) -> bool:
    """Whether two accounts state last fours that positively contradict each other.

    Requires BOTH sides to state one. Silence is not disagreement: an account
    with no known last four is a different gap, and vetoing on it would drop a
    proposal that nothing else surfaces rather than retype it.
    """
    return bool(
        source_last_four
        and candidate_last_four
        and candidate_last_four != source_last_four
    )


# Cap on the fallback pick-list (existing accounts surfaced for the human to pick
# from when no real signal cleared). Bounds an otherwise-unbounded "list all
# accounts" so a large book doesn't dump everything; a personal-finance user
# rarely exceeds this. Neither this cap nor the institution scope applies when a
# null last_four forced the review open — see _fallback_candidates: there any
# omitted account is unpickable, so a long list is the lesser cost.
_FALLBACK_CANDIDATE_CAP = 25

# How long two ledgers may run at once before the reissue signal stops believing
# one account replaced the other. A changeover is not instantaneous — a refund,
# a final fee, or a charge that posts late lands on the retired number after the
# replacement has started — so a few weeks of tail overlap is still the reissue
# shape. Roughly one statement cycle, which is the period such stragglers arrive
# in. Beyond it the two accounts were simply open together, which is the case
# this bound exists to refuse: the live pairs that motivated it ran concurrently
# for 76 to 362 days.
_REISSUE_MAX_CONCURRENT_DAYS = 30


@dataclass(frozen=True)
class _Candidate:
    """A weak-signal candidate for a pending merge proposal.

    ``confidence`` is informational metadata only — weak signals always go to
    review regardless of score, so a fixed value per signal type is correct.
    """

    account_id: str
    # "legacy_pdf_identity" | "institution_last4" | "name" |
    # "institution_reissue" | "institution" | "fallback" — the last two are
    # the gate's last-resort pick-list.
    signal: str
    value: str
    confidence: float


def _dedupe_candidates(*groups: list[_Candidate]) -> list[_Candidate]:
    """Combine candidate rungs without showing the same account twice."""
    seen: set[str] = set()
    combined: list[_Candidate] = []
    for candidate in (item for group in groups for item in group):
        if candidate.account_id in seen:
            continue
        seen.add(candidate.account_id)
        combined.append(candidate)
    return combined


def _retyped_reissue_candidates(
    src: SourceAccount, name_rows: Sequence[Any]
) -> list[_Candidate]:
    """Name matches the last-four veto discarded, relabelled as what they are.

    The veto is right to refuse calling a match across a stated last-four
    disagreement a ``name`` match — that is evidence of a *different* account.
    But refusing the label is not the same as refusing the pair, and the two got
    conflated: ``propose_existing`` runs with ``reissue=False``, so on the
    backfill path a vetoed pair had nothing to fall through to and a duplicate
    the queue used to surface went silently invisible.

    Runs on every path and unconditionally, unlike ``_reissue_candidates``,
    because the two answer different questions. That one sweeps *every*
    same-institution account whose last four differs, which in an established
    book is the pairwise cross product — noise, which is why backfill keeps it
    off and why it stays a last-resort fallback. This one is bounded by what the
    name matcher actually matched, so it stays the reissue shape (same
    institution, same name, a last four that changed) and is cheap enough to
    always ask. It reads only rows the veto discarded, so it can never duplicate
    a candidate the name pass already returned.

    Same-institution only. "Checking" at two different banks with two different
    last fours is a common word, not a reissued card, and retyping that would
    reintroduce the evidence-free merge proposal the veto exists to prevent.

    Also requires ``display_name_is_user_set`` on the vetoed row and
    ``account_name_is_user_set`` on ``src``, for the same reason the name rung
    above does: a generated descriptor colliding with the source's name is not
    evidence a person named either account that way, on either side.
    """
    target_inst = _institution_key(src.institution) if src.institution else None
    if (
        not target_inst
        or not is_a_name(src.account_name)
        or not src.account_name_is_user_set
    ):
        return []
    vetoed = [
        {"account_id": str(row[0]), "account_name": str(row[1] or "")}
        for row in name_rows
        if _last_fours_disagree(src.last_four, row[2])
        and row[3]
        and _institution_key(str(row[3])) == target_inst
        and is_a_name(row[1])
        and row[4]
    ]
    if not vetoed:
        return []
    result = match_account(src.account_name, existing_accounts=vetoed)
    # match_account returns an exact slug hit via .account_id and fuzzy hits via
    # .candidates — the same split the name rung above reads, for the same reason.
    matched = (
        [result.account_id]
        if result.matched and result.account_id
        else [c["account_id"] for c in result.candidates if c["account_id"]]
    )
    return [
        _Candidate(
            account_id=str(account_id),
            signal="institution_reissue",
            value=target_inst,
            confidence=0.3,
        )
        for account_id in matched
    ][:_FALLBACK_CANDIDATE_CAP]


class AccountResolver:
    """Resolve a source account to a canonical account_id via the M1S ladder."""

    def __init__(
        self,
        db: Database,
        *,
        actor: str = "system",
        include_unmaterialized_candidates: bool = False,
        emit_metrics: bool = True,
        observations: MetricObservations | None = None,
    ) -> None:
        """Bind the resolver to a database + audit actor for its link writes."""
        self._db = db
        self._actor = actor
        self._include_unmaterialized_candidates = include_unmaterialized_candidates
        self._emit_metrics = emit_metrics
        self._observations = observations
        self._links = AccountLinksRepo(db)
        self._decisions = AccountLinkDecisionsRepo(db)

    def resolve(
        self, src: SourceAccount, *, in_outer_txn: bool = False
    ) -> ResolvedAccount:
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
        if in_outer_txn:
            return self._run_ladder(src)
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
            self._teach_unpinned_key(src, account_id=src.explicit_account_id)
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

        candidates = self._find_candidates(
            src,
            exclude_account_id=account_id,
            # Quarantine a null-last_four mint. Such an account cannot
            # participate in last4-based resolution at all, so its silence is
            # not evidence of a distinct account — it is an unanswerable
            # question, and letting it mint silently is a merge decision nobody
            # ever sees. Surfacing the pick-list routes it to the identity
            # review queue instead ("magic stays visible"). No-ops on an empty
            # book: nothing to propose against means a clean mint. A blank
            # last_four reaches this as None (SourceAccount canonicalizes it).
            fallback=src.last_four is None,
            reissue=True,
        )
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
            record_observation(
                ACCOUNT_LINK_CONFIDENCE,
                cand.confidence,
                labels={},
                emit_metrics=self._emit_metrics,
                observations=self._observations,
            )
            pending_ids.append(decision_id)
        if self._observations is not None:
            self._observations.callback(
                lambda: refresh_account_link_pending_gauge(self._db)
            )
        elif self._emit_metrics:
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
        on fallback there would gate every fresh multi-account import. Candidates
        that appear only because the caller opted in are preview-only — confirming
        "new" still mints.

        One case overrides the caller either way: a source with no last_four turns
        fallback on here and in resolve() alike (``fallback=src.last_four is
        None``), because an account that cannot answer the last4 rung is an
        unanswerable question rather than evidence of a distinct account. So
        resolve() does use fallback for that source, and the multi-account gate's
        False does not switch it off — which is what keeps this preview agreeing
        with the ladder it previews.

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
            src,
            exclude_account_id=preview_id,
            # A null last_four quarantines regardless of the caller's opt-in, so
            # this preview agrees with resolve()'s ladder. Without it the gate
            # would load rows and surface the question afterwards. Blank reaches
            # this as None (SourceAccount canonicalizes it).
            fallback=fallback or src.last_four is None,
            reissue=True,
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
            # Asking for a fallback pick-list IS the caller declaring this source
            # carried no identity signal. Carry that through: on a first import
            # the pick-list comes back empty, and without this the proposal would
            # be indistinguishable from a confident mint and pass unasked.
            #
            # Deliberately the caller's `fallback`, NOT the widened
            # `fallback or src.last_four is None` used for the search above. The
            # two answer different questions: the search widens because a null
            # last_four cannot support a strong match, while this field says
            # whether the source named an account at all. Widening it here would
            # make `requires_confirm` true for every named account that has no
            # last_four and matches nothing — a first-contact "Checking" column
            # would start gating instead of minting, which is the behavior
            # `test_a_named_first_contact_mint_loads_without_asking` pins.
            identity_unknown=fallback,
        )

    def propose_existing(self, account_id: str) -> AccountProposal | None:
        """Backfill verdict for an account already in core.dim_accounts.

        Looks up the account's institution_slug, last_four, and display_name,
        builds a synthetic SourceAccount (source_type/source_origin="backfill";
        the candidate pass only uses last_four, institution, account_name), then
        delegates to _find_candidates excluding the account itself.

        Returns None when the account is absent from dim_accounts, when
        core.dim_accounts is not yet materialized, or when no candidates are
        found. Read-only — writes nothing.
        """
        try:
            row = self._db.execute(
                f"SELECT institution_slug, last_four, display_name, "  # noqa: S608  # TableRef + parameterized value
                f"display_name_is_user_set FROM {DIM_ACCOUNTS.full_name} "
                "WHERE account_id = ? LIMIT 1",
                [account_id],
            ).fetchone()
        except duckdb.CatalogException:
            logger.debug("core.dim_accounts unavailable in propose_existing")
            return None
        except duckdb.BinderException:
            # dim_accounts exists but predates display_name_is_user_set (a
            # profile that hasn't run `moneybin transform apply` since that
            # migration landed). Degrade like _find_candidates does rather
            # than disable the whole backfill sweep: keep the last-four/
            # institution signals, which don't depend on the new column, and
            # treat this account's own name as unstated. Returning None here
            # unconditionally meant every account produced zero proposals for
            # the whole transitional window, including a genuine last-four
            # duplicate -- `moneybin accounts links run` would silently stop
            # surfacing any backfill candidates until the migration reapplied.
            logger.debug(
                "core.dim_accounts missing display_name_is_user_set; "
                "propose_existing degrading to last-four/institution signals only"
            )
            fallback_row = self._db.execute(
                f"SELECT institution_slug, last_four "  # noqa: S608  # TableRef + parameterized value
                f"FROM {DIM_ACCOUNTS.full_name} WHERE account_id = ? LIMIT 1",
                [account_id],
            ).fetchone()
            if fallback_row is None:
                return None
            institution_slug, last_four = fallback_row
            display_name, display_name_is_user_set = None, False
        else:
            if row is None:
                return None
            # institution_slug, not institution_name: _find_candidates
            # compares against the slug column, so feeding a display name
            # back in here would re-create the very mismatch this reads
            # around.
            institution_slug, last_four, display_name, display_name_is_user_set = row
        src = SourceAccount(
            source_type="backfill",
            source_origin="backfill",
            source_account_key="",
            account_name=str(display_name or ""),
            # A generated display_name (display_name_is_user_set False) is not
            # evidence this account was ever named. account_name_is_user_set
            # is what _find_candidates and _retyped_reissue_candidates now
            # gate the name/reissue rungs on for every source, so mirroring
            # the candidate row's own flag here keeps this synthetic source
            # from smuggling generated-name evidence in from the account
            # side of a backfill pair.
            account_name_is_user_set=display_name_is_user_set,
            last_four=str(last_four) if last_four is not None else None,
            institution=str(institution_slug) if institution_slug is not None else None,
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

    def accepted_native_account_id(self, src: SourceAccount) -> str | None:
        """The canonical account this source's native key is already accepted onto.

        Read-only, and shared with the import gate so both layers ask the same
        question of the same row: the gate refuses a contradicting binding
        before anything loads, and ``_write_native_mapping`` below stays the
        backstop for every path that does not run the gate.
        """
        return self.accepted_native_owner(
            source_type=src.source_type,
            source_origin=src.source_origin,
            key=src.source_account_key,
        )

    def accepted_native_owner(
        self, *, source_type: str, source_origin: str, key: str
    ) -> str | None:
        """The same question as :meth:`accepted_native_account_id`, by coordinates.

        Exists because the pinned channels have to ask it about a key they have
        not built a ``SourceAccount`` around yet — that is the whole point, since
        the answer decides which key the ``SourceAccount`` gets.
        """
        row = self._db.execute(
            f"SELECT account_id FROM {ACCOUNT_LINKS.full_name} "  # noqa: S608  # TableRef + parameterized values
            "WHERE status = 'accepted' AND ref_kind = 'source_native' "
            "AND source_type = ? AND source_origin = ? AND ref_value = ? LIMIT 1",
            [source_type, source_origin, key],
        ).fetchone()
        return row[0] if row is not None else None

    def _teach_unpinned_key(self, src: SourceAccount, *, account_id: str) -> None:
        """Also link the key this source derives without the pin, if it has one.

        A pin makes the row carry the key its account already answers to, so the
        mapping written above says nothing about THIS file. Without this, the
        next import of the same source WITHOUT the pin derives its own key,
        finds no link, and stops to ask — or mints a second account for a
        statement already filed.

        Skips — never raises, never re-points — when the derived key is already
        accepted onto a different account. Re-pointing is an explicit, surfaced
        operation (M1S.5); doing it as a side effect of an unrelated pin is
        exactly the invisible action "magic stays visible" forbids. The pin the
        caller asked for still applies; only the extra teaching link is dropped.

        Taught keys never destabilise the reuse pick:
        :meth:`accepted_native_keys_for_account` orders by decision time, so one
        added now sorts behind whatever the account already held.
        """
        key = src.unpinned_account_key
        if not key or key == src.source_account_key:
            return
        existing = self.accepted_native_owner(
            source_type=src.source_type, source_origin=src.source_origin, key=key
        )
        if existing is not None:
            if existing != account_id:
                # Imported here, not at module scope: import_service imports this
                # module, so a top-level import closes the cycle.
                from moneybin.services.import_service import (  # noqa: PLC0415
                    mask_embedded_account_number,
                )

                # Masked for the reason the contradicted-binding refusal is: an
                # account id is not always a minted surrogate, and this one
                # reaches a log file, which outlives the session.
                logger.warning(
                    f"account_links: not teaching source_native key for "
                    f"{src.source_type}/{src.source_origin} — already accepted "
                    f"onto account {mask_embedded_account_number(existing)}, pin "
                    f"targeted {mask_embedded_account_number(account_id)}. "
                    "Re-point explicitly if that is intended."
                )
            return
        self._links.insert(
            link_id=uuid.uuid4().hex[:12],
            account_id=account_id,
            ref_kind="source_native",
            ref_value=key,
            source_type=src.source_type,
            source_origin=src.source_origin,
            decided_by="user",
            actor=self._actor,
            in_outer_txn=True,  # joins resolve()'s per-account transaction
        )

    def accepted_native_keys_for_account(
        self, *, account_id: str, source_type: str, source_origin: str
    ) -> list[str]:
        """Native keys this canonical account is already accepted under, for one source.

        The reverse of :meth:`accepted_native_account_id`, and scoped the same
        way: one account legitimately holds several native keys (two exports of
        the same account, each with its own key), so the answer is a list and
        the caller decides what a non-singleton means.

        Ordered oldest decision first, and that order is load-bearing: a pinned
        import reuses the first entry, so two imports of one statement have to
        land on the same key or staging cannot dedup them. Sort order alone is
        not enough — it is only stable while the key set is, and a merge or a
        second document pinned to this account inserts a key that can sort ahead
        of the one the existing rows already use. Decision time makes every
        later arrival lose, including a merged-in ref: ``AccountLinksRepo.repoint``
        reverses the old row and inserts a *new* accepted one, stamped now.
        ``ref_value`` only breaks ties, so the answer is still total.
        """
        rows = self._db.execute(
            f"SELECT ref_value FROM {ACCOUNT_LINKS.full_name} "  # noqa: S608  # TableRef + parameterized values
            "WHERE status = 'accepted' AND ref_kind = 'source_native' "
            "AND account_id = ? AND source_type = ? AND source_origin = ? "
            "ORDER BY decided_at, ref_value",
            [account_id, source_type, source_origin],
        ).fetchall()
        return [str(r[0]) for r in rows]

    def account_ids_ever_self_mapped(
        self, candidates: Sequence[str], *, source_type: str, source_origin: str
    ) -> set[str]:
        """Which of these strings a link on THIS source carried as both account and ref.

        That pairing is what a pre-fix ``--account-id`` pin left behind, and it
        is the whole signature. "This string appears somewhere in the
        ``account_id`` column" is not: a legitimate native key is only ever a
        ``ref_value``, so the wider test would call it residue as soon as any
        unrelated account happened to be minted under that same string, and the
        pin would fall back to a content key that rotates when the export grows.

        Scoped to one source for the same reason, and it costs nothing: both
        shapes that leave a self-map put it on the coordinates of the key it sits
        beside. A pre-fix pin wrote it while importing that source, and
        ``AccountLinksRepo.repoint`` re-accepts the ref "with the same ref
        coordinates". A self-map under a *different* source is therefore some
        other account's history, and reading it here would discard a real key.

        Deliberately ignores ``status``. A reversed row is still evidence the
        value was once a canonical account id: ``repoint`` — the merge primitive
        — reverses the losing row in place and re-accepts its ref under the
        winner, so after a merge the only trace that the old id was an id at all
        is the loser's own self-map, no longer accepted.
        """
        if not candidates:
            return set()
        placeholders = ",".join(["?"] * len(candidates))
        rows = self._db.execute(
            f"SELECT DISTINCT account_id FROM {ACCOUNT_LINKS.full_name} "  # noqa: S608  # placeholders are code-owned; values parameterized
            "WHERE account_id = ref_value "
            "AND source_type = ? AND source_origin = ? "
            f"AND account_id IN ({placeholders})",
            [source_type, source_origin, *candidates],
        ).fetchall()
        return {str(r[0]) for r in rows}

    def knows_account_id(self, account_id: str) -> bool:
        """Whether this database already has an account under this canonical id.

        Two sources, because neither alone is complete. ``core.dim_accounts``
        is SQLMesh-materialized, so an account minted by an earlier import is
        absent from it until a refresh runs — and imports default to not
        refreshing. ``app.account_links`` carries every id the resolver has
        bound from the instant it mints one, but only those: an account that
        predates the links table, or that arrives by another path, is in
        dim_accounts alone. Reading both is what lets a caller answer "new" for
        one file and bind its sibling to the id that answer just produced.

        The catalog guard mirrors :func:`fetch_display_name` — ``core`` does
        not exist before its first materialization, and its absence means no
        account is known *there*, not that the question cannot be answered.

        Only ``accepted`` links count, matching
        :meth:`accepted_native_account_id`. ``AccountLinksRepo.repoint`` — the
        merge primitive — reverses the old row *in place*, leaving its
        ``account_id`` intact, so an unpredicated match would keep answering
        "yes" for an account the user already merged away. The caller
        (``_refuse_unknown_binding_targets``) would then let that stale id
        through as a binding target and step 0 would write a fresh accepted
        link onto it, resurrecting the merged-away account as a second,
        disconnected transaction stream.

        **Links outrank the dim for any id they know.** Filtering the links arm
        alone left the same resurrection open through the other one: the merge
        path (``AccountLinksService.set``) repoints and refreshes nothing but a
        metrics gauge, so ``dim_accounts`` still carries the merged-away row
        until the next transform — and a "yes" from that stale materialization
        is exactly the answer the filter was added to prevent. Rows present but
        none accepted therefore means *merged away*, full stop; the dim does not
        get to overrule it. The dim arm still answers for an id links have never
        seen, which is the case it exists for — an account created by sync or
        backfill rather than by an import.
        """
        row = self._db.execute(
            f"SELECT COUNT(*) AS total, "  # noqa: S608  # TableRef + parameterized value
            "COUNT(*) FILTER (WHERE status = 'accepted') AS accepted "
            f"FROM {ACCOUNT_LINKS.full_name} WHERE account_id = ?",
            [account_id],
        ).fetchone()
        if row is not None and row[0]:
            return bool(row[1])
        try:
            row = self._db.execute(
                f"SELECT 1 FROM {DIM_ACCOUNTS.full_name} "  # noqa: S608  # TableRef + parameterized value
                "WHERE account_id = ? LIMIT 1",
                [account_id],
            ).fetchone()
        except duckdb.CatalogException:
            return False
        return row is not None

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
        existing = self.accepted_native_account_id(src)
        if existing is not None:
            if existing != account_id:
                raise ValueError(
                    "account_links: source_native already accepted for a different "
                    f"account_id; existing={existing!r}, requested={account_id!r}"
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
            scope, separator, identifier = scoped.partition(":")
            if separator and len(scope) == 9 and scope.isdigit():
                rows = self._db.execute(
                    f"SELECT account_id, ref_value FROM {ACCOUNT_LINKS.full_name} "  # noqa: S608  # TableRef + parameterized value
                    "WHERE status = 'accepted' AND ref_kind = 'full_number' "
                    "AND STARTS_WITH(ref_value, ?)",
                    [f"{scope}:"],
                ).fetchall()
                normalized = normalize_account_identifier(identifier)
                legacy_accounts = {
                    str(account_id)
                    for account_id, ref_value in rows
                    if normalize_account_identifier(str(ref_value).partition(":")[2])
                    == normalized
                }
                if len(legacy_accounts) == 1:
                    return legacy_accounts.pop(), "full_number"
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
        self,
        src: SourceAccount,
        *,
        exclude_account_id: str,
        fallback: bool = False,
        reissue: bool = False,
    ) -> list[_Candidate]:
        """Weak-signal candidates from materialized and current-batch accounts.

        Each is a review proposal, never an auto-merge. Batch callers also include
        PDF accounts loaded earlier in the batch but not materialized in core yet.

        The name rung requires ``display_name_is_user_set`` on the candidate row
        AND ``account_name_is_user_set`` on ``src`` — a display_name (or source
        account_name) assembled from generated fallback rungs (institution +
        subtype, a bare filename, etc.) is not name evidence, only a
        coincidence of attributes already compared separately. It also skips
        any account whose last four
        positively contradicts the source's (``_last_fours_disagree``). A name
        match across a stated disagreement is not weaker evidence than the
        last-four signal — it is evidence of a *different* account, and
        letting it score merely lower put a checking account and a savings
        account in one merge proposal. When the two also share an
        institution, ``_retyped_reissue_candidates`` re-surfaces
        that exact pair under the signal that is actually true — on every path,
        because the ``reissue`` sweep below is off for backfill and a vetoed pair
        would otherwise have nothing to fall through to. It runs *beside* the
        name pass rather than only when that pass came up empty: the two read
        disjoint halves of the same rows, so gating it let an unrelated namesake
        carrying no last four hide a genuine reissue.

        ``reissue`` (arriving source accounts only): when neither signal clears,
        surface same-institution accounts whose last-four differs — see
        ``_reissue_candidates``. On for ``resolve()`` and its ``propose()``
        preview, which must agree; off for ``propose_existing()``, where every
        account in an established book is already known-distinct and pairwise
        proposals would be noise, not signal.

        ``fallback`` (interactive import gate only — never the backfill link
        queue): when no last4/name signal clears, surface existing accounts as a
        low-confidence pick-list so the human picks from a list instead of an
        empty set. Off by default so ``accounts_links_run`` isn't flooded with an
        all-accounts proposal for every provisional account.
        """
        legacy_candidates = self._legacy_source_candidates(src, exclude_account_id)
        pending_candidates = (
            self._pending_pdf_candidates(src, exclude_account_id)
            if self._include_unmaterialized_candidates
            else []
        )
        try:
            out: list[_Candidate] = list(pending_candidates)
            last_four_candidates = self._last_four_candidates(src, exclude_account_id)
            out.extend(last_four_candidates)
            # Only a *corroborated* last four stands alone. Both sides naming
            # the same bank is near-certain, so a weaker name guess beside it
            # would just give the reviewer a second, worse answer. A bare
            # `last_four` hit is a four-digit coincidence until something
            # corroborates it — and since the rung stopped requiring an
            # institution, returning on one suppresses the name pass for exactly
            # the institution-less sources the widening was meant to help. The
            # twin it hides is the one whose own last four is simply unknown.
            if pending_candidates or any(
                c.signal == "institution_last4" for c in last_four_candidates
            ):
                return _dedupe_candidates(out, legacy_candidates)
            try:
                name_rows = self._db.execute(
                    f"SELECT account_id, display_name, last_four, institution_slug, "  # noqa: S608  # TableRef + parameterized values
                    f"display_name_is_user_set FROM {DIM_ACCOUNTS.full_name} "
                    "WHERE account_id != ? ORDER BY account_id",
                    [exclude_account_id],
                ).fetchall()
            except duckdb.BinderException:
                # dim_accounts exists but predates display_name_is_user_set on
                # a profile that hasn't run `moneybin transform apply` since
                # this migration landed. Scoped to just this query: `out`
                # already holds last_four_candidates gathered above, and it
                # must survive into the reissue/fallback/dedupe logic below
                # rather than being discarded along with the name/reissue
                # signals this query alone feeds.
                logger.debug(
                    "core.dim_accounts missing display_name_is_user_set; "
                    "skipping name/reissue signals"
                )
                name_rows = []
            existing = [
                {"account_id": str(r[0]), "account_name": str(r[1] or "")}
                for r in name_rows
                if not _last_fours_disagree(src.last_four, r[2])
                and is_a_name(r[1])
                and r[4]
            ]
            result = (
                match_account(src.account_name, existing_accounts=existing)
                if is_a_name(src.account_name) and src.account_name_is_user_set
                else AccountMatch(matched=False)
            )
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
            # Beside the name pass, not after it. The two read disjoint halves of
            # name_rows — the veto keeps agreeing/absent last fours, this keeps
            # the disagreeing ones — so a coincidental namesake with no last four
            # would otherwise populate `out` and suppress the reissue it has
            # nothing to do with. Both are weak signals headed for the same
            # review queue; picking between them is the human's call.
            out.extend(_retyped_reissue_candidates(src, name_rows))
            out = self._drop_concurrent_reissues(out, account_id=exclude_account_id)
            # A bare `last_four` hit is not a signal that cleared: it matches
            # on four digits alone, while this sweep matches same-institution
            # accounts whose last four *differs*. The two are disjoint by
            # construction, so letting the coincidence cancel the sweep loses
            # the genuine replacement card — the same mistake the name pass
            # made one rung up, once the rung stopped requiring an institution.
            cleared = [c for c in out if c.signal != "last_four"]
            if not cleared and reissue:
                # Extend rather than replace: `out` may already hold the bare
                # last-four hits this guard deliberately looks past, and the
                # sweep is meant to add to them, not stand in for them. When
                # `out` is empty — the only case that reached here before — the
                # two are the same thing.
                out.extend(
                    self._drop_concurrent_reissues(
                        self._reissue_candidates(src, exclude_account_id),
                        account_id=exclude_account_id,
                    )
                )
            if not out and fallback:
                out = self._fallback_candidates(src, exclude_account_id)
                return _dedupe_candidates(legacy_candidates, out)
            return _dedupe_candidates(out, legacy_candidates)
        except duckdb.CatalogException:
            # core.dim_accounts doesn't exist yet. The other query in this
            # method that could raise duckdb.BinderException (a stale
            # migration missing display_name_is_user_set) is caught locally
            # above, scoped to just that query, so it never reaches here and
            # never discards candidates already gathered in `out`.
            logger.debug("core.dim_accounts unavailable; using raw candidates only")
            return _dedupe_candidates(pending_candidates, legacy_candidates)

    def _last_four_candidates(
        self, src: SourceAccount, exclude_account_id: str
    ) -> list[_Candidate]:
        """Existing accounts stating the same last four — the strongest weak signal.

        Institution is **evidence here, never a precondition.** Keying the rung
        on a shared institution made the whole rung unreachable for an account
        that has none, and one of the two channels routinely mints exactly that:
        a tabular export names its account only in a label ("Everyday Spending
        (...7777)"), which yields a last four and no institution. The account it
        minted was invisible to every last-four comparison, so the cross-source
        twin the review queue exists to surface never appeared and both copies
        of the same ledger counted toward spending and net worth.

        What institution still does is *contradict*. Two banks issuing the same
        four digits is a collision, not a duplicate, so a pair that states two
        different institutions is dropped. Silence is not contradiction: when
        either side has no resolved institution the pair surfaces under
        ``last_four`` instead of ``institution_last4``, because the queue must
        be able to tell "both sides named this bank" from "one side named
        nothing" — that is the difference between a near-certain twin and a
        four-digit coincidence, and both are review-only either way.

        Match on ``institution_slug``, never ``institution_name``: the name is
        for display and the dim's per-field merge lets a later source win it
        outright. Both sides go through ``_institution_key`` so the spellings a
        source happens to carry meet the registry's curated slug, and the
        comparison happens in Python because the two still differ in case. An
        institution that normalizes to nothing (all punctuation) reads as
        unstated rather than as a value that would match every other such row.
        """
        if not src.last_four:
            return []
        target_inst = _institution_key(src.institution) if src.institution else None
        rows = self._db.execute(
            f"SELECT account_id, institution_slug FROM {DIM_ACCOUNTS.full_name} "  # noqa: S608  # TableRef + parameterized values
            "WHERE last_four = ? AND account_id != ? ORDER BY account_id",
            [src.last_four, exclude_account_id],
        ).fetchall()
        candidates: list[_Candidate] = []
        for row in rows:
            candidate_inst = _institution_key(str(row[1])) if row[1] else None
            if target_inst and candidate_inst and target_inst != candidate_inst:
                continue
            shared_institution = target_inst is not None and candidate_inst is not None
            # confidence is informational only — weak signals always go to review.
            candidates.append(
                _Candidate(
                    account_id=str(row[0]),
                    signal="institution_last4" if shared_institution else "last_four",
                    value=(
                        f"{target_inst}:{src.last_four}"
                        if shared_institution
                        else src.last_four
                    ),
                    confidence=0.5 if shared_institution else 0.45,
                )
            )
        # Corroboration decides who survives the cap, not `ORDER BY
        # account_id`: an `institution_last4` pair is the one near-certain match
        # in this list, and a book full of filler collisions would otherwise
        # crowd it out on id order alone. The sort is stable, so ties keep that
        # id order.
        candidates.sort(key=lambda c: c.signal != "institution_last4")
        return candidates[:_FALLBACK_CANDIDATE_CAP]

    def _drop_concurrent_reissues(
        self, candidates: list[_Candidate], *, account_id: str
    ) -> list[_Candidate]:
        """Refuse a reissue proposal whose two ledgers demonstrably ran side by side.

        A reissue is sequential by definition — the old number stops, the
        replacement starts — and the signal never checked that. It fired on a
        shared institution plus a last four that differed, which in an
        established book is every pair of cards at one bank: three accounts at
        one institution draw a proposal for each pair, and two cards at another
        that were deliberately kept apart are proposed against each other while
        both are still open. Each proposal's own evidence contradicted it
        — zero matched transactions over a period both ledgers covered — and a
        queue that fills with self-refuting proposals teaches the user to
        dismiss the queue, which is the whole trust budget "magic stays visible"
        is protecting.

        Only *positive* concurrency drops a candidate. An account with no
        published ledger keeps its proposal: at import time the provisional was
        minted seconds ago and no transform has run, so silence there is the
        normal state rather than an answer. That is what keeps the PDF
        replacement-card case — the shape the signal was written for — working.

        Overlapping spans alone are not that positive evidence, and reading
        them as such inverts the signal on the case that matters most. A true
        cross-source twin — one account arriving from two sources — covers the
        same period by construction *and* holds the same rows, so a drop keyed
        on the date ranges would withhold exactly the duplicate this queue
        exists to surface and leave it double-counting. Both halves of the
        original evidence have to hold: the ledgers ran together **and**
        disagreed across a period both of them covered.
        """
        reissue_ids = [
            c.account_id for c in candidates if c.signal == "institution_reissue"
        ]
        if not reissue_ids:
            return candidates
        spans = fetch_ledger_spans(self._db, [account_id, *reissue_ids])
        own = spans.get(account_id)
        if own is None:
            return candidates
        kept: list[_Candidate] = []
        for candidate in candidates:
            other = spans.get(candidate.account_id)
            if (
                candidate.signal == "institution_reissue"
                and other is not None
                and own.concurrent_with(
                    other, tolerance_days=_REISSUE_MAX_CONCURRENT_DAYS
                )
                and self._ledgers_contradict(account_id, candidate.account_id)
            ):
                continue
            kept.append(candidate)
        return kept

    def _ledgers_contradict(self, account_id: str, other_account_id: str) -> bool:
        """Whether a period both ledgers cover holds none of the same transactions.

        ``measurable`` is the half that keeps this honest: ``comparable == 0``
        means the probe found no shared period to compare, which is absence of
        evidence, not evidence of absence. Only a period both ledgers populated
        and still agreed on nothing refutes a reissue.

        ``unstated_currency_matches`` is the other half. The probe's default
        reading treats a one-sided silence as a mismatch, which is affordable
        where the count is *shown* beside a proposal and only costly where it
        is read as a refutation — here. A tabular export leaving the column
        blank beside a feed that states USD would otherwise score zero on a
        ledger it agrees with row for row, and this drop would read that as the
        pair disagreeing. Two stated and differing currencies still refute.
        """
        overlap = probe_ledger_overlap(
            self._db,
            account_id=account_id,
            against_account_id=other_account_id,
            unstated_currency_matches=True,
        )
        return overlap.measurable and overlap.matched == 0

    def _pending_pdf_candidates(
        self, src: SourceAccount, exclude_account_id: str
    ) -> list[_Candidate]:
        """Match PDF accounts loaded earlier in a batch but not refreshed yet.

        Reads ``raw.tabular_accounts`` by license: these rows are loaded but not
        yet refreshed into ``core.dim_accounts``, so ``core`` cannot see them.
        Layer Rule 2 in ``docs/specs/architecture-shared-primitives.md`` names
        the account-identity resolver's raw reads as an exception — resolution
        runs before the account exists in ``core``, so reading ``core`` here
        would be circular.
        """
        if src.source_type != "pdf" or not src.institution:
            return []
        target_institution = _institution_key(src.institution)
        legacy_key = src.legacy_source_account_key
        if target_institution is None or (not src.last_four and not legacy_key):
            return []
        try:
            materialized_ids = {
                str(row[0])
                for row in self._db.execute(
                    f"SELECT account_id FROM {DIM_ACCOUNTS.full_name}"  # noqa: S608  # TableRef only
                ).fetchall()
            }
        except duckdb.CatalogException:
            materialized_ids = set()
        try:
            rows = self._db.execute(
                f"SELECT DISTINCT link.account_id, raw.account_number_masked, "  # noqa: S608  # TableRef constants + parameterized account id
                f"raw.institution_name FROM {TABULAR_ACCOUNTS.full_name} AS raw "
                f"JOIN {ACCOUNT_LINKS.full_name} AS link "
                "ON link.status = 'accepted' AND link.ref_kind = 'source_native' "
                "AND link.source_type = raw.source_type "
                "AND link.source_origin = raw.source_origin "
                "AND link.ref_value = raw.account_id "
                "WHERE raw.source_type = 'pdf' AND link.account_id != ?",
                [exclude_account_id],
            ).fetchall()
        except duckdb.CatalogException:
            return []
        candidates: list[_Candidate] = []
        for account_id, masked_number, institution in rows:
            if (
                str(account_id) in materialized_ids
                or masked_number is None
                or institution is None
                or _institution_key(str(institution)) != target_institution
            ):
                continue
            digits = "".join(
                character for character in str(masked_number) if character.isdigit()
            )
            if src.last_four:
                if digits[-4:] != src.last_four:
                    continue
                signal = "institution_last4"
                value = f"{target_institution}:{src.last_four}"
            else:
                candidate_legacy_key = legacy_pdf_identifier_key(
                    issuer=str(institution), identifier=str(masked_number)
                )
                if candidate_legacy_key != legacy_key:
                    continue
                # Keep the literal out of the decision payload. The exact
                # issuer-scoped pre-document key is evidence only, never a
                # source_native or full_number ref.
                signal = "legacy_pdf_identity"
                value = "legacy_pdf_identity"
            candidates.append(
                _Candidate(
                    account_id=str(account_id),
                    signal=signal,
                    value=value,
                    confidence=0.5,
                )
            )
        return _dedupe_candidates(candidates)

    def _legacy_source_candidates(
        self, src: SourceAccount, exclude_account_id: str
    ) -> list[_Candidate]:
        """Return superseded PDF native links as review-only evidence.

        A historical tuple must be proven by raw account metadata or the same
        source path. Filename aliases and suffixes are not account identity.
        """
        legacy_key = src.legacy_source_account_key
        if not legacy_key or legacy_key == src.source_account_key:
            return []
        rows = self._db.execute(
            f"SELECT account_id, source_origin, ref_value "  # noqa: S608  # TableRef + parameterized values
            f"FROM {ACCOUNT_LINKS.full_name} "
            "WHERE status = 'accepted' AND ref_kind = 'source_native' "
            "AND source_type = ? ORDER BY account_id, source_origin, ref_value",
            [src.source_type],
        ).fetchall()
        try:
            identifier_refs = {
                (str(row[0]), str(row[1]))
                for row in self._db.execute(
                    f"SELECT DISTINCT source_origin, account_id, "  # noqa: S608  # TableRef only
                    f"account_number_masked FROM {TABULAR_ACCOUNTS.full_name} "
                    "WHERE source_type = 'pdf' AND account_number_masked IS NOT NULL"
                ).fetchall()
                if legacy_pdf_identifier_key(issuer=str(row[0]), identifier=str(row[2]))
                == str(row[1])
            }
        except duckdb.CatalogException:
            identifier_refs = set()
        try:
            alias_refs = {
                (str(row[0]), str(row[1]))
                for row in self._db.execute(
                    f"SELECT DISTINCT source_origin, account_id "  # noqa: S608  # TableRef only
                    f"FROM {TABULAR_ACCOUNTS.full_name} "
                    "WHERE source_type = 'pdf' AND account_number_masked IS NULL"
                ).fetchall()
            }
        except duckdb.CatalogException:
            alias_refs = set()
        expected_origin = src.legacy_source_origin or src.source_origin
        exact = [
            row
            for row in rows
            if str(row[1]) == expected_origin
            and str(row[2]) == legacy_key
            and (
                (str(row[1]), str(row[2])) in identifier_refs
                or (
                    src.legacy_source_account_key_is_filename_alias
                    and (str(row[1]), str(row[2])) in alias_refs
                )
            )
        ]
        if src.source_type == "pdf":
            try:
                provenance_refs = (
                    {
                        (str(row[0]), str(row[1]))
                        for row in self._db.execute(
                            f"SELECT DISTINCT source_origin, account_id "  # noqa: S608  # TableRef + parameterized source path
                            f"FROM {TABULAR_TRANSACTIONS.full_name} "
                            "WHERE source_type = 'pdf' AND source_file = ?",
                            [src.source_file],
                        ).fetchall()
                    }
                    if src.source_file
                    else set()
                )
            except duckdb.CatalogException:
                provenance_refs = set()
            historical = [
                row
                for row in rows
                if row not in exact and (str(row[1]), str(row[2])) in provenance_refs
            ]
        else:
            historical = []
        candidates = [
            _Candidate(
                account_id=str(row[0]),
                signal="legacy_pdf_identity",
                value="legacy_pdf_identity",
                confidence=0.5,
            )
            for row in [*exact, *historical]
            if str(row[0]) != exclude_account_id
        ]
        return _dedupe_candidates(candidates)

    def _reissue_candidates(
        self, src: SourceAccount, exclude_account_id: str
    ) -> list[_Candidate]:
        """Same-institution accounts whose last-four differs — the reissue signal.

        A reissued card changes its last four by definition, so the
        institution+last4 signal cannot fire; and on the PDF path
        ``account_name`` is a per-file filename alias, so the name signal misses
        too. Both signals silent meant the replacement card minted a fresh
        account with no confirm and no review entry — a statement silently
        fragmenting into a second account.

        Requires BOTH sides to carry a last-four, and requires them to differ.
        That keeps this the reissue shape rather than a general "any account at
        this institution" pick-list: an account with no known last-four is a
        different gap, and an equal last-four already fired signal 1. Review-only
        and low confidence — a reissue is proposed, never auto-merged, because a
        wrong silent merge is the hardest inference to notice and undo.
        """
        target_inst = _institution_key(src.institution) if src.institution else None
        if not target_inst or not src.last_four:
            return []
        rows = self._db.execute(
            f"SELECT account_id, institution_slug FROM {DIM_ACCOUNTS.full_name} "  # noqa: S608  # TableRef + parameterized values
            "WHERE account_id != ? AND last_four IS NOT NULL AND last_four != ? "
            "ORDER BY account_id",
            [exclude_account_id, src.last_four],
        ).fetchall()
        return [
            _Candidate(
                account_id=str(r[0]),
                # Distinct from _fallback_candidates' "institution": both are
                # institution-scoped, but this one fired on real evidence (a
                # last-four that changed) and that one fired because nothing did.
                # The string is persisted as match_reason and shown in the review
                # queue, so collapsing them would hide which it was.
                signal="institution_reissue",
                value=target_inst,
                confidence=0.3,
            )
            for r in rows
            if r[1] and _institution_key(str(r[1])) == target_inst
        ][:_FALLBACK_CANDIDATE_CAP]

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
        ``dim_accounts.institution_slug`` (cross-source slug drift, or an
        account name polluting a saved format's institution). When the scoped
        pass matches nothing, fall through to all accounts — the entire point of
        the fallback is a non-empty pick-list, so a mismatched scope must not
        recreate ``candidates: []``.

        Neither the cap nor the institution scope applies when a null last_four
        forced this review open — there the list must be *complete*, not merely
        long. Both narrowings drop accounts silently: the cap orders by opaque
        account id, so past it *which* accounts survive is arbitrary, and the
        scope keeps only slugs that match, which is precisely the drift this
        method already falls through for. Either way
        ``AccountLinksService.set()`` accepts only a target already attached to
        the decision, so an omitted account cannot be picked at all. The human
        would be asked which account this is with the right answer absent and
        only ``--standalone`` as an exit, re-minting the duplicate the quarantine
        was raised to prevent. A long list is the lesser cost; institution
        matches still lead it, so the likely answer stays at the top.
        """
        forced = src.last_four is None
        cap = None if forced else _FALLBACK_CANDIDATE_CAP
        rows = self._db.execute(
            f"SELECT account_id, institution_slug FROM {DIM_ACCOUNTS.full_name} "  # noqa: S608  # TableRef + parameterized value
            "WHERE account_id != ? ORDER BY institution_slug, account_id",
            [exclude_account_id],
        ).fetchall()
        target_inst = _institution_key(src.institution) if src.institution else None
        scoped: list[_Candidate] = []
        if target_inst:
            scoped = [
                _Candidate(
                    account_id=str(r[0]),
                    signal="institution",
                    value=target_inst,
                    confidence=0.2,
                )
                for r in rows
                if r[1] and _institution_key(str(r[1])) == target_inst
            ]
            if scoped and not forced:
                return scoped[:cap]
        led = {cand.account_id for cand in scoped}
        return (
            scoped
            + [
                _Candidate(
                    account_id=str(r[0]), signal="fallback", value="", confidence=0.1
                )
                for r in rows
                if str(r[0]) not in led
            ][:cap]
        )
