"""Adopt-or-mint security identity ladder (sync-plaid-investments.md).

Runs in SyncService.pull() after the raw load, before transforms — bindings must
exist before the staging views' resolution joins materialize into core. Every
rung ends with an accepted binding so every security-bearing row reaches the
ledger on the sync that delivered it (the engine skips NULL-security events; a
held-out row would silently understate lots and gains). Mirrors
merchant_resolver.py's ladder + under-review guard.

The governing invariant is REFUSE TO MERGE ON AMBIGUITY. One identifier matching
more than one catalog entry is never resolved by picking a winner — every tied
candidate goes to the review queue and the provider ref binds to a fresh
provisional mint. A wrong silent merge fuses two instruments' tax lots, and lots
cannot be un-merged after the fact; the cost of a wrong silent action here is
unbounded, so the bar for acting without a confirm is absolute.

Its corollary: normalization never manufactures a false agreement OR a false
contradiction. An exchange string that does not normalize (unknown MIC) is
ABSENT — no signal — never a match and never a contradiction. And a weaker
signal never overrides a stronger one that says "different instrument": a
ticker+exchange agreement is discarded outright when the CUSIP or ISIN
contradicts.

The same corollary is why a lossy normalization never auto-binds: stripping a
ticker suffix turns HEI.A into HEI, manufacturing a UNIQUE hit out of two
genuinely different instruments — which is worse than a tie, because tie-refusal
never engages. Only an EXACT ticker binds silently; a stripped one is always a
proposal (see _suffix_strip_match).
"""

from __future__ import annotations

import difflib
import logging
from dataclasses import dataclass

import duckdb

from moneybin.database import Database
from moneybin.metrics.registry import SECURITY_LINK_OUTCOMES_TOTAL
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.repositories.security_link_decisions_repo import (
    SecurityLinkDecisionsRepo,
)
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.tables import PLAID_SECURITIES, SECURITIES, SEED_EXCHANGE_MIC_MAP

logger = logging.getLogger(__name__)

_SOURCE_TYPE = "plaid"
_FUZZY_CUTOFF = 0.85
# Display-only score on a review-queue row; the human makes the call, so every
# proposal carries the same "needs a look" weight rather than a false precision.
_PROPOSAL_CONFIDENCE = 0.5

# Plaid Security.type is a prose enum, not schema-enforced — map defensively
# onto the app.securities CHECK vocabulary; anything unrecognized is 'other'.
_PLAID_TYPE_MAP = {
    "equity": "equity",
    "etf": "etf",
    "mutual fund": "mutual_fund",
    "fixed income": "bond",
    "cash": "cash",
    "cryptocurrency": "crypto",
    "derivative": "other",
    "loan": "other",
    "other": "other",
}

_PLACEHOLDER_NAME = "(Plaid security)"


def _norm(value: str | None) -> str | None:
    """Uppercase + strip an identifier; empty/whitespace reads as absent (None).

    Absent is the neutral value throughout the ladder: it never matches and
    never contradicts. Normalizing here prevents a false MISS on ' aapl ' —
    it can never create a false match, since the transform is
    case/whitespace-only.
    """
    normalized = (value or "").strip().upper()
    return normalized or None


@dataclass(frozen=True)
class _RawSecurity:
    """One row of raw.plaid_securities, as the ladder sees it."""

    security_id: str
    institution_security_id: str | None
    institution_id: str | None
    ticker: str | None
    mic: str | None
    name: str | None
    security_type: str | None
    cusip: str | None
    isin: str | None
    is_cash_equivalent: bool | None
    currency_code: str | None


@dataclass(frozen=True)
class _CatalogEntry:
    """One row of app.securities, as the ladder sees it."""

    security_id: str
    name: str | None
    ticker: str | None
    exchange: str | None
    cusip: str | None
    isin: str | None
    created_by: str


@dataclass(frozen=True)
class _Match:
    """Rung-2 verdict: bind outright, or flag candidates for human review.

    ``bindable`` and ``flagged`` are mutually exclusive — a candidate is either
    unambiguous enough to auto-bind or it is not, and "not" always means review.
    """

    bindable: _CatalogEntry | None = None
    flagged: tuple[_CatalogEntry, ...] = ()
    reason: str | None = None


class SecurityResolver:
    """Rung ladder: adopt -> strong auto-bind -> provisional-mint+propose -> mint."""

    def __init__(self, db: Database, *, actor: str = "system") -> None:
        """Bind the resolver to a database and set the audit actor."""
        self._db = db
        self._actor = actor
        self._links = SecurityLinksRepo(db)
        self._decisions = SecurityLinkDecisionsRepo(db)
        self._securities = SecuritiesRepo(db)
        self._mic_by_alias = self._load_mic_registry()

    # ------------------------------- entry -------------------------------

    def resolve_all(self) -> dict[str, int]:
        """Resolve every raw Plaid security. Returns per-outcome counts.

        Keys: ``adopted`` | ``auto_bound`` | ``proposed`` | ``minted`` |
        ``pending``. Sparse — an outcome that did not occur is absent, and an
        empty raw table yields ``{}``.
        """
        rows = self._load_raw_securities()
        if not rows:
            return {}
        catalog = self._load_catalog()
        pending = self._load_pending()
        rejected = self._load_rejected()
        minted: set[str] = set()
        counts: dict[str, int] = {}
        for raw in rows:
            outcome = self._resolve_one(raw, catalog, pending, rejected, minted)
            counts[outcome] = counts.get(outcome, 0) + 1
            SECURITY_LINK_OUTCOMES_TOTAL.labels(result=outcome).inc()
        logger.info(f"Security resolution outcomes: {counts}")
        return counts

    # ------------------------------- ladder -------------------------------

    def _resolve_one(
        self,
        raw: _RawSecurity,
        catalog: list[_CatalogEntry],
        pending: set[tuple[str, str]],
        rejected: set[tuple[str, str, str]],
        minted: set[str],
    ) -> str:
        refs = self._refs_for(raw)

        # Rung 1 — adopt: any ref of this row already bound wins outright. The
        # sibling ref is backfilled onto the same security, which is what makes
        # a churned plaid_security_id (corporate action) re-bind to the existing
        # canonical security via the stable institution ref instead of minting a twin.
        for ref_kind, ref_value in refs:
            bound = self._links.lookup(
                ref_kind=ref_kind, ref_value=ref_value, source_type=_SOURCE_TYPE
            )
            if bound is not None:
                self._bind_refs(refs, bound)
                self._refresh_if_minted(raw, bound, catalog)
                return "adopted"

        # A ref under review is never auto-bound or re-minted — the pending
        # decision retains control (magic stays visible). Rung-1 adopt is
        # unaffected: a pending decision is not an accepted binding.
        if any(ref in pending for ref in refs):
            return "pending"

        # Rung 2 — strong identifiers.
        match = self._strong_match(raw, catalog)
        if match.bindable is not None:
            self._bind_refs(refs, match.bindable.security_id)
            return "auto_bound"

        # Rungs 3/4 — provisional mint, then (rung 3) one pending decision per
        # flagged candidate. An identifier tie surfaces EVERY tied candidate as
        # sibling decisions; accepting one auto-rejects the rest (Task 10), so a
        # tie still resolves in a single user action — but the resolver itself
        # never picks.
        primary = refs[0]
        proposals = [
            (candidate, match.reason or "identifier_tie")
            for candidate in match.flagged
            if self._offerable(candidate, primary, rejected, minted)
        ]
        if not proposals:
            proposals = [
                (candidate, "fuzzy_name")
                for candidate in self._fuzzy_candidates(
                    raw, catalog, rejected, primary, minted
                )
            ]

        minted_id = self._mint(raw, catalog, minted)
        self._bind_refs(refs, minted_id)
        if not proposals:
            return "minted"
        for candidate, reason in proposals:
            self._decisions.insert(
                ref_kind=primary[0],
                ref_value=primary[1],
                source_type=_SOURCE_TYPE,
                provider_ticker=raw.ticker,
                provider_name=raw.name,
                candidate_security_id=candidate.security_id,
                confidence_score=_PROPOSAL_CONFIDENCE,
                match_signals={"signal": reason, "value": raw.name},
                match_reason=reason,
                decided_by="auto",
                actor=self._actor,
            )
        pending.add(primary)  # keep the in-batch guard consistent with the DB
        return "proposed"

    def _strong_match(self, raw: _RawSecurity, catalog: list[_CatalogEntry]) -> _Match:
        """Rung 2: bind on an unambiguous identifier, or flag for review.

        CUSIP equality binds outright (exchange irrelevant); else ISIN; else a
        UNIQUE **exact** ticker match with exchange agreement on the normalized
        MIC: same MIC binds, either side absent/unnormalizable binds, both
        present and different flags the candidate (``exchange_contradiction``).

        Only an EXACT ticker auto-binds. A hit found by stripping a suffix
        (``VOD.L`` -> ``VOD``) is a weak inference and is always flagged
        (``ticker_suffix_strip``), never bound: the strip cannot tell an exchange
        suffix from a share-class (``HEI.A``) or preferred-series (``BAC-PL``)
        suffix, those list on the SAME exchange as the stem (so MIC agreement
        confirms rather than discriminates), and Plaid's CUSIP/ISIN are
        license-gated — NULL in practice — so no stronger signal is left to catch
        the error. Binding one would fuse two instruments' tax lots irreversibly.
        The cost of flagging is bounded: the user confirms ``VOD.L`` -> ``VOD``
        once, and rung 1 adopts the binding silently on every later sync.

        Two refusals, both absolute:

        * **Ambiguity.** One identifier matching MORE than one catalog entry is
          a genuine ambiguity — usually a catalog duplicate. Every tied
          candidate is flagged (``identifier_tie``) and NONE is auto-picked, at
          any N.
        * **Contradiction.** A candidate whose CUSIP or ISIN contradicts the
          provider's is a DIFFERENT instrument, whatever the weaker signals say.
          It is discarded — never bound, never even proposed (the same Guard-2
          rule the fuzzy rung applies). Without this, a ticker+MIC agreement
          could silently merge two securities the CUSIP had already told us apart.
        """
        cusip = _norm(raw.cusip)
        if cusip:
            hits = [c for c in catalog if _norm(c.cusip) == cusip]
            if len(hits) > 1:
                return _Match(flagged=tuple(hits), reason="identifier_tie")
            if len(hits) == 1 and not self._contradicts(raw, hits[0]):
                return _Match(bindable=hits[0])
            # A lone CUSIP hit that contradicts on ISIN is not this instrument;
            # fall through — the weaker signals re-reject it and the row mints.

        isin = _norm(raw.isin)
        if isin:
            hits = [c for c in catalog if _norm(c.isin) == isin]
            if len(hits) > 1:
                return _Match(flagged=tuple(hits), reason="identifier_tie")
            if len(hits) == 1 and not self._contradicts(raw, hits[0]):
                return _Match(bindable=hits[0])

        ticker = _norm(raw.ticker)
        if not ticker:
            return _Match()
        hits = [c for c in catalog if _norm(c.ticker) == ticker]
        if len(hits) > 1:
            return _Match(flagged=tuple(hits), reason="identifier_tie")
        if not hits:
            return self._suffix_strip_match(raw, ticker, catalog)
        candidate = hits[0]
        if self._contradicts(raw, candidate):
            return _Match()  # a strong id already said "different instrument"
        catalog_mic = self._normalize_mic(candidate.exchange)
        provider_mic = self._normalize_mic(raw.mic)
        if catalog_mic and provider_mic and catalog_mic != provider_mic:
            return _Match(flagged=(candidate,), reason="exchange_contradiction")
        return _Match(bindable=candidate)  # same MIC, or either side absent

    def _suffix_strip_match(
        self, raw: _RawSecurity, ticker: str, catalog: list[_CatalogEntry]
    ) -> _Match:
        """Catalog entries matching the provider ticker's stem — flagged, never bound.

        EVERY stem hit surfaces: a strip that lands on more than one entry is as
        ambiguous as any other tie, and the resolver never picks. A candidate a
        strong identifier contradicts is discarded outright (Guard 2) — a stripped
        ticker cannot outvote a CUSIP/ISIN that says "different instrument".
        """
        if "." not in ticker and "-" not in ticker:
            return _Match()  # no suffix to strip — the fuzzy rung decides
        stem = ticker.replace("-", ".").split(".")[0]
        hits = tuple(
            c
            for c in catalog
            if _norm(c.ticker) == stem and not self._contradicts(raw, c)
        )
        if not hits:
            return _Match()
        return _Match(flagged=hits, reason="ticker_suffix_strip")

    def _contradicts(self, raw: _RawSecurity, candidate: _CatalogEntry) -> bool:
        """True when a strong identifier proves these are DIFFERENT instruments.

        Only a pair that is present on BOTH sides and unequal contradicts. An
        identifier absent on either side is no signal — it must never
        manufacture a contradiction (that would suppress a legitimate match).
        """
        for provider_id, catalog_id in (
            (_norm(raw.cusip), _norm(candidate.cusip)),
            (_norm(raw.isin), _norm(candidate.isin)),
        ):
            if provider_id and catalog_id and provider_id != catalog_id:
                return True
        return False

    def _offerable(
        self,
        candidate: _CatalogEntry,
        primary: tuple[str, str],
        rejected: set[tuple[str, str, str]],
        minted: set[str],
    ) -> bool:
        """May this candidate be OFFERED to the reviewer as the merge survivor?

        Two disqualifiers. A pairing the user already rejected is never
        re-proposed. And a security minted earlier in THIS batch is not a
        reviewable survivor — it is itself an unreviewed provisional row, so
        offering it would ask the human to merge into something pending review.
        (It remains a valid rung-2 auto-bind target: in-batch dedup on an exact
        identifier is intended.)
        """
        if candidate.security_id in minted:
            return False
        return (primary[0], primary[1], candidate.security_id) not in rejected

    def _fuzzy_candidates(
        self,
        raw: _RawSecurity,
        catalog: list[_CatalogEntry],
        rejected: set[tuple[str, str, str]],
        primary: tuple[str, str],
        minted: set[str],
    ) -> tuple[_CatalogEntry, ...]:
        """Every catalog entry whose name matches above the cutoff. Proposals, never binds.

        Names are grouped, NOT deduplicated: two catalog rows sharing one name are
        a catalog duplicate, and both must reach the reviewer. Keeping only the
        first would hide the other and make which one is shown depend on the
        `security_id` sort — a pick on ambiguity, in the one surface that must
        refuse to pick.
        """
        target = _norm(raw.name)
        if target is None:
            return ()
        by_name: dict[str, list[_CatalogEntry]] = {}
        for entry in catalog:
            name = _norm(entry.name)
            if name is not None:
                by_name.setdefault(name, []).append(entry)
        matches = difflib.get_close_matches(
            target, list(by_name), n=3, cutoff=_FUZZY_CUTOFF
        )
        return tuple(
            candidate
            for match in matches
            for candidate in by_name[match]
            # Guard 2: a contradicting strong identifier disqualifies — a name
            # that reads alike cannot outvote a CUSIP/ISIN that differs.
            if not self._contradicts(raw, candidate)
            and self._offerable(candidate, primary, rejected, minted)
        )

    # ------------------------------- writes -------------------------------

    def _mint(
        self, raw: _RawSecurity, catalog: list[_CatalogEntry], minted: set[str]
    ) -> str:
        """Mint a provider-provenance catalog row and add it to the in-batch catalog.

        The new entry joins ``catalog`` so a later row in the same batch carrying
        the same CUSIP/ISIN/ticker adopts it (rung 2) instead of minting a twin.
        It also joins ``minted``, which bars it from being OFFERED as a merge
        survivor to a later row (see ``_offerable``).
        """
        name = (
            (raw.name or "").strip() or (raw.ticker or "").strip() or _PLACEHOLDER_NAME
        )
        event = self._securities.upsert(
            security_id=None,
            name=name,
            security_type=self._security_type(raw),
            ticker=raw.ticker,
            exchange=raw.mic,
            cusip=raw.cusip,
            isin=raw.isin,
            is_cash_equivalent=raw.is_cash_equivalent,
            currency_code=raw.currency_code or "USD",
            created_by="plaid",
            actor=self._actor,
        )
        minted_id = event.target_id
        if minted_id is None:  # pragma: no cover — upsert always stamps target_id
            raise RuntimeError("securities.upsert returned no target_id")
        minted.add(minted_id)
        catalog.append(
            _CatalogEntry(
                security_id=minted_id,
                name=name,
                ticker=raw.ticker,
                exchange=raw.mic,
                cusip=raw.cusip,
                isin=raw.isin,
                created_by="plaid",
            )
        )
        return minted_id

    def _bind_refs(self, refs: list[tuple[str, str]], security_id: str) -> None:
        """Bind every unbound ref of this row to ``security_id``.

        An already-bound ref is left alone: re-binding it would raise in the
        repo's uniqueness guard. When it points somewhere ELSE the two refs on
        one provider row disagree about identity — the resolver refuses to
        rewrite either binding (a repoint is a reviewed merge, not a sync-time
        side effect) and logs it for the operator.
        """
        for ref_kind, ref_value in refs:
            bound = self._links.lookup(
                ref_kind=ref_kind, ref_value=ref_value, source_type=_SOURCE_TYPE
            )
            if bound is not None:
                if bound != security_id:
                    logger.warning(
                        f"security ref conflict: ref_kind={ref_kind} is bound to "
                        f"security_id={bound}, not {security_id}; left as-is "
                        "(resolve via the security-link review queue)"
                    )
                continue
            self._links.insert(
                security_id=security_id,
                ref_kind=ref_kind,
                ref_value=ref_value,
                source_type=_SOURCE_TYPE,
                decided_by="auto",
                actor=self._actor,
            )

    def _refresh_if_minted(
        self, raw: _RawSecurity, security_id: str, catalog: list[_CatalogEntry]
    ) -> None:
        """Refresh name/type/ticker on a plaid-minted catalog row from the provider.

        User-authored rows are never touched (the repo enforces this too), and
        the repo no-ops when nothing changed — so a daily sync accrues no
        audit churn.
        """
        entry = next((c for c in catalog if c.security_id == security_id), None)
        if entry is None or entry.created_by != "plaid" or not raw.name:
            return
        self._securities.refresh_provider_attributes(
            security_id,
            name=raw.name.strip(),
            security_type=self._security_type(raw),
            ticker=raw.ticker,
            actor=self._actor,
        )

    def _security_type(self, raw: _RawSecurity) -> str:
        return _PLAID_TYPE_MAP.get((raw.security_type or "").strip().lower(), "other")

    def _refs_for(self, raw: _RawSecurity) -> list[tuple[str, str]]:
        """The row's provider refs, primary (plaid_security_id) first.

        The institution ref is namespaced by institution_id — an
        institution_security_id is unique only within its issuing institution.
        """
        refs = [("plaid_security_id", raw.security_id)]
        if raw.institution_id and raw.institution_security_id:
            refs.append((
                "institution_security_id",
                f"{raw.institution_id}:{raw.institution_security_id}",
            ))
        return refs

    # ------------------------------- loads -------------------------------

    def _load_mic_registry(self) -> dict[str, str]:
        """Alias -> canonical MIC. Absent seed = every exchange unnormalizable."""
        try:
            rows = self._db.execute(
                f"SELECT alias, mic FROM {SEED_EXCHANGE_MIC_MAP.full_name}"  # noqa: S608  # TableRef constant
            ).fetchall()
        except duckdb.CatalogException:
            # Seed not materialized yet (first sync on a fresh DB): every
            # exchange normalizes to absent — no signal — so the ladder loses
            # recall, never correctness. It must NOT read as a contradiction.
            return {}
        return {str(r[0]).strip().upper(): str(r[1]) for r in rows}

    def _normalize_mic(self, exchange: str | None) -> str | None:
        """Canonical MIC for a free-text exchange, or None when unnormalizable."""
        alias = _norm(exchange)
        return self._mic_by_alias.get(alias) if alias else None

    def _load_raw_securities(self) -> list[_RawSecurity]:
        try:
            rows = self._db.execute(
                f"""
                SELECT DISTINCT security_id, institution_security_id, institution_id,
                       ticker_symbol, market_identifier_code, security_name,
                       security_type, cusip, isin, is_cash_equivalent,
                       COALESCE(iso_currency_code, unofficial_currency_code)
                FROM {PLAID_SECURITIES.full_name}
                ORDER BY security_id
                """  # noqa: S608  # TableRef constant
            ).fetchall()
        except duckdb.CatalogException:
            return []
        return [_RawSecurity(*row) for row in rows]

    def _load_catalog(self) -> list[_CatalogEntry]:
        rows = self._db.execute(
            f"""
            SELECT security_id, name, ticker, exchange, cusip, isin, created_by
            FROM {SECURITIES.full_name}
            ORDER BY security_id
            """  # noqa: S608  # TableRef constant
        ).fetchall()
        return [_CatalogEntry(*row) for row in rows]

    def _load_pending(self) -> set[tuple[str, str]]:
        return {
            (str(d["ref_kind"]), str(d["ref_value"]))
            for d in self._decisions.list_pending()
            if d["source_type"] == _SOURCE_TYPE
        }

    def _load_rejected(self) -> set[tuple[str, str, str]]:
        return {
            (
                str(d["ref_kind"]),
                str(d["ref_value"]),
                str(d["candidate_security_id"]),
            )
            for d in self._decisions.list_rejected()
            if d["source_type"] == _SOURCE_TYPE
        }
