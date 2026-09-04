"""Unified import service for financial data files.

This service handles the full import pipeline: detect file type, extract
data, load to raw tables, and run SQLMesh transforms. Both CLI commands and
MCP tools call this same service — no duplication.
"""

import dataclasses
import hashlib
import json
import logging
import re
import time
from collections.abc import Callable, Collection, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, NoReturn, cast

import duckdb

if TYPE_CHECKING:
    from moneybin.extractors.pdf.ir import PdfDocument
    from moneybin.extractors.pdf.routing import RouteDecision
    from moneybin.extractors.tabular.formats import TabularFormat
    from moneybin.repositories.pdf_formats_repo import PdfFormat

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError, classify_user_error
from moneybin.extractors.confidence import Confidence
from moneybin.extractors.institution_resolution import resolve_institution_tabular
from moneybin.extractors.tabular.account_label import parse_account_label
from moneybin.extractors.tabular.formats import (
    NumberFormatType,
    SignConventionType,
)
from moneybin.metrics.observations import (
    MetricObservations,
    record_counter,
    record_observation,
)
from moneybin.metrics.registry import (
    ACCOUNT_LINK_OUTCOMES_TOTAL,
    IMPORT_DURATION_SECONDS,
    IMPORT_ERRORS_TOTAL,
    IMPORT_RECORDS_TOTAL,
    TABULAR_DETECTION_CONFIDENCE,
    TABULAR_FORMAT_MATCHES,
)
from moneybin.orchestration.refresh import refresh as _refresh
from moneybin.orchestration.refresh import step_outcome as _step_outcome
from moneybin.repositories.imports_repo import ImportsRepo
from moneybin.repositories.pdf_formats_repo import PdfFormatsRepo
from moneybin.services._validators import validate_slug
from moneybin.services.account_display_name import (
    AccountNameFacts,
    account_category,
    derived_last_four,
)
from moneybin.services.account_resolution_types import (
    AccountProposalDict,
    ResolvedAccount,
    SourceAccount,
    normalize_account_identifier,
)
from moneybin.services.account_resolver import AccountResolver
from moneybin.services.audit_service import AuditService
from moneybin.services.import_confirmation import (
    ActorKind,
    Channel,
    ConfirmationRequired,
    ImportConfirmationRequiredError,
    ProposedMapping,
    SignConventionProposal,
)
from moneybin.services.ledger_overlap import (
    IncomingTransaction,
    probe_incoming_ledger_overlap,
)
from moneybin.services.refresh_outcome import RefreshStepOutcome
from moneybin.tables import (
    IMPORTS,
    OFX_ACCOUNTS,
    OFX_BALANCES,
    OFX_INSTITUTIONS,
    OFX_TRANSACTIONS,
    TABULAR_TRANSACTIONS,
)
from moneybin.utils.file import source_sha256

logger = logging.getLogger(__name__)


class ImportRefreshError(RuntimeError):
    """A closing refresh that failed after it had already reversed transfers.

    Sibling of ``MatchRunError``, and carries state for the same reason: the
    refresh reconciles inside its *match* step and commits there, so a transform
    apply that dies afterwards leaves those reversals on disk. Without a carrier
    the count dies with the exception and the command exits reporting a failed
    transform while saying nothing about a decision the user made being undone.

    Subclasses ``RuntimeError`` because that is what this raise site has always
    been and callers catch it as such; the narrower type only adds the payload.
    """

    def __init__(self, message: str, *, transfers_retired: int) -> None:
        """Carry ``transfers_retired`` — the reversals already committed."""
        super().__init__(message)
        self.transfers_retired = transfers_retired


@dataclass(frozen=True, slots=True)
class CreatedAccount:
    """One canonical account an import minted.

    The visible half of "gate the merge, not the mint": a first-contact mint no
    longer stops the import, so the import has to say what it created. Both
    fields are safe to show — ``account_id`` is an opaque uuid4[:12], and
    ``display_name`` is the name ``core.dim_accounts`` stores for the account,
    derived at mint time from the same seed registries the model joins, never
    its ``source_account_key``, which is an account number on several channels.

    Announcing the *stored* name is the contract, not a nicety. This row is the
    only place a first-contact mint is disclosed, and the agent is told to
    report it to the user; a name derived any other way sends them looking for
    an account ``accounts`` answers to under a different one. That cuts both
    ways: where the file named the account itself the model uses that name, so
    this row shows it too, and where nothing can name the account both read
    ``Unnamed account``.
    """

    account_id: str
    display_name: str


# Five digits, counted across any single NON-ALPHANUMERIC separator. Four is
# the masked last-four banks print (and the shape of a year), so it stays;
# anything longer in an account label is a number, not a label.
#
# **A whole word ends an account number. Nothing else does.**
#
# That sentence is the rule, and it is the fourth attempt at it. The first three
# each described the *gap* between two digits and each shipped a leak: "-", then
# any single non-alphanumeric ("." "/" "_"), then any run of three ("12AB34CD56"
# masked but "12ABCD34EFGH56" did not). Every one was a guess about how account
# numbers are punctuated, and an issuer who punctuated them differently walked
# straight through. Stop guessing at the gap's *shape* and name what actually
# separates two labels: a word.
#
# So a run of digits continues across a gap that is either
#
#   - whitespace-free  — "12ABCD34", "1234-5678", "12X3456789": letters and
#     punctuation inside one token are part of the identifier, however long; or
#   - letter-free      — "4111 1111 1111", "1234 - 5678": spacing and
#     punctuation between digit groups, however long.
#
# and stops at a gap that is neither, which is exactly a whitespace-delimited
# alphabetic word: "Checking 1234 Savings 5678" stays two safe four-digit
# tokens, and "Retirement Plan 2024 Rewards" keeps its name instead of collapsing to
# "****2024".
#
# Every quantifier here must have exactly one way to match a given gap, because
# this runs on file-supplied labels and a failed match backtracks through every
# alternative. An earlier version wrote the second branch as
# `[^0-9A-Za-z]*\s[^0-9A-Za-z]*`, whose leading run can itself match whitespace
# — so a run of N spaces had N places to put the `\s`, and a label that ended up
# not matching cost 2^N. 165 characters took half a second; every further pair
# of spaces doubled it. Excluding whitespace from the leading run pins `\s` to
# the *first* one, which leaves a single parse. The two branches are disjoint on
# whitespace count (zero vs. at least one), so no gap can take both.
#
# The leading and trailing [A-Za-z]* take the rest of the token, so "X12345678"
# masks whole rather than leaving an "X" stub that publishes the prefix.
#
# The cost is over-masking a decimal in a label ("Balance 1234.56"). That is the
# right side to err on: an over-masked label is legible, an under-masked one is
# an account number.
_ACCOUNT_NUMBER_GAP = r"(?:[^\s\d]*|[^\s0-9A-Za-z]*\s[^0-9A-Za-z]*)"
# The lookbehind is the other half of keeping this linear. Without it the
# leading [A-Za-z]* is retried from every character of a long letter run,
# rescanning the whole run each time — quadratic, 1.2s on a 20k-character label,
# and labels come from the file. A match can only begin where the identifier
# does, so requiring a non-alphanumeric (or string start) before it makes every
# interior retry fail in O(1) instead of O(n). It changes no result: a match
# that could start mid-token is already found from that token's start, where the
# greedy prefix covers the same span.
_EMBEDDED_ACCOUNT_NUMBER = re.compile(
    rf"(?<![0-9A-Za-z])[A-Za-z]*\d(?:{_ACCOUNT_NUMBER_GAP}\d){{4,}}[A-Za-z]*"
)


def mask_embedded_account_number(label: str) -> str:
    """Mask an account number embedded in a derived account label.

    ``parse_account_label`` lifts out a *recognized masked* last-four —
    ``(...7777)``, ``x7777``, a bare trailing group — so the shapes it leaves
    behind are the ones that matter, and grouping is what makes them dangerous:
    ``Checking 4111 1111 1111 1111`` loses only its final token and arrives as
    ``Checking 4111 1111 1111``, twelve digits of a card number in a field
    declared ``USER_NOTE`` and shown unmasked wherever a mint is reported.

    Masks the run rather than the whole string, because naming what was created
    is the entire purpose of the field: "Checking 987654321098" has to become
    "Checking ****1098", not "****1098". The kept four are the run's last four
    *digits*, so a grouped number, a contiguous one, and an alphanumeric one
    mask alike — and the suffix stays four digits, the form every other masked
    surface in the codebase shows.
    """

    def _mask(match: re.Match[str]) -> str:
        digits = re.sub(r"\D", "", match.group())
        return f"****{digits[-4:]}"

    return _EMBEDDED_ACCOUNT_NUMBER.sub(_mask, label)


#: What ``mask_embedded_account_number`` leaves behind, so a residue can be
#: examined without counting the mask's own digits as surviving ones.
_MASK_TOKEN = re.compile(r"\*{4}\d{4}")


def authored_label_parts(label: str) -> tuple[str, str, str | None]:
    """Return ``(display_label, clean_name, last_four)`` for an authored label.

    Masks *before* parsing, which is the whole point of the helper. Lifting the
    last-four token out first leaves a truncated number whose new tail is not
    the account's: "Checking 1234 5678 9012 3456" loses 3456, and masking what
    remains states "****9012" -- the middle of the number, in the position
    every other surface fills with the last four. It also blocks the real one,
    since a label already carrying four digits takes no append. Masking first
    collapses the run to its true tail for the parser to lift.

    Every authored-label site goes through here so the ordering cannot drift
    back apart: one path derives the key, one takes --account-name, one reads
    a tabular file's Account column, one reads a connected Google Sheet's, and
    all four feed the same two consumers. Public for that last caller — the
    gsheet transactions adapter imports it rather than re-deriving a label,
    which is what keeps one account exported through both channels named once.

    Refuses the remainder when the mask fired and a digit survived outside it,
    because that digit says the remainder is still the identifier. A formatted
    account number puts a *word* between its digit groups -- a bank code is
    letters -- so "CC00 BANK 1234 5678 9012" masks only from the first group
    onward and leaves "CC00 BANK". Nothing downstream can tell that from a name:
    it carries letters, which is exactly the test `dim_accounts` uses to decide
    a label names the account, so it became the account's name with the last
    four appended. Widening the mask across the word is the other repair and it
    is the wrong one -- the word boundary is what keeps "Checking 1234 Savings
    5678" and "Retirement Plan 2024 Rewards" intact, and no rule can tell a bank
    code from a real word. Collapsing to the mask is also what the same number
    unspaced already did, so the two spellings stop disclosing to different
    depths.

    Refuses it again when the mask fired more than once, because a label naming
    two identifiers discloses both of their tails and no digit has to survive
    outside a token for that to happen: "Primary 123456789 Secondary 987654321
    account" masks to two clean tokens and ends in an ordinary word, so the
    residue test above finds nothing wrong with eight digits drawn from two
    distinct numbers. Both halves ask the one question -- is what is left still
    an identifier -- and a label that had to mask two of them is not a name
    under either.
    """
    masked = mask_embedded_account_number(label)
    if masked != label:
        tokens = _MASK_TOKEN.findall(masked)
        if len(tokens) > 1 or any(ch.isdigit() for ch in _MASK_TOKEN.sub(" ", masked)):
            masked = tokens[-1]
    clean_name, last_four = parse_account_label(masked)
    return (mask_embedded_account_number(clean_name), clean_name, last_four)


def _mask_caller_keys(keys: Iterable[str]) -> str:
    """Render caller-supplied binding/metadata keys for a refusal message.

    A refusal quoting the caller's own key reads as safe — they typed it — but
    the CLI writes these through ``logger.error``, and a log file is exactly the
    "artifact that outlives the session" ``.claude/rules/security.md`` names as a
    boundary. On OFX a key is the institution's ``<ACCTID>``, and the log
    sanitizer masks recognized shapes, not every issuer's numbering.

    Masked rather than omitted: the caller still has to learn *which* of their
    keys was wrong, and the valid-ref list alone cannot tell that to someone who
    bound by source key. Same mask as the mint report, so a key and the label
    derived from it are never disclosed to different depths.
    """
    return ", ".join(repr(mask_embedded_account_number(key)) for key in sorted(keys))


def _created_account(
    src: SourceAccount,
    resolved: ResolvedAccount,
    *,
    settings: dict[str, str] | None = None,
) -> CreatedAccount | None:
    """The account this resolve minted, or None when it adopted an existing one.

    One definition of "created" for all three channels, so a new channel cannot
    report a different set than the one it bound.

    ``settings`` is the caller's ``account_metadata`` for this source account.
    Its ``display_name`` wins outright: ``_capture_new_account_metadata`` writes
    it to ``app.account_settings`` and ``dim_accounts`` COALESCEs that arm ahead
    of everything derived. Its other fields refine the derivation below rather
    than replacing it, in the same order the model reads them.

    Everything else is derived from ``src.name_facts`` — the same facts, from
    the same seed registries, that ``core.dim_accounts`` will name the account
    by. It is *not* read back from ``core``: nothing has refreshed yet at this
    point, and ``import_confirm`` never refreshes at all.
    """
    if resolved.outcome != "minted_new":
        return None
    settings = settings or {}
    if display_name := (settings.get("display_name") or "").strip():
        # The caller's own chosen name, which `_capture_new_account_metadata`
        # also writes to `app.account_settings` — masking the announcement of a
        # name they typed and will see everywhere else would only make the two
        # disagree. Stripped for that same reason: AccountSettings normalizes
        # before the write, so announcing the raw string would announce padding
        # the stored name does not carry.
        return CreatedAccount(account_id=resolved.account_id, display_name=display_name)
    if src.name_facts is not None:
        return CreatedAccount(
            account_id=resolved.account_id,
            display_name=src.name_facts.with_settings(settings).display_name(),
        )
    # No channel reaches here — all three state their facts. Kept so a future
    # source that mints before it can state them announces the id under the
    # file's own label rather than raising mid-import; masked, because that
    # label can be a bare account number.
    return CreatedAccount(
        account_id=resolved.account_id,
        display_name=mask_embedded_account_number(src.account_name),
    )


@dataclass
class ImportResult:
    """Summary of what was imported."""

    file_path: str
    file_type: str
    accounts: int = 0
    transactions: int = 0
    institutions: int = 0
    balances: int = 0
    date_range: str = ""
    details: dict[str, int] = field(default_factory=dict)
    core_tables_rebuilt: bool = False
    transfers_retired: int = 0
    """Standing transfers this import's own refresh reversed.

    Twin of ``BatchImportResult.transfers_retired``: the closing refresh runs
    the matcher, so folding a duplicate can undo a transfer the user accepted.
    Unlike ``core_tables_rebuilt`` this reports something undone, so it has to
    reach the surface rather than being inferred from a success."""
    refresh_steps: RefreshStepOutcome | None = None
    """What that same refresh's four best-effort steps did.

    Twin of ``BatchImportResult.refresh_steps``, and carried for the same
    reason the count above is: a single-file import runs the whole cascade too,
    so it reaches the network for exchange rates. Everything downstream reads
    the synthesized batch, so this has to ride across onto it."""
    sign_correction_suggested: bool = False
    """True if running balance suggests sign inversion; amounts were NOT auto-corrected."""
    sign_override_replayed: bool = False
    """True when this PDF replayed a saved recipe whose sign convention a human set
    with `sign=` — the card-marker detector is bypassed for that format, so the
    replay is surfaced rather than applied silently."""
    import_id: str | None = None
    """UUID of the raw.import_log row this import created."""
    accounts_created: tuple[CreatedAccount, ...] = ()
    """Canonical accounts this import minted; empty when every account was adopted.

    Populated at each channel's ``resolve()`` pass, filtered to
    ``outcome == "minted_new"`` — the same filter ``_capture_new_account_metadata``
    uses, and for the same reason: a ``pending_review`` provisional is
    ``is_new`` too, but a later accept abandons its id, so reporting it would
    name an account the user can never find."""
    pdf_format_name: str | None = None
    """Name a PDF recipe was actually persisted under, or None if not saved
    (save_format off, or save_new skipped/failed). Set only on a confirmed
    save_new so apply_pdf_bridge_response never claims a save that didn't land."""
    field_mapping: dict[str, str] | None = None
    """Authoritative destination → source column mapping the load used.

    Populated for tabular imports from the resolved (matched-format or
    confirmed) mapping. None for OFX/non-tabular paths. Callers
    (import_confirm response, audit log) should prefer this over re-running
    detection, which can diverge in ambiguous-header edge cases."""

    @property
    def rows_loaded(self) -> int:
        """Per-file row count for CLI/MCP JSON output.

        Mirrors ``PerFileResult.rows_loaded``: prefer ``details['seed_rows']``
        when populated (PDF seed path writes no transactions; the seed row
        count is the meaningful one), else fall back to ``transactions``.
        Without this, single-file JSON output reports ``rows_loaded: 0`` for
        every seed-path PDF — same regression that ``import_files`` fixed by
        introducing this property on ``PerFileResult``.
        """
        return self.details.get("seed_rows", self.transactions)

    def summary(self) -> str:
        """Human-readable import summary."""
        label = _display_label(self.file_type, Path(self.file_path))
        lines = [f"Imported {label} file: {self.file_path}"]

        if self.institutions:
            lines.append(f"  Institutions: {self.institutions}")
        if self.accounts:
            lines.append(f"  Accounts: {self.accounts}")
        if self.transactions:
            lines.append(f"  Transactions: {self.transactions}")
        if self.balances:
            lines.append(f"  Balances: {self.balances}")
        # PDF Phase 1 sets transactions=0 (no core rows land), so surface the
        # seed-row count instead — otherwise the summary tells the user
        # nothing about what landed.
        if "seed_rows" in self.details:
            seeded = self.details["seed_rows"]
            extracted = self.details.get("seed_rows_extracted", seeded)
            if extracted == seeded:
                lines.append(f"  Seed rows: {seeded}")
            else:
                lines.append(
                    f"  Seed rows: {seeded} (extracted {extracted}, "
                    f"{extracted - seeded} already present from prior import)"
                )
        if self.date_range:
            lines.append(f"  Date range: {self.date_range}")
        if self.core_tables_rebuilt:
            lines.append("  Core tables rebuilt (dim_accounts, fct_transactions)")

        return "\n".join(lines)


@dataclass(frozen=True, slots=True)
class SavedFormatDeletePlan:
    """Exact live state bound to one saved-format deletion approval."""

    format_name: str
    state_sha256: str

    @property
    def blast_radius(self) -> dict[str, int]:
        """Return the one-row destructive impact for confirmation metadata."""
        return {"saved_formats": 1}


ImportRevertOutcome = Literal[
    "revertable",
    "not_found",
    "already_reverted",
    "unsupported",
    "superseded",
]


@dataclass(frozen=True, slots=True)
class ImportRevertPlan:
    """Exact live state bound to one import reversion approval.

    Non-revertable outcomes are plans too, so a batch that flips state between
    approval and commit changes the binding rather than slipping past it.
    """

    import_id: str
    outcome: ImportRevertOutcome
    reason: str | None = None
    source_type: str | None = None
    source_origin: str | None = None
    table_counts: tuple[tuple[str, int], ...] = ()

    @property
    def revertable(self) -> bool:
        """Return whether this plan would actually delete or flip anything."""
        return self.outcome == "revertable"

    @property
    def rows_to_delete(self) -> int:
        """Return the total raw rows this reversion would destroy."""
        return sum(count for _, count in self.table_counts)

    @property
    def blast_radius(self) -> dict[str, int]:
        """Return the per-table destructive impact for confirmation metadata."""
        radius = dict(self.table_counts)
        radius["total_rows"] = self.rows_to_delete
        return radius

    def as_result(self) -> dict[str, str | int]:
        """Return the legacy non-revertable response for this outcome."""
        if self.reason is None:
            return {"status": self.outcome}
        return {"status": self.outcome, "reason": self.reason}


@dataclass(frozen=True)
class PerFileResult:
    """One file's outcome inside a batch import.

    ``status="confirmation_required"`` is used when ``_import_one`` raised
    ``ImportConfirmationRequiredError`` — the file's detector proposal is
    captured in ``confirmation_payload`` so the multi-file MCP envelope
    can list per-file pending entries with enough context for the agent
    to invoke ``import_confirm`` on each.
    """

    path: str
    status: Literal["imported", "failed", "skipped", "confirmation_required"]
    source_type: str | None
    rows_loaded: int = 0
    rows_skipped: int = 0
    import_id: str | None = None
    error: str | None = None
    error_code: str | None = None
    """Stable error code from `classify_user_error`; None when unclassified.

    Paired with `error`: when this is set, `error` holds the classified
    (sanitized) message. When None, `error` holds only the exception class
    name — raw str(e) may embed PII, so unclassified failures stay opaque.
    """
    hint: str | None = None
    """Actionable recovery advice from the classified `UserError`; None when
    unclassified.

    Set together with `error_code` and from the same source. This is what makes
    a permission failure fixable — the chmod/chown advice on EACCES, the macOS
    Full-Disk-Access walkthrough on a TCC block. Like `error`, it never comes
    from raw str(e).
    """
    details: dict[str, Any] | None = None
    """Structured facts behind `error_code`, for branching instead of parsing.

    A permission failure carries `errno`, `platform`, and — when the macOS
    branch fires — `protected_root`. `hint` says the same thing in prose, but
    prose is not a contract: an agent that wants to know whether this was a TCC
    denial should read `details["protected_root"]`, not grep the hint. Travels
    with `error_code` and `hint`; None whenever they are.
    """
    sign_correction_suggested: bool = False
    """True if running balance suggests sign inversion; amounts were NOT auto-corrected."""
    sign_override_replayed: bool = False
    """Mirrors ``ImportResult.sign_override_replayed`` for batch imports — a saved
    `sign=` override replayed onto this file, bypassing the card-marker detector."""

    accounts_created: tuple[CreatedAccount, ...] = ()
    """Mirrors ``ImportResult.accounts_created`` for batch imports.

    Per file, not per batch: a ten-file import that mints one account has to say
    which file brought it."""

    confirmation_payload: dict[str, object] | None = None
    """Populated only when status == 'confirmation_required': detector proposal
    + samples + flagged + missing_required so the agent can call
    ``import_confirm`` per file. None on imported/failed/skipped rows."""


@dataclass(frozen=True)
class BatchImportResult:
    """Outcome of an import_files call.

    Note: ``per_file`` is a list (mutable), so instances aren't hashable —
    matches the precedent set by other frozen result dataclasses in the
    services layer.
    """

    per_file: list[PerFileResult]
    transforms_applied: bool
    transforms_duration_seconds: float | None
    transforms_error: str | None = None
    # An import's closing refresh runs the matcher, so folding a duplicate can
    # reverse a transfer the user had accepted. Unlike the transform fields,
    # this one reports something undone rather than something built.
    transfers_retired: int = 0
    # The rest of that refresh: a categorizer, an identity pass and a network
    # rate backfill all run here too, and `transforms_error` above reports none
    # of them. None means no refresh ran.
    refresh_steps: RefreshStepOutcome | None = None

    @property
    def imported_count(self) -> int:
        """Number of files that imported successfully."""
        return sum(1 for r in self.per_file if r.status == "imported")

    @property
    def failed_count(self) -> int:
        """Number of files that failed to import."""
        return sum(1 for r in self.per_file if r.status == "failed")

    @property
    def total_count(self) -> int:
        """Total number of files attempted in this batch."""
        return len(self.per_file)


# Routing reasons where Phase 2b escalates `import_preview` to the bridge.
# The deterministic rung found a transaction table but couldn't finalize it —
# the driving agent has a chance to crack the layout where the deterministic
# path couldn't.
#
# ``no_transaction_table``, ``no_rows``, and ``unsupported_number_format`` stay
# out of this set: the document isn't transaction-shaped (so the bridge would be
# off-target — a brokerage positions statement belongs in a seed) or has no
# extractable content (so a text-bridge has nothing to read).
#
# ``transaction_table_underivable`` is the case those three used to swallow:
# routing once reported *every* derivation failure as ``no_transaction_table``,
# so a statement that WAS transaction-shaped and merely defeated derivation was
# silently buried in an opaque seed instead of reaching the agent that could
# read it. Routing now separates the two (see `_Reason` in routing.py).
_BRIDGE_ELIGIBLE_REASONS: frozenset[str] = frozenset({
    "low_confidence",
    "replay_reconciliation_failed",
    "reconciliation_failed",
    "metadata_incomplete",
    "transaction_table_underivable",
})

# A card-statement inversion is a confident inference (the disclosures are
# unambiguous) whose COST IF WRONG is a corrupted ledger, on this import and on
# every future replay. `medium` says "eyeball this" — it is never `high`, because
# `high` is the tier an agent may self-accept at.
_CARD_SIGN_CONFIDENCE = Confidence(
    score=0.75, tier="medium", flagged=("sign_convention",), missing_required=()
)

# How many rows the sign proposal shows as before/after samples.
_SIGN_SAMPLE_LIMIT = 3


def _sign_sample_rows(
    rows: list[dict[str, Any]], *, limit: int = _SIGN_SAMPLE_LIMIT
) -> list[dict[str, str]]:
    """Show the flip concretely: what the statement printed vs what we'd record."""
    from decimal import Decimal

    samples: list[dict[str, str]] = []
    for row in rows[:limit]:
        printed = row.get("amount")
        if printed is None:
            continue
        samples.append({
            "description": str(row.get("description", ""))[:60],
            "as_printed": str(printed),
            "as_recorded": str(-Decimal(str(printed))),
        })
    return samples


@dataclass(frozen=True)
class PdfPreviewResult:
    """Outcome of running ``pdf_preview`` against a native-text PDF.

    Returned when the deterministic rung either succeeded or failed in a way
    that the bridge can't improve on (e.g. ``no_transaction_table``). When the
    deterministic outcome IS bridge-eligible, ``pdf_preview`` raises
    ``ImportConfirmationRequiredError`` carrying a ``BridgePayload`` instead of
    returning — the escalation is the result.
    """

    file_path: str
    deterministic: bool
    """True when the recipe ran cleanly and rows would route to transactions."""

    decision_reason: str
    """Routing reason (``passed`` on success; ``no_transaction_table`` /
    ``no_rows`` / ``unsupported_number_format`` on non-escalating fallbacks).

    A transaction-shaped document that defeated derivation reports
    ``transaction_table_underivable`` and escalates instead of landing here."""

    confidence: float
    row_count: int
    fingerprint: dict[str, Any] | None = None


@dataclass(frozen=True)
class BridgeApplyResult:
    """Outcome of applying a bridge response via ``apply_pdf_bridge_response``.

    ``outcome`` is ``applied`` when the agent's recipe re-executed and the
    re-executed rows passed the reconciliation gate (Req 9), or ``invalid``
    when they did not — in which case nothing loads and ``reject_reason``
    carries the routing reason (e.g. ``reconciliation_failed``).

    The divergence fields verify the agent's *expectation* against the
    *actual* re-execution (per the bridge trust model): ``expected_row_count``
    is how many rows the agent returned; ``actual_row_count`` is how many the
    recipe reproduced against the document; ``rows_diverged`` is True when they
    differ. Divergence does not block a load that reconciles — reconciliation
    on the re-executed rows is the authority — but it is surfaced so the caller
    can flag a recipe that doesn't reproduce its author's own extraction.
    """

    outcome: Literal["applied", "invalid"]
    import_id: str | None
    rows_loaded: int
    format_name: str | None
    expected_row_count: int
    actual_row_count: int
    rows_diverged: bool
    reject_reason: str | None = None
    accounts_created: tuple[CreatedAccount, ...] = ()
    """Mirrors ``ImportResult.accounts_created`` — the bridge is an import path
    like any other, and its caller never sees the underlying ``ImportResult``.
    Always empty on ``outcome='invalid'``: nothing loaded, so nothing minted."""


@dataclass(frozen=True)
class ResolvedMapping:
    """Final per-import mapping from the matched format or auto-detection.

    Both the matched-format branch and the auto-detect branch in
    ``_import_tabular`` produce one of these. Downstream code reads from
    the instance instead of six unpacked local variables.
    """

    field_mapping: dict[str, str]
    date_format: str
    sign_convention: SignConventionType
    number_format: NumberFormatType
    is_multi_account: bool
    confidence: str


@dataclass(frozen=True)
class ReviewedTabularPlan:
    """Complete normalized tabular parse and mapping reviewed at preview."""

    file_type: str
    delimiter: str | None
    encoding: str
    file_size: int
    field_mapping: dict[str, str]
    date_format: str | None
    """None when the detector could not read the date column. Never a
    fabricated default — that replays as a silent zero-row import."""
    sign_convention: SignConventionType
    number_format: NumberFormatType
    is_multi_account: bool
    confidence: str
    skip_rows: int
    has_header: bool
    rows_in_file: int
    rows_skipped_trailing: int
    header_row_looks_like_data: bool
    header_signature: list[str]
    flagged_fields: list[str]
    """Fields the preview flagged as weakly matched. Required, not defaulted:
    an absent value re-scores as a clean mapping, which is how a flagged 0.85
    plan came to report score=1.0 beside its own "low" tier."""

    def to_dict(self) -> dict[str, Any]:
        """Return the canonical JSON-ready representation."""
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ReviewedTabularPlan":
        """Validate and rebuild a persisted reviewed plan."""
        from pydantic import TypeAdapter

        return TypeAdapter(cls).validate_python(value)


def _validate_date_format_override(
    df: Any,
    field_mapping: dict[str, str],
    date_format_override: str | None,
) -> None:
    """Refuse a caller date format that cannot read the mapped date column.

    An unreadable strptime string is not a slow failure: the transform drops
    every row it cannot parse and the import reports success with
    rows_loaded=0. Called from two points because it must run *before* any
    success metric on whichever branch reaches it — the first-contact branch
    records an accepted/overridden confirmation before the branches converge,
    and on the CLI path `observations` is None, so that counter applies
    immediately and no rollback undoes it.

    Reads the WHOLE column, not collect_samples' 20-row head: a sample answers
    "what does this look like", this answers "does this format read the
    column", and a dirty prefix must not refuse a file whose remaining rows
    parse.
    """
    if date_format_override is None:
        return
    import polars as pl

    from moneybin.extractors.tabular.date_detection import format_parses

    date_column = field_mapping.get("transaction_date")
    if date_column is None or date_column not in df.columns:
        return
    if format_parses(df[date_column].cast(pl.Utf8).to_list(), date_format_override):
        return
    raise UserError(
        f"Date format {date_format_override!r} could not read the "
        f"{date_column!r} column. Importing with it would drop most rows. "
        "Check the format against the column's own values.",
        code=error_codes.IMPORT_INVALID_DATE_FORMAT,
    )


# What each channel's import path can actually forward to the resolver.
# ``account_bindings`` is absent by design: every channel honors it, because it
# is the answer to the account gate they all raise.
_HONORED_ACCOUNT_SIGNALS: dict[str, frozenset[str]] = {
    "tabular": frozenset({"account_id", "account_name", "account_metadata"}),
    # OFX names its own accounts (``<ACCTID>``) and a file can carry several,
    # so a single whole-file pin has no coherent target.
    "ofx": frozenset(),
    # A PDF is one statement, so a single pin does have a target; the tabular
    # naming arguments still bottom out in ``_import_tabular`` only.
    "pdf": frozenset({"account_id"}),
}


def channel_honors_account_name(channel: str) -> bool:
    """Whether this channel's import path forwards ``account_name``.

    The channel-keyed form of :func:`honors_account_name`, for the callers that
    already know the channel and have no file to sniff — the inbox sidecar
    decides which account recoveries to print from the pending row's own
    ``channel``. Spelling that as ``channel == "tabular"`` would put a second
    copy of the table beside this one, and the next channel to gain
    ``account_name`` would have to remember both.
    """
    return "account_name" in _HONORED_ACCOUNT_SIGNALS.get(channel, frozenset())


def honors_account_name(file_path: Path) -> bool:
    """Whether this file's channel forwards ``account_name`` to the resolver.

    For a caller-supplied name the answer is enforced by
    :func:`reject_unhonored_account_signals` — passing one to a channel that
    would discard it is an error worth stopping for. This is the question its
    *other* caller has to ask first: the inbox's ``inbox/<account-slug>/``
    layout is a filing convention the user never passed as a signal, so a
    folder name must not fail an import the way a wrong flag should. Reads the
    same table the refusal reads, so the two cannot drift.

    False for a file no channel claims: ``import_file`` raises on it anyway,
    with a better message than this helper could give.
    """
    try:
        file_type = _detect_file_type(file_path)
    except ValueError:
        return False
    return channel_honors_account_name(file_type)


def reject_unhonored_account_signals(
    file_type: str,
    *,
    account_id: str | None = None,
    account_name: str | None = None,
    account_metadata: dict[str, dict[str, str]] | None = None,
) -> None:
    """Refuse an account signal this channel's import path would discard.

    These arguments used to be accepted and dropped on the channels that never
    forwarded them, which is the failure that cannot be noticed: the import
    binds whatever the extractor inferred while the caller believes they chose,
    and nothing at the call site suggests looking. A wrong account is expensive
    to find later and expensive to undo.

    Refuses on the first unhonored signal rather than reporting all of them —
    the caller has to fix one to get to the next, and a channel table beats a
    list of names for understanding why.
    """
    honored = _HONORED_ACCOUNT_SIGNALS.get(file_type)
    if honored is None:  # pragma: no cover — _detect_file_type raised already
        return
    supplied = (
        ("account_id", account_id),
        ("account_name", account_name),
        ("account_metadata", account_metadata),
    )
    unhonored = next(
        (name for name, value in supplied if value and name not in honored), None
    )
    if unhonored is None:
        return
    accepted = ", ".join(sorted(honored)) or "none"
    # Names both spellings: this is the one call site the CLI and the MCP tool
    # share, and a CLI user has no parameter called `account_bindings` any more
    # than an agent has a `--account-binding` flag. A refusal that names the
    # other surface's vocabulary is not actionable on the one that raised it.
    raise UserError(
        f"{unhonored} is not supported for a {file_type} import — this channel "
        f"accepts {accepted}. Name the account per detected source account with "
        "an account binding instead (--account-binding on the CLI, "
        "account_bindings via MCP); the import stops and lists them when it "
        "cannot resolve one on its own.",
        code=error_codes.IMPORT_ACCOUNT_SIGNAL_UNSUPPORTED,
    )


def _validate_explicit_tabular_sign_shape(
    field_mapping: dict[str, str],
    sign: SignConventionType,
) -> None:
    """Reject an explicit sign convention that cannot read the mapped columns."""
    has_split_amount = (
        "debit_amount" in field_mapping and "credit_amount" in field_mapping
    )
    if sign == "split_debit_credit" and not has_split_amount:
        raise UserError(
            "Sign convention 'split_debit_credit' does not fit this file's "
            "columns: the mapping resolves a single amount column, which this "
            "convention does not read. Re-run with --sign negative_is_expense "
            "or --sign negative_is_income, or map both debit_amount and "
            "credit_amount; nothing was imported.",
            code=error_codes.IMPORT_INVALID_SIGN_CONVENTION,
        )
    if sign != "split_debit_credit" and has_split_amount:
        raise UserError(
            f"Sign convention {sign!r} does not fit this file's columns: the "
            "mapping resolves a debit/credit pair, which this convention does "
            "not read. Re-run with --sign split_debit_credit, or map one amount "
            "column; nothing was imported.",
            code=error_codes.IMPORT_INVALID_SIGN_CONVENTION,
        )


def per_file_failure(
    exc: Exception,
) -> tuple[str, str | None, str | None, dict[str, Any] | None]:
    """Return (error_message, error_code, hint, details) for a PerFileResult.

    Public because the single-file MCP path builds its own PerFileResult and
    must reach the same verdict this module's batch loop does.

    Classified errors carry their sanitized message, code, recovery hint, and
    structured `details`. Unclassified ones fall back to the class name with
    nothing else — raw str(e) may embed PII (see extractors/ofx/extractor.py),
    and there is no advice to give for an exception we didn't recognize.

    `details` is the fourth element rather than dropped because it is what an
    agent branches on: a permission failure carries `errno`, `platform`, and
    (on the macOS branch) `protected_root`. Returning only the hint would force
    callers to string-match prose to recover facts the classifier already knew
    — the exact pattern `error_code` exists to replace.
    """
    classified = classify_user_error(exc)
    if classified is None:
        return type(exc).__name__, None, None, None
    return classified.message, classified.code, classified.hint, classified.details


def _display_label(file_type: str, file_path: Path) -> str:
    """User-facing label for a detected file type.

    ``"tabular"`` is an internal bucket (CSV/TSV/XLSX/Parquet/Feather all
    share one pipeline). Resolve it to the file's actual extension so the
    user sees ``CSV`` / ``XLSX`` / ``OFX`` instead of ``TABULAR``.
    """
    if file_type == "tabular":
        return file_path.suffix.lstrip(".").upper() or "TABULAR"
    return file_type.upper()


def label_account_key(account_name: str) -> str:
    """A native key for an account label, guaranteed non-empty.

    ``slugify`` keeps only ``[a-z0-9]``, so a name written in a non-Latin
    script — or in punctuation alone — slugifies to ``""``. An empty string is
    not a key: every such account lands on the same ``source_native``
    coordinates, so the second one is refused as a contradicting binding and
    its statement never imports at all.

    The fallback digests the label rather than the file, so one name keeps one
    key across files and across pinned and unpinned imports — the property the
    slug already had for names the slug survives.
    """
    from moneybin.utils import slugify  # noqa: PLC0415 — matches the call sites

    slug = slugify(account_name)
    if slug:
        return slug
    digest = hashlib.sha256(account_name.encode("utf-8")).hexdigest()
    return f"label-{digest[:12]}"


def _reusable_pinned_keys(
    resolver: AccountResolver,
    *,
    account_id: str,
    source_type: str,
    source_origin: str,
) -> list[str]:
    """Native keys a pinned import may reuse for this account and source.

    The lookup behind both pinned channels, so tabular and PDF answer "what does
    this account already call itself here?" identically and only their policy on
    a non-singleton answer differs.

    Residue is filtered out: a pre-fix pin left the canonical id sitting in the
    source-key column, and reusing THAT would write it straight back into raw —
    the defect this rule exists to remove, not a key to adopt.
    """
    candidates = resolver.accepted_native_keys_for_account(
        account_id=account_id,
        source_type=source_type,
        source_origin=source_origin,
    )
    residue = resolver.account_ids_ever_self_mapped(
        candidates, source_type=source_type, source_origin=source_origin
    )
    return [key for key in candidates if key not in residue]


def _bare_account_key(
    file_path: Path,
    *,
    source_bytes: bytes | None = None,
) -> str:
    """Stable, content-unique source key for a single-account file with no caller-supplied identity.

    A filename stem alone is too incidental to be a source identity — two
    different-account files that share a name (two banks' ``statement.csv``)
    would collide on the same ``source_native`` ref and silently merge
    (``account-identity-resolution.md``, Decision 8 corollary). Binding the key
    to file content makes it unique per file while staying stable across the
    confirm round-trip (same bytes → same key) and idempotent on an exact
    re-import. The digest is a disambiguator, NOT an identity claim.
    """
    from moneybin.utils import slugify  # noqa: PLC0415 — matches _pdf_alias

    content = file_path.read_bytes() if source_bytes is None else source_bytes
    digest = hashlib.sha256(content).hexdigest()[:12]
    return f"{slugify(file_path.stem) or 'file'}-{digest}"


def rekey_bare_proposals_for_path(
    account_proposals: list[AccountProposalDict], moved_path: Path
) -> None:
    """Repoint bare content-keyed proposals to ``moved_path``'s key, in place.

    The inbox may append a collision suffix when moving a pending file
    (``statement.csv`` → ``statement-1.csv``), changing the stem *after* the
    ``account_confirmation`` proposal was built from the original name. The
    sidecar's ``--account-binding`` command must use the key that
    ``import confirm <moved_path>`` will recompute, so repoint any proposal whose
    key is this file's bare content key (its digest suffix matches the moved
    bytes); real, data-derived source keys are left untouched. A no-op when the
    stem did not change.
    """
    digest = hashlib.sha256(moved_path.read_bytes()).hexdigest()[:12]
    new_key = _bare_account_key(moved_path)
    for proposal in account_proposals:
        if str(proposal.get("source_account_key", "")).endswith(f"-{digest}"):
            proposal["source_account_key"] = new_key


def _pdf_alias(file_path: Path) -> str:
    """Resolve the seed alias from the file stem.

    Returns a slug used in ``raw.pdf_<alias>`` view names. The ``pdf_``
    prefix is added by the view-name construction, so the alias itself can
    start with any character (including digits) — the view regex sees
    ``pdf_{alias}``, not just ``{alias}``.

    Capped at 59 chars so the ``pdf_{alias}`` view name fits the shared
    builder's 63-char limit. When truncation would silently merge distinct
    long filenames (two PDFs whose slugified stems share the first 59
    chars), a 4-char content-hash suffix preserves uniqueness within the
    same ceiling.
    """
    import hashlib

    from moneybin.utils import slugify

    slug = slugify(file_path.stem).replace("-", "_")
    if not slug:
        slug = "import"
    if len(slug) > 59:
        suffix = hashlib.sha256(slug.encode()).hexdigest()[:4]
        slug = f"{slug[:54]}_{suffix}"
    return slug


def _pdf_format_name(fp: dict[str, Any]) -> str:
    """Deterministic first-contact format name: issuer slug + fingerprint hash.

    Single source of truth for the ``app.pdf_formats.name`` of an auto-derived
    or bridge-authored recipe on first contact. Both ``_import_pdf_transactions``
    (deterministic) and ``apply_pdf_bridge_response`` (bridge) derive the name
    this way — the hash is built from ``serialize_fingerprint(fp)`` so it stays
    byte-for-byte identical to the JSON the repo stores and looks up by; any
    drift between call sites would silently break duplicate detection.
    """
    from moneybin.extractors.pdf.fingerprint import serialize_fingerprint
    from moneybin.utils import slugify

    issuer_slug = slugify(fp.get("issuer", "unknown"))
    digest = hashlib.sha256(serialize_fingerprint(fp).encode()).hexdigest()[:12]
    return f"{issuer_slug}_{digest}"


def _pdf_account_type(decision: "RouteDecision") -> str | None:
    """The account_type a PDF import stamps on ``raw.tabular_accounts``.

    A ``negative_is_income`` recipe carries a "this is a credit card" verdict:
    either human-confirmed on the deterministic rung (the ``--confirm`` sign
    gate) or agent-authored via the bridge recipe, which reaches
    ``_import_pdf_transactions`` through ``apply_pdf_bridge_response`` →
    ``route_forced_recipe`` and does NOT run the sign gate. Either way
    ``credit`` follows from the recipe's own convention — a fact about the
    account, not a guess. prep normalizes it through ``seeds.account_type_map``
    like every other source's spelling.

    Tolerates a missing recipe rather than asserting one: ``_pdf_source_account``
    also calls this, and it runs on a decision whose recipe the caller may not
    have narrowed yet. A recipe-less decision has stated no convention, so the
    document's own captured type is the answer.

    It does NOT drive liability signing, despite the shared word: PDF balances
    reach ``core.fct_balances`` through the tabular_balances CTE, which applies
    no type-based negation at all (the ``IN ('credit','loan')`` negation is
    scoped to plaid_balances). This value feeds ``display_name`` and the
    ``accounts --type`` filter — which is why the mint report reads it here too,
    from the one expression, rather than deriving a second answer.
    """
    if decision.recipe is not None and (
        decision.recipe.sign_convention == "negative_is_income"
    ):
        return "credit"
    return decision.metadata.account_type


def _to_account_number_mask(raw: str | None) -> str | None:
    """Reduce a captured PDF account identifier to a last-4 display mask.

    Statement layouts emit account identifiers in several forms:

      ``Account Number: 123456789``  → raw = "123456789"  → ``"****6789"``
      ``Account ending in 1234``     → raw = "1234"       → ``"****1234"``
      ``Account Number: ****1234``   → raw = "****1234"   → ``"****1234"``

    The ``raw.tabular_accounts.account_number_masked`` column is contract-
    defined as a last-4 display mask. Storing the full captured token there
    would leak a real institution account number into a column that downstream
    consumers treat as already masked. Apply the reduction at the import
    boundary so the raw schema's privacy contract is preserved.

    Normalisation is load-bearing for privacy and partial-evidence consistency,
    not PDF source-native identity. The output populates the masked raw-account
    field and candidate display; PDF identity is derived separately from the
    document digest and usable statement evidence.

    Returns the original string when fewer than 4 digits are present (e.g. an
    institution-specific token, or a fully-masked "xxxx") so we never silently
    drop a captured value — and never fabricate a short "last 4" that would
    look authoritative to the institution+last4 merge signal.
    """
    if raw is None:
        return None
    stripped = raw.strip()
    if not stripped:
        return None
    digits = "".join(c for c in stripped if c.isdigit())
    if len(digits) < 4:
        return stripped
    return f"****{digits[-4:]}"


def _last4_from_account_number(value: object) -> str | None:
    """Last 4 digits of a mapped account-number column value, else None.

    The account-number column holds the real (or already-masked) number, so its
    trailing 4 digits are an authoritative last4 — used as a fallback when the
    display label carries none. Distinct from ``parse_account_label``, which only
    trusts a recognized last-4 *pattern* in a free-text display name. Tabular
    columns are read as strings (``infer_schema_length=0``), so no float coercion.
    """
    if value is None:
        return None
    digits = "".join(c for c in str(value) if c.isdigit())
    return digits[-4:] if len(digits) >= 4 else None


def _normalize_pdf_amount(row: dict[str, Any], sign_convention: str) -> Decimal:
    """Return one PDF row's canonical amount before or during loading."""
    zero = Decimal("0")
    if sign_convention == "split_debit_credit":
        return Decimal(str(row.get("credit", zero))) - Decimal(
            str(row.get("debit", zero))
        )
    amount = Decimal(str(row.get("amount", zero)))
    return -amount if sign_convention == "negative_is_income" else amount


def _incoming_pdf_transactions(
    decision: "RouteDecision",
) -> tuple[IncomingTransaction, ...]:
    """Normalize routed PDF rows for pre-load candidate evidence."""
    if decision.recipe is None:
        return ()
    transactions: list[IncomingTransaction] = []
    for row in decision.rows:
        transaction_date = row.get("date")
        if not isinstance(transaction_date, date):
            continue
        currency = decision.metadata.currency_code
        transactions.append(
            IncomingTransaction(
                transaction_date=transaction_date,
                amount=_normalize_pdf_amount(row, decision.recipe.sign_convention),
                currency_code=str(currency) if currency is not None else None,
            )
        )
    return tuple(transactions)


# Unambiguous tabular extensions: extension wins, no OFX sniffing attempted.
# (.txt / .dat are excluded because they're generic and may contain OFX content.)
_UNAMBIGUOUS_TABULAR: frozenset[str] = frozenset({
    ".csv",
    ".tsv",
    ".tab",
    ".xlsx",
    ".xls",
    ".parquet",
    ".pq",
    ".feather",
    ".arrow",
    ".ipc",
})


def _detect_file_type(file_path: Path) -> str:
    """Detect file type from extension, falling back to magic-byte sniffing.

    Returns:
        File type string: 'ofx', 'pdf', or 'tabular'.

    Raises:
        ValueError: If the file cannot be classified.
    """
    from moneybin.extractors.tabular.format_detector import TABULAR_EXTENSIONS

    suffix = file_path.suffix.lower()
    if suffix in (".ofx", ".qfx", ".qbo"):
        return "ofx"
    if suffix in _UNAMBIGUOUS_TABULAR:
        return "tabular"

    # Magic-byte sniff wins over ambiguous extensions — a .pdf-named file
    # carrying OFX content gets the clear "ofx" route instead of an opaque
    # pdfplumber error downstream.
    if _sniff_ofx_content(file_path):
        return "ofx"

    if suffix == ".pdf":
        return "pdf"
    if suffix in TABULAR_EXTENSIONS:
        return "tabular"

    raise ValueError(
        f"Unsupported file type: {suffix}. "
        f"Supported: .ofx, .qfx, .qbo, .pdf, .csv, .tsv, .xlsx, .parquet, .feather"
    )


def _sniff_ofx_content(file_path: Path) -> bool:
    """Return True if the file's first 1024 bytes look like OFX/QFX/QBO content."""
    try:
        with open(file_path, "rb") as f:
            head = f.read(1024)
    except PermissionError:
        # "Could not look" is not "is not OFX". Returning False here sends an
        # unreadable file on to the extension checks, where a missing or unknown
        # suffix reports "Unsupported file type" — blaming the file for a
        # permission problem the caller can actually fix. Let it propagate so
        # `classify_user_error` produces the permission code and its hint.
        raise
    except OSError:
        return False
    head_lstripped = head.lstrip()
    if head_lstripped.startswith(b"OFXHEADER:"):
        return True
    if head_lstripped.startswith(b"<?xml") and b"<OFX>" in head:
        return True
    return False


# Fields a caller may capture for a freshly-minted ("new"-bound) account at
# import time. Mirrors the import-time-relevant subset of app.account_settings.
_NEW_ACCOUNT_META_KEYS = frozenset({
    "display_name",
    "account_subtype",
    "last_four",
    "currency_code",
})


def _validate_account_metadata(metadata: dict[str, dict[str, str]] | None) -> None:
    """Validate account_metadata field keys + values before any DB writes.

    Runs up-front (before the Phase-3 resolve()/load writes) so an unknown key or
    a malformed value fails fast. A mid-loop raise would leave the
    ``app.account_links`` rows of already-resolved accounts orphaned with no
    import batch to revert.
    """
    if not metadata:
        return
    from moneybin.services.account_service import AccountSettings

    for meta in metadata.values():
        unknown = set(meta) - _NEW_ACCOUNT_META_KEYS
        if unknown:
            raise ValueError(
                f"Unknown account_metadata field(s): {sorted(unknown)}. "
                f"Valid: {sorted(_NEW_ACCOUNT_META_KEYS)}."
            )
        # Construct AccountSettings to trigger its __post_init__ field
        # validation (last_four 4-digits, display_name length, currency code).
        AccountSettings(
            account_id="_validate_",
            display_name=meta.get("display_name"),
            last_four=meta.get("last_four"),
            account_subtype=meta.get("account_subtype"),
            currency_code=meta.get("currency_code"),
        )


_PROPOSAL_REF_PREFIX = "@"


def proposal_ref(index: int) -> str:
    """Positional referent for the ``index``-th source account in a file.

    Every channel derives ``source_account_key`` from file content — an OFX
    ``<ACCTID>``, a PDF document digest, or a tabular account-name column — and
    that key is an ACCOUNT_IDENTIFIER, so a masking surface may hide it. A
    caller reading the gate through one of those surfaces can see the proposal
    but cannot reliably reproduce the key needed to answer it.

    This names the account by position instead, which discloses nothing and is
    reproducible from the same bytes on the answering call. It is a referent for
    one exchange, not an identifier: MoneyBin already knows which account is
    which, so the caller only has to point at one of the ones it just listed.

    ``@`` rather than ``#`` because a binding is typed at a shell prompt —
    ``--account-binding #0=new`` starts a comment and drops the rest of the
    line.
    """
    return f"{_PROPOSAL_REF_PREFIX}{index}"


def is_proposal_ref(key: str) -> bool:
    """Whether a caller's binding/metadata key is a positional ref, not a raw key.

    One definition, because two callers ask for opposite reasons and a
    disagreement between them is a leak either way: ``_resolve_binding_targets``
    asks so it can resolve refs against positions, and the CLI's sign recovery
    asks so it can print refs and drop raw keys — that path has no proposals to
    re-key from, and a raw ``source_account_key`` in ``actions[]`` sits outside
    the redaction walk.
    """
    return key.startswith(_PROPOSAL_REF_PREFIX)


def _resolve_binding_targets(
    source_accounts: list[SourceAccount], bindings: dict[str, str]
) -> list[str | None]:
    """Per-source-account binding target, accepting a raw key or a positional ref.

    Raises when the two forms name one account with different targets: choosing
    a winner would bind an account to one of two ids the caller asked for, and
    the caller would never learn which — the unrecoverable-by-surprise merge
    this gate exists to prevent.

    Also raises on a key — ref or raw — that names no account in this file.
    Left unchecked either reads as "no binding for this account", so the import
    re-gates with nothing to distinguish a mistyped answer from no answer, and
    the caller re-sends the same binding forever.

    The message never names the file's own source keys. An OFX ``<ACCTID>`` is
    an account number, and this ValueError reaches an MCP caller through
    ``per_file_failure`` — echoing the caller's own unknown keys back is safe
    (they sent them), listing the real ones is not.

    A real source key wins over the positional vocabulary it happens to look
    like, but only where the two readings name the same account.
    ``source_account_key`` is untrusted file content on OFX (the ``<ACCTID>``
    verbatim), so a key of "@0" would otherwise bind both its own account AND
    proposal zero — one answer silently merging two accounts. When the readings
    name *different* accounts the key is refused instead of resolved: picking
    either one delivers the answer to an account the caller was not looking at
    and leaves the other gated behind a ref that no longer reaches it.
    """
    known = {src.source_account_key for src in source_accounts}
    refs = {key for key in bindings if is_proposal_ref(key) and key not in known}
    valid = {proposal_ref(index) for index in range(len(source_accounts))}
    # A key that is BOTH a real source key and a valid ref for a *different*
    # position has no safe reading. Source-key-wins (below) would answer the
    # account whose key it spells while the caller was looking at the other
    # account's ref — and through a masking surface that ref is the only
    # referent they can reproduce, so the miss is invisible and unrecoverable.
    # Same index is not ambiguous: both readings name the one account.
    for index, src in enumerate(source_accounts):
        key = src.source_account_key
        if key in bindings and key in valid and key != proposal_ref(index):
            raise ValueError(
                f"account_bindings key {key!r} is ambiguous for this file: it is "
                f"the source key for {proposal_ref(index)} and the positional ref "
                f"for {key}. Bind {proposal_ref(index)} by its own ref, and the "
                "other account by its source key — read it from the file itself "
                "(the OFX <ACCTID>, or the account column's value), because the "
                "confirmation masks it."
            )
    if unknown := sorted((refs - valid) | (set(bindings) - refs - known)):
        raise ValueError(
            f"account_bindings references unknown source key(s): "
            f"{_mask_caller_keys(unknown)}. "
            f"This file has {len(source_accounts)} account(s) — bind by "
            f"proposal_ref ({', '.join(sorted(valid)) or 'none'}), or by a "
            "source key exactly as the confirmation reported it."
        )
    targets: list[str | None] = []
    for index, src in enumerate(source_accounts):
        by_key = bindings.get(src.source_account_key)
        ref = proposal_ref(index)
        by_ref = None if ref in known else bindings.get(ref)
        if by_key is not None and by_ref is not None and by_key != by_ref:
            raise ValueError(
                f"account_bindings has conflicting values for the same account: "
                f"{proposal_ref(index)}={by_ref!r} and its source key "
                f"={by_key!r}. Send one."
            )
        targets.append(by_key if by_key is not None else by_ref)
    return targets


def _resolve_metadata_keys(
    source_accounts: list[SourceAccount], metadata: dict[str, dict[str, str]]
) -> dict[str, dict[str, str]]:
    """Re-key caller metadata from positional refs onto source keys.

    ``account_bindings`` has taken a ``proposal_ref`` since this gate shipped;
    ``account_metadata`` compared against source keys alone, and the
    confirmation masks ``source_account_key`` — so its refusal asked for a key
    the caller could not read, and naming a newly minted account was unusable on
    the masked path. One vocabulary answers the gate, whichever parameter it
    arrives in.

    Mirrors :func:`_resolve_binding_targets`: a real source key wins over the
    ref it happens to spell, and an unrecognized key is passed through for the
    caller's own unknown-key refusal to name (echoing what they sent is safe;
    enumerating the file's real keys is not).
    """
    if not metadata:
        return metadata
    known = {src.source_account_key for src in source_accounts}
    source_key_by_ref = {
        proposal_ref(index): src.source_account_key
        for index, src in enumerate(source_accounts)
    }
    resolved: dict[str, dict[str, str]] = {}
    for key, fields in metadata.items():
        target = key if key in known else source_key_by_ref.get(key, key)
        if target in resolved:
            # The ref and the source key for one account, sent together. Same
            # answer-twice problem the binding half refuses: picking a winner
            # would apply one of two metadata sets the caller asked for and
            # never say which.
            raise ValueError(
                f"account_metadata names the same account twice — "
                f"{_mask_caller_keys([key])} and its other referent both "
                "resolve to one account. Send one."
            )
        resolved[target] = fields
    return resolved


def _ratified_binding_refs(targets: list[str | None]) -> dict[str, str]:
    """The caller's answers, re-keyed from whatever they sent to positional refs.

    A surface replaying these must not replay the caller's own keys: on OFX a
    key is the ``<ACCTID>``, and the CLI renders the replay into ``actions[]``,
    which sits outside the redaction walk. Re-keying has to happen here because
    a bound account is skipped by :meth:`_gate_account_proposals` and so never
    reaches ``account_proposals`` for a surface to look its ref up in.

    Takes the resolved targets rather than re-deriving them, so the refs cannot
    disagree with the bindings that were actually applied.

    Total by construction, so a surface can drop the caller's dict entirely:
    :func:`_resolve_binding_targets` raises on a key naming no account in this
    file, and any account a binding names is bound, so every surviving entry
    lands here.
    """
    return {
        proposal_ref(index): target
        for index, target in enumerate(targets)
        if target is not None
    }


def _apply_account_bindings(
    source_accounts: list[SourceAccount], bindings: dict[str, str]
) -> tuple[list[SourceAccount], list[str | None]]:
    """Fold each source account's binding into the resolver's input fields.

    Returns the bound accounts and the per-account targets it resolved to reach
    them. Handing the targets back is what lets :func:`_ratified_binding_refs`
    re-key the same answers without resolving them a second time — two
    resolutions of one input could only ever agree or be a bug.

    A binding value of ``"new"`` sets ``force_standalone`` (mint a fresh
    account, skip the merge-candidate pass); any other value is an existing
    canonical ``account_id`` to adopt (``explicit_account_id``). Unbound
    accounts pass through unchanged so the gate can still surface them.

    A binding is keyed by either the raw ``source_account_key`` or the
    positional ``proposal_ref`` the gate surfaced — see :func:`proposal_ref`.

    Raises ``ValueError`` on an empty binding value — ``explicit_account_id=""``
    is falsy and would silently fall through to a fresh mint as if no binding
    were given, discarding the caller's intent ("magic stays visible"). That
    message names the positional ref, never the source key, for the reason
    :func:`_resolve_binding_targets` documents: on OFX the key is an account
    number and this ValueError reaches an MCP caller intact.
    """
    if not bindings:
        return source_accounts, [None] * len(source_accounts)
    targets = _resolve_binding_targets(source_accounts, bindings)
    bound: list[SourceAccount] = []
    for index, (src, target) in enumerate(zip(source_accounts, targets, strict=True)):
        if target is None:
            bound.append(src)
            continue
        if not target.strip():
            # Reject whitespace-only too: CLI input is not stripped (_parse_kv
            # keeps the raw value) and MCP passes JSON as-is, so a bare-spaces
            # value would otherwise be truthy and bind a bogus account_id.
            # Checked before the pin conflict below so a blank value is reported
            # as blank rather than as a contradiction of some other id.
            raise ValueError(
                f"account_bindings entry for {proposal_ref(index)} has an empty "
                'value; use an existing account_id or "new".'
            )
        if target.strip().lower() == "new" and target != "new":
            # "New", "NEW", "new " all fall through to the adopt branch below
            # and become an explicit_account_id. The unknown-target refusal
            # catches them, but in the vocabulary of ids rather than of the
            # keyword the caller plainly meant. Folding case and surrounding
            # space into the keyword instead would be a guess: an account_id is
            # opaque, so "New" cannot be proven to be the keyword rather than
            # an id ("magic stays visible"). Name the near miss and stop.
            raise ValueError(
                f"account_bindings entry for {proposal_ref(index)} is {target!r}; "
                'the mint keyword is exactly "new" (lowercase, no surrounding '
                "spaces). Every other value is read as an existing account_id."
            )
        if src.explicit_account_id and target != src.explicit_account_id:
            # A caller-supplied account_id already answered this account. The
            # binding used to overwrite it (or clear it, on "new") with nothing
            # said, so the pin the caller asked for vanished — the same
            # two-answers-one-account conflict _resolve_binding_targets refuses,
            # arriving through two parameters instead of one. Restating the same
            # id is agreement, not conflict: an agent answering a gate re-sends
            # what it already had.
            raise ValueError(
                f"account_bindings binds {proposal_ref(index)} to {target!r}, "
                f"but account_id pinned {src.explicit_account_id!r}. Send one."
            )
        if target == "new":
            bound.append(
                dataclasses.replace(
                    src, force_standalone=True, explicit_account_id=None
                )
            )
        else:
            bound.append(dataclasses.replace(src, explicit_account_id=target))
    return bound, targets


def _refuse_unknown_binding_targets(
    resolver: AccountResolver,
    source_accounts: list[SourceAccount],
    bindings: dict[str, str],
) -> None:
    """Reject a binding onto an account id this database does not have.

    Step 0 of the ladder adopts ``explicit_account_id`` verbatim and reports
    ``outcome="adopted_strong"``, ``is_new=False`` — so an id naming nothing
    became a canonical account with no mint announced and no ``accounts_created``
    entry carrying it. The statement's rows then landed under an account the
    caller invented by typo, under a name no surface would ever show them. That
    is what the shared reference-resolution contract exists to prevent: a write
    never invents its own target (``.claude/rules/mcp.md``, "Entity resolution").

    Scoped to values that arrived through ``account_bindings``, which is why it
    reads them rather than every ``explicit_account_id``. A binding answers a
    confirmation that *just enumerated* the ids worth naming, so one that
    matches none of them is a typo by construction. The ``account_id``
    parameter is a different contract: it names the account this file becomes,
    minting under that id when it is unknown, and validating it would delete
    that capability rather than protect it.

    Runs before the contradiction refusal below: when a binding is both unknown
    and contradicting, "no such account" is the more useful answer.
    """
    bound_ids = {value for value in bindings.values() if value != "new"}
    if not bound_ids:
        return
    for index, src in enumerate(source_accounts):
        account_id = src.explicit_account_id
        if not account_id or account_id not in bound_ids:
            continue
        if resolver.knows_account_id(account_id):
            continue
        # Names the positional ref, never source_account_key, for the reason
        # _apply_account_bindings documents: this reaches an MCP caller intact.
        # The id itself is echoed back because the caller is the one who sent it.
        raise ValueError(
            f"account_bindings binds {proposal_ref(index)} to {account_id!r}, "
            "which is not an account in this database. Use an id the "
            'confirmation offered as a candidate, or "new" to mint one.'
        )


def _refuse_contradicted_bindings(
    resolver: AccountResolver,
    source_accounts: list[SourceAccount],
    binding_targets: list[str | None],
) -> None:
    """Reject a binding the resolver would later refuse, before anything loads.

    A bound account skips the proposal loop, so it used to reach ``resolve()``
    unchecked — and on OFX that happens *after* the raw frames are ingested.
    ``_write_native_mapping``'s conflict guard then raised with the rows already
    written and nothing to remove them: the batch finalized ``failed``, but
    ``prep.stg_ofx__transactions`` filters on ``_row_num``, not import status,
    so the statement still surfaced — joined to the very account the binding was
    trying to move it away from. Asking here makes the refusal cost nothing,
    and gives the caller a message naming what to send instead.

    Only ``explicit_account_id`` is checked. A ``force_standalone`` ("new")
    binding over an existing native key adopts at the ladder's strong-ref step
    instead of minting, which is the documented re-import idempotency, not a
    contradiction.

    ``binding_targets`` says which parameter supplied each id — non-None for an
    ``account_bindings`` answer, None for the direct ``account_id``. The message
    names it, because a refusal that cites an argument the caller never sent
    sends them looking for it instead of at the one they have to change.
    """
    for index, src in enumerate(source_accounts):
        if not src.explicit_account_id:
            continue
        existing = resolver.accepted_native_account_id(src)
        if existing is None or existing == src.explicit_account_id:
            continue
        # Names the positional ref, never source_account_key, for the reason
        # _apply_account_bindings documents: this reaches an MCP caller intact.
        supplied_by = (
            f"account_bindings binds {proposal_ref(index)}"
            if binding_targets[index] is not None
            else f"account_id pins {proposal_ref(index)}"
        )
        # Masked for the reason _pinned_native_key's refusal is, and one more:
        # `existing` is not even caller input. The caller never typed it, so
        # echoing it verbatim discloses an id they had not seen — and neither id
        # is guaranteed to be a minted surrogate, because
        # stg_tabular__transactions falls back to the source-native key when
        # nothing resolves. Masked, not dropped: the caller still has to learn
        # which account to bind to instead.
        masked_existing = _mask_caller_keys([existing])
        raise ValueError(
            f"{supplied_by} to {_mask_caller_keys([src.explicit_account_id])}, "
            f"but this file's account is already accepted onto "
            f"{masked_existing}. Bind it to {masked_existing}, or re-point the "
            "existing link first."
        )


class PdfAccountIdentity(NamedTuple):
    """What a PDF statement says about its account, and whether it said anything.

    ``identity_unknown`` is returned beside the account rather than derived again
    at each call site: the gate and the resolve pass have to agree about whether
    the file stated an identity, and re-testing the anchor separately is exactly
    the drift ``_pdf_source_account``'s own contract rules out.
    """

    source: SourceAccount
    identity_unknown: bool

    @property
    def fallback_keys(self) -> tuple[str, ...]:
        """The gate's ``fallback_keys`` argument for this identity."""
        return (self.source.source_account_key,) if self.identity_unknown else ()


def _pdf_source_account(
    decision: "RouteDecision",
    *,
    resolver: AccountResolver,
    resolved_alias: str,
    account_id_override: str | None,
    document_sha256: str,
    source_file: str | None = None,
) -> PdfAccountIdentity:
    """Derive the account identity a PDF statement presents, without resolving.

    Shared by the confirm gate (which runs before ``begin_import``) and the
    resolve pass in ``_import_pdf_transactions``, so the identity the user
    ratifies is exactly the one bound.

    Every PDF gets a document-content ``source_native`` key. A complete captured
    identifier separately becomes a validated-routing-scoped ``full_number``
    strong ref inside the encrypted database; a masked, last-four-only, or
    issuer-only value remains weak evidence. This prevents two
    same-issuer/same-last-four accounts from sharing a native key while
    preserving exact-file re-import idempotency.

    A statement with no readable account number has no account identity of its
    own. Its document key still makes the file idempotent, while
    ``identity_unknown`` sends it through the gate's fallback pick-list.
    """
    from moneybin.services.pdf_account_identity import derive_pdf_account_identity
    from moneybin.utils import slugify

    if decision.fp is None:
        # Defensive: route_pdf_import attaches fp on every outcome that reaches
        # the transactions path; this guards a hand-built RouteDecision.
        raise ValueError("PDF routing returned outcome='transactions' but fp is None")
    issuer = decision.fp.get("issuer", "unknown")
    derived = derive_pdf_account_identity(
        issuer=issuer,
        identifier=decision.metadata.account_id,
        document_sha256=document_sha256,
        identifier_is_complete=decision.metadata.account_id_complete,
        routing_number=decision.metadata.routing_number,
    )
    # Whether the document named an account, independent of its idempotency key.
    anchored = derived.has_usable_identifier
    derived_key = derived.source_account_key
    source = SourceAccount(
        source_type="pdf",
        source_origin=derived.source_origin,
        source_account_key=derived_key,
        account_name=(
            decision.metadata.account_label
            or decision.metadata.product_name
            or resolved_alias
        ),
        # account_label is captured from a printed "Account Name:"/"Account
        # Nickname:" line -- a label the account holder set, the PDF analogue
        # of Plaid's acc.name and a tabular --account-name. product_name is
        # the card/product's marketing name (identical across every holder of
        # that product) and resolved_alias is the filename slug; neither is
        # authored, so the flag must follow account_label specifically, not
        # merely "account_name is non-empty".
        account_name_is_user_set=decision.metadata.account_label is not None,
        account_number=derived.scoped_full_number,
        institution=issuer or None,
        # Before document keys, an anchorless PDF used its filename alias.
        # Preserve that accepted binding as review-only migration evidence.
        legacy_source_account_key=(
            derived.legacy_source_account_key
            or (resolved_alias if not anchored else None)
        ),
        legacy_source_origin=(
            derived.legacy_source_origin or (slugify(issuer) if not anchored else None)
        ),
        legacy_source_account_key_is_filename_alias=(
            derived.legacy_source_account_key is None and not anchored
        ),
        source_file=source_file,
        # None for a digits-free token ("xxxx"), which correctly denies the
        # institution+last4 signal and routes to name review rather than
        # inventing a strong match.
        last_four=derived.last_four,
        # What core.dim_accounts will name this account, built from the three
        # values _import_pdf_transactions writes to raw.tabular_accounts for it:
        # the issuer, the recipe-implied account type, and the last-4 display
        # mask. Not `derived.last_four`, which answers a different question (it
        # is None for a digits-free token so the institution+last4 match cannot
        # fire); the model reads the masked column and strips it to digits.
        name_facts=AccountNameFacts(
            institution_name=issuer or None,
            category=account_category(_pdf_account_type(decision)),
            last_four=derived_last_four(
                _to_account_number_mask(decision.metadata.account_id)
            ),
            # Same value and same condition as account_name_is_user_set below
            # -- a captured "Account Name:"/"Account Nickname:" line is the
            # only PDF-side source that counts as authored. Masked the way
            # every other display-safe label site is (mask_embedded_account_
            # number), never the raw captured text.
            source_label=(
                mask_embedded_account_number(decision.metadata.account_label)
                if decision.metadata.account_label
                else None
            ),
        ),
        explicit_account_id=account_id_override,
        # Set even when no key is borrowed below; _teach_unpinned_key ignores it
        # once it equals source_account_key.
        unpinned_account_key=derived_key if account_id_override else None,
    )
    # A pin (agents/users pointing a statement at an existing dim_accounts row)
    # says WHICH account this document belongs to. It does not change what the
    # document's own key is, so it normally travels in explicit_account_id
    # alone and the native key stays derived.
    #
    # Except that the derived key is the document's BYTES, and a bank hands out
    # a byte-different PDF for the same statement (fresh internal timestamps).
    # transaction_id folds the canonical account, which the pin holds still, so
    # a re-download that moves only the source key forks staging's
    # (transaction_id, account_id) dedup and counts the statement twice. So a
    # pin reuses the key this account already answers to — the same rule the
    # tabular channel applies, and the reason both call _reusable_pinned_keys.
    #
    # Applies whether or not the document names an account, because
    # derive_pdf_account_identity keys EVERY statement by its bytes — an
    # anchored one included — so being anchored buys no stability here. The
    # collision that document key prevents (two same-issuer/same-last-four
    # accounts sharing a key) is a question about inferred identity, and a pin
    # states the account outright, so there is nothing left to disambiguate.
    #
    # Several remembered keys take the first in the lookup's stable order
    # rather than refusing or minting. One key per adopted statement is the
    # ordinary state of any card with a history, so minting there would re-open
    # the double count for the accounts holding the most; refusing would
    # hard-fail an import the user has no --account-name to disambiguate with.
    # Which key it lands on does not matter — transaction_id already separates
    # the statements — only that both imports of one statement land on the same
    # one.
    #
    # Gated on the document's own key being unknown, because
    # _refuse_contradicted_bindings asks whether the key on THIS SourceAccount
    # is accepted elsewhere. Substituting the target's key first answers that
    # trivially and loads another account's statement here; a document that
    # already named its account keeps saying so.
    #
    # "Elsewhere" means another account, not this one. A key the pin target
    # already owns contradicts nothing — and it is the ordinary state here,
    # because the borrowed import below teaches this document's own key to the
    # target. Reading that back as a reason to stop borrowing would send the
    # NEXT import of the same regenerated statement to its own digest while the
    # previous one sits under the borrowed key, splitting one statement across
    # two keys — the exact double count the borrowing exists to prevent.
    document_owner = resolver.accepted_native_account_id(source)
    if account_id_override and document_owner in (None, account_id_override):
        reusable = _reusable_pinned_keys(
            resolver,
            account_id=account_id_override,
            source_type="pdf",
            source_origin=derived.source_origin,
        )
        if reusable:
            source = dataclasses.replace(source, source_account_key=reusable[0])
    return PdfAccountIdentity(source=source, identity_unknown=not anchored)


def _ofx_source_accounts(parsed_ofx: Any, source_origin: str) -> list[SourceAccount]:
    """Enumerate the account identities an OFX file presents, without resolving.

    Reads the parsed ofxparse object rather than the extractor's DataFrame so it
    can run *before* ``begin_import`` — the confirm gate has to stop the import
    before any batch is opened or any row is ingested.

    One list serves both the gate and the resolve pass. Deriving them separately
    would let the gate propose one identity while resolve binds another; the
    field derivation is shared with the extractor (``none_if_blank``,
    ``ofx_account_type``) for the same reason.

    Deduped by ``<ACCTID>``, because ofxparse emits one ``Account`` per statement
    response with no de-dup of its own: an export that splits one card across two
    ``<STMTRS>`` blocks would otherwise surface it as two independent identities,
    ask about each, and let two different answers write one native key under two
    canonical accounts. ACCTID alone is the right key — it *is* the
    ``source_account_key`` every downstream link and staging JOIN uses, so two
    entries sharing one cannot resolve to different accounts by design.
    """
    from moneybin.extractors.institution_resolution import (
        display_name_for_fid,
        slug_for_fid,
    )
    from moneybin.extractors.ofx.extractor import none_if_blank, ofx_account_type

    accounts: list[SourceAccount] = []
    seen: set[str] = set()
    for account in parsed_ofx.accounts:
        acctid: str | None = account.account_id
        if not acctid or acctid in seen:
            continue
        seen.add(acctid)
        routing = none_if_blank(account.routing_number)
        normalized_acctid = normalize_account_identifier(acctid)
        institution = account.institution
        fid = none_if_blank(institution.fid if institution else None)
        accounts.append(
            SourceAccount(
                source_type="ofx",
                source_origin=source_origin,
                source_account_key=acctid,
                account_name=f"{source_origin} {ofx_account_type(account) or ''}".strip(),
                # OFX has no account-name element at all (see account_label's
                # NULL arm in dim_accounts.sql) -- this is always the
                # generated institution+type fallback, never a person's own
                # label, so it must never drive the resolver's name rung.
                account_name_is_user_set=False,
                # full_number is a strong ref ONLY when institution/routing-scoped
                # (contains ':'); a bare number is demoted to a candidate signal.
                account_number=(
                    f"{routing}:{normalized_acctid}"
                    if routing and normalized_acctid
                    else None
                ),
                last_four=acctid[-4:],
                # The FID slug, not source_origin. source_origin comes from <ORG>,
                # which is a routing code for some issuers ("B1" = Chase), and it
                # must stay untouched because downstream identity keys on it.
                # Matching needs the same canonical slug
                # core.dim_accounts.institution_slug carries, so resolve it from
                # the FID and fall back to source_origin when the FID is
                # unregistered.
                institution=slug_for_fid(fid) or source_origin,
                # What core.dim_accounts will name this account: the registry's
                # display name for the FID, else the file's own <ORG> — the
                # model's COALESCE(seeds.institutions.display_name,
                # institution_org), and the extractor's `inst_org or
                # source_origin` for the raw column it reads. Not
                # `source_origin` on its own, which is a routing code ("B1" =
                # Chase); not `<ACCTTYPE>` raw, which the type map normalizes;
                # and last four by DIGITS, because the model strips non-digits
                # before taking four.
                # The <ORG> arm is deliberately not none_if_blank'd: the
                # extractor stores `inst_org or source_origin` untrimmed, so a
                # whitespace-only <ORG> is written, staging NULLIFs it, and the
                # dim falls through to the type rung. Normalizing it here would
                # reach source_origin instead and report a name the dim will
                # not store. AccountNameFacts trims what it is given.
                name_facts=AccountNameFacts(
                    institution_name=(
                        display_name_for_fid(fid)
                        or (institution.organization if institution else None)
                        or source_origin
                    ),
                    category=account_category(ofx_account_type(account)),
                    last_four=derived_last_four(acctid),
                ),
            )
        )
    return accounts


@dataclass(frozen=True, slots=True)
class RawTableStat:
    """One row of :meth:`ImportService.raw_data_summary` — per-table row count and date span."""

    schema: str
    table: str
    rows: int
    date_min: date | None
    date_max: date | None


class ImportService:
    """Orchestrates the full file import pipeline.

    Detects file type, extracts and loads to raw tables, runs SQLMesh
    transforms, applies matching, and runs deterministic categorization.
    Both CLI commands and MCP tools call this same service — no
    duplication.
    """

    def __init__(self, db: Database, *, audit: AuditService | None = None) -> None:
        """Initialize ImportService with an open Database connection.

        ``audit`` is keyword-only so existing positional callers
        (``ImportService(db)``) continue to work unchanged. Shared with
        ``ImportsRepo`` so the labels write and its audit row land in one txn.
        """
        self._db = db
        self._audit = audit if audit is not None else AuditService(db)
        self._imports = ImportsRepo(db, audit=self._audit)
        self._pdf_formats = PdfFormatsRepo(db)

    def allocate_import_log(
        self,
        *,
        source_type: str,
        format_name: str,
        actor: str,
    ) -> str:
        """Allocate a fresh ``raw.import_log`` row and return its ``import_id``.

        Thin wrapper around :func:`moneybin.loaders.import_log.begin_import`
        that exposes the lifecycle to callers (manual entry, future API
        connectors) that don't have a source file but still need an
        ``import_id`` to attribute their raw rows. ``source_type`` must be
        in the loader's allowlist (see ``REVERT_TABLES``); ``actor`` is
        recorded as the ``account_names`` payload so audit consumers can
        trace which surface (cli/mcp) initiated the batch. ``format_name``
        is folded into the synthetic ``source_file`` key alongside
        ``source_type`` and ``actor`` — callers that share ``source_type``
        (e.g. manual cash entries and manual investment events both use
        ``"manual"``) but write to different raw tables use distinct
        ``format_name`` values, so this keeps their ``source_file`` keys
        distinct too. Without it, ``revert()``'s superseded-lookup (which
        matches purely on ``source_file``) could cross-match a batch from
        an unrelated domain.
        """
        from moneybin.loaders import import_log

        return import_log.begin_import(
            self._db,
            source_file=f"<{source_type}:{format_name}:{actor}>",
            source_type=source_type,  # type: ignore[arg-type]  # runtime-validated
            source_origin=actor,
            account_names=[actor],
            format_name=format_name,
            format_source="manual",
        )

    def raw_data_summary(self) -> list[RawTableStat]:
        """Return row counts and date ranges for every ``raw.*`` table.

        Backs ``moneybin import status``. Transaction tables (name containing
        ``"transaction"``) additionally report a date range, read from
        ``date_posted`` for OFX tables and ``transaction_date`` otherwise.
        """
        from sqlglot import exp  # noqa: PLC0415

        tables = self._db.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = 'raw'
            ORDER BY table_name
        """).fetchall()

        results: list[RawTableStat] = []
        for schema, table in tables:
            safe_schema = exp.to_identifier(schema, quoted=True).sql("duckdb")  # type: ignore[reportUnknownMemberType]  # sqlglot has no stubs
            safe_table = exp.to_identifier(table, quoted=True).sql("duckdb")  # type: ignore[reportUnknownMemberType]  # sqlglot has no stubs
            row_count = self._db.execute(
                f"SELECT COUNT(*) FROM {safe_schema}.{safe_table}"  # noqa: S608 — sqlglot-quoted catalog identifiers
            ).fetchone()
            count = row_count[0] if row_count else 0

            date_min: date | None = None
            date_max: date | None = None
            if "transaction" in table:
                date_col = "date_posted" if "ofx" in table else "transaction_date"
                safe_date_col = exp.to_identifier(date_col, quoted=True).sql("duckdb")  # type: ignore[reportUnknownMemberType]  # sqlglot has no stubs
                try:
                    dates = self._db.execute(
                        f"SELECT MIN(CAST({safe_date_col} AS DATE)), MAX(CAST({safe_date_col} AS DATE)) FROM {safe_schema}.{safe_table}"  # noqa: S608 — sqlglot-quoted catalog identifiers; date_col from hardcoded map
                    ).fetchone()
                    if dates and dates[0]:
                        date_min, date_max = dates[0], dates[1]
                except Exception:  # noqa: BLE001 — date range is best-effort; any DB failure returns empty range
                    logger.debug(f"Could not get date range for {schema}.{table}")

            results.append(
                RawTableStat(
                    schema=schema,
                    table=table,
                    rows=count,
                    date_min=date_min,
                    date_max=date_max,
                )
            )
        return results

    def _query_date_range(
        self,
        table: str,
        date_expr: str,
        file_path: Path,
    ) -> str:
        """Query min/max date range for a source file from a raw table.

        Both ``table`` and ``date_expr`` are interpolated into SQL — callers
        must only pass hardcoded trusted strings, never user input.

        Args:
            table: Qualified table name (e.g. ``raw.ofx_transactions``).
            date_expr: SQL expression for the date value — may be a bare column
                name (``transaction_date``) or a cast expression
                (``CAST(date_posted AS DATE)``).
            file_path: Source file path to filter on.

        Returns:
            Date range string like ``"2024-01-01 to 2024-03-31"``, or empty
            string if unavailable.
        """
        try:
            result = self._db.execute(
                f"""
                SELECT MIN({date_expr}) AS min_date,
                       MAX({date_expr}) AS max_date
                FROM {table}
                WHERE source_file = ?
                """,  # noqa: S608 — table and date_expr are hardcoded by callers, not user input
                [str(file_path)],
            ).fetchone()
            if result and result[0]:
                return f"{result[0]} to {result[1]}"
        except Exception:  # noqa: BLE001 — date range is best-effort; any DB failure returns empty string
            logger.debug(f"Could not determine date range from {table}", exc_info=True)
        return ""

    def run_transforms(self) -> bool:
        """Apply SQLMesh transforms via :class:`TransformService`.

        Transitional shim: callers will move to ``TransformService.apply()``
        directly in a later phase. Preserves the original fail-loud contract
        — ``TransformService.apply()`` soft-fails to ``ApplyResult(error=...)``,
        but several callers here (``transactions matches run/backfill``,
        ``synthetic generate``) ignore the return value, so raising on
        failure is required to keep the exit code honest.
        """
        from moneybin.services.transform_service import TransformService

        result = TransformService(self._db).apply()
        if not result.applied:
            raise RuntimeError(f"SQLMesh transforms failed: {result.error}")
        return True

    def _import_ofx(
        self,
        file_path: Path,
        *,
        institution: str | None = None,
        force: bool = False,
        interactive: bool = False,
        account_bindings: dict[str, str] | None = None,
    ) -> ImportResult:
        """Import an OFX/QFX/QBO file via the shared import-batch pipeline.

        Args:
            file_path: Path to the file.
            institution: Override-when-missing flag — consulted only if the
                resolution chain (FI/ORG → FID lookup → filename) yields nothing.
            force: If True, allow re-importing a file that's already been imported.
                The previous batch is left in place; this creates a new batch.
            interactive: If True, prompt for institution when the chain yields
                nothing. False for --yes, MCP, and scripts.
            account_bindings: Answers to a prior account-confirmation gate, keyed
                by OFX ``<ACCTID>``: an existing account_id to adopt, or "new".

        Returns:
            ImportResult with summary.

        Raises:
            ValueError: On re-import without force, or when institution can't be derived.
            ImportConfirmationRequiredError: When an account identity in the file
                is not yet ratified. Raised before any batch is opened.
        """
        import ofxparse  # type: ignore[import-untyped]

        from moneybin.extractors.institution_resolution import (
            InstitutionResolutionError,
            resolve_institution,
        )
        from moneybin.extractors.ofx import OFXExtractor
        from moneybin.extractors.ofx.extractor import preprocess_ofx_content
        from moneybin.loaders import import_log
        from moneybin.metrics.registry import OFX_IMPORT_BATCHES

        # Canonicalize the path so relative + absolute + symlink-resolved
        # variants of the same file are detected as the same source.
        canonical_path = file_path.resolve()

        result = ImportResult(file_path=str(canonical_path), file_type="ofx")
        _t0 = time.monotonic()

        # Parse once for institution resolution; the extractor parses again
        # internally. These files are small — the duplicate parse is fine and
        # avoids leaking a parser-internal type into the extractor signature.
        # Wrap read+parse failures as ValueError so MCP's error envelope catches
        # them; otherwise OSError leaks as an internal tool error.
        # PermissionError is deliberately re-raised intact: `classify_user_error`
        # keys the `infra_permission_denied` code and its Full-Disk-Access hint
        # off the exception type, so flattening it to ValueError here would
        # downgrade a TCC/chmod denial to `infra_invalid_input` with no recovery
        # advice — the affordance would work for tabular files but not OFX.
        try:
            with open(canonical_path, "rb") as f:
                raw = f.read()
        except PermissionError:
            IMPORT_ERRORS_TOTAL.labels(source_type="ofx", error_type="read").inc()
            raise
        except OSError as e:
            IMPORT_ERRORS_TOTAL.labels(source_type="ofx", error_type="read").inc()
            raise ValueError(f"Could not read OFX file: {e}") from e
        # One read serves both identities: hashing these bytes rather than
        # reopening the path makes the digest describe exactly what gets parsed
        # below. Hash BEFORE the decode — `errors="replace"` is lossy, so a
        # digest taken from `content` would disagree with every other channel's
        # digest for the same file, and they all hash raw bytes.
        digest = source_sha256(canonical_path, raw)
        content = raw.decode("utf-8", errors="replace")
        if "�" in content:
            logger.warning(
                f"OFX file contained non-UTF-8 bytes; replaced with U+FFFD: "
                f"{canonical_path.name}"
            )

        # Re-import detection: content first, path as the fallback for batches
        # imported before file_sha256 existed. The check sits below the read
        # rather than at the top of the method because institution resolution
        # further down may *prompt*, which a file we're about to reject should
        # never trigger.
        if not force:
            existing = import_log.find_existing_import(
                self._db, str(canonical_path), file_sha256=digest
            )
            if existing:
                existing_id, existing_status = existing
                if existing_status == "importing":
                    raise ValueError(
                        f"A prior import of this file is in-progress or was "
                        f"interrupted (import_id {existing_id[:8]}..., "
                        f"status=importing). If the previous run crashed, pass "
                        f"--force to start a new batch."
                    )
                raise ValueError(
                    f"File already imported (import_id {existing_id[:8]}...). "
                    f"Use --force to re-import."
                )

        content = preprocess_ofx_content(content)
        try:
            parsed_ofx: Any = ofxparse.OfxParser.parse(  # type: ignore[reportUnknownMemberType]
                BytesIO(content.encode("utf-8"))
            )
        except Exception as e:
            IMPORT_ERRORS_TOTAL.labels(source_type="ofx", error_type="parse").inc()
            # Type name, never `e`: ofxparse exception strings can embed
            # payee/amount/memo content from the statement, and this message is
            # no longer log-only — `per_file_failure` puts the classified
            # message on the wire in `PerFileResult.error`, so interpolating
            # `e` here publishes statement contents. Matches the identical
            # guard in extractors/ofx/extractor.py. The `from e` chain keeps
            # the full detail available in a local traceback.
            raise ValueError(f"Invalid OFX file format: {type(e).__name__}") from e

        # Resolve institution (raises InstitutionResolutionError on non-interactive failure)
        try:
            source_origin = resolve_institution(
                parsed_ofx,
                file_path=canonical_path,
                cli_override=institution,
                interactive=interactive,
            )
        except InstitutionResolutionError as e:
            IMPORT_ERRORS_TOTAL.labels(
                source_type="ofx", error_type="institution_unresolved"
            ).inc()
            raise ValueError(str(e)) from e

        # Enumerate the account identities this file presents and gate on any
        # that aren't ratified yet — BEFORE begin_import, so a gated import opens
        # no batch, ingests no rows, and writes no links. An OFX <ACCTID> is a
        # stable institution-assigned key, so the answer binds for good: the next
        # import of the same account adopts via source_native without re-asking.
        resolver = AccountResolver(self._db, actor="system")
        source_accounts = self._gate_account_proposals(
            resolver,
            _ofx_source_accounts(parsed_ofx, source_origin),
            account_bindings,
            channel="ofx",
        )

        # OFX <ACCTID> values are institution-assigned account numbers, not
        # display names. We pass them through to import_log as-is — the
        # naming asymmetry with tabular's account_names is intentional and
        # documented at the begin_import() call site.
        account_ids = [
            a.account_id for a in parsed_ofx.accounts if a.account_id is not None
        ]
        import_id = import_log.begin_import(
            self._db,
            source_file=str(canonical_path),
            source_type="ofx",
            source_origin=source_origin,
            account_names=account_ids,
            file_sha256=digest,
        )
        result.import_id = import_id

        extractor = OFXExtractor()
        try:
            data = extractor.extract_from_file(
                canonical_path,
                import_id=import_id,
                source_origin=source_origin,
                # The bytes `digest` was taken from, not a fresh read: a file
                # replaced in between would be recorded as one version and
                # loaded as another, so a later genuine import of the new
                # version reads as a duplicate of the old.
                source_bytes=raw,
            )
        except Exception:
            import_log.finalize_import(
                self._db,
                import_id,
                status="failed",
                rows_total=0,
                rows_imported=0,
            )
            OFX_IMPORT_BATCHES.labels(status="failed").inc()
            IMPORT_ERRORS_TOTAL.labels(source_type="ofx", error_type="extract").inc()
            raise

        # Write all four DataFrames through the encrypted ingest path. Wrapped
        # in try/except so a load failure marks the batch as failed instead of
        # leaving raw.import_log.status='importing' and blocking re-imports.
        rows_loaded: dict[str, int] = {}
        try:
            for table_key, qualified in (
                ("institutions", OFX_INSTITUTIONS.full_name),
                ("accounts", OFX_ACCOUNTS.full_name),
                ("transactions", OFX_TRANSACTIONS.full_name),
                ("balances", OFX_BALANCES.full_name),
            ):
                df = data[table_key]
                if len(df) > 0:
                    self._db.ingest_dataframe(qualified, df, on_conflict="upsert")
                rows_loaded[table_key] = len(df)
        except Exception:
            import_log.finalize_import(
                self._db,
                import_id,
                status="failed",
                rows_total=sum(rows_loaded.values()),
                rows_imported=sum(rows_loaded.values()),
            )
            OFX_IMPORT_BATCHES.labels(status="failed").inc()
            IMPORT_ERRORS_TOTAL.labels(source_type="ofx", error_type="load").inc()
            raise

        # Resolve each OFX account to a canonical account_id, populating
        # app.account_links (source_native + scoped full_number strong refs) so
        # the staging translation JOIN is total for new OFX imports. Additive:
        # raw.ofx_accounts.account_id still holds the source-native ACCTID. Runs
        # after the raw load so links exist iff their raw account rows landed; a
        # separate try/except finalizes 'failed' rather than leaving the batch
        # stuck in 'importing'. Resolves the SAME list the gate proposed, so the
        # identity confirmed above is exactly the one bound here.
        try:
            created: list[CreatedAccount] = []
            for src in source_accounts:
                resolved_account = resolver.resolve(src)
                ACCOUNT_LINK_OUTCOMES_TOTAL.labels(
                    result=resolved_account.outcome
                ).inc()
                if minted := _created_account(src, resolved_account):
                    created.append(minted)
            result.accounts_created = tuple(created)
        except Exception:
            import_log.finalize_import(
                self._db,
                import_id,
                status="failed",
                rows_total=sum(rows_loaded.values()),
                rows_imported=sum(rows_loaded.values()),
            )
            OFX_IMPORT_BATCHES.labels(status="failed").inc()
            IMPORT_ERRORS_TOTAL.labels(source_type="ofx", error_type="resolve").inc()
            raise

        # Total across all four OFX tables — balance-only statements still
        # count as a successful import. Zero rows means nothing was written
        # (e.g., empty statement period); record as 'failed' so the metric
        # and import log accurately reflect that no data landed.
        total_rows = sum(rows_loaded.values())
        finalize_status: Literal["complete", "partial", "failed"] = (
            "complete" if total_rows > 0 else "failed"
        )
        # IMPORT_RECORDS_TOTAL stays scoped to transactions for cross-source
        # comparability with tabular/Plaid metrics.
        transactions_imported = rows_loaded["transactions"]

        import_log.finalize_import(
            self._db,
            import_id,
            status=finalize_status,
            rows_total=total_rows,
            rows_imported=total_rows,
        )
        OFX_IMPORT_BATCHES.labels(status=finalize_status).inc()
        IMPORT_RECORDS_TOTAL.labels(source_type="ofx").inc(transactions_imported)
        IMPORT_DURATION_SECONDS.labels(source_type="ofx").observe(
            time.monotonic() - _t0
        )

        result.institutions = rows_loaded["institutions"]
        result.accounts = rows_loaded["accounts"]
        result.transactions = rows_loaded["transactions"]
        result.balances = rows_loaded["balances"]
        result.details = rows_loaded

        if transactions_imported > 0:
            result.date_range = self._query_date_range(
                OFX_TRANSACTIONS.full_name, "CAST(date_posted AS DATE)", canonical_path
            )

        return result

    def _capture_new_account_metadata(
        self,
        account_id: str,
        meta: dict[str, str],
        *,
        in_outer_txn: bool = False,
    ) -> None:
        """Write user-supplied metadata for a freshly-minted account to settings.

        Field keys + values are validated up-front by ``_validate_account_metadata``
        (before any writes), so this method assumes a clean ``meta``. The write
        lands in ``app.account_settings`` (audited, Invariant 10) for the minted
        id even before the account materializes in ``core.dim_accounts`` — the
        next transform's LEFT JOIN folds the values in (``dim_accounts.sql``).
        """
        from moneybin.repositories.account_settings_repo import AccountSettingsRepo
        from moneybin.services.account_service import AccountSettings

        # Construct AccountSettings first so its __post_init__ validation runs
        # (display_name length, last_four 4-digits, currency 3-letter, etc.).
        settings = AccountSettings(
            account_id=account_id,
            display_name=meta.get("display_name"),
            last_four=meta.get("last_four"),
            account_subtype=meta.get("account_subtype"),
            currency_code=meta.get("currency_code"),
        )
        AccountSettingsRepo(self._db, audit=self._audit).set(
            account_id=settings.account_id,
            display_name=settings.display_name,
            official_name=settings.official_name,
            last_four=settings.last_four,
            account_subtype=settings.account_subtype,
            holder_category=settings.holder_category,
            currency_code=settings.currency_code,
            credit_limit=settings.credit_limit,
            archived=settings.archived,
            include_in_net_worth=settings.include_in_net_worth,
            default_cost_basis_method=settings.default_cost_basis_method,
            actor="import",
            in_outer_txn=in_outer_txn,
        )

    def _gate_account_proposals(
        self,
        resolver: AccountResolver,
        source_accounts: list[SourceAccount],
        bindings: dict[str, str] | None,
        *,
        channel: Channel,
        resolved_mapping: dict[str, str] | None = None,
        fallback_keys: Collection[str] = (),
        incoming_transactions: Sequence[IncomingTransaction] = (),
        emit_metrics: bool = True,
        observations: MetricObservations | None = None,
    ) -> list[SourceAccount]:
        """Fold in the caller's answers, then stop on any identity still open.

        Returns the ratified accounts for the resolve pass. Applying the
        bindings here rather than at each call site is what makes "gate what you
        are about to resolve" structural: every channel used to apply-then-gate
        as two statements, and a channel that forgot the first got a gate it
        could never satisfy.

        Propose-then-bind: ``propose()`` is read-only, so an unratified identity
        raises ``ImportConfirmationRequiredError`` (no rows load, no links
        written) and the caller answers with an ``account_bindings`` entry —
        adopt an existing id, or declare ``"new"``. Only then does ``resolve()``
        write anything.

        The predicate is ``AccountProposal.requires_confirm`` in full, both
        clauses: weak merge candidates surface, and so does a source that stated
        no identity at all (``identity_unknown``). A first-contact mint of a
        STATED identity does not — it has nothing to merge into and no other
        answer available, so gating it made a first import of N files cost N
        confirms that each had exactly one legal answer. It is reported instead,
        through ``accounts_created``. A remembered binding never re-asks: the
        second import of the same source hits ``source_native`` in the
        resolution ladder, sets ``adopted_via``, and passes straight through.
        The confirm therefore costs one answer per new account identity, once,
        not one per file.

        Actor-independent by design: an agent never self-picks an account
        identity. It receives the same pre-load stop as a human, surfaced as a
        ``confirmation_required`` envelope. This deliberately drops the earlier
        non-human early return, under which agent-driven imports bound accounts
        with no confirm at all — the path most likely to run unattended and
        least likely to have a wrong binding noticed.

        ``fallback_keys`` names the source keys that get ``propose(fallback=True)``
        — a decision-support pick-list of existing accounts instead of an empty
        ``candidates``, and the flag that sets ``identity_unknown``. Two sources
        opt in, both for the same reason: the bare single-account tabular import
        (no ``--account-name``, no account column) and a PDF whose text yields no
        account anchor. Each would otherwise mint under a filename guess, and an
        empty pick-list would force the user to type a raw account id. Every
        other source leaves it off, because a named account that matches nothing
        should mint, not be offered an unrelated list.

        Metrics are emitted here rather than left to ``resolve()``. The gate
        answers every weak candidate before resolution runs, so no import
        reaches ``resolve()``'s candidate pass any more — the confidence
        histogram it used to feed would read zero for the interactive path.
        ``disposition="rollback"`` because raising is this call's success case.
        """
        source_accounts, binding_targets = _apply_account_bindings(
            source_accounts, bindings or {}
        )
        ratified = _ratified_binding_refs(binding_targets)
        _refuse_unknown_binding_targets(resolver, source_accounts, bindings or {})
        _refuse_contradicted_bindings(resolver, source_accounts, binding_targets)
        wanted_fallback = set(fallback_keys)
        proposals: list[AccountProposalDict] = []
        # enumerate over the FULL list, not the surfaced subset: bindings are
        # applied above, so a ref can only index the file's own accounts.
        # Numbering what gets surfaced would shift every ref as soon as one
        # account resolved strongly.
        for index, src in enumerate(source_accounts):
            # A bound account (explicit_account_id / force_standalone) is already
            # decided; only unratified accounts gate.
            if src.explicit_account_id or src.force_standalone:
                continue
            proposal = resolver.propose(
                src, fallback=src.source_account_key in wanted_fallback
            )
            if incoming_transactions and proposal.candidates:
                proposal = dataclasses.replace(
                    proposal,
                    candidates=tuple(
                        dataclasses.replace(
                            candidate,
                            overlap=probe_incoming_ledger_overlap(
                                self._db,
                                transactions=incoming_transactions,
                                against_account_id=candidate.account_id,
                            ),
                        )
                        for candidate in proposal.candidates
                    ),
                )
            if proposal.requires_confirm:
                proposals.append(proposal.to_dict(proposal_ref=proposal_ref(index)))
        if not proposals:
            return source_accounts
        from moneybin.extractors.confidence import Confidence
        from moneybin.metrics.registry import (
            ACCOUNT_LINK_OVERLAP_RATIO,
            IMPORT_CONFIRMATIONS_TOTAL,
        )

        # Observe the measured overlap, not the resolver's per-signal constant:
        # the constant is the same for every candidate on a rung, so a histogram
        # of it reported which rungs fired and never whether a proposal was any
        # good. A candidate with no comparable period is skipped rather than
        # recorded as 0.0 — no shared period is absence of evidence, and folding
        # it in as a zero would read as evidence against every such pair.
        for surfaced in proposals:
            for candidate in surfaced["candidates"]:
                comparable = candidate.get("overlap_comparable")
                matched = candidate.get("overlap_matched")
                if type(comparable) is not int or type(matched) is not int:
                    continue
                if comparable == 0:
                    continue
                record_observation(
                    ACCOUNT_LINK_OVERLAP_RATIO,
                    matched / comparable,
                    labels={},
                    emit_metrics=emit_metrics,
                    observations=observations,
                    disposition="rollback",
                )
        record_counter(
            IMPORT_CONFIRMATIONS_TOTAL,
            # tier mirrors the Confidence below: the layout is settled, only the
            # account identity is open.
            labels={"channel": channel, "tier": "high", "outcome": "proposed"},
            emit_metrics=emit_metrics,
            observations=observations,
            disposition="rollback",
        )

        raise ImportConfirmationRequiredError(
            ConfirmationRequired(
                channel=channel,
                # The layout is already settled (tabular: the column mapping
                # resolved; ofx/pdf: nothing to map). Only the account identity
                # is in question, so high/1.0 is honest.
                confidence=Confidence(
                    score=1.0, tier="high", flagged=(), missing_required=()
                ),
                proposed=ProposedMapping(
                    field_mapping=resolved_mapping or {},
                    sample_values={},
                    unmapped_columns=(),
                ),
                reason="account_confirmation",
                account_proposals=proposals,
                ratified_bindings=ratified,
            )
        )

    def _pinned_native_key(
        self,
        *,
        resolver: AccountResolver,
        account_id: str,
        file_path: Path,
        source_bytes: bytes | None,
        source_type: str,
        source_origin: str,
        account_name: str | None = None,
    ) -> str:
        """The native key a pinned import with no ``--account-name`` should use.

        ``_bare_account_key`` hashes the file's bytes, which identifies a
        *document*: stable for an unchanged file, different the moment a
        recurring export grows by a row. ``account_id`` is folded into
        ``transaction_id``, so a rotated key re-keys every row already imported
        and both copies clear the ``(transaction_id, account_id)`` dedup —
        double-counting the overlap, which is the very thing the pin exists to
        prevent. No file-derived key escapes this: a content hash breaks when
        the file grows, a filename breaks when it is renamed.

        So the pin itself carries the identity. Reuse the key this account
        already resolved to for this source; the first import has none and
        falls back to the content hash, which every later pinned import then
        finds. Nothing canonical enters the key-space — the key stays the one
        the document itself produced.

        Refuses rather than guesses when the account holds several keys for one
        source: picking one would silently bind this file to whichever sorted
        first, and ``--account-name`` states the answer explicitly.

        Reuse only fires while this file's own key is unclaimed.
        ``_refuse_contradicted_bindings`` looks up whatever key the
        ``SourceAccount`` ends up carrying, so handing it the pin target's
        remembered key makes that lookup resolve to the target and find nothing
        to contradict — and a file already accepted onto another account loads
        here as well, putting one file's transactions on two accounts with no
        per-account view able to show it. ``_pdf_source_account`` is gated the
        identical way; a channel that skips the check is the fork the shared
        ``_reusable_pinned_keys`` exists to prevent.
        """
        # A pre-fix pin left a self-map: the canonical id sitting in the
        # source-key column. Reusing THAT would write it back into raw for this
        # import — the exact defect this change removes — so it is residue to be
        # ignored, never a key to adopt.
        # What this file calls its account with no history to consult. A label
        # is the caller naming which account in the file the pin means, so it
        # seeds the key exactly as an unpinned import with the same label would;
        # otherwise the file's own content key does.
        own_key = (
            label_account_key(account_name)
            if account_name
            else _bare_account_key(file_path, source_bytes=source_bytes)
        )
        if (
            resolver.accepted_native_owner(
                source_type=source_type, source_origin=source_origin, key=own_key
            )
            is not None
        ):
            return own_key
        keys = _reusable_pinned_keys(
            resolver,
            account_id=account_id,
            source_type=source_type,
            source_origin=source_origin,
        )
        if len(keys) == 1:
            return keys[0]
        if len(keys) > 1 and account_name:
            # The refusal below asks for exactly this flag, so honour it rather
            # than refusing a caller who already answered.
            return own_key
        if len(keys) > 1:
            # Masked like every other refusal that quotes a caller's key: the pin
            # arrives from the command line, and account_id is not always a minted
            # surrogate — stg_tabular__transactions falls back to the source-native
            # key when nothing resolves, so the id a caller reads back can be the
            # institution's own. source_origin is left out rather than masked; on
            # this branch it is a registered format name, and naming the file's own
            # coordinates tells the caller nothing they did not just type.
            raise ValueError(
                f"--account-id {_mask_caller_keys([account_id])} already has "
                f"{len(keys)} source keys for this {source_type} source; pass "
                "--account-name to say which account in this file the pin refers to"
            )
        return own_key

    def _import_tabular(
        self,
        file_path: Path,
        *,
        source_bytes: bytes | None = None,
        reviewed_plan: ReviewedTabularPlan | None = None,
        account_name: str | None = None,
        account_id: str | None = None,
        format_name: str | None = None,
        overrides: dict[str, str] | None = None,
        sign: str | None = None,
        date_format_override: str | None = None,
        number_format_override: str | None = None,
        save_format: bool = True,
        sheet: str | None = None,
        delimiter: str | None = None,
        encoding: str | None = None,
        no_row_limit: bool = False,
        no_size_limit: bool = False,
        auto_accept: bool = False,
        confirm: bool = False,
        human_sign_confirmation: bool = False,
        actor_kind: "ActorKind" = "human",
        account_bindings: dict[str, str] | None = None,
        account_metadata: dict[str, dict[str, str]] | None = None,
        in_outer_txn: bool = False,
        emit_metrics: bool = True,
        observations: MetricObservations | None = None,
    ) -> ImportResult:
        """Import a tabular file through the five-stage pipeline.

        Args:
            file_path: Path to the file.
            source_bytes: Immutable source object to parse instead of reopening path.
            reviewed_plan: Persisted parse and mapping decisions to replay exactly.
            account_name: Account name for single-account files.
            account_id: Explicit account ID (bypass matching).
            format_name: Explicit format name (bypass detection).
            overrides: Field→column overrides.
            sign: Sign convention override.
            date_format_override: Date format override (strptime string).
            number_format_override: Number format override.
            save_format: Auto-save detected format for future imports.
            sheet: Excel sheet name.
            delimiter: Explicit delimiter.
            encoding: Explicit encoding.
            no_row_limit: Override row count limit.
            no_size_limit: Override file size limit.
            auto_accept: Auto-accept the top fuzzy account match without prompting.
            confirm: If True, acts as Accept signal to resolve_or_confirm.
            human_sign_confirmation: Explicit human approval of an inferred
                tabular sign inversion; independent of mapping acceptance.
            actor_kind: 'human' (always surfaces) or 'agent' (may self-accept at high tier).
            account_bindings: Map of proposal_ref ("@0") or source_account_key ->
                canonical account_id (adopt) or "new" (mint standalone),
                ratifying the account-binding confirmation. An unbound account
                gates when it carries weak candidates, for every caller.
            account_metadata: Map of proposal_ref ("@0") or source_account_key
                -> {display_name, account_subtype, last_four, currency_code}
                captured into app.account_settings for accounts minted this
                import. Same key vocabulary as account_bindings, because the
                confirmation masks source_account_key and the ref is the only
                referent a caller can read back from it.
            in_outer_txn: Join a caller-owned transaction for every write.
            emit_metrics: Emit Prometheus observations during this call.
            observations: Buffer observations for a caller-owned transaction.

        Returns:
            ImportResult with summary.
        """
        import polars as pl

        from moneybin.extractors.tabular import TabularExtractor
        from moneybin.extractors.tabular.column_mapper import map_columns
        from moneybin.extractors.tabular.format_detector import detect_format
        from moneybin.extractors.tabular.formats import (
            TabularFormat,
            load_builtin_formats,
            load_formats_from_db,
            merge_formats,
            save_format_to_db,
        )
        from moneybin.extractors.tabular.readers import read_file
        from moneybin.extractors.tabular.transforms import transform_dataframe
        from moneybin.utils import slugify

        result = ImportResult(file_path=str(file_path), file_type="tabular")
        _t0 = time.monotonic()

        # Fail fast on bad account_metadata before any DB writes — a later raise
        # mid-resolve would orphan account_links rows with no import batch.
        _validate_account_metadata(account_metadata)

        # Load formats for ordinary imports and optional post-load format saving.
        builtin_formats = load_builtin_formats()
        all_formats = merge_formats(builtin_formats, load_formats_from_db(self._db))

        matched_format: TabularFormat | None = None
        if reviewed_plan is None and format_name:
            if format_name not in all_formats:
                raise ValueError(
                    f"Unknown format {format_name!r}. Available: {sorted(all_formats)}"
                )
            matched_format = all_formats[format_name]

        # Stage 1: Format detection — apply matched format's properties as defaults
        effective_delimiter = delimiter or (
            matched_format.delimiter if matched_format else None
        )
        effective_encoding = encoding or (
            matched_format.encoding if matched_format else None
        )
        effective_sheet = sheet or (matched_format.sheet if matched_format else None)

        if reviewed_plan is None:
            format_info = detect_format(
                file_path,
                source_bytes=source_bytes,
                format_override=matched_format.file_type
                if matched_format and matched_format.file_type != "auto"
                else None,
                delimiter_override=effective_delimiter,
                encoding_override=effective_encoding,
                no_size_limit=no_size_limit,
            )
            read_result = read_file(
                file_path,
                format_info,
                sheet=effective_sheet,
                skip_rows=matched_format.skip_rows
                if matched_format and matched_format.skip_rows
                else None,
                skip_trailing_patterns=matched_format.skip_trailing_patterns
                if matched_format
                else None,
                no_row_limit=no_row_limit,
                source_bytes=source_bytes,
            )
        else:
            from moneybin.extractors.tabular.format_detector import FormatInfo

            format_info = FormatInfo(
                file_type=reviewed_plan.file_type,
                delimiter=reviewed_plan.delimiter,
                encoding=reviewed_plan.encoding,
                file_size=reviewed_plan.file_size,
            )
            read_result = read_file(
                file_path,
                format_info,
                skip_rows=reviewed_plan.skip_rows,
                no_row_limit=no_row_limit,
                source_bytes=source_bytes,
                has_header=reviewed_plan.has_header,
            )
        df = read_result.df

        if len(df) == 0:
            raise ValueError(f"No data rows found in {file_path.name}")

        # Digest here rather than at batch creation below: Phase 3 commits
        # account mappings and pending decisions, so a hash that raised after it
        # — an ordinary path import whose file left a synced Downloads folder
        # mid-import — would strand those writes with no batch to belong to.
        # Same reason `_validate_account_metadata` fails fast above. read_file
        # has already proven the path readable, so this cannot raise a denial
        # that the guarded read was supposed to classify.
        digest = source_sha256(file_path, source_bytes)

        # Stage 3: Column mapping — match by headers if not already matched by name
        if reviewed_plan is None and not matched_format:
            headers = list(df.columns)
            for fmt in all_formats.values():
                if fmt.matches_headers(headers):
                    matched_format = fmt
                    break

        sign_evidence_header: str | None = None
        if reviewed_plan is not None:
            if sorted(df.columns) != reviewed_plan.header_signature:
                raise UserError(
                    "The stored import snapshot no longer matches its reviewed "
                    "header signature.",
                    code=error_codes.IMPORT_PREVIEW_PLAN_MISMATCH,
                )
            if read_result.rows_in_file != reviewed_plan.rows_in_file:
                raise UserError(
                    "The stored import snapshot no longer matches its reviewed "
                    "row accounting.",
                    code=error_codes.IMPORT_PREVIEW_PLAN_MISMATCH,
                )
            missing_columns = sorted(
                set(reviewed_plan.field_mapping.values()).difference(df.columns)
            )
            if missing_columns:
                raise UserError(
                    "The reviewed import mapping references unavailable columns.",
                    code=error_codes.IMPORT_PREVIEW_PLAN_MISMATCH,
                )
            if reviewed_plan.confidence == "low" or reviewed_plan.date_format is None:
                # Req 4: low is never auto-acceptable, even replayed from a
                # staged preview — and a plan whose date format was never
                # detected parses to zero rows whatever tier it carries.
                # Without this gate the transform stage silently drops every
                # row it can't parse and the confirm "succeeds" with
                # rows_loaded=0 and no error. A corrected mapping requires a
                # fresh preview (Req 8) — mapping=... on import_preview, not a
                # mutated confirm call.
                from moneybin.config import get_settings
                from moneybin.extractors.confidence import Confidence, resolve_tier
                from moneybin.extractors.tabular.column_mapper import (
                    collect_samples,
                    score_mapping,
                )
                from moneybin.metrics.registry import IMPORT_CONFIRMATIONS_TOTAL
                from moneybin.services.import_confirmation import (
                    ConfirmationRequired,
                    ImportConfirmationRequiredError,
                    ProposedMapping,
                    classify_unconfirmable_plan,
                )

                # The plan persists the tier, not the score, so re-score from
                # the mapping rather than inventing a number: score_mapping is
                # the canonical emitter, and it names the *missing half* of a
                # partial debit/credit pair instead of the contradictory
                # "amount". Feed it the preview's own flagged set — dropping it
                # re-scores a flagged 0.85 plan as a clean 1.0, so the refusal
                # would report score=1.0 beside tier="low" and name none of the
                # fields that earned the tier.
                score, missing_required = score_mapping(
                    reviewed_plan.field_mapping,
                    list(reviewed_plan.flagged_fields),
                    reviewed_plan.date_format,
                )
                # Report the tier the preview already showed rather than a flat
                # "low": an undetected date format alone scores 0.75 (medium),
                # and filing that under low contradicts the agent's own preview
                # and skews the buckets the confidence bands are tuned from. A
                # persisted "low" is kept as-is — a structural red flag pins the
                # tier regardless of score, and re-scoring cannot recover it.
                bands = get_settings().import_.confidence
                declined_tier = (
                    "low"
                    if reviewed_plan.confidence == "low"
                    else resolve_tier(score, t_high=bands.t_high, t_med=bands.t_med)
                )
                mapped_columns = set(reviewed_plan.field_mapping.values())
                plan_samples = {
                    dest: [
                        value
                        for value in collect_samples(df, column)
                        if value is not None
                    ]
                    for dest, column in reviewed_plan.field_mapping.items()
                }
                record_counter(
                    IMPORT_CONFIRMATIONS_TOTAL,
                    labels={
                        "channel": "tabular",
                        "tier": declined_tier,
                        "outcome": "declined",
                    },
                    emit_metrics=emit_metrics,
                    observations=observations,
                    disposition="rollback",
                )
                raise ImportConfirmationRequiredError(
                    ConfirmationRequired(
                        channel="tabular",
                        confidence=Confidence(
                            score=score,
                            tier=declined_tier,
                            flagged=tuple(reviewed_plan.flagged_fields),
                            missing_required=missing_required,
                        ),
                        # Name the file's own columns: the recovery hint tells
                        # the agent to send mapping={dest: source_column}, so an
                        # envelope with no column names forces it to guess.
                        proposed=ProposedMapping(
                            field_mapping=dict(reviewed_plan.field_mapping),
                            sample_values=plan_samples,
                            unmapped_columns=tuple(
                                column
                                for column in df.columns
                                if column not in mapped_columns
                            ),
                        ),
                        samples=plan_samples,
                        # Narrow the reason to the cause, so a surface can name
                        # the recovery that actually applies. Structural first:
                        # a consumed header row outranks any mapping question,
                        # since no caller input touches it. Then an unreadable
                        # date, but only when the column IS mapped and its
                        # *values* are the problem — with no date column at all,
                        # --date-format is useless and a mapping override is
                        # the real recovery.
                        reason=classify_unconfirmable_plan(
                            header_row_looks_like_data=(
                                reviewed_plan.header_row_looks_like_data
                            ),
                            date_format=reviewed_plan.date_format,
                            field_mapping=reviewed_plan.field_mapping,
                            flagged_fields=list(reviewed_plan.flagged_fields),
                        ),
                    )
                )
            resolved = ResolvedMapping(
                field_mapping=dict(reviewed_plan.field_mapping),
                date_format=reviewed_plan.date_format,
                sign_convention=reviewed_plan.sign_convention,
                number_format=reviewed_plan.number_format,
                is_multi_account=reviewed_plan.is_multi_account,
                confidence=reviewed_plan.confidence,
            )
            format_source = "reviewed"
        elif matched_format:
            resolved = ResolvedMapping(
                field_mapping=matched_format.field_mapping,
                date_format=matched_format.date_format,
                sign_convention=matched_format.sign_convention,
                number_format=matched_format.number_format,
                is_multi_account=matched_format.multi_account,
                confidence="high",
            )
            format_source = (
                "built-in" if matched_format.name in builtin_formats else "saved"
            )
        else:
            from moneybin.config import get_settings
            from moneybin.metrics.registry import (
                IMPORT_CONFIRMATIONS_TOTAL,
                IMPORT_DETECTION_SCORE,
                IMPORT_OVERRIDE_TOTAL,
                IMPORT_SELF_ACCEPT_TOTAL,
            )
            from moneybin.services.import_confirmation import (
                Accept,
                ConfirmationRequired,
                ImportConfirmationRequiredError,
                Override,
                ProposedMapping,
                classify_unconfirmable_plan,
                coerce_number_format,
                coerce_sign_convention,
                resolve_or_confirm,
            )

            settings = get_settings()
            bands = settings.import_.confidence
            mapping_result = map_columns(
                df,
                overrides=overrides,
                t_high=bands.t_high,
                t_med=bands.t_med,
                structural_red_flag=read_result.header_row_looks_like_data,
            )
            sign_evidence_header = mapping_result.sign_evidence_header
            confidence = mapping_result.to_confidence(
                t_high=bands.t_high, t_med=bands.t_med
            )
            proposed = ProposedMapping(
                field_mapping=mapping_result.field_mapping,
                sample_values=mapping_result.sample_values,
                unmapped_columns=tuple(mapping_result.unmapped_columns),
            )

            signal: Accept | Override | None
            if overrides:
                signal = Override(mapping=overrides)
            elif confirm:
                signal = Accept()
            else:
                signal = None

            # Required fields depend on the EFFECTIVE amount shape after the
            # override resolves the single/split contention, so this
            # pre-compute and validate_partial_mapping's merge logic must
            # agree — both route through resolve_amount_shape.
            from moneybin.extractors.tabular.field_aliases import FIELD_ALIASES
            from moneybin.services.import_confirmation import (
                tabular_required_fields,
            )

            required_fields = tabular_required_fields(
                proposed_keys=set(proposed.field_mapping.keys()),
                override_keys=set(overrides.keys()) if overrides else set(),
            )
            # Every detection belongs in the histogram — the ones that never
            # reach resolve_or_confirm below *and* the ones that import
            # cleanly. The bands are tuned from this distribution, so dropping
            # either end biases it. This sits before the gates, so it is the
            # one metric here on a path that can end both ways: a buffered
            # caller flushes "commit" on success and "rollback" on the refusal
            # those gates raise, and flush() discards whatever does not match.
            # Queue both, and exactly one lands. The unbuffered path emits
            # directly, so it records once or it would double-count.
            if observations is not None:
                for disposition in ("commit", "rollback"):
                    record_observation(
                        IMPORT_DETECTION_SCORE,
                        confidence.score,
                        labels={},
                        emit_metrics=emit_metrics,
                        observations=observations,
                        disposition=disposition,
                    )
            else:
                record_observation(
                    IMPORT_DETECTION_SCORE,
                    confidence.score,
                    labels={},
                    emit_metrics=emit_metrics,
                    observations=None,
                )

            # An explicit --date-format is the documented way in for a real
            # format the detector carries no candidate for (%Y%m%d), so its
            # presence — not the detector's silence — decides whether this plan
            # has a date format at all. Override first, matching the precedence
            # the replace below applies. Whether it can actually *read* the
            # column is checked once down there, where every branch honours it;
            # a second check here would be a second gate to keep in step.
            # Ahead of resolve_or_confirm, which records an accepted or
            # overridden confirmation below — a counter the CLI path applies
            # immediately, so a later refusal cannot take it back.
            _validate_date_format_override(
                df, mapping_result.field_mapping, date_format_override
            )
            date_format_effective = date_format_override or mapping_result.date_format
            if date_format_effective is None:
                # A date column nothing could parse makes the plan unloadable at
                # any tier: the transform drops every row and the import reports
                # success. This gates *ahead* of resolve_or_confirm because an
                # Override short-circuits there at every tier, including low.
                # map_columns already applied `overrides`, so a correction that
                # names a parseable date column clears this on its own re-run.
                record_counter(
                    IMPORT_CONFIRMATIONS_TOTAL,
                    labels={
                        "channel": "tabular",
                        "tier": confidence.tier,
                        "outcome": "declined",
                    },
                    emit_metrics=emit_metrics,
                    observations=observations,
                    disposition="rollback",
                )
                raise ImportConfirmationRequiredError(
                    ConfirmationRequired(
                        channel="tabular",
                        confidence=confidence,
                        proposed=proposed,
                        # See the reviewed-plan branch: a file with no date
                        # column mapped is a mapping problem, not a format one.
                        reason=classify_unconfirmable_plan(
                            header_row_looks_like_data=False,
                            date_format=None,
                            field_mapping=proposed.field_mapping,
                            flagged_fields=list(mapping_result.flagged_fields),
                        ),
                        samples=dict(proposed.sample_values),
                    )
                )
            outcome = resolve_or_confirm(
                channel="tabular",
                confidence=confidence,
                proposed=proposed,
                available_columns=tuple(df.columns),
                required_fields=required_fields,
                valid_destinations=tuple(FIELD_ALIASES.keys()),
                signal=signal,
                self_accept_enabled=settings.import_.self_accept_high,
                actor_kind=actor_kind,
            )

            if isinstance(outcome, ConfirmationRequired):
                record_counter(
                    IMPORT_CONFIRMATIONS_TOTAL,
                    labels={
                        "channel": "tabular",
                        "tier": confidence.tier,
                        "outcome": "declined",
                    },
                    emit_metrics=emit_metrics,
                    observations=observations,
                )
                # resolve_or_confirm refuses a low tier with its own generic
                # reason, and a consumed header row is what pinned the tier —
                # so re-classify before raising, or every surface prescribes a
                # mapping retry for the one cause no mapping answers. Reached
                # by a headerless XLSX on first contact: pl.read_excel always
                # eats row 0 as the header, so _read_excel sets the flag with
                # no explicit skip_rows involved.
                raise ImportConfirmationRequiredError(
                    dataclasses.replace(
                        outcome,
                        reason=classify_unconfirmable_plan(
                            header_row_looks_like_data=(
                                read_result.header_row_looks_like_data
                            ),
                            date_format=mapping_result.date_format,
                            field_mapping=outcome.proposed.field_mapping
                            if isinstance(outcome.proposed, ProposedMapping)
                            else dict(mapping_result.field_mapping),
                            flagged_fields=list(mapping_result.flagged_fields),
                        ),
                    )
                    if outcome.reason == "unknown_layout"
                    else outcome
                )

            if outcome.self_accepted:
                record_counter(
                    IMPORT_SELF_ACCEPT_TOTAL,
                    labels={"channel": "tabular"},
                    emit_metrics=emit_metrics,
                    observations=observations,
                )
            if isinstance(signal, Override):
                record_counter(
                    IMPORT_OVERRIDE_TOTAL,
                    labels={"channel": "tabular"},
                    emit_metrics=emit_metrics,
                    observations=observations,
                )
                record_counter(
                    IMPORT_CONFIRMATIONS_TOTAL,
                    labels={
                        "channel": "tabular",
                        "tier": confidence.tier,
                        "outcome": "overridden",
                    },
                    emit_metrics=emit_metrics,
                    observations=observations,
                )
            else:
                record_counter(
                    IMPORT_CONFIRMATIONS_TOTAL,
                    labels={
                        "channel": "tabular",
                        "tier": confidence.tier,
                        "outcome": "accepted",
                    },
                    emit_metrics=emit_metrics,
                    observations=observations,
                )

            resolved_sign = coerce_sign_convention(
                field_mapping=outcome.field_mapping,
                detected=mapping_result.sign_convention,
            )
            # Same trigger as the sign coercion above: an override can retire
            # the very column map_columns derived the number format from.
            resolved_number_format = coerce_number_format(
                field_mapping=outcome.field_mapping,
                sample_values=mapping_result.sample_values,
                detected=mapping_result.number_format,
            )
            resolved = ResolvedMapping(
                field_mapping=outcome.field_mapping,
                date_format=date_format_effective,
                sign_convention=resolved_sign,
                number_format=resolved_number_format,
                is_multi_account=mapping_result.is_multi_account,
                confidence=confidence.tier,
            )
            format_source = "detected"

            if (
                mapping_result.sign_needs_confirmation
                and not sign
                and resolved.sign_convention != "negative_is_income"
            ):
                logger.warning(
                    "⚠️  Sign convention is ambiguous (all amounts appear positive). "
                    f"Proceeding with '{resolved.sign_convention}' — "
                    "use --sign to override if expense amounts look wrong."
                )

        # Covers the branches that reach here without one — the first-contact
        # branch validates earlier, before it records a confirmation outcome.
        _validate_date_format_override(df, resolved.field_mapping, date_format_override)

        # All three branches converge here, and it sits ABOVE the success
        # metrics below, because a refusal must not first record a silent
        # format reuse.
        #
        # Which branch actually reaches it depends on the reader. For CSV the
        # flag is computed only for an explicit skip_rows, so it can only be
        # true when a saved or built-in format supplied the skip — the
        # `elif matched_format:` branch, which asserts confidence="high" and
        # would otherwise commit. For XLSX `_read_excel` computes it
        # unconditionally, because pl.read_excel always consumes row 0 as the
        # header; a first-contact headerless sheet therefore sets it too, and
        # resolve_or_confirm refuses that at `low` before reaching here — the
        # re-classification above the raise is what routes it correctly. The
        # reviewed-plan branch refuses earlier with the same reason.
        #
        # No caller input clears it: a mapping override cannot un-consume a
        # header row, and resolve_or_confirm honours an Override at every tier
        # by design.
        if read_result.header_row_looks_like_data:
            # Confidence is imported at module scope, but a sibling branch
            # imports it locally, which makes the name function-local here.
            from moneybin.extractors.confidence import Confidence
            from moneybin.extractors.tabular.column_mapper import collect_samples
            from moneybin.metrics.registry import (
                IMPORT_CONFIRMATIONS_TOTAL,
                IMPORT_REVALIDATION_FAILURE_TOTAL,
            )
            from moneybin.services.import_confirmation import (
                ConfirmationRequired,
                ImportConfirmationRequiredError,
                ProposedMapping,
            )

            gate_samples = {
                dest: [v for v in collect_samples(df, column) if v is not None]
                for dest, column in resolved.field_mapping.items()
                if column in df.columns
            }
            # This IS the replay guard the registry says the counter waits on:
            # a saved layout that no longer reads its own file. Record it as
            # such, and label the decline with the tier the envelope carries —
            # the matched_format branch hardcodes resolved.confidence="high",
            # so using it here would file a low-tier refusal under high.
            record_counter(
                IMPORT_REVALIDATION_FAILURE_TOTAL,
                labels={"channel": "tabular"},
                emit_metrics=emit_metrics,
                observations=observations,
                disposition="rollback",
            )
            record_counter(
                IMPORT_CONFIRMATIONS_TOTAL,
                labels={
                    "channel": "tabular",
                    "tier": "low",
                    "outcome": "declined",
                },
                emit_metrics=emit_metrics,
                observations=observations,
                disposition="rollback",
            )
            raise ImportConfirmationRequiredError(
                ConfirmationRequired(
                    channel="tabular",
                    confidence=Confidence(
                        score=0.0,
                        tier="low",
                        flagged=(),
                        missing_required=(),
                    ),
                    proposed=ProposedMapping(
                        field_mapping=dict(resolved.field_mapping),
                        sample_values=gate_samples,
                        unmapped_columns=tuple(
                            c
                            for c in df.columns
                            if c not in resolved.field_mapping.values()
                        ),
                    ),
                    reason="header_row_consumed",
                    samples=gate_samples,
                )
            )

        # Record format match and detection confidence metrics
        if matched_format:
            from moneybin.metrics.registry import IMPORT_KNOWN_FORMAT_REUSE_TOTAL

            record_counter(
                IMPORT_KNOWN_FORMAT_REUSE_TOTAL,
                labels={"channel": "tabular"},
                emit_metrics=emit_metrics,
                observations=observations,
            )
            record_counter(
                TABULAR_FORMAT_MATCHES,
                labels={
                    "format_name": matched_format.name,
                    "format_source": format_source,
                },
                emit_metrics=emit_metrics,
                observations=observations,
            )
        record_counter(
            TABULAR_DETECTION_CONFIDENCE,
            labels={"confidence": resolved.confidence},
            emit_metrics=emit_metrics,
            observations=observations,
        )

        # Apply CLI overrides — rebuild a new ResolvedMapping (frozen).
        # Validate at runtime: typing.cast has no runtime effect, so an
        # invalid value like ``--sign=backwards`` would silently propagate
        # into the transform pipeline and surface deep inside SQLMesh,
        # leaving a dangling raw.import_log row in ``importing`` state.
        # Guard explicitly via get_args so the failure is a clean UserError
        # at the import boundary.
        from typing import get_args

        if sign and sign not in get_args(SignConventionType):
            raise UserError(
                f"Invalid sign convention: {sign!r}. "
                f"Valid values: {list(get_args(SignConventionType))}.",
                code=error_codes.IMPORT_INVALID_SIGN_CONVENTION,
            )
        if number_format_override and number_format_override not in get_args(
            NumberFormatType
        ):
            raise UserError(
                f"Invalid number format: {number_format_override!r}. "
                f"Valid values: {list(get_args(NumberFormatType))}.",
                code=error_codes.IMPORT_INVALID_NUMBER_FORMAT,
            )
        if sign:
            _validate_explicit_tabular_sign_shape(
                resolved.field_mapping,
                cast(SignConventionType, sign),
            )
        detected_sign = resolved.sign_convention
        if sign or date_format_override or number_format_override:
            resolved = dataclasses.replace(
                resolved,
                sign_convention=cast(SignConventionType, sign)
                if sign
                else resolved.sign_convention,
                date_format=date_format_override or resolved.date_format,
                number_format=cast(NumberFormatType, number_format_override)
                if number_format_override
                else resolved.number_format,
            )

        self._gate_tabular_sign_convention(
            detected_sign=detected_sign,
            sign=sign,
            human_sign_confirmation=human_sign_confirmation,
            is_first_contact=matched_format is None,
            evidence=sign_evidence_header or resolved.field_mapping.get("amount", ""),
            emit_metrics=emit_metrics,
            observations=observations,
        )

        # Determine account info
        source_type = format_info.file_type
        if source_type in ("semicolon", "pipe"):
            source_type = "csv"

        # source_origin scopes the source_native key; compute before resolution so
        # raw.* and app.account_links.source_origin stay identical (a later staging
        # JOIN keys on it). Do NOT change how source_origin is derived.
        # This is the EXPORTER / format identity (Monarch / Tiller / bank export,
        # or the account slug for an unregistered single file) — orthogonal to the
        # per-account institution, which is resolved separately and, for
        # multi-account exporters, comes from row data (Decision 8).
        source_origin = (
            matched_format.name
            if matched_format
            else slugify(account_name or "unknown")
        )
        # institution is best-effort metadata feeding the resolver's weak-signal
        # (institution+last4) candidate pass; unknown is allowed.
        institution = resolve_institution_tabular(
            file_path=file_path,
            format_institution=(
                matched_format.institution_name if matched_format else None
            ),
            cli_override=None,  # no --institution flag on tabular yet
        )
        resolver = AccountResolver(
            self._db,
            actor="system",
            emit_metrics=emit_metrics,
            observations=observations,
        )
        bindings = account_bindings or {}

        # account_ids stamped on raw are source-NATIVE keys (DP-1); the resolver
        # writes the native->canonical app.account_links mapping as a side effect.
        acct_name_col = resolved.field_mapping.get("account_name")
        acct_num_col = resolved.field_mapping.get("account_number")
        acct_id_to_name: dict[str, str] = {}
        # Parse each display label once: (clean_name, label_last4). Reused by the
        # resolver pass (clean name strips mask text → stronger fuzzy match) and
        # by Stage 5's account_number_masked, so authored_label_parts runs once.
        label_parsed_by_key: dict[str, tuple[str, str | None]] = {}
        # last4 from the mapped account-number column — the authoritative fallback
        # when a label carries none (e.g. "Checking" alongside an "Account Number"
        # column). Keyed by native account key.
        number_last4_by_key: dict[str, str | None] = {}
        # Per-account institution for multi-account exporter formats, from a mapped
        # Institution column (Tiller-style); else None. NEVER the exporter/tool name
        # (Decision 8 exporter/institution split). Single-account keeps the
        # format/file institution unchanged.
        multi_acct_inst: dict[str, str | None] = {}
        # Which institution_name Stage 5 will stamp on raw.tabular_accounts, and
        # therefore the one core.dim_accounts names each account by. Decided here
        # rather than at Stage 5 alone because the mint report has to state it in
        # Phase 3, before any row is written — and two spellings of one
        # expression is exactly how the reported name and the stored one drifted
        # apart. `institution` — resolved from the format or the filename at
        # Stage 1 — is the fallback for an unregistered import, whose
        # matched_format is None.
        per_account_inst = (
            resolved.is_multi_account and not account_id and not account_name
        )
        shared_institution_name = (
            matched_format.institution_name if matched_format else None
        ) or institution

        def raw_institution_name(native_key: str) -> str | None:
            """The institution_name this account's raw row will carry."""
            return (
                multi_acct_inst.get(native_key)
                if per_account_inst
                else shared_institution_name
            )

        # The display-safe label Stage 5 stamps on raw.tabular_accounts, and so
        # the top rung core.dim_accounts names each account by. Keyed by native
        # key, and populated ONLY where a human authored the name — the file's
        # account column or --account-name. The bare branch below synthesizes
        # one from the filename, which names the upload rather than the account;
        # promoting that would let renaming a file rename the account.
        source_label_by_key: dict[str, str] = {}

        def tabular_name_facts(
            native_key: str, last_four: str | None
        ) -> AccountNameFacts:
            """Facts for the mint report; call after `multi_acct_inst` is filled.

            No category: the tabular account_df writes account_type=None on every
            row, so the model has neither a subtype nor a type to name this
            account by. The last four comes from the same parse Stage 5 masks
            into account_number_masked, re-derived here through the digit rule
            the model applies to that column.
            """
            return AccountNameFacts(
                source_label=source_label_by_key.get(native_key),
                institution_name=raw_institution_name(native_key),
                category=None,
                last_four=derived_last_four(last_four),
            )

        # Phase 1 — enumerate the source accounts this file presents (one per
        # native key) WITHOUT resolving, so the account-binding gate can run
        # between enumeration and the writing resolve() pass.
        source_accounts: list[SourceAccount] = []
        # Source keys whose gate offers a fallback pick-list (see the bare branch).
        fallback_keys: set[str] = set()
        if account_id:
            # A pin says which account this file belongs to; it does not rename
            # what the file calls that account. Derive the same native key the
            # unpinned branches below would, and carry the pin in
            # explicit_account_id alone.
            # --account-name does not pick the key here. It labels the account,
            # and the pin already said which account this is; letting the label
            # key the row means adding or dropping the flag between two imports
            # of one recurring export re-keys it — and tabular derives
            # transaction_id FROM this key, so every row in the overlap changes
            # id and both copies clear staging's dedup. It still seeds a first
            # import that has nothing to reuse, which is what keeps two accounts
            # exported to one file on separate keys.
            native_key = self._pinned_native_key(
                resolver=resolver,
                account_id=account_id,
                file_path=file_path,
                source_bytes=source_bytes,
                source_type=source_type,
                source_origin=source_origin,
                account_name=account_name,
            )
            account_ids: str | list[str] = native_key
            # Parse only a real display label, never a derived key: both a
            # canonical id and a bare content key can end in 4 digits
            # ("acct-1234", "statement-9f2c1234"), and parsing one would
            # fabricate a "****1234" bank mask in dim_accounts. No label
            # supplied → no derived last4.
            if account_name:
                display_name = account_name
                (
                    source_label_by_key[native_key],
                    clean_name,
                    label_last4,
                ) = authored_label_parts(account_name)
            else:
                display_name = file_path.stem or native_key
                clean_name, label_last4 = display_name, None
            acct_id_to_name[native_key] = display_name
            label_parsed_by_key[native_key] = (clean_name, label_last4)
            source_accounts.append(
                SourceAccount(
                    source_type=source_type,
                    source_origin=source_origin,
                    source_account_key=native_key,
                    account_name=clean_name,
                    # True only when --account-name supplied clean_name; the
                    # unnamed arm falls back to the filename stem or the
                    # native key, neither of which a person typed.
                    account_name_is_user_set=bool(account_name),
                    institution=institution,
                    last_four=label_last4,
                    name_facts=tabular_name_facts(native_key, label_last4),
                    explicit_account_id=account_id,
                    # Deliberately does NOT teach this file's own content key the
                    # way the PDF channel teaches a borrowed document's digest.
                    # There, transaction_id folds the canonical account, so an
                    # extra accepted key is a harmless alias. Here it is derived
                    # FROM the raw key, so an extra key is a second dedup
                    # namespace — and _pinned_native_key reads two keys back as
                    # ambiguity and refuses a file that was never ambiguous. It
                    # would also buy little: this key is the file's bytes, and a
                    # recurring export's bytes change every period, so the key
                    # taught is not the one the next unpinned import derives.
                    # Cost: an unpinned re-import of a CHANGED file stops at the
                    # confirm gate instead of self-recognising. Visible and
                    # safe, and tracked with the rest of the key-identity work.
                )
            )
        elif account_name:
            native_key = label_account_key(account_name)
            account_ids = native_key
            acct_id_to_name[native_key] = account_name
            (
                source_label_by_key[native_key],
                clean_name,
                label_last4,
            ) = authored_label_parts(account_name)
            label_parsed_by_key[native_key] = (clean_name, label_last4)
            if acct_num_col and acct_num_col in df.columns:
                for value in df[acct_num_col].to_list():
                    if l4 := _last4_from_account_number(value):
                        number_last4_by_key[native_key] = l4
                        break
            source_accounts.append(
                SourceAccount(
                    source_type=source_type,
                    source_origin=source_origin,
                    source_account_key=native_key,
                    account_name=clean_name,
                    # This branch is reached only when --account-name was
                    # supplied; clean_name always comes from it.
                    account_name_is_user_set=True,
                    institution=institution,
                    last_four=label_last4 or number_last4_by_key.get(native_key),
                    name_facts=tabular_name_facts(
                        native_key, label_last4 or number_last4_by_key.get(native_key)
                    ),
                )
            )
        elif (
            resolved.is_multi_account and acct_name_col and acct_name_col in df.columns
        ):
            account_cells = df[acct_name_col].to_list()
            raw_names = [str(v) if v is not None else "unknown" for v in account_cells]
            account_ids = [slugify(name) for name in raw_names]
            # Keys the file actually named. A blank cell arrives as NULL and is
            # filled with "unknown" above so those rows still have a key to
            # group on — but that filler reads exactly like a name someone
            # typed, and the label rung is reserved for names someone did. Left
            # out here, such an account is named from its bank fields instead.
            authored_keys = {
                aid
                for aid, cell in zip(account_ids, account_cells, strict=True)
                if cell is not None
            }
            for aid, name in zip(account_ids, raw_names, strict=True):
                if aid not in acct_id_to_name:
                    acct_id_to_name[aid] = name
            authored_parts = {
                nk: authored_label_parts(nm) for nk, nm in acct_id_to_name.items()
            }
            label_parsed_by_key = {
                nk: (clean, last4) for nk, (_, clean, last4) in authored_parts.items()
            }
            source_label_by_key.update({
                nk: display
                for nk, (display, _, _) in authored_parts.items()
                if nk in authored_keys
            })
            if acct_num_col and acct_num_col in df.columns:
                for aid, value in zip(
                    account_ids, df[acct_num_col].to_list(), strict=True
                ):
                    if number_last4_by_key.get(aid):
                        continue
                    if l4 := _last4_from_account_number(value):
                        number_last4_by_key[aid] = l4
            # Per-account institution from a mapped Institution column (Tiller-style):
            # first non-null value per account key. An institution embedded only in a
            # Monarch-style account LABEL is not parsed here — label→institution
            # parsing is not implemented.
            inst_col = resolved.field_mapping.get("institution_name")
            if inst_col and inst_col in df.columns:
                for nm, inst_val in zip(raw_names, df[inst_col].to_list(), strict=True):
                    key = slugify(nm)
                    if key not in multi_acct_inst and inst_val:
                        multi_acct_inst[key] = str(inst_val)
            source_accounts.extend(
                SourceAccount(
                    source_type=source_type,
                    source_origin=source_origin,
                    source_account_key=native_key,
                    account_name=label_parsed_by_key[native_key][0],
                    # authored_keys excludes the "unknown" filler used for a
                    # blank account-name cell -- same rung source_label_by_key
                    # above already gates on.
                    account_name_is_user_set=native_key in authored_keys,
                    institution=multi_acct_inst.get(native_key),
                    last_four=(
                        label_parsed_by_key[native_key][1]
                        or number_last4_by_key.get(native_key)
                    ),
                    name_facts=tabular_name_facts(
                        native_key,
                        label_parsed_by_key[native_key][1]
                        or number_last4_by_key.get(native_key),
                    ),
                )
                for native_key in acct_id_to_name
            )
        else:
            # Single-account file with no caller-supplied identity (no
            # --account-name/--account-id and no account-name column). The
            # account is real but unnamed — surface it through the
            # account_confirmation envelope like every other import ambiguity,
            # not a hard error ("magic stays visible"). The synthetic source key
            # is stable across the confirm round-trip, so an --account-binding
            # answer re-enumerates and applies in Phase 2; --account-name takes
            # the branch above instead.
            native_key = _bare_account_key(file_path, source_bytes=source_bytes)
            account_ids = native_key
            placeholder_name = file_path.stem or native_key
            acct_id_to_name[native_key] = placeholder_name
            label_parsed_by_key[native_key] = (placeholder_name, None)
            if acct_num_col and acct_num_col in df.columns:
                for value in df[acct_num_col].to_list():
                    if l4 := _last4_from_account_number(value):
                        number_last4_by_key[native_key] = l4
                        break
            bare_src = SourceAccount(
                source_type=source_type,
                source_origin=source_origin,
                source_account_key=native_key,
                account_name=placeholder_name,
                # placeholder_name is always the filename stem or the native
                # key -- no caller-supplied identity exists on this branch.
                account_name_is_user_set=False,
                institution=institution,
                last_four=number_last4_by_key.get(native_key),
                name_facts=tabular_name_facts(
                    native_key, number_last4_by_key.get(native_key)
                ),
            )
            source_accounts.append(bare_src)
            # This source has no identity signal at all, so its gate offers a
            # fallback pick-list of existing accounts rather than an empty
            # candidates: [] that would force the user to type a raw account_id.
            # The gate itself is the shared one in Phase 2 — a binding answer
            # (--account-binding <native_key>=<account_id|new>) re-enters here,
            # binds in Phase 2, and passes through; a MISTYPED binding key fails
            # loud on the known_keys check rather than silently re-eliciting; and
            # an exact-same-file re-import adopts via the resolver's source_native
            # step without re-prompting (idempotency, not a filename guess).
            fallback_keys.add(native_key)

        # Fail loud on a metadata source key that doesn't match any of this
        # file's accounts (a typo) — silently ignoring it would do the wrong
        # thing invisibly ("magic stays visible"). account_metadata is
        # tabular-only; the binding half of this check lives in
        # _resolve_binding_targets, which every channel reaches — and this
        # message follows that one's rule: echo the caller's own unknown keys
        # (they sent them), never enumerate the file's real ones. A tabular key
        # is slugify(account_name), and an account label routinely carries the
        # number it names.
        # Refs first: the confirmation masks source_account_key, so a caller on
        # a multi-account file can only key metadata by the ref it showed them.
        account_metadata = _resolve_metadata_keys(
            source_accounts, account_metadata or {}
        )
        known_keys = {s.source_account_key for s in source_accounts}
        if unknown_keys := set(account_metadata or {}) - known_keys:
            raise ValueError(
                f"account_metadata references unknown source key(s): "
                f"{_mask_caller_keys(unknown_keys)}. "
                f"This file has {len(source_accounts)} "
                "account(s) — key each entry by its proposal_ref (@0, @1, …) as "
                "the confirmation reported it, or by a source key."
            )

        # Phase 2 — gate on any account identity the caller hasn't ratified.
        # Raises ImportConfirmationRequiredError (no rows load) and returns the
        # bound accounts for the resolve pass below.
        source_accounts = self._gate_account_proposals(
            resolver,
            source_accounts,
            bindings,
            channel="tabular",
            resolved_mapping=dict(resolved.field_mapping),
            fallback_keys=fallback_keys,
            emit_metrics=emit_metrics,
            observations=observations,
        )

        # Phase 3 — resolve (writes native->canonical mapping + pending decisions),
        # then capture any caller-supplied metadata for accounts minted this import.
        metadata = account_metadata or {}
        created: list[CreatedAccount] = []
        for src in source_accounts:
            resolved_account = resolver.resolve(src, in_outer_txn=in_outer_txn)
            record_counter(
                ACCOUNT_LINK_OUTCOMES_TOTAL,
                labels={"result": resolved_account.outcome},
                emit_metrics=emit_metrics,
                observations=observations,
            )
            meta = metadata.get(src.source_account_key)
            if minted := _created_account(src, resolved_account, settings=meta):
                created.append(minted)
            if not meta:
                continue
            # Capture only for a genuinely-new account (outcome="minted_new",
            # i.e. a "new" binding or a clean no-candidate mint). A
            # pending_review provisional is is_new=True too, but a later
            # accept re-points it onto the candidate and abandons the
            # provisional id — settings written here would be orphaned. An
            # adopted account keeps its existing settings.
            if resolved_account.outcome == "minted_new":
                self._capture_new_account_metadata(
                    resolved_account.account_id,
                    meta,
                    in_outer_txn=in_outer_txn,
                )
            else:
                # Routine on the agent path (a binding adopted an existing
                # account, or the account went to pending_review) — info, not a
                # warning about an error.
                logger.info(
                    "account_metadata ignored: account resolved to "
                    f"{resolved_account.outcome!r}, not a new mint."
                )
        result.accounts_created = tuple(created)

        # Create import batch
        extractor = TabularExtractor(self._db)
        import_id = extractor.create_import_batch(
            source_file=str(file_path),
            source_type=source_type,
            source_origin=source_origin,
            account_names=sorted(acct_id_to_name.values()),
            format_name=matched_format.name if matched_format else None,
            format_source=format_source,
            file_sha256=digest,
        )
        result.import_id = import_id

        # Stage 4: Transform
        from moneybin.config import get_settings

        tabular_cfg = get_settings().providers.tabular
        try:
            transform_result = transform_dataframe(
                df=df,
                field_mapping=resolved.field_mapping,
                date_format=resolved.date_format,
                sign_convention=resolved.sign_convention,
                number_format=resolved.number_format,
                account_id=account_ids,
                source_file=str(file_path),
                source_type=source_type,
                source_origin=source_origin,
                import_id=import_id,
                balance_pass_threshold=tabular_cfg.balance_pass_threshold,
                balance_tolerance_cents=tabular_cfg.balance_tolerance_cents,
            )
        except Exception as e:  # noqa: BLE001  # re-raised as ValueError after recording rejection in DB
            extractor.finalize_import_batch(
                import_id=import_id,
                rows_total=len(df),
                rows_imported=0,
                rows_rejected=len(df),
                emit_metrics=emit_metrics,
                observations=observations,
                metric_disposition="rollback",
            )
            record_counter(
                IMPORT_ERRORS_TOTAL,
                labels={"source_type": source_type, "error_type": "transform"},
                emit_metrics=emit_metrics,
                observations=observations,
                disposition="rollback",
            )
            # Type name only, same reason as the OFX parse guard above: Polars
            # conversion errors quote the offending cell ("could not convert
            # 'SAFEWAY #123'"), and this message now reaches the wire through
            # `per_file_failure`. The rejection row recorded above keeps the
            # diagnostic detail on the operator's side of the boundary.
            raise ValueError(f"Transform failed: {type(e).__name__}") from e

        # Stage 5: Load — one account record per unique account
        unique_ids = sorted(acct_id_to_name.keys())
        # Reuse the Phase 1 parse (label_last4) with the account-number column as
        # fallback — same last4 the resolver saw, never a second parse pass.
        acct_id_to_last4: dict[str, str | None] = {}
        for aid in acct_id_to_name:
            l4 = label_parsed_by_key[aid][1] or number_last4_by_key.get(aid)
            acct_id_to_last4[aid] = f"****{l4}" if l4 else None
        # institution_name per account: per-account institution applies only when
        # the multi-account branch actually ran (no explicit --account-name/
        # --account-id); an explicit account on a multi-account-detected format
        # keeps the shared format/file institution (Decision 8). Single-account
        # uses the shared institution for its one row.
        #
        # `raw_institution_name` holds both halves, decided in Phase 1 — it also
        # feeds the mint report, which must state this value before this stage
        # writes it. Its shared half falls back to `institution` (resolved from
        # the format or the filename at Stage 1) because matched_format is
        # None for an unregistered import. Without that fallback the account's
        # dim row stores institution_name=NULL, and a later cross-source twin
        # can't match it on (institution, last4) — breaking the CSV-first
        # matching direction.
        account_institutions = [raw_institution_name(aid) for aid in unique_ids]
        account_df = pl.DataFrame({
            "account_id": unique_ids,
            "account_name": [acct_id_to_name[aid] for aid in unique_ids],
            # Decided in Phase 1 alongside the mint report, for the same reason
            # institution_name is: dim_accounts names the account by this column
            # and the report has to state that name before this stage writes it.
            "account_label": [source_label_by_key.get(aid) for aid in unique_ids],
            "account_number": [None] * len(unique_ids),
            "account_number_masked": [acct_id_to_last4[aid] for aid in unique_ids],
            "account_type": [None] * len(unique_ids),
            "institution_name": account_institutions,
            "currency": [None] * len(unique_ids),
            "source_file": [str(file_path)] * len(unique_ids),
            "source_type": [source_type] * len(unique_ids),
            "source_origin": [source_origin] * len(unique_ids),
            "import_id": [import_id] * len(unique_ids),
        })

        rows_imported = extractor.load_transactions(transform_result.transactions)
        extractor.load_accounts(account_df)

        extractor.finalize_import_batch(
            import_id=import_id,
            rows_total=len(df),
            rows_imported=rows_imported,
            rows_rejected=transform_result.rows_rejected,
            rows_skipped_trailing=read_result.rows_skipped_trailing,
            rejection_details=[
                {"row_number": str(r.row_number), "reason": r.reason}
                for r in transform_result.rejection_details
            ]
            or None,
            detection_confidence=resolved.confidence,
            number_format=resolved.number_format,
            date_format=resolved.date_format,
            sign_convention=resolved.sign_convention,
            balance_validated=transform_result.balance_validated,
            emit_metrics=emit_metrics,
            observations=observations,
        )

        # Record import metrics
        if observations is None:
            record_counter(
                IMPORT_RECORDS_TOTAL,
                labels={"source_type": source_type},
                amount=rows_imported,
                emit_metrics=emit_metrics,
                observations=None,
            )
            record_observation(
                IMPORT_DURATION_SECONDS,
                time.monotonic() - _t0,
                labels={"source_type": source_type},
                emit_metrics=emit_metrics,
                observations=None,
            )

        result.accounts = len(unique_ids)
        result.transactions = rows_imported
        result.details = {"transactions": rows_imported, "accounts": len(unique_ids)}
        result.sign_correction_suggested = transform_result.sign_correction_suggested
        result.field_mapping = dict(resolved.field_mapping)

        if rows_imported > 0:
            result.date_range = self._query_date_range(
                TABULAR_TRANSACTIONS.full_name, "transaction_date", file_path
            )

        # Auto-save detected format for future imports.
        # Save when EITHER the detector was high/medium confidence (it
        # produced a complete proposal on its own) OR the user supplied an
        # explicit Override (they ratified the resolved mapping themselves,
        # so the resolved mapping is just as trustworthy regardless of the
        # initial detection tier). Previously this gated only on raw
        # detector tier, so a user calling import_confirm with a complete
        # override on a low-tier file got their import to succeed but the
        # --save-format flag was silently ignored.
        user_ratified_via_override = bool(overrides) or reviewed_plan is not None
        if (
            save_format
            and not matched_format
            and (
                resolved.confidence in ("high", "medium") or user_ratified_via_override
            )
            and rows_imported > 0
        ):
            try:
                detected_fmt = TabularFormat(
                    name=source_origin,
                    # Institution is best-effort metadata; the per-account label
                    # (account_name) must NEVER land here — a format describes a
                    # column layout, not an account (bug #5). "unknown" when no
                    # institution resolved; the exporter/format identity is `name`.
                    institution_name=institution or "unknown",
                    file_type=format_info.file_type,
                    delimiter=format_info.delimiter,
                    encoding=format_info.encoding,
                    header_signature=list(df.columns),
                    field_mapping=resolved.field_mapping,
                    sign_convention=resolved.sign_convention,
                    date_format=resolved.date_format,
                    number_format=resolved.number_format,
                    multi_account=resolved.is_multi_account,
                    source="detected",
                    times_used=1,
                )
                # Auto-detected format is a system-learned side-effect of the
                # import (source="detected"), not a user's explicit format edit —
                # audit it as actor="system" (Invariant 10).
                save_format_to_db(
                    self._db,
                    detected_fmt,
                    actor="system",
                    in_outer_txn=in_outer_txn,
                )
                logger.info(f"Auto-saved format {source_origin!r} for future imports")
            except Exception:  # noqa: BLE001 — format save is best-effort; import already succeeded
                logger.debug("Could not auto-save format", exc_info=True)

        return result

    def _raise_pdf_bridge_escalation(
        self,
        canonical: Path,
        doc: "PdfDocument",
        decision: "RouteDecision",
        *,
        emit_metrics: bool = True,
        observations: MetricObservations | None = None,
    ) -> NoReturn:
        """Hand a bridge-eligible PDF to the driving agent. Always raises.

        Builds the bridge payload, writes the ``smart_import_parse`` egress
        audit row (Req 14), bumps ``PDF_BRIDGE_EGRESS_TOTAL{outcome="proposed"}``,
        and raises ``ImportConfirmationRequiredError`` carrying the payload.
        Shared by ``pdf_preview`` (inspection) and ``_import_pdf`` (import) so
        both surface the identical hand-off — the egress is the audited event
        regardless of whether the agent later ratifies via ``import_confirm``.
        """
        from moneybin.config import get_settings
        from moneybin.extractors.confidence import Confidence, tier_for
        from moneybin.extractors.pdf.bridge import (
            build_bridge_request,
            recipe_for_agent,
        )
        from moneybin.metrics.registry import PDF_BRIDGE_EGRESS_TOTAL
        from moneybin.services.import_confirmation import (
            BridgePayload,
            ConfirmationRequired,
            ImportConfirmationRequiredError,
        )

        # Routing guarantees matched_format_name is non-None for
        # replay_reconciliation_failed, but its annotation allows None —
        # falling back to propose_recipe when it's missing avoids ever emitting
        # a replay envelope with no saved recipe to show, which would
        # contradict the BridgeRequest contract.
        is_replay = (
            decision.reason == "replay_reconciliation_failed"
            and decision.matched_format_name is not None
        )
        request_kind = "replay_failed_re_derive" if is_replay else "propose_recipe"
        # For replay failures, surface both the saved format name AND the actual
        # recipe patterns the agent needs to inspect and propose a refreshed
        # version. Carrying only the name forces a first-contact parse,
        # defeating the point of the replay path.
        # recipe_for_agent, not model_dump: `sign_ratified` is not the agent's to
        # see or return (bridge.parse_bridge_response rejects a response naming it).
        saved_recipe = (
            {
                "name": decision.matched_format_name,
                "recipe": recipe_for_agent(decision.recipe)
                if decision.recipe is not None
                else None,
            }
            if is_replay
            else None
        )
        bridge_request = build_bridge_request(
            doc,
            request_kind=request_kind,
            saved_recipe_for_re_derive=saved_recipe,
        )
        payload = BridgePayload(payload=dataclasses.asdict(bridge_request))
        self._audit.record_audit_event(
            action="smart_import_parse",
            target=("raw", "pdf_seeds", str(canonical)),
            before=None,
            after={
                "request_kind": request_kind,
                "fingerprint": bridge_request.fingerprint,
                "source_file": bridge_request.source_file,
                "decision_reason": decision.reason,
            },
            actor="system",
            # Req 14: context carries routing reason + confidence so analytics
            # can filter bridge egress by either dimension via json_extract on
            # app.audit_log.context_json.
            context={
                "decision_reason": decision.reason,
                "confidence": decision.confidence,
            },
        )
        record_counter(
            PDF_BRIDGE_EGRESS_TOTAL,
            labels={"outcome": "proposed"},
            emit_metrics=emit_metrics,
            observations=observations,
            disposition="rollback",
        )
        bands = get_settings().import_.confidence
        confidence_obj = Confidence(
            score=decision.confidence,
            tier=tier_for(decision.confidence, t_high=bands.t_high, t_med=bands.t_med),
            flagged=(),
            missing_required=(),
        )
        raise ImportConfirmationRequiredError(
            ConfirmationRequired(
                channel="pdf",
                confidence=confidence_obj,
                proposed=payload,
                reason=(
                    "validation_failure"
                    if request_kind == "replay_failed_re_derive"
                    else "unknown_layout"
                ),
            )
        )

    def pdf_preview(
        self,
        file_path: Path,
        *,
        confirm: bool = False,
        sign: str | None = None,
        source_bytes: bytes | None = None,
    ) -> PdfPreviewResult:
        """Run the Phase 2a routing state machine on a PDF without importing.

        Four outcomes — same machinery as ``_import_pdf`` but no side effects
        on raw tables and no ``raw.import_log`` row:

        - Deterministic success (``decision.outcome == "transactions"``):
          returns ``PdfPreviewResult(deterministic=True, ...)`` with the row
          count and fingerprint. The caller can then call ``import_files``
          to actually load.
        - Bridge-eligible failure (``decision.outcome == "seed"`` with a
          ``_BRIDGE_ELIGIBLE_REASONS`` reason): escalates by raising
          ``ImportConfirmationRequiredError`` carrying a ``BridgePayload``
          (a typed ``BridgeRequest`` wrapped in the channel-agnostic
          envelope). Writes a ``smart_import_parse`` audit row (Req 14)
          and increments ``PDF_BRIDGE_EGRESS_TOTAL{outcome="proposed"}``
          before raising — the egress is the audited event regardless of
          whether the agent ratifies.

        Args:
            file_path: Path used for provenance and file-type routing.
            confirm: Ratify a proposed sign inversion.
            sign: Explicit sign convention override.
            source_bytes: Immutable PDF object to extract instead of reopening path.
        - Non-bridge-eligible failure (``no_transaction_table`` / ``no_rows``
          / ``unsupported_number_format``): returns
          ``PdfPreviewResult(deterministic=False, ...)``. The bridge would
          not help on these (the document isn't transaction-shaped, or has
          no extractable content), so we surface the gap honestly rather
          than ship an empty payload.
        - Auto-derived credit-card statement: raises
          ``ImportConfirmationRequiredError`` carrying a
          ``SignConventionProposal`` — the same gate ``_import_pdf`` applies, so
          preview and import agree on what the statement will do to the ledger.
          ``confirm=True`` (or an explicit ``sign=``) previews past it.

        This is a read-mostly path: side effects are the audit row on
        escalation (Req 14) and the metric bump. No ``raw.*`` rows land.

        Args:
            file_path: Path to the PDF to preview.
            confirm: Ratify an auto-derived sign inversion instead of raising.
            sign: Override the detected sign convention (``SignConventionType``).
        """
        from moneybin.extractors.pdf.extractor import PDFExtractor
        from moneybin.extractors.pdf.routing import route_pdf_import

        canonical = file_path.resolve()
        if source_bytes is None:
            doc = PDFExtractor().extract(canonical)
        else:
            doc = PDFExtractor().extract(canonical, source_bytes=source_bytes)
        decision = route_pdf_import(doc, self._db)
        decision = self._gate_pdf_sign_convention(decision, sign=sign, confirm=confirm)

        if decision.outcome == "transactions":
            return PdfPreviewResult(
                file_path=str(canonical),
                deterministic=True,
                decision_reason=decision.reason,
                confidence=decision.confidence,
                row_count=len(decision.rows),
                fingerprint=decision.fp,
            )

        if decision.reason in _BRIDGE_ELIGIBLE_REASONS:
            # Bridge escalation — hand the document to the driving agent.
            # Always raises ImportConfirmationRequiredError after auditing the
            # egress (Req 14) and bumping the metric. Shared with _import_pdf
            # so preview and import surface the identical bridge payload.
            self._raise_pdf_bridge_escalation(canonical, doc, decision)

        # Non-bridge-eligible seed fallback — return the honest gap.
        return PdfPreviewResult(
            file_path=str(canonical),
            deterministic=False,
            decision_reason=decision.reason,
            confidence=decision.confidence,
            row_count=0,
            fingerprint=decision.fp,
        )

    def apply_pdf_bridge_response(
        self,
        file_path: Path,
        bridge_response: dict[str, Any],
        *,
        save_format: bool = True,
        account_id: str | None = None,
        account_bindings: dict[str, str] | None = None,
        source_bytes: bytes | None = None,
        in_outer_txn: bool = False,
        emit_metrics: bool = True,
        observations: MetricObservations | None = None,
        confirm: bool = False,
    ) -> BridgeApplyResult:
        """Apply a driving agent's bridge response: validate, reconcile, load.

        Terminal step of the Phase 2b bridge round-trip (Reqs 8, 9). The agent
        previewed a PDF (``pdf_preview`` raised the bridge payload), extracted
        rows per the recipe it authored, and returns ``{recipe, rows}`` here.

        Trust model — re-execute, don't trust returned rows. The agent's rows
        are the *expectation*; we re-extract the document and re-run the agent's
        recipe ourselves (``route_forced_recipe``) to get the *actual* rows,
        then:

        - **Reconciliation gate (Req 9) is the authority.** It runs on the
          re-executed rows. Any non-``transactions`` outcome (reconciliation
          failure, low confidence, missing balances, …) is an invalid proposal:
          nothing loads, ``outcome="invalid"``, ``reject_reason`` carries the
          routing reason, and the egress metric records ``invalid``.
        - **Persist the recipe, load the re-executed rows.** On pass, save the
          recipe to ``app.pdf_formats`` (first contact → ``save_new``; audited,
          Invariant 10) unless ``save_format=False``, then load the re-executed
          rows to ``raw.tabular_transactions`` (``source_type='pdf'``) with a
          reversible ``raw.import_log`` row (Req 17).
        - **Verify expectation vs actual.** If the agent's row count differs
          from the re-executed count, ``rows_diverged=True`` is surfaced (and
          logged) — the saved recipe does not reproduce the agent's own
          extraction. This does not block a load that reconciles; the gate
          already proved the re-executed rows correct.

        Args:
            file_path: Path to the PDF the agent previewed.
            bridge_response: The agent's ``{recipe, rows}`` reply. Validated by
                ``parse_bridge_response`` — a malformed shape or a recipe that
                fails the security bounds (Req 9b) raises ``BridgeResponseError``.
            save_format: Persist the recipe for future deterministic replay.
                False mirrors ``--no-save-format`` (one-off / sensitive
                statement; layout fingerprint never lands in ``app.pdf_formats``).
            account_id: Pin the rows to an existing ``dim_accounts`` row when
                the statement carries no account anchor (mirrors the tabular
                and deterministic-PDF ``account_id`` semantics).
            account_bindings: Answers to a prior account-confirmation gate,
                keyed by the statement's source-native account key.
            source_bytes: Immutable PDF object captured by the preview.
            in_outer_txn: Join a caller-owned transaction for every write.
            emit_metrics: Emit Prometheus observations during this call.
            observations: Buffer observations for a caller-owned transaction.
            confirm: Human-only ratification of an inferred sign inversion.
        """
        from moneybin.extractors.pdf.bridge import (
            BridgeResponseError,
            parse_bridge_response,
        )
        from moneybin.extractors.pdf.extractor import PDFExtractor
        from moneybin.extractors.pdf.routing import route_forced_recipe
        from moneybin.loaders import import_log
        from moneybin.metrics.registry import (
            PDF_BRIDGE_EGRESS_TOTAL,
            PDF_IMPORT_TOTAL,
        )

        # 1. Validate the agent's response. Raises BridgeResponseError on a bad
        #    shape or a recipe that fails the security bounds (Req 9b) — the
        #    caller (CLI / MCP) maps it to a user-facing error. A parse failure
        #    is an "invalid" egress per the metric's documented semantics, so
        #    bump it here (it raises before the reconciliation gate's own bump).
        try:
            response = parse_bridge_response(bridge_response)
        except BridgeResponseError:
            record_counter(
                PDF_BRIDGE_EGRESS_TOTAL,
                labels={"outcome": "invalid"},
                emit_metrics=emit_metrics,
                observations=observations,
                disposition="rollback",
            )
            raise
        expected_row_count = len(response.rows)

        # 2. Re-extract + re-execute the recipe ourselves. The agent's rows are
        #    the expectation; these re-executed rows are what we reconcile and
        #    load — so the persisted recipe is proven to reproduce them.
        canonical = file_path.resolve()
        try:
            immutable_source_bytes = (
                source_bytes if source_bytes is not None else canonical.read_bytes()
            )
            file_sha256 = source_sha256(canonical, immutable_source_bytes)
            doc = PDFExtractor().extract(canonical, source_bytes=immutable_source_bytes)
            decision = route_forced_recipe(doc, response.recipe)
        except Exception:
            # Mirror _import_pdf: a failed extraction/route is a failed PDF
            # import. Bump the metric (rung="bridge") before propagating so the
            # bridge path doesn't silently diverge from the deterministic one.
            record_counter(
                PDF_IMPORT_TOTAL,
                labels={"outcome": "failed", "rung": "bridge"},
                emit_metrics=emit_metrics,
                observations=observations,
                disposition="rollback",
            )
            raise
        actual_row_count = len(decision.rows)
        rows_diverged = expected_row_count != actual_row_count

        # 3. Reconciliation gate decides (Req 9). Anything other than a clean
        #    transactions route is an invalid proposal — nothing loads.
        if decision.outcome != "transactions":
            record_counter(
                PDF_BRIDGE_EGRESS_TOTAL,
                labels={"outcome": "invalid"},
                emit_metrics=emit_metrics,
                observations=observations,
                disposition="rollback",
            )
            logger.info(
                f"PDF bridge apply rejected: reason={decision.reason} "
                f"expected_rows={expected_row_count} "
                f"actual_rows={actual_row_count}"
            )
            return BridgeApplyResult(
                outcome="invalid",
                import_id=None,
                rows_loaded=0,
                format_name=None,
                expected_row_count=expected_row_count,
                actual_row_count=actual_row_count,
                rows_diverged=rows_diverged,
                reject_reason=decision.reason,
            )

        # The bridge response is agent-authored, not a human ratification. A
        # recipe that inverts every amount therefore follows the same gate as
        # deterministic PDFs before it can persist or load any rows.
        decision = self._gate_pdf_sign_convention(
            decision,
            sign=None,
            confirm=confirm,
            emit_metrics=emit_metrics,
            observations=observations,
        )
        if (
            confirm
            and decision.recipe is not None
            and decision.recipe.sign_convention == "negative_is_income"
            and not decision.card_markers
        ):
            # A marker-free bridge recipe has no deterministic replay evidence,
            # so this human approval is the durable replay bypass.
            decision = dataclasses.replace(
                decision,
                recipe=decision.recipe.model_copy(update={"sign_ratified": True}),
            )

        # 4. Load + persist via the shared transactions path (rung="bridge").
        #    begin_import only here: the invalid path above writes nothing, so
        #    it needs no import_log row. The two ValueError guards inside
        #    _import_pdf_transactions (decision.recipe / decision.fp is None)
        #    fire before its own finalize_import try/except, but neither can
        #    fire here: we already gated on outcome=="transactions" above, and
        #    route_forced_recipe attaches both recipe and fp on that outcome —
        #    so begin_import's row can't be stranded in "importing".
        resolved_alias = _pdf_alias(canonical)

        # Account-identity gate, same position as the deterministic path's: after
        # routing settles, before begin_import. A bridge recipe is agent-authored,
        # so the account identity it implies is no more ratified than a
        # deterministic one — and an agent must never self-pick an identity.
        # One resolver for the identity and the gate: the key reuse inside
        # _pdf_source_account reads the same accepted links the gate does, and
        # the identity the user ratifies has to be the one that gets bound.
        pdf_resolver = AccountResolver(self._db, actor="system")
        identity = _pdf_source_account(
            decision,
            resolver=pdf_resolver,
            resolved_alias=resolved_alias,
            account_id_override=account_id,
            document_sha256=file_sha256,
            source_file=str(canonical),
        )
        gated = self._gate_account_proposals(
            pdf_resolver,
            [identity.source],
            account_bindings,
            channel="pdf",
            fallback_keys=identity.fallback_keys,
            incoming_transactions=_incoming_pdf_transactions(decision),
            emit_metrics=emit_metrics,
            observations=observations,
        )

        result = ImportResult(file_path=str(canonical), file_type="pdf")
        import_id = import_log.begin_import(
            self._db,
            source_file=str(canonical),
            source_type="pdf",
            source_origin=resolved_alias,
            account_names=[resolved_alias],
            file_sha256=file_sha256,
        )
        result.import_id = import_id

        # Assign the return value (it mutates `result` in place and returns it)
        # so rows_loaded below doesn't silently depend on the mutation contract
        # — matches the _import_pdf call site.
        result = self._import_pdf_transactions(
            canonical=canonical,
            resolved_alias=resolved_alias,
            import_id=import_id,
            result=result,
            decision=decision,
            doc=doc,
            save_format=save_format,
            bound_source=gated[0],
            rung="bridge",
            in_outer_txn=in_outer_txn,
            emit_metrics=emit_metrics,
            observations=observations,
        )

        record_counter(
            PDF_BRIDGE_EGRESS_TOTAL,
            labels={"outcome": "applied"},
            emit_metrics=emit_metrics,
            observations=observations,
        )
        if rows_diverged:
            logger.warning(
                f"PDF bridge apply divergence: agent returned "
                f"{expected_row_count} rows but the recipe reproduced "
                f"{actual_row_count} (import_id={import_id[:8]}...). Loaded the "
                f"re-executed rows; the saved recipe does not reproduce the "
                f"agent's claimed extraction."
            )

        # Report the name _import_pdf_transactions actually persisted (set only
        # on a confirmed save_new). None when save_format is off, when save_new
        # skipped a pre-existing fingerprint (the replay-failure bridge case —
        # stale recipe untouched until #40's bump_version), or when the save
        # failed for any other reason — so the result never claims a save that
        # didn't land. Agents can't read the warning log; this is their signal.
        return BridgeApplyResult(
            outcome="applied",
            import_id=import_id,
            rows_loaded=result.transactions,
            format_name=result.pdf_format_name,
            expected_row_count=expected_row_count,
            actual_row_count=actual_row_count,
            rows_diverged=rows_diverged,
            reject_reason=None,
            accounts_created=result.accounts_created,
        )

    def _gate_pdf_sign_convention(
        self,
        decision: "RouteDecision",
        *,
        sign: str | None,
        confirm: bool,
        emit_metrics: bool = True,
        observations: MetricObservations | None = None,
    ) -> "RouteDecision":
        """Ratify, override, or gate an auto-derived sign inversion.

        A `negative_is_income` recipe flips every amount. The convention is not
        recoverable from the numbers (a card and a checking statement have
        identical sign distributions and both reconcile), so the inference rests
        on the document's disclosures alone — and the decision is persisted and
        replays on every future statement of this format. It is therefore never
        applied silently on first contact.

        A REPLAY (`matched_format_name` set) was confirmed once already and loads
        without asking again — the confirm is once per format, not per statement.

        ``route_forced_recipe`` (the bridge apply) also reports
        ``matched_format_name is None``. Its recipe is agent-authored, not a
        human ratification, so it deliberately follows this first-contact gate.

        The gate deliberately does NOT route through ``resolve_or_confirm``: that
        seam lets an agent self-accept at ``high``, and a silently agent-accepted
        ledger inversion is the exact outcome this gate exists to prevent.

        Bumps ``PDF_SIGN_GATE_TOTAL`` at each of its three exits: ``overridden``
        (explicit ``sign=`` accepted), ``confirmed`` (``confirm=True`` ratified
        an auto-derived inversion), ``proposed`` (raised for confirmation).
        """
        from typing import get_args

        from moneybin.extractors.pdf.routing import amount_shape_matches_sign_convention
        from moneybin.metrics.registry import PDF_SIGN_GATE_TOTAL

        recipe = decision.recipe
        if recipe is None or decision.outcome != "transactions":
            return decision

        if sign is not None:
            if sign not in get_args(SignConventionType):
                raise UserError(
                    f"Invalid sign convention: {sign!r}. "
                    f"Valid values: {list(get_args(SignConventionType))}.",
                    code=error_codes.IMPORT_INVALID_SIGN_CONVENTION,
                )
            # The loader reads convention-specific row keys: `amount` for the
            # single-column conventions, `debit`/`credit` for the split. An
            # override that names the shape this recipe did not extract would
            # sum absent keys and write every amount as 0 — silent, and worse
            # than the inversion the override was meant to correct.
            if not amount_shape_matches_sign_convention(recipe.fields, sign):
                # The guard only fires when the shapes DON'T match, so the
                # recipe's actual shape is the opposite of what `sign` reads —
                # a `split_debit_credit` override failing means the recipe
                # extracts a single amount column, not a debit/credit pair.
                actual_shape = (
                    "single amount column"
                    if sign == "split_debit_credit"
                    else "debit/credit pair"
                )
                raise UserError(
                    f"Sign convention {sign!r} does not fit this statement's "
                    f"columns — its recipe extracts a {actual_shape} the "
                    f"convention does not read.",
                    code=error_codes.IMPORT_INVALID_SIGN_CONVENTION,
                )
            record_counter(
                PDF_SIGN_GATE_TOTAL,
                labels={"outcome": "overridden"},
                emit_metrics=emit_metrics,
                observations=observations,
            )
            # Ratify only a DISAGREEING override. sign_ratified marks the
            # convention as a human assertion the replay guard must defer to on
            # every future statement of this format
            # (auto_derive.recipe_polarity_fits) — and deferring means the guard
            # no longer refuses a fingerprint-identical statement of the OTHER
            # kind. Only a `sign=` that contradicts the convention in force needs
            # that bypass: without it the corrected recipe is disowned on the same
            # card markers that caused the false positive, derivation re-runs, and
            # the gate raises again next month, and every month after.
            #
            # A `sign=` that AGREES buys nothing — the convention it names is
            # already the one in force, so it replays on its own — and granting it
            # the bypass would strip the guard for free, in the direction that
            # costs (see the `confirm` branch below, which declines for the same
            # reason). `or recipe.sign_ratified` preserves an EARLIER human
            # ratification: an agreeing `--sign` re-typed out of habit must not
            # revoke it.
            ratified = recipe.sign_ratified or sign != recipe.sign_convention
            return dataclasses.replace(
                decision,
                recipe=recipe.model_copy(
                    update={"sign_convention": sign, "sign_ratified": ratified}
                ),
            )

        if decision.rederived_from_sign is not None:
            # A self-heal that changed polarity. Checked BEFORE the short-circuit
            # below for two reasons, both load-bearing: that check exempts
            # replays (this IS one — matched_format_name is set), and it only
            # proposes for negative_is_income, so an income → expense repair
            # would pass through it silently and un-invert a convention a human
            # ratified. The confirm here is per-change, not per-format: the
            # earlier ratification was about the OLD convention.
            if confirm:
                record_counter(
                    PDF_SIGN_GATE_TOTAL,
                    labels={"outcome": "confirmed"},
                    emit_metrics=emit_metrics,
                    observations=observations,
                )
                return decision
            record_counter(
                PDF_SIGN_GATE_TOTAL,
                labels={"outcome": "proposed"},
                emit_metrics=emit_metrics,
                observations=observations,
                disposition="rollback",
            )
            raise ImportConfirmationRequiredError(
                ConfirmationRequired(
                    channel="pdf",
                    confidence=_CARD_SIGN_CONFIDENCE,
                    proposed=SignConventionProposal(
                        sign_convention=recipe.sign_convention,
                        evidence=decision.card_markers,
                        sample_rows=_sign_sample_rows(decision.rows),
                        # Without this every surface renders the first-contact
                        # card framing, which is backwards for an
                        # income → expense repair: it would describe --confirm
                        # as ratifying a card convention while --confirm
                        # actually accepts the as-printed one, and offer no
                        # command that keeps the convention already in force.
                        prior_sign_convention=decision.rederived_from_sign,
                    ),
                    reason="sign_convention",
                    error_message=(
                        f"The saved layout for this statement stopped reading it "
                        f"correctly and was re-derived. The re-derived version "
                        f"records amounts as {recipe.sign_convention!r}, not "
                        f"{decision.rederived_from_sign!r} — every amount's sign "
                        f"flips relative to how this format imported before. A "
                        f"person must confirm or override the change before "
                        f"anything is imported."
                    ),
                )
            )

        is_auto_derived = decision.matched_format_name is None
        if recipe.sign_convention != "negative_is_income" or not is_auto_derived:
            return decision

        if confirm:
            record_counter(
                PDF_SIGN_GATE_TOTAL,
                labels={"outcome": "confirmed"},
                emit_metrics=emit_metrics,
                observations=observations,
            )
            # Deliberately NOT sign_ratified. `confirm` ratifies "this IS a card",
            # and the resulting negative_is_income recipe already replays cleanly —
            # the marker check re-confirms it on every real card statement. Setting
            # the flag here would buy nothing and would disarm the guard in the
            # direction that actually costs: a checking statement sharing this
            # format's fingerprint would import every paycheck as an expense.
            return decision

        record_counter(
            PDF_SIGN_GATE_TOTAL,
            labels={"outcome": "proposed"},
            emit_metrics=emit_metrics,
            observations=observations,
            disposition="rollback",
        )
        raise ImportConfirmationRequiredError(
            ConfirmationRequired(
                channel="pdf",
                confidence=_CARD_SIGN_CONFIDENCE,
                proposed=SignConventionProposal(
                    sign_convention="negative_is_income",
                    evidence=decision.card_markers,
                    sample_rows=_sign_sample_rows(decision.rows),
                ),
                reason="sign_convention",
                # A deterministic PDF has no bridge recipe to re-run, so the CLI
                # confirms it natively (--confirm / --sign). No literal path here —
                # the path isn't in scope at the gate; each surface fills in the
                # concrete command from the file it holds.
                error_message=(
                    "This looks like a credit-card statement "
                    f"(matched: {', '.join(decision.card_markers)}). Charges will be "
                    "recorded as expenses and payments as credits — every amount's "
                    "sign is inverted. A person must confirm or override this "
                    "inversion before anything is imported."
                ),
            )
        )

    def _gate_tabular_sign_convention(
        self,
        *,
        detected_sign: SignConventionType,
        sign: str | None,
        human_sign_confirmation: bool,
        is_first_contact: bool,
        evidence: str,
        emit_metrics: bool = True,
        observations: MetricObservations | None = None,
    ) -> None:
        """Require a human to ratify an inferred tabular ledger inversion."""
        from moneybin.metrics.registry import TABULAR_SIGN_GATE_TOTAL

        if detected_sign != "negative_is_income" or not is_first_contact:
            return
        if sign is not None:
            record_counter(
                TABULAR_SIGN_GATE_TOTAL,
                labels={"outcome": "overridden"},
                emit_metrics=emit_metrics,
                observations=observations,
            )
            return
        if human_sign_confirmation:
            record_counter(
                TABULAR_SIGN_GATE_TOTAL,
                labels={"outcome": "confirmed"},
                emit_metrics=emit_metrics,
                observations=observations,
            )
            return

        record_counter(
            TABULAR_SIGN_GATE_TOTAL,
            labels={"outcome": "proposed"},
            emit_metrics=emit_metrics,
            observations=observations,
            disposition="rollback",
        )
        raise ImportConfirmationRequiredError(
            ConfirmationRequired(
                channel="tabular",
                confidence=_CARD_SIGN_CONFIDENCE,
                proposed=SignConventionProposal(
                    sign_convention="negative_is_income",
                    evidence=(evidence,),
                    sample_rows=[],
                ),
                reason="sign_convention",
                error_message=(
                    "This tabular format would invert every amount: negative values "
                    "become income and positive values become expenses. A person "
                    "must confirm or override this inversion before anything is "
                    "imported."
                ),
            )
        )

    def _persist_replayed_sign_override(
        self,
        decision: "RouteDecision",
        *,
        import_id: str,
        in_outer_txn: bool = False,
    ) -> None:
        """Re-persist a saved format's recipe after an explicit `sign=` on a REPLAY.

        The recipe is otherwise written only on first contact, so a `sign=` on a
        later statement of a saved format would correct that one import and revert
        the next month — and since there is no edit/delete path for saved PDF
        formats, a wrong first-contact ratification would be permanent. The
        override IS the revocation path; this is what makes it one.

        Bumping the version (rather than overwriting) keeps the correction audited
        and undo-reversible, and preserves the prior recipe in
        ``audit_log.before_value``. Best-effort like every other bookkeeping write
        in this block — the read that decides whether to bump included: the rows
        already landed with the corrected convention, and no failure here may roll
        them back or abort the import before ``finalize_import``.

        Callers gate on ``save_format``: this rewrites a saved recipe, which is
        precisely what ``--no-save-format`` asks the import not to do.
        """
        name = decision.matched_format_name
        recipe = decision.recipe
        fp = decision.fp
        # A replay carries all three by construction — the format was matched BY
        # the fingerprint. The guard satisfies the type checker; it is not a branch.
        if name is None or recipe is None or fp is None:  # pragma: no cover
            return
        new_recipe = recipe.model_dump()
        try:
            saved = self._pdf_formats.get_by_fingerprint(fp)
            if saved is not None and saved.extraction_recipe == new_recipe:
                # The saved recipe already says what the user just asserted (a
                # `--sign` re-typed out of habit). Bumping would spend a version
                # and an audit row on a no-op whose before_value equals its
                # after_value.
                return
            self._pdf_formats.bump_version(
                name=name,
                new_recipe=new_recipe,
                reason="explicit sign override supplied on a replay of this format",
                # Not "system": the convention is a human assertion, not a
                # detection. The service can't see which surface it arrived on.
                actor="import",
                in_outer_txn=in_outer_txn,
            )
            logger.info(
                f"PDF format {name!r} recipe re-persisted with the user's sign "
                f"override (import_id={import_id[:8]}...)"
            )
        except Exception:  # noqa: BLE001 — format bump is bookkeeping; data is committed
            if in_outer_txn:
                raise
            logger.warning(
                f"PDF bump_version failed for format {name!r} (import_id="
                f"{import_id[:8]}...) — the sign override applied to this import "
                f"but will not replay onto the next statement",
                exc_info=True,
            )

    def _persist_self_healed_recipe(
        self,
        decision: "RouteDecision",
        *,
        import_id: str,
        in_outer_txn: bool = False,
    ) -> None:
        """Write back a recipe that routing re-derived after finding it stale.

        Without this the repair is per-import: the rows land, but
        ``app.pdf_formats`` still holds the recipe that couldn't read them, so the
        next statement of this layout fails reconciliation all over again. The
        point of self-healing is that the format stops being broken.

        Bumped rather than overwritten, like every other recipe write here — the
        prior recipe stays in ``audit_log.before_value`` so an operator can see
        what changed and undo it. Best-effort: the rows are already committed and
        no bookkeeping failure may roll them back.

        Callers gate on ``save_format``: this rewrites a saved recipe, which is
        exactly what ``--no-save-format`` asks the import not to do.
        """
        name = decision.matched_format_name
        recipe = decision.recipe
        # The reason comes from the decision because only routing knows which
        # trigger fired; a hardcoded one told every operator reading the audit
        # log that reconciliation failed, including for repairs where it didn't.
        reason = decision.rederived_reason
        # A re-derived decision carries all three by construction (see
        # _attempt_self_heal). The guard satisfies the type checker.
        if name is None or recipe is None or reason is None:  # pragma: no cover
            return
        try:
            self._pdf_formats.bump_version(
                name=name,
                new_recipe=recipe.model_dump(),
                reason=reason,
                # "system": a detection repaired its own earlier detection. No
                # human asserted anything here.
                actor="system",
                in_outer_txn=in_outer_txn,
            )
            logger.info(
                f"PDF format {name!r} recipe repaired by re-derivation "
                f"(import_id={import_id[:8]}...)"
            )
        except Exception:  # noqa: BLE001 — format bump is bookkeeping; data is committed
            if in_outer_txn:
                raise
            logger.warning(
                f"PDF bump_version failed for re-derived format {name!r} "
                f"(import_id={import_id[:8]}...) — this import landed but the "
                f"saved recipe is still stale and will fail again",
                exc_info=True,
            )

    def _import_pdf(
        self,
        file_path: Path,
        *,
        source_bytes: bytes | None = None,
        save_format: bool = True,
        account_id: str | None = None,
        actor_kind: "ActorKind" = "human",
        sign: str | None = None,
        confirm: bool = False,
        account_bindings: dict[str, str] | None = None,
        include_unmaterialized_account_candidates: bool = False,
        in_outer_txn: bool = False,
        emit_metrics: bool = True,
        observations: MetricObservations | None = None,
    ) -> ImportResult:
        """Import a native-text PDF via the Phase 2a routing state machine.

        High-confidence PDFs with reconciling rows land in raw.tabular_transactions
        and save their auto-derived recipe to app.pdf_formats (first contact) or
        reuse the saved recipe (replay). These rows feed SQLMesh's
        stg_tabular__transactions model; refresh runs when import_file or
        import_files detect file_type="pdf".

        Bridge escalation (Phase 2b, Option B): when a driving agent is present
        (``actor_kind="agent"``) and the deterministic rung can't crack a
        bridge-eligible layout, the document is handed to the agent
        (``ImportConfirmationRequiredError``) instead of silently seeding — the
        agent can extract real transactions and ratify via ``import_confirm``.
        With no agent (bare CLI / inbox drain), it falls through to the Phase 2a
        seed path (raw.pdf_seeds); the agent-aware CLI signal is tracked as
        follow-up work. Non-bridge-eligible failures (no transaction table, no
        rows) always seed.

        Args:
            file_path: Path to the PDF file.
            source_bytes: Immutable PDF object to extract instead of reopening path.
            save_format: When False, suppresses every write to
                ``app.pdf_formats`` — the auto-derived recipe save on first
                contact AND the recipe re-persist a ``sign=`` triggers on a
                replay. Mirrors the tabular ``--no-save-format`` /
                ``save_format=False`` semantics so a user/agent importing a
                one-off or sensitive statement can avoid leaving (or mutating)
                an ``app.pdf_formats`` row that fingerprints the layout for
                future replays.
            account_id: Optional override for the account_id the rows are
                attached to. Required when reconciliation passes via balances
                alone (no account anchor captured) but the user still wants
                the rows attached to an existing ``dim_accounts`` row. Without
                this or an account-binding answer, the import stops for account
                confirmation; a confirmed new account uses the document alias
                only as its display-name fallback. Mirrors the tabular path's
                ``account_id`` semantics.
            include_unmaterialized_account_candidates: Include PDF accounts loaded
                earlier in the current batch before the final core refresh.
            account_bindings: Answers to a prior account-confirmation gate,
                keyed by the statement's source-native account key: an existing
                account_id to adopt, or "new".
            actor_kind: 'agent' when a driving agent that can fulfill a bridge
                extraction is present (MCP, agent-driven CLI) — enables bridge
                escalation. 'human'/default keeps the Phase 2a seed fallback.
            sign: Override the detected sign convention (a
                ``SignConventionType``). The in-band recovery from a
                false-positive card detection — see
                ``_gate_pdf_sign_convention``.
            confirm: Ratify an auto-derived ``negative_is_income`` (credit-card)
                inversion. Without it, such a statement raises
                ``ImportConfirmationRequiredError`` and loads nothing.
            in_outer_txn: Join a caller-owned transaction for every write.
            emit_metrics: Emit Prometheus observations during this call.
            observations: Buffer observations for a caller-owned transaction.
        """
        from moneybin.extractors.pdf.extractor import PDFExtractor
        from moneybin.extractors.pdf.routing import route_pdf_import
        from moneybin.extractors.pdf.seed_store import write_pdf_seed
        from moneybin.loaders import import_log
        from moneybin.metrics.registry import PDF_IMPORT_TOTAL, PDF_SEED_ROWS_TOTAL
        from moneybin.tables import PDF_SEEDS

        canonical = file_path.resolve()
        result = ImportResult(file_path=str(canonical), file_type="pdf")
        resolved_alias = _pdf_alias(canonical)

        # Extract + route BEFORE opening an import_log row. A bridge escalation
        # and an extraction failure both load nothing, so neither should leave
        # a dangling import row — begin_import below marks the commitment to a
        # write (transactions or seed).
        try:
            immutable_source_bytes = (
                source_bytes if source_bytes is not None else canonical.read_bytes()
            )
            file_sha256 = source_sha256(canonical, immutable_source_bytes)
            doc = PDFExtractor().extract(canonical, source_bytes=immutable_source_bytes)
            decision = route_pdf_import(doc, self._db)
        except Exception:
            record_counter(
                PDF_IMPORT_TOTAL,
                labels={"outcome": "failed", "rung": "deterministic"},
                emit_metrics=emit_metrics,
                observations=observations,
                disposition="rollback",
            )
            raise

        # Scanned / image-only PDF: no selectable text layer. Nothing for the
        # deterministic rung to structure, nothing to seed, and the text bridge
        # carries document text, not page images — so even a driving agent can't
        # read it (vision backends are out of scope, Req 5). Surface an explicit,
        # actionable unsupported outcome — for agent and human callers alike,
        # hence before the bridge-escalation gate below — instead of a generic
        # "No tables extracted" failure or a silent empty seed. Raised before
        # begin_import, so no dangling import_log row is left behind.
        if not doc.text_lines and not doc.tables:
            record_counter(
                PDF_IMPORT_TOTAL,
                labels={"outcome": "unsupported", "rung": "deterministic"},
                emit_metrics=emit_metrics,
                observations=observations,
                disposition="rollback",
            )
            raise UserError(
                "This PDF has no selectable text layer — it looks scanned or "
                "image-only. Extracting transactions from it needs a "
                "vision-capable backend (an agent or bridge that can read the "
                "page image), which MoneyBin does not yet provide. Re-import a "
                "PDF that has a selectable text layer, or run the file through "
                "OCR first.",
                code=error_codes.IMPORT_PDF_NO_TEXT_LAYER,
                hint=(
                    "💡 Scanned PDFs need OCR or a vision-capable agent backend "
                    "(not yet supported)."
                ),
            )

        # Bridge escalation (Option B): with a driving agent present, hand a
        # bridge-eligible layout to the agent instead of silently seeding.
        # Always raises. No agent → fall through to the Phase 2a seed path.
        if (
            actor_kind == "agent"
            and decision.outcome != "transactions"
            and decision.reason in _BRIDGE_ELIGIBLE_REASONS
        ):
            self._raise_pdf_bridge_escalation(
                canonical,
                doc,
                decision,
                emit_metrics=emit_metrics,
                observations=observations,
            )

        # Sign gate: an auto-derived inversion needs ratification, an explicit
        # `sign=` overrules the detector. Sits OUTSIDE the extract/route
        # try/except above — a confirmation is not a failed import, and counting
        # it as one (PDF_IMPORT_TOTAL{outcome="failed"}) would misreport the
        # gate's whole purpose. Raises before begin_import: nothing loads, and no
        # import_log row is stranded in "importing".
        decision = self._gate_pdf_sign_convention(
            decision,
            sign=sign,
            confirm=confirm,
            emit_metrics=emit_metrics,
            observations=observations,
        )

        # A saved `sign=` override just replayed: the polarity guard stood down for
        # this document (auto_derive.recipe_polarity_fits short-circuits on
        # sign_ratified), so the load runs on a human decision the detector may
        # still disagree with. Surface it — an override that acts invisibly on
        # every future statement is the magic this codebase refuses.
        #
        # Two conditions, both load-bearing. A REPLAY (matched_format_name set):
        # on first contact the user typed `sign=` in this very invocation. And no
        # `sign=` in THIS call: the gate sets sign_ratified in the same call it
        # accepts an override, so without this the user who just typed `--sign` on
        # a saved format would be told the convention came from a *saved* override
        # they are in fact supplying right now.
        if (
            sign is None
            and decision.recipe is not None
            and decision.recipe.sign_ratified
            and decision.matched_format_name is not None
        ):
            result.sign_override_replayed = True

        # Account-identity gate, in the position the sign gate above established:
        # after routing settles, before begin_import. Only the transactions path
        # resolves an account identity — a seeded document writes no link, so
        # there is nothing to ratify. Exact bytes use a document digest; a
        # proven-complete routing-scoped identifier can carry identity
        # across statements. A new partial-only statement remains reviewable
        # rather than silently adopting.
        # Carries the gated identity across `begin_import` to the dispatch
        # below. Declared here because the gate and the load sit in two separate
        # `outcome == "transactions"` blocks with the import-log write between
        # them — the compiler cannot see that the second implies the first, which
        # is exactly the coupling that let the load re-derive its own copy.
        pdf_bound: SourceAccount | None = None
        if decision.outcome == "transactions":
            pdf_resolver = AccountResolver(
                self._db,
                actor="system",
                include_unmaterialized_candidates=(
                    include_unmaterialized_account_candidates
                ),
            )
            identity = _pdf_source_account(
                decision,
                resolver=pdf_resolver,
                resolved_alias=resolved_alias,
                account_id_override=account_id,
                document_sha256=file_sha256,
                source_file=str(canonical),
            )
            gated = self._gate_account_proposals(
                pdf_resolver,
                [identity.source],
                account_bindings,
                channel="pdf",
                fallback_keys=identity.fallback_keys,
                incoming_transactions=_incoming_pdf_transactions(decision),
                emit_metrics=emit_metrics,
                observations=observations,
            )
            pdf_bound = gated[0]

        # Committing to a write — open the import_log row now.
        import_id = import_log.begin_import(
            self._db,
            source_file=str(canonical),
            source_type="pdf",
            source_origin=resolved_alias,
            account_names=[resolved_alias],
            file_sha256=file_sha256,
        )
        result.import_id = import_id

        # ------------------------------------------------------------------
        # Dispatch on routing decision
        # ------------------------------------------------------------------

        if decision.outcome == "transactions":
            if pdf_bound is None:  # pragma: no cover — set under this same test
                raise ValueError(
                    "PDF routing reached the transactions load without an "
                    "account-identity gate."
                )
            return self._import_pdf_transactions(
                canonical=canonical,
                resolved_alias=resolved_alias,
                import_id=import_id,
                result=result,
                decision=decision,
                doc=doc,
                save_format=save_format,
                bound_source=pdf_bound,
                sign_override=sign,
                in_outer_txn=in_outer_txn,
                emit_metrics=emit_metrics,
                observations=observations,
            )

        # Seed path (Phase 1 fallback) ——————————————————————————————————
        extracted = 0
        inserted = 0
        try:
            extracted, inserted = write_pdf_seed(
                self._db, doc, alias=resolved_alias, import_id=import_id
            )
            if extracted == 0:
                # A scanned / no-text-layer PDF is caught earlier with a clearer
                # unsupported error; reaching here means the document HAS a text
                # layer but no table structure the seed extractor could parse.
                raise ValueError(
                    "No tables extracted from PDF. The document has a text layer "
                    "but no table structure the importer could parse into rows."
                )
        except Exception:
            try:
                self._db.execute(
                    f"DELETE FROM {PDF_SEEDS.full_name} WHERE import_id = ?",
                    [import_id],
                )
            except Exception:  # noqa: BLE001 — cleanup is best-effort
                logger.warning(
                    f"PDF cleanup DELETE failed for import_id={import_id[:8]}...",
                    exc_info=True,
                )
            try:
                import_log.finalize_import(
                    self._db,
                    import_id,
                    status="failed",
                    rows_total=0,
                    rows_imported=0,
                )
            except Exception:  # noqa: BLE001 — failure-path finalize is best-effort
                logger.warning(
                    f"PDF finalize_import(failed) raised for import_id={import_id[:8]}...",
                    exc_info=True,
                )
            record_counter(
                PDF_IMPORT_TOTAL,
                labels={"outcome": "failed", "rung": "deterministic"},
                emit_metrics=emit_metrics,
                observations=observations,
                disposition="rollback",
            )
            raise

        import_log.finalize_import(
            self._db,
            import_id,
            status="complete",
            rows_total=extracted,
            rows_imported=inserted,
        )
        record_counter(
            PDF_IMPORT_TOTAL,
            labels={"outcome": "seed", "rung": "deterministic"},
            emit_metrics=emit_metrics,
            observations=observations,
        )
        record_counter(
            PDF_SEED_ROWS_TOTAL,
            labels={"alias": resolved_alias},
            amount=inserted,
            emit_metrics=emit_metrics,
            observations=observations,
        )
        result.details = {"seed_rows": inserted, "seed_rows_extracted": extracted}
        result.transactions = 0
        logger.info(
            f"PDF import complete (seed): alias={resolved_alias} "
            f"import_id={import_id[:8]}... extracted={extracted} inserted={inserted}"
        )
        return result

    def _import_pdf_transactions(
        self,
        *,
        canonical: Path,
        resolved_alias: str,
        import_id: str,
        result: ImportResult,
        decision: "RouteDecision",
        doc: "PdfDocument",
        save_format: bool = True,
        bound_source: SourceAccount,
        rung: Literal["deterministic", "bridge"] = "deterministic",
        sign_override: str | None = None,
        in_outer_txn: bool = False,
        emit_metrics: bool = True,
        observations: MetricObservations | None = None,
    ) -> ImportResult:
        """Write PDF transaction rows to raw.tabular_transactions.

        Called by _import_pdf when the routing decision is 'transactions'
        (rung="deterministic") and by apply_pdf_bridge_response after a
        bridge-authored recipe reconciles (rung="bridge"). ``rung`` only
        labels the PDF_IMPORT_TOTAL metric — the load path is identical.
        Saves a new format recipe on first contact (decision.matched_format_name is None),
        unless ``save_format`` is False — mirrors the tabular ``--no-save-format``
        semantics so a user/agent importing a one-off or sensitive statement can
        avoid persisting the layout fingerprint.

        ``bound_source`` is the account ``_gate_account_proposals`` returned,
        already carrying the caller's binding answer. It is a parameter rather
        than a derivation because this function used to re-derive it — same
        inputs, same helpers, a second copy — and OFX and tabular already thread
        the gate's return value instead. Two derivations that must agree are
        correct only by call-site convention; the one a reviewer worried about
        is the one where an edit reaches a single copy and rows bind to an
        account the gate never surfaced. There is now nothing to disagree with.

        ``sign_override`` is the caller's explicit ``sign=`` (already applied to
        ``decision.recipe`` by the gate). On a REPLAY it re-persists the corrected
        recipe — see ``_persist_replayed_sign_override`` — and ``save_format``
        gates that write too, not just the first-contact save.
        """
        import polars as pl

        from moneybin.loaders import import_log
        from moneybin.metrics.registry import PDF_IMPORT_TOTAL
        from moneybin.tables import (
            ACCOUNT_LINKS,
            TABULAR_ACCOUNTS,
            TABULAR_TRANSACTIONS,
        )

        if decision.recipe is None:
            # Should never happen: route_pdf_import only emits outcome="transactions"
            # when a recipe was successfully derived or loaded.
            raise ValueError(
                "PDF routing returned outcome='transactions' but recipe is None"
            )

        # The identity the confirm gate produced, threaded in rather than
        # re-derived, so the ratified account and the bound one are one object.
        source_account = bound_source
        account_id = source_account.source_account_key
        # Stable document origin and fingerprint, both used further down for the
        # raw account row and the format-recipe save. Read the origin from the
        # gated identity rather than recomputing it, so there is one source of truth.
        identity_origin = source_account.source_origin
        fp = decision.fp
        if fp is None:  # pragma: no cover — _pdf_source_account already raised
            raise ValueError(
                "PDF routing returned outcome='transactions' but fp is None"
            )
        # That key is a source-NATIVE key (DP-1), exactly like the tabular
        # path's — never a canonical account id. Registering it with
        # AccountResolver is what writes the native->canonical mapping staging
        # joins on. Skipping that step let the raw key flow into dim_accounts as
        # an account in its own right, so the same card arriving from a second
        # source had nothing to be proposed against and both halves loaded.
        # Guarded like the OFX resolver loop: this runs after begin_import() but
        # outside the ingestion try below, so an unhandled raise here (e.g.
        # _write_native_mapping's conflict guard) would strand import_log at
        # status="importing" forever and never emit the failure metric.
        try:
            resolved_account = AccountResolver(
                self._db,
                actor="system",
                emit_metrics=emit_metrics,
                observations=observations,
            ).resolve(source_account, in_outer_txn=in_outer_txn)
        except Exception:
            try:
                import_log.finalize_import(
                    self._db, import_id, status="failed", rows_total=0, rows_imported=0
                )
            except Exception:  # noqa: BLE001 — failure-path finalize is best-effort
                logger.warning(
                    f"PDF finalize_import(failed) raised for import_id={import_id[:8]}...",
                    exc_info=True,
                )
            record_counter(
                PDF_IMPORT_TOTAL,
                labels={"outcome": "failed", "rung": rung},
                emit_metrics=emit_metrics,
                observations=observations,
                disposition="rollback",
            )
            raise
        # Every other resolve() call site records this; the spec's observability
        # section requires it for each AccountResolver outcome.
        record_counter(
            ACCOUNT_LINK_OUTCOMES_TOTAL,
            labels={"result": resolved_account.outcome},
            emit_metrics=emit_metrics,
            observations=observations,
        )
        if minted := _created_account(source_account, resolved_account):
            result.accounts_created = (minted,)

        # Accepted links preserve every historical PDF source tuple now owned by
        # this account. Their reversed predecessors preserve prior canonical ids.
        # Both can appear in old transaction hashes; today's issuer detector and
        # current merge target are therefore insufficient migration evidence.
        current_file_refs = {
            (str(row[0]), str(row[1]))
            for row in self._db.execute(
                f"SELECT DISTINCT source_origin, account_id "  # noqa: S608  # TableRef + parameterized source path
                f"FROM {TABULAR_TRANSACTIONS.full_name} "
                "WHERE source_type = 'pdf' AND source_file = ?",
                [str(canonical)],
            ).fetchall()
        }
        from moneybin.services.pdf_account_identity import legacy_pdf_identifier_key

        legacy_identifier_refs = {
            (str(row[0]), str(row[1]))
            for row in self._db.execute(
                f"SELECT DISTINCT source_origin, account_id, account_number_masked "  # noqa: S608  # TableRef only
                f"FROM {TABULAR_ACCOUNTS.full_name} "
                "WHERE source_type = 'pdf' AND account_number_masked IS NOT NULL"
            ).fetchall()
            if legacy_pdf_identifier_key(issuer=str(row[0]), identifier=str(row[2]))
            == str(row[1])
        }
        legacy_alias_refs = {
            (str(row[0]), str(row[1]))
            for row in self._db.execute(
                f"SELECT DISTINCT source_origin, account_id "  # noqa: S608  # TableRef only
                f"FROM {TABULAR_ACCOUNTS.full_name} "
                "WHERE source_type = 'pdf' AND account_number_masked IS NULL"
            ).fetchall()
        }
        pdf_hash_namespaces: dict[tuple[str, str], set[str]] = {}
        for (
            historical_origin,
            historical_ref,
            historical_account_id,
        ) in self._db.execute(
            f"SELECT current.source_origin, current.ref_value, "  # noqa: S608  # TableRef + parameterized account id
            "historical.account_id "
            f"FROM {ACCOUNT_LINKS.full_name} AS current "
            f"JOIN {ACCOUNT_LINKS.full_name} AS historical "
            "ON historical.ref_kind = current.ref_kind "
            "AND historical.source_type = current.source_type "
            "AND historical.source_origin = current.source_origin "
            "AND historical.ref_value = current.ref_value "
            "WHERE current.status = 'accepted' "
            "AND current.ref_kind = 'source_native' "
            "AND current.source_type = 'pdf' AND current.account_id = ?",
            [resolved_account.account_id],
        ).fetchall():
            source_ref = (str(historical_origin), str(historical_ref))
            legacy_key = source_account.legacy_source_account_key
            if not (
                source_ref == (identity_origin, account_id)
                or source_ref in current_file_refs
                or str(historical_account_id) != resolved_account.account_id
                or (
                    legacy_key is not None
                    and source_ref[1] == legacy_key
                    and (
                        source_ref in legacy_identifier_refs
                        or (
                            source_account.legacy_source_account_key_is_filename_alias
                            and source_ref in legacy_alias_refs
                        )
                    )
                )
            ):
                continue
            pdf_hash_namespaces.setdefault(source_ref, {source_ref[1]}).add(
                str(historical_account_id)
            )

        sign_conv: str = decision.recipe.sign_convention

        # Per-content-key dedup counter: when two rows in the same statement
        # share (date, amt, desc, canonical account) the first uses the bare content
        # hash; each subsequent collision appends an occurrence index. Position
        # within the statement (`row_number`) is intentionally NOT in the hash
        # so a recipe change that shifts row order (or rejects one extra
        # boundary line) doesn't renumber every following transaction_id and
        # defeat INSERT OR IGNORE on re-import (Req identifiers.md "content
        # hash" contract).
        # Statement scope keeps two legitimately-distinct same-content
        # transactions (e.g. two recurring $5 coffees on the same day in
        # different monthly statements for the same account) on separate
        # transaction_ids. Without this, prep.stg_tabular__transactions
        # dedups by (transaction_id, account_id) and one of the two
        # disappears from core/reports. The resolved canonical account stays
        # stable when the same statement is regenerated with different PDF
        # metadata; the document digest remains only a source-native link key.
        period_marker = ""
        if (
            decision.metadata.period_start is not None
            and decision.metadata.period_end is not None
        ):
            period_marker = (
                f"{decision.metadata.period_start.isoformat()}-"
                f"{decision.metadata.period_end.isoformat()}"
            )
        content_dup_counter: dict[str, int] = {}
        rows_list: list[dict[str, Any]] = []
        historical_transaction_ids: dict[tuple[str, str], list[set[str]]] = {
            ref: [] for ref in pdf_hash_namespaces
        }
        _zero = Decimal("0")
        for idx, row in enumerate(decision.rows, start=1):
            amt = _normalize_pdf_amount(row, sign_conv)
            # rows are canonical-keyed by routing._canonicalize_rows. Credit-card
            # layouts with both columns produce "date" and "post_date"; we keep
            # them on distinct DB columns so neither overwrites the other.
            date_val = row.get("date")
            post_date_val = row.get("post_date")
            desc = row.get("description")

            date_iso = (
                date_val.isoformat()
                if date_val is not None and hasattr(date_val, "isoformat")
                else str(date_val)
            )
            # Build the content key from the RAW per-cell values (pre
            # sign-normalisation) so a later bug-fix to _normalize_amount
            # — for instance correcting how negative_is_income statements
            # flip signs — does not silently rotate every transaction_id.
            # If it did, INSERT OR IGNORE would no longer recognise the
            # already-imported rows and every re-import would create
            # duplicates. Per identifiers.md the content hash must be
            # stable across re-imports of the same source bytes.
            raw_amount = row.get("amount", _zero)
            raw_debit = row.get("debit", _zero)
            raw_credit = row.get("credit", _zero)
            content_key = (
                f"{period_marker}|{date_iso}|{raw_amount}|{raw_debit}|"
                f"{raw_credit}|{desc}|{resolved_account.account_id}"
            )
            dup_idx = content_dup_counter.get(content_key, 0)
            content_dup_counter[content_key] = dup_idx + 1
            raw_hash = content_key if dup_idx == 0 else f"{content_key}|{dup_idx}"
            digest = hashlib.sha256(raw_hash.encode()).hexdigest()[:16]
            transaction_id = f"pdf_{digest}"
            for source_ref, hash_namespaces in pdf_hash_namespaces.items():
                row_ids: set[str] = set()
                for hash_namespace in hash_namespaces:
                    historical_content_key = (
                        f"{period_marker}|{date_iso}|{raw_amount}|{raw_debit}|"
                        f"{raw_credit}|{desc}|{hash_namespace}"
                    )
                    historical_raw_hash = (
                        historical_content_key
                        if dup_idx == 0
                        else f"{historical_content_key}|{dup_idx}"
                    )
                    historical_digest = hashlib.sha256(
                        historical_raw_hash.encode()
                    ).hexdigest()[:16]
                    row_ids.add(f"pdf_{historical_digest}")
                historical_transaction_ids[source_ref].append(row_ids)

            rows_list.append({
                "transaction_id": transaction_id,
                "account_id": account_id,
                "transaction_date": date_val,
                "post_date": post_date_val,
                "amount": amt,
                "description": str(desc) if desc is not None else None,
                "source_file": str(canonical),
                "source_type": "pdf",
                # Must equal the account_links.source_origin written above —
                # prep.stg_tabular__transactions JOINs on the exact triple.
                "source_origin": identity_origin,
                "import_id": import_id,
                "row_number": idx,
            })

        transactions_extracted = len(rows_list)

        try:
            # The account-identity upgrade changed the account token inside PDF
            # transaction hashes. If the exact legacy hash already exists, keep
            # that raw row authoritative instead of inserting its replacement
            # under a second id and double-counting the statement after refresh.
            superseded_row_indexes: set[int] = set()
            for (
                historical_origin,
                historical_key,
            ), row_id_sets in historical_transaction_ids.items():
                historical_ids: set[str] = set()
                for row_ids in row_id_sets:
                    historical_ids.update(row_ids)
                transaction_ids = sorted(historical_ids)
                historical_placeholders = ",".join(["?"] * len(transaction_ids))
                historical_rows = self._db.execute(
                    f"SELECT transaction_id, source_file FROM {TABULAR_TRANSACTIONS.full_name} "  # noqa: S608  # placeholders are code-owned; values parameterized
                    "WHERE source_type = 'pdf' AND source_origin = ? AND account_id = ? "
                    f"AND transaction_id IN ({historical_placeholders})",
                    [
                        historical_origin,
                        historical_key,
                        *transaction_ids,
                    ],
                ).fetchall()
                existing_historical_ids = {str(row[0]) for row in historical_rows}
                current_file_ids = {
                    str(row[0])
                    for row in historical_rows
                    if str(row[1]) == str(canonical)
                }
                superseded_row_indexes.update(
                    index
                    for index, row_ids in enumerate(row_id_sets)
                    if row_ids & current_file_ids
                    or (row_ids - {str(rows_list[index]["transaction_id"])})
                    & existing_historical_ids
                )
            rows_list = [
                row
                for index, row in enumerate(rows_list)
                if index not in superseded_row_indexes
            ]
            # on_conflict="ignore": tabular_transactions PRIMARY KEY is
            # (transaction_id, account_id, source_file). Pre-count by the SAME
            # key the table conflicts on — counting transaction_id alone would
            # under-report when the same PDF is re-imported from a different
            # path (different source_file → insert succeeds with a duplicate
            # raw row, but tx_id pre-count matched and rows_inserted=0).
            # Routing guarantees rows_list is non-empty here (every zero-row
            # outcome sets RouteDecision.outcome="seed"), but guard locally
            # anyway: an empty tx_ids list would generate
            # `WHERE transaction_id IN () AND ...`, which DuckDB rejects, and
            # the failure would land AFTER raw rows had already been ingested
            # — leaving import_log stuck in 'importing' status.
            tx_ids = [r["transaction_id"] for r in rows_list]
            src_file = str(canonical)
            if tx_ids:
                placeholders = ",".join(["?"] * len(tx_ids))
                count_before_row = self._db.execute(
                    f"SELECT COUNT(*) FROM {TABULAR_TRANSACTIONS.full_name} "
                    f"WHERE transaction_id IN ({placeholders}) "
                    f"AND account_id = ? AND source_file = ?",  # noqa: S608  # placeholders are ?-bound; tx_ids is parameter list
                    [*tx_ids, account_id, src_file],
                ).fetchone()
                rows_already_present = count_before_row[0] if count_before_row else 0
            else:
                rows_already_present = 0
            if rows_list:
                self._db.ingest_dataframe(
                    TABULAR_TRANSACTIONS.full_name,
                    pl.DataFrame(rows_list),
                    on_conflict="ignore",
                )
                rows_inserted = len(rows_list) - rows_already_present
            else:
                rows_inserted = 0

            # Account row to raw.tabular_accounts — without this, the SQLMesh
            # stg_tabular__accounts model never produces a core.dim_accounts
            # entry for this account_id, and reports that inner-join dim_accounts
            # (reports.spending_trend, etc.) silently drop the PDF transactions.
            # institution_name carries the issuer (Chase / American Express / …),
            # NOT the masked account number — fp["issuer"] is the canonical source.
            institution = fp.get("issuer", "unknown")
            # account_name is the human-readable display label. The captured
            # account-number mask (e.g. "****1234") is data, not a label, and
            # belongs in account_number_masked; resolved_alias is the canonical
            # slug the rest of the import is keyed on. Reduce to a last-4
            # display mask before storing — the captured value may be a full
            # institution account number ("Account Number: 123456789") and the
            # raw schema's account_number_masked column is contract-defined as
            # last-4 only.
            raw_account_id = (
                str(decision.metadata.account_id)
                if decision.metadata.account_id
                else None
            )
            # One expression, shared with the mint report `_pdf_source_account`
            # builds — the report has to state the name this row will produce
            # before the row exists. on_conflict="ignore" below means a type
            # Plaid/OFX already set is never clobbered.
            account_type = _pdf_account_type(decision)
            account_df = pl.DataFrame({
                "account_id": [account_id],
                "account_name": [source_account.account_name],
                # Distinct from account_name above: dim_accounts.sql's
                # tabular_accounts CTE reads account_label specifically (never
                # account_name) to decide display_name_is_user_set. Without
                # this, a PDF's captured "Account Name:"/"Account Nickname:"
                # line lived only on this live SourceAccount -- once
                # materialized, the very next import or accounts_links_run
                # backfill would read display_name_is_user_set=False for an
                # account a person genuinely named. account_name_is_user_set
                # is exactly that same provenance test, already computed.
                "account_label": [
                    mask_embedded_account_number(source_account.account_name)
                    if source_account.account_name_is_user_set
                    else None
                ],
                "account_number": [None],
                "account_number_masked": [_to_account_number_mask(raw_account_id)],
                "account_type": [account_type],
                "institution_name": [str(institution) if institution else None],
                "currency": [decision.metadata.currency_code],
                "source_file": [str(canonical)],
                "source_type": ["pdf"],
                # Matches the transactions rows and the account_links row —
                # prep.stg_tabular__accounts JOINs on the same triple.
                "source_origin": [identity_origin],
                "import_id": [import_id],
            })
            # on_conflict="ignore" means a PDF account row imported before
            # account_label existed on this write (account_id, source_file
            # already present with account_label=NULL) keeps that NULL on
            # re-import -- this write only reaches fresh rows. Backfilling
            # already-materialized NULLs is a data-repair question (a targeted
            # UPDATE or a migration), not something a per-import write can fix
            # without risking a full upsert's loss of the original import_id/
            # extracted_at history "ignore" exists to protect. Known
            # limitation, not a regression: an account imported before this
            # fix is no worse off than it was before this fix shipped.
            self._db.ingest_dataframe(
                TABULAR_ACCOUNTS.full_name, account_df, on_conflict="ignore"
            )

        except Exception:
            for table_ref in (TABULAR_TRANSACTIONS, TABULAR_ACCOUNTS):
                try:
                    self._db.execute(
                        f"DELETE FROM {table_ref.full_name} WHERE import_id = ?",
                        [import_id],
                    )
                except Exception:  # noqa: BLE001 — cleanup is best-effort
                    logger.warning(
                        f"PDF cleanup DELETE failed on {table_ref.full_name} "
                        f"for import_id={import_id[:8]}...",
                        exc_info=True,
                    )
            try:
                import_log.finalize_import(
                    self._db, import_id, status="failed", rows_total=0, rows_imported=0
                )
            except Exception:  # noqa: BLE001 — failure-path finalize is best-effort
                logger.warning(
                    f"PDF finalize_import(failed) raised for import_id={import_id[:8]}...",
                    exc_info=True,
                )
            record_counter(
                PDF_IMPORT_TOTAL,
                labels={"outcome": "failed", "rung": rung},
                emit_metrics=emit_metrics,
                observations=observations,
                disposition="rollback",
            )
            raise

        # Format save + record_use happen AFTER the data-write try/except so a
        # bookkeeping failure (schema mismatch on app.pdf_formats, etc.) can't
        # trigger the cleanup DELETE on rows that already landed successfully.
        # Both are best-effort: the import succeeds either way.
        # First-contact format name (issuer slug + fingerprint hash). Shared
        # with apply_pdf_bridge_response via _pdf_format_name so the two paths
        # can never drift on the naming scheme — see that helper.
        first_contact_format_name = _pdf_format_name(fp)

        # Backfill format columns on raw.import_log now that routing has
        # decided. Tabular knows its format before begin_import; PDFs only
        # know it post-routing, so without this update every PDF import_log
        # entry would carry NULL format_name/format_source and users could
        # not tell whether a replay or auto-derive served the import.
        if decision.matched_format_name is not None:
            pdf_format_name: str | None = decision.matched_format_name
            pdf_format_source = "saved"
        elif save_format:
            pdf_format_name = first_contact_format_name
            pdf_format_source = "detected"
        else:
            # First-contact import that intentionally won't persist a recipe;
            # leave format_name NULL so it doesn't look saveable to operators
            # tailing import_log.
            pdf_format_name = None
            pdf_format_source = "detected"
        try:
            import_log.update_format(
                self._db,
                import_id,
                format_name=pdf_format_name,
                format_source=pdf_format_source,
            )
        except Exception:  # noqa: BLE001 — observability stamp must not roll back data
            if in_outer_txn:
                raise
            logger.warning(
                f"PDF import_log.update_format failed for import_id="
                f"{import_id[:8]}... — format columns left NULL",
                exc_info=True,
            )

        if decision.matched_format_name is not None:
            # save_format gates this exactly as it gates the first-contact
            # save_new below — re-persisting the recipe IS a format write, and
            # --no-save-format is what a user reaches for on a one-off or
            # sensitive statement they don't want teaching the saved profile.
            if sign_override is not None and save_format:
                # Takes precedence deliberately: it re-persists decision.recipe,
                # which on a re-derived decision IS the repaired recipe — so the
                # repair still lands and the two paths don't spend two versions
                # (and two audit rows) on one import.
                self._persist_replayed_sign_override(
                    decision,
                    import_id=import_id,
                    in_outer_txn=in_outer_txn,
                )
            elif decision.rederived and save_format:
                self._persist_self_healed_recipe(
                    decision,
                    import_id=import_id,
                    in_outer_txn=in_outer_txn,
                )
            try:
                self._pdf_formats.record_use(decision.matched_format_name)
            except Exception:  # noqa: BLE001 — observability bump must not roll back data
                if in_outer_txn:
                    raise
                logger.warning(
                    f"PDF record_use failed for format "
                    f"{decision.matched_format_name!r} (import_id="
                    f"{import_id[:8]}...) — counter not bumped",
                    exc_info=True,
                )
        elif not save_format:
            # First contact, but caller (CLI --no-save-format / MCP
            # save_format=False / agent) requested no persistence. Skip
            # the save_new call so the layout fingerprint never lands in
            # app.pdf_formats. Mirrors the tabular path's behaviour.
            logger.info(
                f"PDF first-contact recipe save suppressed by save_format=False "
                f"(import_id={import_id[:8]}...) — layout will be re-derived "
                f"on every future import of this format"
            )
        else:
            # First-contact auto-derive: persist the recipe under the
            # already-computed first_contact_format_name. The hash is built
            # from serialize_fingerprint(fp) so it stays byte-for-byte
            # identical to the JSON the repo uses for lookup + storage —
            # any drift breaks ConstraintException-based duplicate
            # detection silently.
            format_name = first_contact_format_name
            try:
                self._pdf_formats.save_new(
                    name=format_name,
                    recipe=decision.recipe.model_dump(),
                    fingerprint=fp,
                    institution_name=fp.get("issuer", "unknown"),
                    document_kind="transactions",
                    front_end="pdfplumber",
                    routing="transactions",
                    sign_convention=decision.recipe.sign_convention,
                    date_format=None,  # per-field date_format lives in recipe
                    number_format=decision.recipe.number_format,
                    # The one thing the two rungs must NOT share. Everything else
                    # on this path is deliberately identical, but self-heal's
                    # Guard A keys on `source` to decide whether it may replace a
                    # recipe with a fresh derivation — and a bridge recipe's
                    # anchors were authored by an agent and vetted by a human, so
                    # a machine guess must never silently overwrite them.
                    source="bridge" if rung == "bridge" else "detected",
                    actor="system",  # auto-detected: system-driven (Invariant 10)
                    in_outer_txn=in_outer_txn,
                )
                # Record the actually-persisted name so callers
                # (apply_pdf_bridge_response) report format_name only after a
                # confirmed save — set inside the try, so a ConstraintException
                # (pre-existing) or any swallowed save failure below leaves it None.
                result.pdf_format_name = format_name
                logger.info(
                    f"PDF format saved: name={format_name!r} "
                    f"import_id={import_id[:8]}..."
                )
            except duckdb.ConstraintException:
                # A format with this fingerprint-derived name already exists,
                # yet routing did not match it (matched_format_name is None) —
                # the saved recipe stopped serving this layout: it failed
                # model_validate on replay (→ auto-derive) or stopped
                # reconciling (→ bridge re-derive). The recipe we just ran
                # reconciled, so install it as a NEW version (Req 9a auto-bump):
                # audited + reversible via undo (Invariant 11), never a silent
                # overwrite. This closes the stuck-recipe loop where every future
                # statement of this layout would re-derive/re-escalate forever.
                try:
                    self._pdf_formats.bump_version(
                        name=format_name,
                        new_recipe=decision.recipe.model_dump(),
                        reason=(
                            "replay-guard reconciliation failure — re-derived "
                            f"recipe reconciled (rung={rung})"
                        ),
                        actor="system",  # auto-bump: system-driven (Invariant 10)
                        in_outer_txn=in_outer_txn,
                    )
                    # Record the actually-persisted name so callers
                    # (apply_pdf_bridge_response) report format_name only after a
                    # confirmed persist — the bump landed a new recipe version.
                    result.pdf_format_name = format_name
                    logger.info(
                        f"PDF format {format_name!r} recipe re-derived and "
                        f"bumped to a new version (import_id={import_id[:8]}...)"
                    )
                except Exception:  # noqa: BLE001 — format bump is bookkeeping; data is committed
                    if in_outer_txn:
                        raise
                    logger.warning(
                        f"PDF bump_version failed for format {format_name!r} "
                        f"(import_id={import_id[:8]}...) — stale recipe persists",
                        exc_info=True,
                    )
            except Exception:  # noqa: BLE001 — format save is bookkeeping; data is committed
                if in_outer_txn:
                    raise
                logger.warning(
                    f"PDF save_new failed for format {format_name!r} "
                    f"(import_id={import_id[:8]}...) — recipe not persisted",
                    exc_info=True,
                )

        import_log.finalize_import(
            self._db,
            import_id,
            status="complete",
            rows_total=transactions_extracted,
            rows_imported=rows_inserted,
        )
        record_counter(
            PDF_IMPORT_TOTAL,
            labels={"outcome": "transactions", "rung": rung},
            emit_metrics=emit_metrics,
            observations=observations,
        )

        result.transactions = rows_inserted
        result.accounts = 1
        result.details = {
            "transactions": rows_inserted,
            "transactions_extracted": transactions_extracted,
        }
        logger.info(
            f"PDF import complete (transactions): alias={resolved_alias} "
            f"extracted={transactions_extracted} inserted={rows_inserted} "
            f"import_id={import_id[:8]}..."
        )
        return result

    def import_file(
        self,
        file_path: str | Path,
        *,
        source_bytes: bytes | None = None,
        reviewed_plan: ReviewedTabularPlan | None = None,
        refresh: bool = True,
        institution: str | None = None,
        force: bool = False,
        interactive: bool = False,
        account_id: str | None = None,
        account_name: str | None = None,
        format_name: str | None = None,
        overrides: dict[str, str] | None = None,
        sign: str | None = None,
        date_format: str | None = None,
        number_format: str | None = None,
        save_format: bool = True,
        sheet: str | None = None,
        delimiter: str | None = None,
        encoding: str | None = None,
        no_row_limit: bool = False,
        no_size_limit: bool = False,
        auto_accept: bool = False,
        confirm: bool = False,
        human_sign_confirmation: bool = False,
        actor_kind: ActorKind = "human",
        account_bindings: dict[str, str] | None = None,
        account_metadata: dict[str, dict[str, str]] | None = None,
        in_outer_txn: bool = False,
        emit_metrics: bool = True,
        observations: MetricObservations | None = None,
    ) -> ImportResult:
        """Import a financial data file into DuckDB.

        Auto-detects file type by extension and runs the appropriate
        extract -> load -> transform pipeline.

        Args:
            file_path: Path to the file to import.
            source_bytes: Immutable source object to parse instead of reopening path.
            reviewed_plan: Persisted parse and mapping decisions to replay exactly.
            refresh: Whether to run the post-load refresh pipeline (matching +
                SQLMesh apply + categorization) after loading. Defaults to
                True. PDFs that routed to ``raw.tabular_transactions``
                (deterministic path) trigger refresh so rows propagate through
                SQLMesh into ``dim_accounts``/``fct_transactions``. PDFs that
                fell back to ``raw.pdf_seeds`` (seed path) skip refresh — they
                wrote nothing tabular and a full SQLMesh apply for no purpose
                wastes a refresh cycle (and could raise on unrelated transform
                failures even though no PDF data needs to propagate).
            institution: Institution name override (OFX only). Auto-detected if
                omitted.
            force: Re-import even if the file has been imported before (OFX only).
            interactive: Prompt for institution when resolution fails (OFX only).
            account_id: Explicit account ID for tabular imports (bypasses name
                matching).
            account_name: Account name for single-account tabular files.
            format_name: Explicit format name for tabular imports (bypasses
                auto-detection).
            overrides: Field→column overrides for tabular imports.
            sign: Sign convention override. Tabular: overrides the detected
                format. PDF: overrules the credit-card detector (the in-band
                recovery from a false-positive inversion).
            date_format: Date format override for tabular imports.
            number_format: Number format override for tabular imports.
            save_format: Auto-save detected format for future imports.
            sheet: Excel sheet name for tabular imports.
            delimiter: Explicit delimiter for tabular imports.
            encoding: Explicit encoding for tabular imports.
            no_row_limit: Override row count limit for tabular imports.
            no_size_limit: Override file size limit for tabular imports.
            auto_accept: Auto-accept the top fuzzy account match without prompting
                (CLI: --yes / -y). Defaults to False.
            confirm: Ratify the detected column mapping (tabular) or the
                credit-card sign inversion (PDF).
            human_sign_confirmation: Explicit human approval of an inferred
                tabular sign inversion; never inferred from ``confirm``.
            actor_kind: 'human' (always surfaces) or 'agent' (may self-accept at high tier).
            account_bindings: Map of proposal_ref ("@0", the file's first source
                account) or source_account_key -> canonical account_id (adopt)
                or "new" (mint standalone), ratifying the account-binding
                confirmation. Honored on every channel — tabular, OFX and PDF
                all raise the same gate.
            account_metadata: Map of proposal_ref ("@0") or source_account_key
                -> settings dict captured for accounts minted this import.
                Tabular only; refused with
                ``import_account_signal_unsupported`` elsewhere.
            in_outer_txn: Join a caller-owned transaction for every write.
            emit_metrics: Emit Prometheus observations during this call.
            observations: Buffer observations for a caller-owned transaction.

        Returns:
            ImportResult with summary of what was imported.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the file type is not supported.
        """
        result = self._import_one(
            file_path,
            source_bytes=source_bytes,
            reviewed_plan=reviewed_plan,
            institution=institution,
            force=force,
            interactive=interactive,
            account_id=account_id,
            account_name=account_name,
            format_name=format_name,
            overrides=overrides,
            sign=sign,
            date_format=date_format,
            number_format=number_format,
            save_format=save_format,
            sheet=sheet,
            delimiter=delimiter,
            encoding=encoding,
            no_row_limit=no_row_limit,
            no_size_limit=no_size_limit,
            auto_accept=auto_accept,
            confirm=confirm,
            human_sign_confirmation=human_sign_confirmation,
            actor_kind=actor_kind,
            account_bindings=account_bindings,
            account_metadata=account_metadata,
            in_outer_txn=in_outer_txn,
            emit_metrics=emit_metrics,
            observations=observations,
        )

        # Include PDFs only when the deterministic path landed transactions —
        # seed-path PDFs write nothing tabular, so a refresh would run the
        # full SQLMesh apply for no purpose and could raise on unrelated
        # transform failures even though no PDF data needs to propagate.
        #
        # Gate on "the deterministic path produced rows" (transactions_extracted),
        # not "rows were newly inserted" (result.transactions). raw inserts use
        # INSERT OR IGNORE on the (transaction_id, account_id, source_file)
        # PK, so a re-import after a prior refresh failed reports
        # transactions == 0 even though every row is present. Without this,
        # the user would re-run the same file, see zero inserts, skip
        # refresh, and the rows would stay invisible to core/reports.
        if refresh and (
            result.file_type in ("ofx", "tabular")
            or (
                result.file_type == "pdf"
                and result.details.get("transactions_extracted", 0) > 0
            )
        ):
            # Single-file imports preserve the legacy fail-loud contract so
            # CLI exit codes reflect the broken state. Batch imports use the
            # soft-fail variant via import_files() instead.
            refresh_result = _refresh(self._db)
            # Ahead of the fail-loud raise: the reconciliation runs in the match
            # step and commits there, so a transform apply that dies afterwards
            # leaves the reversal on disk.
            result.transfers_retired = refresh_result.transfers_retired
            result.refresh_steps = _step_outcome(refresh_result)
            if not refresh_result.applied:
                # The raise discards `result`, and this exception escapes past
                # the success path's warning rather than landing in
                # `_single_file_failure`. So the count travels on the exception
                # — the reversal is the user's own decision being undone, and it
                # is disclosed whether or not the transform that followed it
                # succeeded.
                raise ImportRefreshError(
                    f"SQLMesh transforms failed: {refresh_result.error}",
                    transfers_retired=refresh_result.transfers_retired,
                )
            result.core_tables_rebuilt = True

        logger.info(f"Import complete: {result.summary()}")
        return result

    def _import_one(
        self,
        file_path: str | Path,
        *,
        source_bytes: bytes | None = None,
        reviewed_plan: ReviewedTabularPlan | None = None,
        institution: str | None = None,
        force: bool = False,
        interactive: bool = False,
        account_id: str | None = None,
        account_name: str | None = None,
        format_name: str | None = None,
        overrides: dict[str, str] | None = None,
        sign: str | None = None,
        date_format: str | None = None,
        number_format: str | None = None,
        save_format: bool = True,
        sheet: str | None = None,
        delimiter: str | None = None,
        encoding: str | None = None,
        no_row_limit: bool = False,
        no_size_limit: bool = False,
        auto_accept: bool = False,
        confirm: bool = False,
        human_sign_confirmation: bool = False,
        actor_kind: ActorKind = "human",
        account_bindings: dict[str, str] | None = None,
        account_metadata: dict[str, dict[str, str]] | None = None,
        include_unmaterialized_account_candidates: bool = False,
        in_outer_txn: bool = False,
        emit_metrics: bool = True,
        observations: MetricObservations | None = None,
    ) -> ImportResult:
        """Extract + load one file. Does NOT run the refresh pipeline.

        Refresh (matching, SQLMesh apply, categorization) is the caller's
        responsibility — see :func:`moneybin.orchestration.refresh.refresh` and
        ``import_files``.
        """
        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        file_type = _detect_file_type(path)
        reject_unhonored_account_signals(
            file_type,
            account_id=account_id,
            account_name=account_name,
            account_metadata=account_metadata,
        )
        logger.info(f"Importing {_display_label(file_type, path)} file: {path}")

        if file_type == "ofx":
            return self._import_ofx(
                path,
                institution=institution,
                force=force,
                interactive=interactive,
                account_bindings=account_bindings,
            )
        if file_type == "tabular":
            return self._import_tabular(
                path,
                source_bytes=source_bytes,
                reviewed_plan=reviewed_plan,
                account_name=account_name,
                account_id=account_id,
                format_name=format_name,
                overrides=overrides,
                sign=sign,
                date_format_override=date_format,
                number_format_override=number_format,
                save_format=save_format,
                sheet=sheet,
                delimiter=delimiter,
                encoding=encoding,
                no_row_limit=no_row_limit,
                no_size_limit=no_size_limit,
                auto_accept=auto_accept,
                confirm=confirm,
                human_sign_confirmation=human_sign_confirmation,
                actor_kind=actor_kind,
                account_bindings=account_bindings,
                account_metadata=account_metadata,
                in_outer_txn=in_outer_txn,
                emit_metrics=emit_metrics,
                observations=observations,
            )
        if file_type == "pdf":
            return self._import_pdf(
                path,
                source_bytes=source_bytes,
                save_format=save_format,
                account_id=account_id,
                actor_kind=actor_kind,
                sign=sign,
                confirm=confirm,
                account_bindings=account_bindings,
                include_unmaterialized_account_candidates=(
                    include_unmaterialized_account_candidates
                ),
                in_outer_txn=in_outer_txn,
                emit_metrics=emit_metrics,
                observations=observations,
            )
        raise ValueError(f"Unsupported file type: {file_type}")

    def import_files(
        self,
        paths: list[str | Path],
        *,
        refresh: bool = True,
        force: bool = False,
        interactive: bool = False,
        confirm: bool = False,
        actor_kind: ActorKind = "human",
    ) -> BatchImportResult:
        """Import a list of files; run refresh once at end of batch.

        Per-file failures do not abort the batch. Refresh runs only if at
        least one file succeeded AND at least one file was transformable
        (ofx/tabular). On
        SQLMesh failure the per-file outcomes are preserved and the error
        surfaces in ``transforms_error`` on the result envelope.

        Per-file overrides (account_name, institution, format_name, etc.)
        are not available for batch — use ``import_file()`` for single
        imports with overrides.
        """
        from moneybin.metrics.registry import IMPORT_BATCH_SIZE

        IMPORT_BATCH_SIZE.observe(len(paths))
        per_file: list[PerFileResult] = []
        any_succeeded = False
        any_transformable = False
        for raw_path in paths:
            path = Path(raw_path)
            try:
                r = self._import_one(
                    path,
                    force=force,
                    interactive=interactive,
                    confirm=confirm,
                    actor_kind=actor_kind,
                    include_unmaterialized_account_candidates=True,
                )
                # PDFs land in raw.pdf_seeds (transactions=0); report the seed
                # count so batch output reflects actual rows persisted.
                rows_loaded = r.details.get("seed_rows", r.transactions)
                per_file.append(
                    PerFileResult(
                        path=str(path),
                        status="imported",
                        source_type=r.file_type,
                        rows_loaded=rows_loaded,
                        import_id=r.import_id,
                        sign_correction_suggested=r.sign_correction_suggested,
                        sign_override_replayed=r.sign_override_replayed,
                        accounts_created=r.accounts_created,
                    )
                )
                any_succeeded = True
                # Match the single-file refresh gate: the deterministic PDF
                # path is transformable when it produced rows
                # (transactions_extracted), regardless of how many were
                # newly inserted. INSERT OR IGNORE means a re-import after
                # a prior refresh failure has transactions == 0 even though
                # the rows are present and waiting for transform — gating
                # on insert count would skip refresh and leave them invisible.
                if r.file_type in ("ofx", "tabular") or (
                    r.file_type == "pdf"
                    and r.details.get("transactions_extracted", 0) > 0
                ):
                    any_transformable = True
            except ImportConfirmationRequiredError as e:
                # Distinct from generic failure: the file's detector formed
                # a proposal (or surfaced low-tier with no proposal); the
                # caller needs the payload to ratify or override per file.
                from moneybin.services.import_confirmation import (
                    confirmation_payload_dict,
                )

                logger.info(
                    f"Import requires confirmation for {path}: "
                    f"tier={e.outcome.confidence.tier} reason={e.outcome.reason}"
                )
                per_file.append(
                    PerFileResult(
                        path=str(path),
                        status="confirmation_required",
                        source_type=None,
                        confirmation_payload=confirmation_payload_dict(e.outcome),
                    )
                )
            except Exception as e:  # noqa: BLE001 — per-file failure must not abort batch
                error_message, error_code, error_hint, error_details = per_file_failure(
                    e
                )
                # Log the class name, never the message: a classified message is
                # user-safe but still names the path, and logs stay PII-free.
                logger.warning(f"Import failed for {path}: {type(e).__name__}")
                per_file.append(
                    PerFileResult(
                        path=str(path),
                        status="failed",
                        source_type=None,
                        error=error_message,
                        error_code=error_code,
                        hint=error_hint,
                        details=error_details,
                    )
                )

        applied = False
        duration_seconds: float | None = None
        error: str | None = None
        transfers_retired = 0
        refresh_steps: RefreshStepOutcome | None = None
        if refresh and any_succeeded and any_transformable:
            refresh_result = _refresh(self._db)
            applied = refresh_result.applied
            duration_seconds = refresh_result.duration_seconds
            error = refresh_result.error
            transfers_retired = refresh_result.transfers_retired
            refresh_steps = _step_outcome(refresh_result)

        return BatchImportResult(
            per_file=per_file,
            transforms_applied=applied,
            transforms_duration_seconds=duration_seconds,
            transforms_error=error,
            transfers_retired=transfers_retired,
            refresh_steps=refresh_steps,
        )

    def list_formats(
        self,
    ) -> tuple[
        dict[str, "TabularFormat"],
        dict[str, "TabularFormat"],
        list["PdfFormat"],
    ]:
        """Return the complete tabular/PDF format catalog for either surface."""
        from moneybin.extractors.tabular.formats import (  # noqa: PLC0415
            load_builtin_formats,
            load_formats_from_db,
            merge_formats,
        )

        builtin = load_builtin_formats()
        formats = merge_formats(builtin, load_formats_from_db(self._db))
        try:
            pdf_formats = PdfFormatsRepo(self._db).list_all()
        except Exception:  # noqa: BLE001  # PDF catalog is optional to tabular reads.
            logger.debug("PDF formats unavailable; returning tabular formats only")
            pdf_formats = []
        return formats, builtin, pdf_formats

    def plan_saved_format_delete(self, format_name: str) -> SavedFormatDeletePlan:
        """Return the exact current saved-format state for confirmation binding."""
        from moneybin.extractors.tabular.formats import (  # noqa: PLC0415
            load_builtin_formats,
        )
        from moneybin.repositories.tabular_formats_repo import (  # noqa: PLC0415
            TabularFormatsRepo,
        )

        if format_name in load_builtin_formats():
            raise UserError(
                f"Built-in format {format_name!r} cannot be deleted.",
                code=error_codes.IMPORT_SAVED_FORMAT_BUILTIN_IMMUTABLE,
            )
        row = TabularFormatsRepo(self._db).get(format_name)
        if row is None:
            raise UserError(
                f"Saved format {format_name!r} was not found.",
                code=error_codes.IMPORT_SAVED_FORMAT_NOT_FOUND,
            )
        canonical = json.dumps(
            row,
            default=str,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return SavedFormatDeletePlan(
            format_name=format_name,
            state_sha256=hashlib.sha256(canonical).hexdigest(),
        )

    def delete_saved_format_confirmed(
        self,
        format_name: str,
        *,
        actor: str,
        verify: Callable[[SavedFormatDeletePlan], None],
    ) -> str:
        """Revalidate and audit-delete one saved format in the same transaction."""
        from moneybin.repositories.tabular_formats_repo import (  # noqa: PLC0415
            TabularFormatsRepo,
        )

        self._db.begin()
        try:
            live = self.plan_saved_format_delete(format_name)
            verify(live)
            event = TabularFormatsRepo(self._db).delete(
                format_name,
                actor=actor,
                in_outer_txn=True,
            )
            if event is None:  # pragma: no cover - live plan proved row existence
                raise RuntimeError(
                    "saved format disappeared inside its write transaction"
                )
            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise
        return event.operation_id

    def plan_revert(self, import_id: str) -> ImportRevertPlan:
        """Return the exact live state bound to one import reversion.

        Read-only. The four non-revertable outcomes are plans too, so a batch
        whose state flips between approval and commit changes the confirmation
        binding instead of slipping past it.

        Args:
            import_id: UUID of the import batch in ``raw.import_log``.
        """
        # REVERT_TABLES is owned by import_log because begin_import also consults it.
        from moneybin.loaders.import_log import REVERT_TABLES  # noqa: PLC0415
        from moneybin.tables import IMPORT_LOG  # noqa: PLC0415

        row = self._db.execute(
            f"SELECT source_type, status, source_file, started_at, source_origin "
            f"FROM {IMPORT_LOG.full_name} WHERE import_id = ?",
            [import_id],
        ).fetchone()

        if row is None:
            return ImportRevertPlan(
                import_id=import_id,
                outcome="not_found",
                reason=f"No import with ID {import_id}",
            )

        src_type, status, source_file, started_at, source_origin = row

        if status == "reverted":
            return ImportRevertPlan(import_id=import_id, outcome="already_reverted")

        if src_type not in REVERT_TABLES:
            return ImportRevertPlan(
                import_id=import_id,
                outcome="unsupported",
                reason=f"Cannot revert source_type {src_type!r}",
                source_type=src_type,
            )

        # Count every table the source_type populates. OFX statements with zero
        # transactions but populated accounts/balances must still be detectable
        # as live (not superseded) and reportable in rows_deleted.
        table_counts: list[tuple[str, int]] = []
        for table in REVERT_TABLES[src_type]:
            result = self._db.execute(
                f"SELECT COUNT(*) FROM {table.full_name} WHERE import_id = ?",
                [import_id],
            ).fetchone()
            table_counts.append((table.full_name, int(result[0]) if result else 0))

        if sum(count for _, count in table_counts) == 0:
            # If a later import upserted over this one's rows, surface that
            # instead of a silent no-op revert.
            reimport_row = self._db.execute(
                f"""
                SELECT import_id
                FROM {IMPORT_LOG.full_name}
                WHERE source_file = ?
                  AND import_id != ?
                  AND started_at > ?
                  AND status NOT IN ('reverted', 'failed')
                ORDER BY started_at DESC
                LIMIT 1
                """,
                [source_file, import_id, started_at],
            ).fetchone()
            if reimport_row:
                newer_id = reimport_row[0]
                return ImportRevertPlan(
                    import_id=import_id,
                    outcome="superseded",
                    reason=(
                        f"File was re-imported as {newer_id[:8]}...; "
                        f"revert that batch to remove the data."
                    ),
                    source_type=src_type,
                )

        return ImportRevertPlan(
            import_id=import_id,
            outcome="revertable",
            source_type=src_type,
            source_origin=source_origin,
            table_counts=tuple(table_counts),
        )

    def revert_confirmed(
        self,
        import_id: str,
        *,
        verify: Callable[[ImportRevertPlan], None],
    ) -> dict[str, str | int]:
        """Revalidate and revert one import batch in the same transaction.

        ``verify`` re-checks the caller's approval against the live plan read
        *inside* the write transaction, immediately before the first delete, so
        approval can never be applied to state it did not describe.

        Returns:
            ``{'status': 'reverted', 'rows_deleted': N}`` on success, else the
            live non-revertable outcome.
        """
        from moneybin.loaders.import_log import REVERT_TABLES  # noqa: PLC0415
        from moneybin.tables import IMPORT_LOG  # noqa: PLC0415

        self._db.begin()
        try:
            live = self.plan_revert(import_id)
            verify(live)
            if live.revertable:
                for table in REVERT_TABLES[cast(str, live.source_type)]:
                    self._db.execute(
                        f"DELETE FROM {table.full_name} WHERE import_id = ?",
                        [import_id],
                    )
                self._db.execute(
                    f"""
                    UPDATE {IMPORT_LOG.full_name} SET
                        status = 'reverted',
                        reverted_at = CURRENT_TIMESTAMP
                    WHERE import_id = ?
                    """,
                    [import_id],
                )
            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise

        if not live.revertable:
            return live.as_result()

        # Drop the auto-generated raw.pdf_<alias> view after row deletion
        # succeeds. DDL is autocommit in DuckDB (cannot be inside the transaction),
        # so this runs after commit. IF EXISTS guards against re-reverts or a
        # view that was never created (zero-row import path).
        # Only drop the view if no other completed imports remain for this alias —
        # reverting one import should not hide rows from sibling imports of the
        # same source. Note: with on_conflict='ignore' the first import owns
        # every row of identical content; a sibling import's log entry can be
        # 'complete' while holding zero rows, so the preserved view may be
        # legitimately empty after this revert.
        if live.source_type == "pdf" and live.source_origin:
            other_row = self._db.execute(
                f"SELECT COUNT(*) FROM {IMPORT_LOG.full_name} "
                f"WHERE source_type = 'pdf' AND source_origin = ? "
                f"AND status = 'complete' AND import_id != ?",
                [live.source_origin, import_id],
            ).fetchone()
            if other_row is not None and other_row[0] == 0:
                from sqlglot import exp  # noqa: PLC0415

                safe_view = exp.to_identifier(
                    f"pdf_{live.source_origin}", quoted=True
                ).sql("duckdb")
                # DDL runs post-commit (DuckDB autocommits DDL outside the
                # transaction). The rows are already gone and import_log is
                # already 'reverted', so a catalog error here would orphan
                # the view with no recovery path other than manual SQL.
                # Best-effort log + continue keeps the rest of the revert
                # outcome intact.
                try:
                    self._db.execute(f"DROP VIEW IF EXISTS raw.{safe_view}")
                except Exception:  # noqa: BLE001 — DDL best-effort post-commit
                    logger.warning(
                        f"DROP VIEW raw.{safe_view} failed during revert of "
                        f"import_id={import_id[:8]}...; view may be orphaned",
                        exc_info=True,
                    )

        logger.info(
            f"Reverted import {import_id[:8]}...: {live.rows_to_delete} rows deleted"
        )
        return {"status": "reverted", "rows_deleted": live.rows_to_delete}

    # ------------------------------------------------------------------
    # Import labels (spec Req 22–24).
    # ------------------------------------------------------------------

    def list_labels(self, import_id: str) -> list[str]:
        """Return the labels currently attached to ``import_id`` (or empty)."""
        row = self._db.conn.execute(
            f"SELECT labels FROM {IMPORTS.full_name} WHERE import_id = ?",  # noqa: S608  # TableRef constant
            [import_id],
        ).fetchone()
        if row is None or row[0] is None:
            return []
        return list(row[0])

    def list_distinct_labels(self) -> list[tuple[str, int]]:
        """Return ``(label, usage_count)`` across all import rows, sorted desc."""
        rows = self._db.conn.execute(
            f"""
            SELECT label, COUNT(*) AS n
              FROM (SELECT UNNEST(labels) AS label FROM {IMPORTS.full_name})
             WHERE label IS NOT NULL
             GROUP BY label
             ORDER BY n DESC, label ASC
            """  # noqa: S608  # IMPORTS is a TableRef constant
        ).fetchall()
        return [(str(r[0]), int(r[1])) for r in rows]

    def add_labels(self, import_id: str, labels: list[str], *, actor: str) -> list[str]:
        """Append ``labels`` to the import's set; return the resulting labels.

        Reads the prior set and writes the union in one transaction via
        ``ImportsRepo.set`` (one paired ``import.set`` audit row, Invariant 10).
        """
        for label in labels:
            validate_slug(label)
        self._db.begin()
        try:
            prior = self.list_labels(import_id)
            new = _merge_unique(prior, labels)
            # Skip the write (and its audit row) when nothing changed — e.g.
            # re-adding labels the import already has — so a no-op doesn't
            # materialize a spurious app.imports row or audit entry.
            if new != prior:
                self._imports.set(import_id, labels=new, actor=actor, in_outer_txn=True)
            self._db.commit()
        except BaseException:
            # Roll back on BaseException, not just Exception, so a
            # KeyboardInterrupt/SystemExit mid-write doesn't leave the outer
            # transaction open (matches BaseRepo._transaction). Re-raised, never
            # swallowed.
            self._db.rollback()
            raise
        return new

    def remove_labels(
        self, import_id: str, labels: list[str], *, actor: str
    ) -> list[str]:
        """Drop ``labels`` from the import's set; return the resulting labels.

        Reads the prior set and writes the difference in one transaction via
        ``ImportsRepo.set`` (one paired ``import.set`` audit row, Invariant 10).
        """
        for label in labels:
            validate_slug(label)
        drop = set(labels)
        self._db.begin()
        try:
            prior = self.list_labels(import_id)
            new = [x for x in prior if x not in drop]
            # Skip the write (and its audit row) when nothing was removed — e.g.
            # removing a label the import lacks, or operating on a never-labeled
            # import — so a no-op doesn't materialize a spurious app.imports row
            # or audit entry.
            if new != prior:
                self._imports.set(import_id, labels=new, actor=actor, in_outer_txn=True)
            self._db.commit()
        except BaseException:
            # Roll back on BaseException, not just Exception, so a
            # KeyboardInterrupt/SystemExit mid-write doesn't leave the outer
            # transaction open (matches BaseRepo._transaction). Re-raised, never
            # swallowed.
            self._db.rollback()
            raise
        return new

    def set_labels(self, import_id: str, labels: list[str], *, actor: str) -> list[str]:
        """Replace the import's labels declaratively; return the canonical set.

        Validates every requested label, dedups while preserving order, then
        upserts via ``ImportsRepo.set`` — one ``import.set`` audit row capturing
        the full before/after row (Invariant 10).
        """
        for label in labels:
            validate_slug(label)
        # Dedup while preserving order so the stored list is canonical.
        canonical = _merge_unique([], labels)
        self._imports.set(import_id, labels=canonical, actor=actor)
        return canonical


def _merge_unique(prior: list[str], additions: list[str]) -> list[str]:
    """Return ``prior + additions`` with duplicates dropped, order preserved."""
    seen = set(prior)
    out = list(prior)
    for label in additions:
        if label not in seen:
            seen.add(label)
            out.append(label)
    return out
