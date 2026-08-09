"""Channel-agnostic confirm/confidence primitive.

`resolve_or_confirm` (added in a later task) is the single seam every
smart-import channel calls when it has detected a layout but does NOT
know whether to auto-load. Its inputs and outputs are the types in this
module.

Channel-specific known-layout lookup (saved tabular formats / pdf_formats
/ gsheet connection match) stays in the channel — the primitive is
invoked only when a confirm decision is needed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from moneybin.extractors.confidence import Confidence, Tier
from moneybin.extractors.tabular.formats import NumberFormatType, SignConventionType
from moneybin.services.account_resolution_types import AccountProposalDict

Channel = Literal["tabular", "gsheet", "pdf", "ofx"]
ActorKind = Literal["human", "agent"]
ConfirmationReason = Literal[
    "unknown_layout",
    "validation_failure",
    "account_confirmation",
    "sign_convention",
    "unreadable_date",
    "header_row_consumed",
]
ConfirmationOutcome = Literal["accepted", "overridden", "declined"]


@dataclass(frozen=True)
class ProposedMapping:
    """Detector output for tabular / gsheet channels.

    `field_mapping` is destination_field -> source_column. `sample_values`
    keys match `field_mapping` keys. `unmapped_columns` are source columns
    the detector did not consume (shown so the user can spot a missed dest).
    """

    field_mapping: dict[str, str]
    sample_values: dict[str, list[str]]
    unmapped_columns: tuple[str, ...]


@dataclass(frozen=True)
class BridgePayload:
    """PDF-channel bridge payload. Shape is channel-specific.

    v1 carries an opaque dict (the PDF extractor populates IR + extraction
    request); this gives `import_confirm` a stable signature without
    pulling PDF types into this module.
    """

    payload: dict[str, Any]


@dataclass(frozen=True)
class SignConventionProposal:
    """Proposal to invert every amount's sign.

    Carried when a statement names itself a credit card (`evidence`) and the
    detector therefore proposes `negative_is_income`. The convention is not
    recoverable from the amounts — a card and a checking statement have identical
    sign distributions and both reconcile — so this proposal is never applied
    silently: the caller ratifies it, or overrides it with `sign=`.

    `sample_rows`, when the extractor can provide them, show the flip concretely:
    what the source printed vs what MoneyBin would record.
    """

    sign_convention: str
    evidence: tuple[str, ...]
    sample_rows: list[dict[str, str]]
    #: The convention this proposal would REPLACE, when one was already in force
    #: (a self-healed recipe that re-derived to the opposite polarity). None on a
    #: first-contact inference, where there is no prior to keep. Surfaces are
    #: expected to branch on it: with no prior, the proposal is always
    #: negative_is_income and "is this a credit card?" is the honest question;
    #: with one, the flip can run either direction and the question is instead
    #: "keep the old convention or take the new one?".
    prior_sign_convention: str | None = None


#: What each sign convention does to the ledger, in the user's terms. Lives here
#: rather than in either surface so the CLI prompt and the MCP ``actions[]`` can't
#: drift into describing the same flip two different ways.
_SIGN_CONVENTION_EFFECT = {
    "negative_is_expense": "records amounts exactly as printed",
    "negative_is_income": "records charges as expenses and payments as credits",
    "split_debit_credit": "reads debit and credit columns separately",
}


def sign_convention_effect(convention: str) -> str:
    """Plain-English effect of ``convention`` on the ledger."""
    return _SIGN_CONVENTION_EFFECT.get(convention, f"records amounts as {convention}")


@dataclass(frozen=True)
class Accept:
    """Caller signal: accept the proposed mapping/recipe as-is."""


@dataclass(frozen=True)
class Override:
    """Caller signal: partial-merge override.

    `mapping` carries ONLY the destination fields the caller is correcting;
    unspecified fields fall back to the proposed mapping.
    """

    mapping: dict[str, str]


@dataclass(frozen=True)
class Resolved:
    """Terminal outcome: the import can load.

    `format_ref` names the saved format (tabular: `app.tabular_formats.name`;
    gsheet: connection_id; PDF: `app.pdf_formats.name`) — None when no
    format was saved (e.g. agent self-accept without persistence, not v1).
    `self_accepted=True` records that an agent accepted at `high` without
    surfacing to the human.
    """

    field_mapping: dict[str, str]
    format_ref: str | None
    self_accepted: bool


@dataclass(frozen=True)
class ConfirmationRequired:
    """Terminal outcome: caller must confirm before data lands.

    `reason='unknown_layout'` surfaces the proposal + samples for first-
    encounter confirm. `reason='validation_failure'` surfaces a known
    layout that failed its replay/validation guard (Req 9) — for tabular
    the proposed mapping carries the failing signal; for PDF this re-
    escalates to the bridge per `smart-import-pdf.md`. `reason=
    'account_confirmation'` surfaces a resolved layout whose *account*
    identity needs ratification — either a detected source account with
    weak merge candidates (the caller picks one or mints new), or a bare
    single-account file with no identity signal at all (no candidates; the
    caller names or binds the account). The column mapping in `proposed` is
    already accepted; the caller ratifies the account binding via
    `account_proposals`.

    `reason='header_row_consumed'` narrows `unknown_layout` to the one cause
    no caller input can answer: the first row was read as column names but
    parses as a transaction, so a real record is already gone. It exists
    because `resolve_or_confirm` honours an `Override` at every tier by
    design — the caller's column correction outranks a low score — and that
    is right for a mapping problem and wrong for this one, which no column
    correction touches. Surfaces route it to source repair, not a retry.

    `reason='unreadable_date'` narrows `unknown_layout` to one cause: a
    `transaction_date` column is mapped and nothing could read its values.
    It exists so a surface can name the two fixes that work — remap the
    column, or supply `--date-format` — instead of a generic accept hint,
    which returns to this same gate. It is NOT raised when no date column is
    mapped at all; that stays `unknown_layout`, where a mapping override is
    the only recovery and no date format would help.

    `account_proposals` carries the `AccountProposal.to_dict()` payload for
    each detected source account whose resolution needs ratification — weak
    candidates to choose among, or a no-candidate proposal for the bare
    single-account case. Empty for mapping-only confirmations.

    `ratified_bindings` carries the answers the caller has *already* given,
    re-keyed from whatever they sent to the positional `proposal_ref`. A
    multi-account file is answered one at a time and retries persist no partial
    state, so a surface must replay the earlier answers or the gate re-asks and
    never converges — but an answered account is skipped by
    `_gate_account_proposals` and so never appears in `account_proposals`,
    leaving a surface nothing to re-key its key from. On OFX that key is the
    `<ACCTID>`, an account number, so replaying the caller's own key verbatim
    publishes it. The ref is the account's index in the file, which makes this
    the only layer that can still name it once the proposal is gone.
    """

    channel: Channel
    confidence: Confidence
    proposed: ProposedMapping | BridgePayload | SignConventionProposal
    reason: ConfirmationReason
    samples: dict[str, list[str]] = field(default_factory=dict)
    error_message: str = ""
    account_proposals: list[AccountProposalDict] = field(default_factory=list)
    # Keys are refs and values are MoneyBin's own account ids (or "new"), so
    # unlike a source-keyed map this is safe on an unredacted surface. It is
    # deliberately absent from confirmation_payload_dict: no transport consumer
    # needs it, and MCP's actions[] never replays a caller's bindings at all.
    ratified_bindings: dict[str, str] = field(default_factory=dict)


def confirmation_payload_dict(outcome: ConfirmationRequired) -> dict[str, object]:
    """Serialize a ConfirmationRequired to the transport-neutral payload dict.

    Single source for the per-file ``confirmation_payload`` carried by both the
    batch service path (``ImportService.import_files``) and the single-file MCP
    path (``import_files`` tool) — they must produce the identical shape, and a
    new channel field (e.g. ``bridge_payload``) should land in one place. The
    tabular fields (``proposed_mapping``, ``unmapped_columns``) are populated
    from a ``ProposedMapping`` proposal; ``bridge_payload`` from a
    ``BridgePayload`` proposal; ``sign_convention``/``sign_prior_convention``/
    ``sign_evidence``/``sign_sample_rows`` from a ``SignConventionProposal``;
    the unused fields are empty/None for the channel.

    Every surface that renders a sign decision — the CLI prompt and recovery
    commands, the MCP ``actions[]`` and elicitation prompt, and the inbox
    pending sidecar — must branch on ``sign_prior_convention``. It is what
    distinguishes a first-contact card inference (always toward
    ``negative_is_income``) from a self-healed recipe that re-derived to the
    opposite polarity; describing the latter with the former's wording tells
    the reader the reverse of what confirming does.
    """
    proposed = outcome.proposed
    proposed_mapping: dict[str, str] = {}
    unmapped: list[str] = []
    bridge_payload: dict[str, Any] | None = None
    sign_convention: str | None = None
    sign_prior_convention: str | None = None
    sign_evidence: list[str] = []
    sign_sample_rows: list[dict[str, str]] = []
    if isinstance(proposed, ProposedMapping):
        proposed_mapping = dict(proposed.field_mapping)
        unmapped = list(proposed.unmapped_columns)
    elif isinstance(proposed, SignConventionProposal):
        sign_convention = proposed.sign_convention
        sign_prior_convention = proposed.prior_sign_convention
        sign_evidence = list(proposed.evidence)
        sign_sample_rows = [dict(r) for r in proposed.sample_rows]
    else:
        bridge_payload = proposed.payload
    return {
        "channel": outcome.channel,
        "tier": outcome.confidence.tier,
        "score": outcome.confidence.score,
        "reason": outcome.reason,
        "error_message": outcome.error_message,
        "proposed_mapping": proposed_mapping,
        "samples": dict(outcome.samples),
        "flagged": list(outcome.confidence.flagged),
        "missing_required": list(outcome.confidence.missing_required),
        "unmapped_columns": unmapped,
        "bridge_payload": bridge_payload,
        "sign_convention": sign_convention,
        "sign_prior_convention": sign_prior_convention,
        "sign_evidence": sign_evidence,
        "sign_sample_rows": sign_sample_rows,
        "account_proposals": list(outcome.account_proposals),
    }


def unreadable_date_recovery(file_path: str) -> str:
    """Name both recoveries for a date column nothing could parse.

    Lives here, beside the reason it answers, because four surfaces need the
    identical sentence — the CLI's three confirmation handlers and the inbox
    sidecar. It must name *both*, because `unreadable_date` covers two
    different mistakes. The detector may have read the right column and carry
    no candidate for its format, which only `--date-format` fixes. Or a status
    column ("Pending"/"Cleared") claimed the date alias while the real dates
    sit in an unmapped column — there `map_columns` re-runs detection against
    whatever an override names, so a plain column correction recovers the file
    and `--date-format` aimed at the wrong column would just be refused again.
    Mirrors the MCP hint in import_tools.py; keep the two in step.
    """
    import shlex

    # Quoted like every other suggested command in the CLI: a bank export
    # lands in "Bank Exports/" often enough that an unquoted path makes the
    # prescribed recovery uncopyable exactly when the user needs it.
    quoted = shlex.quote(file_path)
    return (
        "No date format could be read from the mapped date column. If the "
        "wrong column is mapped — a status column can claim the date alias "
        "while the real dates sit in an unmapped one — re-run with `--mapping "
        "transaction_date=<source_column>`, which re-runs detection against "
        f"that column; `moneybin import preview {quoted}` names the file's "
        "columns. If the mapped column is right and its format is simply "
        "unrecognized, no mapping can change that: re-run `moneybin import "
        f"files {quoted} --confirm --date-format <strptime>`."
    )


def classify_unconfirmable_plan(
    *,
    header_row_looks_like_data: bool,
    date_format: str | None,
    field_mapping: dict[str, str],
    flagged_fields: list[str],
) -> ConfirmationReason:
    """Name the cause a caller's next action has to answer.

    One rule, because it was implemented three times and corrected one site
    at a time: the service's reviewed-plan replay, its first-contact branch,
    and the MCP preview action builder all decide which recovery to offer for
    the same staged plan, and every divergence between them shipped a hint
    that could not resolve the refusal it accompanied.

    Precedence is by what the caller can actually do:

    1. A consumed header row — no input touches it, so it outranks everything.
    2. A missing required destination — the date recoveries remap a column or
       supply a format, and neither supplies a field that is absent.
    3. An unreadable date on a *mapped* column — remap it or name its format.
    4. Otherwise a plain mapping correction.
    """
    from moneybin.extractors.tabular.column_mapper import score_mapping

    if header_row_looks_like_data:
        return "header_row_consumed"
    _, missing_required = score_mapping(field_mapping, flagged_fields, date_format)
    if missing_required:
        return "unknown_layout"
    if date_format is None and "transaction_date" in field_mapping:
        return "unreadable_date"
    return "unknown_layout"


def header_row_consumed_recovery() -> str:
    """The consumed-header recovery, for the CLI and the inbox sidecar.

    Identical substance to the MCP wording below and deliberately adjacent to
    it: there is no command to offer on either surface, because MoneyBin
    exposes no skip-rows override. Only the closing sentence differs, since a
    CLI reader re-runs a command rather than re-previewing through a tool.
    """
    return (
        "This file's first row was read as column names, but it parses as a "
        "transaction — a real record was consumed as the header. No --mapping "
        "or --override correction can recover it, and MoneyBin exposes no "
        "skip-rows override. Add a header row to the source file, or correct "
        "the saved format's skip_rows, then import it again."
    )


def header_row_consumed_recovery_mcp() -> str:
    """The only honest recovery when a transaction was read as the header.

    Takes no file path because there is no command to run: MoneyBin exposes
    no skip-rows override (`skip_rows` is only ever written from detection),
    so every mapping retry restages the same unconfirmable plan. Shared by
    the preview- and confirm-side action builders — keeping one text per
    state is what stops the two from drifting, which they did for three
    consecutive review rounds.
    """
    return (
        "This file's first row was read as column names, but it parses as a "
        "transaction — a real record was consumed as the header. No column "
        "correction can recover it, and MoneyBin exposes no skip-rows "
        "override. Add a header row to the source file and preview it again."
    )


def unreadable_date_recovery_mcp(file_path: str) -> str:
    """The same two recoveries as above, in MCP's idiom.

    Kept adjacent to the CLI wording on purpose: this claim has drifted three
    times across the surfaces that print it, each time by fixing one copy and
    leaving a sibling asserting the opposite. The *substance* — remap the
    column, or supply a format, and never claim one is impossible — is what
    must stay identical; only the command names differ. MCP exposes no
    date-format parameter, so that branch names the CLI.
    """
    return (
        "This mapping is not confirmable as staged: no date format could "
        "be read from the mapped date column. If the wrong column is "
        f"mapped, use import_preview(file_path={file_path!r}, "
        "mapping={'transaction_date': '<source_column>'}); "
        "data.sample_values and data.unmapped_columns name the file's "
        "columns. If the column is right and its format is simply "
        "unrecognized, no mapping correction can change that — import "
        "it with `moneybin import files <file> --confirm --date-format "
        "<strptime>`."
    )


class ImportConfirmationRequiredError(Exception):
    """Raised when an import cannot proceed without explicit confirmation.

    Carries the ConfirmationRequired outcome so MCP/CLI surfaces can render
    the proposal + actions[]. The data load did NOT happen.
    """

    def __init__(self, outcome: ConfirmationRequired) -> None:
        """Wrap a ConfirmationRequired outcome as an exception."""
        super().__init__(
            f"{outcome.channel} import requires confirmation: {outcome.reason}"
        )
        self.outcome = outcome


class MappingValidationError(ValueError):
    """A merged mapping is missing required fields or names unknown columns."""


def resolve_amount_shape(
    *,
    proposed_keys: set[str] | frozenset[str],
    override_keys: set[str] | frozenset[str],
) -> tuple[str, ...]:
    """Resolve which amount-shape destination fields the merged mapping needs.

    Amount shape is mutually exclusive: a row uses either a single ``amount``
    column OR a ``debit_amount`` + ``credit_amount`` pair, never both. This
    returns the winning shape after the override would merge onto the
    proposed mapping, so callers that need to know the *post-merge*
    required-field set (e.g., to pre-validate before ``resolve_or_confirm``)
    do not have to re-derive it.

    The branch rules MUST stay in lockstep with the merge logic in
    ``validate_partial_mapping`` — both call this helper so a future shape
    addition only updates one place.
    """
    override_has_amount = "amount" in override_keys
    override_has_split = (
        "debit_amount" in override_keys or "credit_amount" in override_keys
    )
    if override_has_amount and not override_has_split:
        return ("amount",)
    if override_has_split and not override_has_amount:
        return ("debit_amount", "credit_amount")
    proposed_is_split = (
        "debit_amount" in proposed_keys
        and "credit_amount" in proposed_keys
        and "amount" not in proposed_keys
    )
    return ("debit_amount", "credit_amount") if proposed_is_split else ("amount",)


def tabular_required_fields(
    *,
    proposed_keys: set[str] | frozenset[str],
    override_keys: set[str] | frozenset[str],
) -> tuple[str, ...]:
    """The tabular channel's required destinations for the post-merge amount shape.

    The one place the channel's required set lives, so adding a field (or an
    amount shape) doesn't mean hunting every caller that pre-validates.
    """
    amount_fields = resolve_amount_shape(
        proposed_keys=proposed_keys, override_keys=override_keys
    )
    return ("transaction_date", *amount_fields, "description")


def coerce_sign_convention(
    *,
    field_mapping: dict[str, str],
    detected: SignConventionType,
) -> SignConventionType:
    """Keep sign_convention consistent with the amount shape actually mapped.

    A split rule against a single ``amount`` mapping rejects every row, and a
    single-amount rule against a debit/credit pair does the same — so an
    override that swaps the shape must not carry the detector's convention
    forward. Every path that merges an override onto a detected mapping calls
    this; skipping it produces a plan that parses to zero rows and still
    reports success.
    """
    resolved_is_split = (
        "debit_amount" in field_mapping and "credit_amount" in field_mapping
    )
    detected_is_split = detected == "split_debit_credit"
    if resolved_is_split and not detected_is_split:
        return "split_debit_credit"
    if not resolved_is_split and detected_is_split:
        # The detector's split-only convention no longer applies. Fall back to
        # the default; callers can still pass an explicit sign override.
        return "negative_is_expense"
    return detected


def coerce_number_format(
    *,
    field_mapping: dict[str, str],
    sample_values: dict[str, list[str | None]] | dict[str, list[str]],
    detected: NumberFormatType,
) -> NumberFormatType:
    """Keep number_format consistent with the amount shape actually mapped.

    Sibling of ``coerce_sign_convention`` and called from the same places, for
    the same reason. ``map_columns`` derives the format from ``amount`` and
    only falls back to ``debit_amount``, so an override that swaps a detected
    single-amount layout to a debit/credit pair leaves the format derived from
    the column it just retired. A US-formatted loser beside European survivors
    silently parses ``1.234,56`` as ``1.23456`` — and the wrong format is then
    saved for every later import of that layout.

    Re-derives from the surviving amount column, pooling both halves of a
    split pair. Pooling is not an optimization: ``collect_samples`` takes the
    first rows unfiltered, and each row of a debit/credit layout fills only one
    side, so a sample window that happens to be all credits leaves
    ``debit_amount`` a non-empty list of blanks. Reading the first *present*
    destination would hand that to ``detect_number_format``, which finds
    nothing parseable and returns its own ``us`` default — the wrong answer,
    never having looked at the column holding the values. The two halves are
    the same currency and locale by construction, so pooling them is also
    strictly more signal than either alone.

    Keeps ``detected`` when nothing parseable survives, since a guess from
    blanks is worse than the detector's answer.
    """
    from moneybin.extractors.tabular.date_detection import detect_number_format

    sources = (
        ("amount",) if "amount" in field_mapping else ("debit_amount", "credit_amount")
    )
    usable: list[str | None] = []
    for dest in sources:
        if dest not in field_mapping:
            continue
        for value in sample_values.get(dest) or ():
            if value is not None and str(value).strip():
                usable.append(str(value))
    if not usable:
        return detected
    return detect_number_format(usable)


def coerce_confidence_tier(
    *,
    field_mapping: dict[str, str],
    detected_flagged: list[str],
    override_keys: set[str],
    date_format: str | None,
    structural_red_flag: bool,
    t_high: float,
    t_med: float,
) -> Tier:
    """Re-band a merged mapping instead of asserting a tier for it.

    Third sibling of ``coerce_sign_convention`` / ``coerce_number_format``:
    an override answers only for the fields it names, so the merged plan has
    to be re-scored rather than declared. The predecessor asserted ``high``
    whenever a hand-kept list of exceptions was empty, and that list was
    reported incomplete twice — first missing the structural red flag and the
    unreadable date, then missing a *second* required field still weakly
    matched, which promoted an untouched weak match to the one tier eligible
    for agent self-accept.

    Lives here rather than in the MCP adapter because it is a domain decision,
    and ``test_adapters_dont_bypass_service_layer`` enforces that boundary —
    it failed the moment the scoring call was written into the tool wrapper.
    """
    from moneybin.extractors.confidence import resolve_tier
    from moneybin.extractors.tabular.column_mapper import score_mapping

    # A flag counts only while the destination it names survives into the
    # merged mapping. An amount-shape swap retires `amount` without naming it
    # as an override key, so a flag left on it would otherwise keep scoring
    # against a field the corrected plan no longer has.
    remaining = [
        flag
        for flag in detected_flagged
        if flag not in override_keys and flag in field_mapping
    ]
    score, _ = score_mapping(field_mapping, remaining, date_format)
    return resolve_tier(
        score,
        t_high=t_high,
        t_med=t_med,
        structural_red_flag=structural_red_flag,
    )


def validate_partial_mapping(
    *,
    proposed: dict[str, str],
    override: dict[str, str],
    available_columns: tuple[str, ...] | list[str],
    required_fields: tuple[str, ...],
    valid_destinations: tuple[str, ...] | None = None,
) -> dict[str, str]:
    """Merge override onto proposed, validate, and return the merged mapping.

    Override is partial-merge: it overrides only the destination fields it
    names; unspecified fields fall back to `proposed`. Three failure modes:

    1. After merging, a required destination field is unmapped.
    2. The merged mapping names a source column not present in the file/
       sheet (a transform-time error surfaced early as a user-fixable
       validation error).
    3. The override names a destination field not in `valid_destinations`
       (when supplied) — catches typos like ``--mapping descrption=Memo``
       silently passing because `description` is already proposed.

    Args:
        proposed: Detector-emitted mapping (destination -> source column).
        override: Caller-supplied corrections (same shape; partial).
        available_columns: Source columns actually present in the data.
        required_fields: Destination fields that MUST appear in the merged
            mapping for this channel (tabular and gsheet differ — see the
            spec's "channel scorers feed the contract" note).
        valid_destinations: Optional allowlist of acceptable destination
            field names for this channel. When provided, every override
            key must be in this set; ``None`` skips the check (back-compat
            for callers that don't know the full schema).

    Returns:
        The merged mapping. Callers should pass this to the loader rather
        than recomputing it.
    """
    if valid_destinations is not None:
        valid_set = set(valid_destinations)
        unknown_dests = [k for k in override if k not in valid_set]
        if unknown_dests:
            raise MappingValidationError(
                f"Mapping override names unknown destination field(s): "
                f"{unknown_dests}. Valid destinations: {list(valid_destinations)}."
            )
    merged = {**proposed, **override}
    # Amount shape is mutually exclusive: a row has EITHER a single
    # ``amount`` column OR a ``debit_amount`` + ``credit_amount`` pair, never
    # both. If the override resolves the contention, drop the losing keys
    # so the merged mapping has a single, unambiguous shape.
    # Otherwise transform_dataframe would silently use one shape (driven by
    # sign_convention) and ignore the other, masking the override's intent.
    override_has_amount = "amount" in override
    override_has_debit = "debit_amount" in override
    override_has_credit = "credit_amount" in override
    override_has_split = override_has_debit or override_has_credit
    # An override naming amount AND any split key at the same time is
    # contradictory — amount-single and amount-split are mutually
    # exclusive shapes. Reject up front rather than silently keeping
    # both in merged (downstream would coerce sign_convention to split
    # and silently drop the single amount column).
    if override_has_amount and override_has_split:
        raise MappingValidationError(
            "Mapping override is contradictory: 'amount' and the split "
            "pair ('debit_amount'/'credit_amount') are mutually exclusive — "
            "supply 'amount' alone OR 'debit_amount'+'credit_amount' alone, "
            "never both in the same override."
        )
    if override_has_split and not override_has_amount:
        # Switching FROM a single-amount proposal TO split via override
        # requires BOTH halves of the pair. Catch the partial-switch case
        # here so the user sees the actionable message — otherwise the
        # downstream missing-required check fires "missing amount" after
        # we drop it, which doesn't reveal the real problem.
        if "amount" in proposed and not (override_has_debit and override_has_credit):
            partial = "debit_amount" if override_has_debit else "credit_amount"
            other = "credit_amount" if override_has_debit else "debit_amount"
            raise MappingValidationError(
                f"Override supplies only {partial!r} but the proposed "
                f"mapping uses single-column 'amount'. Switching to split "
                f"debit/credit requires BOTH {partial!r} and {other!r} in "
                f"the same override (they are mutually exclusive with the "
                f"single 'amount' field)."
            )
    shape = resolve_amount_shape(
        proposed_keys=set(proposed.keys()),
        override_keys=set(override.keys()),
    )
    if shape == ("amount",):
        merged.pop("debit_amount", None)
        merged.pop("credit_amount", None)
    else:
        merged.pop("amount", None)
    missing = [f for f in required_fields if f not in merged]
    if missing:
        raise MappingValidationError(
            f"Mapping is missing required dest field(s): {missing}. "
            f"Channel needs {list(required_fields)}."
        )
    available_set = set(available_columns)
    unknown = [src for src in merged.values() if src not in available_set]
    if unknown:
        raise MappingValidationError(
            f"Mapping references source column(s) not in the source: "
            f"{unknown}. Available columns: {list(available_columns)}."
        )
    return merged


def resolve_or_confirm(
    *,
    channel: Channel,
    confidence: Confidence,
    proposed: ProposedMapping,
    available_columns: tuple[str, ...] | list[str],
    required_fields: tuple[str, ...],
    signal: Accept | Override | None,
    self_accept_enabled: bool,
    actor_kind: ActorKind,
    valid_destinations: tuple[str, ...] | None = None,
) -> Resolved | ConfirmationRequired:
    """Decide whether to auto-load, self-accept, or surface for confirmation.

    Invoked by a channel only when a confirm decision is needed (i.e. the
    channel has already failed to match a known layout, or a known layout
    failed its replay/validation guard). Channel-agnostic.

    Decision order (each step short-circuits):

    1. Override signal — partial-merge + validate the user's correction;
       honored at every tier, INCLUDING `low`. An explicit override is the
       spec's first-class recovery path (Req 11); short-circuiting `low`
       before Override would block the documented `import_confirm
       --mapping ...` recovery.
       Pass -> Resolved. Fail -> ConfirmationRequired(reason=validation_failure)
       (carrying the validator's message so callers see why their override
       was rejected).
    2. `low` tier without an Override — never auto-accept (Req 4). Even an
       explicit Accept on `low` surfaces, because Accept ratifies the
       detector's proposal as-is and `low` means the detector could not
       form a complete one.
    3. Accept signal — Resolved with the proposed mapping (validated).
    4. No signal, actor=agent, tier=high, self_accept_enabled=True —
       self-accept (Req 10, behind the calibration gate).
    5. Otherwise — ConfirmationRequired(reason=unknown_layout).
    """
    if isinstance(signal, Override):
        try:
            merged = validate_partial_mapping(
                proposed=proposed.field_mapping,
                override=signal.mapping,
                available_columns=available_columns,
                required_fields=required_fields,
                valid_destinations=valid_destinations,
            )
        except MappingValidationError as e:
            return ConfirmationRequired(
                channel=channel,
                confidence=confidence,
                proposed=proposed,
                reason="validation_failure",
                samples=dict(proposed.sample_values),
                error_message=str(e),
            )
        return Resolved(field_mapping=merged, format_ref=None, self_accepted=False)

    if confidence.tier == "low":
        return ConfirmationRequired(
            channel=channel,
            confidence=confidence,
            proposed=proposed,
            reason="unknown_layout",
            samples=dict(proposed.sample_values),
        )

    if isinstance(signal, Accept):
        # Accept ratifies the detector's proposal as-is. Surface validation
        # failure as ConfirmationRequired(validation_failure) for symmetry
        # with the Override branch — callers see a uniform result type
        # regardless of which signal they sent. A failure here typically
        # means the channel detector produced a malformed proposal (a
        # caller bug), but surfacing it lets the caller render the
        # error_message instead of seeing an unhandled exception.
        try:
            merged = validate_partial_mapping(
                proposed=proposed.field_mapping,
                override={},
                available_columns=available_columns,
                required_fields=required_fields,
                valid_destinations=valid_destinations,
            )
        except MappingValidationError as e:
            return ConfirmationRequired(
                channel=channel,
                confidence=confidence,
                proposed=proposed,
                reason="validation_failure",
                samples=dict(proposed.sample_values),
                error_message=str(e),
            )
        return Resolved(field_mapping=merged, format_ref=None, self_accepted=False)

    if actor_kind == "agent" and confidence.tier == "high" and self_accept_enabled:
        # Self-accept symmetric with Accept: surface validation failure
        # as ConfirmationRequired rather than letting the exception
        # propagate. A malformed proposal at self-accept time is a
        # channel bug, but uniform return-type contract means callers
        # don't branch on signal/actor combinations.
        try:
            merged = validate_partial_mapping(
                proposed=proposed.field_mapping,
                override={},
                available_columns=available_columns,
                required_fields=required_fields,
                valid_destinations=valid_destinations,
            )
        except MappingValidationError as e:
            return ConfirmationRequired(
                channel=channel,
                confidence=confidence,
                proposed=proposed,
                reason="validation_failure",
                samples=dict(proposed.sample_values),
                error_message=str(e),
            )
        return Resolved(field_mapping=merged, format_ref=None, self_accepted=True)

    return ConfirmationRequired(
        channel=channel,
        confidence=confidence,
        proposed=proposed,
        reason="unknown_layout",
        samples=dict(proposed.sample_values),
    )
