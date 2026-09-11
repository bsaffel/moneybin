"""OFX/QFX file extractor using ofxparse library.

This module extracts financial data from OFX (Open Financial Exchange) and QFX
(Quicken Web Connect) files and converts them into raw table structures suitable
for data warehousing and analysis.

Documentation: https://github.com/jseutter/ofxparse
"""

import hashlib
import html
import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from typing import Any

import ofxparse
import polars as pl
from pydantic import BaseModel, Field, field_validator

from moneybin.database import Database
from moneybin.extractors._types import ExtractionResult, FilePath, ProviderSource
from moneybin.extractors.institution_resolution import (
    display_name_for_fid,
    slug_for_fid,
)
from moneybin.extractors.ofx.config import OFXProviderConfig

# DEPRECATED: extractors-to-services — extractors/ should not import from
# services/ (upward layering inversion); allowlisted in
# test_extractor_layering.py pending MB-246, which relocates SourceAccount and
# friends to a layer both extractors/ and services/ can import.
from moneybin.services.account_display_name import (
    AccountNameFacts,
    account_category,
    derived_last_four,
)

# DEPRECATED: extractors-to-services — see MB-246 (same as above).
from moneybin.services.account_resolution_types import (
    SourceAccount,
    normalize_account_identifier,
)
from moneybin.tables import (
    OFX_ACCOUNTS,
    OFX_BALANCES,
    OFX_INSTITUTIONS,
    OFX_TRANSACTIONS,
)
from moneybin.utils.parsing import coerce_to_decimal

logger = logging.getLogger(__name__)


def _decode_text_field(value: str | None) -> str | None:
    """Repeatedly HTML-unescape a text field until it stabilizes.

    Some banks (notably Wells Fargo) emit SGML payee/memo content that is
    already entity-escaped (e.g. ``AT&amp;T``) and ofxparse decodes only one
    level, so ``AT&amp;amp;T`` survives one pass as ``AT&amp;T`` and lands in
    the database with stale entities. Looping ``html.unescape`` is idempotent
    on already-clean strings — ``html.unescape("AT&T")`` returns ``"AT&T"``.
    """
    if value is None:
        return None
    current = value
    for _ in range(3):  # 3 passes covers single + double + paranoid triple-escape
        decoded = html.unescape(current)
        if decoded == current:
            return current
        current = decoded
    return current


# Fields that distinguish two transactions the institution stamped with the same
# FITID. Order is fixed so the derived suffix is stable across re-imports.
_FITID_SIGNATURE_FIELDS = (
    "transaction_type",
    "date_posted",
    "amount",
    "payee",
    "memo",
    "check_number",
)
# Separates the raw FITID from the content-derived disambiguation suffix. Chosen
# because FITIDs are observed in practice to be alphanumeric and not contain it
# (the OFX spec does not guarantee this, but no real-world FITID we handle does),
# so the marked id is extremely unlikely to collide with a real one.
_FITID_COLLISION_MARKER = "#"


def none_if_blank(value: str | None) -> str | None:
    """Normalize ofxparse's empty-string defaults to NULL.

    ``ofxparse.Account.__init__`` seeds ``account_type``, ``routing_number`` and
    ``branch_id`` with ``''`` and only overwrites them when the corresponding tag
    is present. A credit-card statement uses ``<CCACCTFROM>``, which carries no
    ``<ACCTTYPE>`` and no ``<BANKID>`` per the OFX spec, so both keep ``''``.
    That is a value, not an absence: it survives the
    ``FILTER(WHERE NOT account_type IS NULL)`` golden-record merge in
    core.dim_accounts and can out-rank a real value from a stronger source on
    recency. Absent fields must reach raw as NULL so the merge can skip them.
    """
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


#: Statement containers that state the account kind structurally instead of via
#: ``<ACCTTYPE>``. Per the OFX spec neither ``<CCACCTFROM>`` (inside
#: ``<CCSTMTRS>``) nor ``<INVACCTFROM>`` (inside ``<INVSTMTRS>``) carries that
#: element — being wrapped in the container *is* the statement of type. ofxparse
#: reports it as ``account.type`` while leaving ``account_type`` at ``''``.
_CONTAINER_ACCOUNT_TYPES = {
    ofxparse.AccountType.CreditCard: "CREDITCARD",
    ofxparse.AccountType.Investment: "INVESTMENT",
}


def ofx_account_type(account: Any) -> str | None:
    """Account type from ``<ACCTTYPE>``, falling back to the statement container.

    Reading only ``<ACCTTYPE>`` discards a distinction the file does state, just
    in a different place — which left every credit-card and brokerage account
    untyped. Values are emitted in the file's own uppercase vocabulary;
    seeds.account_type_map is what maps them to the canonical ``credit`` /
    ``investment``. An unrecognized container yields None rather than a guess.
    """
    declared = none_if_blank(account.account_type)
    if declared is not None:
        return declared
    container: Any = getattr(account, "type", None)
    if container is None:
        return None
    return _CONTAINER_ACCOUNT_TYPES.get(container)


def _fitid_content_signature(row: dict[str, Any]) -> str:
    """Unambiguous signature of the fields that make a transaction distinct.

    JSON-encodes the field values rather than joining on a delimiter: a
    free-text ``payee``/``memo`` that itself contains the delimiter would
    otherwise let two genuinely distinct transactions serialize identically
    (``payee="A|B",memo="C"`` vs ``payee="A",memo="B|C"``), making the collision
    check treat them as one and drop a row — the very bug this repairs.

    Values are passed to ``json.dumps`` untouched (``default=str`` stringifies
    only non-JSON-native types like ``Decimal``) so a genuinely absent optional
    field stays JSON ``null`` and can never serialize identically to the literal
    string ``"None"`` — pre-coercing every field with ``str()`` would collapse
    that distinction and reintroduce the same drop-a-row bug in a narrower form.
    """
    return json.dumps(
        [row[field] for field in _FITID_SIGNATURE_FIELDS],
        default=str,
        separators=(",", ":"),
    )


def _disambiguate_colliding_fitids(transactions: list[dict[str, Any]]) -> int:
    """Repair non-unique OFX FITIDs within a file so distinct rows aren't lost.

    The OFX spec promises FITID is unique per account. MoneyBin's raw primary key
    is ``(source_transaction_id, account_id, source_file)`` and the staging dedup
    window keys on ``(source_transaction_id, account_id)``; two colliding rows
    always share ``source_file`` within one import, so both layers collapse them.
    Some institutions violate this — Chase stamps a foreign purchase and its
    foreign-transaction fee (two distinct transactions posted the same day) with
    one shared FITID. Left unrepaired, the raw write path (``on_conflict="upsert"``
    → ``INSERT OR REPLACE``, keyed on that primary key) and the
    ``stg_ofx__transactions`` dedup window each keep only one of the two.

    For each ``(account_id, source_transaction_id)`` group whose members differ
    in content, append a deterministic content-hash suffix to *every* member's
    ``source_transaction_id``. Members with identical content hash to the same
    suffix (so genuine in-file duplicates still collapse); members with differing
    content get distinct ids and both survive. The suffix is a pure function of
    the row's own content, so re-importing the same file reproduces the same ids
    and dedup stays idempotent.

    Suffixing *all* colliding members (rather than leaving one "plain") is
    deliberate: a suffixed id can never equal a plain FITID, so the worst case is
    a missed cross-file dedup (a visible duplicate surfaced for review) — never a
    silent false merge, which is the data-loss failure mode this repairs.

    Scope is one file per call — also deliberate, and the boundary where this is
    provably sound. Within a single export a bank lists each transaction once, so
    two same-FITID rows there are genuinely distinct and safe to split. *Across*
    files a second same-FITID row is ambiguous: it may be a distinct transaction,
    or the same one re-exported with drifted ``payee``/``memo`` (e.g.
    pending→posted). Disambiguating that by content would turn a re-export into a
    duplicate — the opposite failure — and content alone can't tell the two
    apart. Cross-file same-FITID collisions are therefore intentionally not
    repaired here (rare in practice: the observed pattern, a foreign purchase and
    its same-day fee, always co-occurs in one export); a fuller solution needs a
    stronger signal than content and remains open.

    Mutates ``transactions`` in place; returns the number of rows rewritten.
    """
    by_key: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in transactions:
        by_key[(row["account_id"], row["source_transaction_id"])].append(row)

    rewritten = 0
    for rows in by_key.values():
        if len(rows) < 2:
            continue
        if len({_fitid_content_signature(row) for row in rows}) < 2:
            # Every member is byte-identical → a genuine duplicate; leave the id
            # untouched and let the raw PK / staging window collapse it.
            continue
        for row in rows:
            signature = _fitid_content_signature(row)
            # 8 hex (32 bits) rather than identifiers.md's 64-bit content-hash
            # convention: the digest only needs to separate members of one
            # collision group (always 2-3 rows), not be globally unique, so
            # 32 bits is ample and keeps the suffixed id short and log-readable.
            digest = hashlib.sha256(signature.encode()).hexdigest()[:8]
            row["source_transaction_id"] = (
                f"{row['source_transaction_id']}{_FITID_COLLISION_MARKER}{digest}"
            )
            # The marker alone cannot prove this id was generated here: the OFX
            # spec does not reserve it, so an institution may mint both `X` and
            # `X#reference` for two distinct transactions. Staging retires a bare
            # id only against a row flagged here, never against a marker it found.
            row["fitid_repaired"] = True
            rewritten += 1
    return rewritten


_DECIMAL_AMOUNT = pl.Decimal(precision=18, scale=2)
_BALANCE_AMOUNT_OVERRIDES = {
    "ledger_balance": _DECIMAL_AMOUNT,
    "available_balance": _DECIMAL_AMOUNT,
}
_TRANSACTIONS_AMOUNT_OVERRIDES = {"amount": _DECIMAL_AMOUNT}


# Pydantic schemas for OFX data validation
class OFXTransactionSchema(BaseModel):
    """OFX transaction data with validation."""

    id: str = Field(..., description="Financial institution transaction ID (FITID)")
    type: str = Field(..., description="Transaction type (e.g., DEBIT, CREDIT)")
    date: datetime = Field(..., description="Transaction posting date")
    amount: Decimal = Field(..., description="Transaction amount")
    payee: str | None = Field(None, description="Transaction payee/merchant name")
    memo: str | None = Field(None, description="Transaction memo/description")
    checknum: str | None = Field(None, description="Check number if applicable")

    @field_validator("amount", mode="before")
    @classmethod
    def validate_amount(cls, v: Any) -> Decimal:
        """Coerce numeric input to Decimal (required field — None rejected)."""
        result = coerce_to_decimal(v)
        if result is None:
            raise ValueError("amount is required")
        return result

    model_config = {"extra": "allow"}


def preprocess_ofx_content(content: str) -> str:
    """Preprocess OFX content to handle SGML format without newlines.

    Some institutions (notably Wells Fargo QFX exports) emit a single-line
    SGML header that ofxparse rejects. Inserting newlines before each header
    tag normalizes both single- and multi-line forms.
    """
    if content.startswith("OFXHEADER:") and "\n" not in content[:100]:
        if "<OFX>" in content:
            header_part, xml_part = content.split("<OFX>", 1)
            for tag in (
                "OFXHEADER:",
                "DATA:",
                "VERSION:",
                "SECURITY:",
                "ENCODING:",
                "CHARSET:",
                "COMPRESSION:",
                "OLDFILEUID:",
                "NEWFILEUID:",
            ):
                header_part = header_part.replace(tag, f"\n{tag}")
            header_part = header_part.lstrip("\n")
            content = header_part + "\n<OFX>" + xml_part
    return content


def sniff_ofx_content(file_path: Path) -> bool:
    """Return True if the file's first 1024 bytes look like OFX/QFX/QBO content.

    Used by the lifecycle service's extension-based dispatch as a fallback
    when a file's suffix doesn't already say "ofx" — e.g. a ``.pdf``-named
    file that actually carries OFX content.
    """
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


def parse_ofx_content(source_bytes: bytes, *, source_label: str) -> Any:
    """Decode, SGML-preprocess, and parse OFX/QFX bytes into an ofxparse object.

    Shared by ``extract_from_file`` (raw-table extraction) and the lifecycle
    service's pre-import gate (identity enumeration before any batch opens),
    so both apply the same non-UTF-8 handling and SGML preprocessing and fail
    identically on malformed content. Raises whatever ``ofxparse.OfxParser.parse``
    raises — callers wrap it (both do, for the "Invalid OFX file format" message).
    """
    content = source_bytes.decode("utf-8", errors="replace")
    if "�" in content:
        logger.warning(
            f"OFX file contained non-UTF-8 bytes; replaced with U+FFFD: {source_label}"
        )
    content = preprocess_ofx_content(content)
    return ofxparse.OfxParser.parse(BytesIO(content.encode("utf-8")))  # type: ignore[reportUnknownMemberType]


def ofx_source_accounts(parsed_ofx: Any, source_origin: str) -> list[SourceAccount]:
    """Enumerate the account identities an OFX file presents, without resolving.

    Reads the parsed ofxparse object rather than the extractor's DataFrame so it
    can run *before* ``begin_import`` — the confirm gate has to stop the import
    before any batch is opened or any row is ingested.

    One list serves both the gate and the resolve pass. Deriving them separately
    would let the gate propose one identity while resolve binds another; the
    field derivation is shared with ``extract_from_file`` (``none_if_blank``,
    ``ofx_account_type``) for the same reason.

    Deduped by ``<ACCTID>``, because ofxparse emits one ``Account`` per statement
    response with no de-dup of its own: an export that splits one card across two
    ``<STMTRS>`` blocks would otherwise surface it as two independent identities,
    ask about each, and let two different answers write one native key under two
    canonical accounts. ACCTID alone is the right key — it *is* the
    ``source_account_key`` every downstream link and staging JOIN uses, so two
    entries sharing one cannot resolve to different accounts by design.
    """
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
class OFXLoadResult:
    """Per-table row counts returned by :meth:`OFXExtractor.load`."""

    institutions_loaded: int
    accounts_loaded: int
    transactions_loaded: int
    balances_loaded: int

    @property
    def total_rows(self) -> int:
        """Sum across all four raw.ofx_* tables."""
        return (
            self.institutions_loaded
            + self.accounts_loaded
            + self.transactions_loaded
            + self.balances_loaded
        )


class OFXLoadError(RuntimeError):
    """A raw-table write failed partway through :meth:`OFXExtractor.load`.

    Carries the per-table counts of what was actually written before the
    failure. This matters because every ``raw.ofx_*`` write uses
    ``on_conflict="upsert"`` (``INSERT OR REPLACE`` — load-bearing for the
    FITID-collision repair, see ``_disambiguate_colliding_fitids``), and none
    of the four tables' primary keys include ``import_id``: re-importing a
    file whose rows already exist replaces those rows and re-stamps them with
    *this* attempt's ``import_id`` — ``raw.ofx_institutions`` most sharply,
    since its PK (``organization``, ``fid``) has no ``source_file`` at all, so
    loading any file from a known institution re-stamps its shared row. A
    caller that doesn't know what was actually written cannot safely clean up
    on failure — a DELETE scoped to this ``import_id`` would remove rows that
    belong to a previously successful import. The caller must instead
    finalize the batch with these real partial counts, never a hardcoded
    zero and never a same-``import_id`` DELETE.
    """

    def __init__(self, message: str, *, rows_loaded: OFXLoadResult) -> None:
        """Store the per-table counts written before the failure."""
        super().__init__(message)
        self.rows_loaded = rows_loaded


class OFXExtractor:
    """Extract financial data from OFX/QFX files into raw table structures."""

    name = "ofx"
    """Provider name; matches raw.ofx_* table prefix."""

    source_type = "ofx"
    """Written into source_type column on every row produced by this provider."""

    def __init__(
        self,
        config: OFXProviderConfig | None = None,
        db: Database | None = None,
    ):
        """Initialize the OFX extractor.

        Args:
            config: Extraction configuration settings.
            db: An active Database connection (caller-managed per ADR-010),
                matching ``PlaidExtractor``'s shape. Required only for
                ``load()``; ``extract_from_file()`` alone needs no database,
                so existing callers that construct with just a config keep
                working. Keyword-preferred: unlike ``PlaidExtractor`` (which
                takes ``db`` first and always requires it), OFX has long
                had ``config`` as its sole positional argument across many
                call sites — reordering would silently mis-bind ``db`` for
                any caller still passing it positionally.
        """
        from moneybin.config import get_raw_data_path

        self.db = db
        self.config = config or OFXProviderConfig()

        # Resolve raw_data_path locally so the (frozen) config stays
        # immutable. When None, fall back to the profile-aware default.
        self.raw_data_path: Path = (
            self.config.raw_data_path or get_raw_data_path() / "ofx"
        )
        self.raw_data_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"Initialized OFX extractor with output: {self.raw_data_path}")

    def extract(self, source: ProviderSource) -> ExtractionResult:
        """Provider Protocol entry point.

        OFX accepts ``FilePath`` only. ``import_id`` and ``source_origin``
        are framework-supplied — currently still threaded through callers
        via ``extract_from_file()`` directly until Task 5 (the framework
        wiring) lands. This stub satisfies the Protocol's structural shape
        but is not yet a live call path.
        """
        if not isinstance(source, FilePath):
            raise TypeError(
                f"OFXExtractor expects FilePath; got {type(source).__name__}"
            )
        raise NotImplementedError(
            "OFXExtractor.extract() requires framework-supplied import_id and "
            "source_origin; wiring lands in Task 5 of the provider-framework "
            "refactor. Call extract_from_file() directly for now."
        )

    def schema_files(self) -> list[Path]:
        """Return paths to raw.ofx_* DDL files bundled with this package."""
        schema_dir = Path(__file__).parent / "schema"
        return sorted(schema_dir.glob("raw_ofx_*.sql"))

    def extract_from_file(
        self,
        file_path: Path,
        *,
        import_id: str,
        source_origin: str,
        source_bytes: bytes | None = None,
    ) -> dict[str, pl.DataFrame]:
        """Extract all data from an OFX/QFX/QBO file.

        Args:
            file_path: Path to the file.
            import_id: UUID of the import batch this extraction belongs to.
                Stamped on every row in every returned DataFrame.
            source_origin: Institution slug resolved by the caller (service layer).
                Stamped on transactions.
            source_bytes: Contents the caller already read from ``file_path``.
                Pass the same buffer that produced the batch's digest so the
                loaded rows and the recorded content identity describe one
                version of the file; omit it and the path is read again here,
                leaving a window a synced folder can rewrite.

        Returns:
            dict with DataFrames for institutions, accounts, transactions, balances.

        Raises:
            FileNotFoundError: If the file doesn't exist.
            ValueError: If the file cannot be parsed.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"OFX file not found: {file_path}")

        logger.info(f"Extracting data from OFX file: {file_path}")

        try:
            if source_bytes is None:
                with open(file_path, "rb") as f:
                    source_bytes = f.read()
            ofx = parse_ofx_content(source_bytes, source_label=str(file_path))

            extraction_timestamp = datetime.now()
            source_file = str(file_path)

            results = {
                "institutions": self._extract_institutions(
                    ofx, source_file, extraction_timestamp, import_id, source_origin
                ),
                "accounts": self._extract_accounts(
                    ofx, source_file, extraction_timestamp, import_id, source_origin
                ),
                "transactions": self._extract_transactions(
                    ofx, source_file, extraction_timestamp, import_id, source_origin
                ),
                "balances": self._extract_balances(
                    ofx, source_file, extraction_timestamp, import_id, source_origin
                ),
            }

            logger.info(
                f"Extracted {len(results['institutions'])} institution(s), "
                f"{len(results['accounts'])} account(s), "
                f"{len(results['transactions'])} transaction(s)"
            )

            return results

        except Exception as e:
            # Don't interpolate `e` into the log message: ofxparse exception
            # strings can embed payee/amount/memo content from the file. The
            # exception type name + file path is enough for diagnostics.
            logger.error(f"Failed to parse OFX file {file_path}: {type(e).__name__}")
            raise ValueError(f"Invalid OFX file format: {type(e).__name__}") from e

    def load(
        self,
        file_path: Path,
        *,
        import_id: str,
        source_origin: str,
        source_bytes: bytes | None = None,
    ) -> OFXLoadResult:
        """Extract an OFX/QFX file and write its rows to raw.ofx_* tables.

        Matches :meth:`PlaidExtractor.load`'s shape: the extractor owns the
        raw-table write, the caller (the lifecycle service) owns everything
        upstream (re-import detection, account gating/resolution) and
        downstream (import-log finalization, metrics).

        Raises:
            RuntimeError: If this instance was constructed without a
                Database — ``load()`` needs one; ``extract_from_file()``
                alone does not.
            OFXLoadError: If a raw-table write fails partway through. Carries
                the per-table counts of what was actually written so the
                caller can finalize with real partial progress — see the
                exception's docstring for why the caller must never delete
                by ``import_id`` to "clean up" instead.
        """
        if self.db is None:
            raise RuntimeError(
                "OFXExtractor.load() requires a Database; construct with "
                "OFXExtractor(config, db=...)."
            )
        data = self.extract_from_file(
            file_path,
            import_id=import_id,
            source_origin=source_origin,
            source_bytes=source_bytes,
        )
        rows_loaded: dict[str, int] = {
            "institutions": 0,
            "accounts": 0,
            "transactions": 0,
            "balances": 0,
        }
        for table_key, qualified in (
            ("institutions", OFX_INSTITUTIONS.full_name),
            ("accounts", OFX_ACCOUNTS.full_name),
            ("transactions", OFX_TRANSACTIONS.full_name),
            ("balances", OFX_BALANCES.full_name),
        ):
            df = data[table_key]
            try:
                if len(df) > 0:
                    self.db.ingest_dataframe(qualified, df, on_conflict="upsert")
                rows_loaded[table_key] = len(df)
            except Exception as e:
                raise OFXLoadError(
                    f"OFX raw-table write failed on {table_key}: {type(e).__name__}",
                    rows_loaded=OFXLoadResult(
                        institutions_loaded=rows_loaded["institutions"],
                        accounts_loaded=rows_loaded["accounts"],
                        transactions_loaded=rows_loaded["transactions"],
                        balances_loaded=rows_loaded["balances"],
                    ),
                ) from e
        return OFXLoadResult(
            institutions_loaded=rows_loaded["institutions"],
            accounts_loaded=rows_loaded["accounts"],
            transactions_loaded=rows_loaded["transactions"],
            balances_loaded=rows_loaded["balances"],
        )

    def _extract_institutions(
        self,
        ofx: Any,
        source_file: str,
        extraction_timestamp: datetime,
        import_id: str,
        source_origin: str,
    ) -> pl.DataFrame:
        """Extract institution information from OFX data.

        ``raw.ofx_institutions.organization`` is part of the primary key, so a
        NULL ORG element would break the insert. Fall back to ``source_origin``
        (the resolved slug) so files lacking ``<FI><ORG>`` still load.
        """
        institutions_data: list[dict[str, Any]] = []

        for account in ofx.accounts:
            if account.institution:
                institution_data = {
                    "organization": account.institution.organization or source_origin,
                    "fid": account.institution.fid,
                    "source_file": source_file,
                    "extracted_at": extraction_timestamp.isoformat(),
                    "import_id": import_id,
                    "source_type": "ofx",
                }
                institutions_data.append(institution_data)

        # Deduplicate institutions
        if institutions_data:
            df = pl.DataFrame(institutions_data)
            return df.unique(  # pyright: ignore[reportUnknownMemberType]  # polars stubs partially unknown
                subset=["organization", "fid"], maintain_order=True
            )
        return pl.DataFrame(
            schema={
                "organization": pl.String,
                "fid": pl.String,
                "source_file": pl.String,
                "extracted_at": pl.String,
                "import_id": pl.String,
                "source_type": pl.String,
            }
        )

    def _extract_accounts(
        self,
        ofx: Any,
        source_file: str,
        extraction_timestamp: datetime,
        import_id: str,
        source_origin: str,
    ) -> pl.DataFrame:
        """Extract account information from OFX data."""
        accounts_data: list[dict[str, Any]] = []

        for account in ofx.accounts:
            inst_org = account.institution.organization if account.institution else None
            account_info = {
                "account_id": account.account_id,
                "routing_number": none_if_blank(account.routing_number),
                "account_type": ofx_account_type(account),
                "institution_org": inst_org or source_origin,
                "institution_fid": none_if_blank(
                    account.institution.fid if account.institution else None
                ),
                "source_file": source_file,
                "extracted_at": extraction_timestamp.isoformat(),
                "import_id": import_id,
                "source_type": "ofx",
                # source_origin must match app.account_links.source_origin so the
                # staging translation JOIN in stg_ofx__accounts is total (B1).
                # Do NOT change how source_origin is derived here.
                "source_origin": source_origin,
            }
            accounts_data.append(account_info)

        if accounts_data:
            return pl.DataFrame(accounts_data)
        return pl.DataFrame(
            schema={
                "account_id": pl.String,
                "routing_number": pl.String,
                "account_type": pl.String,
                "institution_org": pl.String,
                "institution_fid": pl.String,
                "source_file": pl.String,
                "extracted_at": pl.String,
                "import_id": pl.String,
                "source_type": pl.String,
                "source_origin": pl.String,
            }
        )

    def _extract_transactions(
        self,
        ofx: Any,
        source_file: str,
        extraction_timestamp: datetime,
        import_id: str,
        source_origin: str,
    ) -> pl.DataFrame:
        """Extract transaction data from OFX file."""
        transactions_data: list[dict[str, Any]] = []

        for account in ofx.accounts:
            for transaction in account.statement.transactions:
                tx_schema = OFXTransactionSchema(
                    id=transaction.id,
                    type=transaction.type,
                    date=transaction.date,
                    amount=transaction.amount,
                    payee=transaction.payee,
                    memo=transaction.memo,
                    checknum=transaction.checknum
                    if hasattr(transaction, "checknum")
                    else None,
                )

                tx_data = {
                    "source_transaction_id": tx_schema.id,
                    "account_id": account.account_id,
                    "transaction_type": tx_schema.type,
                    "currency_code": account.curdef
                    if hasattr(account, "curdef")
                    else None,
                    "date_posted": tx_schema.date.isoformat(),
                    "amount": tx_schema.amount,
                    "payee": _decode_text_field(tx_schema.payee),
                    "memo": _decode_text_field(tx_schema.memo),
                    "check_number": tx_schema.checknum,
                    "source_file": source_file,
                    "extracted_at": extraction_timestamp.isoformat(),
                    "import_id": import_id,
                    "source_type": "ofx",
                    "source_origin": source_origin,
                    # Overwritten by _disambiguate_colliding_fitids on the rows it
                    # rewrites. Every row carries the key so the frame keeps a
                    # BOOLEAN column even when no collision occurred.
                    "fitid_repaired": False,
                }
                transactions_data.append(tx_data)

        if transactions_data:
            repaired = _disambiguate_colliding_fitids(transactions_data)
            if repaired:
                from moneybin.metrics.registry import (
                    OFX_FITID_COLLISION_REPAIRED_TOTAL,
                )

                OFX_FITID_COLLISION_REPAIRED_TOTAL.inc(repaired)
                logger.warning(
                    f"Repaired {repaired} OFX transaction(s) sharing a non-unique "
                    f"FITID within one file (institution reused a FITID for distinct "
                    f"transactions); disambiguated by content to prevent dedup loss"
                )
            return pl.DataFrame(
                transactions_data,
                schema_overrides=_TRANSACTIONS_AMOUNT_OVERRIDES,
            )
        return self._build_empty_transactions_df()

    def _build_empty_transactions_df(self) -> pl.DataFrame:
        """Build an empty transactions DataFrame with the correct schema."""
        return pl.DataFrame(
            schema={
                "source_transaction_id": pl.String,
                "account_id": pl.String,
                "transaction_type": pl.String,
                "currency_code": pl.String,
                "date_posted": pl.String,
                "amount": _DECIMAL_AMOUNT,
                "payee": pl.String,
                "memo": pl.String,
                "check_number": pl.String,
                "source_file": pl.String,
                "extracted_at": pl.String,
                "import_id": pl.String,
                "source_type": pl.String,
                "source_origin": pl.String,
                "fitid_repaired": pl.Boolean,
            }
        )

    def _extract_balances(
        self,
        ofx: Any,
        source_file: str,
        extraction_timestamp: datetime,
        import_id: str,
        source_origin: str,
    ) -> pl.DataFrame:
        """Extract balance information from OFX file."""
        balances_data: list[dict[str, Any]] = []

        for account in ofx.accounts:
            statement = account.statement
            if statement:
                balance_info = {
                    "account_id": account.account_id,
                    "statement_start_date": statement.start_date.isoformat()
                    if statement.start_date
                    else None,
                    "statement_end_date": statement.end_date.isoformat()
                    if statement.end_date
                    else None,
                    "ledger_balance": statement.balance
                    if statement.balance is not None
                    else None,
                    "ledger_balance_date": statement.balance_date.isoformat()
                    if hasattr(statement, "balance_date") and statement.balance_date
                    else None,
                    "available_balance": statement.available_balance
                    if hasattr(statement, "available_balance")
                    and statement.available_balance is not None
                    else None,
                    "currency_code": account.curdef
                    if hasattr(account, "curdef")
                    else None,
                    "source_file": source_file,
                    "extracted_at": extraction_timestamp.isoformat(),
                    "import_id": import_id,
                    "source_type": "ofx",
                    # source_origin must match app.account_links.source_origin so the
                    # staging translation JOIN in stg_ofx__balances is total (B2).
                    # Do NOT change how source_origin is derived here.
                    "source_origin": source_origin,
                }
                balances_data.append(balance_info)

        if balances_data:
            return pl.DataFrame(
                balances_data, schema_overrides=_BALANCE_AMOUNT_OVERRIDES
            )
        return pl.DataFrame(
            schema={
                "account_id": pl.String,
                "statement_start_date": pl.String,
                "statement_end_date": pl.String,
                "ledger_balance": _DECIMAL_AMOUNT,
                "ledger_balance_date": pl.String,
                "available_balance": _DECIMAL_AMOUNT,
                "currency_code": pl.String,
                "source_file": pl.String,
                "extracted_at": pl.String,
                "import_id": pl.String,
                "source_type": pl.String,
                "source_origin": pl.String,
            }
        )


def extract_ofx_file(
    file_path: Path | str,
    *,
    import_id: str,
    source_origin: str,
) -> dict[str, pl.DataFrame]:
    """Convenience function to extract data from an OFX/QFX/QBO file."""
    extractor = OFXExtractor()
    return extractor.extract_from_file(
        Path(file_path), import_id=import_id, source_origin=source_origin
    )
