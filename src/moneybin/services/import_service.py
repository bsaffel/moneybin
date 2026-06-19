"""Unified import service for financial data files.

This service handles the full import pipeline: detect file type, extract
data, load to raw tables, and run SQLMesh transforms. Both CLI commands and
MCP tools call this same service — no duplication.
"""

import dataclasses
import hashlib
import logging
import time
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, NoReturn, cast

import duckdb

if TYPE_CHECKING:
    from moneybin.extractors.pdf.ir import PdfDocument
    from moneybin.extractors.pdf.routing import RouteDecision

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.extractors.institution_resolution import resolve_institution_tabular
from moneybin.extractors.tabular.account_label import parse_account_label
from moneybin.extractors.tabular.formats import (
    NumberFormatType,
    SignConventionType,
)
from moneybin.metrics.registry import (
    ACCOUNT_LINK_OUTCOMES_TOTAL,
    IMPORT_DURATION_SECONDS,
    IMPORT_ERRORS_TOTAL,
    IMPORT_RECORDS_TOTAL,
    TABULAR_DETECTION_CONFIDENCE,
    TABULAR_FORMAT_MATCHES,
)
from moneybin.repositories.imports_repo import ImportsRepo
from moneybin.repositories.pdf_formats_repo import PdfFormatsRepo
from moneybin.services._validators import validate_slug
from moneybin.services.account_resolution_types import (
    AccountProposalDict,
    SourceAccount,
)
from moneybin.services.account_resolver import AccountResolver
from moneybin.services.audit_service import AuditService
from moneybin.services.import_confirmation import (
    ActorKind,
    ImportConfirmationRequiredError,
)
from moneybin.services.refresh import refresh as _refresh

logger = logging.getLogger(__name__)


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
    sign_correction_suggested: bool = False
    """True if running balance suggests sign inversion; amounts were NOT auto-corrected."""
    import_id: str | None = None
    """UUID of the raw.import_log row this import created."""
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
    sign_correction_suggested: bool = False
    """True if running balance suggests sign inversion; amounts were NOT auto-corrected."""

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
# The deterministic rung produced *something* but couldn't finalize it — the
# driving agent has a chance to crack the layout where the deterministic path
# couldn't. ``no_transaction_table``, ``no_rows``, and ``unsupported_number_format``
# are deliberately NOT in this set: the document isn't transaction-shaped (so
# bridge would be off-target) or has no extractable content (so a text-bridge
# has nothing to read).
_BRIDGE_ELIGIBLE_REASONS: frozenset[str] = frozenset({
    "low_confidence",
    "replay_reconciliation_failed",
    "reconciliation_failed",
    "metadata_incomplete",
})


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
    ``no_rows`` / ``unsupported_number_format`` on non-escalating fallbacks)."""

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


def _display_label(file_type: str, file_path: Path) -> str:
    """User-facing label for a detected file type.

    ``"tabular"`` is an internal bucket (CSV/TSV/XLSX/Parquet/Feather all
    share one pipeline). Resolve it to the file's actual extension so the
    user sees ``CSV`` / ``XLSX`` / ``OFX`` instead of ``TABULAR``.
    """
    if file_type == "tabular":
        return file_path.suffix.lstrip(".").upper() or "TABULAR"
    return file_type.upper()


def _bare_account_key(file_path: Path) -> str:
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

    digest = hashlib.sha256(file_path.read_bytes()).hexdigest()[:12]
    return f"{slugify(file_path.stem) or 'file'}-{digest}"


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


_ACCOUNT_MASK_PREFIXES: tuple[str, ...] = ("****", "xxxx", "XXXX")


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

    Returns the original string when no digits are present (e.g. an
    institution-specific token) so we never silently drop a captured value.
    """
    if raw is None:
        return None
    stripped = raw.strip()
    if not stripped:
        return None
    if any(stripped.startswith(prefix) for prefix in _ACCOUNT_MASK_PREFIXES):
        return stripped
    digits = "".join(c for c in stripped if c.isdigit())
    if not digits:
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
    "iso_currency_code",
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
            iso_currency_code=meta.get("iso_currency_code"),
        )


def _apply_account_bindings(
    source_accounts: list[SourceAccount], bindings: dict[str, str]
) -> list[SourceAccount]:
    """Fold each source account's binding into the resolver's input fields.

    A binding value of ``"new"`` sets ``force_standalone`` (mint a fresh
    account, skip the merge-candidate pass); any other value is an existing
    canonical ``account_id`` to adopt (``explicit_account_id``). Unbound
    accounts pass through unchanged so the gate can still surface them.

    Raises ``ValueError`` on an empty binding value — ``explicit_account_id=""``
    is falsy and would silently fall through to a fresh mint as if no binding
    were given, discarding the caller's intent ("magic stays visible").
    """
    if not bindings:
        return source_accounts
    bound: list[SourceAccount] = []
    for src in source_accounts:
        target = bindings.get(src.source_account_key)
        if target is None:
            bound.append(src)
        elif target == "new":
            bound.append(
                dataclasses.replace(
                    src, force_standalone=True, explicit_account_id=None
                )
            )
        elif not target.strip():
            # Reject whitespace-only too: CLI input is not stripped (_parse_kv
            # keeps the raw value) and MCP passes JSON as-is, so a bare-spaces
            # value would otherwise be truthy and bind a bogus account_id.
            raise ValueError(
                f"account_bindings for source key {src.source_account_key!r} "
                'has an empty value; use an existing account_id or "new".'
            )
        else:
            bound.append(dataclasses.replace(src, explicit_account_id=target))
    return bound


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
        trace which surface (cli/mcp) initiated the batch.
        """
        from moneybin.loaders import import_log

        return import_log.begin_import(
            self._db,
            source_file=f"<{source_type}:{actor}>",
            source_type=source_type,  # type: ignore[arg-type]  # runtime-validated
            source_origin=actor,
            account_names=[actor],
            format_name=format_name,
            format_source="manual",
        )

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

        Returns:
            ImportResult with summary.

        Raises:
            ValueError: On re-import without force, or when institution can't be derived.
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

        # Re-import detection
        if not force:
            existing = import_log.find_existing_import(self._db, str(canonical_path))
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

        # Parse once for institution resolution; the extractor parses again
        # internally. These files are small — the duplicate parse is fine and
        # avoids leaking a parser-internal type into the extractor signature.
        # Wrap read+parse failures as ValueError so MCP's error envelope catches
        # them; otherwise PermissionError/OSError leak as internal tool errors.
        try:
            with open(canonical_path, "rb") as f:
                content = f.read().decode("utf-8", errors="replace")
        except OSError as e:
            IMPORT_ERRORS_TOTAL.labels(source_type="ofx", error_type="read").inc()
            raise ValueError(f"Could not read OFX file: {e}") from e
        if "�" in content:
            logger.warning(
                f"OFX file contained non-UTF-8 bytes; replaced with U+FFFD: "
                f"{canonical_path.name}"
            )
        content = preprocess_ofx_content(content)
        try:
            parsed_ofx: Any = ofxparse.OfxParser.parse(  # type: ignore[reportUnknownMemberType]
                BytesIO(content.encode("utf-8"))
            )
        except Exception as e:
            IMPORT_ERRORS_TOTAL.labels(source_type="ofx", error_type="parse").inc()
            raise ValueError(f"Invalid OFX file format: {e}") from e

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
        )
        result.import_id = import_id

        extractor = OFXExtractor()
        try:
            data = extractor.extract_from_file(
                canonical_path,
                import_id=import_id,
                source_origin=source_origin,
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
                ("institutions", "raw.ofx_institutions"),
                ("accounts", "raw.ofx_accounts"),
                ("transactions", "raw.ofx_transactions"),
                ("balances", "raw.ofx_balances"),
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
        # stuck in 'importing'.
        try:
            resolver = AccountResolver(self._db, actor="system")
            for row in data["accounts"].iter_rows(named=True):
                acctid: str | None = row["account_id"]
                if not acctid:
                    continue
                routing = row.get("routing_number")
                # full_number is a strong ref ONLY when institution/routing-scoped
                # (contains ':'); a bare number is demoted to a candidate signal.
                scoped_number = f"{routing}:{acctid}" if routing else None
                resolved_account = resolver.resolve(
                    SourceAccount(
                        source_type="ofx",
                        source_origin=source_origin,
                        source_account_key=acctid,
                        account_name=f"{source_origin} "
                        f"{row.get('account_type') or ''}".strip(),
                        account_number=scoped_number,
                        last_four=acctid[-4:],
                        institution=source_origin,
                    )
                )
                ACCOUNT_LINK_OUTCOMES_TOTAL.labels(
                    result=resolved_account.outcome
                ).inc()
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
                "raw.ofx_transactions", "CAST(date_posted AS DATE)", canonical_path
            )

        return result

    def _capture_new_account_metadata(
        self, account_id: str, meta: dict[str, str]
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
            iso_currency_code=meta.get("iso_currency_code"),
        )
        AccountSettingsRepo(self._db, audit=self._audit).set(
            account_id=settings.account_id,
            display_name=settings.display_name,
            official_name=settings.official_name,
            last_four=settings.last_four,
            account_subtype=settings.account_subtype,
            holder_category=settings.holder_category,
            iso_currency_code=settings.iso_currency_code,
            credit_limit=settings.credit_limit,
            archived=settings.archived,
            include_in_net_worth=settings.include_in_net_worth,
            actor="import",
        )

    def _gate_account_proposals(
        self,
        resolver: AccountResolver,
        source_accounts: list[SourceAccount],
        *,
        actor_kind: "ActorKind",
        resolved_mapping: dict[str, str],
    ) -> None:
        """Surface weak account-merge candidates for confirmation before load.

        Interactive-human first contact only: when an unbound source account
        resolves to weak merge candidate(s), raise
        ``ImportConfirmationRequiredError`` (no rows load) so the human ratifies
        the account identity (adopt a candidate or declare ``"new"``). Agent /
        non-interactive imports never gate here — they mint+propose and the
        proposal stays visible in the account-link review queue (M1S.5), per
        ``account-identity-resolution.md`` Decision 7.
        """
        if actor_kind != "human":
            return
        proposals: list[AccountProposalDict] = []
        for src in source_accounts:
            # A bound account (explicit_account_id / force_standalone) is already
            # decided; only ambiguous unbound accounts gate.
            if src.explicit_account_id or src.force_standalone:
                continue
            proposal = resolver.propose(src)
            if proposal.candidates:
                proposals.append(proposal.to_dict())
        if not proposals:
            return
        from moneybin.extractors.confidence import Confidence
        from moneybin.services.import_confirmation import (
            ConfirmationRequired,
            ImportConfirmationRequiredError,
            ProposedMapping,
        )

        raise ImportConfirmationRequiredError(
            ConfirmationRequired(
                channel="tabular",
                # The column layout is already resolved; only the account
                # identity is in question. high/1.0 reflects the settled mapping.
                confidence=Confidence(
                    score=1.0, tier="high", flagged=(), missing_required=()
                ),
                proposed=ProposedMapping(
                    field_mapping=resolved_mapping,
                    sample_values={},
                    unmapped_columns=(),
                ),
                reason="account_confirmation",
                account_proposals=proposals,
            )
        )

    def _import_tabular(
        self,
        file_path: Path,
        *,
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
        actor_kind: "ActorKind" = "human",
        account_bindings: dict[str, str] | None = None,
        account_metadata: dict[str, dict[str, str]] | None = None,
    ) -> ImportResult:
        """Import a tabular file through the five-stage pipeline.

        Args:
            file_path: Path to the file.
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
            actor_kind: 'human' (always surfaces) or 'agent' (may self-accept at high tier).
            account_bindings: Map of source_account_key -> canonical account_id
                (adopt) or "new" (mint standalone), ratifying the account-binding
                confirmation. Unbound accounts with weak candidates gate for a
                human caller.
            account_metadata: Map of source_account_key -> {display_name,
                account_subtype, last_four, iso_currency_code} captured into
                app.account_settings for accounts minted this import.

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

        # Load formats early so explicit --format can influence file reading
        builtin_formats = load_builtin_formats()
        all_formats = merge_formats(builtin_formats, load_formats_from_db(self._db))

        matched_format: TabularFormat | None = None
        if format_name:
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

        format_info = detect_format(
            file_path,
            format_override=matched_format.file_type
            if matched_format and matched_format.file_type != "auto"
            else None,
            delimiter_override=effective_delimiter,
            encoding_override=effective_encoding,
            no_size_limit=no_size_limit,
        )

        # Stage 2: Read file
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
        )
        df = read_result.df

        if len(df) == 0:
            raise ValueError(f"No data rows found in {file_path.name}")

        # Stage 3: Column mapping — match by headers if not already matched by name
        if not matched_format:
            headers = list(df.columns)
            for fmt in all_formats.values():
                if fmt.matches_headers(headers):
                    matched_format = fmt
                    break

        if matched_format:
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
                resolve_or_confirm,
            )

            settings = get_settings()
            bands = settings.import_.confidence
            mapping_result = map_columns(
                df, overrides=overrides, t_high=bands.t_high, t_med=bands.t_med
            )
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
            # override resolves the single/split contention. Both this
            # pre-compute and validate_partial_mapping's merge logic call
            # resolve_amount_shape so a future shape addition only updates
            # one place.
            from moneybin.extractors.tabular.field_aliases import FIELD_ALIASES
            from moneybin.services.import_confirmation import resolve_amount_shape

            amount_required = resolve_amount_shape(
                proposed_keys=set(proposed.field_mapping.keys()),
                override_keys=set(overrides.keys()) if overrides else set(),
            )
            required_fields = (
                "transaction_date",
                *amount_required,
                "description",
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

            IMPORT_DETECTION_SCORE.observe(confidence.score)

            if isinstance(outcome, ConfirmationRequired):
                IMPORT_CONFIRMATIONS_TOTAL.labels(
                    channel="tabular",
                    tier=confidence.tier,
                    outcome="declined",
                ).inc()
                raise ImportConfirmationRequiredError(outcome)

            if outcome.self_accepted:
                IMPORT_SELF_ACCEPT_TOTAL.labels(channel="tabular").inc()
            if isinstance(signal, Override):
                IMPORT_OVERRIDE_TOTAL.labels(channel="tabular").inc()
                IMPORT_CONFIRMATIONS_TOTAL.labels(
                    channel="tabular",
                    tier=confidence.tier,
                    outcome="overridden",
                ).inc()
            else:
                IMPORT_CONFIRMATIONS_TOTAL.labels(
                    channel="tabular",
                    tier=confidence.tier,
                    outcome="accepted",
                ).inc()

            # Coerce sign_convention based on the resolved amount shape so an
            # override that swaps single ⇄ split doesn't carry a stale
            # detector-derived convention into transform_dataframe (split
            # rule against a single ``amount`` mapping rejects every row,
            # and vice versa). Detector-derived sign for the resolved shape
            # is preserved when the override didn't change the shape.
            resolved_has_split = (
                "debit_amount" in outcome.field_mapping
                and "credit_amount" in outcome.field_mapping
            )
            detector_was_split = mapping_result.sign_convention == "split_debit_credit"
            if resolved_has_split and not detector_was_split:
                resolved_sign = "split_debit_credit"
            elif not resolved_has_split and detector_was_split:
                # Split → single: detector's split-only convention is no
                # longer valid. Fall back to the default
                # ``negative_is_expense``; callers can pass --sign to
                # override if their export uses a different convention.
                resolved_sign = "negative_is_expense"
            else:
                resolved_sign = mapping_result.sign_convention
            resolved = ResolvedMapping(
                field_mapping=outcome.field_mapping,
                date_format=mapping_result.date_format or "%Y-%m-%d",
                sign_convention=resolved_sign,
                number_format=mapping_result.number_format,
                is_multi_account=mapping_result.is_multi_account,
                confidence=confidence.tier,
            )
            format_source = "detected"

            if mapping_result.sign_needs_confirmation and not sign:
                logger.warning(
                    "⚠️  Sign convention is ambiguous (all amounts appear positive). "
                    f"Proceeding with '{resolved.sign_convention}' — "
                    "use --sign to override if expense amounts look wrong."
                )

        # Record format match and detection confidence metrics
        if matched_format:
            from moneybin.metrics.registry import IMPORT_KNOWN_FORMAT_REUSE_TOTAL

            IMPORT_KNOWN_FORMAT_REUSE_TOTAL.labels(channel="tabular").inc()
            TABULAR_FORMAT_MATCHES.labels(
                format_name=matched_format.name, format_source=format_source
            ).inc()
        TABULAR_DETECTION_CONFIDENCE.labels(confidence=resolved.confidence).inc()

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
                code="invalid_sign_convention",
            )
        if number_format_override and number_format_override not in get_args(
            NumberFormatType
        ):
            raise UserError(
                f"Invalid number format: {number_format_override!r}. "
                f"Valid values: {list(get_args(NumberFormatType))}.",
                code="invalid_number_format",
            )
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
        # Stage 5 reassigns `institution` for the account_df flow; keep the
        # resolved (format / filename) value for the auto-save block below so a
        # saved format records its real institution rather than always "unknown".
        resolved_institution = institution
        resolver = AccountResolver(self._db, actor="system")
        bindings = account_bindings or {}

        # account_ids stamped on raw are source-NATIVE keys (DP-1); the resolver
        # writes the native->canonical app.account_links mapping as a side effect.
        acct_name_col = resolved.field_mapping.get("account_name")
        acct_num_col = resolved.field_mapping.get("account_number")
        acct_id_to_name: dict[str, str] = {}
        # Parse each display label once: (clean_name, label_last4). Reused by the
        # resolver pass (clean name strips mask text → stronger fuzzy match) and
        # by Stage 5's account_number_masked, so parse_account_label runs once.
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

        # Phase 1 — enumerate the source accounts this file presents (one per
        # native key) WITHOUT resolving, so the account-binding gate can run
        # between enumeration and the writing resolve() pass.
        source_accounts: list[SourceAccount] = []
        if account_id:
            account_ids: str | list[str] = account_id
            acct_id_to_name[account_id] = account_name or account_id
            # Parse only a real display label, never the canonical --account-id
            # itself: an opaque id ending in 4 digits ("acct-1234") would
            # otherwise fabricate a "****1234" bank mask in dim_accounts. No
            # label supplied → no derived last4.
            label_parsed_by_key[account_id] = (
                parse_account_label(account_name)
                if account_name
                else (account_id, None)
            )
            source_accounts.append(
                SourceAccount(
                    source_type=source_type,
                    source_origin=source_origin,
                    source_account_key=account_id,
                    account_name=account_name or account_id,
                    institution=institution,
                    explicit_account_id=account_id,
                )
            )
        elif account_name:
            native_key = slugify(account_name)
            account_ids = native_key
            acct_id_to_name[native_key] = account_name
            clean_name, label_last4 = parse_account_label(account_name)
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
                    institution=institution,
                    last_four=label_last4 or number_last4_by_key.get(native_key),
                )
            )
        elif (
            resolved.is_multi_account and acct_name_col and acct_name_col in df.columns
        ):
            raw_names = [
                str(v) if v is not None else "unknown"
                for v in df[acct_name_col].to_list()
            ]
            account_ids = [slugify(name) for name in raw_names]
            for aid, name in zip(account_ids, raw_names, strict=True):
                if aid not in acct_id_to_name:
                    acct_id_to_name[aid] = name
            label_parsed_by_key = {
                nk: parse_account_label(nm) for nk, nm in acct_id_to_name.items()
            }
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
                    institution=multi_acct_inst.get(native_key),
                    last_four=(
                        label_parsed_by_key[native_key][1]
                        or number_last4_by_key.get(native_key)
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
            native_key = _bare_account_key(file_path)
            account_ids = native_key
            placeholder_name = file_path.stem or native_key
            acct_id_to_name[native_key] = placeholder_name
            label_parsed_by_key[native_key] = (placeholder_name, None)
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
                    account_name=placeholder_name,
                    institution=institution,
                    last_four=number_last4_by_key.get(native_key),
                )
            )
            # No binding answer yet → surface the no-candidate account
            # confirmation (no rows load). A later import_confirm with
            # --account-binding <native_key>=<account_id|new> re-enters here and
            # proceeds through Phase 2; --account-name re-enters the branch above.
            # Elicit only when genuinely unknown: no confirm answer (binding)
            # AND no prior accepted source_native for this exact content. An
            # exact-same-file re-import adopts via resolve() Step-1 without
            # re-prompting (idempotency, not a filename guess).
            if native_key not in bindings and not resolver.source_native_exists(
                source_type, source_origin, native_key
            ):
                from moneybin.extractors.confidence import Confidence
                from moneybin.services.account_resolution_types import (
                    AccountProposal,
                )
                from moneybin.services.import_confirmation import (
                    ConfirmationRequired,
                    ImportConfirmationRequiredError,
                    ProposedMapping,
                )

                raise ImportConfirmationRequiredError(
                    ConfirmationRequired(
                        channel="tabular",
                        # Layout is settled; only the account identity is open.
                        confidence=Confidence(
                            score=1.0, tier="high", flagged=(), missing_required=()
                        ),
                        proposed=ProposedMapping(
                            field_mapping=dict(resolved.field_mapping),
                            sample_values={},
                            unmapped_columns=(),
                        ),
                        reason="account_confirmation",
                        account_proposals=[
                            AccountProposal(
                                source_account_key=native_key,
                                proposed_account_id=None,
                                is_new=True,
                                candidates=(),
                                adopted_via=None,
                            ).to_dict()
                        ],
                    )
                )

        # Phase 2 — apply explicit bindings, then gate on weak account proposals.
        # The gate raises ImportConfirmationRequiredError (no rows load) for an
        # interactive human first-contact with ambiguous candidates.
        #
        # Fail loud on a binding/metadata source key that doesn't match any of
        # this file's accounts (a typo) — silently ignoring it would do the
        # wrong thing invisibly ("magic stays visible").
        known_keys = {s.source_account_key for s in source_accounts}
        for label, keyed in (
            ("account_bindings", bindings),
            ("account_metadata", account_metadata or {}),
        ):
            unknown_keys = set(keyed) - known_keys
            if unknown_keys:
                raise ValueError(
                    f"{label} references unknown source key(s): "
                    f"{sorted(unknown_keys)}. This file's source keys: "
                    f"{sorted(known_keys)}."
                )
        source_accounts = _apply_account_bindings(source_accounts, bindings)
        self._gate_account_proposals(
            resolver,
            source_accounts,
            actor_kind=actor_kind,
            resolved_mapping=dict(resolved.field_mapping),
        )

        # Phase 3 — resolve (writes native->canonical mapping + pending decisions),
        # then capture any caller-supplied metadata for accounts minted this import.
        metadata = account_metadata or {}
        for src in source_accounts:
            resolved_account = resolver.resolve(src)
            ACCOUNT_LINK_OUTCOMES_TOTAL.labels(result=resolved_account.outcome).inc()
            meta = metadata.get(src.source_account_key)
            if not meta:
                continue
            # Capture only for a genuinely-new account (outcome="minted_new",
            # i.e. a "new" binding or a clean no-candidate mint). A
            # pending_review provisional is is_new=True too, but a later
            # accept re-points it onto the candidate and abandons the
            # provisional id — settings written here would be orphaned. An
            # adopted account keeps its existing settings.
            if resolved_account.outcome == "minted_new":
                self._capture_new_account_metadata(resolved_account.account_id, meta)
            else:
                # Routine on the agent path (a binding adopted an existing
                # account, or the account went to pending_review) — info, not a
                # warning about an error.
                logger.info(
                    "account_metadata ignored: account resolved to "
                    f"{resolved_account.outcome!r}, not a new mint."
                )

        # Create import batch
        extractor = TabularExtractor(self._db)
        import_id = extractor.create_import_batch(
            source_file=str(file_path),
            source_type=source_type,
            source_origin=source_origin,
            account_names=sorted(acct_id_to_name.values()),
            format_name=matched_format.name if matched_format else None,
            format_source=format_source,
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
            )
            IMPORT_ERRORS_TOTAL.labels(
                source_type=source_type, error_type="transform"
            ).inc()
            raise ValueError(f"Transform failed: {e}") from e

        # Stage 5: Load — one account record per unique account
        institution = matched_format.institution_name if matched_format else None
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
        # Fall back to resolved_institution (the filename/format value captured at
        # Stage 1) because Stage 5 above clobbers `institution` to None for an
        # unregistered import (no matched_format). Without it the account's dim row
        # stores institution_name=NULL, and a later cross-source twin can't match it
        # on (institution, last4) — breaking the CSV-first matching direction.
        per_account_inst = (
            resolved.is_multi_account and not account_id and not account_name
        )
        account_institutions = [
            multi_acct_inst.get(aid)
            if per_account_inst
            else (institution or resolved_institution)
            for aid in unique_ids
        ]
        account_df = pl.DataFrame({
            "account_id": unique_ids,
            "account_name": [acct_id_to_name[aid] for aid in unique_ids],
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
        )

        # Record import metrics
        IMPORT_RECORDS_TOTAL.labels(source_type=source_type).inc(rows_imported)
        IMPORT_DURATION_SECONDS.labels(source_type=source_type).observe(
            time.monotonic() - _t0
        )

        result.accounts = len(unique_ids)
        result.transactions = rows_imported
        result.details = {"transactions": rows_imported, "accounts": len(unique_ids)}
        result.sign_correction_suggested = transform_result.sign_correction_suggested
        result.field_mapping = dict(resolved.field_mapping)

        if rows_imported > 0:
            result.date_range = self._query_date_range(
                "raw.tabular_transactions", "transaction_date", file_path
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
        user_ratified_via_override = bool(overrides)
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
                    institution_name=resolved_institution or "unknown",
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
                save_format_to_db(self._db, detected_fmt, actor="system")
                logger.info(f"Auto-saved format {source_origin!r} for future imports")
            except Exception:  # noqa: BLE001 — format save is best-effort; import already succeeded
                logger.debug("Could not auto-save format", exc_info=True)

        return result

    def _raise_pdf_bridge_escalation(
        self,
        canonical: Path,
        doc: "PdfDocument",
        decision: "RouteDecision",
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
        from moneybin.extractors.pdf.bridge import build_bridge_request
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
        saved_recipe = (
            {
                "name": decision.matched_format_name,
                "recipe": decision.recipe.model_dump()
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
        PDF_BRIDGE_EGRESS_TOTAL.labels(outcome="proposed").inc()
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

    def pdf_preview(self, file_path: Path) -> PdfPreviewResult:
        """Run the Phase 2a routing state machine on a PDF without importing.

        Three outcomes — same machinery as ``_import_pdf`` but no side effects
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
        - Non-bridge-eligible failure (``no_transaction_table`` / ``no_rows``
          / ``unsupported_number_format``): returns
          ``PdfPreviewResult(deterministic=False, ...)``. The bridge would
          not help on these (the document isn't transaction-shaped, or has
          no extractable content), so we surface the gap honestly rather
          than ship an empty payload.

        This is a read-mostly path: side effects are the audit row on
        escalation (Req 14) and the metric bump. No ``raw.*`` rows land.
        """
        from moneybin.extractors.pdf.extractor import PDFExtractor
        from moneybin.extractors.pdf.routing import route_pdf_import

        canonical = file_path.resolve()
        doc = PDFExtractor().extract(canonical)
        decision = route_pdf_import(doc, self._db)

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
            PDF_BRIDGE_EGRESS_TOTAL.labels(outcome="invalid").inc()
            raise
        expected_row_count = len(response.rows)

        # 2. Re-extract + re-execute the recipe ourselves. The agent's rows are
        #    the expectation; these re-executed rows are what we reconcile and
        #    load — so the persisted recipe is proven to reproduce them.
        canonical = file_path.resolve()
        try:
            doc = PDFExtractor().extract(canonical)
            decision = route_forced_recipe(doc, response.recipe)
        except Exception:
            # Mirror _import_pdf: a failed extraction/route is a failed PDF
            # import. Bump the metric (rung="bridge") before propagating so the
            # bridge path doesn't silently diverge from the deterministic one.
            PDF_IMPORT_TOTAL.labels(outcome="failed", rung="bridge").inc()
            raise
        actual_row_count = len(decision.rows)
        rows_diverged = expected_row_count != actual_row_count

        # 3. Reconciliation gate decides (Req 9). Anything other than a clean
        #    transactions route is an invalid proposal — nothing loads.
        if decision.outcome != "transactions":
            PDF_BRIDGE_EGRESS_TOTAL.labels(outcome="invalid").inc()
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

        # 4. Load + persist via the shared transactions path (rung="bridge").
        #    begin_import only here: the invalid path above writes nothing, so
        #    it needs no import_log row. The two ValueError guards inside
        #    _import_pdf_transactions (decision.recipe / decision.fp is None)
        #    fire before its own finalize_import try/except, but neither can
        #    fire here: we already gated on outcome=="transactions" above, and
        #    route_forced_recipe attaches both recipe and fp on that outcome —
        #    so begin_import's row can't be stranded in "importing".
        resolved_alias = _pdf_alias(canonical)
        result = ImportResult(file_path=str(canonical), file_type="pdf")
        import_id = import_log.begin_import(
            self._db,
            source_file=str(canonical),
            source_type="pdf",
            source_origin=resolved_alias,
            account_names=[resolved_alias],
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
            account_id_override=account_id,
            rung="bridge",
        )

        PDF_BRIDGE_EGRESS_TOTAL.labels(outcome="applied").inc()
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
        )

    def _import_pdf(
        self,
        file_path: Path,
        *,
        save_format: bool = True,
        account_id: str | None = None,
        actor_kind: "ActorKind" = "human",
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
            save_format: When False, suppresses the auto-derived recipe save
                on first contact. Mirrors the tabular ``--no-save-format`` /
                ``save_format=False`` semantics so a user/agent importing a
                one-off or sensitive statement can avoid leaving an
                ``app.pdf_formats`` row that fingerprints the layout for
                future replays.
            account_id: Optional override for the account_id the rows are
                attached to. Required when reconciliation passes via balances
                alone (no account anchor captured) but the user still wants
                the rows attached to an existing ``dim_accounts`` row —
                without this, the import falls back to the filename-derived
                alias and creates a new ``dim_accounts`` row. Mirrors the
                tabular path's ``account_id`` semantics.
            actor_kind: 'agent' when a driving agent that can fulfill a bridge
                extraction is present (MCP, agent-driven CLI) — enables bridge
                escalation. 'human'/default keeps the Phase 2a seed fallback.
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
            doc = PDFExtractor().extract(canonical)
            decision = route_pdf_import(doc, self._db)
        except Exception:
            PDF_IMPORT_TOTAL.labels(outcome="failed", rung="deterministic").inc()
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
            PDF_IMPORT_TOTAL.labels(outcome="unsupported", rung="deterministic").inc()
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
            self._raise_pdf_bridge_escalation(canonical, doc, decision)

        # Committing to a write — open the import_log row now.
        import_id = import_log.begin_import(
            self._db,
            source_file=str(canonical),
            source_type="pdf",
            source_origin=resolved_alias,
            account_names=[resolved_alias],
        )
        result.import_id = import_id

        # ------------------------------------------------------------------
        # Dispatch on routing decision
        # ------------------------------------------------------------------

        if decision.outcome == "transactions":
            return self._import_pdf_transactions(
                canonical=canonical,
                resolved_alias=resolved_alias,
                import_id=import_id,
                result=result,
                decision=decision,
                doc=doc,
                save_format=save_format,
                account_id_override=account_id,
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
            PDF_IMPORT_TOTAL.labels(outcome="failed", rung="deterministic").inc()
            raise

        import_log.finalize_import(
            self._db,
            import_id,
            status="complete",
            rows_total=extracted,
            rows_imported=inserted,
        )
        PDF_IMPORT_TOTAL.labels(outcome="seed", rung="deterministic").inc()
        PDF_SEED_ROWS_TOTAL.labels(alias=resolved_alias).inc(inserted)
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
        account_id_override: str | None = None,
        rung: Literal["deterministic", "bridge"] = "deterministic",
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

        ``account_id_override`` short-circuits the issuer-slug + masked-account
        prefix logic and uses the supplied value verbatim. Required when the
        statement contains no account anchor and the user/agent wants the rows
        attached to an existing ``dim_accounts`` row rather than the
        filename-derived alias.
        """
        from decimal import Decimal

        import polars as pl

        from moneybin.loaders import import_log
        from moneybin.metrics.registry import PDF_IMPORT_TOTAL
        from moneybin.tables import TABULAR_ACCOUNTS, TABULAR_TRANSACTIONS
        from moneybin.utils import slugify

        if decision.recipe is None:
            # Should never happen: route_pdf_import only emits outcome="transactions"
            # when a recipe was successfully derived or loaded.
            raise ValueError(
                "PDF routing returned outcome='transactions' but recipe is None"
            )

        # Account ID: prefix with issuer slug so the same masked suffix
        # (e.g. "...1234") from two different banks doesn't collide on a
        # single core.dim_accounts row. Reuse the fingerprint already
        # computed by route_pdf_import (attached to RouteDecision) instead
        # of recomputing it here.
        if decision.fp is None:
            # Defensive: route_pdf_import attaches fp on every outcome that
            # reaches this method; this branch is a guard against future
            # callers that build a RouteDecision by hand.
            raise ValueError(
                "PDF routing returned outcome='transactions' but fp is None"
            )
        fp = decision.fp
        issuer_slug = slugify(fp.get("issuer", "unknown"))
        account_id: str
        # Explicit account override takes precedence — agents/users can
        # pin a PDF whose statement omits an account anchor to an existing
        # dim_accounts row instead of accepting the filename-derived alias
        # and creating a fresh dim_accounts entry.
        if account_id_override:
            account_id = account_id_override
        else:
            # Mask the captured account identifier BEFORE slugifying it into
            # the account_id PK. The captured value may be a full unmasked
            # institution account number ("Account Number: 123456789"), and
            # storing that verbatim into raw.tabular_transactions.account_id
            # / raw.tabular_accounts.account_id leaks it through every
            # downstream surface that treats account_id as an opaque identifier.
            # `_to_account_number_mask` reduces it to a last-4 mask; slugify
            # then strips the asterisks, yielding a stable digits-only suffix
            # ("chase_6789") that is safe to flow through raw/core/app.
            masked_acct = _to_account_number_mask(decision.metadata.account_id)
            if masked_acct:
                account_id = f"{issuer_slug}_{slugify(masked_acct)}"
            else:
                # Fallback: routing requires metadata for reconciliation, but
                # guard against a future path that relaxes that constraint.
                account_id = resolved_alias

        sign_conv: str = decision.recipe.sign_convention

        def _normalize_amount(row: dict[str, Any]) -> Decimal:
            """Return canonical-signed Decimal (negative=expense, positive=income).

            Rows in decision.rows are pre-canonicalized by routing.py — keys
            are "amount" / "debit" / "credit" regardless of the original
            PDF column header text.
            """
            # Use dict.get(key, default) instead of `or Decimal("0")`:
            # Decimal("0") is falsy in Python, so the `or` idiom collapses
            # an explicit zero amount onto the same path as a missing key.
            # Numerically equivalent today but conflates two distinct cases
            # and silently masks upstream type mistakes.
            _zero = Decimal("0")
            if sign_conv == "split_debit_credit":
                return Decimal(str(row.get("credit", _zero))) - Decimal(
                    str(row.get("debit", _zero))
                )
            amount_d = Decimal(str(row.get("amount", _zero)))
            if sign_conv == "negative_is_income":
                return -amount_d
            return amount_d  # negative_is_expense already matches canonical convention

        # Per-content-key dedup counter: when two rows in the same statement
        # share (date, amt, desc, account_id) the first uses the bare content
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
        # disappears from core/reports. Includes only fields the PDF
        # already captured for routing, so a re-import of the *same*
        # statement bytes still produces the same content_key.
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
        _zero = Decimal("0")
        for idx, row in enumerate(decision.rows, start=1):
            amt = _normalize_amount(row)
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
                f"{raw_credit}|{desc}|{account_id}"
            )
            dup_idx = content_dup_counter.get(content_key, 0)
            content_dup_counter[content_key] = dup_idx + 1
            raw_hash = content_key if dup_idx == 0 else f"{content_key}|{dup_idx}"
            digest = hashlib.sha256(raw_hash.encode()).hexdigest()[:16]
            transaction_id = f"pdf_{digest}"

            rows_list.append({
                "transaction_id": transaction_id,
                "account_id": account_id,
                "transaction_date": date_val,
                "post_date": post_date_val,
                "amount": amt,
                "description": str(desc) if desc is not None else None,
                "source_file": str(canonical),
                "source_type": "pdf",
                "source_origin": resolved_alias,
                "import_id": import_id,
                "row_number": idx,
            })

        try:
            df = pl.DataFrame(rows_list)
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
            self._db.ingest_dataframe(
                TABULAR_TRANSACTIONS.full_name, df, on_conflict="ignore"
            )
            rows_inserted = len(rows_list) - rows_already_present

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
            account_df = pl.DataFrame({
                "account_id": [account_id],
                "account_name": [resolved_alias],
                "account_number": [None],
                "account_number_masked": [_to_account_number_mask(raw_account_id)],
                "account_type": [None],
                "institution_name": [str(institution) if institution else None],
                "currency": [None],
                "source_file": [str(canonical)],
                "source_type": ["pdf"],
                "source_origin": [resolved_alias],
                "import_id": [import_id],
            })
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
            PDF_IMPORT_TOTAL.labels(outcome="failed", rung=rung).inc()
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
            logger.warning(
                f"PDF import_log.update_format failed for import_id="
                f"{import_id[:8]}... — format columns left NULL",
                exc_info=True,
            )

        if decision.matched_format_name is not None:
            try:
                self._pdf_formats.record_use(decision.matched_format_name)
            except Exception:  # noqa: BLE001 — observability bump must not roll back data
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
                    source="detected",
                    actor="system",  # auto-detected: system-driven (Invariant 10)
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
                    logger.warning(
                        f"PDF bump_version failed for format {format_name!r} "
                        f"(import_id={import_id[:8]}...) — stale recipe persists",
                        exc_info=True,
                    )
            except Exception:  # noqa: BLE001 — format save is bookkeeping; data is committed
                logger.warning(
                    f"PDF save_new failed for format {format_name!r} "
                    f"(import_id={import_id[:8]}...) — recipe not persisted",
                    exc_info=True,
                )

        import_log.finalize_import(
            self._db,
            import_id,
            status="complete",
            rows_total=len(rows_list),
            rows_imported=rows_inserted,
        )
        PDF_IMPORT_TOTAL.labels(outcome="transactions", rung=rung).inc()

        result.transactions = rows_inserted
        result.accounts = 1
        result.details = {
            "transactions": rows_inserted,
            "transactions_extracted": len(rows_list),
        }
        logger.info(
            f"PDF import complete (transactions): alias={resolved_alias} "
            f"extracted={len(rows_list)} inserted={rows_inserted} "
            f"import_id={import_id[:8]}..."
        )
        return result

    def import_file(
        self,
        file_path: str | Path,
        *,
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
        actor_kind: ActorKind = "human",
        account_bindings: dict[str, str] | None = None,
        account_metadata: dict[str, dict[str, str]] | None = None,
    ) -> ImportResult:
        """Import a financial data file into DuckDB.

        Auto-detects file type by extension and runs the appropriate
        extract -> load -> transform pipeline.

        Args:
            file_path: Path to the file to import.
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
            sign: Sign convention override for tabular imports.
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
            confirm: Accept the proposed mapping without further prompting.
            actor_kind: 'human' (always surfaces) or 'agent' (may self-accept at high tier).
            account_bindings: Map of source_account_key -> canonical account_id
                (adopt) or "new" (mint standalone), ratifying the account-binding
                confirmation for tabular imports.
            account_metadata: Map of source_account_key -> settings dict captured
                for accounts minted this import (tabular).

        Returns:
            ImportResult with summary of what was imported.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the file type is not supported.
        """
        result = self._import_one(
            file_path,
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
            actor_kind=actor_kind,
            account_bindings=account_bindings,
            account_metadata=account_metadata,
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
            if not refresh_result.applied:
                raise RuntimeError(f"SQLMesh transforms failed: {refresh_result.error}")
            result.core_tables_rebuilt = True

        logger.info(f"Import complete: {result.summary()}")
        return result

    def _import_one(
        self,
        file_path: str | Path,
        *,
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
        actor_kind: ActorKind = "human",
        account_bindings: dict[str, str] | None = None,
        account_metadata: dict[str, dict[str, str]] | None = None,
    ) -> ImportResult:
        """Extract + load one file. Does NOT run the refresh pipeline.

        Refresh (matching, SQLMesh apply, categorization) is the caller's
        responsibility — see :func:`moneybin.services.refresh.refresh` and
        ``import_files``.
        """
        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        file_type = _detect_file_type(path)
        logger.info(f"Importing {_display_label(file_type, path)} file: {path}")

        if file_type == "ofx":
            return self._import_ofx(
                path, institution=institution, force=force, interactive=interactive
            )
        if file_type == "tabular":
            return self._import_tabular(
                path,
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
                actor_kind=actor_kind,
                account_bindings=account_bindings,
                account_metadata=account_metadata,
            )
        if file_type == "pdf":
            return self._import_pdf(
                path,
                save_format=save_format,
                account_id=account_id,
                actor_kind=actor_kind,
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
                # error_type only; raw str(e) may embed PII per extractors/ofx/extractor.py
                error_type = type(e).__name__
                logger.warning(f"Import failed for {path}: {error_type}")
                per_file.append(
                    PerFileResult(
                        path=str(path),
                        status="failed",
                        source_type=None,
                        error=error_type,
                    )
                )

        applied = False
        duration_seconds: float | None = None
        error: str | None = None
        if refresh and any_succeeded and any_transformable:
            refresh_result = _refresh(self._db)
            applied = refresh_result.applied
            duration_seconds = refresh_result.duration_seconds
            error = refresh_result.error

        return BatchImportResult(
            per_file=per_file,
            transforms_applied=applied,
            transforms_duration_seconds=duration_seconds,
            transforms_error=error,
        )

    def revert(self, import_id: str) -> dict[str, str | int]:
        """Revert an import batch by deleting its raw rows and flipping status.

        Looks up source_type from raw.import_log to determine which tables to
        delete from (via the ``REVERT_TABLES`` allowlist). Updates status to
        'reverted'.

        Args:
            import_id: UUID of the import batch in ``raw.import_log``.

        Returns:
            ``{'status': 'reverted', 'rows_deleted': N}`` on success.
            ``{'status': 'not_found', 'reason': ...}`` if import_id doesn't exist.
            ``{'status': 'already_reverted'}`` if already reverted.
            ``{'status': 'unsupported', 'reason': ...}`` if the source_type is
            not in the revert allowlist.
            ``{'status': 'superseded', 'reason': ...}`` if a later import for
            the same source_file already overwrote the rows.
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
            return {"status": "not_found", "reason": f"No import with ID {import_id}"}

        src_type, status, source_file, started_at, source_origin = row

        if status == "reverted":
            return {"status": "already_reverted"}

        if src_type not in REVERT_TABLES:
            return {
                "status": "unsupported",
                "reason": f"Cannot revert source_type {src_type!r}",
            }

        tables = REVERT_TABLES[src_type]

        # Sum across every table the source_type populates. OFX statements with
        # zero transactions but populated accounts/balances must still be
        # detectable as live (not superseded) and reportable in rows_deleted.
        rows_to_delete = 0
        for table in tables:
            result = self._db.execute(
                f"SELECT COUNT(*) FROM {table.full_name} WHERE import_id = ?",
                [import_id],
            ).fetchone()
            if result:
                rows_to_delete += result[0]

        if rows_to_delete == 0:
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
                return {
                    "status": "superseded",
                    "reason": (
                        f"File was re-imported as {newer_id[:8]}...; "
                        f"revert that batch to remove the data."
                    ),
                }

        self._db.begin()
        try:
            for table in tables:
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
        except Exception:
            self._db.rollback()
            raise

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
        if src_type == "pdf" and source_origin:
            other_row = self._db.execute(
                f"SELECT COUNT(*) FROM {IMPORT_LOG.full_name} "
                f"WHERE source_type = 'pdf' AND source_origin = ? "
                f"AND status = 'complete' AND import_id != ?",
                [source_origin, import_id],
            ).fetchone()
            if other_row is not None and other_row[0] == 0:
                from sqlglot import exp  # noqa: PLC0415

                safe_view = exp.to_identifier(f"pdf_{source_origin}", quoted=True).sql(
                    "duckdb"
                )
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
            f"Reverted import {import_id[:8]}...: {rows_to_delete} rows deleted"
        )
        return {"status": "reverted", "rows_deleted": rows_to_delete}

    # ------------------------------------------------------------------
    # Import labels (spec Req 22–24).
    # ------------------------------------------------------------------

    def list_labels(self, import_id: str) -> list[str]:
        """Return the labels currently attached to ``import_id`` (or empty)."""
        row = self._db.conn.execute(
            "SELECT labels FROM app.imports WHERE import_id = ?",
            [import_id],
        ).fetchone()
        if row is None or row[0] is None:
            return []
        return list(row[0])

    def list_distinct_labels(self) -> list[tuple[str, int]]:
        """Return ``(label, usage_count)`` across all import rows, sorted desc."""
        rows = self._db.conn.execute(
            """
            SELECT label, COUNT(*) AS n
              FROM (SELECT UNNEST(labels) AS label FROM app.imports)
             WHERE label IS NOT NULL
             GROUP BY label
             ORDER BY n DESC, label ASC
            """
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
