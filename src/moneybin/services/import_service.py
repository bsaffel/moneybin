"""Unified import service for financial data files.

This service handles the full import pipeline: detect file type, extract
data, load to raw tables, and run SQLMesh transforms. Both CLI commands and
MCP tools call this same service — no duplication.
"""

import dataclasses
import logging
import time
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from typing import Any, Literal, cast

import duckdb

from moneybin.database import Database, sqlmesh_context
from moneybin.extractors.tabular.formats import (
    NumberFormatType,
    SignConventionType,
)
from moneybin.metrics.registry import (
    IMPORT_DURATION_SECONDS,
    IMPORT_ERRORS_TOTAL,
    IMPORT_RECORDS_TOTAL,
    TABULAR_DETECTION_CONFIDENCE,
    TABULAR_FORMAT_MATCHES,
)

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
    w2_forms: int = 0
    date_range: str = ""
    details: dict[str, int] = field(default_factory=dict)
    core_tables_rebuilt: bool = False
    import_id: str | None = None
    """UUID of the raw.import_log row this import created. None for file types
    that don't write to import_log (currently W-2/PDF)."""

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
        if self.w2_forms:
            lines.append(f"  W-2 forms: {self.w2_forms}")
        if self.date_range:
            lines.append(f"  Date range: {self.date_range}")
        if self.core_tables_rebuilt:
            lines.append("  Core tables rebuilt (dim_accounts, fct_transactions)")

        return "\n".join(lines)


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
    user sees ``CSV`` / ``XLSX`` / ``OFX`` / ``W-2`` instead of ``TABULAR``.
    """
    if file_type == "tabular":
        return file_path.suffix.lstrip(".").upper() or "TABULAR"
    if file_type == "w2":
        return "W-2"
    return file_type.upper()


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
        File type string: 'ofx', 'w2', or 'tabular'.

    Raises:
        ValueError: If the file cannot be classified.
    """
    from moneybin.extractors.tabular.format_detector import TABULAR_EXTENSIONS

    suffix = file_path.suffix.lower()
    if suffix in (".ofx", ".qfx", ".qbo"):
        return "ofx"
    if suffix == ".pdf":
        return "w2"
    if suffix in _UNAMBIGUOUS_TABULAR:
        return "tabular"

    # Try magic-byte sniffing before falling back to remaining tabular extensions
    # (.txt, .dat) and the unknown-extension error path.
    if _sniff_ofx_content(file_path):
        return "ofx"

    if suffix in TABULAR_EXTENSIONS:
        return "tabular"

    raise ValueError(
        f"Unsupported file type: {suffix}. "
        f"Supported: .ofx, .qfx, .qbo, .csv, .tsv, .xlsx, .parquet, .feather, .pdf"
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


class ImportService:
    """Orchestrates the full file import pipeline.

    Detects file type, extracts and loads to raw tables, runs SQLMesh
    transforms, applies matching, and runs deterministic categorization.
    Both CLI commands and MCP tools call this same service — no
    duplication.
    """

    def __init__(self, db: Database) -> None:
        """Initialize ImportService with an open Database connection."""
        self._db = db

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

    def _resolve_account_via_matcher(
        self,
        *,
        account_name: str,
        account_number: str | None,
        threshold: float,
        auto_accept: bool,
    ) -> str:
        """Resolve an account name to an account_id using match_account().

        Outcomes:
          1. Matched (number or exact slug) → return the existing account_id.
          2. Fuzzy candidates and auto_accept=True → take the top candidate, log it.
          3. Fuzzy candidates and auto_accept=False → log a warning listing
             candidates, fall back to slugify(account_name).
          4. No candidates → fall back to slugify(account_name) (creates new).

        Args:
            account_name: Account name from CLI/file.
            account_number: Account number if available, for strongest match.
            threshold: Minimum SequenceMatcher.ratio for a fuzzy candidate.
            auto_accept: True if the user passed --yes (or stdin is non-interactive).

        Returns:
            The resolved account_id (existing or freshly slugified).
        """
        from moneybin.extractors.tabular.account_matching import match_account
        from moneybin.metrics.registry import ACCOUNT_MATCH_OUTCOMES_TOTAL
        from moneybin.utils import slugify

        try:
            # GROUP BY account_id collapses duplicates from the same account being
            # imported with slightly different names (e.g. "Chase Checking" vs
            # "CHASE CHECKING"); take the most-recently-seen name.
            rows = self._db.execute(
                """
                SELECT account_id,
                       LAST(account_name ORDER BY loaded_at) AS account_name,
                       LAST(account_number ORDER BY loaded_at) AS account_number
                FROM raw.tabular_accounts
                GROUP BY account_id
                """
            ).fetchall()
        except duckdb.CatalogException:  # raw.tabular_accounts absent on first import
            logger.debug("raw.tabular_accounts unavailable; skipping account match")
            ACCOUNT_MATCH_OUTCOMES_TOTAL.labels(result="table_missing").inc()
            return slugify(account_name)

        existing = [
            {"account_id": r[0], "account_name": r[1], "account_number": r[2]}
            for r in rows
        ]

        result = match_account(
            account_name,
            account_number=account_number,
            existing_accounts=existing,
            threshold=threshold,
        )

        if result.matched and result.account_id:
            logger.info(f"Matched account to existing id {result.account_id!r}")
            ACCOUNT_MATCH_OUTCOMES_TOTAL.labels(result="exact").inc()
            return result.account_id

        if result.candidates:
            if auto_accept:
                top = result.candidates[0]
                if top["account_id"]:
                    logger.info(
                        f"⚙️  Auto-accepting fuzzy match → {top['account_id']!r}"
                    )
                    ACCOUNT_MATCH_OUTCOMES_TOTAL.labels(result="fuzzy_accepted").inc()
                    return top["account_id"]
                # Fuzzy candidates exist but none have an account_id — this should
                # be rare (suggests stale/orphaned rows in raw.tabular_accounts),
                # but we must surface it instead of silently slugifying.
                logger.warning(
                    "⚠️  Auto-accept requested but top fuzzy candidate has no "
                    "account_id; falling back to slugify(account_name)."
                )
                ACCOUNT_MATCH_OUTCOMES_TOTAL.labels(result="fuzzy_no_id").inc()
            else:
                candidate_ids = ", ".join(
                    filter(None, (c["account_id"] for c in result.candidates))
                )
                logger.warning(
                    f"⚠️  Account did not match exactly. Fuzzy candidate ids: "
                    f"{candidate_ids}. "
                    "Use --yes to auto-accept the top candidate, "
                    "or --account-id to pick explicitly."
                )
                ACCOUNT_MATCH_OUTCOMES_TOTAL.labels(result="fuzzy_ambiguous").inc()
        else:
            ACCOUNT_MATCH_OUTCOMES_TOTAL.labels(result="slugify_new").inc()

        return slugify(account_name)

    def run_transforms(self) -> bool:
        """Run SQLMesh transforms to rebuild core tables.

        Uses ``sqlmesh_context()`` to handle encrypted DB injection into
        SQLMesh's adapter cache. ``sqlmesh_context()`` reuses the active
        ``get_database()`` singleton internally, so ``self._db`` should
        be that same singleton (typical caller pattern is
        ``ImportService(get_database()).run_transforms()``).

        Seeds ``app.seed_source_priority`` from config before running so
        ``int_transactions__merged`` can resolve per-field winners. Without
        this, the LEFT JOIN onto ``seed_source_priority`` produces NULL
        priorities for every row, causing ARG_MIN(value, NULL_key) to drop
        non-NULL values for fields that key on a CASE-with-NULL-fallthrough
        pattern (description, memo, etc.). Callers that go straight to
        transforms (``transform apply``, synthetic generation) would
        otherwise materialize NULL descriptions in core.fct_transactions.

        Returns:
            True if transforms ran successfully.
        """
        import time

        from moneybin.config import get_settings
        from moneybin.matching.priority import seed_source_priority
        from moneybin.metrics.registry import SQLMESH_RUN_DURATION_SECONDS
        from moneybin.seeds import refresh_views

        logger.info("Running SQLMesh transforms")

        seed_source_priority(self._db, get_settings().matching)

        t0 = time.monotonic()
        try:
            with sqlmesh_context() as ctx:
                ctx.plan(auto_apply=True, no_prompts=True)
            refresh_views(self._db)
        finally:
            elapsed = time.monotonic() - t0
            SQLMESH_RUN_DURATION_SECONDS.labels(model="import_plan_apply").observe(
                elapsed
            )
            logger.info(f"SQLMesh transforms completed in {elapsed:.2f}s")
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
        from moneybin.extractors.ofx_extractor import (
            OFXExtractor,
            preprocess_ofx_content,
        )
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

        # Best-effort account matching for OFX (passthrough today; emits metrics).
        self._match_ofx_accounts(account_ids)

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

    def _match_ofx_accounts(self, account_ids: list[str]) -> None:
        """Best-effort account matching for OFX. Emits metrics; doesn't mutate data.

        Today's behavior (carried forward): the OFX file's account_id IS the
        matching key downstream. This method exists so future improvements have
        a single place to live and so account-match metrics are emitted for
        OFX too.
        """
        from moneybin.metrics.registry import ACCOUNT_MATCH_OUTCOMES_TOTAL

        for _aid in account_ids:
            ACCOUNT_MATCH_OUTCOMES_TOTAL.labels(result="not_attempted").inc()

    def _import_w2(
        self,
        file_path: Path,
    ) -> ImportResult:
        """Import a W-2 PDF file.

        Args:
            file_path: Path to the W-2 PDF.

        Returns:
            ImportResult with summary of imported data.
        """
        from moneybin.extractors.w2_extractor import W2Extractor
        from moneybin.loaders.w2_loader import W2Loader

        result = ImportResult(file_path=str(file_path), file_type="w2")

        # Extract
        extractor = W2Extractor()
        data = extractor.extract_from_file(file_path)

        # Load
        loader = W2Loader(self._db)
        row_count = loader.load_data(data)

        result.w2_forms = row_count
        result.details = {"w2_forms": row_count}

        return result

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

        Returns:
            ImportResult with summary.
        """
        import polars as pl

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
        from moneybin.loaders.tabular_loader import TabularLoader
        from moneybin.utils import slugify

        result = ImportResult(file_path=str(file_path), file_type="tabular")
        _t0 = time.monotonic()

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
            mapping_result = map_columns(df, overrides=overrides)
            resolved = ResolvedMapping(
                field_mapping=mapping_result.field_mapping,
                date_format=mapping_result.date_format or "%Y-%m-%d",
                sign_convention=mapping_result.sign_convention,
                number_format=mapping_result.number_format,
                is_multi_account=mapping_result.is_multi_account,
                confidence=mapping_result.confidence,
            )
            format_source = "detected"

            if mapping_result.sign_needs_confirmation and not sign:
                logger.warning(
                    "⚠️  Sign convention is ambiguous (all amounts appear positive). "
                    f"Proceeding with '{resolved.sign_convention}' — "
                    "use --sign to override if expense amounts look wrong."
                )

            if mapping_result.confidence == "low":
                raise ValueError(
                    f"Could not reliably detect column mapping for "
                    f"{file_path.name}. Use --override to specify columns manually."
                )

        # Record format match and detection confidence metrics
        if matched_format:
            TABULAR_FORMAT_MATCHES.labels(
                format_name=matched_format.name, format_source=format_source
            ).inc()
        TABULAR_DETECTION_CONFIDENCE.labels(confidence=resolved.confidence).inc()

        # Apply CLI overrides — rebuild a new ResolvedMapping (frozen)
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

        # Build per-row account_ids and a name→id mapping for the accounts table
        acct_name_col = resolved.field_mapping.get("account_name")
        acct_id_to_name: dict[str, str] = {}

        if account_id:
            account_ids: str | list[str] = account_id
            acct_id_to_name[account_id] = account_name or account_id
        elif account_name:
            from moneybin.config import get_settings

            threshold = get_settings().data.tabular.account_match_threshold
            aid = self._resolve_account_via_matcher(
                account_name=account_name,
                account_number=None,  # no --account-number CLI flag yet
                threshold=threshold,
                auto_accept=auto_accept,
            )
            account_ids = aid
            acct_id_to_name[aid] = account_name
        elif (
            resolved.is_multi_account and acct_name_col and acct_name_col in df.columns
        ):
            # Per-row account assignment from the DataFrame column
            raw_names = [
                str(v) if v is not None else "unknown"
                for v in df[acct_name_col].to_list()
            ]
            account_ids = [slugify(name) for name in raw_names]
            for aid, name in zip(account_ids, raw_names, strict=True):
                if aid not in acct_id_to_name:
                    acct_id_to_name[aid] = name
        else:
            raise ValueError(
                "Single-account files require --account-name or --account-id"
            )

        source_origin = (
            matched_format.name
            if matched_format
            else slugify(account_name or "unknown")
        )

        # Create import batch
        loader = TabularLoader(self._db)
        import_id = loader.create_import_batch(
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

        tabular_cfg = get_settings().data.tabular
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
            loader.finalize_import_batch(
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
        account_df = pl.DataFrame({
            "account_id": unique_ids,
            "account_name": [acct_id_to_name[aid] for aid in unique_ids],
            "account_number": [None] * len(unique_ids),
            "account_number_masked": [None] * len(unique_ids),
            "account_type": [None] * len(unique_ids),
            "institution_name": [institution] * len(unique_ids),
            "currency": [None] * len(unique_ids),
            "source_file": [str(file_path)] * len(unique_ids),
            "source_type": [source_type] * len(unique_ids),
            "source_origin": [source_origin] * len(unique_ids),
            "import_id": [import_id] * len(unique_ids),
        })

        rows_imported = loader.load_transactions(transform_result.transactions)
        loader.load_accounts(account_df)

        loader.finalize_import_batch(
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

        if rows_imported > 0:
            result.date_range = self._query_date_range(
                "raw.tabular_transactions", "transaction_date", file_path
            )

        # Auto-save detected format for future imports
        if (
            save_format
            and not matched_format
            and resolved.confidence in ("high", "medium")
            and rows_imported > 0
        ):
            try:
                detected_fmt = TabularFormat(
                    name=source_origin,
                    institution_name=account_name or source_origin,
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
                save_format_to_db(self._db, detected_fmt)
                logger.info(f"Auto-saved format {source_origin!r} for future imports")
            except Exception:  # noqa: BLE001 — format save is best-effort; import already succeeded
                logger.debug("Could not auto-save format", exc_info=True)

        return result

    def import_file(
        self,
        file_path: str | Path,
        *,
        apply_transforms: bool = True,
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
    ) -> ImportResult:
        """Import a financial data file into DuckDB.

        Auto-detects file type by extension and runs the appropriate
        extract -> load -> transform pipeline.

        Args:
            file_path: Path to the file to import.
            apply_transforms: Whether to run SQLMesh transforms after loading.
                Defaults to True.
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

        Returns:
            ImportResult with summary of what was imported.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the file type is not supported.
        """
        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        file_type = _detect_file_type(path)
        logger.info(f"Importing {_display_label(file_type, path)} file: {path}")

        if file_type == "ofx":
            result = self._import_ofx(
                path, institution=institution, force=force, interactive=interactive
            )
        elif file_type == "w2":
            result = self._import_w2(path)
        elif file_type == "tabular":
            result = self._import_tabular(
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
            )
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

        # Run matching and SQLMesh transforms after loading raw data
        if apply_transforms and file_type in ("ofx", "tabular"):
            try:
                self._run_matching()
            except Exception:  # noqa: BLE001 — matching is best-effort; first import may precede SQLMesh views
                logger.debug(
                    "Matching skipped (views may not exist yet)", exc_info=True
                )
            result.core_tables_rebuilt = self.run_transforms()

            # Apply deterministic categorization to new transactions
            self._apply_categorization()

        logger.info(f"Import complete: {result.summary()}")
        return result

    def _run_matching(self) -> None:
        """Run transaction matching after import.

        Seeds source priority from config and runs the matcher engine.
        Results are logged; pending matches prompt user action.
        """
        from moneybin.config import get_settings
        from moneybin.matching.engine import TransactionMatcher
        from moneybin.matching.priority import seed_source_priority

        settings = get_settings().matching
        seed_source_priority(self._db, settings)
        matcher = TransactionMatcher(self._db, settings)
        result = matcher.run()

        if result.has_matches:
            logger.info(f"Matching: {result.summary()}")
            if result.has_pending:
                logger.info(
                    "Run 'moneybin transactions review --type matches' when ready"
                )

    def _apply_categorization(self) -> None:
        """Run deterministic categorization on uncategorized transactions.

        Called after SQLMesh transforms complete. Applies merchant lookups
        and active rules — no LLM dependency.
        """
        from moneybin.services.auto_rule_service import AutoRuleService
        from moneybin.services.categorization_service import CategorizationService

        try:
            service = CategorizationService(self._db)
            stats = service.apply_deterministic()
            if stats["total"] > 0:
                logger.info(
                    f"Auto-categorized {stats['total']} transactions "
                    f"({stats['merchant']} merchant, {stats['rule']} rule)"
                )
            pending = AutoRuleService(self._db).stats().pending_proposals
            if pending:
                logger.info(f"  {pending} new auto-rule proposals")
                logger.info(
                    "  💡 Run 'moneybin transactions categorize auto review' to review proposed rules"
                )
        except Exception:  # noqa: BLE001 — categorization is best-effort; failure skips without aborting import
            logger.debug(
                "Categorization skipped (tables may not exist yet)",
                exc_info=True,
            )
