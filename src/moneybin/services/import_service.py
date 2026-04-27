"""Unified import service for financial data files.

This service handles the full import pipeline: detect file type, extract
data, load to raw tables, and run SQLMesh transforms. Both CLI commands and
MCP tools call this same service — no duplication.
"""

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

from moneybin.database import Database, sqlmesh_context
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


def _query_date_range(
    db: Database,
    table: str,
    date_expr: str,
    file_path: Path,
) -> str:
    """Query min/max date range for a source file from a raw table.

    Both ``table`` and ``date_expr`` are interpolated into SQL — callers
    must only pass hardcoded trusted strings, never user input.

    Args:
        db: Database instance.
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
        result = db.execute(
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


def _detect_file_type(file_path: Path) -> str:
    """Detect file type from extension.

    Args:
        file_path: Path to the file.

    Returns:
        File type string: 'ofx', 'w2', or 'tabular'.

    Raises:
        ValueError: If extension is not recognized.
    """
    from moneybin.extractors.tabular.format_detector import TABULAR_EXTENSIONS

    suffix = file_path.suffix.lower()
    if suffix in (".ofx", ".qfx"):
        return "ofx"
    if suffix == ".pdf":
        return "w2"
    if suffix in TABULAR_EXTENSIONS:
        return "tabular"
    raise ValueError(
        f"Unsupported file type: {suffix}. "
        f"Supported: .ofx, .qfx, .csv, .tsv, .xlsx, .parquet, .feather, .pdf"
    )


def run_transforms() -> bool:
    """Run SQLMesh transforms to rebuild core tables.

    Uses ``sqlmesh_context()`` to handle encrypted DB injection into
    SQLMesh's adapter cache.  Requires an active Database singleton
    (via ``get_database()``) — ``sqlmesh_context()`` reuses its
    connection.

    Returns:
        True if transforms ran successfully.
    """
    logger.info("Running SQLMesh transforms")

    with sqlmesh_context() as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    logger.info("SQLMesh transforms completed")
    return True


def _import_ofx(
    db: Database,
    file_path: Path,
    *,
    institution: str | None = None,
) -> ImportResult:
    """Import an OFX/QFX file.

    Args:
        db: Database instance.
        file_path: Path to the OFX/QFX file.
        institution: Optional institution name override.

    Returns:
        ImportResult with summary of imported data.
    """
    from moneybin.extractors.ofx_extractor import OFXExtractor
    from moneybin.loaders.ofx_loader import OFXLoader

    result = ImportResult(file_path=str(file_path), file_type="ofx")

    # Extract
    extractor = OFXExtractor()
    data = extractor.extract_from_file(file_path, institution)

    # Load using OFXLoader (which manages its own connection)
    loader = OFXLoader(db)
    row_counts = loader.load_data(data)

    result.institutions = row_counts.get("institutions", 0)
    result.accounts = row_counts.get("accounts", 0)
    result.transactions = row_counts.get("transactions", 0)
    result.balances = row_counts.get("balances", 0)
    result.details = row_counts

    if result.transactions > 0:
        result.date_range = _query_date_range(
            db, "raw.ofx_transactions", "CAST(date_posted AS DATE)", file_path
        )

    return result


def _import_w2(
    db: Database,
    file_path: Path,
) -> ImportResult:
    """Import a W-2 PDF file.

    Args:
        db: Database instance.
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
    loader = W2Loader(db)
    row_count = loader.load_data(data)

    result.w2_forms = row_count
    result.details = {"w2_forms": row_count}

    return result


def _import_tabular(
    db: Database,
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
) -> ImportResult:
    """Import a tabular file through the five-stage pipeline.

    Args:
        db: Database instance.
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
    all_formats = merge_formats(builtin_formats, load_formats_from_db(db))

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
        mapping_result_mapping = matched_format.field_mapping
        mapping_result_date_format = matched_format.date_format
        mapping_result_sign_convention = matched_format.sign_convention
        mapping_result_number_format = matched_format.number_format
        mapping_result_is_multi_account = matched_format.multi_account
        mapping_result_confidence = "high"
        format_source = (
            "built-in" if matched_format.name in builtin_formats else "saved"
        )
    else:
        mapping_result = map_columns(df, overrides=overrides)
        mapping_result_mapping = mapping_result.field_mapping
        mapping_result_date_format = mapping_result.date_format or "%Y-%m-%d"
        mapping_result_sign_convention = mapping_result.sign_convention
        mapping_result_number_format = mapping_result.number_format
        mapping_result_is_multi_account = mapping_result.is_multi_account
        mapping_result_confidence = mapping_result.confidence
        format_source = "detected"

        if mapping_result.sign_needs_confirmation and not sign:
            logger.warning(
                "⚠️  Sign convention is ambiguous (all amounts appear positive). "
                f"Proceeding with '{mapping_result_sign_convention}' — "
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
    TABULAR_DETECTION_CONFIDENCE.labels(confidence=mapping_result_confidence).inc()

    # Apply CLI overrides (take precedence over detected/built-in values)
    if sign:
        mapping_result_sign_convention = sign
    if date_format_override:
        mapping_result_date_format = date_format_override
    if number_format_override:
        mapping_result_number_format = number_format_override

    # Determine account info
    source_type = format_info.file_type
    if source_type in ("semicolon", "pipe"):
        source_type = "csv"

    # Build per-row account_ids and a name→id mapping for the accounts table
    acct_name_col = mapping_result_mapping.get("account_name")
    acct_id_to_name: dict[str, str] = {}

    if account_id:
        account_ids: str | list[str] = account_id
        acct_id_to_name[account_id] = account_name or account_id
    elif account_name:
        aid = slugify(account_name)
        account_ids = aid
        acct_id_to_name[aid] = account_name
    elif (
        mapping_result_is_multi_account
        and acct_name_col
        and acct_name_col in df.columns
    ):
        # Per-row account assignment from the DataFrame column
        raw_names = [
            str(v) if v is not None else "unknown" for v in df[acct_name_col].to_list()
        ]
        account_ids = [slugify(name) for name in raw_names]
        for aid, name in zip(account_ids, raw_names, strict=True):
            if aid not in acct_id_to_name:
                acct_id_to_name[aid] = name
    else:
        raise ValueError("Single-account files require --account-name or --account-id")

    source_origin = (
        matched_format.name if matched_format else slugify(account_name or "unknown")
    )

    # Create import batch
    loader = TabularLoader(db)
    import_id = loader.create_import_batch(
        source_file=str(file_path),
        source_type=source_type,
        source_origin=source_origin,
        account_names=sorted(acct_id_to_name.values()),
        format_name=matched_format.name if matched_format else None,
        format_source=format_source,
    )

    # Stage 4: Transform
    try:
        transform_result = transform_dataframe(
            df=df,
            field_mapping=mapping_result_mapping,
            date_format=mapping_result_date_format,
            sign_convention=mapping_result_sign_convention,
            number_format=mapping_result_number_format,
            account_id=account_ids,
            source_file=str(file_path),
            source_type=source_type,
            source_origin=source_origin,
            import_id=import_id,
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
        detection_confidence=mapping_result_confidence,
        number_format=mapping_result_number_format,
        date_format=mapping_result_date_format,
        sign_convention=mapping_result_sign_convention,
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
        result.date_range = _query_date_range(
            db, "raw.tabular_transactions", "transaction_date", file_path
        )

    # Auto-save detected format for future imports
    if (
        save_format
        and not matched_format
        and mapping_result_confidence in ("high", "medium")
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
                field_mapping=mapping_result_mapping,
                sign_convention=mapping_result_sign_convention,  # type: ignore[reportArgumentType]  # validated by CLI and Pydantic validator
                date_format=mapping_result_date_format,
                number_format=mapping_result_number_format,  # type: ignore[reportArgumentType]  # validated by CLI and Pydantic validator
                multi_account=mapping_result_is_multi_account,
                source="detected",
                times_used=1,
            )
            save_format_to_db(db, detected_fmt)
            logger.info(f"Auto-saved format {source_origin!r} for future imports")
        except Exception:  # noqa: BLE001 — format save is best-effort; import already succeeded
            logger.debug("Could not auto-save format", exc_info=True)

    return result


def import_file(
    db: Database,
    file_path: str | Path,
    *,
    apply_transforms: bool = True,
    institution: str | None = None,
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
) -> ImportResult:
    """Import a financial data file into DuckDB.

    Auto-detects file type by extension and runs the appropriate
    extract -> load -> transform pipeline.

    Args:
        db: Database instance.
        file_path: Path to the file to import.
        apply_transforms: Whether to run SQLMesh transforms after loading.
            Defaults to True.
        institution: Institution name (OFX only). Auto-detected for OFX if
            omitted.
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
        result = _import_ofx(db, path, institution=institution)
    elif file_type == "w2":
        result = _import_w2(db, path)
    elif file_type == "tabular":
        result = _import_tabular(
            db,
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
        )
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    # Run matching and SQLMesh transforms after loading raw data
    if apply_transforms and file_type in ("ofx", "tabular"):
        try:
            _run_matching(db)
        except Exception:  # noqa: BLE001 — matching is best-effort; first import may precede SQLMesh views
            logger.debug("Matching skipped (views may not exist yet)", exc_info=True)
        result.core_tables_rebuilt = run_transforms()

        # Apply deterministic categorization to new transactions
        _apply_categorization(db)

    logger.info(f"Import complete: {result.summary()}")
    return result


def _run_matching(db: Database) -> None:
    """Run transaction matching after import.

    Seeds source priority from config and runs the matcher engine.
    Results are logged; pending matches prompt user action.
    """
    from moneybin.config import get_settings
    from moneybin.matching.engine import TransactionMatcher
    from moneybin.matching.priority import seed_source_priority

    settings = get_settings().matching
    seed_source_priority(db, settings)
    matcher = TransactionMatcher(db, settings)
    result = matcher.run()

    if result.has_matches:
        logger.info(f"Matching: {result.summary()}")
        if result.has_pending:
            logger.info("Run 'moneybin matches review' when ready")


def _apply_categorization(db: Database) -> None:
    """Run deterministic categorization on uncategorized transactions.

    Called after SQLMesh transforms complete. Applies merchant lookups
    and active rules — no LLM dependency.

    Args:
        db: Database instance.
    """
    from moneybin.services.categorization_service import CategorizationService

    try:
        service = CategorizationService(db)
        stats = service.apply_deterministic()
        if stats["total"] > 0:
            logger.info(
                f"Auto-categorized {stats['total']} transactions "
                f"({stats['merchant']} merchant, {stats['rule']} rule)"
            )
        from moneybin.tables import PROPOSED_RULES

        pending_row = db.execute(
            f"SELECT COUNT(*) FROM {PROPOSED_RULES.full_name} WHERE status = 'pending'"
        ).fetchone()
        pending = int(pending_row[0]) if pending_row else 0
        if pending:
            logger.info(f"  {pending} new auto-rule proposals")
            logger.info(
                "  💡 Run 'moneybin categorize auto-review' to review proposed rules"
            )
    except Exception:  # noqa: BLE001 — categorization is best-effort; failure skips without aborting import
        logger.debug(
            "Categorization skipped (tables may not exist yet)",
            exc_info=True,
        )
