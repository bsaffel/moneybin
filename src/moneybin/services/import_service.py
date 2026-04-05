"""Unified import service for financial data files.

This service handles the full import pipeline: detect file type, extract
data, load to raw tables, and run SQLMesh transforms. Both CLI commands and
MCP tools call this same service — no duplication.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

_SQLMESH_ROOT = Path(__file__).resolve().parents[3] / "sqlmesh"


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
        lines = [f"Imported {self.file_type.upper()} file: {self.file_path}"]

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


def _detect_file_type(file_path: Path) -> str:
    """Detect file type from extension.

    Args:
        file_path: Path to the file.

    Returns:
        File type string: 'ofx', 'w2', or raises ValueError.
    """
    suffix = file_path.suffix.lower()
    if suffix in (".ofx", ".qfx"):
        return "ofx"
    if suffix == ".pdf":
        return "w2"
    if suffix == ".csv":
        return "csv"
    raise ValueError(
        f"Unsupported file type: {suffix}. "
        f"Supported: .ofx, .qfx (bank statements), .pdf (W-2 forms), "
        f".csv (bank transaction exports)"
    )


def _run_transforms(db_path: Path) -> bool:
    """Run SQLMesh transforms to rebuild core tables.

    SQLMesh manages its own connection — the caller must close any
    existing connections before calling this.

    Args:
        db_path: Path to the DuckDB database file.

    Returns:
        True if transforms ran successfully.
    """
    from sqlmesh import Context  # type: ignore[import-untyped]

    logger.info("Running SQLMesh transforms")
    ctx = Context(
        paths=str(_SQLMESH_ROOT),
        gateway={  # type: ignore[reportArgumentType] — SQLMesh accepts dict at runtime
            "connection": {"type": "duckdb", "database": str(db_path)},
        },
    )
    ctx.plan(auto_apply=True, no_prompts=True)
    logger.info("SQLMesh transforms completed")
    return True


def _import_ofx(
    db_path: Path,
    file_path: Path,
    *,
    institution: str | None = None,
) -> ImportResult:
    """Import an OFX/QFX file.

    Args:
        db_path: Path to the DuckDB database file.
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
    loader = OFXLoader(db_path)
    row_counts = loader.load_data(data)

    result.institutions = row_counts.get("institutions", 0)
    result.accounts = row_counts.get("accounts", 0)
    result.transactions = row_counts.get("transactions", 0)
    result.balances = row_counts.get("balances", 0)
    result.details = row_counts

    # Get date range from transactions
    if result.transactions > 0:
        try:
            conn = duckdb.connect(str(db_path), read_only=True)
            try:
                date_result = conn.execute("""
                    SELECT
                        MIN(CAST(date_posted AS DATE)) AS min_date,
                        MAX(CAST(date_posted AS DATE)) AS max_date
                    FROM raw.ofx_transactions
                """).fetchone()
                if date_result and date_result[0]:
                    result.date_range = f"{date_result[0]} to {date_result[1]}"
            finally:
                conn.close()
        except Exception:
            logger.debug("Could not determine date range from transactions")

    return result


def _import_w2(
    db_path: Path,
    file_path: Path,
) -> ImportResult:
    """Import a W-2 PDF file.

    Args:
        db_path: Path to the DuckDB database file.
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
    loader = W2Loader(db_path)
    row_count = loader.load_data(data)

    result.w2_forms = row_count
    result.details = {"w2_forms": row_count}

    return result


def _import_csv(
    db_path: Path,
    file_path: Path,
    *,
    account_id: str | None = None,
    institution: str | None = None,
) -> ImportResult:
    """Import a CSV file.

    Args:
        db_path: Path to the DuckDB database file.
        file_path: Path to the CSV file.
        account_id: Account identifier (required for CSV).
        institution: Optional profile name to use instead of auto-detection.

    Returns:
        ImportResult with summary of imported data.
    """
    from moneybin.config import get_raw_data_path
    from moneybin.extractors.csv_extractor import CSVExtractor
    from moneybin.extractors.csv_profiles import load_profiles
    from moneybin.loaders.csv_loader import CSVLoader

    result = ImportResult(file_path=str(file_path), file_type="csv")

    user_profiles_dir = get_raw_data_path().parent / "csv_profiles"

    # Resolve profile if institution name provided
    profile = None
    if institution:
        profiles = load_profiles(user_profiles_dir)
        if institution not in profiles:
            available = ", ".join(sorted(profiles.keys())) or "(none)"
            raise ValueError(
                f"Unknown institution profile: '{institution}'. Available: {available}"
            )
        profile = profiles[institution]

    # Extract
    extractor = CSVExtractor()
    data = extractor.extract_from_file(
        file_path,
        profile=profile,
        account_id=account_id,
        user_profiles_dir=user_profiles_dir,
    )

    # Load
    loader = CSVLoader(db_path)
    row_counts = loader.load_data(data)

    result.accounts = row_counts.get("accounts", 0)
    result.transactions = row_counts.get("transactions", 0)
    result.details = row_counts

    # Get date range from transactions
    if result.transactions > 0:
        try:
            conn = duckdb.connect(str(db_path), read_only=True)
            try:
                date_result = conn.execute("""
                    SELECT
                        MIN(transaction_date) AS min_date,
                        MAX(transaction_date) AS max_date
                    FROM raw.csv_transactions
                """).fetchone()
                if date_result and date_result[0]:
                    result.date_range = f"{date_result[0]} to {date_result[1]}"
            finally:
                conn.close()
        except Exception:
            logger.debug("Could not determine date range from CSV transactions")

    return result


def import_file(
    db_path: Path,
    file_path: str | Path,
    *,
    run_transforms: bool = True,
    institution: str | None = None,
    account_id: str | None = None,
) -> ImportResult:
    """Import a financial data file into DuckDB.

    Auto-detects file type by extension and runs the appropriate
    extract -> load -> transform pipeline.

    Args:
        db_path: Path to the DuckDB database file.
        file_path: Path to the file to import.
        run_transforms: Whether to run SQLMesh transforms after loading.
            Defaults to True.
        institution: Institution name (OFX) or CSV profile name. Auto-detected
            for CSV if omitted.
        account_id: Account identifier (CSV only, required).

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
    logger.info("Importing %s file: %s", file_type, path)

    if file_type == "ofx":
        result = _import_ofx(db_path, path, institution=institution)
    elif file_type == "w2":
        result = _import_w2(db_path, path)
    elif file_type == "csv":
        result = _import_csv(
            db_path, path, account_id=account_id, institution=institution
        )
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    # Run SQLMesh transforms after loading raw data
    if run_transforms and file_type in ("ofx", "csv"):
        result.core_tables_rebuilt = _run_transforms(db_path)

        # Apply deterministic categorization to new transactions
        _apply_categorization(db_path)

    logger.info("Import complete: %s", result.summary())
    return result


def _apply_categorization(db_path: Path) -> None:
    """Run deterministic categorization on uncategorized transactions.

    Called after SQLMesh transforms complete. Applies merchant lookups
    and active rules — no LLM dependency.

    Args:
        db_path: Path to the DuckDB database file.
    """
    from moneybin.services.categorization_service import (
        apply_deterministic_categorization,
    )

    try:
        conn = duckdb.connect(str(db_path), read_only=False)
        try:
            stats = apply_deterministic_categorization(conn)
            if stats["total"] > 0:
                logger.info(
                    "Auto-categorized %d transactions (%d merchant, %d rule)",
                    stats["total"],
                    stats["merchant"],
                    stats["rule"],
                )
        finally:
            conn.close()
    except Exception:
        logger.debug(
            "Categorization skipped (tables may not exist yet)",
            exc_info=True,
        )
