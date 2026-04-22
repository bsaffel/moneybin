"""Unified import service for financial data files.

This service handles the full import pipeline: detect file type, extract
data, load to raw tables, and run SQLMesh transforms. Both CLI commands and
MCP tools call this same service — no duplication.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path

from moneybin.database import Database

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


def _query_date_range(
    db: Database,
    table: str,
    date_column: str,
    file_path: Path,
) -> str:
    """Query min/max date range for a source file from a raw table.

    Args:
        db: Database instance.
        table: Qualified table name (e.g. ``raw.ofx_transactions``).
        date_column: Column containing the date value.
        file_path: Source file path to filter on.

    Returns:
        Date range string like ``"2024-01-01 to 2024-03-31"``, or empty
        string if unavailable.
    """
    try:
        result = db.execute(
            f"""
            SELECT MIN({date_column}) AS min_date,
                   MAX({date_column}) AS max_date
            FROM {table}
            WHERE source_file = ?
            """,
            [str(file_path)],
        ).fetchone()
        if result and result[0]:
            return f"{result[0]} to {result[1]}"
    except Exception:
        logger.debug(f"Could not determine date range from {table}", exc_info=True)
    return ""


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

    Because the database is encrypted with AES-256-GCM (ATTACH ...
    ENCRYPTION_KEY), and SQLMesh's DuckDB gateway config does not support
    encryption_key, we create a properly-connected DuckDB adapter and
    pre-populate SQLMesh's adapter cache so it reuses our connection
    instead of creating one that can't decrypt the file.

    Args:
        db_path: Path to the DuckDB database file.

    Returns:
        True if transforms ran successfully.
    """
    import duckdb as duckdb_mod
    from sqlmesh.core.config import Config, GatewayConfig
    from sqlmesh.core.config.connection import (
        BaseDuckDBConnectionConfig,
        DuckDBConnectionConfig,
    )
    from sqlmesh.core.engine_adapter.duckdb import DuckDBEngineAdapter

    from moneybin.database import build_attach_sql
    from moneybin.secrets import SecretStore
    from sqlmesh import (
        Context,  # type: ignore[import-untyped] — sqlmesh has no type stubs
    )

    logger.info("Running SQLMesh transforms")

    store = SecretStore()
    encryption_key = store.get_key("DATABASE__ENCRYPTION_KEY")

    conn = duckdb_mod.connect()
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(build_attach_sql(db_path, encryption_key))
    conn.execute("USE moneybin")

    # Pre-populate SQLMesh's adapter cache (keyed by database path).
    # BaseDuckDBConnectionConfig.create_engine_adapter checks this cache
    # before creating a new adapter, so SQLMesh will reuse our encrypted
    # connection rather than opening its own unencrypted one.
    adapter = DuckDBEngineAdapter(
        lambda: conn,
        default_catalog="moneybin",
        register_comments=True,
    )
    cache_key = str(db_path)
    BaseDuckDBConnectionConfig._data_file_to_adapter[cache_key] = adapter  # type: ignore[reportPrivateUsage]  # no public API for encrypted DB injection

    try:
        config = Config(
            default_gateway="moneybin",
            gateways={
                "moneybin": GatewayConfig(
                    connection=DuckDBConnectionConfig(database=str(db_path)),
                ),
            },
        )
        ctx = Context(
            paths=str(_SQLMESH_ROOT),
            config=config,
            gateway="moneybin",
        )
        ctx.plan(auto_apply=True, no_prompts=True)
        logger.info("SQLMesh transforms completed")
        return True
    finally:
        BaseDuckDBConnectionConfig._data_file_to_adapter.pop(cache_key, None)  # type: ignore[reportPrivateUsage]  # cleanup matches injection above
        conn.close()


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


def _import_csv(
    db: Database,
    file_path: Path,
    *,
    account_id: str | None = None,
    institution: str | None = None,
) -> ImportResult:
    """Import a CSV file.

    Args:
        db: Database instance.
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
    loader = CSVLoader(db)
    row_counts = loader.load_data(data)

    result.accounts = row_counts.get("accounts", 0)
    result.transactions = row_counts.get("transactions", 0)
    result.details = row_counts

    if result.transactions > 0:
        result.date_range = _query_date_range(
            db, "raw.csv_transactions", "transaction_date", file_path
        )

    return result


def import_file(
    db: Database,
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
        db: Database instance.
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
    logger.info(f"Importing {file_type} file: {path}")

    if file_type == "ofx":
        result = _import_ofx(db, path, institution=institution)
    elif file_type == "w2":
        result = _import_w2(db, path)
    elif file_type == "csv":
        result = _import_csv(db, path, account_id=account_id, institution=institution)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    # Run SQLMesh transforms after loading raw data
    if run_transforms and file_type in ("ofx", "csv"):
        result.core_tables_rebuilt = _run_transforms(db.path)

        # Apply deterministic categorization to new transactions
        _apply_categorization(db)

    logger.info(f"Import complete: {result.summary()}")
    return result


def _apply_categorization(db: Database) -> None:
    """Run deterministic categorization on uncategorized transactions.

    Called after SQLMesh transforms complete. Applies merchant lookups
    and active rules — no LLM dependency.

    Args:
        db: Database instance.
    """
    from moneybin.services.categorization_service import (
        apply_deterministic_categorization,
    )

    try:
        stats = apply_deterministic_categorization(db)
        if stats["total"] > 0:
            logger.info(
                "Auto-categorized %d transactions (%d merchant, %d rule)",
                stats["total"],
                stats["merchant"],
                stats["rule"],
            )
    except Exception:
        logger.debug(
            "Categorization skipped (tables may not exist yet)",
            exc_info=True,
        )
