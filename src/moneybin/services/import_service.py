"""Unified import service for financial data files.

This service handles the full import pipeline: detect file type, extract
data, load to raw tables, and run core transforms. Both CLI commands and
MCP tools call this same service — no duplication.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

_TRANSFORM_DIR = Path(__file__).resolve().parents[1] / "sql" / "transforms"


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
    raise ValueError(
        f"Unsupported file type: {suffix}. "
        f"Supported: .ofx, .qfx (bank statements), .pdf (W-2 forms)"
    )


def _run_core_transforms(conn: duckdb.DuckDBPyConnection) -> bool:
    """Run inline SQL transforms to rebuild core tables.

    Args:
        conn: Active DuckDB connection.

    Returns:
        True if transforms ran successfully.
    """
    transform_files = [
        "core_dim_accounts.sql",
        "core_fct_transactions.sql",
    ]

    for sql_file in transform_files:
        sql_path = _TRANSFORM_DIR / sql_file
        if not sql_path.exists():
            logger.warning("Transform file not found: %s", sql_path)
            return False

        logger.info("Running transform: %s", sql_file)
        conn.execute(sql_path.read_text())

    return True


def _import_ofx(conn: duckdb.DuckDBPyConnection, file_path: Path) -> ImportResult:
    """Import an OFX/QFX file.

    Args:
        conn: Active DuckDB connection.
        file_path: Path to the OFX/QFX file.

    Returns:
        ImportResult with summary of imported data.
    """
    from moneybin.extractors.ofx_extractor import OFXExtractor
    from moneybin.loaders.ofx_loader import OFXLoader

    result = ImportResult(file_path=str(file_path), file_type="ofx")

    # Extract
    extractor = OFXExtractor()
    data = extractor.extract_from_file(file_path)

    # Load using OFXLoader (which manages its own connection)
    db_path = Path(conn.execute("PRAGMA database_list").fetchone()[2])  # type: ignore[index]
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
            date_result = conn.execute("""
                SELECT
                    MIN(CAST(date_posted AS DATE)) AS min_date,
                    MAX(CAST(date_posted AS DATE)) AS max_date
                FROM raw.ofx_transactions
            """).fetchone()
            if date_result and date_result[0]:
                result.date_range = f"{date_result[0]} to {date_result[1]}"
        except Exception:
            logger.debug("Could not determine date range from transactions")

    return result


def _import_w2(conn: duckdb.DuckDBPyConnection, file_path: Path) -> ImportResult:
    """Import a W-2 PDF file.

    Args:
        conn: Active DuckDB connection.
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
    db_path = Path(conn.execute("PRAGMA database_list").fetchone()[2])  # type: ignore[index]
    loader = W2Loader(db_path)
    row_count = loader.load_data(data)

    result.w2_forms = row_count
    result.details = {"w2_forms": row_count}

    return result


def import_file(
    conn: duckdb.DuckDBPyConnection,
    file_path: str | Path,
) -> ImportResult:
    """Import a financial data file into DuckDB.

    Auto-detects file type by extension and runs the appropriate
    extract → load → transform pipeline.

    Args:
        conn: Active DuckDB connection.
        file_path: Path to the file to import.

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
        result = _import_ofx(conn, path)
    elif file_type == "w2":
        result = _import_w2(conn, path)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    # Run core transforms after loading raw data
    if file_type == "ofx":
        result.core_tables_rebuilt = _run_core_transforms(conn)

    logger.info("Import complete: %s", result.summary())
    return result
