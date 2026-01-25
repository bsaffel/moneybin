"""File utilities for MoneyBin.

Handles copying source files into the data/raw directory structure.
"""

import hashlib
import logging
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)


def copy_to_raw(
    source_file: Path | str,
    file_type: str,
    base_data_path: Path | str = Path("data/raw"),
) -> Path:
    """Copy a file to the raw data directory (idempotent).

    Args:
        source_file: Path to the source file to copy
        file_type: File type for directory organization (e.g., 'ofx', 'csv', 'pdf')
        base_data_path: Base path for raw data storage

    Returns:
        Path: Path to the copied file in the raw data directory

    Raises:
        FileNotFoundError: If source file doesn't exist

    Examples:
        >>> copy_to_raw("~/Downloads/bank.qfx", "ofx")
        Path('data/raw/ofx/bank.qfx')

        >>> copy_to_raw("statement.pdf", "pdf", base_data_path="data/raw")
        Path('data/raw/pdf/statement.pdf')
    """
    source_path = Path(source_file).expanduser().resolve()
    base_path = Path(base_data_path)

    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    # Normalize file type and determine target directory
    # OFX and QFX files go to the same directory
    normalized_type = file_type.lower()
    if normalized_type in ("qfx", "ofx"):
        target_dir = base_path / "ofx"
    else:
        target_dir = base_path / normalized_type

    target_dir.mkdir(parents=True, exist_ok=True)

    # Use original filename (preserve name)
    target_path = target_dir / source_path.name

    # Idempotent behavior: check if file already exists
    if target_path.exists():
        if _files_are_identical(source_path, target_path):
            logger.info(f"File already exists with identical content: {target_path}")
            return target_path
        else:
            logger.info(f"Overwriting existing file with new content: {target_path}")

    # Copy the file
    shutil.copy2(source_path, target_path)
    logger.info(f"Copied {source_path} to {target_path}")

    return target_path


def _files_are_identical(file1: Path, file2: Path) -> bool:
    """Check if two files have identical content.

    Uses file size for quick check, then SHA-256 hash for verification.

    Args:
        file1: First file path
        file2: Second file path

    Returns:
        bool: True if files have identical content
    """
    # Quick check: compare file sizes first
    if file1.stat().st_size != file2.stat().st_size:
        return False

    # Compare SHA-256 hashes
    hash1 = hashlib.sha256(file1.read_bytes()).hexdigest()
    hash2 = hashlib.sha256(file2.read_bytes()).hexdigest()

    return hash1 == hash2
