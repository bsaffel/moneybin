"""Stage 1: File format detection.

Determines file type, delimiter, encoding, and enforces size guardrails
before any data is read.
"""

import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

# Extension → file_type mapping
_EXTENSION_MAP: dict[str, str] = {
    ".csv": "csv",
    ".tsv": "tsv",
    ".tab": "tsv",
    ".xlsx": "excel",
    ".xls": "excel",
    ".parquet": "parquet",
    ".pq": "parquet",
    ".feather": "feather",
    ".arrow": "feather",
    ".ipc": "feather",
}

# Magic bytes for binary format confirmation
_MAGIC_BYTES: dict[str, tuple[bytes, ...]] = {
    "parquet": (b"PAR1",),
    "excel": (b"PK\x03\x04",),
    "feather": (b"ARROW1",),
}

# File types that are text-based
_TEXT_TYPES: frozenset[str] = frozenset({"csv", "tsv", "pipe", "semicolon"})

# Delimiter → file_type mapping
_DELIMITER_TYPE: dict[str, str] = {
    ",": "csv",
    "\t": "tsv",
    "|": "pipe",
    ";": "semicolon",
}

_TEXT_SIZE_LIMIT = 25 * 1024 * 1024
_BINARY_SIZE_LIMIT = 100 * 1024 * 1024


@dataclass(frozen=True)
class FormatInfo:
    """Result of format detection (Stage 1 output)."""

    file_type: str
    delimiter: str | None = None
    encoding: str = "utf-8"
    file_size: int = 0


def detect_format(
    path: Path,
    *,
    format_override: str | None = None,
    delimiter_override: str | None = None,
    encoding_override: str | None = None,
    no_size_limit: bool = False,
) -> FormatInfo:
    """Detect the file format and basic parameters.

    Args:
        path: Path to the file to detect.
        format_override: Explicit file type (skips detection).
        delimiter_override: Explicit delimiter (text formats only).
        encoding_override: Explicit encoding (text formats only).
        no_size_limit: If True, skip file size checks.

    Returns:
        FormatInfo with detected parameters.

    Raises:
        ValueError: If file type is unsupported or size limit exceeded.
        FileNotFoundError: If the file does not exist.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    file_size = path.stat().st_size

    if format_override:
        file_type = format_override
    else:
        file_type = _detect_type_from_extension(path)

    if file_type in _MAGIC_BYTES and file_size >= 4:
        _verify_magic_bytes(path, file_type)

    if not no_size_limit:
        _check_size_limit(path, file_type, file_size)

    if file_type in _TEXT_TYPES or file_type in ("csv", "tsv", "pipe", "semicolon"):
        encoding = encoding_override or detect_encoding(path)
        if delimiter_override:
            delimiter = delimiter_override
            file_type = _DELIMITER_TYPE.get(delimiter, "csv")
        elif file_type == "csv":
            sample_lines = _read_sample_lines(path, encoding, n=20)
            delimiter = detect_delimiter(sample_lines)
            file_type = _DELIMITER_TYPE.get(delimiter, "csv")
        elif file_type == "tsv":
            delimiter = "\t"
        else:
            sample_lines = _read_sample_lines(path, encoding, n=20)
            delimiter = detect_delimiter(sample_lines)
            file_type = _DELIMITER_TYPE.get(delimiter, "csv")

        return FormatInfo(
            file_type=file_type,
            delimiter=delimiter,
            encoding=encoding,
            file_size=file_size,
        )

    return FormatInfo(file_type=file_type, file_size=file_size)


def _detect_type_from_extension(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in _EXTENSION_MAP:
        return _EXTENSION_MAP[suffix]
    if suffix in (".txt", ".dat"):
        return "csv"
    raise ValueError(
        f"Unsupported file type: '{suffix}'. "
        f"Supported: .csv, .tsv, .tab, .txt, .dat, .xlsx, .parquet, .pq, "
        f".feather, .arrow, .ipc"
    )


def _verify_magic_bytes(path: Path, expected_type: str) -> None:
    with open(path, "rb") as f:
        header = f.read(8)
    for magic in _MAGIC_BYTES.get(expected_type, ()):
        if header.startswith(magic):
            return
    logger.debug(
        f"Magic bytes for {path.name} don't match expected type "
        f"'{expected_type}' — proceeding with extension-based detection"
    )


def _check_size_limit(path: Path, file_type: str, file_size: int) -> None:
    is_binary = file_type in ("excel", "parquet", "feather")
    limit = _BINARY_SIZE_LIMIT if is_binary else _TEXT_SIZE_LIMIT
    limit_mb = limit // (1024 * 1024)

    if file_size > limit:
        size_mb = file_size / (1024 * 1024)
        raise ValueError(
            f"File {path.name} is {size_mb:.1f} MB, exceeding the "
            f"{limit_mb} MB limit for {'binary' if is_binary else 'text'} "
            f"formats. Use --no-size-limit to override."
        )


def detect_delimiter(lines: list[str]) -> str:
    """Detect the most likely delimiter from sample lines.

    Args:
        lines: Sample lines from the file (typically first 20).

    Returns:
        Detected delimiter character. Defaults to comma.
    """
    candidates = [",", "\t", "|", ";"]
    best_delimiter = ","
    best_score = -1.0

    for delim in candidates:
        counts = [line.count(delim) for line in lines if line.strip()]
        if not counts or max(counts) == 0:
            continue
        avg = sum(counts) / len(counts)
        if avg == 0:
            continue
        variance = sum((c - avg) ** 2 for c in counts) / len(counts)
        score = avg / (1 + variance)
        if score > best_score:
            best_score = score
            best_delimiter = delim

    return best_delimiter


def detect_encoding(path: Path) -> str:
    """Detect file encoding using charset-normalizer.

    Args:
        path: Path to the text file.

    Returns:
        Detected encoding string.
    """
    with open(path, "rb") as f:
        bom = f.read(4)
    if bom.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    if bom.startswith(b"\xff\xfe"):
        return "utf-16-le"
    if bom.startswith(b"\xfe\xff"):
        return "utf-16-be"

    try:
        with open(path, encoding="utf-8") as f:
            f.read(8192)
        return "utf-8"
    except UnicodeDecodeError:
        pass

    from charset_normalizer import from_path

    result = from_path(path)
    best = result.best()
    if best and best.encoding:
        return best.encoding

    return "utf-8"


def _read_sample_lines(path: Path, encoding: str, n: int = 20) -> list[str]:
    lines: list[str] = []
    try:
        with open(path, encoding=encoding, errors="replace") as f:
            for i, line in enumerate(f):
                if i >= n:
                    break
                lines.append(line.rstrip("\n\r"))
    except Exception:  # noqa: BLE001 — broad catch intentional: best-effort sampling, failure handled by caller
        logger.debug(f"Could not read sample lines from {path}", exc_info=True)
    return lines
