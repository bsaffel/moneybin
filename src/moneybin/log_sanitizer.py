"""PII-aware log formatter for MoneyBin.

SanitizedLogFormatter scans formatted log output for PII patterns and
masks them before they reach the log file. It is a runtime safety net,
not a substitute for writing clean log statements.

The formatter masks and emits a warning — it never suppresses log entries.
"""

import logging
import re

_sanitizer_logger = logging.getLogger(__name__)

# SSN: NNN-NN-NNNN. Dates like 2026-04-20 have 4 digits before the first dash
# and don't match this pattern, so no date-exclusion guard is needed.
_SSN_PATTERN = re.compile(r"\b(\d{3})-(\d{2})-(\d{4})\b")

# Account numbers: 8+ consecutive digits (not preceded or followed by a digit).
# Known false-positive risk: numeric dates (20260420), row IDs, file offsets,
# and other long digit sequences will also be masked. This is intentional —
# the formatter is a safety-net layer, not a substitute for clean log statements.
# Real account numbers should never appear in log output at all.
_ACCOUNT_PATTERN = re.compile(r"(?<!\d)(\d{8,})(?!\d)")

# Dollar amounts: $N or $N,NNN or $N.NN etc.
_DOLLAR_PATTERN = re.compile(r"\$[\d,]+(?:\.\d{2})?")


class SanitizedLogFormatter(logging.Formatter):
    """Log formatter that detects and masks PII patterns.

    Wraps an inner formatter, applying PII masking to its output.
    Can also be used standalone with a format string.

    Patterns detected:
    - SSN: NNN-NN-NNNN → ***-**-****
    - Account numbers: 8+ digits → ****...NNNN (last 4)
    - Dollar amounts: $N,NNN.NN → $***

    When a pattern is masked, a separate WARNING is emitted identifying
    the leak source (module, line number).

    Args:
        inner: Either a format string (str) or a Formatter instance to wrap.
    """

    def __init__(self, inner: str | logging.Formatter = "") -> None:
        """Initialize the sanitized formatter.

        Args:
            inner: Either a format string (str) for standalone use, or a
                Formatter instance whose output will be sanitized.
        """
        if isinstance(inner, logging.Formatter):
            super().__init__()
            self._inner = inner
        else:
            super().__init__(inner)
            self._inner = None

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record, masking any PII patterns found.

        Args:
            record: The log record to format.

        Returns:
            Formatted and sanitized log string.
        """
        if self._inner is not None:
            formatted = self._inner.format(record)
        else:
            formatted = super().format(record)
        masked = False

        # Mask SSNs
        def ssn_replacer(match: re.Match[str]) -> str:
            nonlocal masked
            masked = True
            return "***-**-****"

        result = _SSN_PATTERN.sub(ssn_replacer, formatted)

        # Mask dollar amounts
        new_result = _DOLLAR_PATTERN.sub("$***", result)
        if new_result != result:
            masked = True
            result = new_result

        # Mask account numbers (8+ digit sequences; regex guarantees len >= 8)
        def account_replacer(match: re.Match[str]) -> str:
            nonlocal masked
            masked = True
            return f"****...{match.group(1)[-4:]}"

        result = _ACCOUNT_PATTERN.sub(account_replacer, result)

        # Guard against re-entrant calls: if the sanitizer's own warning record
        # is passed back through this formatter (e.g. via the root logger's file
        # handler), don't emit another warning — that would be infinite recursion.
        if masked and record.name != __name__:
            _sanitizer_logger.warning(
                f"PII pattern detected and masked in log output (source: {record.pathname}:{record.lineno})"
            )

        return result
