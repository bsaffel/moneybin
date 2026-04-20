"""PII-aware log formatter for MoneyBin.

SanitizedLogFormatter scans formatted log output for PII patterns and
masks them before they reach the log file. It is a runtime safety net,
not a substitute for writing clean log statements.

The formatter masks and emits a warning — it never suppresses log entries.
"""

import logging
import re

_sanitizer_logger = logging.getLogger(__name__)

# SSN: NNN-NN-NNNN (but not dates like 2026-04-20)
_SSN_PATTERN = re.compile(r"\b(\d{3})-(\d{2})-(\d{4})\b")

# Account numbers: 8+ consecutive digits (not preceded by year-like context)
_ACCOUNT_PATTERN = re.compile(r"(?<!\d)(\d{8,})(?!\d)")

# Dollar amounts: $N or $N,NNN or $N.NN etc.
_DOLLAR_PATTERN = re.compile(r"\$[\d,]+(?:\.\d{2})?")

# Date-like patterns to exclude from SSN matching: YYYY-MM-DD
_DATE_PATTERN = re.compile(r"\b(19|20)\d{2}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\b")


def _is_date(match: re.Match[str]) -> bool:
    """Check if an SSN-like match is actually a date."""
    year = int(match.group(1))
    month = int(match.group(2))
    day = int(match.group(3))
    return (1900 <= year <= 2099) and (1 <= month <= 12) and (1 <= day <= 31)


class SanitizedLogFormatter(logging.Formatter):
    """Log formatter that detects and masks PII patterns.

    Patterns detected:
    - SSN: NNN-NN-NNNN → ***-**-****
    - Account numbers: 8+ digits → ****...NNNN (last 4)
    - Dollar amounts: $N,NNN.NN → $***

    When a pattern is masked, a separate WARNING is emitted identifying
    the leak source (module, line number).
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record, masking any PII patterns found.

        Args:
            record: The log record to format.

        Returns:
            Formatted and sanitized log string.
        """
        formatted = super().format(record)
        masked = False

        # Mask SSNs (but not dates)
        def ssn_replacer(match: re.Match[str]) -> str:
            nonlocal masked
            if _is_date(match):
                return match.group(0)
            masked = True
            return "***-**-****"

        result = _SSN_PATTERN.sub(ssn_replacer, formatted)

        # Mask dollar amounts
        new_result = _DOLLAR_PATTERN.sub("$***", result)
        if new_result != result:
            masked = True
            result = new_result

        # Mask account numbers (8+ digit sequences)
        def account_replacer(match: re.Match[str]) -> str:
            nonlocal masked
            digits = match.group(1)
            if len(digits) >= 8:
                masked = True
                return f"****...{digits[-4:]}"
            return digits

        result = _ACCOUNT_PATTERN.sub(account_replacer, result)

        if masked:
            _sanitizer_logger.warning(
                "PII pattern detected and masked in log output (source: %s:%s)",
                record.pathname,
                record.lineno,
            )

        return result
