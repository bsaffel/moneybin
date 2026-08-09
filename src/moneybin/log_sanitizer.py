"""PII-aware log formatter for MoneyBin.

SanitizedLogFormatter scans formatted log output for PII patterns and
masks them before they reach the log file. It is a runtime safety net,
not a substitute for writing clean log statements.

The formatter masks and emits a warning — it never suppresses log entries.
"""

import hashlib
import logging
import re

_sanitizer_logger = logging.getLogger(__name__)


def sql_digest(sql: str) -> str:
    """A stable, non-reversible handle for one SQL statement, safe to log.

    DuckDB and sqlglot quote the statement they failed on — a binder error
    carries a whole ``LINE 1: SELECT ...`` echo — so interpolating one of their
    messages into a log record writes any inline literal it holds to a log file,
    which ``.claude/rules/security.md`` forbids. The formatter below cannot save
    that record: it recognises SSNs, runs of eight or more digits, and dollar
    amounts, and a merchant name or a transaction description is none of those.

    Log this beside the exception's *type* instead. Two records about the same
    statement still correlate, which is what a reader of the log actually needs.
    """
    return hashlib.sha256(sql.encode()).hexdigest()[:12]


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


def _mask_ssn(text: str) -> tuple[str, bool]:
    """Mask SSN-shaped substrings; report whether anything matched."""
    masked = False

    def replacer(_match: re.Match[str]) -> str:
        nonlocal masked
        masked = True
        return "***-**-****"

    return _SSN_PATTERN.sub(replacer, text), masked


def _mask_account(text: str) -> tuple[str, bool]:
    """Mask account-number-shaped substrings; report whether anything matched."""
    masked = False

    def replacer(match: re.Match[str]) -> str:
        nonlocal masked
        masked = True
        return f"****...{match.group(1)[-4:]}"

    return _ACCOUNT_PATTERN.sub(replacer, text), masked


def _mask_dollar(text: str) -> tuple[str, bool]:
    """Mask dollar-amount-shaped substrings; report whether anything matched."""
    result = _DOLLAR_PATTERN.sub("$***", text)
    return result, result != text


def mask_pii_shaped(text: str) -> tuple[str, bool]:
    """Mask SSN- and account-shaped substrings; report whether anything matched.

    ``SanitizedLogFormatter.format()`` does NOT call this function — it runs
    the same three primitives (SSN, dollar, account) directly, in its own
    order, deliberately different from the order here (see the "Order
    matters" comment in ``format()`` below). This function is the two-of-three
    subset ``sql_query.py`` reuses for its DuckDB-error-hint backstop, which
    never carries a dollar amount to mask.
    """
    result, ssn_masked = _mask_ssn(text)
    result, account_masked = _mask_account(result)
    return result, ssn_masked or account_masked


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

        # Order matters: dollar must run before account, or an 8+ digit
        # amount (e.g. $12345678.00) gets read as an account number first
        # and leaks its last four digits instead of being fully masked.
        result, ssn_masked = _mask_ssn(formatted)
        result, dollar_masked = _mask_dollar(result)
        result, account_masked = _mask_account(result)
        masked = ssn_masked or dollar_masked or account_masked

        # Guard against re-entrant calls: if the sanitizer's own warning record
        # is passed back through this formatter (e.g. via the root logger's file
        # handler), don't emit another warning — that would be infinite recursion.
        if masked and record.name != __name__:
            # Debug, not warning: this fires routinely on sqlmesh internal
            # logs that incidentally contain dollar-amount-shaped strings.
            # The masking already happened; the audit trail belongs in file
            # logs at DEBUG level, not on the user's console.
            _sanitizer_logger.debug(
                f"PII pattern detected and masked in log output (source: {record.pathname}:{record.lineno})"
            )

        return result
