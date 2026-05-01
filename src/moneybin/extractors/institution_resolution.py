"""Institution name resolution for OFX/QFX/QBO imports.

Per .claude/rules/data-extraction.md, callers should not be required to
supply values present in the file. This module implements the resolution
chain so the service layer can derive a canonical institution slug:

1. parsed_ofx.fi.org (when populated)
2. parsed_ofx.fi.fid → static lookup table
3. filename heuristic (regex against known patterns)
4. CLI/MCP override (only when 1-3 yield nothing)
5. interactive prompt (only when interactive=True)
6. raise InstitutionResolutionError (non-interactive failure)

The static FID lookup starts small and grows via PR contributions. Unknown
FIDs fall through to step 3.
"""

import logging
import re
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class InstitutionResolutionError(ValueError):
    """Raised when institution cannot be derived in non-interactive mode."""


# Static lookup: well-known OFX FID → institution slug.
# Add entries here as PRs identify new institutions in the wild.
_FID_TO_SLUG: dict[str, str] = {
    "3000": "wells_fargo",
    "10898": "chase",
    "1601": "bank_of_america",
    "10247": "citi",
    "5950": "us_bank",
}

# Filename heuristic: regex → slug.
_FILENAME_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"wells[\s_-]*fargo", re.IGNORECASE), "wells_fargo"),
    (re.compile(r"chase", re.IGNORECASE), "chase"),
    (
        re.compile(r"bank[\s_-]*of[\s_-]*america|\bboa\b", re.IGNORECASE),
        "bank_of_america",
    ),
    (re.compile(r"\bciti\b|citibank", re.IGNORECASE), "citi"),
    (re.compile(r"us[\s_-]*bank", re.IGNORECASE), "us_bank"),
    (re.compile(r"capital[\s_-]*one", re.IGNORECASE), "capital_one"),
    (re.compile(r"discover", re.IGNORECASE), "discover"),
    (re.compile(r"amex|american[\s_-]*express", re.IGNORECASE), "amex"),
]


def resolve_institution(
    parsed_ofx: Any,
    *,
    file_path: Path,
    cli_override: str | None,
    interactive: bool,
) -> str:
    """Resolve an institution slug for an OFX/QFX/QBO file.

    Returns:
        Institution slug (snake_case, e.g. 'wells_fargo').

    Raises:
        InstitutionResolutionError: If the chain yields nothing and
            interactive=False.
    """
    # Step 1: <FI><ORG> from the file.
    org = _first_org(parsed_ofx)
    if org:
        if cli_override:
            logger.info(
                f"--institution {cli_override!r} ignored; using <FI><ORG> from file"
            )
        return _to_slug(org)

    # Step 2: <FI><FID> lookup.
    fid = _first_fid(parsed_ofx)
    if fid and fid in _FID_TO_SLUG:
        if cli_override:
            logger.info(
                f"--institution {cli_override!r} ignored; using FID lookup for {fid!r}"
            )
        return _FID_TO_SLUG[fid]

    # Step 3: filename heuristic.
    for pattern, slug in _FILENAME_PATTERNS:
        if pattern.search(file_path.name):
            if cli_override:
                logger.info(
                    f"--institution {cli_override!r} ignored; matched filename pattern {slug!r}"
                )
            return slug

    # Step 4: CLI override.
    if cli_override:
        return _to_slug(cli_override)

    # Step 5: interactive prompt.
    if interactive:
        try:
            answer = input("Institution name (e.g. 'Wells Fargo'): ").strip()
            if answer:
                return _to_slug(answer)
        except EOFError:
            pass

    # Step 6: fail.
    raise InstitutionResolutionError(
        f"Institution could not be derived from file {file_path.name!r}. "
        f"Pass --institution <name> to override."
    )


def _to_slug(name: str) -> str:
    """Convert a human-readable name to a snake_case slug."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _first_org(parsed_ofx: Any) -> str | None:
    """Return the first non-empty <FI><ORG> across all accounts in the file."""
    for account in getattr(parsed_ofx, "accounts", []):
        inst = getattr(account, "institution", None)
        if inst is None:
            continue
        org = getattr(inst, "organization", None)
        if org:
            return str(org).strip() or None
    return None


def _first_fid(parsed_ofx: Any) -> str | None:
    """Return the first non-empty <FI><FID> across all accounts in the file."""
    for account in getattr(parsed_ofx, "accounts", []):
        inst = getattr(account, "institution", None)
        if inst is None:
            continue
        fid = getattr(inst, "fid", None)
        if fid:
            return str(fid).strip() or None
    return None
