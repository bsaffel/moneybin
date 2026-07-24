"""Institution name resolution for OFX/QFX/QBO imports.

Per .claude/rules/data-extraction.md, callers should not be required to
supply values present in the file. This module implements the resolution
chain so the service layer can derive a canonical institution slug:

1. parsed_ofx.fi.org (when populated)
2. parsed_ofx.fi.fid → the shared institution registry
3. filename heuristic (regex against known patterns)
4. CLI/MCP override (only when 1-3 yield nothing)
5. interactive prompt (only when interactive=True)
6. raise InstitutionResolutionError (non-interactive failure)

The registry starts small and grows via PR contributions. Unknown FIDs fall
through to step 3.

The slug this returns becomes the import's ``source_origin``, which is an input
to the ``transaction_id`` content hash, so changing what this function returns
re-keys transactions. **Editing an existing slug** re-keys every transaction
imported under it and needs a migration. **Adding a row** is safe only when no
prior import of that FID resolved a *different* slug through a later step — a
file that previously fell through to the filename heuristic (step 3) or a
``--institution`` override (step 4) will start resolving via step 2 the moment
its FID appears here, silently changing ``source_origin`` for that institution's
next import. Check existing ``source_origin`` values for the FID before adding.

Note also that step 1 wins over step 2, so a bank publishing
any ``<ORG>`` never reaches the registry: Chase files resolve to ``b1``, not
``chase``. That is deliberate and load-bearing, not an oversight — changing it
would churn ledger identity. The registry's *display* half (used by
core.dim_accounts via ``seeds.institutions``) is what fixes the user-visible
"B1" without touching any of this.
"""

import csv
import io
import logging
import re
from functools import lru_cache
from importlib import resources
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class InstitutionResolutionError(ValueError):
    """Raised when institution cannot be derived in non-interactive mode."""


#: The shared institution registry, relative to the installed ``moneybin`` package.
#: Ships via the ``sqlmesh/models/seeds/*.csv`` package-data glob.
_REGISTRY_RESOURCE = "sqlmesh/models/seeds/institutions.csv"


@lru_cache(maxsize=1)
def _fid_to_slug() -> dict[str, str]:
    """Well-known OFX FID → institution slug, from the shared registry.

    Read from ``seeds/institutions.csv`` rather than a dict literal so the FID
    mapping has exactly one home: the same CSV backs ``seeds.institutions``,
    which core.dim_accounts joins to resolve a display name. Two copies would
    drift the moment someone added a bank to one and not the other.

    Loaded lazily and cached — the import path is already deferred for
    cold-start, and this keeps a file read off module import.
    """
    raw = resources.files("moneybin").joinpath(_REGISTRY_RESOURCE).read_text()
    return {
        row["fid"]: row["slug"]
        for row in csv.DictReader(io.StringIO(raw))
        if row["fid"]
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
            logger.info("--institution argument ignored; using <FI><ORG> from file")
        return _to_slug(org)

    # Step 2: <FI><FID> lookup.
    fid = _first_fid(parsed_ofx)
    registry = _fid_to_slug()
    if fid and fid in registry:
        if cli_override:
            logger.info(f"--institution argument ignored; using FID lookup for {fid!r}")
        return registry[fid]

    # Step 3: filename heuristic.
    for pattern, slug in _FILENAME_PATTERNS:
        if pattern.search(file_path.name):
            if cli_override:
                logger.info(
                    f"--institution argument ignored; matched filename pattern {slug!r}"
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


def resolve_institution_tabular(
    *,
    file_path: Path,
    format_institution: str | None,
    cli_override: str | None,
) -> str | None:
    """Best-effort institution slug for a tabular import.

    Chain: format metadata (Tiller Institution / registered format) -> filename
    heuristic -> --institution override -> unknown (None). Unlike the OFX chain,
    *unknown is allowed* — institution is best-effort metadata, never required
    (spec Decision 3 / Decision 7).
    """
    if format_institution:
        return _to_slug(format_institution)
    for pattern, slug in _FILENAME_PATTERNS:
        if pattern.search(file_path.name):
            return slug
    if cli_override:
        return _to_slug(cli_override)
    return None


def _to_slug(name: str) -> str:
    """Convert a human-readable name to a snake_case slug.

    Raises InstitutionResolutionError if normalization yields an empty string
    (e.g., input contained only separators).
    """
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    if not slug:
        raise InstitutionResolutionError(
            f"Institution name {name!r} produced an empty slug after normalization."
        )
    return slug


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
