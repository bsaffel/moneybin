"""Which models the SQLMesh project declares, read from the project files.

Answering "is the warehouse fully built?" needs the *registered* set, not the
built set — a model that was never materialised leaves no trace in the catalog
to notice. Every other signal is built-set-derived, which is why a missing
model was invisible to all of them.

The names come from the model files rather than a ``sqlmesh.Context`` because
both callers are on hot paths: ``system_status`` and the doctor. Context init
costs seconds and opens its own state connection, which the encrypted-DB
design does not permit under the MCP timeout guard.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

from moneybin import error_codes
from moneybin.errors import UserError
from moneybin.seeds import INIT_CREATED_MODELS

if TYPE_CHECKING:
    from moneybin.database import Database

logger = logging.getLogger(__name__)

_MODELS_DIR = Path(__file__).parent / "sqlmesh" / "models"

# `MODEL (` header in .sql models: `name <schema>.<model>,` on its own line.
_SQL_MODEL_NAME = re.compile(
    r"\bMODEL\s*\(.*?\bname\s+([a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*)",
    re.IGNORECASE | re.DOTALL,
)
# `@model("<schema>.<model>", ...)` decorator in Python models.
_PY_MODEL_NAME = re.compile(
    r"@model\(\s*[\"']([a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*)[\"']",
    re.IGNORECASE,
)


@lru_cache(maxsize=1)
def registered_model_names() -> frozenset[str]:
    """Fully-qualified ``schema.model`` names the project declares.

    Cached: the model files are read-only project source that cannot change
    within a process, and both callers run on request paths.
    """
    names: set[str] = set()
    unparsed: list[str] = []
    for path, pattern in (
        *((p, _SQL_MODEL_NAME) for p in sorted(_MODELS_DIR.rglob("*.sql"))),
        *((p, _PY_MODEL_NAME) for p in sorted(_MODELS_DIR.rglob("*.py"))),
    ):
        match = pattern.search(path.read_text())
        if match is None:
            unparsed.append(path.name)
            continue
        names.add(match.group(1).lower())
    if unparsed:
        # A model whose header this regex cannot read vanishes from the
        # registered set — which would silently shrink the very check whose
        # job is noticing absent models. Say so rather than under-reporting.
        logger.warning(
            f"{len(unparsed)} SQLMesh model file(s) have an unreadable name "
            f"header and are excluded from the registered set: "
            f"{', '.join(sorted(unparsed))}"
        )
    return frozenset(names)


@dataclass(frozen=True, slots=True)
class ModelPresence:
    """Which registered models exist, and whether the warehouse was ever built.

    ``never_built`` separates two states a bare "N models missing" conflates:
    a warehouse nobody has run ``refresh_run`` against yet — the normal state
    right after ``db init``, where nothing is wrong — and one that was built
    but is now incomplete, which is a real defect. Reporting the first as a
    failure would fail the doctor on the most common first-run state.
    """

    missing: tuple[str, ...]
    built_beyond_init_count: int

    @property
    def never_built(self) -> bool:
        """True when SQLMesh has never materialised anything.

        Keyed on the registered models that opening a database does *not*
        create, because neither simpler count is honest. The total built count
        is never zero — ``db init`` alone creates five registered relations
        (:data:`~moneybin.seeds.INIT_CREATED_MODELS`) — so a fresh profile
        would read as built. An empty ``prep`` layer has the opposite failure:
        a warehouse that lost its staging views while materialised ``core``
        models survive reads as brand new, which silences the doctor invariant
        and ``freshness().pending`` on exactly the broken state they exist to
        report. A surviving non-init model is positive evidence of a prior
        apply, and nothing else creates one.
        """
        return self.built_beyond_init_count == 0


def model_presence(db: Database) -> ModelPresence:
    """Compare the registered model set against the live catalog.

    Shared by the doctor invariant and ``TransformService.freshness()`` so the
    two cannot drift — they previously ran the same query with different
    exception scopes.

    A failing catalog read propagates: there is no ``ModelPresence`` that means
    "unknown". Swallowing it returned ``built_beyond_init_count=0``, the exact
    value :attr:`ModelPresence.never_built` keys on, so an unreadable catalog
    took the healthy first-run branch in both callers — the doctor answered
    "run refresh_run" and ``freshness()`` reported ``pending=False`` for a
    database it could not inspect at all.

    It propagates as a ``UserError`` rather than DuckDB's own exception because
    ``freshness()`` has no catch of its own: this error is what reaches
    ``moneybin system status`` and ``moneybin transform status``, and
    ``handle_cli_errors`` re-raises whatever ``classify_user_error`` does not
    recognize. Raw, it is a traceback on a user-facing command; classified,
    every surface degrades — the doctor isolates it per-invariant, the MCP
    section marks itself unavailable, and the CLI prints one clean line.
    """
    try:
        rows = db.execute(
            """
            SELECT LOWER(schema_name || '.' || table_name) FROM duckdb_tables()
            UNION
            SELECT LOWER(schema_name || '.' || view_name) FROM duckdb_views()
            """
        ).fetchall()
    except Exception as e:  # noqa: BLE001 — duckdb raises untyped errors on catalog reads
        logger.debug("model catalog read failed", exc_info=True)
        raise UserError(
            "Could not read the database model catalog.",
            code=error_codes.INFRA_CATALOG_UNAVAILABLE,
            hint="💡 Run 'moneybin system doctor' to check the database.",
        ) from e
    built = {str(row[0]) for row in rows}
    registered = registered_model_names()
    return ModelPresence(
        missing=tuple(sorted(registered - built)),
        built_beyond_init_count=len((registered & built) - INIT_CREATED_MODELS),
    )
