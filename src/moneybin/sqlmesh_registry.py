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

import re
from functools import lru_cache
from pathlib import Path

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
    for path in sorted(_MODELS_DIR.rglob("*.sql")):
        match = _SQL_MODEL_NAME.search(path.read_text())
        if match is not None:
            names.add(match.group(1).lower())
    for path in sorted(_MODELS_DIR.rglob("*.py")):
        match = _PY_MODEL_NAME.search(path.read_text())
        if match is not None:
            names.add(match.group(1).lower())
    return frozenset(names)
