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

import ast
import logging
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

from moneybin import error_codes
from moneybin.errors import UserError, exception_origin
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
# Any `raw.<table>` reference anywhere in a model file. Deliberately not
# anchored to FROM/JOIN: a prose mention that isn't really read only ever
# *adds* a table, which fails the scan-set guard loudly and visibly. Missing
# a real reference is the silent direction, so the pattern errs wide.
_RAW_TABLE_REF = re.compile(r"\braw\.([a-z_][a-z0-9_]*)", re.IGNORECASE)
# Any `<schema>.<table>` reference in a model file, schema-anchored so an
# `alias.column` never matches.
_RELATION_REF = re.compile(
    r"\b(raw|prep|core|app|meta|seeds|synthetic|reports)\.([a-z_][a-z0-9_]*)",
    re.IGNORECASE,
)
# Comments are stripped before the relation scan, unlike `_RAW_TABLE_REF` above.
# That scan feeds a CI completeness guard, where an extra table costs a
# reviewer's attention; this one feeds a user-facing `degraded_reason` with no
# review step, and the convention here is prose that cross-references
# `schema.table` freely — `bridge_merchant_entities.sql` names
# `core.fct_transactions` in an FK comment while reading
# `prep.int_transactions__merged`. `test_model_reads_match_the_dependencies_
# sqlmesh_parses` holds the result to SQLMesh's own parse of the same files.
_SQL_COMMENT = re.compile(r"/\*.*?\*/|--[^\n]*", re.DOTALL)
# `kind <KIND>` inside the `MODEL (` header, read off the comment-stripped text.
_SQL_MODEL_KIND = re.compile(
    r"\bMODEL\s*\(.*?\bkind\s+([a-z_]+)", re.IGNORECASE | re.DOTALL
)


@dataclass(frozen=True, slots=True)
class _ModelNode:
    """One registered model's place in the dependency graph."""

    reads: frozenset[str]
    kind: str


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
            f"{len(unparsed)} transform model file(s) have an unreadable name "
            f"header and are excluded from the registered set: "
            f"{', '.join(sorted(unparsed))}"
        )
    return frozenset(names)


@lru_cache(maxsize=1)
def raw_tables_read_by_models() -> frozenset[str]:
    """Bare ``raw`` table names any SQLMesh model reads.

    This is the definition of "raw data a refresh would consume", and so of
    which arrivals make the warehouse stale. Read from the project files for
    the same reason as :func:`registered_model_names` — the caller is
    ``system_status``, and a ``Context`` costs seconds.
    """
    names: set[str] = set()
    for path in (
        *sorted(_MODELS_DIR.rglob("*.sql")),
        *sorted(_MODELS_DIR.rglob("*.py")),
    ):
        names.update(
            m.group(1).lower() for m in _RAW_TABLE_REF.finditer(path.read_text())
        )
    return frozenset(names)


def _sql_model_node(text: str) -> tuple[str, _ModelNode] | None:
    """One ``.sql`` model's name, reads and kind, read off its source."""
    match = _SQL_MODEL_NAME.search(text)
    if match is None:
        return None
    name = match.group(1).lower()
    body = _SQL_COMMENT.sub(" ", text)
    reads = frozenset(
        f"{m.group(1)}.{m.group(2)}".lower() for m in _RELATION_REF.finditer(body)
    ) - {name}
    kind = _SQL_MODEL_KIND.search(body)
    return name, _ModelNode(reads, kind.group(1).upper() if kind else "")


def _python_model_node(text: str) -> tuple[str, _ModelNode] | None:
    """One ``.py`` model's name, declared ``depends_on`` and kind.

    A Python model's reads live inside ``context.fetchdf()`` SQL strings that
    SQLMesh itself cannot see, so its ``depends_on=`` declaration is the only
    statement of them there is — and the same one SQLMesh orders the graph by.
    """
    for node in ast.walk(ast.parse(text)):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "model"
            and node.args
            and isinstance(node.args[0], ast.Constant)
        ):
            continue
        name = str(node.args[0].value).lower()
        kind, reads = "", set[str]()
        for keyword in node.keywords:
            if keyword.arg == "kind" and isinstance(keyword.value, ast.Constant):
                kind = str(keyword.value.value).upper()
            elif keyword.arg == "depends_on" and isinstance(
                keyword.value, ast.Set | ast.List | ast.Tuple
            ):
                reads = {
                    str(element.value).lower()
                    for element in keyword.value.elts
                    if isinstance(element, ast.Constant)
                }
        if not reads:
            # An undeclared read *shrinks* the downstream set, which is the
            # direction that silently drops a caveat rather than over-warning.
            logger.warning(
                f"Python model {name} declares no depends_on; anything it reads "
                "is missing from the dependency graph"
            )
        return name, _ModelNode(frozenset(reads) - {name}, kind)
    return None


@lru_cache(maxsize=1)
def _model_graph() -> dict[str, _ModelNode]:
    """Every registered model's declared reads and kind, keyed by model name."""
    graph: dict[str, _ModelNode] = {}
    for path in sorted(_MODELS_DIR.rglob("*.sql")):
        parsed = _sql_model_node(path.read_text())
        if parsed is not None:
            graph[parsed[0]] = parsed[1]
    for path in sorted(_MODELS_DIR.rglob("*.py")):
        parsed = _python_model_node(path.read_text())
        if parsed is not None:
            graph[parsed[0]] = parsed[1]
    return graph


def _relations_read_by_model() -> dict[str, frozenset[str]]:
    """Each registered model's own ``schema.table`` read set, keyed by model."""
    return {name: node.reads for name, node in _model_graph().items()}


@lru_cache(maxsize=8)
def relations_downstream_of(relation: str) -> frozenset[str]:
    """``relation`` plus every registered model that reads it, transitively."""
    downstream = {relation.lower()}
    reads = _relations_read_by_model()
    grew = True
    while grew:
        grew = False
        for name, read_set in reads.items():
            if name not in downstream and read_set & downstream:
                downstream.add(name)
                grew = True
    return frozenset(downstream)


@lru_cache(maxsize=8)
def relations_upstream_of(relation: str) -> frozenset[str]:
    """``relation`` plus every relation it is built from, transitively."""
    upstream = {relation.lower()}
    reads = _relations_read_by_model()
    frontier = [relation.lower()]
    while frontier:
        for read in reads.get(frontier.pop(), frozenset()):
            if read not in upstream:
                upstream.add(read)
                frontier.append(read)
    return frozenset(upstream)


@lru_cache(maxsize=1)
def materialized_model_names() -> frozenset[str]:
    """Models SQLMesh writes as tables, reached by a write only on a refresh.

    Everything else here is ``kind VIEW``, recomputed on read. The symbolic
    kinds a freshness scan has to exclude (``EXTERNAL``, ``EMBEDDED``) are
    declared in ``external_models.yaml``, not under ``models/``, so nothing in
    this set is symbolic.
    """
    return frozenset(
        name for name, node in _model_graph().items() if node.kind != "VIEW"
    )


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
        # Frame chain, not the traceback: `exc_info` would append
        # `<Type>: <str(exc)>`, and DuckDB names the database file in its
        # catalog errors. `SanitizedLogFormatter` masks amounts and digit runs,
        # not filesystem paths, so the raw message would defeat the generic
        # UserError below. Same shape the MCP decorator uses.
        logger.debug(
            f"model catalog read failed: {type(e).__name__} at {exception_origin(e)}"
        )
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
