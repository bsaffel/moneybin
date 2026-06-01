"""Audited writes to ``app.pdf_formats`` (saved PDF import-format profiles).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` that pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction.

``name`` is the semantic-slug primary key (``identifiers.md`` strategy 4); there
is no shadow id, so ``target_id`` is the format ``name``.

Recipe payload accepted as ``dict[str, object]`` to keep repo independent of the
extractors layer; callers are responsible for validation (typically via
``extractors.pdf.recipe.Recipe.model_dump()``).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import PDF_FORMATS

_PDF_FORMATS_COLUMNS = (
    "name",
    "institution_name",
    "document_kind",
    "layout_fingerprint",
    "front_end",
    "extraction_recipe",
    "routing",
    "field_mapping",
    "seed_alias",
    "sign_convention",
    "date_format",
    "number_format",
    "source",
    "version",
    "times_used",
    "last_used_at",
    "created_at",
    "updated_at",
)

# Columns stored as JSON-encoded text in DuckDB's JSON type. Reads decode them to
# Python objects so the audit before/after payload carries nested JSON rather than
# a doubly-encoded string.
_JSON_COLUMNS = frozenset({
    "layout_fingerprint",
    "extraction_recipe",
    "field_mapping",
})


def _decode_row(row: tuple[Any, ...]) -> dict[str, Any]:
    """Map a fetched row to a column → value dict, decoding JSON columns."""
    out: dict[str, Any] = {}
    for col, val in zip(_PDF_FORMATS_COLUMNS, row, strict=True):
        if col in _JSON_COLUMNS and isinstance(val, str):
            out[col] = json.loads(val)
        else:
            out[col] = val
    return out


@dataclass
class PdfFormat:
    """Row representation returned by read methods.

    Co-located with the repo (unlike ``TabularFormat``, which lives in the
    extractors layer) because ``get_by_fingerprint`` and ``list_all`` return
    full rows to service callers — there is no separate extractor type to
    reuse here, and the repo is the only producer.
    """

    name: str
    institution_name: str
    document_kind: str
    layout_fingerprint: dict[str, Any]
    front_end: str
    extraction_recipe: dict[str, Any]
    routing: str
    field_mapping: dict[str, Any] | None
    seed_alias: str | None
    sign_convention: str | None
    date_format: str | None
    number_format: str
    source: str
    version: int
    times_used: int
    last_used_at: datetime | None
    created_at: datetime | None
    updated_at: datetime | None


def _row_to_pdf_format(row: dict[str, Any]) -> PdfFormat:
    return PdfFormat(
        name=row["name"],
        institution_name=row["institution_name"],
        document_kind=row["document_kind"],
        layout_fingerprint=row["layout_fingerprint"],
        front_end=row["front_end"],
        extraction_recipe=row["extraction_recipe"],
        routing=row["routing"],
        field_mapping=row.get("field_mapping"),
        seed_alias=row.get("seed_alias"),
        sign_convention=row.get("sign_convention"),
        date_format=row.get("date_format"),
        number_format=row["number_format"],
        source=row["source"],
        version=row["version"],
        times_used=row["times_used"],
        last_used_at=row.get("last_used_at"),
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
    )


class PdfFormatsRepo(BaseRepo):
    """Audited mutations on app.pdf_formats (Invariant 10).

    On every save/bump, emits a paired app.audit_log row. Recipe versioning
    (Req 9a): bump_version() increments ``version`` and stores the prior recipe
    in audit_log.before_value, so the undo consumer can restore it.

    Recipe payload accepted as ``dict[str, object]`` to keep repo independent of
    the extractors layer; callers are responsible for validation (typically via
    ``extractors.pdf.recipe.Recipe.model_dump()``).
    """

    repository = "pdf_formats"

    table_ref = PDF_FORMATS
    pk_columns = ("name",)

    def _fetch_row(self, name: str) -> dict[str, Any] | None:
        return self._fetch_one(
            PDF_FORMATS,
            _PDF_FORMATS_COLUMNS,
            "name",
            name,
            decode=_decode_row,
        )

    def save_new(
        self,
        name: str,
        recipe: dict[str, Any],
        *,
        fingerprint: dict[str, Any],
        institution_name: str,
        document_kind: str,
        front_end: str,
        routing: str,
        field_mapping: dict[str, Any] | None = None,
        seed_alias: str | None = None,
        sign_convention: str | None = None,
        date_format: str | None = None,
        number_format: str = "us",
        source: str = "detected",
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Insert a new PDF format profile (version=1) + audit row.

        ``before`` is None (new row); ``after`` is the resulting full row.
        ``target_id`` is ``name``. Raises if a format with this name already
        exists (no upsert — bump_version() handles recipe evolution).
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            self._db.execute(
                f"""
                INSERT INTO {PDF_FORMATS.full_name} (
                    name, institution_name, document_kind,
                    layout_fingerprint, front_end, extraction_recipe, routing,
                    field_mapping, seed_alias, sign_convention, date_format,
                    number_format, source, version, times_used,
                    created_at, updated_at
                ) VALUES (
                    ?, ?, ?,
                    ?::JSON, ?, ?::JSON, ?,
                    ?::JSON, ?, ?, ?,
                    ?, ?, 1, 1,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    name,
                    institution_name,
                    document_kind,
                    json.dumps(fingerprint, sort_keys=True),
                    front_end,
                    json.dumps(recipe, sort_keys=True),
                    routing,
                    json.dumps(field_mapping, sort_keys=True)
                    if field_mapping is not None
                    else None,
                    seed_alias,
                    sign_convention,
                    date_format,
                    number_format,
                    source,
                ],
            )
            after = self._fetch_row(name)
            return self._emit_audit(
                action="pdf_format.save",
                target=(*self._audit_target, name),
                before=None,
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def bump_version(
        self,
        name: str,
        new_recipe: dict[str, Any],
        *,
        reason: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Increment ``version`` and update ``extraction_recipe`` + audit.

        ``before_value`` captures the full prior row (including the old recipe),
        enabling the undo consumer to restore it. ``after_value`` reflects the new
        recipe at the new version number. Raises ``ValueError`` if the format does
        not exist.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(self._fetch_row(name), "name", name)
            self._db.execute(
                f"""
                UPDATE {PDF_FORMATS.full_name}
                SET extraction_recipe = ?::JSON,
                    version = version + 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE name = ?
                """,  # noqa: S608  # TableRef + parameterized values
                [json.dumps(new_recipe, sort_keys=True), name],
            )
            after = self._fetch_row(name)
            return self._emit_audit(
                action="pdf_format.bump_version",
                target=(*self._audit_target, name),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
                context={"reason": reason},
            )

    def get_by_fingerprint(self, fp: dict[str, Any]) -> PdfFormat | None:
        """Return the highest-version format matching ``fp``, or None on miss.

        Serializes ``fp`` to a canonical (sorted-keys) JSON string and matches
        against the ``layout_fingerprint`` JSON column using DuckDB's JSON
        equality cast. If multiple names share the fingerprint, returns the one
        with the highest ``version`` (the most recently bumped).
        """
        fp_json = json.dumps(fp, sort_keys=True)
        row = self._db.execute(
            f"""
            SELECT {", ".join(f'"{c}"' for c in _PDF_FORMATS_COLUMNS)}
            FROM {PDF_FORMATS.full_name}
            WHERE layout_fingerprint = ?::JSON
            ORDER BY version DESC, name ASC
            LIMIT 1
            """,  # noqa: S608  # TableRef + parameterized JSON value
            [fp_json],
        ).fetchone()
        if row is None:
            return None
        return _row_to_pdf_format(_decode_row(row))

    def record_use(self, name: str) -> None:
        """Bump times_used + stamp last_used_at on an import that used this format.

        ``times_used`` counts every import a format served, including its own
        first-contact auto-derive (which ``save_new`` initialises to 1) and
        every subsequent replay. The service calls this on the replay path so
        replay imports contribute to the counter just like the first-contact
        import did.

        Per-import usage counters are observability, not user-state mutation —
        emitting an audit row for every replay would bloat app.audit_log
        without adding undo value (there is nothing to undo about "we used
        the saved recipe again"). save_new and bump_version still go through
        the audited path; only the counter bump is unaudited.
        """
        self._db.execute(
            f"""
            UPDATE {PDF_FORMATS.full_name}
            SET times_used = times_used + 1,
                last_used_at = CURRENT_TIMESTAMP
            WHERE name = ?
            """,  # noqa: S608  # TableRef + parameterized value
            [name],
        )

    def list_all(self) -> list[PdfFormat]:
        """Return all saved PDF format profiles, ordered by name."""
        rows = self._db.execute(
            f"""
            SELECT {", ".join(f'"{c}"' for c in _PDF_FORMATS_COLUMNS)}
            FROM {PDF_FORMATS.full_name}
            ORDER BY name ASC
            """,  # noqa: S608  # TableRef; read-only, no user values in SQL
        ).fetchall()
        return [_row_to_pdf_format(_decode_row(r)) for r in rows]
