"""Prepared export orchestration."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime

from pydantic import JsonValue

from moneybin.database import Database
from moneybin.exports.models import RedactionMode
from moneybin.exports.redaction import apply_export_redaction
from moneybin.exports.snapshot import PreparedExport, build_bundle_snapshot


class ExportService:
    """Prepare format-neutral exports from trusted semantic sources."""

    def __init__(self, db: Database) -> None:
        """Bind the database used for canonical snapshot reads."""
        self._db = db

    def prepare_bundle(
        self,
        *,
        profile: str,
        redaction_mode: RedactionMode = "redacted",
        report_id: str | None = None,
        report_parameters: Mapping[str, JsonValue] | None = None,
    ) -> PreparedExport:
        """Prepare the closed canonical bundle under one per-run output policy."""
        if report_id is not None:
            raise ValueError("bundle exports cannot include a report id")
        if report_parameters:
            raise ValueError("bundle exports cannot include report parameters")
        snapshot = build_bundle_snapshot(
            self._db,
            profile=profile,
            created_at=datetime.now(UTC),
        )
        return apply_export_redaction(snapshot, redaction_mode)
