"""Minimal intermediate representation for PDF extraction (Phase 1).

A document is text lines plus zero or more tables. Phase 2 extends this with
positions/bboxes for recipe authoring; Phase 1 needs only header + rows.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field


@dataclass(frozen=True)
class PdfTable:
    """One extracted table on one page: a header and equal-width string rows."""

    page: int
    header: list[str]
    rows: list[list[str]]

    def __post_init__(self) -> None:
        """Validate that all rows have the same width as the header."""
        width = len(self.header)
        for r in self.rows:
            if len(r) != width:
                raise ValueError(
                    f"Row width {len(r)} != header width {width} on page {self.page}"
                )


@dataclass(frozen=True)
class PdfDocument:
    """Front-end-neutral extraction result."""

    source_file: str
    tables: list[PdfTable] = field(default_factory=list)
    text_lines: list[str] = field(default_factory=list)

    def iter_rows(self) -> Iterator[tuple[int, dict[str, str]]]:
        """Yield (page, {header: cell}) for every row across all tables."""
        for table in self.tables:
            for row in table.rows:
                yield table.page, dict(zip(table.header, row, strict=True))
