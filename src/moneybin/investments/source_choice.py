"""Exact source-choice impact shared by terminal and MCP confirmation."""

from dataclasses import dataclass
from typing import Any

from moneybin import error_codes
from moneybin.errors import UserError
from moneybin.investments.event_curation import CurationRemap


@dataclass(frozen=True)
class SourceChoicePreview:
    """Read-only financial impact bound to current membership and complete elections."""

    account_id: str
    source: str | None
    changes: dict[str, dict[str, tuple[Any, Any]]]
    curation: CurationRemap
    fingerprint: str

    def verify(self, fresh: "SourceChoicePreview") -> None:
        """Refuse changed evidence before the first write."""
        if fresh.fingerprint != self.fingerprint:
            raise UserError(
                "Investment source impact changed while confirmation was open.",
                code=error_codes.MUTATION_CONFIRMATION_MISMATCH,
                hint="Review a fresh source-switch preview and confirm again.",
            )

    def message(self) -> str:
        """Describe precisely the values and complete elections being approved."""
        lines = [
            f"Use {self.source or 'every source'} for this account's investment history?",
            "Accepted Matches and existing Golden IDs are preserved.",
        ]
        for identity, fields in sorted(self.changes.items()):
            lines.append(f"Event {identity}:")
            for name, (before, after) in sorted(fields.items()):
                if name == "source_origin":
                    continue
                label = {
                    "source_type": "source",
                    "included": "included in history",
                }.get(name, name.replace("_", " "))
                lines.append(f"  {label}: {before} → {after}")
        if self.curation.replacements:
            for identity, after in sorted(self.curation.replacements.items()):
                lines.append(
                    f"Lot selection {identity}: {self.curation.before.get(identity, [])} → {after}"
                )
        else:
            lines.append("Existing lot selections are preserved.")
        lines.append("The switch commits together after full lot-selection validation.")
        return "\n".join(lines)
