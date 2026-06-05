"""Write critical-section lock primitive for the encrypted DuckDB store.

See ``docs/specs/database-writer-coordination.md`` § "PR B hardening pass" and
ADR-010 for design rationale. The lock is exclusively a write-write
coordination primitive — read-mode connections never touch it.
"""

from moneybin.db_lock._types import CheckpointReason, OperationType

__all__ = ["CheckpointReason", "OperationType"]
