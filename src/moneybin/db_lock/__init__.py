"""Write critical-section lock primitive for the encrypted DuckDB store.

See ``docs/specs/database-writer-coordination.md`` § "PR B hardening pass" and
ADR-010 for design rationale. The lock is exclusively a write-write
coordination primitive — read-mode connections never touch it.
"""

from moneybin.db_lock._types import CheckpointReason, OperationType
from moneybin.db_lock.lock import lock_path_for, write_lock

__all__ = ["CheckpointReason", "OperationType", "lock_path_for", "write_lock"]
