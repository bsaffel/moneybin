"""Repository layer — audited writes to protected app.* tables.

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 9), every mutation of
a protected ``app.*`` table flows through a ``*Repo`` class that pairs the
write with an ``app.audit_log`` row in the same DuckDB transaction. Services
compose repositories instead of executing raw ``INSERT``/``UPDATE``/``DELETE``
against ``app.*``.
"""
