"""Residual package holding only ``import_log.py``; destination is ``repositories/``.

Provider-specific loaders moved to ``src/moneybin/extractors/<name>/`` per
``docs/specs/extension-contracts.md`` (Plan 1 Tasks 2-4). What remains is
``import_log.py`` — batch-lifecycle bookkeeping over ``raw.import_log``.

Its destination is ``repositories/import_log_repo.py`` (``ImportLogRepo``), beside
the ``ImportsRepo`` that already owns ``app.imports`` for the same entity (MB-248).
Do NOT fold it into ``ImportService``, which an earlier version of this docstring
proposed: ``extractors/tabular/extractor.py`` imports this module at module level,
so moving it under ``services/`` creates the extractor-to-services inversion
MB-246 exists to delete. Two prerequisites gate the move — MB-52 slice 2 must drop
that extractor import, and ``raw.import_log`` must become ``app.import_log``,
because ``repositories/`` owns ``app.*`` tables only.
"""

__all__: list[str] = []
