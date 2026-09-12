"""Residual package holding only ``import_log.py``; destination is ``repositories/``.

Provider-specific loaders moved to ``src/moneybin/extractors/<name>/`` per
``docs/specs/extension-contracts.md`` (Plan 1 Tasks 2-4). What remains is
``import_log.py`` — batch-lifecycle bookkeeping over ``raw.import_log``.

Its destination is ``repositories/import_log_repo.py`` (``ImportLogRepo``), beside
the ``ImportsRepo`` that already owns ``app.imports`` for the same entity (MB-248).
Do NOT fold it into ``ImportService``, which an earlier version of this docstring
proposed: ``extractors/tabular/extractor.py`` imports this module at module level,
so moving it under ``services/`` creates the extractor-to-services inversion
MB-246 exists to delete. One prerequisite gates the move: MB-52 slice 2 must drop
that extractor import.

The schema is a separate decision, NOT a prerequisite. ``BaseRepo`` describes
itself as owning protected ``app.*`` tables and 33 of its 35 subclasses are
``app.*``, but nothing enforces that and ``ManualInvestmentTransactionsRepo``
already owns ``raw.manual_investment_transactions`` — so a ``raw.*`` repo is
possible today. Whether ``raw.import_log`` should become ``app.import_log`` is
worth deciding on its own merits; an on-disk schema change is a one-way door, and
relocating this module does not require one.
"""

__all__: list[str] = []
