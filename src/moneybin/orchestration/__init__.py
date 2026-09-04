"""Cross-service orchestration — pipelines that compose the service layer.

An orchestrator composes services; a service does not compose an orchestrator.
The direction is enforced by
``tests/moneybin/test_architecture/test_orchestration_layering.py``, which also
enumerates every upward import that predates this package — module top and
deferred method body alike, since both execute.

Keep this ``__init__`` free of re-exports. ``moneybin.orchestration.refresh``
defers most of its own imports for cold-start reasons (see its module
docstring), and a re-export here would pull it — and the service graph behind
it — into every ``import moneybin.orchestration.<anything>``.
"""
