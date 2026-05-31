"""Doctor invariant audits and their recipe registry (M2D PR 4).

The recipe registry lives in :mod:`moneybin.audits.recipes`; importing that
package populates the registry as a side effect. ``DoctorService`` looks up
each invariant's recipe by ``InvariantResult.name`` (the same string each
``_run_*`` method assigns to ``name=``) and uses it to fill the result's
``recovery_actions``.
"""
