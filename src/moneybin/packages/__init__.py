"""Analysis package namespace.

Reference packages (assets, us_tax — Plan 4) install as siblings here at
distribution install time via entry-points (see the
``[project.entry-points."moneybin.packages"]`` group in pyproject.toml). The
prior discovery/validation framework was removed as unreachable (MB-56); the
extension-package work designs its replacement fresh, or deliberately
resurrects the removed one from git history.
"""
