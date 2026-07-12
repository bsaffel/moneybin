#!/usr/bin/env bash
#
# Clean-install smoke test: prove a built wheel actually runs the real user
# path once installed.
#
# Why this runs from a directory OUTSIDE the checkout: when the working
# directory is the repo, Python and every resource lookup can resolve through
# the source tree, so a wheel that ships none of its SQL schema, migrations,
# SQLMesh models or seed data still "works". That is exactly the defect this
# guard exists to catch — it is invisible to the normal test suite, which
# always runs inside the repo against the locked dependency set. Installing
# into a venv with no repo on sys.path and running from elsewhere is the only
# way to exercise what a `pip install moneybin` user actually gets.
#
# Usage:
#   scripts/smoke_install.sh [DIST_DIR]
#
#   DIST_DIR   Directory holding exactly one built wheel. Default: ./dist
#              (may also be passed as the DIST_DIR environment variable)
#   WORK_DIR   Scratch directory to install and run in. Must be outside the
#              repo. Default: a fresh mktemp -d. CI passes $RUNNER_TEMP.
#
# Called by .github/workflows/ci.yml on every PR and by the release workflow
# across its OS x Python matrix — one definition of "does the wheel work", so
# the release matrix cannot drift from the PR guard.

set -euo pipefail

DIST_DIR="${1:-${DIST_DIR:-dist}}"
WORK_DIR="${WORK_DIR:-$(mktemp -d)}"

# Resolve to an absolute path before leaving the repo, or the wheel becomes
# unfindable the moment we cd away.
DIST_DIR="$(cd "$DIST_DIR" && pwd)"
mkdir -p "$WORK_DIR"
WORK_DIR="$(cd "$WORK_DIR" && pwd)"

shopt -s nullglob
wheels=("$DIST_DIR"/*.whl)
shopt -u nullglob

if [ "${#wheels[@]}" -ne 1 ]; then
  echo "Expected exactly one wheel in $DIST_DIR, found ${#wheels[@]}" >&2
  printf '  %s\n' "${wheels[@]}" >&2
  exit 1
fi
WHEEL="${wheels[0]}"

VENV="$WORK_DIR/smoke-venv"
echo "==> Installing $(basename "$WHEEL") into a clean venv at $VENV"

# --seed provides pip: this venv stands in for a user's environment, so the
# wheel goes in with pip exactly as a real `pip install moneybin` would. That
# is not a violation of the project's uv-only rule, which governs project
# tooling, not the simulated user.
uv venv --seed --clear "$VENV"

# keyrings.alt is the headless keyring backend. SecretStore.set_key raises
# SecretStorageUnavailableError when no OS keyring exists and its own error
# message names keyrings.alt as the fix, so this is the documented path for a
# machine with no desktop keyring — not a test-only shim.
"$VENV/bin/pip" install --quiet "$WHEEL" keyrings.alt

MB="$VENV/bin/moneybin"

# Everything below runs from OUTSIDE the checkout (see header).
cd "$WORK_DIR"

export MONEYBIN_HOME="$WORK_DIR/moneybin-home"
export PYTHON_KEYRING_BACKEND="keyrings.alt.file.PlaintextKeyring"
# Pin the plaintext keyring's store into the scratch dir so the smoke run stays
# hermetic and never touches the real user's (or the runner's) keyring data.
export XDG_DATA_HOME="$WORK_DIR/xdg-data"

echo "==> moneybin --version"
"$MB" --version

# demo exercises the resources the wheel must actually ship: profile creation
# (SQL schema + migrations), the SQLMesh transform (models), and seed/synthetic
# data. A wheel missing any of them fails here.
echo "==> moneybin demo --persona basic"
"$MB" demo --persona basic

echo "==> moneybin system doctor"
"$MB" system doctor

echo "==> Smoke test passed: the installed wheel runs the real user path."
