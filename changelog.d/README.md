# Changelog fragments

Each user-visible change adds a unique Markdown fragment here. Feature PRs
leave `CHANGELOG.md` to release preparation so concurrent changes do not edit
the same section.

Use `<PR>.<category>.md` when the PR number is known, or
`+<unique-change-slug>.<category>.md` before opening the PR. Categories are
`added`, `changed`, `deprecated`, `removed`, `fixed`, and `security`.
Use separate slugs for independent changes. Keep the fragment to one or two
sentences describing the user-visible result, without a heading or bullet.
Towncrier links numeric filenames to the PR; orphan (`+`) fragments can cite
the PR in their text once it exists. Never include private account details.

```sh
uv run towncrier create +profile-overrides.fixed.md -c "Profile environment overrides now take precedence over profile defaults."
uv run towncrier check --compare-with origin/main
uv run towncrier build --draft --version 0.1.0
```

CI requires a fragment, or a reviewed `skip-changelog` label with the reason
in the PR description for internal-only work. A `release-preparation` label
identifies a PR that assembles notes and permits editing `CHANGELOG.md`.
Reviewers verify the exemption reason and release scope; labels are not a
substitute for that review. Even exempt PRs validate pending fragment names
and rendering. Add the label in GitHub if it does not yet exist. The one
exemption that needs no per-PR reason is the standing one below.

### Standing exemption: Dependabot dependency updates

One class of PR is exempt without a per-PR reason: a PR opened by
`dependabot[bot]` whose diff changes only `uv.lock`, workflow files under
`.github/workflows/`, and the dependency tables of `pyproject.toml`
(`project.dependencies`, `project.optional-dependencies`, `dependency-groups`).
`scripts/check_changelog_exemption.py` decides this from the PR's author and
diff, which are fixed when the PR opens. It never reads a label, because
Dependabot labels a PR after opening it and the `opened` run would race the
label. A commit that changes anything else on a Dependabot branch ends the
exemption.

The reason is recorded once, here: dependency updates are frequent, mechanical,
and land weekly, and a per-PR fragment would turn each into a manual stop.
Release preparation records them instead, once per release (step 2 below).

The exemption waives the fragment. It does not waive the review: a human still
reviews and merges each update. When a bumped package's release notes describe
a change a MoneyBin user will notice, such as fastmcp 4.1 expiring idle
streamable-http sessions (#700), add a fragment to the PR anyway. This is the
only automatic exemption; adding another means editing this section.

## Prepare a release

1. Update the version in `pyproject.toml` and its lockfile. Preview the matching
   version with `uv run towncrier build --draft --version 0.1.0` (substitute
   the actual version).
2. **Record dependency updates.** List the requirement changes since the last
   release with `git diff <last-tag> -- pyproject.toml`. Add one fragment that
   names each raised minimum, filed under `security` when any raise fixes a
   vulnerability and `changed` otherwise, and call out any upstream behavior
   change a user will notice that no fragment records yet.
3. Run `uv run towncrier build --yes --version 0.1.0`. Towncrier inserts the
   release after the marker and removes consumed fragments. Review both the
   generated notes and fragment deletions in the release PR.
4. **First release after adoption:** move the legacy bullets currently under
   `[Unreleased]` into the matching categories of the generated release,
   preserving their text and relative order. This is a manual reconciliation:
   Towncrier does not import those bullets. Keep one heading per category and
   leave the `[Unreleased]` heading, pending-fragment link, and marker above
   the generated release. Preserve all older dated milestone sections.
5. **Reconcile stale claims.** No ordinary PR may edit `CHANGELOG.md` (the
   rule above), so a fact a bullet states — a count, a column or table name,
   a class — can go stale between releases with nothing catching it: guards
   like `tests/moneybin/test_docs/test_internal_critical_docs.py` bind that
   kind of claim to live code everywhere *except* `CHANGELOG.md`, which they
   exclude for exactly this reason. Release prep is where that drift gets
   corrected instead. Before merging the release PR, re-read every bullet
   being moved out of `[Unreleased]` (step 4) and every bullet in the newly
   generated section against current code, and fix wording that a later
   change invalidated — update the fact, keep the bullet's shape and intent.
   This is a correction, not a rewrite: don't relitigate old phrasing that
   is still accurate.
6. Run `uv run pytest tests/test_documentation_policy.py` and
   `uv run python scripts/check_release_metadata.py --tag v0.1.0` with the
   actual version. Confirm every pending change is accounted for, then review
   and merge the release PR before creating its version tag.

The existing tag-triggered release workflow publishes the prepared changelog;
it does not build or consume fragments. Milestone-only documentation updates
do not consume release fragments.
