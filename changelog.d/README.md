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
and rendering. Add the label in GitHub if it does not yet exist.

## Prepare a release

1. Update the version in `pyproject.toml` and its lockfile. Preview the matching
   version with `uv run towncrier build --draft --version 0.1.0` (substitute
   the actual version).
2. Run `uv run towncrier build --yes --version 0.1.0`. Towncrier inserts the
   release after the marker and removes consumed fragments. Review both the
   generated notes and fragment deletions in the release PR.
3. **First release after adoption:** move the legacy bullets currently under
   `[Unreleased]` into the matching categories of the generated release,
   preserving their text and relative order. This is a manual reconciliation:
   Towncrier does not import those bullets. Keep one heading per category and
   leave the `[Unreleased]` heading, pending-fragment link, and marker above
   the generated release. Preserve all older dated milestone sections.
4. Run `uv run pytest tests/test_documentation_policy.py` and
   `uv run python scripts/check_release_metadata.py --tag v0.1.0` with the
   actual version. Confirm every pending change is accounted for, then review
   and merge the release PR before creating its version tag.

The existing tag-triggered release workflow publishes the prepared changelog;
it does not build or consume fragments. Milestone-only documentation updates
do not consume release fragments.
